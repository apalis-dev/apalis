use std::{fmt::Debug, marker::PhantomData, sync::Arc};

use aes_gcm::{
    Aes256Gcm, Key, Nonce,
    aead::{Aead, AeadCore, KeyInit, OsRng},
};
use apalis_core::{error::BoxDynError, task::Task};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tower::Service;

const NONCE_SIZE: usize = 12;

#[derive(Debug, thiserror::Error)]
pub enum EncryptionError {
    #[error("encryption failed: {0}")]
    Encrypt(String),
    #[error("decryption failed: {0}")]
    Decrypt(String),
    #[error("serialization failed: {0}")]
    Serialize(#[from] serde_json::Error),
    #[error("payload too short to contain nonce")]
    PayloadTooShort,
}

#[derive(Clone)]
pub struct EncryptionKey {
    cipher: Arc<Aes256Gcm>,
}

impl Debug for EncryptionKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EncryptionKey")
            .field("cipher", &"<redacted>")
            .finish()
    }
}

impl EncryptionKey {
    pub fn generate() -> Self {
        let key = Aes256Gcm::generate_key(&mut OsRng);
        Self {
            cipher: Arc::new(Aes256Gcm::new(&key)),
        }
    }

    pub fn from_bytes(key_bytes: &[u8; 32]) -> Self {
        let key = Key::<Aes256Gcm>::from_slice(key_bytes);
        Self {
            cipher: Arc::new(Aes256Gcm::new(key)),
        }
    }

    fn encrypt(&self, plaintext: &[u8]) -> Result<Vec<u8>, EncryptionError> {
        let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
        let ciphertext = self
            .cipher
            .encrypt(&nonce, plaintext)
            .map_err(|e| EncryptionError::Encrypt(e.to_string()))?;
        let mut output = Vec::with_capacity(NONCE_SIZE + ciphertext.len());
        output.extend_from_slice(&nonce);
        output.extend_from_slice(&ciphertext);
        Ok(output)
    }

    fn decrypt(&self, data: &[u8]) -> Result<Vec<u8>, EncryptionError> {
        if data.len() < NONCE_SIZE {
            return Err(EncryptionError::PayloadTooShort);
        }
        let (nonce_bytes, ciphertext) = data.split_at(NONCE_SIZE);
        let nonce = Nonce::from_slice(nonce_bytes);
        self.cipher
            .decrypt(nonce, ciphertext)
            .map_err(|e| EncryptionError::Decrypt(e.to_string()))
    }
}

/// An encrypted job envelope that preserves the original job type `T` for routing
/// while storing the payload as encrypted bytes.
///
/// The type parameter `T` is never stored directly — it exists only as a phantom
/// so that `EncryptedJob<Email>` and `EncryptedJob<Report>` are distinct types,
/// giving each its own queue name for backend routing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptedJob<T> {
    payload: Vec<u8>,
    #[serde(skip)]
    _marker: PhantomData<T>,
}

impl<T: Serialize> EncryptedJob<T> {
    pub fn seal(key: &EncryptionKey, job: &T) -> Result<Self, EncryptionError> {
        let plaintext = serde_json::to_vec(job)?;
        let payload = key.encrypt(&plaintext)?;
        Ok(Self {
            payload,
            _marker: PhantomData,
        })
    }
}

impl<T: DeserializeOwned> EncryptedJob<T> {
    pub fn open(&self, key: &EncryptionKey) -> Result<T, EncryptionError> {
        let plaintext = key.decrypt(&self.payload)?;
        serde_json::from_slice(&plaintext).map_err(EncryptionError::Serialize)
    }
}

/// Encrypts a job and pushes it to the backend.
///
/// The backend must accept `EncryptedJob<T>` as its `Args` type.
pub async fn push_encrypted<T, B>(
    backend: &mut B,
    key: &EncryptionKey,
    job: T,
) -> Result<(), BoxDynError>
where
    T: Serialize + Send,
    B: apalis_core::backend::TaskSink<EncryptedJob<T>>,
    B::Error: std::error::Error + Send + Sync + 'static,
{
    let encrypted = EncryptedJob::seal(key, &job)?;
    backend.push(encrypted).await?;
    Ok(())
}

/// A `Service` that accepts `Task<EncryptedJob<T>>`, decrypts it, and delegates
/// to an inner handler function that works with the plaintext `T`.
///
/// Pass this directly to `WorkerBuilder::build()`:
/// ```ignore
/// WorkerBuilder::new("worker")
///     .backend(storage)  // MemoryStorage<EncryptedJob<Email>>
///     .build(DecryptService::new(key, handle_email))
/// ```
#[derive(Debug, Clone)]
pub struct DecryptService<F, T> {
    key: EncryptionKey,
    handler: F,
    _marker: PhantomData<T>,
}

impl<F, T> DecryptService<F, T> {
    pub fn new(key: EncryptionKey, handler: F) -> Self {
        Self {
            key,
            handler,
            _marker: PhantomData,
        }
    }
}

impl<F, T, Fut, Ctx, IdType> Service<Task<EncryptedJob<T>, Ctx, IdType>> for DecryptService<F, T>
where
    F: FnMut(T) -> Fut + Clone,
    Fut: std::future::Future + Send + 'static,
    Fut::Output: apalis_core::task_fn::IntoResponse,
    T: DeserializeOwned,
{
    type Response = <Fut::Output as apalis_core::task_fn::IntoResponse>::Output;
    type Error = BoxDynError;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
    >;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, task: Task<EncryptedJob<T>, Ctx, IdType>) -> Self::Future {
        match task.args.open(&self.key) {
            Ok(decrypted) => {
                let fut = (self.handler.clone())(decrypted);
                Box::pin(async move {
                    use apalis_core::task_fn::IntoResponse;
                    fut.await.into_response()
                })
            }
            Err(e) => Box::pin(async move { Err(e.into()) }),
        }
    }
}


