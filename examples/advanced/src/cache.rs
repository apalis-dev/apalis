use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

#[derive(Debug, Clone)]
pub struct EmailValidation {
    pub valid: bool,
    pub disposable: bool,
}

#[derive(Debug, Clone)]
pub struct ValidEmailCache(Arc<Mutex<HashMap<String, EmailValidation>>>);

impl Default for ValidEmailCache {
    fn default() -> Self {
        Self::new()
    }
}

impl ValidEmailCache {
    #[must_use]
    pub fn new() -> Self {
        Self(Arc::default())
    }

    #[must_use]
    pub fn get(&self, key: &str) -> Option<EmailValidation> {
        self.0.lock().unwrap().get(key).cloned()
    }

    #[must_use]
    pub fn contains(&self, key: &str) -> bool {
        self.0.lock().unwrap().contains_key(key)
    }

    pub fn insert(&self, key: impl Into<String>, value: EmailValidation) {
        self.0
            .lock()
            .map(|mut s| s.insert(key.into(), value))
            .unwrap();
    }

    #[must_use]
    pub fn remove(&self, key: &str) -> Option<EmailValidation> {
        self.0.lock().unwrap().remove(key)
    }

    pub fn clear(&self) {
        self.0.lock().unwrap().clear();
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.0.lock().unwrap().len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.lock().unwrap().is_empty()
    }
}

pub async fn refresh(email_to: String, cache: &ValidEmailCache) {
    // Simulate an expensive validation request.
    tokio::time::sleep(Duration::from_secs(1)).await;

    let validation = EmailValidation {
        valid: true,
        disposable: false,
    };

    cache.insert(email_to, validation.clone());

    tracing::debug!(
        valid = validation.valid,
        disposable = validation.disposable,
        "Email validation cache refreshed"
    );
}
