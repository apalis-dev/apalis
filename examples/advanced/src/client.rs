use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use email_service::Email;

use crate::Error;

#[derive(Debug)]
pub(crate) struct ExpensiveClient;

#[derive(Debug, Clone)]
pub(crate) struct EmailClient {
    client: Arc<ExpensiveClient>,
    remaining_api_calls: Arc<AtomicUsize>,
}

impl EmailClient {
    pub(crate) fn new() -> Self {
        // We want to simulate having less than enough api calls
        Self {
            client: Arc::new(ExpensiveClient),
            remaining_api_calls: Arc::new(10.into()),
        }
    }

    /// This assumes the email is already validated
    pub(crate) async fn send_unchecked(&self, email: Email) -> Result<(), Error> {
        tracing::info!(
            "Sending email {email:?} using the reused client: {:?}",
            self.client
        );
        self.remaining_api_calls
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                value.checked_sub(1)
            })
            .map_err(|_| Error::ApiRateLimit)?;
        Ok(())
    }

    /// Sends but validates first
    pub(crate) async fn send(&self, email: Email) -> Result<(), Error> {
        tracing::info!(
            "Sending email {email:?} using the reused client: {:?}",
            self.client
        );
        self.remaining_api_calls
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                value.checked_sub(2)
            })
            .map_err(|_| Error::ApiRateLimit)?;
        Ok(())
    }

    pub(crate) fn remaining_api_calls(&self) -> usize {
        self.remaining_api_calls.load(Ordering::SeqCst)
    }
}
