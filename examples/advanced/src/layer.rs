use std::{
    fmt::Debug,
    task::{Context, Poll},
};

use apalis::prelude::*;
use tracing::warn;

use crate::client::EmailClient;

#[derive(Debug, Clone)]
pub(crate) struct ClientLayer {
    client: EmailClient,
}

impl ClientLayer {
    pub(crate) fn new(client: EmailClient) -> Self {
        Self { client }
    }
}

impl<S> Layer<S> for ClientLayer {
    type Service = ClientService<S>;

    fn layer(&self, service: S) -> Self::Service {
        ClientService {
            client: self.client.clone(),
            service,
            worker: None,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ClientService<S> {
    client: EmailClient,
    service: S,
    worker: Option<WorkerContext>,
}

impl<S, Args> Service<Task<Args>> for ClientService<S>
where
    S: Service<Task<Args>> + Clone,
    Args: Debug,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    /// This allows you to pause the worker execution by returning [Poll::Pending]
    /// If you do this remember to store the waker and wake when you get more calls
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self.client.remaining_api_calls() == 0 {
            warn!("You are out of tokens the worker will not consume anymore tasks");
            if self.worker.as_ref().is_some_and(|s| s.is_shutting_down()) {
                return Poll::Ready(Ok(()));
            }
            return Poll::Pending;
        }
        self.service.poll_ready(cx)
    }

    fn call(&mut self, mut req: Task<Args>) -> Self::Future {
        if self.worker.is_none() {
            let worker: &WorkerContext = req.data().get().unwrap();
            self.worker = Some(worker.clone());
        }

        req.inject_data(self.client.clone());
        self.service.call(req)
    }
}
