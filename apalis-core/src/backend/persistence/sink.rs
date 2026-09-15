use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures_sink::Sink;
use futures_util::FutureExt;

use crate::{
    backend::persistence::{Persisted, Persistence},
    task::Task,
};

impl<P> Sink<Task<P::Compact>> for Persisted<P>
where
    P: Persistence + Unpin,
    P::Compact: Send + Unpin + 'static,
{
    type Error = P::Error;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: Task<P::Compact>) -> Result<(), Self::Error> {
        self.get_mut().sink_buffer.push(item);
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();

        // If there's no existing future and buffer is empty, we're done
        if this.sink_future.is_none() && this.sink_buffer.is_empty() {
            return Poll::Ready(Ok(()));
        }

        // Create the future only if we don't have one and there's work to do
        if this.sink_future.is_none() && !this.sink_buffer.is_empty() {
            let mut provider = this.inner.clone();
            let buffer = std::mem::take(&mut this.sink_buffer);
            let sink_fut = async move { provider.push_tasks(buffer).await };
            this.sink_future = Some(sink_fut.boxed().into());
        }

        if let Some(mut fut) = this.sink_future.take() {
            match fut.poll_unpin(cx) {
                Poll::Ready(Ok(())) => Poll::Ready(Ok(())),
                Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
                Poll::Pending => {
                    // Future is still pending, put it back and return Pending
                    this.sink_future = Some(fut);
                    Poll::Pending
                }
            }
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.poll_flush(cx)
    }
}
