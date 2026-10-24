#![cfg(feature = "shared")]

use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll},
};

use apalis_core::{
    backend::{Backend, FetchById, WireFormatBackend, ext::shared::Shared},
    task::{Task, builder::TaskBuilder, task_id::TaskId},
    worker::context::WorkerContext,
};
use futures_channel::{mpsc, oneshot};
use futures_sink::Sink;
use futures_util::{SinkExt, StreamExt, task::noop_waker};

#[derive(Default)]
struct Probe {
    block_ready: AtomicBool,
    ready_calls: AtomicUsize,
    finish_calls: AtomicUsize,
}

struct TestBackend {
    probe: Arc<Probe>,
    sender: mpsc::UnboundedSender<Task<u32>>,
    release_fetch: oneshot::Receiver<()>,
}

impl WireFormatBackend for TestBackend {
    type Codec = ();
    type Compact = u32;

    fn codec(&self) -> &Self::Codec {
        &()
    }
}

impl Backend for TestBackend {
    type Task = Task<u32>;
    type Error = mpsc::SendError;

    fn poll_ready(
        &mut self,
        _: &mut Context<'_>,
        _: &WorkerContext,
    ) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_next(
        &mut self,
        _: &mut Context<'_>,
        _: &WorkerContext,
    ) -> Poll<Option<Result<Self::Task, Self::Error>>> {
        Poll::Ready(None)
    }

    fn poll_close(
        &mut self,
        _: &mut Context<'_>,
        _: &WorkerContext,
    ) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

impl FetchById for TestBackend {
    async fn fetch_by_id(&mut self, _: &TaskId) -> Result<Option<Self::Task>, Self::Error> {
        (&mut self.release_fetch).await.unwrap();
        Ok(None)
    }
}

impl Sink<Task<u32>> for TestBackend {
    type Error = mpsc::SendError;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.probe.ready_calls.fetch_add(1, Ordering::Relaxed);
        if self.probe.block_ready.load(Ordering::Relaxed) {
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        self.sender.poll_ready_unpin(cx)
    }

    fn start_send(mut self: Pin<&mut Self>, item: Task<u32>) -> Result<(), Self::Error> {
        self.sender.start_send_unpin(item)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.probe.finish_calls.fetch_add(1, Ordering::Relaxed);
        self.sender.poll_flush_unpin(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.probe.finish_calls.fetch_add(1, Ordering::Relaxed);
        self.sender.poll_close_unpin(cx)
    }
}

fn check_delivery(contended: bool, close: bool, pending: bool, disconnected: bool) {
    let (sender, mut receiver) = mpsc::unbounded();
    let (release, release_fetch) = oneshot::channel();
    let probe = Arc::new(Probe::default());
    let mut shared = Shared::new(TestBackend {
        probe: probe.clone(),
        sender,
        release_fetch,
    });
    let mut reader = shared.clone();
    let task_id = TaskId::from_string("fetch".to_owned());
    let mut fetch = Box::pin(reader.fetch_by_id(&task_id));
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);

    assert!(matches!(
        shared.poll_ready_unpin(&mut cx),
        Poll::Ready(Ok(()))
    ));
    if contended {
        // A public operation on another handle takes the lock after readiness.
        assert!(fetch.as_mut().poll(&mut cx).is_pending());
    }
    shared
        .start_send_unpin(TaskBuilder::new(42).build())
        .unwrap();

    if contended {
        let result = if close {
            shared.poll_close_unpin(&mut cx)
        } else {
            shared.poll_flush_unpin(&mut cx)
        };
        assert!(result.is_pending());
        release.send(()).unwrap();
        assert!(matches!(
            fetch.as_mut().poll(&mut cx),
            Poll::Ready(Ok(None))
        ));
    }
    drop(fetch);
    drop(reader);

    if pending {
        probe.block_ready.store(true, Ordering::Relaxed);
        let result = if close {
            shared.poll_close_unpin(&mut cx)
        } else {
            shared.poll_flush_unpin(&mut cx)
        };
        assert!(result.is_pending());
        assert_eq!(probe.finish_calls.load(Ordering::Relaxed), 0);
        assert!(receiver.poll_next_unpin(&mut cx).is_pending());
        probe.block_ready.store(false, Ordering::Relaxed);
    }
    if disconnected {
        receiver.close();
    }
    let result = if close {
        shared.poll_close_unpin(&mut cx)
    } else {
        shared.poll_flush_unpin(&mut cx)
    };
    if disconnected {
        assert!(matches!(result, Poll::Ready(Err(_))));
        assert_eq!(probe.finish_calls.load(Ordering::Relaxed), 0);
        return;
    }
    assert!(matches!(result, Poll::Ready(Ok(()))));
    drop(shared);

    match receiver.poll_next_unpin(&mut cx) {
        Poll::Ready(Some(task)) => assert_eq!(task.args, 42),
        other => panic!("accepted task was not delivered: {other:?}"),
    }
    assert!(matches!(
        receiver.poll_next_unpin(&mut cx),
        Poll::Ready(None)
    ));
}

#[test]
fn flush_delivers_task_buffered_during_contention() {
    check_delivery(true, false, false, false);
}

#[test]
fn close_delivers_task_buffered_during_contention() {
    check_delivery(true, true, false, false);
}

#[test]
fn flush_delivers_task_without_contention() {
    check_delivery(false, false, false, false);
}

#[test]
fn close_delivers_task_without_contention() {
    check_delivery(false, true, false, false);
}

#[test]
fn flush_preserves_buffer_while_backend_is_pending() {
    check_delivery(true, false, true, false);
}

#[test]
fn close_preserves_buffer_while_backend_is_pending() {
    check_delivery(true, true, true, false);
}

#[test]
fn flush_propagates_buffer_drain_error() {
    check_delivery(true, false, false, true);
}

#[test]
fn close_propagates_buffer_drain_error() {
    check_delivery(true, true, false, true);
}

#[test]
fn empty_flush_and_close_do_not_require_backend_readiness() {
    for close in [false, true] {
        let (sender, _receiver) = mpsc::unbounded();
        let (_release, release_fetch) = oneshot::channel();
        let probe = Arc::new(Probe::default());
        probe.block_ready.store(true, Ordering::Relaxed);
        let mut shared = Shared::new(TestBackend {
            probe: probe.clone(),
            sender,
            release_fetch,
        });
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let result = if close {
            shared.poll_close_unpin(&mut cx)
        } else {
            shared.poll_flush_unpin(&mut cx)
        };
        assert!(matches!(result, Poll::Ready(Ok(()))));
        assert_eq!(probe.ready_calls.load(Ordering::Relaxed), 0);
        assert_eq!(probe.finish_calls.load(Ordering::Relaxed), 1);
    }
}
