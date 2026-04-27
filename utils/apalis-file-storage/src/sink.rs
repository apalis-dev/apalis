use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures_channel::mpsc::SendError;
use futures_sink::Sink;
use serde::{Serialize, de::Deserialize};
use serde_json::Value;

use apalis_core::task::{
    Task,
    task_id::{RandomId, TaskId},
};

use crate::{
    JsonMapMetadata, JsonStorage,
    util::{TaskKey, TaskWithMeta},
};

impl<Args> Sink<Task<Value, JsonMapMetadata, RandomId>> for JsonStorage<Args>
where
    Args: Unpin + Serialize + for<'de> Deserialize<'de>,
{
    type Error = SendError;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(
        self: Pin<&mut Self>,
        item: Task<Value, JsonMapMetadata, RandomId>,
    ) -> Result<(), Self::Error> {
        let this = Pin::get_mut(self);

        let task_id = item
            .parts
            .task_id
            .clone()
            .unwrap_or(TaskId::new(RandomId::default()));

        let queue = std::any::type_name::<Args>().to_owned();
        let idempotency_key = item.parts.idempotency_key.clone();

        // Prevent duplicates already stored in tasks
        if let Some(ref key) = idempotency_key {
            let tasks = this.tasks.read().unwrap();

            let exists = tasks.values().any(|task| {
                task.idempotency_key
                    .as_ref()
                    .map(|existing| existing == key)
                    .unwrap_or(false)
            });

            if exists {
                return Ok(());
            }
        }

        let task_key = TaskKey {
            task_id,
            queue,
            status: apalis_core::task::status::Status::Pending,
        };

        this.tasks.write().unwrap().insert(
            task_key,
            TaskWithMeta {
                args: item.args,
                ctx: item.parts.ctx,
                result: None,
                idempotency_key,
            },
        );

        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = Pin::get_mut(self);

        let tasks: Vec<_> = this.buffer.drain(..).collect();

        for task in tasks {
            let task_id = task
                .parts
                .task_id
                .clone()
                .unwrap_or(TaskId::new(RandomId::default()));

            let key = TaskKey {
                task_id,
                queue: std::any::type_name::<Args>().to_owned(),
                status: apalis_core::task::status::Status::Pending,
            };

            this.insert(
                &key,
                TaskWithMeta {
                    args: task.args,
                    ctx: task.parts.ctx,
                    result: None,
                    idempotency_key: task.parts.idempotency_key.clone(),
                },
            )
            .unwrap();
        }

        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Sink::<Task<Value, JsonMapMetadata, RandomId>>::poll_flush(self, cx)
    }
}
