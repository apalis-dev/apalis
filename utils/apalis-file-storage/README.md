# File based backends

File-based backend for persisting tasks and results.
Currently only supports JSON.

## Features

- **Sink support**: Ability to push new tasks.
- **Codec Support**: Serialization support for arguments using JSON.
- **Workflow Support**: Flexible enough to support workflows.
- **Ack Support**: Allows acknowledgement of task completion.
- **WaitForCompletion**: Wait for tasks to complete without blocking.

## Usage Example

```rust
use apalis_file_storage::JsonStorage;
use apalis_core::worker::builder::WorkerBuilder;
use std::time::Duration;
use apalis_core::worker::context::WorkerContext;
use apalis_core::backend::TaskSink;
use apalis_core::error::BoxDynError;
use apalis_core::worker::ext::event_listener::EventListenerExt;

#[tokio::main]
async fn main() {
    let mut json_store = JsonStorage::new_temp().unwrap();
    json_store.push(42).await.unwrap();


    async fn task(task: u32, ctx: WorkerContext) -> Result<(), BoxDynError> {
        tokio::time::sleep(Duration::from_secs(1)).await;
        ctx.stop().unwrap();
        Ok(())
    }

    let worker = WorkerBuilder::new("rango-tango")
        .backend(json_store)
        .on_event(|ctx, ev| {
            println!("On Event = {:?}", ev);
        })
        .build(task);
    worker.run().await.unwrap();
}
```

## Implementation Notes

- Tasks are stored in a file, each line representing a serialized task entry.
- All operations are thread-safe using `RwLock`.
- Data is atomically persisted to disk to avoid corruption.
- Supports temporary storage for testing and ephemeral use cases.
