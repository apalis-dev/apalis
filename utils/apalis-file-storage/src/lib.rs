#![cfg_attr(docsrs, feature(doc_cfg))]
#![doc = include_str!("../README.md")]
use apalis_codec::json::JsonCodec;
use futures_core::{Stream, stream::BoxStream};
use futures_util::{
    StreamExt, TryStreamExt,
    future::{Ready, ready},
    stream,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use std::{
    collections::{BTreeMap, HashMap},
    fmt::Debug,
    fs::{File, OpenOptions},
    io::{self, BufRead, Seek, SeekFrom, Write},
    marker::PhantomData,
    num::ParseIntError,
    path::{Path, PathBuf},
    pin::Pin,
    str::FromStr,
    sync::{Arc, Mutex, RwLock},
    task::{Context, Poll},
    time::Duration,
};
use tempfile::NamedTempFile;

use crate::error::FileStorageError;

use self::util::RawTask;
use apalis_core::{
    backend::{Backend, BackendExt, TaskResult, TaskStream, WaitForCompletion, queue::Queue},
    error::BoxDynError,
    features_table,
    task::{
        ExecutionContext, Task,
        builder::TaskBuilder,
        metadata::MetadataStore,
        status::Status,
        task_id::{RandomId, TaskId},
    },
    worker::{
        context::WorkerContext,
        ext::ack::{Acknowledge, AcknowledgeLayer},
    },
};
use std::io::BufReader;

use fd_lock::RwLock as FileLock;

mod adapter;
mod error;
mod shared;
mod sink;
mod util;

pub use self::shared::SharedJsonStore;
pub use adapter::Adapter;

/// Handles the sync policy for when to flush pending changes to disk.
#[derive(Debug, Clone)]
pub enum SyncPolicy {
    /// Flush to disk after every change.
    Instant,
    /// Accumulate changes; caller drives `tick()` on a timer.
    Periodic(Duration),
    /// Never flush automatically; caller drives `flush()`.
    Manual,
}

#[derive(Debug)]
enum PendingChange {
    /// Append a new entry (already serialized, includes `\n`).
    Append(Vec<u8>),
    /// Rewrite the line_id` with new content.
    RewriteLine {
        line_id: usize,
        /// The fully serialized replacement line, including `\n`.
        new_bytes: Vec<u8>,
    },
}

/// A backend that persists to a file using json encoding
///
/// *Warning*: This backend is not optimized for high-throughput scenarios and is best suited for development, testing, or low-volume workloads.
///
/// # Example
///
/// Creates a temporary JSON storage backend
/// ```rust
/// # use apalis_file_storage::JsonStorage;;
/// # pub fn setup_json_storage() -> JsonStorage<u32> {
/// let mut backend = JsonStorage::new_temp().unwrap();
/// # backend
/// # }
/// ```
#[doc = features_table! {
    setup = r#"
        # {
        #   use apalis_file_storage::JsonStorage;;
        #   let mut backend = JsonStorage::new_temp().unwrap();
        #   backend
        # };
    "#,
    Backend => supported("Basic Backend functionality", true),
    TaskSink => supported("Ability to push new tasks", true),
    Serialization => limited("Serialization support for arguments. Only accepts `json`", false),
    WebUI => not_implemented("Expose a web interface for monitoring tasks"),
    FetchById => not_implemented("Allow fetching a task by its ID"),
    RegisterWorker => not_supported("Allow registering a worker with the backend"),
    "[`PipeExt`]" => supported("Allow other backends to pipe to this backend", false),
    MakeShared => supported("Share the same storage across multiple workers via [`SharedJsonStore`]", false),
    Workflow => supported("Flexible enough to support workflows", true),
    WaitForCompletion => supported("Wait for tasks to complete without blocking", true),
    ResumeById => not_implemented("Resume a task by its ID"),
    ResumeAbandoned => not_implemented("Resume abandoned tasks"),
    ListWorkers => not_supported("List all workers registered with the backend"),
    ListTasks => not_implemented("List all tasks in the backend"),
}]
#[derive(Debug)]
pub struct FileStorage<Args, A: Adapter> {
    adapter: A,
    lock: Arc<RwLock<FileLock<File>>>,
    path: PathBuf,
    /// In-memory view of all entries; index == job_id.
    entries: Arc<RwLock<Vec<RawTask>>>,
    /// Staged mutations not yet flushed to disk.
    pending: Arc<RwLock<Vec<PendingChange>>>,
    /// Next job_id to yield from the `Stream` impl.
    read_cursor: usize,
    sync_policy: SyncPolicy,
    last_flush: std::time::Instant,
    _args: PhantomData<Args>,
}

impl<Args, A> Clone for FileStorage<Args, A>
where
    A: Adapter + Clone,
{
    fn clone(&self) -> Self {
        Self {
            adapter: self.adapter.clone(),
            lock: Arc::clone(&self.lock),
            path: self.path.clone(),
            entries: Arc::clone(&self.entries),
            pending: Arc::clone(&self.pending),
            read_cursor: self.read_cursor,
            sync_policy: self.sync_policy.clone(),
            last_flush: self.last_flush,
            _args: PhantomData,
        }
    }
}

/// A JSON adapter for serializing and deserializing tasks.
#[derive(Debug, Clone, Copy, Default)]
pub struct JsonAdapter;

/// A `FileStorage` using `JsonAdapter` for line encoding.
pub type JsonStorage<Args> = FileStorage<Args, JsonAdapter>;

impl Adapter for JsonAdapter {
    type Line = serde_json::Value;

    type Error = serde_json::Error;

    fn serialize(&self, line: &Self::Line) -> Result<Vec<u8>, Self::Error> {
        let mut bytes = serde_json::to_vec(line)?;
        bytes.push(b'\n');
        Ok(bytes)
    }

    fn deserialize(&self, raw: &[u8]) -> Result<Self::Line, Self::Error> {
        serde_json::from_slice(raw.trim_ascii())
    }

    fn to_entry(&self, line: Self::Line) -> Result<RawTask, Self::Error> {
        serde_json::from_value(line)
    }

    fn from_entry(entry: &RawTask) -> Result<Self::Line, Self::Error> {
        serde_json::to_value(entry)
    }
}

impl<A: Adapter, Args> FileStorage<Args, A> {
    /// Creates a new `FileStorage` instance using the specified file path.
    pub fn new(path: impl AsRef<Path>) -> Result<Self, FileStorageError<A>>
    where
        A: Default,
    {
        let adapter = A::default();
        let path = path.as_ref().to_path_buf();

        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(&path)
            .map_err(FileStorageError::Io)?;

        let lock = Arc::new(RwLock::new(FileLock::new(file)));

        let entries = {
            let guard = lock.read().map_err(|e| {
                FileStorageError::Io(std::io::Error::new(
                    std::io::ErrorKind::WouldBlock,
                    e.to_string(),
                ))
            })?;
            let guard = guard
                .try_read()
                .map_err(|_| FileStorageError::WouldBlockLock)?;
            Self::load_entries(&adapter, &guard)?
        };

        Ok(Self {
            adapter,
            lock,
            path,
            entries: Arc::new(RwLock::new(entries)),
            pending: Default::default(),
            read_cursor: 0,
            sync_policy: SyncPolicy::Instant,
            last_flush: std::time::Instant::now(),
            _args: PhantomData,
        })
    }

    /// Creates a new temporary `FileStorage` instance.
    pub fn new_temp() -> Result<Self, FileStorageError<A>>
    where
        A: Default,
    {
        let p = NamedTempFile::new()?.path().to_path_buf();
        Self::new(p)
    }

    /// Attach a result to the entry identified by `line_id`.
    pub fn set_result(
        &mut self,
        line_id: usize,
        result: serde_json::Value,
    ) -> Result<(), FileStorageError<A>> {
        {
            let mut entries = self
                .entries
                .try_write()
                .map_err(|_| FileStorageError::WouldBlockLock)?;
            let entry = entries
                .get_mut(line_id)
                .ok_or(FileStorageError::JobNotFound { line_id })?;

            entry.result = Some(result);

            // Serialize the updated entry through the adapter's native Line type.
            let line = A::from_entry(entry).map_err(FileStorageError::AdapterError)?;
            let new_bytes = self
                .adapter
                .serialize(&line)
                .map_err(FileStorageError::AdapterError)?;

            self.pending
                .try_write()
                .map_err(|_| FileStorageError::WouldBlockLock)?
                .push(PendingChange::RewriteLine { line_id, new_bytes });
        }

        if matches!(self.sync_policy, SyncPolicy::Instant) {
            self.flush()?;
        }
        Ok(())
    }

    /// Persist all pending changes under an exclusive file lock.
    pub fn flush(&mut self) -> Result<(), FileStorageError<A>> {
        if self
            .pending
            .try_read()
            .map_err(|_| FileStorageError::WouldBlockLock)?
            .is_empty()
        {
            return Ok(());
        }

        let mut outer_guard = self.lock.write().map_err(|e| {
            FileStorageError::Io(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                e.to_string(),
            ))
        })?;
        let mut guard = outer_guard
            .try_write()
            .map_err(|_| FileStorageError::WouldBlockLock)?;

        // Partition: rewrites require a full-file atomic swap; appends do not.
        let has_rewrites = self
            .pending
            .try_read()
            .map_err(|_| FileStorageError::WouldBlockLock)?
            .iter()
            .any(|c| matches!(c, PendingChange::RewriteLine { .. }));

        if has_rewrites {
            // Build the complete new file from the in-memory entries,
            // applying any pending appends along the way so that the
            // serialized file is always consistent with `self.entries`.
            //
            // Collect rewrite targets so we can substitute updated bytes.
            let mut rewrites: std::collections::HashMap<usize, Vec<u8>> =
                std::collections::HashMap::new();
            let mut extra_appends: Vec<Vec<u8>> = Vec::new();

            for change in self
                .pending
                .try_write()
                .map_err(|_| FileStorageError::WouldBlockLock)?
                .drain(..)
            {
                match change {
                    PendingChange::RewriteLine { line_id, new_bytes } => {
                        rewrites.insert(line_id, new_bytes);
                    }
                    PendingChange::Append(bytes) => {
                        extra_appends.push(bytes);
                    }
                }
            }

            // Write tmp file.
            let tmp_path = self.path.with_extension("tmp");
            {
                let mut tmp = File::create(&tmp_path).map_err(FileStorageError::Io)?;

                let raw_tasks = self
                    .entries
                    .try_read()
                    .map_err(|_| FileStorageError::WouldBlockLock)?;
                if let Some(headers) = self.adapter.header(&raw_tasks) {
                    tmp.write_all(&headers).map_err(FileStorageError::Io)?;
                }
                for (job_id, entry) in raw_tasks.iter().enumerate() {
                    let bytes = if let Some(b) = rewrites.remove(&job_id) {
                        b
                    } else {
                        let line = A::from_entry(entry).map_err(FileStorageError::AdapterError)?;
                        self.adapter
                            .serialize(&line)
                            .map_err(FileStorageError::AdapterError)?
                    };
                    tmp.write_all(&bytes).map_err(FileStorageError::Io)?;
                }

                tmp.flush().map_err(FileStorageError::Io)?;
            }

            // Atomic rename over the live file.
            std::fs::rename(&tmp_path, &self.path).map_err(FileStorageError::Io)?;

            // Re-open the renamed file so `guard` / `lock` stays valid.
            // We re-open for read+write and replace the lock's inner file.
            // (fd_lock works on the file descriptor, so we update it.)
            drop(guard);
            drop(outer_guard);
            let new_file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&self.path)
                .map_err(FileStorageError::Io)?;
            self.lock = Arc::new(RwLock::new(FileLock::new(new_file)));
        } else {
            // Appends only — fast path: seek to end and write.
            for change in self
                .pending
                .try_write()
                .map_err(|_| FileStorageError::WouldBlockLock)?
                .drain(..)
            {
                if let PendingChange::Append(bytes) = change {
                    guard.seek(SeekFrom::End(0)).map_err(FileStorageError::Io)?;
                    guard.write_all(&bytes).map_err(FileStorageError::Io)?;
                }
            }
            guard.flush().map_err(FileStorageError::Io)?;
        }

        self.last_flush = std::time::Instant::now();
        Ok(())
    }

    /// Drive periodic syncing. Call this from your timer loop.
    pub fn tick(&mut self) -> Result<(), FileStorageError<A>> {
        if let SyncPolicy::Periodic(interval) = self.sync_policy.clone() {
            if self.last_flush.elapsed() >= interval {
                self.flush()?;
            }
        }
        Ok(())
    }

    /// Read every non-blank line from `file` and decode via the adapter.
    fn load_entries(adapter: &A, file: &File) -> Result<Vec<RawTask>, FileStorageError<A>> {
        let mut reader = BufReader::new(file);
        reader
            .seek(SeekFrom::Start(0))
            .map_err(FileStorageError::Io)?;

        let mut entries = Vec::new();
        let mut raw: Vec<u8> = Vec::new();

        loop {
            raw.clear();
            let n = reader
                .read_until(b'\n', &mut raw)
                .map_err(FileStorageError::Io)?;
            if n == 0 {
                break;
            }
            let trimmed = raw.trim_ascii();
            if trimmed.is_empty() {
                continue;
            }

            if adapter.is_header(trimmed) {
                continue;
            }
            let line = adapter
                .deserialize(trimmed)
                .map_err(FileStorageError::AdapterError)?;
            entries.push(
                adapter
                    .to_entry(line)
                    .map_err(FileStorageError::AdapterError)?,
            );
        }
        Ok(entries)
    }
}

impl<A: Adapter + Unpin, Args: Unpin> Stream for FileStorage<Args, A> {
    type Item = Result<(usize, RawTask), FileStorageError<A>>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let raw_tasks = this
            .entries
            .try_read()
            .map_err(|_| FileStorageError::WouldBlockLock)?;
        if this.read_cursor < raw_tasks.len() {
            let line_id = this.read_cursor;
            let job = raw_tasks[line_id].clone();
            this.read_cursor += 1;
            Poll::Ready(Some(Ok((line_id, job))))
        } else {
            Poll::Ready(None)
        }
    }
}

impl<Args, A> Backend for FileStorage<Args, A>
where
    Args: 'static + Send + Serialize + for<'de> Deserialize<'de> + Unpin,
    A: Adapter + Unpin + Clone,
{
    type Args = Args;
    type IdType = RandomId;
    type Error = FileStorageError<A>;
    type Connection = MetadataStore;
    type Stream = TaskStream<Task<Args, MetadataStore, RandomId>, Self::Error>;
    type Layer = AcknowledgeLayer<Self>;
    type Beat = BoxStream<'static, Result<(), Self::Error>>;

    fn heartbeat(&self, _: &WorkerContext) -> Self::Beat {
        stream::once(async { Ok(()) }).boxed()
    }
    fn middleware(&self) -> Self::Layer {
        AcknowledgeLayer::new(self.clone())
    }
    fn poll(self, _worker: &WorkerContext) -> Self::Stream {
        (self
            .map_ok(|(line_id, mut job)| {
                let args = Args::deserialize(&job.args).unwrap();
                job.ctx.insert("line_id", line_id.to_string()).unwrap();
                let mut task = TaskBuilder::new(args).with_metadata(job.ctx);

                if let Some(task_id) = job.task_id {
                    task = task.task_id(task_id);
                }
                Some(task.build())
            })
            .boxed()) as _
    }
}

impl<A, Args> BackendExt for FileStorage<Args, A>
where
    Args: 'static + Send + Serialize + for<'de> Deserialize<'de> + Unpin,
    A: Adapter + Unpin + Clone,
{
    type Codec = JsonCodec<Value>;
    type Compact = Value;

    type CompactStream =
        TaskStream<Task<Self::Compact, MetadataStore, RandomId>, FileStorageError<A>>;

    fn get_queue(&self) -> Queue {
        std::any::type_name::<Args>().into()
    }

    fn poll_compact(self, worker: &WorkerContext) -> Self::CompactStream {
        self.poll(worker)
            .map_ok(|c| {
                c.map(|t| {
                    t.into_builder()
                        .map(|args| serde_json::to_value(args).expect("to be encodable"))
                        .build()
                })
            })
            .boxed()
    }
}

/// A CSV adapter for serializing and deserializing tasks.
#[derive(Debug, Clone, Default)]
pub struct CsvAdapter {
    /// Column names, in BTreeMap (alphabetical) order.
    /// Populated on first serialize (write path) or first deserialize (read path).
    header: Arc<Mutex<Option<Vec<String>>>>,
}

/// A `FileStorage` using `CsvAdapter` for line encoding.
pub type CsvStorage<Args> = FileStorage<Args, CsvAdapter>;

impl Adapter for CsvAdapter {
    type Line = BTreeMap<String, String>;

    type Error = io::Error;

    fn serialize(&self, line: &Self::Line) -> Result<Vec<u8>, Self::Error> {
        let mut out = Vec::new();

        let mut header = self
            .header
            .lock()
            .map_err(|_| io::Error::other("header mutex poisoned"))?;

        if header.is_none() {
            let cols: Vec<String> = line.keys().cloned().collect();
            out.extend_from_slice(format!("{}\n", cols.join(",")).as_bytes());
            *header = Some(cols);
        }

        let row = line
            .values()
            .map(|v| v.as_str())
            .collect::<Vec<_>>()
            .join(",");

        out.extend_from_slice(format!("{row}\n").as_bytes());

        Ok(out)
    }

    fn deserialize(&self, raw: &[u8]) -> Result<Self::Line, Self::Error> {
        let row = std::str::from_utf8(raw)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
            .trim_end_matches('\n');

        let header = self
            .header
            .lock()
            .map_err(|_| io::Error::other("header mutex poisoned"))?;

        let cols = header.as_ref().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "CsvAdapter: deserialize called before header was parsed",
            )
        })?;

        let map = row
            .split(',')
            .zip(cols.iter())
            .map(|(value, key)| (key.clone(), value.to_owned()))
            .collect::<BTreeMap<_, _>>();

        Ok(map)
    }

    fn to_entry(&self, line: Self::Line) -> Result<RawTask, Self::Error> {
        let args = util::to_value(Some("args"), &line);

        let mut result = None;

        if line.contains_key("result") {
            result = Some(util::to_value(Some("result"), &line));
        }

        let idempotency_key = line.get("idempotency_key").cloned();

        let task_id = line.get("task_id").and_then(|s| FromStr::from_str(s).ok());

        let ctx = line
            .iter()
            .filter(|(k, _)| k.starts_with("ctx."))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect::<HashMap<String, String>>();

        let ctx = MetadataStore::from_map(ctx);

        Ok(RawTask {
            task_id,
            args,
            ctx,
            result,
            idempotency_key,
        })
    }

    fn from_entry(entry: &RawTask) -> Result<Self::Line, Self::Error> {
        let mut line = BTreeMap::new();

        line.insert(
            "task_id".to_owned(),
            entry
                .task_id
                .as_ref()
                .map(|t| t.to_string())
                .unwrap_or_default(),
        );

        let args = util::from_value(Some("args"), &entry.args);

        line.extend(args);

        let result = entry
            .result
            .as_ref()
            .map(|r| util::from_value(Some("result"), r))
            .unwrap_or_else(|| util::from_value(Some("result"), &serde_json::Value::Null));

        line.extend(result);

        if let Some(idempotency_key) = &entry.idempotency_key {
            line.insert("idempotency_key".to_owned(), idempotency_key.clone());
        }

        for (k, value) in entry.ctx.iter() {
            line.insert(format!("ctx.{k}"), value.clone());
        }

        Ok(line)
    }

    fn is_header(&self, raw: &[u8]) -> bool {
        let Ok(mut header) = self.header.lock() else {
            return false;
        };

        if header.is_none() {
            if let Ok(row) = std::str::from_utf8(raw) {
                let cols: Vec<String> = row
                    .trim_end_matches('\n')
                    .split(',')
                    .map(|s| s.to_owned())
                    .collect();

                *header = Some(cols);
            }

            return true; // this line is the header, skip it
        }

        false
    }

    fn header(&self, entries: &Vec<RawTask>) -> Option<Vec<u8>> {
        let first = entries.first()?;

        let line = Self::from_entry(first).ok()?;

        let cols: Vec<String> = line.keys().cloned().collect();
        Some(format!("{}\n", cols.join(",")).as_bytes().to_vec())
    }
}

impl<Args, Res, A> Acknowledge<Res, MetadataStore, RandomId> for FileStorage<Args, A>
where
    Args: Send + 'static + Debug,
    Res: Serialize,
    A: Adapter + Clone,
{
    type Error = FileStorageError<A>;

    type Future = Ready<Result<(), Self::Error>>;

    fn ack(
        &mut self,
        res: &Result<Res, BoxDynError>,
        ctx: &ExecutionContext<MetadataStore, RandomId>,
    ) -> Self::Future {
        let res = |this: &mut Self| {
            let val = serde_json::to_value(res.as_ref().map_err(|e| e.to_string()))?;
            let line_id = ctx
                .metadata
                .get("line_id")
                .unwrap()
                .parse()
                .map_err(|e: ParseIntError| FileStorageError::Parse(e.to_string()))?;
            this.set_result(line_id, val)?;
            Ok(())
        };

        ready(res(self))
    }
}

impl<Res: 'static + DeserializeOwned + Send, Args: 'static + Sync, A> WaitForCompletion<Res>
    for FileStorage<Args, A>
where
    Args: Send + DeserializeOwned + 'static + Unpin + Serialize,
    A: Adapter + Unpin + Sync + Clone,
{
    type ResultStream = BoxStream<'static, Result<TaskResult<Res, RandomId>, FileStorageError<A>>>;
    fn wait_for(
        &self,
        task_ids: impl IntoIterator<Item = TaskId<Self::IdType>>,
    ) -> Self::ResultStream {
        use futures_util::StreamExt;
        use std::{collections::HashSet, time::Duration};

        let task_ids: HashSet<_> = task_ids.into_iter().collect();
        struct PollState<Res, T, A: Adapter> {
            vault: FileStorage<T, A>,
            pending_tasks: HashSet<TaskId<RandomId>>,
            poll_interval: Duration,
            _phantom: std::marker::PhantomData<Res>,
        }
        let state = PollState {
            vault: self.clone(),
            pending_tasks: task_ids,
            poll_interval: Duration::from_millis(100),
            _phantom: std::marker::PhantomData,
        };
        futures_util::stream::unfold(state, |mut state: PollState<Res, Args, A>| {
            async move {
                if state.pending_tasks.is_empty() {
                    return None;
                }

                loop {
                    // Check for completed tasks
                    let vault = &state.vault;

                    let completed_task = {
                        let vault = vault.entries.try_read().ok()?;
                        vault.iter().find_map(|value| {
                            let task_id = value.task_id.clone()?;
                            if state.pending_tasks.contains(&task_id) {
                                Some((task_id, value.result.clone()?))
                            } else {
                                None
                            }
                        })
                    };

                    if let Some((task_id, result)) = completed_task {
                        state.pending_tasks.remove(&task_id);
                        let result: Result<Res, String> = serde_json::from_value(result).unwrap();
                        return Some((
                            Ok(TaskResult {
                                task_id,
                                status: Status::Done,
                                result,
                            }),
                            state,
                        ));
                    }

                    // No completed tasks, wait and try again
                    apalis_core::timer::sleep(state.poll_interval).await;
                }
            }
        })
        .boxed()
    }

    async fn check_status(
        &self,
        task_ids: impl IntoIterator<Item = TaskId<Self::IdType>> + Send,
    ) -> Result<Vec<TaskResult<Res, RandomId>>, Self::Error> {
        use apalis_core::task::status::Status;
        use std::collections::HashSet;
        let task_ids: HashSet<_> = task_ids.into_iter().collect();
        let mut results = Vec::new();
        for task_id in task_ids {
            if let Some(value) = self
                .entries
                .try_read()
                .unwrap()
                .iter()
                .find(|s| s.task_id.as_ref().unwrap() == &task_id)
            {
                if value.result.is_none() {
                    results.push(TaskResult {
                        task_id: task_id.clone(),
                        status: Status::Pending,
                        result: Err("Task not completed yet".to_owned()),
                    });
                    continue;
                }
                let result = match serde_json::from_value::<Result<Res, String>>(
                    value.result.clone().unwrap(),
                ) {
                    Ok(result) => TaskResult {
                        task_id: task_id.clone(),
                        status: Status::Done,
                        result,
                    },
                    Err(e) => TaskResult {
                        task_id: task_id.clone(),
                        status: Status::Failed,
                        result: Err(format!("Deserialization error: {e}")),
                    },
                };
                results.push(result);
            }
        }
        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    use apalis_core::{
        backend::TaskSink,
        error::BoxDynError,
        worker::{
            builder::WorkerBuilder, context::WorkerContext, ext::event_listener::EventListenerExt,
        },
    };

    const ITEMS: u32 = 100;

    #[tokio::test]
    async fn json_worker() {
        let mut json_store = JsonStorage::new_temp().unwrap();
        for i in 0..ITEMS {
            json_store.push(i).await.unwrap();
        }

        async fn task(task: u32, ctx: WorkerContext) -> Result<(), BoxDynError> {
            tokio::time::sleep(Duration::from_secs(1)).await;
            if task == ITEMS - 1 {
                ctx.stop().unwrap();
                return Err("Worker stopped!")?;
            }
            Ok(())
        }

        let worker = WorkerBuilder::new("rango-tango-json")
            .backend(json_store)
            .on_event(|ctx, ev| {
                println!("On Event = {ev:?} from = {}", ctx.name());
            })
            .build(task);
        worker.run().await.unwrap();
    }

    #[tokio::test]
    async fn csv_worker() {
        #[derive(Debug, Serialize, Deserialize, Default)]
        struct Email {
            to: String,
            subject: String,
            index: u32,
        }

        let mut csv_store = CsvStorage::new_temp().unwrap();
        for i in 0..ITEMS {
            csv_store
                .push(Email {
                    subject: "Test".to_string(),
                    to: "test".to_string(),
                    index: i,
                })
                .await
                .unwrap();
        }

        async fn task(task: Email, ctx: WorkerContext) -> Result<(), BoxDynError> {
            tokio::time::sleep(Duration::from_secs(1)).await;
            if task.index == ITEMS - 1 {
                ctx.stop().unwrap();
                return Err("Worker stopped!")?;
            }
            Ok(())
        }

        let worker = WorkerBuilder::new("rango-tango-csv")
            .backend(csv_store)
            .on_event(|ctx, ev| {
                println!("On Event = {ev:?} from = {}", ctx.name());
            })
            .build(task);
        worker.run().await.unwrap();
    }
}
