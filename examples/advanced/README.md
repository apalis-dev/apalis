# advanced-example

This example simulates an email-sending service that validates addresses, respects an API rate limit, caches validation results, and gracefully handles panics — all while remaining fully observable via `tracing`.

## What this example demonstrates

- **In-memory job queue** — jobs are pushed onto an `apalis::layers::MemoryStorage` and consumed by a worker pool.
- **Custom middleware (`Layer`)** — `ClientLayer` / `ClientService` inject a shared `EmailClient` into every job's context and can pause the worker (`Poll::Pending`) when the simulated API budget is exhausted.
- **Panic isolation** — `CatchPanicLayer` converts panics inside a job into a typed `Error::Panic`, aborting just that job instead of crashing the worker.
- **Shared state via `Data<T>`** — a `ValidEmailCache` and `EmailClient` are registered once and injected into every job handler.
- **Fire-and-forget follow-up work** — after a job completes, `TaskContext::executed()` is awaited inside a detached `tokio::spawn`, so cache maintenance happens _after_ the job is marked done, without blocking or delaying it.
- **Concurrency tuned to available CPUs** — `.concurrency(cpu_n)` uses `std::thread::available_parallelism()`.
- **Structured logging** — `tracing` + `tracing_subscriber` with `EnvFilter`, plus `.enable_tracing()` to automatically instrument each job with a span.
- **Graceful shutdown** — the worker runs until `Ctrl+C` is received via `.run_until(tokio::signal::ctrl_c())`.

## How it works

1. **Job production** — `produce_jobs` pushes 10 `Email` jobs into a `MemoryStorage` to simulate work arriving from an upstream source (an HTTP endpoint, message queue, etc.).
2. **Worker construction** — `WorkerBuilder` wires together:
   - `.parallelize(tokio::spawn)` to run each job's future as its own task,
   - `.enable_tracing()` for automatic span instrumentation,
   - `.concurrency(cpu_n)` to process several emails at once,
   - `CatchPanicLayer` to contain panics,
   - `ClientLayer` to inject the `EmailClient` and enforce backpressure once API calls run out,
   - `.data(ValidEmailCache::new())` to share a validation cache across jobs.
3. **Job handling (`send_email`)**:
   - If the recipient is already cached as **valid**, the job calls `send_unchecked` (cheap, 1 API call) and skips validation.
   - If cached as **invalid**, the job fails fast with `Error::InvalidEmail`.
   - Otherwise, it calls `send` (validates + sends, 2 API calls), returns success immediately, then spawns a detached task that waits for the job to be marked `executed()` before refreshing the cache — decoupling cache upkeep from job latency.
4. **Rate limiting** — `EmailClient` starts with a budget of exactly 10 API calls. Since `send` costs 2 calls and `send_unchecked` costs 1, the worker will run out partway through, causing `ClientService::poll_ready` to return `Poll::Pending` (logged via `warn!`) until — in this simplified example — it never recovers, illustrating how a layer can throttle a worker.

## Running it

```bash
RUST_LOG=debug cargo run
```

You'll see:

- Each email job being sent (`send` or `send_unchecked`).
- Debug logs when the validation cache is refreshed asynchronously after job completion.
- A warning once the simulated API budget is exhausted and the worker stops pulling new jobs.
- Graceful shutdown on `Ctrl+C`.

## Notable patterns worth reusing

- **Decoupling "job done" from "cleanup done"** via `ctx.executed().await` inside a spawned task — useful whenever post-processing shouldn't hold up job completion or worker throughput.
- **Backpressure from external resource limits** — implementing `poll_ready` in a custom `Service` to pause consumption when a downstream dependency (here, a simulated rate limiter) is exhausted, rather than failing every job.
- **Typed, `thiserror`-based error enums** that distinguish panics, validation failures, and rate-limit errors for clearer job-failure reporting.
