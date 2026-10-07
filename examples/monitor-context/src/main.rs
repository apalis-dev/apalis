#![allow(missing_docs)]
use std::io::{self, Write};
use std::time::Duration;

use anyhow::Result;
use apalis::layers::tracing::*;
use apalis::prelude::*;
use apalis_file_storage::JsonStorage;
use email_service::Email;
use rand::RngExt;
use tokio::time::sleep;

async fn email_service(_: Email, task: TaskContext) -> Result<(), BoxDynError> {
    let range = rand::rng().random_range(1000..=20000);
    tokio::spawn(task.run_until_executed(async move {
        sleep(Duration::from_millis(12000)).await;
    }));
    if range > 7000 {
        task.cancel().unwrap();
        return Ok(());
    }
    sleep(Duration::from_millis(range)).await;
    Err(BoxDynError::from("Failed"))
}

async fn produce_task(storage: &mut JsonStorage<Email>) -> Result<()> {
    loop {
        let _ = storage
            .push(Email {
                to: "test@example".to_owned(),
                text: "Test background job from apalis".to_owned(),
                subject: "Welcome Sentry Email".to_owned(),
            })
            .await;
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn produce_task_with_ctx(storage: &mut JsonStorage<Email>) -> Result<()> {
    loop {
        let email = Email {
            to: "test@example".to_owned(),
            text: "Test background job from apalis".to_owned(),
            subject: "Welcome Sentry Email".to_owned(),
        };
        let context = TracingContext::from(OtelTraceContext::current());
        let task = TaskBuilder::new(email).metadata(&context).build();
        let _ = storage.push_task(task).await;
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let avocado_backend = JsonStorage::new_temp()?;
    let mut av = avocado_backend.clone();
    tokio::spawn(async move {
        produce_task(&mut av).await.unwrap();
    });

    let pear_backend = JsonStorage::new_temp()?;
    let mut pb = pear_backend.clone();

    tokio::spawn(async move {
        produce_task_with_ctx(&mut pb).await.unwrap();
    });

    let monitor = Monitor::new()
        .register(move |_restarts| {
            WorkerBuilder::new("tasty-avocado")
                .backend(avocado_backend.clone())
                .concurrency(30)
                .enable_tracing()
                .build(email_service)
        })
        .register(move |_restarts| {
            WorkerBuilder::new("tasty-pear")
                .backend(pear_backend.clone())
                .concurrency(20)
                .layer(TraceLayer::new().make_span_with(ContextualTaskSpan::new()))
                .build(email_service)
        })
        .should_restart(|wrk, err| {
            let runs = wrk.restarts();
            if wrk.name() == "tasty-pear"
                && err.to_string().contains("Recoverable Error")
                && runs < 5
            {
                return false;
            }
            true
        })
        .shutdown_timeout(Duration::from_secs(5));
    let ctx = monitor.context();
    let _terminal = TerminalGuard::enter()?;
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            basic_tui(ctx.workers()).unwrap();
        }
    });
    monitor.run_with_signal(tokio::signal::ctrl_c()).await?;

    Ok(())
}

fn format_duration(duration: Duration) -> String {
    let secs = duration.as_secs();

    let hours = secs / 3600;
    let minutes = (secs % 3600) / 60;
    let seconds = secs % 60;

    if hours > 0 {
        format!("{hours}h {minutes:02}m {seconds:02}s")
    } else if minutes > 0 {
        format!("{minutes}m {seconds:02}s")
    } else {
        format!("{seconds}s")
    }
}

pub fn basic_tui(workers: &[WorkerContext]) -> io::Result<()> {
    let mut out = io::stdout();

    // Move to top-left and clear the entire terminal.
    write!(out, "\x1b[H\x1b[2J")?;

    writeln!(
        out,
        "╭─────────────────────────────────────────────────────────────────────────────╮"
    )?;
    writeln!(
        out,
        "│ Apalis Workers                                                               │"
    )?;
    writeln!(
        out,
        "├────┬────────────────────┬────────┬────────┬──────────┬──────────────────────┤"
    )?;
    writeln!(
        out,
        "│ #  │ Running            │ Tasks  │ Ready  │ Restarts │ Elapsed              │"
    )?;
    writeln!(
        out,
        "├────┼────────────────────┼────────┼────────┼──────────┼──────────────────────┤"
    )?;

    for (i, worker) in workers.iter().enumerate() {
        writeln!(
            out,
            "│ {:<2} │ {:<18} │ {:>6} │ {:<6} │ {:>8} │ {:<20} │",
            i,
            worker.is_running(),
            worker.task_count(),
            if worker.is_ready() { "yes" } else { "no" },
            worker.restarts(),
            format_duration(worker.elapsed()),
        )?;
    }

    writeln!(
        out,
        "╰────┴────────────────────┴────────┴────────┴──────────┴──────────────────────╯"
    )?;

    writeln!(out)?;
    writeln!(out, "Services:")?;

    for (i, worker) in workers.iter().enumerate() {
        writeln!(out, "  [{i}] {}", worker.name())?;
    }

    out.flush()
}

struct TerminalGuard;

impl TerminalGuard {
    fn enter() -> io::Result<Self> {
        let mut out = io::stdout();

        // Alternate screen.
        write!(out, "\x1b[?1049h")?;

        // Hide cursor.
        write!(out, "\x1b[?25l")?;

        // Clear screen and position cursor.
        write!(out, "\x1b[2J\x1b[H")?;

        out.flush()?;

        Ok(Self)
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let mut out = io::stdout();

        // Show cursor.
        let _ = write!(out, "\x1b[?25h");

        // Leave alternate screen.
        let _ = write!(out, "\x1b[?1049l");

        let _ = out.flush();
    }
}
