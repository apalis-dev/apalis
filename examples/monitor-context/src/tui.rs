use std::time::{Duration, Instant};

use apalis::prelude::{BoxDynError, MonitorContext, TaskContext, WorkerContext};
use crossterm::event::{self, Event, KeyCode, KeyEventKind};
use ratatui::DefaultTerminal;

use crate::view;

/// Which pane currently has keyboard focus.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub(crate) enum Focus {
    Workers,
    Tasks,
}

/// A transient status/error line shown at the bottom of the UI.
#[derive(Debug, Clone)]
pub(crate) struct StatusMessage {
    pub text: String,
    pub shown_at: Instant,
}

pub(crate) struct App {
    pub monitor: MonitorContext,
    pub focus: Focus,
    pub selected_worker: usize,
    pub selected_task: usize,
    pub last_refresh: Instant,
    pub refresh_interval: Duration,
    pub status: Option<StatusMessage>,
    pub should_quit: bool,
    /// Confirmation pending for a destructive action, so a single wrong
    /// keypress can't kill a worker or task by accident.
    pub pending_confirm: Option<PendingAction>,
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub(crate) enum PendingAction {
    StopWorker { name: String },
    Pause { name: String },
    Resume { name: String },
    CancelTask { name: String, task_id: String },
    StopAll,
}

impl App {
    pub(crate) fn new(monitor: MonitorContext, refresh_interval: Duration) -> Self {
        Self {
            monitor,
            focus: Focus::Workers,
            selected_worker: 0,
            selected_task: 0,
            last_refresh: Instant::now(),
            refresh_interval,
            status: None,
            should_quit: false,
            pending_confirm: None,
        }
    }

    pub(crate) fn refresh(&mut self) {
        self.last_refresh = Instant::now();
        let workers = self.monitor.workers();

        // Keep selection in bounds if workers/tasks disappeared.
        if self.selected_worker >= workers.len() {
            self.selected_worker = workers.len().saturating_sub(1);
        }
        if let Some(w) = workers.get(self.selected_worker) {
            if self.selected_task >= w.task_count() {
                self.selected_task = w.task_count().saturating_sub(1);
            }
        }
    }

    pub(crate) fn due_for_refresh(&self) -> bool {
        self.last_refresh.elapsed() >= self.refresh_interval
    }

    pub(crate) fn set_status(&mut self, text: impl Into<String>) {
        self.status = Some(StatusMessage {
            text: text.into(),
            shown_at: Instant::now(),
        });
    }

    /// Status messages auto-clear after this long so the footer doesn't get
    /// stuck showing a stale confirmation.
    pub(crate) fn tick_status(&mut self) {
        if let Some(s) = &self.status {
            if s.shown_at.elapsed() > Duration::from_secs(4) {
                self.status = None;
            }
        }
    }

    pub(crate) fn current_worker(&self) -> Option<&WorkerContext> {
        self.monitor.workers().get(self.selected_worker)
    }

    pub(crate) fn current_task(&self) -> Option<TaskContext> {
        self.current_worker().and_then(|w| {
            let tasks = w.tasks();
            tasks.into_iter().nth(self.selected_task)
        })
    }

    pub(crate) fn move_selection(&mut self, delta: isize) {
        match self.focus {
            Focus::Workers => {
                let len = self.monitor.workers().len();
                if len == 0 {
                    return;
                }
                self.selected_worker = wrap_index(self.selected_worker, delta, len);
                self.selected_task = 0;
            }
            Focus::Tasks => {
                let len = self.current_worker().map_or(0, |w| w.task_count());
                if len == 0 {
                    return;
                }
                self.selected_task = wrap_index(self.selected_task, delta, len);
            }
        }
    }

    pub(crate) fn toggle_focus(&mut self) {
        self.focus = match self.focus {
            Focus::Workers => Focus::Tasks,
            Focus::Tasks => Focus::Workers,
        };
    }

    pub(crate) fn request_pause_selected(&mut self) {
        if let Some(w) = self.current_worker() {
            if !w.is_running() {
                return;
            }
            let name = w.name().to_owned();
            self.pending_confirm = Some(PendingAction::Pause { name: name.clone() });
            self.set_status(format!(
                "Pause worker '{name}'? Press 'y' to confirm, any other key to cancel."
            ));
        }
    }

    pub(crate) fn request_resume_selected(&mut self) {
        if let Some(w) = self.current_worker() {
            if !w.is_paused() {
                return;
            }
            let name = w.name().to_owned();
            self.pending_confirm = Some(PendingAction::Resume { name: name.clone() });
            self.set_status(format!(
                "Resume worker '{name}'? Press 'y' to confirm, any other key to cancel."
            ));
        }
    }

    /// Called on the first keypress of a destructive action; arms a
    /// confirmation instead of acting immediately.
    pub(crate) fn request_stop_selected(&mut self) {
        match self.focus {
            Focus::Workers => {
                if let Some(w) = self.current_worker() {
                    let name = w.name().to_owned();
                    self.pending_confirm = Some(PendingAction::StopWorker { name: name.clone() });
                    self.set_status(format!(
                        "Stop worker '{name}'? Press 'y' to confirm, any other key to cancel."
                    ));
                }
            }
            Focus::Tasks => {
                if let (Some(w), Some(t)) = (self.current_worker(), self.current_task()) {
                    let task_id = t.task_id().to_owned();
                    let name = w.name().to_owned();
                    self.pending_confirm = Some(PendingAction::CancelTask {
                        name,
                        task_id: task_id.clone(),
                    });

                    self.set_status(format!(
                        "Cancel task '{task_id}'? Press 'y' to confirm, any other key to cancel."
                    ));
                }
            }
        }
    }

    pub(crate) fn request_stop_all(&mut self) {
        self.pending_confirm = Some(PendingAction::StopAll);
        self.set_status("Stop ALL workers? Press 'y' to confirm, any other key to cancel.");
    }

    /// Resolve a pending confirmation. `confirmed` should be true only on 'y'.
    pub(crate) fn resolve_confirmation(&mut self, confirmed: bool) {
        let Some(action) = self.pending_confirm.take() else {
            return;
        };
        if !confirmed {
            self.set_status("Cancelled.");
            return;
        }
        match action {
            PendingAction::StopWorker { name } => {
                self.monitor.stop_worker(&name).unwrap();
                self.set_status(format!("Stop requested for worker '{name}'."));
            }
            PendingAction::CancelTask { name, task_id } => {
                if self.monitor.cancel_task(&name, &task_id).is_ok() {
                    self.set_status(format!("Cancel requested for task '{task_id}'."));
                };
            }
            PendingAction::StopAll => {
                self.monitor.shutdown().unwrap();
                self.set_status("Shutdown requested for all workers.");
            }
            PendingAction::Pause { name } => {
                self.monitor.pause_worker(&name).unwrap();
                self.set_status(format!("Pause requested for worker '{name}'."));
            }

            PendingAction::Resume { name } => {
                self.monitor.resume_worker(&name).unwrap();
                self.set_status(format!("Resume requested for worker '{name}'."));
            }
        }
        self.refresh();
    }

    pub(crate) fn workers(&self) -> &Vec<WorkerContext> {
        self.monitor.workers()
    }
}

fn wrap_index(current: usize, delta: isize, len: usize) -> usize {
    let len = len as isize;
    let mut next = current as isize + delta;
    next = ((next % len) + len) % len;
    next as usize
}

fn run(mut terminal: DefaultTerminal, monitor: MonitorContext) -> Result<(), BoxDynError> {
    let mut app = App::new(monitor, Duration::from_secs(2));

    loop {
        terminal.draw(|f| view::draw(f, &app))?;

        app.tick_status();

        // Poll with a short timeout so the auto-refresh tick keeps firing
        // even when the user isn't pressing anything.
        if event::poll(Duration::from_millis(200))? {
            if let Event::Key(key) = event::read()? {
                if key.kind == KeyEventKind::Press {
                    handle_key(&mut app, key.code);
                }
            }
        }

        if app.due_for_refresh() {
            app.refresh();
        }

        if app.should_quit {
            app.monitor.shutdown().unwrap();
            if app.workers().iter().find(|w| !w.is_terminated()).is_some() {
                app.refresh();
            } else {
                break;
            }
        }
    }

    Ok(())
}

fn handle_key(app: &mut App, code: KeyCode) {
    // If a destructive action is pending, the next keypress resolves it
    // regardless of what else is bound, so a stray key can't slip through.
    if app.pending_confirm.is_some() {
        match code {
            KeyCode::Char('y' | 'Y') => app.resolve_confirmation(true),
            _ => app.resolve_confirmation(false),
        }
        return;
    }

    match code {
        KeyCode::Char('q') | KeyCode::Esc => app.should_quit = true,
        KeyCode::Char('r') => {
            app.refresh();
            app.set_status("Refreshed.");
        }
        KeyCode::Tab => app.toggle_focus(),
        KeyCode::Up | KeyCode::Char('k') => app.move_selection(-1),
        KeyCode::Down | KeyCode::Char('j') => app.move_selection(1),
        KeyCode::Char('x') => app.request_stop_selected(),
        KeyCode::Char('X') => app.request_stop_all(),
        KeyCode::Char('p') => app.request_pause_selected(),
        KeyCode::Char('o') => app.request_resume_selected(),
        _ => {}
    }
}

pub(crate) fn run_tui(context: MonitorContext) -> Result<(), BoxDynError> {
    color_eyre::install()?;
    let terminal = ratatui::init();
    let result = run(terminal, context);
    ratatui::restore();
    result
}
