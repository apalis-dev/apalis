use crate::tui::{App, Focus};
use apalis::prelude::{TaskContext, WorkerContext};
use ratatui::{
    Frame,
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Gauge, List, ListItem, ListState, Paragraph},
};

pub(crate) fn draw(f: &mut Frame<'_>, app: &App) {
    let root = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // header
            Constraint::Min(0),    // body
            Constraint::Length(3), // footer / help / status
        ])
        .split(f.area());

    draw_header(f, app, root[0]);

    let body = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(35), Constraint::Percentage(65)])
        .split(root[1]);

    draw_worker_list(f, app, body[0]);
    draw_task_panel(f, app, body[1]);

    draw_footer(f, app, root[2]);
}

fn draw_header(f: &mut Frame<'_>, app: &App, area: Rect) {
    let age = app.last_refresh.elapsed().as_secs();
    let text = Line::from(vec![
        Span::styled(" Monitor ", Style::default().add_modifier(Modifier::BOLD)),
        Span::raw(format!(
            "— {} workers — last refresh {age}s ago (auto every {}s)",
            app.workers().len(),
            app.refresh_interval.as_secs()
        )),
    ]);
    let block = Block::default().borders(Borders::ALL).title("monitor-tui");
    f.render_widget(Paragraph::new(text).block(block), area);
}

fn draw_worker_list(f: &mut Frame<'_>, app: &App, area: Rect) {
    let items: Vec<ListItem<'_>> = app.workers().iter().map(|w| worker_list_item(w)).collect();

    let mut state = ListState::default();
    if !app.workers().is_empty() {
        state.select(Some(app.selected_worker));
    }

    let border_style = if app.focus == Focus::Workers {
        Style::default().fg(Color::Cyan)
    } else {
        Style::default()
    };

    let list = List::new(items)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title("Workers (↑/↓, Tab to switch pane)")
                .border_style(border_style),
        )
        .highlight_style(
            Style::default()
                .bg(Color::DarkGray)
                .add_modifier(Modifier::BOLD),
        )
        .highlight_symbol("➤ ");

    f.render_stateful_widget(list, area, &mut state);
}

fn worker_list_item(w: &WorkerContext) -> ListItem<'static> {
    let status_color = if w.is_shutting_down() {
        Color::Red
    } else if w.is_ready() {
        Color::Green
    } else {
        Color::Yellow
    };
    let status_label = w.state();

    let active_tasks = w.tasks().iter().filter(|t| !t.is_executed()).count();

    let line = Line::from(vec![
        Span::styled("● ".to_owned(), Style::default().fg(status_color)),
        Span::styled(
            format!("{:<16}", w.name()),
            Style::default().add_modifier(Modifier::BOLD),
        ),
        Span::raw(format!(
            "{status_label:<9} tasks:{active_tasks:<3} restarts:{}",
            w.restarts()
        )),
        Span::raw(format!(" {}(s)", w.elapsed().as_secs())),
    ]);
    ListItem::new(line)
}

fn draw_task_panel(f: &mut Frame<'_>, app: &App, area: Rect) {
    let Some(worker) = app.current_worker() else {
        let block = Block::default().borders(Borders::ALL).title("Tasks");
        f.render_widget(Paragraph::new("No worker selected.").block(block), area);
        return;
    };

    if worker.tasks().is_empty() {
        let block = Block::default()
            .borders(Borders::ALL)
            .title(format!("Tasks — {}", worker.name()));
        f.render_widget(
            Paragraph::new("No active tasks for this worker.").block(block),
            area,
        );
        return;
    }

    // Split the task panel into one row per task (min height 3 each) so
    // every task gets its own gauge + detail line.
    let constraints: Vec<Constraint> = worker
        .tasks()
        .iter()
        .map(|_| Constraint::Length(2))
        .collect();

    let outer_block = Block::default()
        .borders(Borders::ALL)
        .title(format!(
            "Tasks — {} (↑/↓, Tab to switch pane)",
            worker.name()
        ))
        .border_style(if app.focus == Focus::Tasks {
            Style::default().fg(Color::Cyan)
        } else {
            Style::default()
        });
    let inner = outer_block.inner(area);
    f.render_widget(outer_block, area);

    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints(constraints)
        .split(inner);

    for (i, (task, row)) in worker.tasks().iter().zip(rows.iter()).enumerate() {
        draw_task_gauge(
            f,
            task,
            *row,
            i == app.selected_task && app.focus == Focus::Tasks,
        );
    }
}

fn draw_task_gauge(f: &mut Frame<'_>, task: &TaskContext, area: Rect, selected: bool) {
    // These tasks don't expose a known total, so treat "progress" as
    // activity: 100% + green when completed, a pulsing-style ratio based on
    // elapsed time capped at 95% while running, red/greyed if cancelled.
    let (ratio, color, label) = if task.is_completed() {
        (1.0, Color::Green, "done".to_owned())
    } else if task.is_cancelled() {
        (1.0, Color::Red, "cancelled".to_owned())
    } else {
        // Fake a soft progress indicator from elapsed time so the bar isn't
        // static; replace with real progress if TaskContext exposes it.
        let secs = task.elapsed().as_secs_f64();
        let ratio = (1.0 - (-secs / 10.0).exp()).min(0.95);
        (ratio, Color::Blue, format!("{secs:.0}s"))
    };

    let title = format!(
        "{}{}  [{} sub-tasks]",
        if selected { "➤ " } else { "  " },
        task.task_id(),
        task.len()
    );

    let gauge = Gauge::default()
        .block(Block::default().title(title))
        .gauge_style(Style::default().fg(color))
        .ratio(ratio)
        .label(label);

    f.render_widget(gauge, area);
}

fn draw_footer(f: &mut Frame<'_>, app: &App, area: Rect) {
    let text = if let Some(status) = &app.status {
        Line::from(Span::styled(
            status.text.clone(),
            Style::default().fg(Color::Yellow),
        ))
    } else {
        Line::from(
            "r: refresh  x: stop selected  X: stop all o: resume p: pause  q: quit  (y/n to confirm)",
        )
    };

    let block = Block::default().borders(Borders::ALL);
    f.render_widget(
        Paragraph::new(text).block(block).alignment(Alignment::Left),
        area,
    );
}
