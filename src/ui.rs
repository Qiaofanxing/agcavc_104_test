use std::{io, sync::Arc, time::Duration};

use chrono::{DateTime, Local, Utc};
use crossterm::{
    event::{
        self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEvent, KeyEventKind,
        KeyModifiers, MouseButton, MouseEvent, MouseEventKind,
    },
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{
    Terminal,
    backend::CrosstermBackend,
    prelude::{
        Alignment, Color, Constraint, Direction as LayoutDirection, Frame, Layout, Line, Modifier,
        Rect, Span, Style,
    },
    widgets::{Block, Borders, Cell, Clear, List, ListItem, Paragraph, Row, Table, Tabs, Wrap},
};
use tokio::sync::{mpsc, watch};

use crate::{
    config::{DispatchPointConfig, ToolConfig},
    model::{
        AppSnapshot, ControlPhase, Direction, DispatchAction, DispatchPointKind, DispatchRequest,
        FaultPolicy, LogCategory, LogEntry, PointView, RuntimeCommand, Severity, Side, UploadKind,
    },
};

const TAB_TITLES: [&str; 7] = [
    "1 状态",
    "2 采集日志",
    "3 调度日志",
    "4 采集指令",
    "5 调度指令",
    "6 采集值",
    "7 调度值",
];

pub async fn run(
    config: &ToolConfig,
    commands: mpsc::Sender<RuntimeCommand>,
    snapshots: watch::Receiver<Arc<AppSnapshot>>,
) -> Result<(), String> {
    let _guard = TerminalGuard::enter()?;
    let backend = CrosstermBackend::new(io::stdout());
    let mut terminal = Terminal::new(backend).map_err(|error| format!("创建终端失败: {error}"))?;
    terminal
        .clear()
        .map_err(|error| format!("清理终端失败: {error}"))?;

    let mut app = UiState::new(config);
    loop {
        let snapshot = snapshots.borrow().clone();
        let mut screen = Rect::default();
        terminal
            .draw(|frame| {
                screen = frame.area();
                render(frame, &app, &snapshot);
            })
            .map_err(|error| format!("绘制 TUI 失败: {error}"))?;

        while event::poll(Duration::ZERO).map_err(|error| format!("轮询终端输入失败: {error}"))?
        {
            let event = event::read().map_err(|error| format!("读取终端输入失败: {error}"))?;
            match event {
                Event::Key(key)
                    if matches!(key.kind, KeyEventKind::Press | KeyEventKind::Repeat) =>
                {
                    if handle_key(&mut app, key, &commands, &snapshot) {
                        let _ = commands.try_send(RuntimeCommand::Quit);
                        return Ok(());
                    }
                }
                Event::Mouse(mouse) => {
                    handle_mouse(&mut app, mouse, &commands, &snapshot, screen);
                }
                _ => {}
            }
        }
        tokio::time::sleep(config.ui.tick).await;
    }
}

struct TerminalGuard;

impl TerminalGuard {
    fn enter() -> Result<Self, String> {
        enable_raw_mode().map_err(|error| format!("启用 raw mode 失败: {error}"))?;
        if let Err(error) = execute!(io::stdout(), EnterAlternateScreen, EnableMouseCapture) {
            let _ = disable_raw_mode();
            return Err(format!("进入备用屏幕失败: {error}"));
        }
        Ok(Self)
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let _ = execute!(io::stdout(), LeaveAlternateScreen, DisableMouseCapture);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LogFilter {
    All,
    Protocol,
    Control,
    Interrogation,
    Connection,
}

impl LogFilter {
    const fn next(self) -> Self {
        match self {
            Self::All => Self::Protocol,
            Self::Protocol => Self::Control,
            Self::Control => Self::Interrogation,
            Self::Interrogation => Self::Connection,
            Self::Connection => Self::All,
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::All => "全部",
            Self::Protocol => "协议",
            Self::Control => "控制",
            Self::Interrogation => "召唤",
            Self::Connection => "连接",
        }
    }

    fn matches(self, category: LogCategory) -> bool {
        if category == LogCategory::CommandDivider {
            return true;
        }
        match self {
            Self::All => true,
            Self::Protocol => matches!(category, LogCategory::Protocol | LogCategory::KeepAlive),
            Self::Control => category == LogCategory::Control,
            Self::Interrogation => category == LogCategory::Interrogation,
            Self::Connection => category == LogCategory::Connection,
        }
    }
}

struct UiState {
    tab: usize,
    collect_selection: usize,
    dispatch_selection: usize,
    log_offset: usize,
    collect_value_offset: usize,
    dispatch_general_offset: usize,
    dispatch_energy_offset: usize,
    log_filter: LogFilter,
    detail: Option<LogEntry>,
    fault_popup: Option<usize>,
    dispatch_popup: Option<DispatchPopup>,
    collect_value_popup: Option<CollectValuePopup>,
    form: DispatchForm,
    last_action: String,
    protocol_status: String,
    queue_status: String,
}

impl UiState {
    fn new(config: &ToolConfig) -> Self {
        Self {
            tab: 0,
            collect_selection: 0,
            dispatch_selection: 0,
            log_offset: 0,
            collect_value_offset: 0,
            dispatch_general_offset: 0,
            dispatch_energy_offset: 0,
            log_filter: LogFilter::All,
            detail: None,
            fault_popup: None,
            dispatch_popup: None,
            collect_value_popup: None,
            form: DispatchForm::new(config),
            last_action: "就绪；两侧任务已同时启动".to_owned(),
            protocol_status: format!(
                "T1={}ms  T2={}ms  T3={}ms  K={}  W={}",
                config.protocol.t1.as_millis(),
                config.protocol.t2.as_millis(),
                config.protocol.t3.as_millis(),
                config.protocol.k,
                config.protocol.w
            ),
            queue_status: format!(
                "队列容量：命令={}  事件={}  日志环={}",
                config.ui.command_capacity, config.ui.event_capacity, config.ui.log_capacity
            ),
        }
    }

    fn selected_action(&self) -> DispatchAction {
        DispatchAction::ALL[self.dispatch_selection]
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FormField {
    Ioa,
    Value,
    Phase,
    CommonAddress,
    OriginatorAddress,
    Qualifier,
    Qoi,
    QccRequest,
    QccFreeze,
    Repeat,
    Interval,
    Clock,
    Test,
}

impl FormField {
    const fn label(self) -> &'static str {
        match self {
            Self::Ioa => "IOA",
            Self::Value => "值/状态",
            Self::Phase => "控制阶段",
            Self::CommonAddress => "CA",
            Self::OriginatorAddress => "OA",
            Self::Qualifier => "限定词",
            Self::Qoi => "QOI",
            Self::QccRequest => "QCC 请求组",
            Self::QccFreeze => "QCC 冻结",
            Self::Repeat => "重复次数",
            Self::Interval => "间隔 ms",
            Self::Clock => "校时时间",
            Self::Test => "测试位",
        }
    }

    const fn is_toggle(self) -> bool {
        matches!(self, Self::Phase | Self::Test)
    }
}

const GENERAL_FIELDS: &[FormField] = &[
    FormField::CommonAddress,
    FormField::OriginatorAddress,
    FormField::Qoi,
    FormField::Repeat,
    FormField::Interval,
    FormField::Test,
];
const COUNTER_FIELDS: &[FormField] = &[
    FormField::CommonAddress,
    FormField::OriginatorAddress,
    FormField::QccRequest,
    FormField::QccFreeze,
    FormField::Repeat,
    FormField::Interval,
    FormField::Test,
];
const CLOCK_FIELDS: &[FormField] = &[
    FormField::CommonAddress,
    FormField::OriginatorAddress,
    FormField::Clock,
    FormField::Repeat,
    FormField::Interval,
    FormField::Test,
];
const READ_FIELDS: &[FormField] = &[
    FormField::Ioa,
    FormField::CommonAddress,
    FormField::OriginatorAddress,
    FormField::Repeat,
    FormField::Interval,
    FormField::Test,
];
const CONTROL_FIELDS: &[FormField] = &[
    FormField::Ioa,
    FormField::Value,
    FormField::Phase,
    FormField::CommonAddress,
    FormField::OriginatorAddress,
    FormField::Qualifier,
    FormField::Repeat,
    FormField::Interval,
    FormField::Test,
];

const fn action_fields(action: DispatchAction) -> &'static [FormField] {
    match action {
        DispatchAction::StartDt | DispatchAction::StopDt | DispatchAction::TestFr => &[],
        DispatchAction::GeneralInterrogation => GENERAL_FIELDS,
        DispatchAction::CounterInterrogation => COUNTER_FIELDS,
        DispatchAction::ClockSync => CLOCK_FIELDS,
        DispatchAction::Read => READ_FIELDS,
        DispatchAction::SingleControl
        | DispatchAction::DoubleControl
        | DispatchAction::RegulatingStep
        | DispatchAction::NormalizedSetpoint
        | DispatchAction::ScaledSetpoint
        | DispatchAction::FloatSetpoint => CONTROL_FIELDS,
    }
}

struct DispatchPopup {
    action: DispatchAction,
    field: usize,
    error: String,
}

impl DispatchPopup {
    const fn new(action: DispatchAction) -> Self {
        Self {
            action,
            field: 0,
            error: String::new(),
        }
    }

    fn current_field(&self) -> Option<FormField> {
        action_fields(self.action).get(self.field).copied()
    }

    fn next_field(&mut self, backwards: bool) {
        let length = action_fields(self.action).len();
        if length == 0 {
            return;
        }
        if backwards {
            self.field = self.field.checked_sub(1).unwrap_or(length - 1);
        } else {
            self.field = (self.field + 1) % length;
        }
    }
}

struct CollectValuePopup {
    ioa: u32,
    type_id: String,
    name: String,
    type_name: String,
    value: String,
    error: String,
}

impl CollectValuePopup {
    fn new(point: &PointView) -> Self {
        let value = point
            .value
            .as_deref()
            .and_then(|value| value.split_whitespace().next())
            .unwrap_or_default()
            .to_owned();
        Self {
            ioa: point.ioa,
            type_id: point.type_id.clone(),
            name: point.name.clone(),
            type_name: point.type_name.clone(),
            value,
            error: String::new(),
        }
    }
}

struct DispatchForm {
    ioa: String,
    value: String,
    common_address: String,
    originator_address: String,
    qualifier: String,
    qoi: String,
    qcc_request: String,
    qcc_freeze: String,
    repeat: String,
    interval: String,
    clock: String,
    phase: ControlPhase,
    test: bool,
}

impl DispatchForm {
    fn new(config: &ToolConfig) -> Self {
        Self {
            ioa: "201".to_owned(),
            value: "1".to_owned(),
            common_address: config.dispatch.common_address.to_string(),
            originator_address: config.protocol.originator_address.to_string(),
            qualifier: "0".to_owned(),
            qoi: "20".to_owned(),
            qcc_request: "5".to_owned(),
            qcc_freeze: "0".to_owned(),
            repeat: "1".to_owned(),
            interval: "200".to_owned(),
            clock: "now".to_owned(),
            phase: ControlPhase::Execute,
            test: false,
        }
    }

    fn text_mut(&mut self, field: FormField) -> Option<&mut String> {
        match field {
            FormField::Ioa => Some(&mut self.ioa),
            FormField::Value => Some(&mut self.value),
            FormField::CommonAddress => Some(&mut self.common_address),
            FormField::OriginatorAddress => Some(&mut self.originator_address),
            FormField::Qualifier => Some(&mut self.qualifier),
            FormField::Qoi => Some(&mut self.qoi),
            FormField::QccRequest => Some(&mut self.qcc_request),
            FormField::QccFreeze => Some(&mut self.qcc_freeze),
            FormField::Repeat => Some(&mut self.repeat),
            FormField::Interval => Some(&mut self.interval),
            FormField::Clock => Some(&mut self.clock),
            FormField::Phase | FormField::Test => None,
        }
    }

    fn display_value(&self, field: FormField) -> String {
        match field {
            FormField::Ioa => self.ioa.clone(),
            FormField::Value => self.value.clone(),
            FormField::Phase => self.phase.label().to_owned(),
            FormField::CommonAddress => self.common_address.clone(),
            FormField::OriginatorAddress => self.originator_address.clone(),
            FormField::Qualifier => self.qualifier.clone(),
            FormField::Qoi => self.qoi.clone(),
            FormField::QccRequest => self.qcc_request.clone(),
            FormField::QccFreeze => self.qcc_freeze.clone(),
            FormField::Repeat => self.repeat.clone(),
            FormField::Interval => self.interval.clone(),
            FormField::Clock => self.clock.clone(),
            FormField::Test => if self.test { "1 / 开" } else { "0 / 关" }.to_owned(),
        }
    }

    fn toggle(&mut self, field: FormField) {
        match field {
            FormField::Phase => {
                self.phase = match self.phase {
                    ControlPhase::Select => ControlPhase::Execute,
                    ControlPhase::Execute => ControlPhase::Select,
                };
            }
            FormField::Test => self.test = !self.test,
            _ => {}
        }
    }

    fn request(&self, action: DispatchAction) -> Result<DispatchRequest, String> {
        let is_link_action = matches!(
            action,
            DispatchAction::StartDt | DispatchAction::StopDt | DispatchAction::TestFr
        );
        let ioa = if action.uses_ioa() {
            let ioa = parse::<u32>(&self.ioa, "IOA")?;
            if ioa > 0xFF_FFFF {
                return Err("IOA 超出三字节范围".to_owned());
            }
            ioa
        } else {
            0
        };
        let common_address = if is_link_action {
            1
        } else {
            let address = parse::<u16>(&self.common_address, "CA")?;
            if address == 0 {
                return Err("CA 不能为 0；允许具体站地址或 65535 广播".to_owned());
            }
            address
        };
        let clock_time_ms = if action == DispatchAction::ClockSync {
            if self.clock.trim().eq_ignore_ascii_case("now") {
                None
            } else {
                Some(parse::<u64>(&self.clock, "校时时间（Unix ms 或 now）")?)
            }
        } else {
            None
        };
        let qualifier = if action.is_control() {
            let value = parse::<u8>(&self.qualifier, "限定词")?;
            let maximum = if matches!(
                action,
                DispatchAction::NormalizedSetpoint
                    | DispatchAction::ScaledSetpoint
                    | DispatchAction::FloatSetpoint
            ) {
                127
            } else {
                31
            };
            if value > maximum {
                return Err(format!("{} 限定词必须在 0..={maximum}", action.label()));
            }
            value
        } else {
            0
        };
        let qoi = if action == DispatchAction::GeneralInterrogation {
            let value = parse::<u8>(&self.qoi, "QOI")?;
            if !(20..=36).contains(&value) {
                return Err("QOI 必须在 20..=36".to_owned());
            }
            value
        } else {
            20
        };
        let qcc_request = if action == DispatchAction::CounterInterrogation {
            let value = parse::<u8>(&self.qcc_request, "QCC 请求组")?;
            if !(1..=5).contains(&value) {
                return Err("QCC 请求组必须在 1..=5（5=全局）".to_owned());
            }
            value
        } else {
            5
        };
        let qcc_freeze = if action == DispatchAction::CounterInterrogation {
            let value = parse::<u8>(&self.qcc_freeze, "QCC 冻结")?;
            if value > 3 {
                return Err("QCC 冻结限定词必须在 0..=3".to_owned());
            }
            value
        } else {
            0
        };
        Ok(DispatchRequest {
            action,
            ioa,
            value: if action.is_control() {
                parse::<f64>(&self.value, "值")?
            } else {
                0.0
            },
            phase: self.phase,
            common_address,
            originator_address: if is_link_action {
                0
            } else {
                parse::<u8>(&self.originator_address, "OA")?
            },
            qualifier,
            test: self.test,
            qoi,
            qcc_request,
            qcc_freeze,
            repeat: if is_link_action {
                1
            } else {
                let repeat = parse::<u16>(&self.repeat, "重复次数")?.max(1);
                if repeat > 1_000 {
                    return Err("重复次数不能大于 1000".to_owned());
                }
                repeat
            },
            interval_ms: if is_link_action {
                0
            } else {
                parse::<u64>(&self.interval, "发送间隔")?
            },
            clock_time_ms,
        })
    }

    fn apply_hint(&mut self, action: DispatchAction, points: &[DispatchPointConfig]) {
        let kind = match action {
            DispatchAction::SingleControl => Some(DispatchPointKind::SingleControl),
            DispatchAction::DoubleControl => Some(DispatchPointKind::DoubleControl),
            DispatchAction::RegulatingStep => Some(DispatchPointKind::RegulatingStep),
            DispatchAction::NormalizedSetpoint => Some(DispatchPointKind::NormalizedSetpoint),
            DispatchAction::ScaledSetpoint => Some(DispatchPointKind::ScaledSetpoint),
            DispatchAction::FloatSetpoint => Some(DispatchPointKind::FloatSetpoint),
            _ => None,
        };
        let Some(kind) = kind else {
            return;
        };
        let Some(point) = points.iter().find(|point| point.kind == kind) else {
            return;
        };
        self.ioa = point.ioa.to_string();
        let hinted_value = point
            .default_value
            .or_else(|| match (point.min, point.max) {
                (Some(min), Some(max)) => Some((min + max) / 2.0),
                (Some(min), None) => Some(min),
                (None, Some(max)) => Some(max),
                (None, None) => None,
            });
        if let Some(value) = hinted_value {
            self.value = value.to_string();
        } else if matches!(
            kind,
            DispatchPointKind::SingleControl
                | DispatchPointKind::DoubleControl
                | DispatchPointKind::RegulatingStep
        ) {
            self.value = "1".to_owned();
        }
    }
}

fn parse<T>(text: &str, label: &str) -> Result<T, String>
where
    T: std::str::FromStr,
{
    text.trim()
        .parse()
        .map_err(|_| format!("{label}格式不正确: {text:?}"))
}

fn handle_key(
    app: &mut UiState,
    key: KeyEvent,
    commands: &mpsc::Sender<RuntimeCommand>,
    snapshot: &AppSnapshot,
) -> bool {
    if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
        return true;
    }
    if app.detail.is_some() {
        if matches!(key.code, KeyCode::Esc | KeyCode::Enter | KeyCode::Char('q')) {
            app.detail = None;
        }
        return false;
    }
    if app.collect_value_popup.is_some() {
        handle_collect_value_popup_key(app, key, commands);
        return false;
    }
    if app.dispatch_popup.is_some() {
        handle_dispatch_popup_key(app, key, commands);
        return false;
    }
    if let Some(selection) = &mut app.fault_popup {
        match key.code {
            KeyCode::Esc => app.fault_popup = None,
            KeyCode::Up => *selection = selection.saturating_sub(1),
            KeyCode::Down => *selection = (*selection + 1).min(FaultPolicy::ALL.len()),
            KeyCode::Enter => {
                let policy = selection
                    .checked_sub(1)
                    .map(|index| FaultPolicy::ALL[index]);
                send_command(
                    app,
                    commands,
                    RuntimeCommand::SetFaultPolicy(policy),
                    "响应策略已提交",
                );
                app.fault_popup = None;
            }
            _ => {}
        }
        return false;
    }

    match key.code {
        KeyCode::Char('q') => return true,
        KeyCode::Char('r') => {
            send_command(
                app,
                commands,
                RuntimeCommand::ReloadConfig,
                "已请求重新加载配置",
            );
        }
        KeyCode::Char('f') => app.fault_popup = Some(0),
        KeyCode::Char(value @ '1'..='7') => {
            app.tab = value.to_digit(10).unwrap_or(1) as usize - 1;
            app.log_offset = 0;
        }
        KeyCode::Left => {
            app.tab = app.tab.checked_sub(1).unwrap_or(TAB_TITLES.len() - 1);
            app.log_offset = 0;
        }
        KeyCode::Right => {
            app.tab = (app.tab + 1) % TAB_TITLES.len();
            app.log_offset = 0;
        }
        _ => match app.tab {
            1 | 2 => handle_log_key(app, key, snapshot, commands),
            3 => handle_collect_key(app, key, commands),
            4 => handle_dispatch_key(app, key, commands, &snapshot.dispatch_points),
            _ => {}
        },
    }
    false
}

fn handle_mouse(
    app: &mut UiState,
    mouse: MouseEvent,
    commands: &mpsc::Sender<RuntimeCommand>,
    snapshot: &AppSnapshot,
    screen: Rect,
) {
    match mouse.kind {
        MouseEventKind::Down(MouseButton::Left) => {
            handle_left_click(app, mouse.column, mouse.row, commands, snapshot, screen);
        }
        MouseEventKind::ScrollUp => {
            if matches!(app.tab, 1 | 2) {
                app.log_offset = app.log_offset.saturating_add(1);
            } else {
                handle_value_scroll(app, mouse.column, mouse.row, snapshot, screen, false);
            }
        }
        MouseEventKind::ScrollDown => {
            if matches!(app.tab, 1 | 2) {
                app.log_offset = app.log_offset.saturating_sub(1);
            } else {
                handle_value_scroll(app, mouse.column, mouse.row, snapshot, screen, true);
            }
        }
        _ => {}
    }
}

fn handle_value_scroll(
    app: &mut UiState,
    column: u16,
    row: u16,
    snapshot: &AppSnapshot,
    screen: Rect,
    forward: bool,
) {
    if app.detail.is_some()
        || app.collect_value_popup.is_some()
        || app.dispatch_popup.is_some()
        || app.fault_popup.is_some()
    {
        return;
    }
    let Some(content) = main_content_area(screen) else {
        return;
    };
    match app.tab {
        5 if rect_contains(content, column, row) => {
            app.collect_value_offset = scroll_point_offset(
                app.collect_value_offset,
                snapshot.collect_values.len(),
                content,
                forward,
            );
        }
        6 => {
            let [general, energy] = dispatch_value_areas(content);
            if rect_contains(general, column, row) {
                app.dispatch_general_offset = scroll_point_offset(
                    app.dispatch_general_offset,
                    snapshot.dispatch_current_general.len(),
                    general,
                    forward,
                );
            } else if rect_contains(energy, column, row) {
                app.dispatch_energy_offset = scroll_point_offset(
                    app.dispatch_energy_offset,
                    snapshot.dispatch_current_energy.len(),
                    energy,
                    forward,
                );
            }
        }
        _ => {}
    }
}

fn main_content_area(screen: Rect) -> Option<Rect> {
    if screen.width < 80 || screen.height < 24 {
        return None;
    }
    Some(
        Layout::default()
            .direction(LayoutDirection::Vertical)
            .constraints([
                Constraint::Length(3),
                Constraint::Min(8),
                Constraint::Length(2),
            ])
            .split(screen)[1],
    )
}

fn dispatch_value_areas(area: Rect) -> [Rect; 2] {
    let rows = Layout::default()
        .direction(LayoutDirection::Vertical)
        .constraints([Constraint::Percentage(62), Constraint::Percentage(38)])
        .split(area);
    [rows[0], rows[1]]
}

fn point_table_visible_rows(area: Rect) -> usize {
    usize::from(area.height.saturating_sub(3).max(1))
}

fn clamped_point_offset(offset: usize, point_count: usize, area: Rect) -> usize {
    offset.min(point_count.saturating_sub(point_table_visible_rows(area)))
}

fn scroll_point_offset(offset: usize, point_count: usize, area: Rect, forward: bool) -> usize {
    let maximum = point_count.saturating_sub(point_table_visible_rows(area));
    let current = offset.min(maximum);
    if forward {
        current.saturating_add(1).min(maximum)
    } else {
        current.saturating_sub(1)
    }
}

fn point_index_at(
    area: Rect,
    column: u16,
    row: u16,
    offset: usize,
    point_count: usize,
) -> Option<usize> {
    let first_row = area.y.saturating_add(2);
    let last_row_exclusive = area.y.saturating_add(area.height).saturating_sub(1);
    if !rect_contains(area, column, row) || row < first_row || row >= last_row_exclusive {
        return None;
    }
    let visible_index = usize::from(row.saturating_sub(first_row));
    let offset = clamped_point_offset(offset, point_count, area);
    let index = offset.saturating_add(visible_index);
    (index < point_count).then_some(index)
}

fn handle_left_click(
    app: &mut UiState,
    column: u16,
    row: u16,
    commands: &mpsc::Sender<RuntimeCommand>,
    snapshot: &AppSnapshot,
    screen: Rect,
) {
    if app.detail.is_some() {
        if !rect_contains(centered_rect(screen, 82, 76), column, row) {
            app.detail = None;
        }
        return;
    }
    if app.collect_value_popup.is_some() {
        handle_collect_value_popup_click(app, column, row, commands, screen);
        return;
    }
    if app.dispatch_popup.is_some() {
        handle_dispatch_popup_click(app, column, row, commands, screen);
        return;
    }
    if app.fault_popup.is_some() {
        handle_fault_popup_click(app, column, row, commands, snapshot, screen);
        return;
    }
    if screen.width < 80 || screen.height < 24 {
        return;
    }

    let layout = Layout::default()
        .direction(LayoutDirection::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(8),
            Constraint::Length(2),
        ])
        .split(screen);
    if let Some(tab) = tab_at(layout[0], column, row) {
        app.tab = tab;
        app.log_offset = 0;
        return;
    }

    match app.tab {
        1 | 2 => handle_log_click(app, column, row, commands, snapshot, layout[1]),
        3 => {
            let index = row.saturating_sub(layout[1].y.saturating_add(1)) as usize;
            if rect_contains(layout[1], column, row)
                && row >= layout[1].y.saturating_add(1)
                && index < UploadKind::ALL.len()
            {
                app.collect_selection = index;
                let kind = UploadKind::ALL[index];
                send_command(
                    app,
                    commands,
                    RuntimeCommand::CollectUpload(kind),
                    format!("已提交：{}；结果请看采集日志", kind.label()),
                );
            }
        }
        4 => {
            let index = row.saturating_sub(layout[1].y.saturating_add(1)) as usize;
            if rect_contains(layout[1], column, row)
                && row >= layout[1].y.saturating_add(1)
                && index < DispatchAction::ALL.len()
            {
                app.dispatch_selection = index;
                activate_dispatch_action(
                    app,
                    DispatchAction::ALL[index],
                    commands,
                    &snapshot.dispatch_points,
                );
            }
        }
        5 => {
            if let Some(index) = point_index_at(
                layout[1],
                column,
                row,
                app.collect_value_offset,
                snapshot.collect_values.len(),
            ) && let Some(point) = snapshot.collect_values.get(index)
            {
                app.collect_value_popup = Some(CollectValuePopup::new(point));
            }
        }
        _ => {}
    }
}

fn tab_at(area: Rect, column: u16, row: u16) -> Option<usize> {
    if row != area.y.saturating_add(1) {
        return None;
    }
    let mut start = area.x.saturating_add(1);
    for (index, title) in TAB_TITLES.iter().enumerate() {
        let width = Span::raw(*title).width() as u16;
        if column >= start && column < start.saturating_add(width) {
            return Some(index);
        }
        start = start.saturating_add(width).saturating_add(1);
    }
    None
}

fn handle_log_click(
    app: &mut UiState,
    column: u16,
    row: u16,
    commands: &mpsc::Sender<RuntimeCommand>,
    snapshot: &AppSnapshot,
    area: Rect,
) {
    let rows = Layout::default()
        .direction(LayoutDirection::Vertical)
        .constraints([Constraint::Length(3), Constraint::Min(3)])
        .split(area);
    if row == rows[0].y.saturating_add(1) {
        let clear = Rect::new(rows[0].x.saturating_add(1), row, 10, 1);
        let filter = Rect::new(rows[0].x.saturating_add(13), row, 20, 1);
        if rect_contains(clear, column, row) {
            let side = if app.tab == 1 {
                Side::Collect
            } else {
                Side::Dispatch
            };
            send_command(
                app,
                commands,
                RuntimeCommand::ClearLogs(side),
                format!("{}日志已清屏", side.label()),
            );
            app.log_offset = 0;
        } else if rect_contains(filter, column, row) {
            app.log_filter = app.log_filter.next();
            app.log_offset = 0;
        }
        return;
    }

    let panes = Layout::default()
        .direction(LayoutDirection::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(rows[1]);
    let (pane, direction) = if rect_contains(panes[0], column, row) {
        (panes[0], Direction::Incoming)
    } else if rect_contains(panes[1], column, row) {
        (panes[1], Direction::Outgoing)
    } else {
        return;
    };
    if row <= pane.y || row >= pane.y.saturating_add(pane.height).saturating_sub(1) {
        return;
    }
    let logs = if app.tab == 1 {
        snapshot.collect_logs.iter().cloned().collect::<Vec<_>>()
    } else {
        snapshot.dispatch_logs.iter().cloned().collect::<Vec<_>>()
    };
    let visible = log_window(&logs, direction, app.log_filter, app.log_offset, pane);
    let index = row.saturating_sub(pane.y.saturating_add(1)) as usize;
    app.detail = visible.get(index).map(|entry| (*entry).clone());
}

fn handle_fault_popup_click(
    app: &mut UiState,
    column: u16,
    row: u16,
    commands: &mpsc::Sender<RuntimeCommand>,
    _snapshot: &AppSnapshot,
    screen: Rect,
) {
    let area = centered_rect(screen, 62, 58);
    if !rect_contains(area, column, row) {
        app.fault_popup = None;
        return;
    }
    let index = row.saturating_sub(area.y.saturating_add(1)) as usize;
    if row >= area.y.saturating_add(1) && index <= FaultPolicy::ALL.len() {
        let policy = index.checked_sub(1).map(|index| FaultPolicy::ALL[index]);
        send_command(
            app,
            commands,
            RuntimeCommand::SetFaultPolicy(policy),
            "响应策略已提交",
        );
        app.fault_popup = None;
    }
}

fn handle_dispatch_popup_click(
    app: &mut UiState,
    column: u16,
    row: u16,
    commands: &mpsc::Sender<RuntimeCommand>,
    screen: Rect,
) {
    let area = dispatch_popup_area(screen);
    if !rect_contains(area, column, row) {
        app.dispatch_popup = None;
        return;
    }
    let sections = Layout::default()
        .direction(LayoutDirection::Vertical)
        .constraints([
            Constraint::Min(5),
            Constraint::Length(2),
            Constraint::Length(2),
        ])
        .split(area);
    if rect_contains(sections[0], column, row) && row >= sections[0].y.saturating_add(2) {
        let index = row.saturating_sub(sections[0].y.saturating_add(2)) as usize;
        let field = app
            .dispatch_popup
            .as_ref()
            .and_then(|popup| action_fields(popup.action).get(index))
            .copied();
        if let Some(field) = field {
            if let Some(popup) = &mut app.dispatch_popup {
                popup.field = index;
                popup.error.clear();
            }
            if field.is_toggle() {
                app.form.toggle(field);
            }
        }
    } else if rect_contains(sections[2], column, row) {
        if column < sections[2].x.saturating_add(sections[2].width / 2) {
            submit_dispatch_popup(app, commands);
        } else {
            app.dispatch_popup = None;
        }
    }
}

fn handle_collect_value_popup_click(
    app: &mut UiState,
    column: u16,
    row: u16,
    commands: &mpsc::Sender<RuntimeCommand>,
    screen: Rect,
) {
    let area = collect_value_popup_area(screen);
    if !rect_contains(area, column, row) {
        app.collect_value_popup = None;
        return;
    }
    let sections = Layout::default()
        .direction(LayoutDirection::Vertical)
        .constraints([
            Constraint::Min(4),
            Constraint::Length(2),
            Constraint::Length(2),
        ])
        .split(area);
    if rect_contains(sections[2], column, row) {
        if column < sections[2].x.saturating_add(sections[2].width / 2) {
            submit_collect_value_popup(app, commands);
        } else {
            app.collect_value_popup = None;
        }
    }
}

fn rect_contains(area: Rect, column: u16, row: u16) -> bool {
    column >= area.x
        && column < area.x.saturating_add(area.width)
        && row >= area.y
        && row < area.y.saturating_add(area.height)
}

fn handle_log_key(
    app: &mut UiState,
    key: KeyEvent,
    snapshot: &AppSnapshot,
    commands: &mpsc::Sender<RuntimeCommand>,
) {
    match key.code {
        KeyCode::Up | KeyCode::PageUp => app.log_offset = app.log_offset.saturating_add(1),
        KeyCode::Down | KeyCode::PageDown => app.log_offset = app.log_offset.saturating_sub(1),
        KeyCode::Home => app.log_offset = usize::MAX / 2,
        KeyCode::End => app.log_offset = 0,
        KeyCode::Char('p') => {
            app.log_filter = app.log_filter.next();
            app.log_offset = 0;
        }
        KeyCode::Char('c') => {
            let side = if app.tab == 1 {
                Side::Collect
            } else {
                Side::Dispatch
            };
            send_command(
                app,
                commands,
                RuntimeCommand::ClearLogs(side),
                format!("{}日志已清屏", side.label()),
            );
            app.log_offset = 0;
        }
        KeyCode::Enter => {
            let logs = if app.tab == 1 {
                &snapshot.collect_logs
            } else {
                &snapshot.dispatch_logs
            };
            app.detail = logs
                .iter()
                .rev()
                .filter(|entry| app.log_filter.matches(entry.category))
                .nth(app.log_offset)
                .cloned();
        }
        _ => {}
    }
}

fn handle_collect_key(app: &mut UiState, key: KeyEvent, commands: &mpsc::Sender<RuntimeCommand>) {
    match key.code {
        KeyCode::Up => app.collect_selection = app.collect_selection.saturating_sub(1),
        KeyCode::Down => {
            app.collect_selection = (app.collect_selection + 1).min(UploadKind::ALL.len() - 1);
        }
        KeyCode::Enter => {
            let kind = UploadKind::ALL[app.collect_selection];
            send_command(
                app,
                commands,
                RuntimeCommand::CollectUpload(kind),
                format!("已提交：{}；结果请看采集日志", kind.label()),
            );
        }
        _ => {}
    }
}

fn handle_dispatch_key(
    app: &mut UiState,
    key: KeyEvent,
    commands: &mpsc::Sender<RuntimeCommand>,
    points: &[DispatchPointConfig],
) {
    match key.code {
        KeyCode::Up | KeyCode::Char('k') => {
            app.dispatch_selection = app.dispatch_selection.saturating_sub(1);
        }
        KeyCode::Down | KeyCode::Char('j') => {
            app.dispatch_selection =
                (app.dispatch_selection + 1).min(DispatchAction::ALL.len() - 1);
        }
        KeyCode::Enter => activate_dispatch_action(app, app.selected_action(), commands, points),
        _ => {}
    }
}

fn activate_dispatch_action(
    app: &mut UiState,
    action: DispatchAction,
    commands: &mpsc::Sender<RuntimeCommand>,
    points: &[DispatchPointConfig],
) {
    app.form.apply_hint(action, points);
    if action_fields(action).is_empty() {
        match app.form.request(action) {
            Ok(request) => {
                send_command(
                    app,
                    commands,
                    RuntimeCommand::Dispatch(request),
                    format!("已提交：{}；结果请看调度日志", action.label()),
                );
            }
            Err(error) => app.last_action = error,
        }
    } else {
        app.dispatch_popup = Some(DispatchPopup::new(action));
    }
}

fn handle_dispatch_popup_key(
    app: &mut UiState,
    key: KeyEvent,
    commands: &mpsc::Sender<RuntimeCommand>,
) {
    match key.code {
        KeyCode::Esc => app.dispatch_popup = None,
        KeyCode::Tab => {
            if let Some(popup) = &mut app.dispatch_popup {
                popup.next_field(key.modifiers.contains(KeyModifiers::SHIFT));
                popup.error.clear();
            }
        }
        KeyCode::BackTab => {
            if let Some(popup) = &mut app.dispatch_popup {
                popup.next_field(true);
                popup.error.clear();
            }
        }
        KeyCode::Backspace => {
            let field = app
                .dispatch_popup
                .as_ref()
                .and_then(DispatchPopup::current_field);
            if let Some(text) = field.and_then(|field| app.form.text_mut(field)) {
                text.pop();
            }
        }
        KeyCode::Char('s') => app.form.toggle(FormField::Phase),
        KeyCode::Char('t') => app.form.toggle(FormField::Test),
        KeyCode::Enter => submit_dispatch_popup(app, commands),
        KeyCode::Char(character) => {
            let field = app
                .dispatch_popup
                .as_ref()
                .and_then(DispatchPopup::current_field);
            if let Some(field) = field
                && valid_form_character(character, field)
                && let Some(text) = app.form.text_mut(field)
            {
                text.push(character);
                if let Some(popup) = &mut app.dispatch_popup {
                    popup.error.clear();
                }
            }
        }
        _ => {}
    }
}

fn submit_dispatch_popup(app: &mut UiState, commands: &mpsc::Sender<RuntimeCommand>) {
    let Some(action) = app.dispatch_popup.as_ref().map(|popup| popup.action) else {
        return;
    };
    match app.form.request(action) {
        Ok(request) => {
            if send_command(
                app,
                commands,
                RuntimeCommand::Dispatch(request),
                format!("已提交：{}；结果请看调度日志", action.label()),
            ) {
                app.dispatch_popup = None;
            }
        }
        Err(error) => {
            if let Some(popup) = &mut app.dispatch_popup {
                popup.error = error;
            }
        }
    }
}

fn handle_collect_value_popup_key(
    app: &mut UiState,
    key: KeyEvent,
    commands: &mpsc::Sender<RuntimeCommand>,
) {
    match key.code {
        KeyCode::Esc => app.collect_value_popup = None,
        KeyCode::Backspace => {
            if let Some(popup) = &mut app.collect_value_popup {
                popup.value.pop();
                popup.error.clear();
            }
        }
        KeyCode::Enter => submit_collect_value_popup(app, commands),
        KeyCode::Char(character)
            if character.is_ascii_digit() || matches!(character, '-' | '+' | '.') =>
        {
            if let Some(popup) = &mut app.collect_value_popup {
                popup.value.push(character);
                popup.error.clear();
            }
        }
        _ => {}
    }
}

fn submit_collect_value_popup(app: &mut UiState, commands: &mpsc::Sender<RuntimeCommand>) {
    let Some(popup) = &app.collect_value_popup else {
        return;
    };
    let ioa = popup.ioa;
    let type_id = popup.type_id.clone();
    let value = match parse::<f64>(&popup.value, "采集点值") {
        Ok(value) if value.is_finite() => value,
        Ok(_) => {
            if let Some(popup) = &mut app.collect_value_popup {
                popup.error = "采集点值必须是有限数字".to_owned();
            }
            return;
        }
        Err(error) => {
            if let Some(popup) = &mut app.collect_value_popup {
                popup.error = error;
            }
            return;
        }
    };
    if send_command(
        app,
        commands,
        RuntimeCommand::SetCollectValue {
            ioa,
            type_id: type_id.clone(),
            value,
        },
        format!("已提交采集点修改：{type_id}/IOA={ioa}，值={value}"),
    ) {
        app.collect_value_popup = None;
    }
}

fn valid_form_character(character: char, field: FormField) -> bool {
    if field == FormField::Clock {
        character.is_ascii_alphanumeric() || matches!(character, '-' | '_')
    } else {
        character.is_ascii_digit()
            || (field == FormField::Value && matches!(character, '-' | '+' | '.'))
    }
}

fn send_command(
    app: &mut UiState,
    commands: &mpsc::Sender<RuntimeCommand>,
    command: RuntimeCommand,
    success: impl Into<String>,
) -> bool {
    match commands.try_send(command) {
        Ok(()) => {
            app.last_action = success.into();
            true
        }
        Err(error) => {
            app.last_action = format!("命令队列拒绝请求: {error}");
            false
        }
    }
}

fn render(frame: &mut Frame<'_>, app: &UiState, snapshot: &AppSnapshot) {
    let area = frame.area();
    frame.render_widget(Block::default().style(base_style()), area);
    if area.width < 80 || area.height < 24 {
        frame.render_widget(
            Paragraph::new(format!(
                "终端尺寸过小：当前 {}×{}，至少需要 80×24",
                area.width, area.height
            ))
            .alignment(Alignment::Center)
            .style(error_style())
            .block(light_block("agcavc104test")),
            centered_rect(area, 80, 30),
        );
        return;
    }

    let layout = Layout::default()
        .direction(LayoutDirection::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(8),
            Constraint::Length(2),
        ])
        .split(area);
    render_tabs(frame, app, layout[0]);
    match app.tab {
        0 => render_status(frame, app, snapshot, layout[1]),
        1 => render_logs(
            frame,
            app,
            &snapshot.collect_logs.iter().cloned().collect::<Vec<_>>(),
            "采集",
            layout[1],
        ),
        2 => render_logs(
            frame,
            app,
            &snapshot.dispatch_logs.iter().cloned().collect::<Vec<_>>(),
            "调度",
            layout[1],
        ),
        3 => render_collect_commands(frame, app, layout[1]),
        4 => render_dispatch_commands(frame, app, layout[1]),
        5 => render_point_table(
            frame,
            "采集当前值 · 实时刷新 · 单击点位修改",
            &snapshot.collect_values,
            app.collect_value_offset,
            layout[1],
        ),
        6 => render_dispatch_values(frame, app, snapshot, layout[1]),
        _ => {}
    }
    render_footer(frame, app, layout[2]);
    if let Some(selection) = app.fault_popup {
        render_fault_popup(frame, selection, snapshot.fault_policy);
    }
    if let Some(popup) = &app.dispatch_popup {
        render_dispatch_popup(frame, popup, &app.form);
    }
    if let Some(popup) = &app.collect_value_popup {
        render_collect_value_popup(frame, popup);
    }
    if let Some(entry) = &app.detail {
        render_log_detail(frame, entry);
    }
}

fn render_tabs(frame: &mut Frame<'_>, app: &UiState, area: Rect) {
    let titles = TAB_TITLES
        .iter()
        .map(|title| Line::from(Span::styled(*title, Style::default().fg(DARK_TEXT))))
        .collect::<Vec<_>>();
    frame.render_widget(
        Tabs::new(titles)
            .select(app.tab)
            .style(base_style())
            .highlight_style(selected_style())
            .padding("", "")
            .divider(Span::styled("│", Style::default().fg(BORDER)))
            .block(light_block("IEC 60870-5-104 双端人工测试台")),
        area,
    );
}

fn render_status(frame: &mut Frame<'_>, app: &UiState, snapshot: &AppSnapshot, area: Rect) {
    let rows = Layout::default()
        .direction(LayoutDirection::Vertical)
        .constraints([Constraint::Percentage(58), Constraint::Percentage(42)])
        .split(area);
    let columns = Layout::default()
        .direction(LayoutDirection::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(rows[0]);
    render_connection(
        frame,
        "采集侧模拟子站",
        &snapshot.collect_connection,
        columns[0],
    );
    render_connection(
        frame,
        "调度侧模拟主站",
        &snapshot.dispatch_connection,
        columns[1],
    );

    let policy = snapshot
        .fault_policy
        .map_or("无（使用点位默认策略）", FaultPolicy::label);
    let mut lines = vec![
        Line::from(vec![
            Span::styled("总召事务  ", label_style()),
            Span::styled(
                if snapshot.general_pending {
                    "等待 ACTTERM"
                } else {
                    "空闲"
                },
                if snapshot.general_pending {
                    warning_style()
                } else {
                    success_style()
                },
            ),
            Span::raw("    "),
            Span::styled("电度事务  ", label_style()),
            Span::styled(
                if snapshot.energy_pending {
                    "等待 ACTTERM"
                } else {
                    "空闲"
                },
                if snapshot.energy_pending {
                    warning_style()
                } else {
                    success_style()
                },
            ),
            Span::raw("    "),
            Span::styled("应用回执  ", label_style()),
            Span::styled(
                snapshot.application_pending.to_string(),
                if snapshot.application_pending == 0 {
                    success_style()
                } else {
                    warning_style()
                },
            ),
        ]),
        Line::from(vec![
            Span::styled("下一条采集控制响应  ", label_style()),
            Span::styled(policy, warning_style()),
        ]),
        Line::from(vec![
            Span::styled("链路参数  ", label_style()),
            Span::raw(&app.protocol_status),
        ]),
        Line::from(vec![
            Span::styled("背压参数  ", label_style()),
            Span::raw(&app.queue_status),
        ]),
        Line::from(Span::styled("最近系统事件", label_style())),
    ];
    for entry in snapshot.system_logs.iter().rev().take(4).rev() {
        lines.push(Line::from(Span::styled(log_text(entry), log_style(entry))));
    }
    frame.render_widget(
        Paragraph::new(lines)
            .style(base_style())
            .wrap(Wrap { trim: true })
            .block(light_block("事务、故障注入与协调器")),
        rows[1],
    );
}

fn render_connection(
    frame: &mut Frame<'_>,
    title: &str,
    view: &crate::model::ConnectionView,
    area: Rect,
) {
    let phase_style = match view.phase {
        crate::model::ConnectionPhase::Active => success_style(),
        crate::model::ConnectionPhase::Error => error_style(),
        crate::model::ConnectionPhase::ReconnectWait
        | crate::model::ConnectionPhase::Connecting
        | crate::model::ConnectionPhase::Starting
        | crate::model::ConnectionPhase::Stopping => warning_style(),
        _ => normal_badge_style(),
    };
    let lines = vec![
        Line::from(vec![
            Span::styled("状态  ", label_style()),
            Span::styled(view.phase.label(), phase_style),
        ]),
        Line::from(vec![
            Span::styled("端点  ", label_style()),
            Span::raw(&view.endpoint),
        ]),
        Line::from(vec![
            Span::styled("对端  ", label_style()),
            Span::raw(view.peer.as_deref().unwrap_or("—")),
        ]),
        Line::from(vec![
            Span::styled("说明  ", label_style()),
            Span::raw(if view.detail.is_empty() {
                "—"
            } else {
                &view.detail
            }),
        ]),
        Line::from(vec![
            Span::styled("最后接收  ", label_style()),
            Span::raw(format_optional_time(view.last_rx_ms)),
        ]),
        Line::from(vec![
            Span::styled("最后发送  ", label_style()),
            Span::raw(format_optional_time(view.last_tx_ms)),
        ]),
    ];
    frame.render_widget(
        Paragraph::new(lines)
            .style(base_style())
            .block(light_block(title))
            .wrap(Wrap { trim: true }),
        area,
    );
}

fn render_logs(frame: &mut Frame<'_>, app: &UiState, logs: &[LogEntry], side: &str, area: Rect) {
    let rows = Layout::default()
        .direction(LayoutDirection::Vertical)
        .constraints([Constraint::Length(3), Constraint::Min(3)])
        .split(area);
    let controls = Line::from(vec![
        Span::styled(" [ 清屏 ] ", error_style()),
        Span::raw("  "),
        Span::styled(
            format!(" [ 过滤：{} ] ", app.log_filter.label()),
            selected_style(),
        ),
        Span::raw("  单击日志行查看完整结构化报文"),
    ]);
    frame.render_widget(
        Paragraph::new(controls)
            .style(base_style())
            .block(light_block(&format!("{side}日志操作"))),
        rows[0],
    );
    let panes = Layout::default()
        .direction(LayoutDirection::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(rows[1]);
    render_log_pane(
        frame,
        app,
        logs,
        Direction::Incoming,
        &format!("{side}接收"),
        panes[0],
    );
    render_log_pane(
        frame,
        app,
        logs,
        Direction::Outgoing,
        &format!("{side}发送 / 状态 / 广播"),
        panes[1],
    );
}

fn render_log_pane(
    frame: &mut Frame<'_>,
    app: &UiState,
    logs: &[LogEntry],
    direction: Direction,
    title: &str,
    area: Rect,
) {
    let filtered = log_window(logs, direction, app.log_filter, app.log_offset, area);
    let items = filtered
        .iter()
        .map(|entry| ListItem::new(Line::from(log_text(entry))).style(log_style(entry)))
        .collect::<Vec<_>>();
    frame.render_widget(
        List::new(items)
            .style(base_style())
            .block(light_block(&format!("{title} · ↑↓滚动 · 单击查看",))),
        area,
    );
}

fn log_window(
    logs: &[LogEntry],
    direction: Direction,
    filter: LogFilter,
    log_offset: usize,
    area: Rect,
) -> Vec<&LogEntry> {
    let filtered = logs
        .iter()
        .filter(|entry| {
            let direction_matches = if entry.category == LogCategory::CommandDivider {
                true
            } else if direction == Direction::Outgoing {
                entry.direction != Direction::Incoming
            } else {
                entry.direction == direction
            };
            direction_matches && filter.matches(entry.category)
        })
        .collect::<Vec<_>>();
    let visible = area.height.saturating_sub(2) as usize;
    let offset = log_offset.min(filtered.len().saturating_sub(1));
    let end = filtered.len().saturating_sub(offset);
    let start = end.saturating_sub(visible);
    filtered[start..end].to_vec()
}

fn render_collect_commands(frame: &mut Frame<'_>, app: &UiState, area: Rect) {
    let items = UploadKind::ALL
        .iter()
        .enumerate()
        .map(|(index, kind)| {
            let style = if index == app.collect_selection {
                selected_style()
            } else {
                base_style()
            };
            ListItem::new(format!(" {:>2}. {}", index + 1, kind.label())).style(style)
        })
        .collect::<Vec<_>>();
    frame.render_widget(
        List::new(items).block(light_block(
            "采集主动上送 · 单击立即执行 · ↑↓选择 · Enter执行",
        )),
        area,
    );
}

fn render_dispatch_commands(frame: &mut Frame<'_>, app: &UiState, area: Rect) {
    let items = DispatchAction::ALL
        .iter()
        .enumerate()
        .map(|(index, action)| {
            let style = if index == app.dispatch_selection {
                selected_style()
            } else {
                base_style()
            };
            ListItem::new(format!(" {:>2}. {}", index + 1, action.label())).style(style)
        })
        .collect::<Vec<_>>();
    frame.render_widget(
        List::new(items).block(light_block(
            "调度指令 · 单击执行或按需填写参数 · ↑↓或 j/k 选择 · Enter执行/打开",
        )),
        area,
    );
}

fn render_point_table(
    frame: &mut Frame<'_>,
    title: &str,
    points: &[PointView],
    offset: usize,
    area: Rect,
) {
    let offset = clamped_point_offset(offset, points.len(), area);
    let rows = points.iter().skip(offset).map(|point| {
        Row::new(vec![
            Cell::from(point.ioa.to_string()),
            Cell::from(point.name.clone()),
            Cell::from(point.type_name.clone()),
            Cell::from(point.value.clone().unwrap_or_else(|| "—".to_owned())),
            Cell::from(point.quality.clone()),
            Cell::from(format_optional_time(point.updated_ms)),
        ])
        .style(if point.configured {
            base_style()
        } else {
            warning_style()
        })
    });
    frame.render_widget(
        Table::new(
            rows,
            [
                Constraint::Length(9),
                Constraint::Percentage(24),
                Constraint::Percentage(22),
                Constraint::Percentage(22),
                Constraint::Length(11),
                Constraint::Length(13),
            ],
        )
        .header(Row::new(["IOA", "名称", "类型", "值", "品质", "更新时间"]).style(header_style()))
        .column_spacing(1)
        .block(light_block(title)),
        area,
    );
}

fn render_dispatch_values(
    frame: &mut Frame<'_>,
    app: &UiState,
    snapshot: &AppSnapshot,
    area: Rect,
) {
    let [general, energy] = dispatch_value_areas(area);
    render_point_table(
        frame,
        "调度当前遥信/遥测 · 实时刷新",
        &snapshot.dispatch_current_general,
        app.dispatch_general_offset,
        general,
    );
    render_point_table(
        frame,
        "调度当前电度 · 实时刷新",
        &snapshot.dispatch_current_energy,
        app.dispatch_energy_offset,
        energy,
    );
}

fn render_footer(frame: &mut Frame<'_>, app: &UiState, area: Rect) {
    let line = Line::from(vec![
        Span::styled(" 鼠标/←→/1-7 ", selected_style()),
        Span::raw("操作/切页  "),
        Span::styled(" f ", warning_style()),
        Span::raw("故障策略  "),
        Span::styled(" r ", success_style()),
        Span::raw("重载  "),
        Span::styled(" q/Ctrl+C ", error_style()),
        Span::raw("退出    "),
        Span::styled(&app.last_action, label_style()),
    ]);
    frame.render_widget(Paragraph::new(line).style(base_style()), area);
}

fn render_fault_popup(frame: &mut Frame<'_>, selection: usize, active: Option<FaultPolicy>) {
    let area = centered_rect(frame.area(), 62, 58);
    frame.render_widget(Clear, area);
    frame.render_widget(Block::default().style(base_style()), area);
    let mut options = vec![None];
    options.extend(FaultPolicy::ALL.into_iter().map(Some));
    let items = options
        .iter()
        .enumerate()
        .map(|(index, option)| {
            let label = option.map_or("清除等待策略 / 使用点位默认", FaultPolicy::label);
            let marker = if *option == active { "●" } else { "○" };
            let style = if index == selection {
                selected_style()
            } else {
                base_style()
            };
            ListItem::new(format!(" {marker} {label}")).style(style)
        })
        .collect::<Vec<_>>();
    frame.render_widget(
        List::new(items).block(light_block(
            "下一条采集控制响应 · ↑↓选择 · Enter确认 · Esc取消",
        )),
        area,
    );
}

fn render_dispatch_popup(frame: &mut Frame<'_>, popup: &DispatchPopup, form: &DispatchForm) {
    let area = dispatch_popup_area(frame.area());
    frame.render_widget(Clear, area);
    frame.render_widget(Block::default().style(base_style()), area);
    let sections = Layout::default()
        .direction(LayoutDirection::Vertical)
        .constraints([
            Constraint::Min(5),
            Constraint::Length(2),
            Constraint::Length(2),
        ])
        .split(area);
    let fields = action_fields(popup.action);
    if fields.is_empty() {
        frame.render_widget(
            Paragraph::new(format!("确认执行 {}？", popup.action.label()))
                .alignment(Alignment::Center)
                .style(base_style())
                .block(light_block("指令确认")),
            sections[0],
        );
    } else {
        let rows = fields.iter().enumerate().map(|(index, field)| {
            Row::new(vec![
                Cell::from(field.label()),
                Cell::from(form.display_value(*field)),
            ])
            .style(if index == popup.field {
                selected_style()
            } else {
                base_style()
            })
        });
        frame.render_widget(
            Table::new(rows, [Constraint::Length(16), Constraint::Min(12)])
                .header(Row::new(["参数", "值"]).style(header_style()))
                .column_spacing(1)
                .block(light_block(&format!(
                    "{} · 单击字段编辑 · Tab切换 · s阶段 · t测试位",
                    popup.action.label()
                ))),
            sections[0],
        );
    }
    frame.render_widget(
        Paragraph::new(popup.error.as_str())
            .alignment(Alignment::Center)
            .style(if popup.error.is_empty() {
                base_style()
            } else {
                error_style()
            }),
        sections[1],
    );
    frame.render_widget(
        Paragraph::new(Line::from(vec![
            Span::styled(" [ 确认发送 ] ", success_style()),
            Span::raw("          "),
            Span::styled(" [ 取消 ] ", error_style()),
        ]))
        .alignment(Alignment::Center)
        .style(base_style()),
        sections[2],
    );
}

fn render_collect_value_popup(frame: &mut Frame<'_>, popup: &CollectValuePopup) {
    let area = collect_value_popup_area(frame.area());
    frame.render_widget(Clear, area);
    frame.render_widget(Block::default().style(base_style()), area);
    let sections = Layout::default()
        .direction(LayoutDirection::Vertical)
        .constraints([
            Constraint::Min(4),
            Constraint::Length(2),
            Constraint::Length(2),
        ])
        .split(area);
    frame.render_widget(
        Paragraph::new(vec![
            Line::from(format!(
                "IOA={}  名称={}  类型={}",
                popup.ioa, popup.name, popup.type_name
            )),
            Line::from(vec![
                Span::styled("新值  ", label_style()),
                Span::styled(&popup.value, selected_style()),
            ]),
        ])
        .style(base_style())
        .block(light_block("手工修改采集点值 · 输入后单击确认")),
        sections[0],
    );
    frame.render_widget(
        Paragraph::new(popup.error.as_str())
            .alignment(Alignment::Center)
            .style(if popup.error.is_empty() {
                base_style()
            } else {
                error_style()
            }),
        sections[1],
    );
    frame.render_widget(
        Paragraph::new(Line::from(vec![
            Span::styled(" [ 确认修改 ] ", success_style()),
            Span::raw("          "),
            Span::styled(" [ 取消 ] ", error_style()),
        ]))
        .alignment(Alignment::Center)
        .style(base_style()),
        sections[2],
    );
}

fn dispatch_popup_area(area: Rect) -> Rect {
    centered_rect(area, 78, 82)
}

fn collect_value_popup_area(area: Rect) -> Rect {
    centered_rect(area, 64, 42)
}

fn render_log_detail(frame: &mut Frame<'_>, entry: &LogEntry) {
    let area = centered_rect(frame.area(), 82, 76);
    frame.render_widget(Clear, area);
    let mut lines = vec![
        Line::from(Span::styled(log_text(entry), log_style(entry))),
        Line::from(""),
    ];
    if let Some(meta) = &entry.protocol {
        lines.push(Line::from(format!(
            "TypeID={}  COT={}  CA={}  OA={}  SQ={}  Test={}  Negative={}",
            meta.type_id,
            meta.cot,
            meta.common_address,
            meta.originator_address,
            u8::from(meta.sequence),
            u8::from(meta.test),
            u8::from(meta.negative),
        )));
        lines.push(Line::from(format!("IOA={:?}", meta.ioas)));
        lines.push(Line::from(""));
    }
    lines.extend(
        entry
            .details
            .iter()
            .map(|detail| Line::from(detail.clone())),
    );
    frame.render_widget(
        Paragraph::new(lines)
            .style(base_style())
            .wrap(Wrap { trim: false })
            .block(light_block("结构化报文详情 · 单击弹窗外或 Enter/Esc 关闭")),
        area,
    );
}

fn centered_rect(area: Rect, percent_x: u16, percent_y: u16) -> Rect {
    let vertical = Layout::default()
        .direction(LayoutDirection::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(area);
    Layout::default()
        .direction(LayoutDirection::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(vertical[1])[1]
}

fn log_text(entry: &LogEntry) -> String {
    format!(
        "{} {:<3} {:<2} {:<4} {}",
        format_time(entry.timestamp_ms),
        entry.side.label(),
        entry.direction.label(),
        category_label(entry.category),
        entry.summary
    )
}

const fn category_label(category: LogCategory) -> &'static str {
    match category {
        LogCategory::Connection => "连接",
        LogCategory::Protocol => "报文",
        LogCategory::ActiveUpload => "主动",
        LogCategory::CommandDivider => "分隔",
        LogCategory::Interrogation => "召唤",
        LogCategory::Control => "控制",
        LogCategory::Configuration => "配置",
        LogCategory::KeepAlive => "保活",
    }
}

fn format_optional_time(value: Option<u64>) -> String {
    value.map_or_else(|| "—".to_owned(), format_time)
}

fn format_time(timestamp_ms: u64) -> String {
    i64::try_from(timestamp_ms)
        .ok()
        .and_then(DateTime::<Utc>::from_timestamp_millis)
        .map(|value| {
            value
                .with_timezone(&Local)
                .format("%H:%M:%S%.3f")
                .to_string()
        })
        .unwrap_or_else(|| "时间超界".to_owned())
}

const IVORY: Color = Color::Rgb(255, 253, 245);
const LIGHT_GRAY: Color = Color::Rgb(246, 248, 250);
const DARK_TEXT: Color = Color::Rgb(38, 45, 52);
const BORDER: Color = Color::Rgb(135, 150, 160);
const LIGHT_BLUE: Color = Color::Rgb(190, 225, 255);
const LIGHT_CYAN: Color = Color::Rgb(204, 245, 247);
const LIGHT_ORANGE: Color = Color::Rgb(255, 226, 190);
const LIGHT_PURPLE: Color = Color::Rgb(234, 218, 255);
const LIGHT_GREEN: Color = Color::Rgb(211, 244, 213);
const LIGHT_YELLOW: Color = Color::Rgb(255, 245, 184);
const LIGHT_RED: Color = Color::Rgb(255, 211, 211);

const fn base_style() -> Style {
    Style::new().fg(DARK_TEXT).bg(IVORY)
}

const fn selected_style() -> Style {
    Style::new()
        .fg(DARK_TEXT)
        .bg(LIGHT_BLUE)
        .add_modifier(Modifier::BOLD)
}

const fn header_style() -> Style {
    Style::new()
        .fg(DARK_TEXT)
        .bg(LIGHT_GRAY)
        .add_modifier(Modifier::BOLD)
}

const fn label_style() -> Style {
    Style::new()
        .fg(Color::Rgb(45, 84, 112))
        .bg(IVORY)
        .add_modifier(Modifier::BOLD)
}

const fn success_style() -> Style {
    Style::new().fg(DARK_TEXT).bg(LIGHT_GREEN)
}

const fn warning_style() -> Style {
    Style::new().fg(DARK_TEXT).bg(LIGHT_YELLOW)
}

const fn error_style() -> Style {
    Style::new().fg(DARK_TEXT).bg(LIGHT_RED)
}

const fn normal_badge_style() -> Style {
    Style::new().fg(DARK_TEXT).bg(LIGHT_CYAN)
}

fn log_style(entry: &LogEntry) -> Style {
    if entry.category == LogCategory::CommandDivider {
        return selected_style();
    }
    let background = match entry.severity {
        Severity::Error => LIGHT_RED,
        Severity::Warning => LIGHT_YELLOW,
        Severity::Success => LIGHT_GREEN,
        Severity::Normal => match entry.category {
            LogCategory::ActiveUpload => LIGHT_PURPLE,
            _ => match entry.direction {
                Direction::Incoming => LIGHT_CYAN,
                Direction::Outgoing => LIGHT_ORANGE,
                Direction::Internal => LIGHT_GRAY,
            },
        },
    };
    Style::new().fg(DARK_TEXT).bg(background)
}

fn light_block(title: &str) -> Block<'_> {
    Block::default()
        .title(title)
        .borders(Borders::ALL)
        .border_style(Style::default().fg(BORDER).bg(IVORY))
        .style(base_style())
}

#[cfg(test)]
mod tests {
    use std::{collections::VecDeque, sync::Arc};

    use ratatui::{Terminal, backend::TestBackend};

    use super::*;
    use crate::model::{ConnectionPhase, ConnectionView};

    fn point_views(prefix: &str, count: usize) -> Vec<PointView> {
        (0..count)
            .map(|index| PointView {
                ioa: 10_000 + index as u32,
                name: format!("{prefix}{index:03}"),
                type_id: "M_ME_NC_1".to_owned(),
                type_name: "短浮点遥测".to_owned(),
                value: Some(format!("{index}.000000")),
                quality: "GOOD".to_owned(),
                updated_ms: Some(index as u64),
                configured: true,
            })
            .collect()
    }

    fn snapshot_with_values(
        config: &ToolConfig,
        collect_values: Vec<PointView>,
        dispatch_current_general: Vec<PointView>,
        dispatch_current_energy: Vec<PointView>,
    ) -> AppSnapshot {
        AppSnapshot {
            collect_connection: ConnectionView::new(ConnectionPhase::Listening, "collect"),
            dispatch_connection: ConnectionView::new(ConnectionPhase::Active, "dispatch"),
            collect_logs: VecDeque::new(),
            dispatch_logs: VecDeque::new(),
            system_logs: VecDeque::new(),
            collect_values,
            dispatch_general: Vec::new(),
            dispatch_energy: Vec::new(),
            dispatch_current_general,
            dispatch_current_energy,
            dispatch_points: config.dispatch_points.clone(),
            general_pending: false,
            energy_pending: false,
            application_pending: 0,
            fault_policy: None,
        }
    }

    fn wheel(kind: MouseEventKind, area: Rect) -> MouseEvent {
        MouseEvent {
            kind,
            column: area.x.saturating_add(1),
            row: area.y.saturating_add(2),
            modifiers: KeyModifiers::NONE,
        }
    }

    #[test]
    fn all_seven_tabs_render_without_dark_background() {
        let config = crate::config::load_from(crate::config::MAIN_CONFIG_PATH).expect("config");
        let snapshot = Arc::new(AppSnapshot {
            collect_connection: ConnectionView::new(ConnectionPhase::Listening, "127.0.0.1:2405"),
            dispatch_connection: ConnectionView::new(
                ConnectionPhase::Disconnected,
                "127.0.0.1:2404",
            ),
            collect_logs: VecDeque::new(),
            dispatch_logs: VecDeque::new(),
            system_logs: VecDeque::new(),
            collect_values: Vec::new(),
            dispatch_general: Vec::new(),
            dispatch_energy: Vec::new(),
            dispatch_current_general: Vec::new(),
            dispatch_current_energy: Vec::new(),
            dispatch_points: config.dispatch_points.clone(),
            general_pending: false,
            energy_pending: false,
            application_pending: 0,
            fault_policy: None,
        });
        for tab in 0..TAB_TITLES.len() {
            let mut app = UiState::new(&config);
            app.tab = tab;
            let backend = TestBackend::new(120, 42);
            let mut terminal = Terminal::new(backend).expect("terminal");
            terminal
                .draw(|frame| render(frame, &app, &snapshot))
                .expect("draw");
            for cell in terminal.backend().buffer().content() {
                assert_ne!(cell.bg, Color::Black);
            }
        }
    }

    #[test]
    fn all_tab_titles_fit_at_the_minimum_supported_width() {
        let config = crate::config::load_from(crate::config::MAIN_CONFIG_PATH).expect("config");
        let snapshot = Arc::new(AppSnapshot {
            collect_connection: ConnectionView::new(ConnectionPhase::Listening, "127.0.0.1:2405"),
            dispatch_connection: ConnectionView::new(
                ConnectionPhase::Disconnected,
                "127.0.0.1:2404",
            ),
            collect_logs: VecDeque::new(),
            dispatch_logs: VecDeque::new(),
            system_logs: VecDeque::new(),
            collect_values: Vec::new(),
            dispatch_general: Vec::new(),
            dispatch_energy: Vec::new(),
            dispatch_current_general: Vec::new(),
            dispatch_current_energy: Vec::new(),
            dispatch_points: config.dispatch_points.clone(),
            general_pending: false,
            energy_pending: false,
            application_pending: 0,
            fault_policy: None,
        });
        let app = UiState::new(&config);
        let backend = TestBackend::new(80, 24);
        let mut terminal = Terminal::new(backend).expect("terminal");
        terminal
            .draw(|frame| render(frame, &app, &snapshot))
            .expect("draw");
        let tab_text = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .take(80 * 3)
            .map(|cell| cell.symbol())
            .collect::<String>();
        let compact = tab_text
            .chars()
            .filter(|character| !character.is_whitespace())
            .collect::<String>();
        assert!(
            compact.contains("7调度值"),
            "tab bar was clipped: {tab_text}"
        );
    }

    #[test]
    fn too_small_terminal_renders_a_size_message_without_panicking() {
        let config = crate::config::load_from(crate::config::MAIN_CONFIG_PATH).expect("config");
        let snapshot = Arc::new(AppSnapshot {
            collect_connection: ConnectionView::new(ConnectionPhase::Listening, "collect"),
            dispatch_connection: ConnectionView::new(ConnectionPhase::Disconnected, "dispatch"),
            collect_logs: VecDeque::new(),
            dispatch_logs: VecDeque::new(),
            system_logs: VecDeque::new(),
            collect_values: Vec::new(),
            dispatch_general: Vec::new(),
            dispatch_energy: Vec::new(),
            dispatch_current_general: Vec::new(),
            dispatch_current_energy: Vec::new(),
            dispatch_points: config.dispatch_points.clone(),
            general_pending: false,
            energy_pending: false,
            application_pending: 0,
            fault_policy: None,
        });
        let app = UiState::new(&config);
        let backend = TestBackend::new(60, 12);
        let mut terminal = Terminal::new(backend).expect("terminal");
        terminal
            .draw(|frame| render(frame, &app, &snapshot))
            .expect("draw");
        let compact = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .flat_map(|cell| cell.symbol().chars())
            .filter(|character| !character.is_whitespace())
            .collect::<String>();
        assert!(compact.contains("终端尺寸过小"));
    }

    #[test]
    fn collect_and_dispatch_value_tabs_are_labeled_as_realtime() {
        let config = crate::config::load_from(crate::config::MAIN_CONFIG_PATH).expect("config");
        let snapshot = Arc::new(AppSnapshot {
            collect_connection: ConnectionView::new(ConnectionPhase::Listening, "collect"),
            dispatch_connection: ConnectionView::new(ConnectionPhase::Active, "dispatch"),
            collect_logs: VecDeque::new(),
            dispatch_logs: VecDeque::new(),
            system_logs: VecDeque::new(),
            collect_values: Vec::new(),
            dispatch_general: Vec::new(),
            dispatch_energy: Vec::new(),
            dispatch_current_general: Vec::new(),
            dispatch_current_energy: Vec::new(),
            dispatch_points: config.dispatch_points.clone(),
            general_pending: false,
            energy_pending: false,
            application_pending: 0,
            fault_policy: None,
        });

        for (tab, expected) in [
            (5, "采集当前值·实时刷新"),
            (6, "调度当前遥信/遥测·实时刷新"),
            (6, "调度当前电度·实时刷新"),
        ] {
            let mut app = UiState::new(&config);
            app.tab = tab;
            let backend = TestBackend::new(120, 42);
            let mut terminal = Terminal::new(backend).expect("terminal");
            terminal
                .draw(|frame| render(frame, &app, &snapshot))
                .expect("draw");
            let compact = terminal
                .backend()
                .buffer()
                .content()
                .iter()
                .flat_map(|cell| cell.symbol().chars())
                .filter(|character| !character.is_whitespace())
                .collect::<String>();
            assert!(
                compact.contains(expected),
                "tab {} did not contain {expected}: {compact}",
                tab + 1
            );
        }
    }

    #[test]
    fn value_table_offsets_follow_scroll_direction_and_clamp_to_last_full_page() {
        let area = Rect::new(0, 0, 80, 10);
        assert_eq!(point_table_visible_rows(area), 7);
        assert_eq!(scroll_point_offset(0, 10, area, true), 1);
        assert_eq!(scroll_point_offset(1, 10, area, false), 0);
        assert_eq!(scroll_point_offset(0, 10, area, false), 0);
        assert_eq!(scroll_point_offset(usize::MAX, 10, area, true), 3);
        assert_eq!(clamped_point_offset(usize::MAX, 10, area), 3);
        assert_eq!(scroll_point_offset(5, 3, area, true), 0);
    }

    #[test]
    fn mouse_wheel_scrolls_collection_and_dispatch_panes_independently() {
        let config = crate::config::load_from(crate::config::MAIN_CONFIG_PATH).expect("config");
        let snapshot = snapshot_with_values(
            &config,
            point_views("COL", 50),
            point_views("GEN", 50),
            point_views("ENE", 50),
        );
        let (commands, _receiver) = mpsc::channel(1);
        let screen = Rect::new(0, 0, 120, 42);
        let content = main_content_area(screen).expect("content");

        let mut app = UiState::new(&config);
        app.tab = 5;
        handle_mouse(
            &mut app,
            wheel(MouseEventKind::ScrollDown, content),
            &commands,
            &snapshot,
            screen,
        );
        assert_eq!(app.collect_value_offset, 1);

        app.tab = 6;
        let [general, energy] = dispatch_value_areas(content);
        handle_mouse(
            &mut app,
            wheel(MouseEventKind::ScrollDown, general),
            &commands,
            &snapshot,
            screen,
        );
        assert_eq!(app.dispatch_general_offset, 1);
        assert_eq!(app.dispatch_energy_offset, 0);

        handle_mouse(
            &mut app,
            wheel(MouseEventKind::ScrollDown, energy),
            &commands,
            &snapshot,
            screen,
        );
        assert_eq!(app.dispatch_general_offset, 1);
        assert_eq!(app.dispatch_energy_offset, 1);

        handle_mouse(
            &mut app,
            wheel(MouseEventKind::ScrollUp, general),
            &commands,
            &snapshot,
            screen,
        );
        assert_eq!(app.dispatch_general_offset, 0);
        assert_eq!(app.dispatch_energy_offset, 1);
    }

    #[test]
    fn collection_render_and_click_use_the_same_scrolled_offset() {
        let config = crate::config::load_from(crate::config::MAIN_CONFIG_PATH).expect("config");
        let snapshot =
            snapshot_with_values(&config, point_views("ROW", 50), Vec::new(), Vec::new());
        let screen = Rect::new(0, 0, 80, 24);
        let content = main_content_area(screen).expect("content");
        let mut app = UiState::new(&config);
        app.tab = 5;
        app.collect_value_offset = 10;

        let backend = TestBackend::new(screen.width, screen.height);
        let mut terminal = Terminal::new(backend).expect("terminal");
        terminal
            .draw(|frame| render(frame, &app, &snapshot))
            .expect("draw");
        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .flat_map(|cell| cell.symbol().chars())
            .collect::<String>();
        assert!(
            rendered.contains("ROW010"),
            "scrolled row missing: {rendered}"
        );
        assert!(
            !rendered.contains("ROW000"),
            "row before offset remained visible: {rendered}"
        );

        let (commands, _receiver) = mpsc::channel(1);
        handle_left_click(
            &mut app,
            content.x.saturating_add(1),
            content.y.saturating_add(2),
            &commands,
            &snapshot,
            screen,
        );
        let popup = app.collect_value_popup.as_ref().expect("value popup");
        assert_eq!(popup.ioa, snapshot.collect_values[10].ioa);
        assert_eq!(popup.name, "ROW010");
    }

    #[test]
    fn dispatch_form_rejects_qualifiers_that_would_be_truncated_on_the_wire() {
        let config = crate::config::load_from(crate::config::MAIN_CONFIG_PATH).expect("config");
        let mut form = DispatchForm::new(&config);
        form.qualifier = "32".to_owned();
        assert!(form.request(DispatchAction::SingleControl).is_err());
        form.qualifier = "128".to_owned();
        assert!(form.request(DispatchAction::FloatSetpoint).is_err());
        form.qualifier = "0".to_owned();
        form.qoi = "19".to_owned();
        assert!(form.request(DispatchAction::GeneralInterrogation).is_err());
        form.qoi = "20".to_owned();
        form.qcc_request = "0".to_owned();
        assert!(form.request(DispatchAction::CounterInterrogation).is_err());
        form.qcc_request = "5".to_owned();
        form.qcc_freeze = "4".to_owned();
        assert!(form.request(DispatchAction::CounterInterrogation).is_err());
    }

    #[test]
    fn fault_popup_has_separate_force_success_and_clear_choices() {
        assert!(FaultPolicy::ALL.contains(&FaultPolicy::Success));
        assert!(FaultPolicy::Success.matches(ControlPhase::Select));
        assert!(FaultPolicy::Success.matches(ControlPhase::Execute));
    }
}
