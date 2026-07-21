use std::{collections::VecDeque, time::SystemTime};

use serde::Deserialize;

use crate::config::{CollectPointConfig, DispatchPointConfig};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CollectPointKind {
    SinglePoint,
    DoublePoint,
    Normalized,
    NormalizedNoQuality,
    Float,
    Counter,
}

impl CollectPointKind {
    pub const fn label(self) -> &'static str {
        match self {
            Self::SinglePoint => "单点",
            Self::DoublePoint => "双点",
            Self::Normalized => "归一化遥测",
            Self::NormalizedNoQuality => "无品质遥测",
            Self::Float => "短浮点遥测",
            Self::Counter => "电度",
        }
    }

    pub const fn type_id(self) -> &'static str {
        match self {
            Self::SinglePoint => "M_SP_NA_1",
            Self::DoublePoint => "M_DP_NA_1",
            Self::Normalized => "M_ME_NA_1",
            Self::NormalizedNoQuality => "M_ME_ND_1",
            Self::Float => "M_ME_NC_1",
            Self::Counter => "M_IT_NA_1",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum PointPurpose {
    #[default]
    Data,
    ControlTarget,
    Both,
}

impl PointPurpose {
    pub const fn includes_data(self) -> bool {
        matches!(self, Self::Data | Self::Both)
    }

    pub const fn includes_control(self) -> bool {
        matches!(self, Self::ControlTarget | Self::Both)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ControlKind {
    Single,
    Double,
    Setpoint,
    Regulating,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum AddressingMode {
    #[default]
    Individual,
    Sequence,
    Auto,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum DefaultResponseMode {
    #[default]
    Success,
    Reject,
    ActConOnly,
    Silent,
    DelaySuccess,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DispatchPointKind {
    SinglePoint,
    DoublePoint,
    Float,
    Counter,
    SingleControl,
    DoubleControl,
    RegulatingStep,
    NormalizedSetpoint,
    ScaledSetpoint,
    FloatSetpoint,
}

impl DispatchPointKind {
    pub const fn label(self) -> &'static str {
        match self {
            Self::SinglePoint => "单点",
            Self::DoublePoint => "双点",
            Self::Float => "遥测",
            Self::Counter => "电度",
            Self::SingleControl => "单点遥控",
            Self::DoubleControl => "双点遥控",
            Self::RegulatingStep => "升降控制",
            Self::NormalizedSetpoint => "归一化遥调",
            Self::ScaledSetpoint => "标度化遥调",
            Self::FloatSetpoint => "短浮点遥调",
        }
    }

    pub const fn is_command(self) -> bool {
        matches!(
            self,
            Self::SingleControl
                | Self::DoubleControl
                | Self::RegulatingStep
                | Self::NormalizedSetpoint
                | Self::ScaledSetpoint
                | Self::FloatSetpoint
        )
    }

    pub const fn type_id(self) -> &'static str {
        match self {
            Self::SinglePoint => "M_SP_NA_1",
            Self::DoublePoint => "M_DP_NA_1",
            Self::Float => "M_ME_NC_1",
            Self::Counter => "M_IT_NA_1",
            Self::SingleControl => "C_SC_NA_1",
            Self::DoubleControl => "C_DC_NA_1",
            Self::RegulatingStep => "C_RC_NA_1",
            Self::NormalizedSetpoint => "C_SE_NA_1",
            Self::ScaledSetpoint => "C_SE_NB_1",
            Self::FloatSetpoint => "C_SE_NC_1",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum DispatchPointPurpose {
    ExpectedGeneral,
    ExpectedEnergy,
    #[default]
    Command,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Collect,
    Dispatch,
    System,
}

impl Side {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Collect => "采集侧",
            Self::Dispatch => "调度侧",
            Self::System => "系统",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Incoming,
    Outgoing,
    Internal,
}

impl Direction {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Incoming => "接收",
            Self::Outgoing => "发送",
            Self::Internal => "状态",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Severity {
    Normal,
    Success,
    Warning,
    Error,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogCategory {
    Connection,
    Protocol,
    ActiveUpload,
    CommandDivider,
    Interrogation,
    Control,
    Configuration,
    KeepAlive,
}

#[derive(Debug, Clone)]
pub struct LogEntry {
    pub timestamp_ms: u64,
    pub side: Side,
    pub direction: Direction,
    pub severity: Severity,
    pub category: LogCategory,
    pub summary: String,
    pub details: Vec<String>,
    pub protocol: Option<ProtocolMeta>,
}

#[derive(Debug, Clone)]
pub struct ProtocolMeta {
    pub type_id: String,
    pub cot: String,
    pub common_address: u16,
    pub originator_address: u8,
    pub sequence: bool,
    pub test: bool,
    pub negative: bool,
    pub ioas: Vec<u32>,
}

impl LogEntry {
    pub fn new(
        side: Side,
        direction: Direction,
        severity: Severity,
        category: LogCategory,
        summary: impl Into<String>,
    ) -> Self {
        Self {
            timestamp_ms: now_millis(),
            side,
            direction,
            severity,
            category,
            summary: summary.into(),
            details: Vec::new(),
            protocol: None,
        }
    }

    pub fn with_details(mut self, details: Vec<String>) -> Self {
        self.details = details;
        self
    }

    pub fn with_protocol(mut self, protocol: Option<ProtocolMeta>) -> Self {
        self.protocol = protocol;
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionPhase {
    Disabled,
    Listening,
    Disconnected,
    Connecting,
    ConnectedStopped,
    Starting,
    Active,
    Stopping,
    ReconnectWait,
    Error,
}

impl ConnectionPhase {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Disabled => "未启动",
            Self::Listening => "监听中",
            Self::Disconnected => "未连接",
            Self::Connecting => "连接中",
            Self::ConnectedStopped => "已连接/未启动传输",
            Self::Starting => "STARTDT 等待确认",
            Self::Active => "数据传输激活",
            Self::Stopping => "STOPDT 等待确认",
            Self::ReconnectWait => "等待重连",
            Self::Error => "异常",
        }
    }
}

#[derive(Debug, Clone)]
pub struct ConnectionView {
    pub phase: ConnectionPhase,
    pub endpoint: String,
    pub peer: Option<String>,
    pub detail: String,
    pub last_rx_ms: Option<u64>,
    pub last_tx_ms: Option<u64>,
}

impl ConnectionView {
    pub fn new(phase: ConnectionPhase, endpoint: impl Into<String>) -> Self {
        Self {
            phase,
            endpoint: endpoint.into(),
            peer: None,
            detail: String::new(),
            last_rx_ms: None,
            last_tx_ms: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PointView {
    pub ioa: u32,
    pub name: String,
    pub type_id: String,
    pub type_name: String,
    pub value: Option<String>,
    pub quality: String,
    pub updated_ms: Option<u64>,
    pub configured: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotKind {
    General,
    Energy,
}

impl SnapshotKind {
    pub const fn label(self) -> &'static str {
        match self {
            Self::General => "总召",
            Self::Energy => "电度总召",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlPhase {
    Select,
    Execute,
}

impl ControlPhase {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Select => "选择",
            Self::Execute => "执行/直接执行",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultPolicy {
    Success,
    RejectSelect,
    RejectExecute,
    ActConOnly,
    Silent,
    DelaySuccess,
    Disconnect,
}

impl FaultPolicy {
    pub const ALL: [Self; 7] = [
        Self::Success,
        Self::RejectSelect,
        Self::RejectExecute,
        Self::ActConOnly,
        Self::Silent,
        Self::DelaySuccess,
        Self::Disconnect,
    ];

    pub const fn label(self) -> &'static str {
        match self {
            Self::Success => "下一条控制：强制正常成功",
            Self::RejectSelect => "下一条选择：拒绝",
            Self::RejectExecute => "下一条执行：拒绝",
            Self::ActConOnly => "下一条执行：仅 ACTCON",
            Self::Silent => "下一条控制：完全静默",
            Self::DelaySuccess => "下一条控制：延迟成功",
            Self::Disconnect => "下一条控制：立即断链",
        }
    }

    pub const fn matches(self, phase: ControlPhase) -> bool {
        match self {
            Self::Success => true,
            Self::RejectSelect => matches!(phase, ControlPhase::Select),
            Self::RejectExecute | Self::ActConOnly => matches!(phase, ControlPhase::Execute),
            Self::Silent | Self::DelaySuccess | Self::Disconnect => true,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UploadKind {
    CurrentValues,
    RefreshAll,
    SinglePoint,
    DoublePoint,
    Normalized,
    NormalizedNoQuality,
    Float,
    Counter,
    SingleSoe,
    DoubleSoe,
    InitializationEnd,
}

impl UploadKind {
    pub const ALL: [Self; 11] = [
        Self::CurrentValues,
        Self::RefreshAll,
        Self::SinglePoint,
        Self::DoublePoint,
        Self::Normalized,
        Self::NormalizedNoQuality,
        Self::Float,
        Self::Counter,
        Self::SingleSoe,
        Self::DoubleSoe,
        Self::InitializationEnd,
    ];

    pub const fn label(self) -> &'static str {
        match self {
            Self::CurrentValues => "主动上送全部当前值（不改变数据）",
            Self::RefreshAll => "全部点变化并主动上送",
            Self::SinglePoint => "全部单点遥信变化上送",
            Self::DoublePoint => "全部双点遥信变化上送",
            Self::Normalized => "全部归一化遥测变化上送",
            Self::NormalizedNoQuality => "全部无品质遥测变化上送",
            Self::Float => "全部短浮点遥测变化上送",
            Self::Counter => "全部电度变化上送",
            Self::SingleSoe => "全部单点 SOE 变化上送",
            Self::DoubleSoe => "全部双点 SOE 变化上送",
            Self::InitializationEnd => "初始化结束主动上送",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DispatchAction {
    StartDt,
    StopDt,
    TestFr,
    GeneralInterrogation,
    CounterInterrogation,
    ClockSync,
    Read,
    SingleControl,
    DoubleControl,
    RegulatingStep,
    NormalizedSetpoint,
    ScaledSetpoint,
    FloatSetpoint,
}

impl DispatchAction {
    pub const ALL: [Self; 13] = [
        Self::StartDt,
        Self::StopDt,
        Self::TestFr,
        Self::GeneralInterrogation,
        Self::CounterInterrogation,
        Self::ClockSync,
        Self::Read,
        Self::SingleControl,
        Self::DoubleControl,
        Self::RegulatingStep,
        Self::NormalizedSetpoint,
        Self::ScaledSetpoint,
        Self::FloatSetpoint,
    ];

    pub const fn label(self) -> &'static str {
        match self {
            Self::StartDt => "STARTDT",
            Self::StopDt => "STOPDT",
            Self::TestFr => "手工 TESTFR",
            Self::GeneralInterrogation => "总召 C_IC_NA_1",
            Self::CounterInterrogation => "电度总召 C_CI_NA_1",
            Self::ClockSync => "校时 C_CS_NA_1",
            Self::Read => "读命令 C_RD_NA_1",
            Self::SingleControl => "单点遥控 C_SC_NA_1",
            Self::DoubleControl => "双点遥控 C_DC_NA_1",
            Self::RegulatingStep => "升降控制 C_RC_NA_1",
            Self::NormalizedSetpoint => "归一化遥调 C_SE_NA_1",
            Self::ScaledSetpoint => "标度化遥调 C_SE_NB_1",
            Self::FloatSetpoint => "短浮点遥调 C_SE_NC_1",
        }
    }

    pub const fn uses_ioa(self) -> bool {
        matches!(
            self,
            Self::Read
                | Self::SingleControl
                | Self::DoubleControl
                | Self::RegulatingStep
                | Self::NormalizedSetpoint
                | Self::ScaledSetpoint
                | Self::FloatSetpoint
        )
    }

    pub const fn is_control(self) -> bool {
        matches!(
            self,
            Self::SingleControl
                | Self::DoubleControl
                | Self::RegulatingStep
                | Self::NormalizedSetpoint
                | Self::ScaledSetpoint
                | Self::FloatSetpoint
        )
    }
}

#[derive(Debug, Clone)]
pub struct DispatchRequest {
    pub action: DispatchAction,
    pub ioa: u32,
    pub value: f64,
    pub phase: ControlPhase,
    pub common_address: u16,
    pub originator_address: u8,
    pub qualifier: u8,
    pub test: bool,
    pub qoi: u8,
    pub qcc_request: u8,
    pub qcc_freeze: u8,
    pub repeat: u16,
    pub interval_ms: u64,
    /// 指定校时的 Unix 毫秒；None 表示在实际发送时读取当前时间。
    pub clock_time_ms: Option<u64>,
}

#[derive(Debug, Clone)]
pub enum RuntimeCommand {
    CollectUpload(UploadKind),
    SetCollectValue {
        ioa: u32,
        type_id: String,
        value: f64,
    },
    Dispatch(DispatchRequest),
    ClearLogs(Side),
    SetFaultPolicy(Option<FaultPolicy>),
    ReloadConfig,
    Quit,
}

#[derive(Debug)]
pub enum CollectCommand {
    Upload(UploadKind),
    SetPointValue {
        ioa: u32,
        type_id: String,
        value: f64,
    },
    SetFaultPolicy(Option<FaultPolicy>),
    PrepareReload {
        generation: u64,
        points: Vec<CollectPointConfig>,
        reply: tokio::sync::oneshot::Sender<Result<(), String>>,
    },
    CommitReload {
        generation: u64,
        reply: tokio::sync::oneshot::Sender<Result<(), String>>,
    },
    AbortReload(u64),
}

#[derive(Debug)]
pub enum DispatchCommand {
    Execute(DispatchRequest),
    PrepareReload {
        generation: u64,
        points: Vec<DispatchPointConfig>,
        reply: tokio::sync::oneshot::Sender<Result<(), String>>,
    },
    CommitReload {
        generation: u64,
        reply: tokio::sync::oneshot::Sender<Result<(), String>>,
    },
    AbortReload(u64),
}

#[derive(Debug, Clone)]
pub enum RuntimeEvent {
    Log(LogEntry),
    Connection {
        side: Side,
        view: ConnectionView,
    },
    CollectValues(Vec<PointView>),
    DispatchSnapshot {
        kind: SnapshotKind,
        values: Vec<PointView>,
    },
    DispatchValues {
        kind: SnapshotKind,
        values: Vec<PointView>,
    },
    RoundPending {
        kind: SnapshotKind,
        pending: bool,
    },
    ApplicationPending(usize),
    FaultPolicyChanged(Option<FaultPolicy>),
    DispatchExpectedReloaded {
        general: Vec<PointView>,
        energy: Vec<PointView>,
        points: Vec<DispatchPointConfig>,
    },
}

#[derive(Debug, Clone)]
pub struct AppSnapshot {
    pub collect_connection: ConnectionView,
    pub dispatch_connection: ConnectionView,
    pub collect_logs: VecDeque<LogEntry>,
    pub dispatch_logs: VecDeque<LogEntry>,
    pub system_logs: VecDeque<LogEntry>,
    pub collect_values: Vec<PointView>,
    pub dispatch_general: Vec<PointView>,
    pub dispatch_energy: Vec<PointView>,
    pub dispatch_current_general: Vec<PointView>,
    pub dispatch_current_energy: Vec<PointView>,
    pub dispatch_points: Vec<DispatchPointConfig>,
    pub general_pending: bool,
    pub energy_pending: bool,
    pub application_pending: usize,
    pub fault_policy: Option<FaultPolicy>,
}

pub fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis() as u64)
}
