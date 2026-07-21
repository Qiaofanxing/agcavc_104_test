use std::{collections::HashSet, fs, path::Path, time::Duration};

use iec104::config::ProtocolConfig;
use serde::Deserialize;

use crate::model::{
    AddressingMode, CollectPointKind, ControlKind, DefaultResponseMode, DispatchPointKind,
    DispatchPointPurpose, PointPurpose,
};

pub type ConfigResult<T> = Result<T, String>;

pub const MAIN_CONFIG_PATH: &str = "config/agcavc104test.toml";
pub const DEFAULT_COLLECT_POINTS_PATH: &str = "config/collect-points.toml";
pub const DEFAULT_DISPATCH_POINTS_PATH: &str = "config/dispatch-points.toml";

#[derive(Debug, Clone)]
pub struct ToolConfig {
    pub protocol: ProtocolSettings,
    pub collect: CollectSettings,
    pub dispatch: DispatchSettings,
    pub ui: UiSettings,
    pub collect_points: Vec<CollectPointConfig>,
    pub dispatch_points: Vec<DispatchPointConfig>,
}

#[derive(Debug, Clone)]
pub struct ProtocolSettings {
    pub t0: Duration,
    pub t1: Duration,
    pub t2: Duration,
    pub t3: Duration,
    pub k: u16,
    pub w: u16,
    pub max_pending_outgoing_asdu: u32,
    pub originator_address: u8,
}

impl ProtocolSettings {
    pub fn to_iec104(&self) -> ProtocolConfig {
        ProtocolConfig {
            t0: self.t0,
            t1: self.t1,
            t2: self.t2,
            t3: self.t3,
            k: self.k,
            w: self.w,
            max_pending_outgoing_asdu: self.max_pending_outgoing_asdu,
            originator_address: self.originator_address,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CollectSettings {
    pub bind_host: String,
    pub bind_port: u16,
    pub common_address: u16,
    pub fault_delay: Duration,
}

#[derive(Debug, Clone)]
pub struct DispatchSettings {
    pub target_host: String,
    pub target_port: u16,
    pub common_address: u16,
    pub reconnect: Duration,
}

#[derive(Debug, Clone)]
pub struct UiSettings {
    pub tick: Duration,
    pub log_capacity: usize,
    pub command_capacity: usize,
    pub event_capacity: usize,
}

#[derive(Debug, Clone)]
pub struct CollectPointConfig {
    pub name: String,
    pub ioa: u32,
    pub kind: CollectPointKind,
    pub purpose: PointPurpose,
    pub control: Option<ControlKind>,
    pub group: u8,
    pub initial: f64,
    pub min: f64,
    pub max: f64,
    pub step: f64,
    pub soe: bool,
    pub quality: String,
    pub addressing: AddressingMode,
    pub response: DefaultResponseMode,
    pub response_delay: Duration,
}

#[derive(Debug, Clone)]
pub struct DispatchPointConfig {
    pub name: String,
    pub ioa: u32,
    pub kind: DispatchPointKind,
    pub purpose: DispatchPointPurpose,
    pub min: Option<f64>,
    pub max: Option<f64>,
    pub default_value: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct MainFile {
    #[serde(default)]
    protocol: ProtocolFile,
    #[serde(default)]
    collect: CollectFile,
    #[serde(default)]
    dispatch: DispatchFile,
    #[serde(default)]
    ui: UiFile,
    #[serde(default)]
    points: PointPathsFile,
}

#[derive(Debug, Deserialize)]
struct ProtocolFile {
    #[serde(default = "default_t0_ms")]
    t0_ms: u64,
    #[serde(default = "default_t1_ms")]
    t1_ms: u64,
    #[serde(default = "default_t2_ms")]
    t2_ms: u64,
    #[serde(default = "default_t3_ms")]
    t3_ms: u64,
    #[serde(default = "default_k")]
    k: u16,
    #[serde(default = "default_w")]
    w: u16,
    #[serde(default = "default_pending")]
    max_pending_outgoing_asdu: u32,
    #[serde(default)]
    originator_address: u8,
}

impl Default for ProtocolFile {
    fn default() -> Self {
        Self {
            t0_ms: default_t0_ms(),
            t1_ms: default_t1_ms(),
            t2_ms: default_t2_ms(),
            t3_ms: default_t3_ms(),
            k: default_k(),
            w: default_w(),
            max_pending_outgoing_asdu: default_pending(),
            originator_address: 0,
        }
    }
}

#[derive(Debug, Deserialize)]
struct CollectFile {
    #[serde(default = "default_host")]
    bind_host: String,
    #[serde(default = "default_collect_port")]
    bind_port: u16,
    #[serde(default = "default_common_address")]
    common_address: u16,
    #[serde(default = "default_fault_delay_ms")]
    fault_delay_ms: u64,
}

impl Default for CollectFile {
    fn default() -> Self {
        Self {
            bind_host: default_host(),
            bind_port: default_collect_port(),
            common_address: default_common_address(),
            fault_delay_ms: default_fault_delay_ms(),
        }
    }
}

#[derive(Debug, Deserialize)]
struct DispatchFile {
    #[serde(default = "default_host")]
    target_host: String,
    #[serde(default = "default_dispatch_port")]
    target_port: u16,
    #[serde(default = "default_common_address")]
    common_address: u16,
    #[serde(default = "default_reconnect_ms")]
    reconnect_ms: u64,
}

impl Default for DispatchFile {
    fn default() -> Self {
        Self {
            target_host: default_host(),
            target_port: default_dispatch_port(),
            common_address: default_common_address(),
            reconnect_ms: default_reconnect_ms(),
        }
    }
}

#[derive(Debug, Deserialize)]
struct UiFile {
    #[serde(default = "default_tick_ms")]
    tick_ms: u64,
    #[serde(default = "default_log_capacity")]
    log_capacity: usize,
    #[serde(default = "default_command_capacity")]
    command_capacity: usize,
    #[serde(default = "default_event_capacity")]
    event_capacity: usize,
}

impl Default for UiFile {
    fn default() -> Self {
        Self {
            tick_ms: default_tick_ms(),
            log_capacity: default_log_capacity(),
            command_capacity: default_command_capacity(),
            event_capacity: default_event_capacity(),
        }
    }
}

#[derive(Debug, Deserialize)]
struct PointPathsFile {
    #[serde(default = "default_collect_points_path")]
    collect: String,
    #[serde(default = "default_dispatch_points_path")]
    dispatch: String,
}

impl Default for PointPathsFile {
    fn default() -> Self {
        Self {
            collect: default_collect_points_path(),
            dispatch: default_dispatch_points_path(),
        }
    }
}

#[derive(Debug, Deserialize)]
struct CollectPointFile {
    #[serde(default)]
    points: Vec<CollectPointRow>,
}

#[derive(Debug, Deserialize)]
struct CollectPointRow {
    name: String,
    ioa: u32,
    #[serde(rename = "type")]
    kind: CollectPointKind,
    #[serde(default)]
    purpose: PointPurpose,
    #[serde(default)]
    control: Option<ControlKind>,
    #[serde(default = "default_group")]
    group: u8,
    initial: f64,
    min: Option<f64>,
    max: Option<f64>,
    step: Option<f64>,
    #[serde(default)]
    soe: bool,
    #[serde(default = "default_quality")]
    quality: String,
    #[serde(default)]
    addressing: AddressingMode,
    #[serde(default)]
    response: DefaultResponseMode,
    #[serde(default)]
    response_delay_ms: u64,
}

#[derive(Debug, Deserialize)]
struct DispatchPointFile {
    #[serde(default)]
    points: Vec<DispatchPointRow>,
}

#[derive(Debug, Deserialize)]
struct DispatchPointRow {
    name: String,
    ioa: u32,
    #[serde(rename = "type")]
    kind: DispatchPointKind,
    #[serde(default)]
    purpose: DispatchPointPurpose,
    min: Option<f64>,
    max: Option<f64>,
    default_value: Option<f64>,
}

pub fn load() -> ConfigResult<ToolConfig> {
    reject_cli_args()?;
    load_from(MAIN_CONFIG_PATH)
}

pub fn load_from(path: impl AsRef<Path>) -> ConfigResult<ToolConfig> {
    let text = fs::read_to_string(path.as_ref())
        .map_err(|error| format!("读取主配置 {} 失败: {error}", path.as_ref().display()))?;
    let file: MainFile = toml::from_str(&text)
        .map_err(|error| format!("解析主配置 {} 失败: {error}", path.as_ref().display()))?;

    let protocol = ProtocolSettings {
        t0: Duration::from_millis(file.protocol.t0_ms),
        t1: Duration::from_millis(file.protocol.t1_ms),
        t2: Duration::from_millis(file.protocol.t2_ms),
        t3: Duration::from_millis(file.protocol.t3_ms),
        k: file.protocol.k,
        w: file.protocol.w,
        max_pending_outgoing_asdu: file.protocol.max_pending_outgoing_asdu,
        originator_address: file.protocol.originator_address,
    };
    validate_protocol(&protocol)?;

    let collect = CollectSettings {
        bind_host: non_empty("collect.bind_host", file.collect.bind_host)?,
        bind_port: non_zero_port("collect.bind_port", file.collect.bind_port)?,
        common_address: validate_common_address(
            "collect.common_address",
            file.collect.common_address,
        )?,
        fault_delay: Duration::from_millis(file.collect.fault_delay_ms),
    };
    let dispatch = DispatchSettings {
        target_host: non_empty("dispatch.target_host", file.dispatch.target_host)?,
        target_port: non_zero_port("dispatch.target_port", file.dispatch.target_port)?,
        common_address: validate_common_address(
            "dispatch.common_address",
            file.dispatch.common_address,
        )?,
        reconnect: positive_duration("dispatch.reconnect_ms", file.dispatch.reconnect_ms)?,
    };
    let ui = UiSettings {
        tick: positive_duration("ui.tick_ms", file.ui.tick_ms)?,
        log_capacity: positive_usize("ui.log_capacity", file.ui.log_capacity)?,
        command_capacity: positive_usize("ui.command_capacity", file.ui.command_capacity)?,
        event_capacity: positive_usize("ui.event_capacity", file.ui.event_capacity)?,
    };
    let collect_path = non_empty("points.collect", file.points.collect)?;
    let dispatch_path = non_empty("points.dispatch", file.points.dispatch)?;
    let collect_points = load_collect_points(&collect_path)?;
    let dispatch_points = load_dispatch_points(&dispatch_path)?;

    Ok(ToolConfig {
        protocol,
        collect,
        dispatch,
        ui,
        collect_points,
        dispatch_points,
    })
}

pub fn load_collect_points(path: impl AsRef<Path>) -> ConfigResult<Vec<CollectPointConfig>> {
    let text = fs::read_to_string(path.as_ref())
        .map_err(|error| format!("读取采集点配置 {} 失败: {error}", path.as_ref().display()))?;
    let file: CollectPointFile = toml::from_str(&text)
        .map_err(|error| format!("解析采集点配置 {} 失败: {error}", path.as_ref().display()))?;
    if file.points.is_empty() {
        return Err("采集点配置至少需要一个 [[points]]".to_owned());
    }
    let mut identities = HashSet::new();
    let mut control_ioas = HashSet::new();
    file.points
        .into_iter()
        .enumerate()
        .map(|(index, row)| {
            let label = format!("采集点 #{} ({})", index + 1, row.name);
            validate_name_ioa(&label, &row.name, row.ioa)?;
            if !(1..=16).contains(&row.group) {
                return Err(format!("{label} 的 group 必须在 1..=16"));
            }
            if row.kind == CollectPointKind::Counter && row.group > 4 {
                return Err(format!("{label} 的电度 group 必须在 1..=4"));
            }
            if !identities.insert((row.kind, row.ioa)) {
                return Err(format!("{label} 与已有点重复 type/IOA"));
            }
            let (default_min, default_max, default_step) = defaults_for_collect(row.kind);
            let min = row.min.unwrap_or(default_min);
            let max = row.max.unwrap_or(default_max);
            let step = row.step.unwrap_or(default_step);
            validate_collect_range(&label, row.kind, row.initial, min, max, step)?;
            let control = row.control.or_else(|| infer_control(row.kind, row.purpose));
            if control.is_some() && !row.purpose.includes_control() {
                return Err(format!("{label} 配置了 control，但 purpose 不包含控制目标"));
            }
            if row.purpose.includes_control() && control.is_none() {
                return Err(format!(
                    "{label} 是控制目标，但无法从 type 推断 control 类型"
                ));
            }
            if row.purpose.includes_control() && !control_ioas.insert(row.ioa) {
                return Err(format!("控制目标 IOA={} 必须唯一", row.ioa));
            }
            let quality = row.quality.trim().to_ascii_lowercase();
            if !matches!(
                quality.as_str(),
                "good" | "invalid" | "blocked" | "substituted"
            ) {
                return Err(format!(
                    "{label} 的 quality={quality:?} 不支持；可用 good/invalid/blocked/substituted"
                ));
            }
            Ok(CollectPointConfig {
                name: row.name,
                ioa: row.ioa,
                kind: row.kind,
                purpose: row.purpose,
                control,
                group: row.group,
                initial: row.initial,
                min,
                max,
                step,
                soe: row.soe,
                quality,
                addressing: row.addressing,
                response: row.response,
                response_delay: Duration::from_millis(row.response_delay_ms),
            })
        })
        .collect()
}

pub fn load_dispatch_points(path: impl AsRef<Path>) -> ConfigResult<Vec<DispatchPointConfig>> {
    let text = fs::read_to_string(path.as_ref())
        .map_err(|error| format!("读取调度点配置 {} 失败: {error}", path.as_ref().display()))?;
    let file: DispatchPointFile = toml::from_str(&text)
        .map_err(|error| format!("解析调度点配置 {} 失败: {error}", path.as_ref().display()))?;
    let mut identities = HashSet::new();
    file.points
        .into_iter()
        .enumerate()
        .map(|(index, row)| {
            let label = format!("调度点 #{} ({})", index + 1, row.name);
            validate_name_ioa(&label, &row.name, row.ioa)?;
            if !identities.insert((row.purpose, row.kind, row.ioa)) {
                return Err(format!("{label} 与已有点重复 purpose/type/IOA"));
            }
            if row.purpose == DispatchPointPurpose::Command && !row.kind.is_command() {
                return Err(format!("{label} purpose=command 需要使用控制/遥调类型"));
            }
            if row.purpose != DispatchPointPurpose::Command && row.kind.is_command() {
                return Err(format!("{label} 预期实时值不能使用命令类型"));
            }
            if let (Some(min), Some(max)) = (row.min, row.max)
                && (!min.is_finite() || !max.is_finite() || min > max)
            {
                return Err(format!("{label} 的 min/max 非法"));
            }
            if row.default_value.is_some_and(|value| !value.is_finite()) {
                return Err(format!("{label} 的 default_value 必须是有限数字"));
            }
            if let Some(value) = row.default_value {
                if row.min.is_some_and(|min| value < min) || row.max.is_some_and(|max| value > max)
                {
                    return Err(format!("{label} 的 default_value 超出 min/max"));
                }
                validate_dispatch_default(&label, row.kind, value)?;
            }
            Ok(DispatchPointConfig {
                name: row.name,
                ioa: row.ioa,
                kind: row.kind,
                purpose: row.purpose,
                min: row.min,
                max: row.max,
                default_value: row.default_value,
            })
        })
        .collect()
}

fn reject_cli_args() -> ConfigResult<()> {
    if std::env::args_os().nth(1).is_some() {
        return Err("本工具不接受命令行参数，请修改 config/agcavc104test.toml".to_owned());
    }
    Ok(())
}

fn validate_protocol(protocol: &ProtocolSettings) -> ConfigResult<()> {
    if protocol.t0.is_zero()
        || protocol.t1.is_zero()
        || protocol.t2.is_zero()
        || protocol.t3.is_zero()
    {
        return Err("protocol T0/T1/T2/T3 必须大于 0".to_owned());
    }
    if !(protocol.t2 < protocol.t1 && protocol.t1 < protocol.t3) {
        return Err("protocol 必须满足 T2 < T1 < T3".to_owned());
    }
    protocol
        .to_iec104()
        .validate()
        .map_err(|error| format!("protocol K/W 非法: {error}"))
}

fn validate_collect_range(
    label: &str,
    kind: CollectPointKind,
    initial: f64,
    min: f64,
    max: f64,
    step: f64,
) -> ConfigResult<()> {
    if !initial.is_finite() || !min.is_finite() || !max.is_finite() || !step.is_finite() {
        return Err(format!("{label} 的 initial/min/max/step 必须是有限数字"));
    }
    if min > max || initial < min || initial > max || step <= 0.0 {
        return Err(format!(
            "{label} 需要满足 min <= initial <= max 且 step > 0"
        ));
    }
    match kind {
        CollectPointKind::SinglePoint | CollectPointKind::DoublePoint => {
            if !matches!(initial, 0.0 | 1.0) {
                return Err(format!("{label} 的离散初值只能为 0 或 1"));
            }
        }
        CollectPointKind::Normalized | CollectPointKind::NormalizedNoQuality => {
            for (field, value) in [
                ("initial", initial),
                ("min", min),
                ("max", max),
                ("step", step),
            ] {
                if value.fract() != 0.0 || value < i16::MIN as f64 || value > i16::MAX as f64 {
                    return Err(format!("{label} 的 {field} 必须是 i16 范围整数"));
                }
            }
        }
        CollectPointKind::Counter => {
            for (field, value) in [
                ("initial", initial),
                ("min", min),
                ("max", max),
                ("step", step),
            ] {
                if value.fract() != 0.0 || value < i32::MIN as f64 || value > i32::MAX as f64 {
                    return Err(format!("{label} 的 {field} 必须是 i32 范围整数"));
                }
            }
        }
        CollectPointKind::Float => {}
    }
    Ok(())
}

fn defaults_for_collect(kind: CollectPointKind) -> (f64, f64, f64) {
    match kind {
        CollectPointKind::SinglePoint | CollectPointKind::DoublePoint => (0.0, 1.0, 1.0),
        CollectPointKind::Normalized | CollectPointKind::NormalizedNoQuality => {
            (-32768.0, 32767.0, 1.0)
        }
        CollectPointKind::Float => (-1_000_000.0, 1_000_000.0, 0.5),
        CollectPointKind::Counter => (i32::MIN as f64, i32::MAX as f64, 1.0),
    }
}

fn infer_control(kind: CollectPointKind, purpose: PointPurpose) -> Option<ControlKind> {
    if !purpose.includes_control() {
        return None;
    }
    match kind {
        CollectPointKind::SinglePoint => Some(ControlKind::Single),
        CollectPointKind::DoublePoint => Some(ControlKind::Double),
        CollectPointKind::Float => Some(ControlKind::Setpoint),
        CollectPointKind::Normalized => Some(ControlKind::Regulating),
        CollectPointKind::NormalizedNoQuality | CollectPointKind::Counter => None,
    }
}

fn validate_dispatch_default(label: &str, kind: DispatchPointKind, value: f64) -> ConfigResult<()> {
    match kind {
        DispatchPointKind::SingleControl | DispatchPointKind::DoubleControl => {
            if !matches!(value, 0.0 | 1.0) {
                return Err(format!("{label} 的离散控制默认值只能为 0 或 1"));
            }
        }
        DispatchPointKind::RegulatingStep => {
            if !matches!(value, -1.0 | 1.0) {
                return Err(format!("{label} 的升降默认值只能为 -1 或 1"));
            }
        }
        DispatchPointKind::NormalizedSetpoint | DispatchPointKind::ScaledSetpoint => {
            if value.fract() != 0.0 || value < i16::MIN as f64 || value > i16::MAX as f64 {
                return Err(format!("{label} 的设点默认值必须是 i16 范围整数"));
            }
        }
        DispatchPointKind::SinglePoint
        | DispatchPointKind::DoublePoint
        | DispatchPointKind::Float
        | DispatchPointKind::Counter
        | DispatchPointKind::FloatSetpoint => {}
    }
    Ok(())
}

fn validate_name_ioa(label: &str, name: &str, ioa: u32) -> ConfigResult<()> {
    if name.trim().is_empty() {
        return Err(format!("{label} 的 name 不能为空"));
    }
    if ioa > 0xFF_FFFF {
        return Err(format!("{label} 的 IOA={ioa} 超出三字节范围"));
    }
    Ok(())
}

fn non_empty(field: &str, value: String) -> ConfigResult<String> {
    if value.trim().is_empty() {
        Err(format!("{field} 不能为空"))
    } else {
        Ok(value)
    }
}

fn non_zero_port(field: &str, value: u16) -> ConfigResult<u16> {
    if value == 0 {
        Err(format!("{field} 不能为 0"))
    } else {
        Ok(value)
    }
}

fn validate_common_address(field: &str, value: u16) -> ConfigResult<u16> {
    if value == 0 || value == u16::MAX {
        Err(format!("{field} 必须是 1..=65534 的具体站地址"))
    } else {
        Ok(value)
    }
}

fn positive_duration(field: &str, millis: u64) -> ConfigResult<Duration> {
    if millis == 0 {
        Err(format!("{field} 必须大于 0"))
    } else {
        Ok(Duration::from_millis(millis))
    }
}

fn positive_usize(field: &str, value: usize) -> ConfigResult<usize> {
    if value == 0 {
        Err(format!("{field} 必须大于 0"))
    } else {
        Ok(value)
    }
}

const fn default_t0_ms() -> u64 {
    10_000
}
const fn default_t1_ms() -> u64 {
    15_000
}
const fn default_t2_ms() -> u64 {
    10_000
}
const fn default_t3_ms() -> u64 {
    20_000
}
const fn default_k() -> u16 {
    12
}
const fn default_w() -> u16 {
    8
}
const fn default_pending() -> u32 {
    1024
}
fn default_host() -> String {
    "127.0.0.1".to_owned()
}
const fn default_collect_port() -> u16 {
    2405
}
const fn default_dispatch_port() -> u16 {
    2404
}
const fn default_common_address() -> u16 {
    1
}
const fn default_fault_delay_ms() -> u64 {
    2_000
}
const fn default_reconnect_ms() -> u64 {
    3_000
}
const fn default_tick_ms() -> u64 {
    100
}
const fn default_log_capacity() -> usize {
    2_000
}
const fn default_command_capacity() -> usize {
    256
}
const fn default_event_capacity() -> usize {
    4_096
}
fn default_collect_points_path() -> String {
    DEFAULT_COLLECT_POINTS_PATH.to_owned()
}
fn default_dispatch_points_path() -> String {
    DEFAULT_DISPATCH_POINTS_PATH.to_owned()
}
const fn default_group() -> u8 {
    1
}
fn default_quality() -> String {
    "good".to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shipped_configuration_loads_and_covers_both_point_files() {
        let config = load_from(MAIN_CONFIG_PATH).expect("shipped config");
        assert!(!config.collect_points.is_empty());
        assert!(!config.dispatch_points.is_empty());
        assert!(config.protocol.t2 < config.protocol.t1);
        assert!(config.protocol.t1 < config.protocol.t3);
    }

    #[test]
    fn protocol_timer_order_and_common_addresses_are_validated() {
        let mut protocol = load_from(MAIN_CONFIG_PATH).expect("config").protocol;
        protocol.t2 = protocol.t1;
        assert!(validate_protocol(&protocol).is_err());
        assert!(validate_common_address("CA", 0).is_err());
        assert!(validate_common_address("CA", u16::MAX).is_err());
    }

    #[test]
    fn point_value_boundaries_reject_invalid_discrete_and_setpoint_defaults() {
        assert!(
            validate_collect_range("single", CollectPointKind::SinglePoint, 2.0, 0.0, 2.0, 1.0,)
                .is_err()
        );
        assert!(
            validate_dispatch_default("single", DispatchPointKind::SingleControl, 2.0).is_err()
        );
        assert!(
            validate_dispatch_default(
                "normalized",
                DispatchPointKind::NormalizedSetpoint,
                32_768.0,
            )
            .is_err()
        );
    }
}
