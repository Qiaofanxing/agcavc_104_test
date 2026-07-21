use std::{collections::VecDeque, sync::Arc, time::Duration};

use tokio::{
    sync::{mpsc, oneshot, watch},
    task::JoinHandle,
    time::sleep,
};

use crate::{
    config::{MAIN_CONFIG_PATH, ToolConfig, load_from},
    model::{
        AppSnapshot, CollectCommand, ConnectionPhase, ConnectionView, Direction, DispatchCommand,
        LogCategory, LogEntry, PointView, RuntimeCommand, RuntimeEvent, Severity, Side,
        SnapshotKind,
    },
    protocol::{collect, dispatch},
};

pub struct RuntimeHandle {
    pub commands: mpsc::Sender<RuntimeCommand>,
    pub snapshots: watch::Receiver<Arc<AppSnapshot>>,
    joins: Vec<JoinHandle<()>>,
}

impl RuntimeHandle {
    pub async fn wait(self) {
        for join in self.joins {
            let _ = join.await;
        }
    }
}

pub fn start(config: ToolConfig) -> RuntimeHandle {
    let (runtime_tx, runtime_rx) = mpsc::channel(config.ui.command_capacity);
    let (collect_tx, collect_rx) = mpsc::channel(config.ui.command_capacity);
    let (dispatch_tx, dispatch_rx) = mpsc::channel(config.ui.command_capacity);
    let (event_tx, event_rx) = mpsc::channel(config.ui.event_capacity);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let initial = initial_snapshot(&config);
    let (snapshot_tx, snapshot_rx) = watch::channel(Arc::new(initial));

    let collect_join = tokio::spawn(collect::run(
        config.collect.clone(),
        config.protocol.clone(),
        config.collect_points.clone(),
        collect_rx,
        event_tx.clone(),
        shutdown_rx.clone(),
    ));
    let dispatch_join = tokio::spawn(dispatch::run(
        config.dispatch.clone(),
        config.protocol.clone(),
        config.dispatch_points.clone(),
        dispatch_rx,
        event_tx,
        shutdown_rx,
    ));
    let coordinator_join = tokio::spawn(coordinator(
        config,
        runtime_rx,
        event_rx,
        collect_tx,
        dispatch_tx,
        snapshot_tx,
        shutdown_tx,
    ));

    RuntimeHandle {
        commands: runtime_tx,
        snapshots: snapshot_rx,
        joins: vec![coordinator_join, collect_join, dispatch_join],
    }
}

fn initial_snapshot(config: &ToolConfig) -> AppSnapshot {
    AppSnapshot {
        collect_connection: ConnectionView::new(
            ConnectionPhase::Listening,
            format!("{}:{}", config.collect.bind_host, config.collect.bind_port),
        ),
        dispatch_connection: ConnectionView::new(
            ConnectionPhase::Disconnected,
            format!(
                "{}:{}",
                config.dispatch.target_host, config.dispatch.target_port
            ),
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
    }
}

async fn coordinator(
    config: ToolConfig,
    mut commands: mpsc::Receiver<RuntimeCommand>,
    mut events: mpsc::Receiver<RuntimeEvent>,
    collect_tx: mpsc::Sender<CollectCommand>,
    dispatch_tx: mpsc::Sender<DispatchCommand>,
    snapshots: watch::Sender<Arc<AppSnapshot>>,
    shutdown: watch::Sender<bool>,
) {
    let mut snapshot = snapshots.borrow().as_ref().clone();
    let capacity = config.ui.log_capacity;
    let mut events_open = true;
    let mut reload_generation = 0_u64;

    loop {
        tokio::select! {
            command = commands.recv() => {
                let Some(command) = command else {
                    let _ = shutdown.send(true);
                    break;
                };
                match command {
                    RuntimeCommand::CollectUpload(kind) => {
                        match collect_tx.try_send(CollectCommand::Upload(kind)) {
                            Ok(()) => push_command_divider(
                                &mut snapshot,
                                capacity,
                                Side::Collect,
                                kind.label(),
                            ),
                            Err(error) => push_system_error(
                                    &mut snapshot,
                                    capacity,
                                    format!("采集命令队列拒绝 {}: {error}", kind.label()),
                                ),
                        }
                    }
                    RuntimeCommand::SetCollectValue { ioa, type_id, value } => {
                        if let Err(error) = collect_tx.try_send(CollectCommand::SetPointValue {
                            ioa,
                            type_id: type_id.clone(),
                            value,
                        }) {
                            push_system_error(
                                &mut snapshot,
                                capacity,
                                format!("采集点值修改队列拒绝 {type_id}/IOA={ioa}: {error}"),
                            );
                        }
                    }
                    RuntimeCommand::Dispatch(request) => {
                        let label = request.action.label();
                        match dispatch_tx.try_send(DispatchCommand::Execute(request)) {
                            Ok(()) => push_command_divider(
                                &mut snapshot,
                                capacity,
                                Side::Dispatch,
                                label,
                            ),
                            Err(error) => push_system_error(
                                    &mut snapshot,
                                    capacity,
                                    format!("调度命令队列拒绝 {label}: {error}"),
                                ),
                        }
                    }
                    RuntimeCommand::ClearLogs(side) => {
                        match side {
                            Side::Collect => snapshot.collect_logs.clear(),
                            Side::Dispatch => snapshot.dispatch_logs.clear(),
                            Side::System => snapshot.system_logs.clear(),
                        }
                    }
                    RuntimeCommand::SetFaultPolicy(policy) => {
                        if let Err(error) = collect_tx.try_send(CollectCommand::SetFaultPolicy(policy)) {
                            push_system_error(
                                &mut snapshot,
                                capacity,
                                format!("响应策略队列拒绝请求: {error}"),
                            );
                        }
                    }
                    RuntimeCommand::ReloadConfig => {
                        reload_generation = reload_generation.wrapping_add(1).max(1);
                        if reload_points_atomically(
                            reload_generation,
                            &config,
                            &collect_tx,
                            &dispatch_tx,
                            &mut commands,
                            &mut events,
                            &mut events_open,
                            &mut snapshot,
                            &snapshots,
                            &shutdown,
                            capacity,
                        ).await {
                            break;
                        }
                    }
                    RuntimeCommand::Quit => {
                        let _ = shutdown.send(true);
                        break;
                    }
                }
                publish(&snapshots, &snapshot);
            }
            event = events.recv(), if events_open => {
                match event {
                    Some(event) => {
                        reduce(&mut snapshot, event, capacity);
                        publish(&snapshots, &snapshot);
                    }
                    None => {
                        events_open = false;
                        push_system_error(
                            &mut snapshot,
                            capacity,
                            "两侧协议事件通道已关闭".to_owned(),
                        );
                        publish(&snapshots, &snapshot);
                    }
                }
            }
        }
    }
}

fn reduce(snapshot: &mut AppSnapshot, event: RuntimeEvent, capacity: usize) {
    match event {
        RuntimeEvent::Log(entry) => match entry.side {
            Side::Collect => push_log(&mut snapshot.collect_logs, capacity, entry),
            Side::Dispatch => push_log(&mut snapshot.dispatch_logs, capacity, entry),
            Side::System => push_log(&mut snapshot.system_logs, capacity, entry),
        },
        RuntimeEvent::Connection { side, view } => match side {
            Side::Collect => snapshot.collect_connection = view,
            Side::Dispatch => snapshot.dispatch_connection = view,
            Side::System => {}
        },
        RuntimeEvent::CollectValues(values) => snapshot.collect_values = values,
        RuntimeEvent::DispatchSnapshot { kind, values } => match kind {
            SnapshotKind::General => snapshot.dispatch_general = values,
            SnapshotKind::Energy => snapshot.dispatch_energy = values,
        },
        RuntimeEvent::DispatchValues { kind, values } => match kind {
            SnapshotKind::General => {
                merge_current_values(&mut snapshot.dispatch_current_general, values);
            }
            SnapshotKind::Energy => {
                merge_current_values(&mut snapshot.dispatch_current_energy, values);
            }
        },
        RuntimeEvent::RoundPending { kind, pending } => match kind {
            SnapshotKind::General => snapshot.general_pending = pending,
            SnapshotKind::Energy => snapshot.energy_pending = pending,
        },
        RuntimeEvent::ApplicationPending(pending) => snapshot.application_pending = pending,
        RuntimeEvent::FaultPolicyChanged(policy) => snapshot.fault_policy = policy,
        RuntimeEvent::DispatchExpectedReloaded {
            general,
            energy,
            points,
        } => {
            snapshot.dispatch_current_general =
                preserve_current_values(general.clone(), &snapshot.dispatch_current_general);
            snapshot.dispatch_current_energy =
                preserve_current_values(energy.clone(), &snapshot.dispatch_current_energy);
            snapshot.dispatch_general = preserve_values(general, &snapshot.dispatch_general);
            snapshot.dispatch_energy = preserve_values(energy, &snapshot.dispatch_energy);
            snapshot.dispatch_points = points;
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ReloadWait {
    Ready,
    Failed(String),
    Shutdown,
}

#[allow(clippy::too_many_arguments)]
async fn reload_points_atomically(
    generation: u64,
    running: &ToolConfig,
    collect_tx: &mpsc::Sender<CollectCommand>,
    dispatch_tx: &mpsc::Sender<DispatchCommand>,
    commands: &mut mpsc::Receiver<RuntimeCommand>,
    events: &mut mpsc::Receiver<RuntimeEvent>,
    events_open: &mut bool,
    snapshot: &mut AppSnapshot,
    snapshots: &watch::Sender<Arc<AppSnapshot>>,
    shutdown: &watch::Sender<bool>,
    capacity: usize,
) -> bool {
    let fresh = match load_from(MAIN_CONFIG_PATH) {
        Ok(config) => config,
        Err(error) => {
            push_system_error(
                snapshot,
                capacity,
                format!("配置重载失败，保留运行中配置: {error}"),
            );
            return false;
        }
    };

    let (collect_reply_tx, collect_reply_rx) = oneshot::channel();
    let (dispatch_reply_tx, dispatch_reply_rx) = oneshot::channel();
    let (collect_permit, dispatch_permit) =
        match (collect_tx.try_reserve(), dispatch_tx.try_reserve()) {
            (Ok(collect), Ok(dispatch)) => (collect, dispatch),
            (collect, dispatch) => {
                push_system_error(
                    snapshot,
                    capacity,
                    format!(
                        "配置已校验，但无法同时预留两侧重载队列: collect={:?}, dispatch={:?}",
                        collect.err(),
                        dispatch.err()
                    ),
                );
                return false;
            }
        };
    collect_permit.send(CollectCommand::PrepareReload {
        generation,
        points: fresh.collect_points,
        reply: collect_reply_tx,
    });
    dispatch_permit.send(DispatchCommand::PrepareReload {
        generation,
        points: fresh.dispatch_points,
        reply: dispatch_reply_tx,
    });

    match wait_reload_phase(
        "准备",
        running.protocol.t0.saturating_mul(2),
        collect_reply_rx,
        dispatch_reply_rx,
        commands,
        events,
        events_open,
        snapshot,
        snapshots,
        shutdown,
        capacity,
    )
    .await
    {
        ReloadWait::Ready => {}
        ReloadWait::Failed(error) => {
            enqueue_reload_abort(generation, collect_tx, dispatch_tx);
            push_system_error(
                snapshot,
                capacity,
                format!("两侧点表重载已取消，运行中配置不变: {error}"),
            );
            return false;
        }
        ReloadWait::Shutdown => {
            enqueue_reload_abort(generation, collect_tx, dispatch_tx);
            return true;
        }
    }

    let (collect_reply_tx, collect_reply_rx) = oneshot::channel();
    let (dispatch_reply_tx, dispatch_reply_rx) = oneshot::channel();
    let (collect_commit, dispatch_commit) =
        match (collect_tx.try_reserve(), dispatch_tx.try_reserve()) {
            (Ok(collect), Ok(dispatch)) => (collect, dispatch),
            (collect, dispatch) => {
                enqueue_reload_abort(generation, collect_tx, dispatch_tx);
                push_system_error(
                    snapshot,
                    capacity,
                    format!(
                        "重载已准备，但无法同时预留提交队列，已取消: collect={:?}, dispatch={:?}",
                        collect.err(),
                        dispatch.err()
                    ),
                );
                return false;
            }
        };
    collect_commit.send(CollectCommand::CommitReload {
        generation,
        reply: collect_reply_tx,
    });
    dispatch_commit.send(DispatchCommand::CommitReload {
        generation,
        reply: dispatch_reply_tx,
    });

    match wait_reload_phase(
        "提交",
        running.protocol.t0.saturating_mul(2),
        collect_reply_rx,
        dispatch_reply_rx,
        commands,
        events,
        events_open,
        snapshot,
        snapshots,
        shutdown,
        capacity,
    )
    .await
    {
        ReloadWait::Ready => {
            push_log(
                &mut snapshot.system_logs,
                capacity,
                LogEntry::new(
                    Side::System,
                    Direction::Internal,
                    Severity::Success,
                    LogCategory::Configuration,
                    "两侧点表已通过两阶段原子重载并确认提交；新点表路径已生效，协议地址和计时参数变更需重启",
                ),
            );
            false
        }
        ReloadWait::Failed(error) => {
            push_system_error(
                snapshot,
                capacity,
                format!(
                    "点表提交未获两侧完整确认，运行状态可能分裂；已停止两侧协议任务，请检查日志并重启测试台: {error}"
                ),
            );
            publish(snapshots, snapshot);
            let _ = shutdown.send(true);
            true
        }
        ReloadWait::Shutdown => true,
    }
}

#[allow(clippy::too_many_arguments)]
async fn wait_reload_phase(
    phase: &str,
    timeout: Duration,
    mut collect_reply: oneshot::Receiver<Result<(), String>>,
    mut dispatch_reply: oneshot::Receiver<Result<(), String>>,
    commands: &mut mpsc::Receiver<RuntimeCommand>,
    events: &mut mpsc::Receiver<RuntimeEvent>,
    events_open: &mut bool,
    snapshot: &mut AppSnapshot,
    snapshots: &watch::Sender<Arc<AppSnapshot>>,
    shutdown: &watch::Sender<bool>,
    capacity: usize,
) -> ReloadWait {
    let deadline = sleep(timeout);
    tokio::pin!(deadline);
    let mut collect_result = None;
    let mut dispatch_result = None;

    while collect_result.is_none() || dispatch_result.is_none() {
        tokio::select! {
            result = &mut collect_reply, if collect_result.is_none() => {
                collect_result = Some(result.unwrap_or_else(|_| {
                    Err(format!("采集侧重载{phase}回复通道关闭"))
                }));
            }
            result = &mut dispatch_reply, if dispatch_result.is_none() => {
                dispatch_result = Some(result.unwrap_or_else(|_| {
                    Err(format!("调度侧重载{phase}回复通道关闭"))
                }));
            }
            command = commands.recv() => {
                match command {
                    None | Some(RuntimeCommand::Quit) => {
                        let _ = shutdown.send(true);
                        return ReloadWait::Shutdown;
                    }
                    Some(command) => {
                        push_log(
                            &mut snapshot.system_logs,
                            capacity,
                            LogEntry::new(
                                Side::System,
                                Direction::Internal,
                                Severity::Warning,
                                LogCategory::Configuration,
                                format!(
                                    "点表重载{phase}中，{}已本地拒绝，请稍后重试",
                                    runtime_command_label(&command)
                                ),
                            ),
                        );
                        publish(snapshots, snapshot);
                    }
                }
            }
            event = events.recv(), if *events_open => {
                match event {
                    Some(event) => reduce(snapshot, event, capacity),
                    None => {
                        *events_open = false;
                        push_system_error(
                            snapshot,
                            capacity,
                            "两侧协议事件通道已关闭".to_owned(),
                        );
                    }
                }
                publish(snapshots, snapshot);
            }
            () = &mut deadline => {
                return ReloadWait::Failed(format!(
                    "重载{phase}等待超时（{} ms）",
                    timeout.as_millis()
                ));
            }
        }
    }

    let mut failures = Vec::new();
    if let Some(Err(error)) = collect_result {
        failures.push(error);
    }
    if let Some(Err(error)) = dispatch_result {
        failures.push(error);
    }
    if failures.is_empty() {
        ReloadWait::Ready
    } else {
        ReloadWait::Failed(failures.join("；"))
    }
}

fn runtime_command_label(command: &RuntimeCommand) -> &'static str {
    match command {
        RuntimeCommand::CollectUpload(_) => "采集主动上送命令",
        RuntimeCommand::SetCollectValue { .. } => "采集点值修改命令",
        RuntimeCommand::Dispatch(_) => "调度协议命令",
        RuntimeCommand::ClearLogs(_) => "日志清屏命令",
        RuntimeCommand::SetFaultPolicy(_) => "一次性响应策略命令",
        RuntimeCommand::ReloadConfig => "重复配置重载命令",
        RuntimeCommand::Quit => "退出命令",
    }
}

fn enqueue_reload_abort(
    generation: u64,
    collect_tx: &mpsc::Sender<CollectCommand>,
    dispatch_tx: &mpsc::Sender<DispatchCommand>,
) {
    if let Err(mpsc::error::TrySendError::Full(command)) =
        collect_tx.try_send(CollectCommand::AbortReload(generation))
    {
        let sender = collect_tx.clone();
        tokio::spawn(async move {
            let _ = sender.send(command).await;
        });
    }
    if let Err(mpsc::error::TrySendError::Full(command)) =
        dispatch_tx.try_send(DispatchCommand::AbortReload(generation))
    {
        let sender = dispatch_tx.clone();
        tokio::spawn(async move {
            let _ = sender.send(command).await;
        });
    }
}

fn preserve_values(mut fresh: Vec<PointView>, old: &[PointView]) -> Vec<PointView> {
    for row in &mut fresh {
        if let Some(previous) = old
            .iter()
            .find(|previous| previous.ioa == row.ioa && previous.type_id == row.type_id)
        {
            row.value.clone_from(&previous.value);
            row.quality.clone_from(&previous.quality);
            row.updated_ms = previous.updated_ms;
        }
    }
    fresh
}

fn preserve_current_values(mut fresh: Vec<PointView>, old: &[PointView]) -> Vec<PointView> {
    for row in &mut fresh {
        if let Some(previous) = old.iter().find(|previous| {
            previous.ioa == row.ioa && current_types_compatible(&previous.type_id, &row.type_id)
        }) {
            row.value.clone_from(&previous.value);
            row.quality.clone_from(&previous.quality);
            row.updated_ms = previous.updated_ms;
        }
    }
    fresh
}

fn merge_current_values(current: &mut Vec<PointView>, values: Vec<PointView>) {
    for mut value in values {
        if let Some(existing) = current.iter_mut().find(|row| {
            row.ioa == value.ioa && current_types_compatible(&row.type_id, &value.type_id)
        }) {
            existing.value = value.value.take();
            existing.quality = value.quality;
            existing.updated_ms = value.updated_ms;
            continue;
        }

        value.name = if current.iter().any(|row| row.ioa == value.ioa) {
            format!("类型不匹配（实际 {}）", value.type_id)
        } else {
            "未配置 IOA".to_owned()
        };
        value.configured = false;
        current.push(value);
    }
    current.sort_by(|left, right| {
        left.ioa
            .cmp(&right.ioa)
            .then_with(|| right.configured.cmp(&left.configured))
            .then_with(|| left.type_id.cmp(&right.type_id))
    });
}

fn current_types_compatible(left: &str, right: &str) -> bool {
    left == right
        || matches!(
            (left, right),
            ("M_SP_NA_1", "M_SP_TB_1")
                | ("M_SP_TB_1", "M_SP_NA_1")
                | ("M_DP_NA_1", "M_DP_TB_1")
                | ("M_DP_TB_1", "M_DP_NA_1")
        )
}

fn push_log(logs: &mut VecDeque<LogEntry>, capacity: usize, entry: LogEntry) {
    while logs.len() >= capacity {
        logs.pop_front();
    }
    logs.push_back(entry);
}

fn push_command_divider(snapshot: &mut AppSnapshot, capacity: usize, side: Side, label: &str) {
    let entry = LogEntry::new(
        side,
        Direction::Internal,
        Severity::Normal,
        LogCategory::CommandDivider,
        format!("━━━━━━━━━━ 手工下发：{label} ━━━━━━━━━━"),
    );
    match side {
        Side::Collect => push_log(&mut snapshot.collect_logs, capacity, entry),
        Side::Dispatch => push_log(&mut snapshot.dispatch_logs, capacity, entry),
        Side::System => push_log(&mut snapshot.system_logs, capacity, entry),
    }
}

fn push_system_error(snapshot: &mut AppSnapshot, capacity: usize, message: String) {
    push_log(
        &mut snapshot.system_logs,
        capacity,
        LogEntry::new(
            Side::System,
            Direction::Internal,
            Severity::Error,
            LogCategory::Configuration,
            message,
        ),
    );
}

fn publish(sender: &watch::Sender<Arc<AppSnapshot>>, snapshot: &AppSnapshot) {
    let _ = sender.send(Arc::new(snapshot.clone()));
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::model::{ControlPhase, DispatchAction, DispatchRequest};

    fn point(ioa: u32, type_id: &str, value: &str, configured: bool) -> PointView {
        PointView {
            ioa,
            name: "测试点".to_owned(),
            type_id: type_id.to_owned(),
            type_name: "测试类型".to_owned(),
            value: Some(value.to_owned()),
            quality: "GOOD".to_owned(),
            updated_ms: Some(1),
            configured,
        }
    }

    #[test]
    fn soe_updates_the_matching_current_indication_without_touching_the_snapshot() {
        let config = load_from(MAIN_CONFIG_PATH).expect("config");
        let mut snapshot = initial_snapshot(&config);
        snapshot.dispatch_current_general = vec![point(1, "M_SP_NA_1", "0/分", true)];
        snapshot.dispatch_general = vec![point(1, "M_SP_NA_1", "0/分", true)];

        reduce(
            &mut snapshot,
            RuntimeEvent::DispatchValues {
                kind: SnapshotKind::General,
                values: vec![point(1, "M_SP_TB_1", "1/合", false)],
            },
            config.ui.log_capacity,
        );

        assert_eq!(snapshot.dispatch_current_general.len(), 1);
        assert_eq!(snapshot.dispatch_current_general[0].type_id, "M_SP_NA_1");
        assert_eq!(
            snapshot.dispatch_current_general[0].value.as_deref(),
            Some("1/合")
        );
        assert_eq!(snapshot.dispatch_general[0].value.as_deref(), Some("0/分"));
    }

    #[tokio::test]
    async fn point_reload_commits_both_sides_only_after_both_prepare() {
        let config = load_from(MAIN_CONFIG_PATH).expect("config");
        let (collect_tx, mut collect_rx) = mpsc::channel(4);
        let (dispatch_tx, mut dispatch_rx) = mpsc::channel(4);
        let collect = tokio::spawn(async move {
            let CollectCommand::PrepareReload {
                generation,
                points,
                reply,
            } = collect_rx.recv().await.expect("collect prepare")
            else {
                panic!("expected collect prepare");
            };
            assert_eq!(generation, 1);
            assert!(!points.is_empty());
            reply.send(Ok(())).expect("collect prepared");
            match collect_rx.recv().await {
                Some(CollectCommand::CommitReload { generation, reply }) => {
                    reply.send(Ok(())).expect("collect committed");
                    generation == 1
                }
                _ => false,
            }
        });
        let dispatch = tokio::spawn(async move {
            let DispatchCommand::PrepareReload {
                generation,
                points,
                reply,
            } = dispatch_rx.recv().await.expect("dispatch prepare")
            else {
                panic!("expected dispatch prepare");
            };
            assert_eq!(generation, 1);
            assert!(!points.is_empty());
            reply.send(Ok(())).expect("dispatch prepared");
            match dispatch_rx.recv().await {
                Some(DispatchCommand::CommitReload { generation, reply }) => {
                    reply.send(Ok(())).expect("dispatch committed");
                    generation == 1
                }
                _ => false,
            }
        });
        let mut snapshot = initial_snapshot(&config);
        let capacity = config.ui.log_capacity;
        let (_runtime_tx, mut runtime_rx) = mpsc::channel(4);
        let (_event_tx, mut event_rx) = mpsc::channel(4);
        let mut events_open = true;
        let (snapshots, _snapshot_rx) = watch::channel(Arc::new(snapshot.clone()));
        let (shutdown, _shutdown_rx) = watch::channel(false);
        reload_points_atomically(
            1,
            &config,
            &collect_tx,
            &dispatch_tx,
            &mut runtime_rx,
            &mut event_rx,
            &mut events_open,
            &mut snapshot,
            &snapshots,
            &shutdown,
            capacity,
        )
        .await;
        assert!(collect.await.expect("collect task"));
        assert!(dispatch.await.expect("dispatch task"));
        assert!(
            snapshot
                .system_logs
                .back()
                .is_some_and(|entry| entry.severity == Severity::Success)
        );
    }

    #[tokio::test]
    async fn point_reload_aborts_both_sides_when_one_prepare_rejects() {
        let config = load_from(MAIN_CONFIG_PATH).expect("config");
        let (collect_tx, mut collect_rx) = mpsc::channel(4);
        let (dispatch_tx, mut dispatch_rx) = mpsc::channel(4);
        let collect = tokio::spawn(async move {
            let CollectCommand::PrepareReload { reply, .. } =
                collect_rx.recv().await.expect("collect prepare")
            else {
                panic!("expected collect prepare");
            };
            reply.send(Ok(())).expect("collect prepared");
            matches!(
                collect_rx.recv().await,
                Some(CollectCommand::AbortReload(2))
            )
        });
        let dispatch = tokio::spawn(async move {
            let DispatchCommand::PrepareReload { reply, .. } =
                dispatch_rx.recv().await.expect("dispatch prepare")
            else {
                panic!("expected dispatch prepare");
            };
            reply
                .send(Err("有未完成事务".to_owned()))
                .expect("dispatch rejected");
            matches!(
                dispatch_rx.recv().await,
                Some(DispatchCommand::AbortReload(2))
            )
        });
        let mut snapshot = initial_snapshot(&config);
        let capacity = config.ui.log_capacity;
        let (_runtime_tx, mut runtime_rx) = mpsc::channel(4);
        let (_event_tx, mut event_rx) = mpsc::channel(4);
        let mut events_open = true;
        let (snapshots, _snapshot_rx) = watch::channel(Arc::new(snapshot.clone()));
        let (shutdown, _shutdown_rx) = watch::channel(false);
        reload_points_atomically(
            2,
            &config,
            &collect_tx,
            &dispatch_tx,
            &mut runtime_rx,
            &mut event_rx,
            &mut events_open,
            &mut snapshot,
            &snapshots,
            &shutdown,
            capacity,
        )
        .await;
        assert!(collect.await.expect("collect task"));
        assert!(dispatch.await.expect("dispatch task"));
        assert!(
            snapshot
                .system_logs
                .back()
                .is_some_and(|entry| entry.severity == Severity::Error)
        );
    }

    #[tokio::test]
    async fn one_sided_commit_failure_stops_both_protocol_tasks() {
        let mut config = load_from(MAIN_CONFIG_PATH).expect("config");
        config.protocol.t0 = Duration::from_millis(100);
        let (collect_tx, mut collect_rx) = mpsc::channel(4);
        let (dispatch_tx, mut dispatch_rx) = mpsc::channel(4);
        let collect = tokio::spawn(async move {
            let CollectCommand::PrepareReload { reply, .. } =
                collect_rx.recv().await.expect("collect prepare")
            else {
                panic!("expected collect prepare");
            };
            reply.send(Ok(())).expect("collect prepared");
            let CollectCommand::CommitReload { reply, .. } =
                collect_rx.recv().await.expect("collect commit")
            else {
                panic!("expected collect commit");
            };
            reply.send(Ok(())).expect("collect committed");
        });
        let dispatch = tokio::spawn(async move {
            let DispatchCommand::PrepareReload { reply, .. } =
                dispatch_rx.recv().await.expect("dispatch prepare")
            else {
                panic!("expected dispatch prepare");
            };
            reply.send(Ok(())).expect("dispatch prepared");
            let DispatchCommand::CommitReload { reply, .. } =
                dispatch_rx.recv().await.expect("dispatch commit")
            else {
                panic!("expected dispatch commit");
            };
            reply
                .send(Err("synthetic commit failure".to_owned()))
                .expect("dispatch rejected commit");
        });

        let mut snapshot = initial_snapshot(&config);
        let capacity = config.ui.log_capacity;
        let (_runtime_tx, mut runtime_rx) = mpsc::channel(4);
        let (_event_tx, mut event_rx) = mpsc::channel(4);
        let mut events_open = true;
        let (snapshots, _snapshot_rx) = watch::channel(Arc::new(snapshot.clone()));
        let (shutdown, shutdown_rx) = watch::channel(false);
        let should_stop = reload_points_atomically(
            3,
            &config,
            &collect_tx,
            &dispatch_tx,
            &mut runtime_rx,
            &mut event_rx,
            &mut events_open,
            &mut snapshot,
            &snapshots,
            &shutdown,
            capacity,
        )
        .await;

        collect.await.expect("collect task");
        dispatch.await.expect("dispatch task");
        assert!(should_stop);
        assert!(*shutdown_rx.borrow());
        assert!(snapshot.system_logs.back().is_some_and(|entry| {
            entry.severity == Severity::Error && entry.summary.contains("已停止两侧协议任务")
        }));
        assert!(
            !snapshot
                .system_logs
                .iter()
                .any(|entry| entry.severity == Severity::Success)
        );
    }

    #[tokio::test]
    async fn reload_abort_never_blocks_the_coordinator_when_child_queues_are_full() {
        let (collect_tx, mut collect_rx) = mpsc::channel(1);
        let (dispatch_tx, mut dispatch_rx) = mpsc::channel(1);
        collect_tx
            .try_send(CollectCommand::SetFaultPolicy(None))
            .expect("fill collect queue");
        dispatch_tx
            .try_send(DispatchCommand::Execute(DispatchRequest {
                action: DispatchAction::Read,
                ioa: 1,
                value: 0.0,
                phase: ControlPhase::Execute,
                common_address: 1,
                originator_address: 0,
                qualifier: 0,
                test: false,
                qoi: 20,
                qcc_request: 5,
                qcc_freeze: 0,
                repeat: 1,
                interval_ms: 1,
                clock_time_ms: None,
            }))
            .expect("fill dispatch queue");

        enqueue_reload_abort(9, &collect_tx, &dispatch_tx);
        assert!(matches!(
            collect_rx.recv().await,
            Some(CollectCommand::SetFaultPolicy(None))
        ));
        assert!(matches!(
            dispatch_rx.recv().await,
            Some(DispatchCommand::Execute(_))
        ));
        assert!(matches!(
            tokio::time::timeout(Duration::from_millis(100), collect_rx.recv())
                .await
                .expect("collect abort enqueue"),
            Some(CollectCommand::AbortReload(9))
        ));
        assert!(matches!(
            tokio::time::timeout(Duration::from_millis(100), dispatch_rx.recv())
                .await
                .expect("dispatch abort enqueue"),
            Some(DispatchCommand::AbortReload(9))
        ));
    }

    #[tokio::test]
    async fn reload_wait_drains_protocol_events_under_backpressure() {
        let config = load_from(MAIN_CONFIG_PATH).expect("config");
        let mut snapshot = initial_snapshot(&config);
        let capacity = config.ui.log_capacity;
        let (_runtime_tx, mut runtime_rx) = mpsc::channel(1);
        let (event_tx, mut event_rx) = mpsc::channel(1);
        event_tx
            .send(RuntimeEvent::ApplicationPending(1))
            .await
            .expect("prefill event queue");
        let mut events_open = true;
        let (snapshots, _snapshot_rx) = watch::channel(Arc::new(snapshot.clone()));
        let (shutdown, _shutdown_rx) = watch::channel(false);
        let (collect_reply_tx, collect_reply_rx) = oneshot::channel();
        let (dispatch_reply_tx, dispatch_reply_rx) = oneshot::channel();

        let second_event = tokio::spawn(async move {
            event_tx
                .send(RuntimeEvent::ApplicationPending(2))
                .await
                .expect("second event");
            collect_reply_tx.send(Ok(())).expect("collect ready");
            dispatch_reply_tx.send(Ok(())).expect("dispatch ready");
        });

        let result = wait_reload_phase(
            "测试",
            Duration::from_millis(200),
            collect_reply_rx,
            dispatch_reply_rx,
            &mut runtime_rx,
            &mut event_rx,
            &mut events_open,
            &mut snapshot,
            &snapshots,
            &shutdown,
            capacity,
        )
        .await;

        second_event.await.expect("event producer");
        assert_eq!(result, ReloadWait::Ready);
        while let Ok(event) = event_rx.try_recv() {
            reduce(&mut snapshot, event, capacity);
        }
        assert_eq!(snapshot.application_pending, 2);
    }

    #[tokio::test]
    async fn reload_wait_honors_quit_without_waiting_for_participants() {
        let config = load_from(MAIN_CONFIG_PATH).expect("config");
        let mut snapshot = initial_snapshot(&config);
        let capacity = config.ui.log_capacity;
        let (runtime_tx, mut runtime_rx) = mpsc::channel(1);
        runtime_tx
            .send(RuntimeCommand::Quit)
            .await
            .expect("queue quit");
        let (_event_tx, mut event_rx) = mpsc::channel(1);
        let mut events_open = true;
        let (snapshots, _snapshot_rx) = watch::channel(Arc::new(snapshot.clone()));
        let (shutdown, shutdown_rx) = watch::channel(false);
        let (_collect_reply_tx, collect_reply_rx) = oneshot::channel();
        let (_dispatch_reply_tx, dispatch_reply_rx) = oneshot::channel();

        let result = wait_reload_phase(
            "测试",
            Duration::from_secs(5),
            collect_reply_rx,
            dispatch_reply_rx,
            &mut runtime_rx,
            &mut event_rx,
            &mut events_open,
            &mut snapshot,
            &snapshots,
            &shutdown,
            capacity,
        )
        .await;

        assert_eq!(result, ReloadWait::Shutdown);
        assert!(*shutdown_rx.borrow());
    }

    #[tokio::test]
    async fn live_runtime_reports_reload_success_only_after_both_commit_acks() {
        let mut config = load_from(MAIN_CONFIG_PATH).expect("config");
        config.collect.bind_port = 0;
        config.dispatch.target_port = 0;
        config.dispatch.reconnect = Duration::from_millis(10);
        config.protocol.t0 = Duration::from_millis(100);

        let runtime = start(config);
        let commands = runtime.commands.clone();
        let mut snapshots = runtime.snapshots.clone();
        commands
            .send(RuntimeCommand::ReloadConfig)
            .await
            .expect("request reload");

        let committed = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if snapshots.borrow().system_logs.iter().any(|entry| {
                    entry.severity == Severity::Success && entry.summary.contains("确认提交")
                }) {
                    return true;
                }
                if snapshots.changed().await.is_err() {
                    return false;
                }
            }
        })
        .await
        .unwrap_or(false);

        let _ = commands.send(RuntimeCommand::Quit).await;
        runtime.wait().await;
        assert!(
            committed,
            "live runtime did not confirm both reload commits"
        );
    }
}
