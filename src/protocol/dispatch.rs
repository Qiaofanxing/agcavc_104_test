use std::time::Duration;

use iec104::{
    asdu::Asdu,
    cot::Cot,
    types::{
        InformationObjects,
        commands::{Qoi, Rqt},
    },
    types_id::TypeId,
};
use tokio::{
    net::TcpStream,
    sync::{mpsc, watch},
    time::{Instant, MissedTickBehavior, interval, sleep, timeout},
};

use crate::{
    config::{DispatchPointConfig, DispatchSettings, ProtocolSettings},
    model::{
        ConnectionPhase, ConnectionView, Direction, DispatchAction, DispatchCommand,
        DispatchPointPurpose, DispatchRequest, LogCategory, LogEntry, PointView, RuntimeEvent,
        Severity, Side, SnapshotKind, now_millis,
    },
    protocol::{
        asdu::{
            build_dispatch_request, counter_interrogation_cot, interrogation_cot,
            is_counter_data_cot, is_general_data_cot, rows_from_asdu,
        },
        wire::{LinkRole, LinkSession, WireEvent, read_apdu, tick_interval},
    },
};

pub async fn run(
    settings: DispatchSettings,
    protocol: ProtocolSettings,
    initial_points: Vec<DispatchPointConfig>,
    mut commands: mpsc::Receiver<DispatchCommand>,
    events: mpsc::Sender<RuntimeEvent>,
    mut shutdown: watch::Receiver<bool>,
) {
    let endpoint = format!("{}:{}", settings.target_host, settings.target_port);
    let mut view = ConnectionView::new(ConnectionPhase::Disconnected, endpoint.clone());
    let mut expected = ExpectedPoints::new(initial_points);
    let mut prepared_reload = None;
    emit_expected(&events, &expected).await;
    emit_connection(&events, &view).await;

    'runtime: loop {
        view.phase = ConnectionPhase::Connecting;
        view.peer = None;
        view.detail = format!("正在连接 {endpoint}");
        emit_connection(&events, &view).await;
        emit_log(
            &events,
            Direction::Internal,
            Severity::Normal,
            LogCategory::Connection,
            format!("正在连接调度子站 {endpoint}"),
        )
        .await;

        let connected = tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break 'runtime;
                }
                continue 'runtime;
            }
            result = timeout(protocol.t0, TcpStream::connect(&endpoint)) => result,
        };
        let stream = match connected {
            Ok(Ok(stream)) => stream,
            Ok(Err(error)) => {
                emit_log(
                    &events,
                    Direction::Internal,
                    Severity::Warning,
                    LogCategory::Connection,
                    format!("连接调度子站失败: {error}"),
                )
                .await;
                view.phase = ConnectionPhase::ReconnectWait;
                view.detail = format!("{} ms 后重试", settings.reconnect.as_millis());
                emit_connection(&events, &view).await;
                if wait_for_reconnect(
                    settings.reconnect,
                    &mut expected,
                    &mut prepared_reload,
                    &mut commands,
                    &events,
                    &mut shutdown,
                )
                .await
                {
                    break;
                }
                continue;
            }
            Err(_) => {
                emit_log(
                    &events,
                    Direction::Internal,
                    Severity::Warning,
                    LogCategory::Connection,
                    format!("连接调度子站超时（T0={} ms）", protocol.t0.as_millis()),
                )
                .await;
                view.phase = ConnectionPhase::ReconnectWait;
                view.detail = format!("{} ms 后重试", settings.reconnect.as_millis());
                emit_connection(&events, &view).await;
                if wait_for_reconnect(
                    settings.reconnect,
                    &mut expected,
                    &mut prepared_reload,
                    &mut commands,
                    &events,
                    &mut shutdown,
                )
                .await
                {
                    break;
                }
                continue;
            }
        };

        let peer = stream.peer_addr().ok().map(|value| value.to_string());
        view.phase = ConnectionPhase::ConnectedStopped;
        view.peer = peer;
        view.detail = "TCP 已连接；请在调度指令页手动 STARTDT".to_owned();
        emit_connection(&events, &view).await;
        emit_log(
            &events,
            Direction::Incoming,
            Severity::Success,
            LogCategory::Connection,
            "调度 TCP 已连接，当前保持 CONNECTED_STOPPED",
        )
        .await;

        let exit = run_session(
            stream,
            &protocol,
            settings.common_address,
            &mut expected,
            &mut prepared_reload,
            &mut commands,
            &events,
            &mut view,
            &mut shutdown,
        )
        .await;
        if matches!(exit, SessionExit::Shutdown) {
            break;
        }

        view.phase = ConnectionPhase::ReconnectWait;
        view.peer = None;
        view.detail = format!("连接断开，{} ms 后重试", settings.reconnect.as_millis());
        emit_connection(&events, &view).await;
        if wait_for_reconnect(
            settings.reconnect,
            &mut expected,
            &mut prepared_reload,
            &mut commands,
            &events,
            &mut shutdown,
        )
        .await
        {
            break;
        }
    }

    view.phase = ConnectionPhase::Disabled;
    view.peer = None;
    view.detail = "调度侧任务已停止".to_owned();
    emit_connection(&events, &view).await;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SessionExit {
    Disconnected,
    Shutdown,
}

#[allow(clippy::too_many_arguments)]
async fn run_session(
    stream: TcpStream,
    protocol: &ProtocolSettings,
    configured_common_address: u16,
    expected: &mut ExpectedPoints,
    prepared_reload: &mut Option<(u64, ExpectedPoints)>,
    commands: &mut mpsc::Receiver<DispatchCommand>,
    events: &mpsc::Sender<RuntimeEvent>,
    view: &mut ConnectionView,
    shutdown: &mut watch::Receiver<bool>,
) -> SessionExit {
    if let Err(error) = stream.set_nodelay(true) {
        emit_log(
            events,
            Direction::Internal,
            Severity::Warning,
            LogCategory::Connection,
            format!("设置 TCP_NODELAY 失败: {error}"),
        )
        .await;
    }
    let (mut reader, writer) = stream.into_split();
    let mut session = LinkSession::new(LinkRole::Client, writer, protocol.to_iec104());
    let mut rounds = Rounds::default();
    let mut applications = PendingApplications::default();
    let mut scheduled = Vec::<ScheduledRequest>::new();
    let mut ticker = interval(tick_interval());
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    abort_rounds(&mut rounds, events, "程序关闭").await;
                    applications.abort_all(events, "程序关闭").await;
                    return SessionExit::Shutdown;
                }
            }
            command = commands.recv() => {
                match command {
                    None => {
                        abort_rounds(&mut rounds, events, "程序关闭").await;
                        applications.abort_all(events, "程序关闭").await;
                        return SessionExit::Shutdown;
                    }
                    Some(DispatchCommand::PrepareReload { generation, points, reply }) => {
                        if rounds.is_idle() && applications.is_empty() && scheduled.is_empty() {
                            *prepared_reload = Some((generation, ExpectedPoints::new(points)));
                            let _ = reply.send(Ok(()));
                        } else {
                            let _ = reply.send(Err(
                                "调度侧存在未完成召唤、应用回执或定时发送".to_owned(),
                            ));
                        }
                    }
                    Some(DispatchCommand::CommitReload { generation, reply }) => {
                        let result = if rounds.is_idle()
                            && applications.is_empty()
                            && scheduled.is_empty()
                        {
                            commit_expected_reload(
                                generation,
                                prepared_reload,
                                expected,
                                events,
                            ).await
                        } else {
                            Err("调度侧提交时出现未完成召唤、应用回执或定时发送".to_owned())
                        };
                        let _ = reply.send(result);
                    }
                    Some(DispatchCommand::AbortReload(generation)) => {
                        abort_expected_reload(generation, prepared_reload);
                    }
                    Some(DispatchCommand::Execute(request)) => {
                        if prepared_reload.is_some() {
                            emit_log(
                                events,
                                Direction::Internal,
                                Severity::Warning,
                                LogCategory::Configuration,
                                format!(
                                    "{}本地拒绝：点表重载已准备，等待提交或取消",
                                    request.action.label()
                                ),
                            ).await;
                        } else if let Err(error) = schedule_or_execute(
                            request,
                            &mut session,
                            &mut rounds,
                            &mut scheduled,
                            &mut applications,
                            protocol,
                            configured_common_address,
                            events,
                            view,
                        ).await {
                            emit_log(
                                events,
                                Direction::Internal,
                                Severity::Error,
                                LogCategory::Control,
                                format!("调度指令本地拒绝: {error}"),
                            ).await;
                        }
                    }
                }
            }
            frame = read_apdu(&mut reader) => {
                let apdu = match frame {
                    Ok(apdu) => apdu,
                    Err(error) => {
                        emit_log(
                            events,
                            Direction::Incoming,
                            Severity::Warning,
                            LogCategory::Connection,
                            format!("调度连接已结束: {error}"),
                        ).await;
                        abort_rounds(&mut rounds, events, "连接断开").await;
                        applications.abort_all(events, "连接断开").await;
                        return SessionExit::Disconnected;
                    }
                };
                let processed = match session.process(apdu.frame).await {
                    Ok(processed) => processed,
                    Err(error) => {
                        emit_log(
                            events,
                            Direction::Internal,
                            Severity::Error,
                            LogCategory::Protocol,
                            format!("调度链路处理失败: {error}"),
                        ).await;
                        abort_rounds(&mut rounds, events, "协议错误").await;
                        applications.abort_all(events, "协议错误").await;
                        return SessionExit::Disconnected;
                    }
                };
                for event in processed.events {
                    record_wire(events, view, event).await;
                }
                if let Some(phase) = processed.phase_changed {
                    view.phase = phase;
                    view.detail = phase.label().to_owned();
                    emit_connection(events, view).await;
                    if phase == ConnectionPhase::ConnectedStopped {
                        abort_rounds(&mut rounds, events, "STOPDT").await;
                        applications.abort_all(events, "STOPDT").await;
                    }
                }
                if let Some(asdu) = processed.asdu {
                    handle_incoming(
                        asdu,
                        configured_common_address,
                        &mut rounds,
                        &mut applications,
                        expected,
                        events,
                    ).await;
                }
            }
            _ = ticker.tick() => {
                match session.tick().await {
                    Ok(wire_events) => {
                        for event in wire_events {
                            record_wire(events, view, event).await;
                        }
                    }
                    Err(error) => {
                        emit_log(
                            events,
                            Direction::Internal,
                            Severity::Error,
                            LogCategory::KeepAlive,
                            format!("调度链路计时器失败: {error}"),
                        ).await;
                        abort_rounds(&mut rounds, events, "链路 T1 超时").await;
                        applications.abort_all(events, "链路 T1 超时").await;
                        return SessionExit::Disconnected;
                    }
                }

                if let Err(error) = flush_scheduled(
                    &mut scheduled,
                    &mut session,
                    &mut rounds,
                    &mut applications,
                    protocol.t1.saturating_mul(4),
                    configured_common_address,
                    events,
                    view,
                ).await {
                    emit_log(
                        events,
                        Direction::Internal,
                        Severity::Error,
                        LogCategory::Control,
                        format!("发送调度指令失败: {error}"),
                    ).await;
                }
                expire_rounds(&mut rounds, events).await;
                applications.expire(events).await;
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn schedule_or_execute(
    mut request: DispatchRequest,
    session: &mut LinkSession,
    rounds: &mut Rounds,
    scheduled: &mut Vec<ScheduledRequest>,
    applications: &mut PendingApplications,
    protocol: &ProtocolSettings,
    configured_common_address: u16,
    events: &mpsc::Sender<RuntimeEvent>,
    view: &mut ConnectionView,
) -> Result<(), String> {
    match request.action {
        DispatchAction::StartDt => {
            let event = session.start_dt().await?;
            view.phase = session.phase();
            record_wire(events, view, event).await;
            emit_connection(events, view).await;
            return Ok(());
        }
        DispatchAction::StopDt => {
            let event = session.stop_dt().await?;
            view.phase = session.phase();
            record_wire(events, view, event).await;
            emit_connection(events, view).await;
            return Ok(());
        }
        DispatchAction::TestFr => {
            let event = session.manual_test().await?;
            record_wire(events, view, event).await;
            return Ok(());
        }
        _ => {}
    }
    if !session.is_active() {
        return Err(format!(
            "链路状态 {}，需要先 STARTDT",
            session.phase().label()
        ));
    }
    build_dispatch_request(&request)?;

    if matches!(
        request.action,
        DispatchAction::GeneralInterrogation | DispatchAction::CounterInterrogation
    ) {
        if request.repeat > 1 {
            emit_log(
                events,
                Direction::Internal,
                Severity::Warning,
                LogCategory::Interrogation,
                "召唤事务不支持重复发送，已按一次执行",
            )
            .await;
        }
        request.repeat = 1;
        rounds
            .reserve(
                &request,
                configured_common_address,
                protocol.t1.saturating_mul(4),
                events,
            )
            .await?;
    }

    let repeat = request.repeat.max(1);
    let spacing = Duration::from_millis(request.interval_ms.max(1));
    request.repeat = 1;
    for index in 0..repeat {
        scheduled.push(ScheduledRequest {
            request: request.clone(),
            due: Instant::now() + spacing.saturating_mul(u32::from(index)),
        });
    }
    scheduled.sort_by_key(|item| item.due);
    flush_scheduled(
        scheduled,
        session,
        rounds,
        applications,
        protocol.t1.saturating_mul(4),
        configured_common_address,
        events,
        view,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn flush_scheduled(
    scheduled: &mut Vec<ScheduledRequest>,
    session: &mut LinkSession,
    rounds: &mut Rounds,
    applications: &mut PendingApplications,
    application_timeout: Duration,
    configured_common_address: u16,
    events: &mpsc::Sender<RuntimeEvent>,
    view: &mut ConnectionView,
) -> Result<(), String> {
    while scheduled
        .first()
        .is_some_and(|item| item.due <= Instant::now())
    {
        let item = scheduled.remove(0);
        let action = item.request.action;
        let asdu = build_dispatch_request(&item.request)?
            .ok_or_else(|| format!("动作 {action:?} 没有应用层 ASDU"))?;
        match session.send_asdu(asdu).await {
            Ok(event) => {
                record_wire(events, view, event).await;
                let pending_before = applications.len();
                applications.register(
                    item.request,
                    configured_common_address,
                    application_timeout,
                )?;
                if applications.len() != pending_before {
                    emit(events, RuntimeEvent::ApplicationPending(applications.len())).await;
                }
            }
            Err(error) => {
                if matches!(
                    action,
                    DispatchAction::GeneralInterrogation | DispatchAction::CounterInterrogation
                ) {
                    rounds.abort(action, events, "发送失败").await;
                }
                return Err(error);
            }
        }
    }
    Ok(())
}

async fn handle_incoming(
    asdu: Asdu,
    configured_common_address: u16,
    rounds: &mut Rounds,
    applications: &mut PendingApplications,
    expected: &ExpectedPoints,
    events: &mpsc::Sender<RuntimeEvent>,
) {
    emit_current_values(&asdu, configured_common_address, events).await;
    process_round(
        &mut rounds.general,
        SnapshotKind::General,
        &asdu,
        expected,
        events,
    )
    .await;
    process_round(
        &mut rounds.energy,
        SnapshotKind::Energy,
        &asdu,
        expected,
        events,
    )
    .await;
    let pending_before = applications.len();
    applications.handle(&asdu, events).await;
    if applications.len() != pending_before {
        emit(events, RuntimeEvent::ApplicationPending(applications.len())).await;
    }
}

async fn emit_current_values(
    asdu: &Asdu,
    configured_common_address: u16,
    events: &mpsc::Sender<RuntimeEvent>,
) {
    if asdu.address_field != configured_common_address || asdu.test || asdu.negative {
        return;
    }
    let values = rows_from_asdu(asdu);
    if values.is_empty() {
        return;
    }
    let kind = if asdu.type_id == TypeId::M_IT_NA_1 {
        SnapshotKind::Energy
    } else {
        SnapshotKind::General
    };
    emit(events, RuntimeEvent::DispatchValues { kind, values }).await;
}

#[derive(Debug)]
struct ScheduledRequest {
    request: DispatchRequest,
    due: Instant,
}

#[derive(Debug)]
struct InterrogationRound {
    request: DispatchRequest,
    response_common_address: u16,
    phase: RoundPhase,
    deadline: Instant,
    values: Vec<PointView>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RoundPhase {
    AwaitActcon,
    Collecting,
}

#[derive(Debug, Default)]
struct Rounds {
    general: Option<InterrogationRound>,
    energy: Option<InterrogationRound>,
}

impl Rounds {
    const fn is_idle(&self) -> bool {
        self.general.is_none() && self.energy.is_none()
    }

    async fn reserve(
        &mut self,
        request: &DispatchRequest,
        configured_common_address: u16,
        timeout: Duration,
        events: &mpsc::Sender<RuntimeEvent>,
    ) -> Result<(), String> {
        let (slot, kind) = match request.action {
            DispatchAction::GeneralInterrogation => (&mut self.general, SnapshotKind::General),
            DispatchAction::CounterInterrogation => (&mut self.energy, SnapshotKind::Energy),
            _ => return Ok(()),
        };
        if slot.is_some() {
            return Err(format!("{}已有未完成轮次", kind.label()));
        }
        *slot = Some(InterrogationRound {
            request: request.clone(),
            response_common_address: if request.common_address == u16::MAX {
                configured_common_address
            } else {
                request.common_address
            },
            phase: RoundPhase::AwaitActcon,
            deadline: Instant::now() + timeout,
            values: Vec::new(),
        });
        emit(
            events,
            RuntimeEvent::RoundPending {
                kind,
                pending: true,
            },
        )
        .await;
        Ok(())
    }

    async fn abort(
        &mut self,
        action: DispatchAction,
        events: &mpsc::Sender<RuntimeEvent>,
        reason: &str,
    ) {
        let (slot, kind) = match action {
            DispatchAction::GeneralInterrogation => (&mut self.general, SnapshotKind::General),
            DispatchAction::CounterInterrogation => (&mut self.energy, SnapshotKind::Energy),
            _ => return,
        };
        if slot.take().is_some() {
            emit(
                events,
                RuntimeEvent::RoundPending {
                    kind,
                    pending: false,
                },
            )
            .await;
            emit_log(
                events,
                Direction::Internal,
                Severity::Error,
                LogCategory::Interrogation,
                format!("{}轮次失败，保留上一份完整快照: {reason}", kind.label()),
            )
            .await;
        }
    }
}

enum RoundOutcome {
    Unrelated,
    Confirmed,
    Data(Vec<PointView>),
    Complete,
    Abort(String),
    Mismatch(String),
}

async fn process_round(
    slot: &mut Option<InterrogationRound>,
    kind: SnapshotKind,
    asdu: &Asdu,
    expected: &ExpectedPoints,
    events: &mpsc::Sender<RuntimeEvent>,
) {
    let Some(round) = slot.as_mut() else {
        return;
    };
    let outcome = evaluate_round(round, kind, asdu);
    match outcome {
        RoundOutcome::Unrelated => {}
        RoundOutcome::Confirmed => {
            emit_log(
                events,
                Direction::Internal,
                Severity::Success,
                LogCategory::Interrogation,
                format!("{}已收到完整匹配的正向 ACTCON，开始累积数据", kind.label()),
            )
            .await;
        }
        RoundOutcome::Data(values) => round.values.extend(values),
        RoundOutcome::Complete => {
            let round = slot.take().expect("完成召唤轮次必须存在");
            let values = match kind {
                SnapshotKind::General => expected.merge_general(round.values),
                SnapshotKind::Energy => expected.merge_energy(round.values),
            };
            emit(events, RuntimeEvent::DispatchSnapshot { kind, values }).await;
            emit(
                events,
                RuntimeEvent::RoundPending {
                    kind,
                    pending: false,
                },
            )
            .await;
            emit_log(
                events,
                Direction::Internal,
                Severity::Success,
                LogCategory::Interrogation,
                format!("{}收到完整匹配的 ACTTERM，已原子提交快照", kind.label()),
            )
            .await;
        }
        RoundOutcome::Abort(reason) => {
            let action = round.request.action;
            let _ = round;
            let mut wrapper = Rounds::default();
            match kind {
                SnapshotKind::General => std::mem::swap(slot, &mut wrapper.general),
                SnapshotKind::Energy => std::mem::swap(slot, &mut wrapper.energy),
            }
            wrapper.abort(action, events, &reason).await;
        }
        RoundOutcome::Mismatch(reason) => {
            emit_log(
                events,
                Direction::Internal,
                Severity::Warning,
                LogCategory::Interrogation,
                format!("{}忽略不匹配响应: {reason}", kind.label()),
            )
            .await;
        }
    }
}

fn evaluate_round(round: &mut InterrogationRound, kind: SnapshotKind, asdu: &Asdu) -> RoundOutcome {
    let command_type = match kind {
        SnapshotKind::General => TypeId::C_IC_NA_1,
        SnapshotKind::Energy => TypeId::C_CI_NA_1,
    };
    let is_data_type = match kind {
        SnapshotKind::General => matches!(
            asdu.type_id,
            TypeId::M_SP_NA_1 | TypeId::M_DP_NA_1 | TypeId::M_ME_NC_1
        ),
        SnapshotKind::Energy => asdu.type_id == TypeId::M_IT_NA_1,
    };
    let expected_data_cot = match kind {
        SnapshotKind::General => interrogation_cot(Qoi::from_byte(round.request.qoi)),
        SnapshotKind::Energy => Some(counter_interrogation_cot(Rqt::from_byte(
            round.request.qcc_request,
        ))),
    };
    let has_round_data_cot = match kind {
        SnapshotKind::General => is_general_data_cot(asdu.cot),
        SnapshotKind::Energy => is_counter_data_cot(asdu.cot),
    };
    let candidate_command = asdu.type_id == command_type
        && matches!(
            asdu.cot,
            Cot::ActivationConfirmation | Cot::ActivationTermination
        );
    let candidate_data = is_data_type && expected_data_cot == Some(asdu.cot);
    if has_round_data_cot && !is_data_type {
        return RoundOutcome::Mismatch(format!("轮次数据 TypeID 不支持: {:?}", asdu.type_id));
    }
    if has_round_data_cot && expected_data_cot != Some(asdu.cot) {
        return RoundOutcome::Mismatch(format!(
            "数据 COT 期望 {:?} 实际 {:?}",
            expected_data_cot, asdu.cot
        ));
    }
    if !candidate_command && !candidate_data {
        return RoundOutcome::Unrelated;
    }
    if asdu.address_field != round.response_common_address {
        return RoundOutcome::Mismatch(format!(
            "CA 期望 {} 实际 {}",
            round.response_common_address, asdu.address_field
        ));
    }
    if asdu.originator_address != round.request.originator_address {
        return RoundOutcome::Mismatch(format!(
            "OA 期望 {} 实际 {}",
            round.request.originator_address, asdu.originator_address
        ));
    }
    if asdu.test != round.request.test {
        return RoundOutcome::Mismatch(format!(
            "Test 期望 {} 实际 {}",
            round.request.test, asdu.test
        ));
    }
    if asdu.negative {
        return RoundOutcome::Abort(format!("收到否定 {:?}", asdu.cot));
    }
    if candidate_command && !round_qualifier_matches(&round.request, &asdu.information_objects) {
        return RoundOutcome::Mismatch("QOI/QCC 或信息体不匹配".to_owned());
    }
    if asdu.cot == Cot::ActivationConfirmation {
        round.phase = RoundPhase::Collecting;
        return RoundOutcome::Confirmed;
    }
    if asdu.cot == Cot::ActivationTermination {
        return if round.phase == RoundPhase::Collecting {
            RoundOutcome::Complete
        } else {
            RoundOutcome::Mismatch("ACTCON 之前收到 ACTTERM".to_owned())
        };
    }
    if round.phase != RoundPhase::Collecting {
        return RoundOutcome::Mismatch("ACTCON 之前收到召唤数据".to_owned());
    }
    RoundOutcome::Data(rows_from_asdu(asdu))
}

fn round_qualifier_matches(request: &DispatchRequest, objects: &InformationObjects) -> bool {
    match (request.action, objects) {
        (DispatchAction::GeneralInterrogation, InformationObjects::CIcNa1(values)) => {
            values.len() == 1
                && values[0].address == 0
                && values[0].object.qoi.to_byte() == request.qoi
        }
        (DispatchAction::CounterInterrogation, InformationObjects::CCiNa1(values)) => {
            values.len() == 1
                && values[0].address == 0
                && values[0].object.rqt.to_byte() == request.qcc_request
                && values[0].object.frz as u8 == request.qcc_freeze
        }
        _ => false,
    }
}

#[derive(Debug)]
struct PendingApplication {
    request: DispatchRequest,
    expected: Asdu,
    response_common_address: u16,
    deadline: Instant,
    accepted: bool,
}

#[derive(Debug, Default)]
struct PendingApplications {
    entries: Vec<PendingApplication>,
}

impl PendingApplications {
    fn len(&self) -> usize {
        self.entries.len()
    }

    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn register(
        &mut self,
        request: DispatchRequest,
        configured_common_address: u16,
        timeout: Duration,
    ) -> Result<(), String> {
        if !request.action.is_control() && request.action != DispatchAction::ClockSync {
            return Ok(());
        }
        let expected = build_dispatch_request(&request)?
            .ok_or_else(|| format!("{} 没有应用 ASDU", request.action.label()))?;
        self.entries.push(PendingApplication {
            response_common_address: if request.common_address == u16::MAX {
                configured_common_address
            } else {
                request.common_address
            },
            request,
            expected,
            deadline: Instant::now() + timeout,
            accepted: false,
        });
        Ok(())
    }

    async fn handle(&mut self, asdu: &Asdu, events: &mpsc::Sender<RuntimeEvent>) {
        if !is_application_confirmation(asdu) {
            return;
        }
        let Some(index) = self
            .entries
            .iter()
            .position(|pending| pending.matches(asdu))
        else {
            emit_log(
                events,
                Direction::Internal,
                Severity::Warning,
                LogCategory::Control,
                format!(
                    "收到未关联的应用回执：TypeID={:?} COT={:?} CA={} OA={} Test={}",
                    asdu.type_id, asdu.cot, asdu.address_field, asdu.originator_address, asdu.test
                ),
            )
            .await;
            return;
        };
        let action = self.entries[index].request.action;
        let ioa = self.entries[index].request.ioa;
        if asdu.cot == Cot::ActivationConfirmation {
            if asdu.negative {
                self.entries.remove(index);
                emit_log(
                    events,
                    Direction::Internal,
                    Severity::Error,
                    LogCategory::Control,
                    format!("{} IOA={ioa} 收到否定 ACTCON", action.label()),
                )
                .await;
            } else if action == DispatchAction::ClockSync
                || self.entries[index].request.phase == crate::model::ControlPhase::Select
            {
                self.entries.remove(index);
                emit_log(
                    events,
                    Direction::Internal,
                    Severity::Success,
                    LogCategory::Control,
                    format!("{} IOA={ioa} 收到完整匹配的 ACTCON", action.label()),
                )
                .await;
            } else {
                self.entries[index].accepted = true;
                emit_log(
                    events,
                    Direction::Internal,
                    Severity::Success,
                    LogCategory::Control,
                    format!("{} IOA={ioa} 已接受，等待 ACTTERM", action.label()),
                )
                .await;
            }
            return;
        }
        if asdu.cot == Cot::ActivationTermination {
            if asdu.negative {
                self.entries.remove(index);
                emit_log(
                    events,
                    Direction::Internal,
                    Severity::Error,
                    LogCategory::Control,
                    format!("{} IOA={ioa} 收到否定 ACTTERM", action.label()),
                )
                .await;
            } else if self.entries[index].accepted {
                self.entries.remove(index);
                emit_log(
                    events,
                    Direction::Internal,
                    Severity::Success,
                    LogCategory::Control,
                    format!("{} IOA={ioa} 已完成，回执字段全部匹配", action.label()),
                )
                .await;
            } else {
                emit_log(
                    events,
                    Direction::Internal,
                    Severity::Warning,
                    LogCategory::Control,
                    format!(
                        "{} IOA={ioa} 在正向 ACTCON 前收到 ACTTERM，已忽略",
                        action.label()
                    ),
                )
                .await;
            }
        }
    }

    async fn expire(&mut self, events: &mpsc::Sender<RuntimeEvent>) {
        let now = Instant::now();
        let mut expired = Vec::new();
        self.entries.retain(|pending| {
            if now >= pending.deadline {
                expired.push((pending.request.action, pending.request.ioa));
                false
            } else {
                true
            }
        });
        let had_expired = !expired.is_empty();
        for (action, ioa) in expired {
            emit_log(
                events,
                Direction::Internal,
                Severity::Error,
                LogCategory::Control,
                format!("{} IOA={ioa} 等待应用回执超时", action.label()),
            )
            .await;
        }
        if had_expired {
            emit(events, RuntimeEvent::ApplicationPending(self.len())).await;
        }
    }

    async fn abort_all(&mut self, events: &mpsc::Sender<RuntimeEvent>, reason: &str) {
        if self.entries.is_empty() {
            return;
        }
        let count = self.entries.len();
        self.entries.clear();
        emit(events, RuntimeEvent::ApplicationPending(0)).await;
        emit_log(
            events,
            Direction::Internal,
            Severity::Error,
            LogCategory::Control,
            format!("已取消 {count} 个未完成应用事务: {reason}"),
        )
        .await;
    }
}

impl PendingApplication {
    fn matches(&self, asdu: &Asdu) -> bool {
        asdu.type_id == self.expected.type_id
            && asdu.address_field == self.response_common_address
            && asdu.originator_address == self.expected.originator_address
            && asdu.test == self.expected.test
            && asdu.information_objects == self.expected.information_objects
    }
}

fn is_application_confirmation(asdu: &Asdu) -> bool {
    matches!(
        asdu.type_id,
        TypeId::C_SC_NA_1
            | TypeId::C_DC_NA_1
            | TypeId::C_RC_NA_1
            | TypeId::C_SE_NA_1
            | TypeId::C_SE_NB_1
            | TypeId::C_SE_NC_1
            | TypeId::C_CS_NA_1
    ) && matches!(
        asdu.cot,
        Cot::ActivationConfirmation | Cot::ActivationTermination
    )
}

async fn expire_rounds(rounds: &mut Rounds, events: &mpsc::Sender<RuntimeEvent>) {
    let now = Instant::now();
    if rounds
        .general
        .as_ref()
        .is_some_and(|round| now >= round.deadline)
    {
        rounds
            .abort(
                DispatchAction::GeneralInterrogation,
                events,
                "等待 ACTTERM 超时",
            )
            .await;
    }
    if rounds
        .energy
        .as_ref()
        .is_some_and(|round| now >= round.deadline)
    {
        rounds
            .abort(
                DispatchAction::CounterInterrogation,
                events,
                "等待 ACTTERM 超时",
            )
            .await;
    }
}

async fn abort_rounds(rounds: &mut Rounds, events: &mpsc::Sender<RuntimeEvent>, reason: &str) {
    rounds
        .abort(DispatchAction::GeneralInterrogation, events, reason)
        .await;
    rounds
        .abort(DispatchAction::CounterInterrogation, events, reason)
        .await;
}

#[derive(Debug, Clone)]
struct ExpectedPoints {
    points: Vec<DispatchPointConfig>,
}

impl ExpectedPoints {
    fn new(points: Vec<DispatchPointConfig>) -> Self {
        Self { points }
    }

    fn general_views(&self) -> Vec<PointView> {
        expected_views(&self.points, DispatchPointPurpose::ExpectedGeneral)
    }

    fn energy_views(&self) -> Vec<PointView> {
        expected_views(&self.points, DispatchPointPurpose::ExpectedEnergy)
    }

    fn merge_general(&self, values: Vec<PointView>) -> Vec<PointView> {
        merge_values(self.general_views(), values)
    }

    fn merge_energy(&self, values: Vec<PointView>) -> Vec<PointView> {
        merge_values(self.energy_views(), values)
    }
}

fn expected_views(points: &[DispatchPointConfig], purpose: DispatchPointPurpose) -> Vec<PointView> {
    let mut rows: Vec<_> = points
        .iter()
        .filter(|point| point.purpose == purpose)
        .map(|point| PointView {
            ioa: point.ioa,
            name: point.name.clone(),
            type_id: point.kind.type_id().to_owned(),
            type_name: point.kind.label().to_owned(),
            value: None,
            quality: "—".to_owned(),
            updated_ms: None,
            configured: true,
        })
        .collect();
    rows.sort_by_key(|row| row.ioa);
    rows
}

fn merge_values(mut expected: Vec<PointView>, values: Vec<PointView>) -> Vec<PointView> {
    for mut value in values {
        if let Some(existing) = expected
            .iter_mut()
            .find(|row| row.ioa == value.ioa && row.type_id == value.type_id)
        {
            existing.value = value.value.take();
            existing.quality = value.quality;
            existing.updated_ms = value.updated_ms;
        } else {
            value.name = if expected.iter().any(|row| row.ioa == value.ioa) {
                format!("类型不匹配（实际 {}）", value.type_id)
            } else {
                "未配置 IOA".to_owned()
            };
            value.configured = false;
            expected.push(value);
        }
    }
    expected.sort_by_key(|row| row.ioa);
    expected
}

async fn wait_for_reconnect(
    delay: Duration,
    expected: &mut ExpectedPoints,
    prepared_reload: &mut Option<(u64, ExpectedPoints)>,
    commands: &mut mpsc::Receiver<DispatchCommand>,
    events: &mpsc::Sender<RuntimeEvent>,
    shutdown: &mut watch::Receiver<bool>,
) -> bool {
    let timer = sleep(delay);
    tokio::pin!(timer);
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return true;
                }
            }
            () = &mut timer => return false,
            command = commands.recv() => {
                match command {
                    None => return true,
                    Some(DispatchCommand::PrepareReload { generation, points, reply }) => {
                        *prepared_reload = Some((generation, ExpectedPoints::new(points)));
                        let _ = reply.send(Ok(()));
                    }
                    Some(DispatchCommand::CommitReload { generation, reply }) => {
                        let result = commit_expected_reload(
                            generation,
                            prepared_reload,
                            expected,
                            events,
                        ).await;
                        let _ = reply.send(result);
                    }
                    Some(DispatchCommand::AbortReload(generation)) => {
                        abort_expected_reload(generation, prepared_reload);
                    }
                    Some(DispatchCommand::Execute(request)) => {
                        let reason = if prepared_reload.is_some() {
                            "点表重载已准备，等待提交或取消"
                        } else {
                            "调度 TCP 尚未连接"
                        };
                        emit_log(
                            events,
                            Direction::Internal,
                            Severity::Warning,
                            LogCategory::Control,
                            format!("{}本地拒绝：{reason}", request.action.label()),
                        ).await;
                    }
                }
            }
        }
    }
}

async fn commit_expected_reload(
    generation: u64,
    prepared: &mut Option<(u64, ExpectedPoints)>,
    expected: &mut ExpectedPoints,
    events: &mpsc::Sender<RuntimeEvent>,
) -> Result<(), String> {
    let Some((prepared_generation, fresh)) = prepared.take() else {
        return Err(format!("调度侧没有待提交的重载版本 {generation}"));
    };
    if prepared_generation != generation {
        *prepared = Some((prepared_generation, fresh));
        return Err(format!(
            "调度侧待提交版本为 {prepared_generation}，收到版本 {generation}"
        ));
    }
    *expected = fresh;
    emit_expected(events, expected).await;
    emit_log(
        events,
        Direction::Internal,
        Severity::Success,
        LogCategory::Configuration,
        format!("调度点表已提交重载版本 {generation}"),
    )
    .await;
    Ok(())
}

fn abort_expected_reload(generation: u64, prepared: &mut Option<(u64, ExpectedPoints)>) {
    if prepared
        .as_ref()
        .is_some_and(|(prepared_generation, _)| *prepared_generation == generation)
    {
        *prepared = None;
    }
}

async fn emit_expected(events: &mpsc::Sender<RuntimeEvent>, expected: &ExpectedPoints) {
    emit(
        events,
        RuntimeEvent::DispatchExpectedReloaded {
            general: expected.general_views(),
            energy: expected.energy_views(),
            points: expected.points.clone(),
        },
    )
    .await;
}

async fn record_wire(
    events: &mpsc::Sender<RuntimeEvent>,
    view: &mut ConnectionView,
    event: WireEvent,
) {
    let now = now_millis();
    match event.direction {
        Direction::Incoming => view.last_rx_ms = Some(now),
        Direction::Outgoing => view.last_tx_ms = Some(now),
        Direction::Internal => {}
    }
    emit(
        events,
        RuntimeEvent::Log(
            LogEntry::new(
                Side::Dispatch,
                event.direction,
                event.severity,
                event.category,
                event.summary,
            )
            .with_details(event.details)
            .with_protocol(event.protocol),
        ),
    )
    .await;
    emit_connection(events, view).await;
}

async fn emit_connection(events: &mpsc::Sender<RuntimeEvent>, view: &ConnectionView) {
    emit(
        events,
        RuntimeEvent::Connection {
            side: Side::Dispatch,
            view: view.clone(),
        },
    )
    .await;
}

async fn emit_log(
    events: &mpsc::Sender<RuntimeEvent>,
    direction: Direction,
    severity: Severity,
    category: LogCategory,
    summary: impl Into<String>,
) {
    emit(
        events,
        RuntimeEvent::Log(LogEntry::new(
            Side::Dispatch,
            direction,
            severity,
            category,
            summary,
        )),
    )
    .await;
}

async fn emit(events: &mpsc::Sender<RuntimeEvent>, event: RuntimeEvent) {
    let _ = events.send(event).await;
}

#[cfg(test)]
mod tests {
    use iec104::{
        cot::Cot,
        types::{
            GenericObject, InformationObjects, MItNa1, MMeNc1, MSpNa1,
            information_elements::{Siq, Spi},
            quality_descriptors::{Qds, SeqQd},
        },
        types_id::TypeId,
    };

    use super::*;
    use crate::{config::load_dispatch_points, protocol::asdu::make_asdu};

    fn request(action: DispatchAction) -> DispatchRequest {
        DispatchRequest {
            action,
            ioa: 201,
            value: 1.0,
            phase: crate::model::ControlPhase::Execute,
            common_address: 1,
            originator_address: 7,
            qualifier: 0,
            test: false,
            qoi: 20,
            qcc_request: 5,
            qcc_freeze: 0,
            repeat: 1,
            interval_ms: 100,
            clock_time_ms: None,
        }
    }

    fn response_for(
        request: &DispatchRequest,
        cot: Cot,
        negative: bool,
        common_address: u16,
    ) -> Asdu {
        let mut response = build_dispatch_request(request)
            .expect("valid request")
            .expect("application ASDU");
        response.cot = cot;
        response.negative = negative;
        response.address_field = common_address;
        response
    }

    fn general_data(common_address: u16, originator_address: u8) -> Asdu {
        make_asdu(
            TypeId::M_SP_NA_1,
            Cot::InterrogationGeneral,
            common_address,
            originator_address,
            false,
            InformationObjects::MSpNa1(vec![GenericObject {
                address: 101,
                object: MSpNa1 {
                    siq: Siq {
                        spi: Spi::On,
                        ..Siq::default()
                    },
                },
            }]),
        )
    }

    #[tokio::test]
    async fn spontaneous_data_emits_current_values_without_an_interrogation_round() {
        let points = load_dispatch_points(crate::config::DEFAULT_DISPATCH_POINTS_PATH)
            .expect("dispatch points");
        let expected = ExpectedPoints::new(points);
        let (events_tx, mut events_rx) = mpsc::channel(8);
        let mut rounds = Rounds::default();
        let mut applications = PendingApplications::default();
        let mut data = general_data(1, 0);
        data.cot = Cot::SpontaneousData;

        handle_incoming(
            data,
            1,
            &mut rounds,
            &mut applications,
            &expected,
            &events_tx,
        )
        .await;

        handle_incoming(
            make_asdu(
                TypeId::M_ME_NC_1,
                Cot::SpontaneousData,
                1,
                0,
                false,
                InformationObjects::MMeNc1(vec![GenericObject {
                    address: 16_385,
                    object: MMeNc1 {
                        value: 12.5,
                        qds: Qds::default(),
                    },
                }]),
            ),
            1,
            &mut rounds,
            &mut applications,
            &expected,
            &events_tx,
        )
        .await;

        let events = std::iter::from_fn(|| events_rx.try_recv().ok()).collect::<Vec<_>>();
        let values = events
            .iter()
            .find_map(|event| match event {
                RuntimeEvent::DispatchValues {
                    kind: SnapshotKind::General,
                    values,
                } => Some(values),
                _ => None,
            })
            .expect("dispatch current values");
        assert_eq!(values[0].ioa, 101);
        assert_eq!(values[0].value.as_deref(), Some("1/合"));
        let float = events
            .iter()
            .filter_map(|event| match event {
                RuntimeEvent::DispatchValues {
                    kind: SnapshotKind::General,
                    values,
                } => Some(values),
                _ => None,
            })
            .flatten()
            .find(|value| value.ioa == 16_385)
            .expect("spontaneous float current value");
        assert_eq!(float.value.as_deref(), Some("12.500000"));
        assert!(rounds.is_idle());
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, RuntimeEvent::DispatchSnapshot { .. }))
        );
    }

    #[tokio::test]
    async fn test_negative_and_other_station_data_do_not_update_current_values() {
        let (events_tx, mut events_rx) = mpsc::channel(8);
        let mut wrong_ca = general_data(2, 0);
        wrong_ca.cot = Cot::SpontaneousData;
        emit_current_values(&wrong_ca, 1, &events_tx).await;

        let mut test_data = general_data(1, 0);
        test_data.cot = Cot::SpontaneousData;
        test_data.test = true;
        emit_current_values(&test_data, 1, &events_tx).await;

        let mut negative = general_data(1, 0);
        negative.cot = Cot::SpontaneousData;
        negative.negative = true;
        emit_current_values(&negative, 1, &events_tx).await;

        assert!(events_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn general_snapshot_is_emitted_only_after_actterm() {
        let points = load_dispatch_points(crate::config::DEFAULT_DISPATCH_POINTS_PATH)
            .expect("dispatch points");
        let expected = ExpectedPoints::new(points);
        let (events_tx, mut events_rx) = mpsc::channel(16);
        let mut rounds = Rounds::default();
        let mut applications = PendingApplications::default();
        let request = request(DispatchAction::GeneralInterrogation);
        rounds
            .reserve(&request, 1, Duration::from_secs(1), &events_tx)
            .await
            .expect("reserve");
        let _ = events_rx.recv().await.expect("pending event");

        handle_incoming(
            general_data(1, 7),
            1,
            &mut rounds,
            &mut applications,
            &expected,
            &events_tx,
        )
        .await;
        assert!(rounds.general.as_ref().expect("round").values.is_empty());

        let actcon = response_for(&request, Cot::ActivationConfirmation, false, 1);
        handle_incoming(
            actcon,
            1,
            &mut rounds,
            &mut applications,
            &expected,
            &events_tx,
        )
        .await;
        assert_eq!(
            rounds.general.as_ref().expect("round").phase,
            RoundPhase::Collecting
        );

        handle_incoming(
            general_data(1, 7),
            1,
            &mut rounds,
            &mut applications,
            &expected,
            &events_tx,
        )
        .await;
        assert_eq!(rounds.general.as_ref().expect("round").values.len(), 1);

        let end = response_for(&request, Cot::ActivationTermination, false, 1);
        handle_incoming(
            end,
            1,
            &mut rounds,
            &mut applications,
            &expected,
            &events_tx,
        )
        .await;
        let emitted = std::iter::from_fn(|| events_rx.try_recv().ok()).collect::<Vec<_>>();
        let (kind, values) = emitted
            .iter()
            .find_map(|event| match event {
                RuntimeEvent::DispatchSnapshot { kind, values } => Some((*kind, values)),
                _ => None,
            })
            .expect("snapshot event");
        assert_eq!(kind, SnapshotKind::General);
        let row = values
            .iter()
            .find(|row| row.ioa == 101)
            .expect("configured row");
        assert_eq!(row.value.as_deref(), Some("1/合"));
        assert!(matches!(
            emitted.iter().find(|event| matches!(
                event,
                RuntimeEvent::RoundPending {
                    kind: SnapshotKind::General,
                    pending: false
                }
            )),
            Some(RuntimeEvent::RoundPending {
                kind: SnapshotKind::General,
                pending: false
            })
        ));
    }

    #[tokio::test]
    async fn duplicate_general_round_is_rejected_locally() {
        let (events_tx, mut events_rx) = mpsc::channel(4);
        let mut rounds = Rounds::default();
        let request = request(DispatchAction::GeneralInterrogation);
        rounds
            .reserve(&request, 1, Duration::from_secs(1), &events_tx)
            .await
            .expect("first reserve");
        let _ = events_rx.recv().await;
        let error = rounds
            .reserve(&request, 1, Duration::from_secs(1), &events_tx)
            .await
            .expect_err("duplicate should fail");
        assert!(error.contains("已有未完成轮次"));
    }

    #[tokio::test]
    async fn counter_round_requires_actcon_and_commits_only_on_matching_actterm() {
        let points = load_dispatch_points(crate::config::DEFAULT_DISPATCH_POINTS_PATH)
            .expect("dispatch points");
        let expected = ExpectedPoints::new(points);
        let request = request(DispatchAction::CounterInterrogation);
        let (events_tx, mut events_rx) = mpsc::channel(32);
        let mut rounds = Rounds::default();
        let mut applications = PendingApplications::default();
        rounds
            .reserve(&request, 1, Duration::from_secs(1), &events_tx)
            .await
            .expect("reserve CI");
        let _ = events_rx.recv().await;

        handle_incoming(
            response_for(&request, Cot::ActivationConfirmation, false, 1),
            1,
            &mut rounds,
            &mut applications,
            &expected,
            &events_tx,
        )
        .await;
        let data = make_asdu(
            TypeId::M_IT_NA_1,
            Cot::CounterInterrogationGeneral,
            1,
            7,
            false,
            InformationObjects::MItNa1(vec![GenericObject {
                address: 106,
                object: MItNa1 {
                    bcr: 55,
                    sqd: SeqQd {
                        seq: 3,
                        ..SeqQd::default()
                    },
                },
            }]),
        );
        handle_incoming(
            data,
            1,
            &mut rounds,
            &mut applications,
            &expected,
            &events_tx,
        )
        .await;
        handle_incoming(
            response_for(&request, Cot::ActivationTermination, false, 1),
            1,
            &mut rounds,
            &mut applications,
            &expected,
            &events_tx,
        )
        .await;

        let emitted = std::iter::from_fn(|| events_rx.try_recv().ok()).collect::<Vec<_>>();
        let values = emitted
            .iter()
            .find_map(|event| match event {
                RuntimeEvent::DispatchSnapshot {
                    kind: SnapshotKind::Energy,
                    values,
                } => Some(values),
                _ => None,
            })
            .expect("energy snapshot");
        let counter = values.iter().find(|row| row.ioa == 106).expect("counter");
        assert_eq!(counter.value.as_deref(), Some("55"));
        assert_eq!(counter.quality, "GOOD|S3");
    }

    #[test]
    fn broadcast_round_accepts_only_the_configured_concrete_address() {
        let mut request = request(DispatchAction::GeneralInterrogation);
        request.common_address = u16::MAX;
        let mut round = InterrogationRound {
            request: request.clone(),
            response_common_address: 1,
            phase: RoundPhase::AwaitActcon,
            deadline: Instant::now() + Duration::from_secs(1),
            values: Vec::new(),
        };
        assert!(matches!(
            evaluate_round(
                &mut round,
                SnapshotKind::General,
                &response_for(&request, Cot::ActivationConfirmation, false, 1)
            ),
            RoundOutcome::Confirmed
        ));

        round.phase = RoundPhase::AwaitActcon;
        assert!(matches!(
            evaluate_round(
                &mut round,
                SnapshotKind::General,
                &response_for(&request, Cot::ActivationConfirmation, false, u16::MAX)
            ),
            RoundOutcome::Mismatch(_)
        ));
    }

    #[tokio::test]
    async fn control_responses_require_all_correlation_fields_and_actcon_before_actterm() {
        let (events_tx, mut events_rx) = mpsc::channel(16);
        let request = request(DispatchAction::SingleControl);
        let mut pending = PendingApplications::default();
        pending
            .register(request.clone(), 1, Duration::from_secs(1))
            .expect("register");

        let mut wrong_value_request = request.clone();
        wrong_value_request.value = 0.0;
        pending
            .handle(
                &response_for(&wrong_value_request, Cot::ActivationConfirmation, false, 1),
                &events_tx,
            )
            .await;
        assert_eq!(pending.entries.len(), 1);
        assert!(!pending.entries[0].accepted);

        pending
            .handle(
                &response_for(&request, Cot::ActivationTermination, false, 1),
                &events_tx,
            )
            .await;
        assert_eq!(pending.entries.len(), 1);
        assert!(!pending.entries[0].accepted);

        pending
            .handle(
                &response_for(&request, Cot::ActivationConfirmation, false, 1),
                &events_tx,
            )
            .await;
        assert!(pending.entries[0].accepted);
        pending
            .handle(
                &response_for(&request, Cot::ActivationTermination, false, 1),
                &events_tx,
            )
            .await;
        assert!(pending.is_empty());
        assert!(
            std::iter::from_fn(|| events_rx.try_recv().ok()).any(|event| matches!(
                event,
                RuntimeEvent::Log(entry) if entry.summary.contains("字段全部匹配")
            ))
        );
    }

    #[test]
    fn type_mismatch_does_not_fill_a_configured_snapshot_row() {
        let points = load_dispatch_points(crate::config::DEFAULT_DISPATCH_POINTS_PATH)
            .expect("dispatch points");
        let expected = ExpectedPoints::new(points);
        let configured_ioa = expected
            .general_views()
            .into_iter()
            .find(|row| row.type_id == "M_SP_NA_1")
            .expect("configured single point")
            .ioa;
        let rows = rows_from_asdu(&make_asdu(
            TypeId::M_DP_NA_1,
            Cot::InterrogationGeneral,
            1,
            0,
            false,
            InformationObjects::MDpNa1(vec![GenericObject {
                address: configured_ioa,
                object: iec104::types::MDpNa1::default(),
            }]),
        ));
        let merged = expected.merge_general(rows);
        let configured = merged
            .iter()
            .find(|row| row.ioa == configured_ioa && row.configured)
            .expect("configured row");
        assert!(configured.value.is_none());
        assert!(merged.iter().any(|row| {
            row.ioa == configured_ioa && !row.configured && row.name.contains("类型不匹配")
        }));
    }

    #[tokio::test]
    async fn shutdown_cancels_an_in_progress_tcp_connect() {
        let config = crate::config::load_from(crate::config::MAIN_CONFIG_PATH).expect("config");
        let mut settings = config.dispatch;
        settings.target_host = "192.0.2.1".to_owned();
        settings.target_port = 65_000;
        let mut protocol = config.protocol;
        protocol.t0 = Duration::from_secs(10);
        let (_commands_tx, commands_rx) = mpsc::channel(4);
        let (events_tx, _events_rx) = mpsc::channel(32);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let task = tokio::spawn(run(
            settings,
            protocol,
            config.dispatch_points,
            commands_rx,
            events_tx,
            shutdown_rx,
        ));
        tokio::task::yield_now().await;
        shutdown_tx.send(true).expect("shutdown");
        tokio::time::timeout(Duration::from_millis(500), task)
            .await
            .expect("connect should be cancellable")
            .expect("dispatch task");
    }
}
