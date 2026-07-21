use std::{collections::VecDeque, net::SocketAddr, time::Duration};

use iec104::{
    asdu::Asdu,
    cot::Cot,
    types::{
        GenericObject, InformationObjects, MDpNa1, MDpTb1, MEiNa1, MItNa1, MMeNa1, MMeNc1, MMeNd1,
        MSpNa1, MSpTb1,
        information_elements::{Coi, Diq, Dpi, Lpc, SelectExecute, Siq, Spi},
        quality_descriptors::{Qds, SeqQd},
    },
    types_id::TypeId,
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{mpsc, watch},
    time::{Instant, MissedTickBehavior, interval},
};

use crate::{
    config::{CollectPointConfig, CollectSettings, ProtocolSettings},
    model::{
        AddressingMode, CollectCommand, CollectPointKind, ConnectionPhase, ConnectionView,
        ControlKind, ControlPhase, DefaultResponseMode, Direction, FaultPolicy, LogCategory,
        LogEntry, PointView, RuntimeEvent, Severity, Side, UploadKind, now_millis,
    },
    protocol::{
        asdu::{
            confirmation, counter_interrogation_cot, current_cp56_time, interrogation_cot,
            make_asdu, qoi_group,
        },
        wire::{LinkRole, LinkSession, WireEvent, read_apdu, tick_interval},
    },
};

const MAX_ASDU_BYTES: usize = 249;
const MAX_DEFERRED_ASDUS: usize = 256;

pub async fn run(
    settings: CollectSettings,
    protocol: ProtocolSettings,
    initial_points: Vec<CollectPointConfig>,
    mut commands: mpsc::Receiver<CollectCommand>,
    events: mpsc::Sender<RuntimeEvent>,
    mut shutdown: watch::Receiver<bool>,
) {
    let endpoint = format!("{}:{}", settings.bind_host, settings.bind_port);
    let mut view = ConnectionView::new(ConnectionPhase::Listening, endpoint.clone());
    let mut station = Station::new(initial_points);
    let mut fault_policy = None;
    let mut prepared_reload = None;

    emit(&events, RuntimeEvent::CollectValues(station.views())).await;
    emit_connection(&events, &view).await;

    let listener = match TcpListener::bind(&endpoint).await {
        Ok(listener) => listener,
        Err(error) => {
            view.phase = ConnectionPhase::Error;
            view.detail = format!("监听失败: {error}");
            emit_connection(&events, &view).await;
            emit_log(
                &events,
                Side::Collect,
                Direction::Internal,
                Severity::Error,
                LogCategory::Connection,
                format!("采集侧监听 {endpoint} 失败: {error}"),
            )
            .await;
            return;
        }
    };

    emit_log(
        &events,
        Side::Collect,
        Direction::Internal,
        Severity::Success,
        LogCategory::Connection,
        format!("采集侧正在监听 {endpoint}"),
    )
    .await;

    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
            command = commands.recv() => {
                match command {
                    None => break,
                    Some(CollectCommand::SetFaultPolicy(policy)) => {
                        fault_policy = policy;
                        emit(&events, RuntimeEvent::FaultPolicyChanged(policy)).await;
                    }
                    Some(CollectCommand::SetPointValue { ioa, type_id, value }) => {
                        update_station_value(ioa, &type_id, value, &mut station, &events).await;
                    }
                    Some(CollectCommand::PrepareReload { generation, points, reply }) => {
                        prepared_reload = Some((generation, station.reconfigured(points)));
                        let _ = reply.send(Ok(()));
                    }
                    Some(CollectCommand::CommitReload { generation, reply }) => {
                        let result = commit_station_reload(
                            generation,
                            &mut prepared_reload,
                            &mut station,
                            &events,
                        ).await;
                        let _ = reply.send(result);
                    }
                    Some(CollectCommand::AbortReload(generation)) => {
                        abort_prepared_reload(generation, &mut prepared_reload);
                    }
                    Some(CollectCommand::Upload(kind)) => {
                        emit_log(
                            &events,
                            Side::Collect,
                            Direction::Internal,
                            Severity::Warning,
                            LogCategory::ActiveUpload,
                            format!("{}失败：采集链路尚未连接", kind.label()),
                        ).await;
                    }
                }
            }
            accepted = listener.accept() => {
                match accepted {
                    Ok((stream, peer)) => {
                        view.phase = ConnectionPhase::ConnectedStopped;
                        view.peer = Some(peer.to_string());
                        view.detail = "等待主站 STARTDT".to_owned();
                        emit_connection(&events, &view).await;
                        emit_log(
                            &events,
                            Side::Collect,
                            Direction::Incoming,
                            Severity::Success,
                            LogCategory::Connection,
                            format!("采集主站 {peer} 已建立 TCP 连接，等待 STARTDT"),
                        ).await;

                        let exit = run_session(
                            stream,
                            peer,
                            &settings,
                            &protocol,
                            &mut station,
                            &mut fault_policy,
                            &mut prepared_reload,
                            &mut commands,
                            &events,
                            &mut view,
                            &mut shutdown,
                        ).await;
                        if matches!(exit, SessionExit::Shutdown) {
                            break;
                        }
                        view.phase = ConnectionPhase::Listening;
                        view.peer = None;
                        view.detail = "等待采集主站连接".to_owned();
                        emit_connection(&events, &view).await;
                    }
                    Err(error) => {
                        emit_log(
                            &events,
                            Side::Collect,
                            Direction::Internal,
                            Severity::Error,
                            LogCategory::Connection,
                            format!("接受采集连接失败: {error}"),
                        ).await;
                    }
                }
            }
        }
    }

    view.phase = ConnectionPhase::Disabled;
    view.peer = None;
    view.detail = "采集侧任务已停止".to_owned();
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
    peer: SocketAddr,
    settings: &CollectSettings,
    protocol: &ProtocolSettings,
    station: &mut Station,
    fault_policy: &mut Option<FaultPolicy>,
    prepared_reload: &mut Option<(u64, Station)>,
    commands: &mut mpsc::Receiver<CollectCommand>,
    events: &mpsc::Sender<RuntimeEvent>,
    view: &mut ConnectionView,
    shutdown: &mut watch::Receiver<bool>,
) -> SessionExit {
    if let Err(error) = stream.set_nodelay(true) {
        emit_log(
            events,
            Side::Collect,
            Direction::Internal,
            Severity::Warning,
            LogCategory::Connection,
            format!("设置 TCP_NODELAY 失败: {error}"),
        )
        .await;
    }
    let (mut reader, writer) = stream.into_split();
    let mut session = LinkSession::new(LinkRole::Server, writer, protocol.to_iec104());
    let mut ticker = interval(tick_interval());
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut delayed_controls = Vec::new();
    let mut deferred_asdus = VecDeque::new();

    loop {
        let mut reload_resolved = false;
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return SessionExit::Shutdown;
                }
            }
            command = commands.recv() => {
                match command {
                    None => return SessionExit::Shutdown,
                    Some(CollectCommand::SetFaultPolicy(policy)) => {
                        *fault_policy = policy;
                        emit(events, RuntimeEvent::FaultPolicyChanged(policy)).await;
                        emit_log(
                            events,
                            Side::Collect,
                            Direction::Internal,
                            Severity::Warning,
                            LogCategory::Control,
                            policy.map_or_else(
                                || "已清除一次性采集控制响应策略".to_owned(),
                                |value| format!("已设置：{}", value.label()),
                            ),
                        ).await;
                    }
                    Some(CollectCommand::SetPointValue { ioa, type_id, value }) => {
                        update_station_value(ioa, &type_id, value, station, events).await;
                    }
                    Some(CollectCommand::PrepareReload { generation, points, reply }) => {
                        if delayed_controls.is_empty() {
                            *prepared_reload = Some((generation, station.reconfigured(points)));
                            let _ = reply.send(Ok(()));
                        } else {
                            let _ = reply.send(Err(format!(
                                "采集侧有 {} 条延迟控制回执未完成",
                                delayed_controls.len()
                            )));
                        }
                    }
                    Some(CollectCommand::CommitReload { generation, reply }) => {
                        let result = if delayed_controls.is_empty() {
                            commit_station_reload(
                                generation,
                                prepared_reload,
                                station,
                                events,
                            ).await
                        } else {
                            Err(format!(
                                "采集侧提交时仍有 {} 条延迟控制回执未完成",
                                delayed_controls.len()
                            ))
                        };
                        reload_resolved = result.is_ok();
                        let _ = reply.send(result);
                    }
                    Some(CollectCommand::AbortReload(generation)) => {
                        reload_resolved = abort_prepared_reload(generation, prepared_reload);
                    }
                    Some(CollectCommand::Upload(kind)) => {
                        if let Err(error) = handle_upload(
                            kind,
                            station,
                            &mut session,
                            settings.common_address,
                            protocol.originator_address,
                            events,
                            view,
                        ).await {
                            emit_log(
                                events,
                                Side::Collect,
                                Direction::Internal,
                                Severity::Error,
                                LogCategory::ActiveUpload,
                                format!("{}失败: {error}", kind.label()),
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
                            Side::Collect,
                            Direction::Incoming,
                            Severity::Warning,
                            LogCategory::Connection,
                            format!("采集连接 {peer} 已结束: {error}"),
                        ).await;
                        return SessionExit::Disconnected;
                    }
                };
                let processed = match session.process(apdu.frame).await {
                    Ok(processed) => processed,
                    Err(error) => {
                        emit_log(
                            events,
                            Side::Collect,
                            Direction::Internal,
                            Severity::Error,
                            LogCategory::Protocol,
                            format!("采集链路处理失败: {error}"),
                        ).await;
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
                }
                if let Some(asdu) = processed.asdu {
                    if prepared_reload.is_some() {
                        if deferred_asdus.len() >= MAX_DEFERRED_ASDUS {
                            emit_log(
                                events,
                                Side::Collect,
                                Direction::Internal,
                                Severity::Error,
                                LogCategory::Configuration,
                                "点表重载准备期间的业务 ASDU 缓冲已满，为避免语义混用已断开链路",
                            ).await;
                            return SessionExit::Disconnected;
                        }
                        deferred_asdus.push_back(asdu);
                    } else if process_business_asdu(
                        asdu,
                        station,
                        &mut session,
                        settings,
                        fault_policy,
                        &mut delayed_controls,
                        events,
                        view,
                    ).await {
                        return SessionExit::Disconnected;
                    }
                }
            }
            _ = ticker.tick() => {
                if let Err(error) = flush_delayed_controls(
                    &mut delayed_controls,
                    station,
                    &mut session,
                    settings,
                    events,
                    view,
                ).await {
                    emit_log(
                        events,
                        Side::Collect,
                        Direction::Internal,
                        Severity::Error,
                        LogCategory::Control,
                        format!("发送延迟控制回执失败: {error}"),
                    ).await;
                    return SessionExit::Disconnected;
                }
                match session.tick().await {
                    Ok(wire_events) => {
                        for event in wire_events {
                            record_wire(events, view, event).await;
                        }
                    }
                    Err(error) => {
                        emit_log(
                            events,
                            Side::Collect,
                            Direction::Internal,
                            Severity::Error,
                            LogCategory::KeepAlive,
                            format!("采集链路计时器失败: {error}"),
                        ).await;
                        return SessionExit::Disconnected;
                    }
                }
            }
        }
        if reload_resolved {
            while let Some(asdu) = deferred_asdus.pop_front() {
                if process_business_asdu(
                    asdu,
                    station,
                    &mut session,
                    settings,
                    fault_policy,
                    &mut delayed_controls,
                    events,
                    view,
                )
                .await
                {
                    return SessionExit::Disconnected;
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn process_business_asdu(
    asdu: Asdu,
    station: &mut Station,
    session: &mut LinkSession,
    settings: &CollectSettings,
    fault_policy: &mut Option<FaultPolicy>,
    delayed_controls: &mut Vec<DelayedControlResponse>,
    events: &mpsc::Sender<RuntimeEvent>,
    view: &mut ConnectionView,
) -> bool {
    match handle_asdu(
        asdu,
        station,
        session,
        settings,
        fault_policy,
        delayed_controls,
        events,
        view,
    )
    .await
    {
        Ok(disconnect) => disconnect,
        Err(error) => {
            emit_log(
                events,
                Side::Collect,
                Direction::Internal,
                Severity::Error,
                LogCategory::Protocol,
                format!("处理采集请求失败: {error}"),
            )
            .await;
            false
        }
    }
}

async fn handle_upload(
    kind: UploadKind,
    station: &mut Station,
    session: &mut LinkSession,
    common_address: u16,
    originator_address: u8,
    events: &mpsc::Sender<RuntimeEvent>,
    view: &mut ConnectionView,
) -> Result<(), String> {
    if !session.is_active() {
        return Err(format!(
            "链路状态 {}，需要先完成 STARTDT",
            session.phase().label()
        ));
    }

    if kind == UploadKind::InitializationEnd {
        let asdu = make_asdu(
            TypeId::M_EI_NA_1,
            Cot::Initiated,
            common_address,
            originator_address,
            false,
            InformationObjects::MEiNa1(vec![GenericObject {
                address: 0,
                object: MEiNa1 {
                    lpc: Lpc::NoChange,
                    coi: Coi::LocalPowerOn,
                },
            }]),
        );
        send_asdu(session, asdu, events, view).await?;
        return Ok(());
    }

    let mut candidate = station.clone();
    let selected = candidate.refresh(kind);
    if selected == 0 {
        return Err("点表中没有与该操作匹配的可上送点".to_owned());
    }
    let frames = candidate.frames_for_upload(kind, common_address, originator_address)?;
    if frames.is_empty() {
        return Err("没有生成任何 ASDU".to_owned());
    }
    for asdu in frames {
        send_asdu(session, asdu, events, view).await?;
    }
    *station = candidate;
    emit(events, RuntimeEvent::CollectValues(station.views())).await;
    emit_log(
        events,
        Side::Collect,
        Direction::Outgoing,
        Severity::Success,
        LogCategory::ActiveUpload,
        if kind == UploadKind::CurrentValues {
            format!("{}完成，共上送 {selected} 个点，点值保持不变", kind.label())
        } else {
            format!("{}完成，共刷新 {selected} 个点", kind.label())
        },
    )
    .await;
    Ok(())
}

async fn update_station_value(
    ioa: u32,
    type_id: &str,
    value: f64,
    station: &mut Station,
    events: &mpsc::Sender<RuntimeEvent>,
) {
    match station.set_value(ioa, type_id, value) {
        Ok(display) => {
            emit(events, RuntimeEvent::CollectValues(station.views())).await;
            emit_log(
                events,
                Side::Collect,
                Direction::Internal,
                Severity::Success,
                LogCategory::Configuration,
                format!("手工修改采集点 {type_id}/IOA={ioa}，新值={display}"),
            )
            .await;
        }
        Err(error) => {
            emit_log(
                events,
                Side::Collect,
                Direction::Internal,
                Severity::Error,
                LogCategory::Configuration,
                format!("手工修改采集点失败: {error}"),
            )
            .await;
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_asdu(
    request: Asdu,
    station: &mut Station,
    session: &mut LinkSession,
    settings: &CollectSettings,
    fault_policy: &mut Option<FaultPolicy>,
    delayed_controls: &mut Vec<DelayedControlResponse>,
    events: &mpsc::Sender<RuntimeEvent>,
    view: &mut ConnectionView,
) -> Result<bool, String> {
    if request.address_field != settings.common_address && request.address_field != u16::MAX {
        let response = confirmation(
            &request,
            Cot::UnknownAsduAddress,
            true,
            settings.common_address,
        );
        send_asdu(session, response, events, view).await?;
        return Ok(false);
    }

    match request.information_objects.clone() {
        InformationObjects::CIcNa1(objects) => {
            let Some(object) = objects.first() else {
                return Err("总召请求没有信息体".to_owned());
            };
            let Some(data_cot) = interrogation_cot(object.object.qoi) else {
                let response =
                    confirmation(&request, Cot::UnknownCause, true, settings.common_address);
                send_asdu(session, response, events, view).await?;
                return Ok(false);
            };
            send_asdu(
                session,
                confirmation(
                    &request,
                    Cot::ActivationConfirmation,
                    false,
                    settings.common_address,
                ),
                events,
                view,
            )
            .await?;
            let group = qoi_group(object.object.qoi);
            for asdu in station.general_frames(
                group,
                data_cot,
                settings.common_address,
                request.originator_address,
                request.test,
            )? {
                send_asdu(session, asdu, events, view).await?;
            }
            send_asdu(
                session,
                confirmation(
                    &request,
                    Cot::ActivationTermination,
                    false,
                    settings.common_address,
                ),
                events,
                view,
            )
            .await?;
        }
        InformationObjects::CCiNa1(objects) => {
            let Some(object) = objects.first() else {
                return Err("电度总召请求没有信息体".to_owned());
            };
            let data_cot = counter_interrogation_cot(object.object.rqt);
            send_asdu(
                session,
                confirmation(
                    &request,
                    Cot::ActivationConfirmation,
                    false,
                    settings.common_address,
                ),
                events,
                view,
            )
            .await?;
            let group = counter_group(object.object.rqt.to_byte());
            for asdu in station.counter_frames(
                group,
                data_cot,
                settings.common_address,
                request.originator_address,
                request.test,
            )? {
                send_asdu(session, asdu, events, view).await?;
            }
            send_asdu(
                session,
                confirmation(
                    &request,
                    Cot::ActivationTermination,
                    false,
                    settings.common_address,
                ),
                events,
                view,
            )
            .await?;
        }
        InformationObjects::CCsNa1(_) => {
            send_asdu(
                session,
                confirmation(
                    &request,
                    Cot::ActivationConfirmation,
                    false,
                    settings.common_address,
                ),
                events,
                view,
            )
            .await?;
        }
        InformationObjects::CScNa1(objects) => {
            let Some(object) = objects.first() else {
                return Err("单点控制没有信息体".to_owned());
            };
            let value = match object.object.sco.scs {
                Spi::Off => 0.0,
                Spi::On => 1.0,
            };
            let phase = control_phase(object.object.sco.se);
            return control_response(
                request,
                object.address,
                ControlKind::Single,
                value,
                phase,
                station,
                session,
                settings,
                fault_policy,
                delayed_controls,
                events,
                view,
            )
            .await;
        }
        InformationObjects::CdcNa1(objects) => {
            let Some(object) = objects.first() else {
                return Err("双点控制没有信息体".to_owned());
            };
            let value = match object.object.dco.dcs {
                Dpi::Off => 0.0,
                Dpi::On => 1.0,
                Dpi::Indeterminate | Dpi::Invalid => f64::NAN,
            };
            let phase = control_phase(object.object.dco.se);
            return control_response(
                request,
                object.address,
                ControlKind::Double,
                value,
                phase,
                station,
                session,
                settings,
                fault_policy,
                delayed_controls,
                events,
                view,
            )
            .await;
        }
        InformationObjects::CrcNa1(objects) => {
            let Some(object) = objects.first() else {
                return Err("升降控制没有信息体".to_owned());
            };
            let value = match object.object.rco.rcs {
                iec104::types::commands::Rcs::Decrement => -1.0,
                iec104::types::commands::Rcs::Increment => 1.0,
                iec104::types::commands::Rcs::None | iec104::types::commands::Rcs::Invalid => 0.0,
            };
            let phase = control_phase(object.object.rco.se);
            return control_response(
                request,
                object.address,
                ControlKind::Regulating,
                value,
                phase,
                station,
                session,
                settings,
                fault_policy,
                delayed_controls,
                events,
                view,
            )
            .await;
        }
        InformationObjects::CSeNc1(objects) => {
            let Some(object) = objects.first() else {
                return Err("短浮点遥调没有信息体".to_owned());
            };
            let phase = control_phase(object.object.qos.se);
            return control_response(
                request,
                object.address,
                ControlKind::Setpoint,
                f64::from(object.object.value),
                phase,
                station,
                session,
                settings,
                fault_policy,
                delayed_controls,
                events,
                view,
            )
            .await;
        }
        _ => {
            emit_log(
                events,
                Side::Collect,
                Direction::Incoming,
                Severity::Warning,
                LogCategory::Protocol,
                format!("当前采集模拟器不处理 TypeID={:?}", request.type_id),
            )
            .await;
        }
    }
    Ok(false)
}

#[allow(clippy::too_many_arguments)]
async fn control_response(
    request: Asdu,
    ioa: u32,
    control_kind: ControlKind,
    requested_value: f64,
    phase: ControlPhase,
    station: &mut Station,
    session: &mut LinkSession,
    settings: &CollectSettings,
    fault_policy: &mut Option<FaultPolicy>,
    delayed_controls: &mut Vec<DelayedControlResponse>,
    events: &mpsc::Sender<RuntimeEvent>,
    view: &mut ConnectionView,
) -> Result<bool, String> {
    let desired = match station.control_value(ioa, control_kind, requested_value) {
        Ok(value) => value,
        Err(error) => {
            emit_log(
                events,
                Side::Collect,
                Direction::Internal,
                Severity::Warning,
                LogCategory::Control,
                error,
            )
            .await;
            send_asdu(
                session,
                confirmation(
                    &request,
                    Cot::UnknownObjectAddress,
                    true,
                    settings.common_address,
                ),
                events,
                view,
            )
            .await?;
            return Ok(false);
        }
    };

    let point = station
        .point(ioa, control_kind)
        .ok_or_else(|| format!("控制目标 IOA={ioa} 消失"))?;
    let mut decision = match point.config.response {
        DefaultResponseMode::Success => ControlDecision::Success,
        DefaultResponseMode::Reject => ControlDecision::Reject,
        DefaultResponseMode::ActConOnly => ControlDecision::ActConOnly,
        DefaultResponseMode::Silent => ControlDecision::Silent,
        DefaultResponseMode::DelaySuccess => ControlDecision::Delay(point.config.response_delay),
    };
    if fault_policy.is_some_and(|policy| policy.matches(phase)) {
        let policy = fault_policy.take().expect("fault policy checked above");
        emit(events, RuntimeEvent::FaultPolicyChanged(None)).await;
        decision = match policy {
            FaultPolicy::Success => ControlDecision::Success,
            FaultPolicy::RejectSelect | FaultPolicy::RejectExecute => ControlDecision::Reject,
            FaultPolicy::ActConOnly => ControlDecision::ActConOnly,
            FaultPolicy::Silent => ControlDecision::Silent,
            FaultPolicy::DelaySuccess => ControlDecision::Delay(settings.fault_delay),
            FaultPolicy::Disconnect => ControlDecision::Disconnect,
        };
    }

    match decision {
        ControlDecision::Disconnect => {
            emit_log(
                events,
                Side::Collect,
                Direction::Internal,
                Severity::Warning,
                LogCategory::Control,
                format!("一次性策略命中：收到 IOA={ioa} 后立即断开采集连接"),
            )
            .await;
            return Ok(true);
        }
        ControlDecision::Silent => {
            emit_log(
                events,
                Side::Collect,
                Direction::Internal,
                Severity::Warning,
                LogCategory::Control,
                format!("一次性/点位策略命中：IOA={ioa} 完全静默"),
            )
            .await;
            return Ok(false);
        }
        ControlDecision::Delay(delay) => {
            let delay = if delay.is_zero() {
                settings.fault_delay
            } else {
                delay
            };
            emit_log(
                events,
                Side::Collect,
                Direction::Internal,
                Severity::Warning,
                LogCategory::Control,
                format!("IOA={ioa} 延迟 {} ms 后应答", delay.as_millis()),
            )
            .await;
            delayed_controls.push(DelayedControlResponse {
                due: Instant::now() + delay,
                request,
                ioa,
                control_kind,
                desired,
                phase,
            });
            delayed_controls.sort_by_key(|pending| pending.due);
            return Ok(false);
        }
        ControlDecision::Reject => {
            send_asdu(
                session,
                confirmation(
                    &request,
                    Cot::ActivationConfirmation,
                    true,
                    settings.common_address,
                ),
                events,
                view,
            )
            .await?;
            return Ok(false);
        }
        ControlDecision::Success | ControlDecision::ActConOnly => {}
    }

    complete_success_control(
        request,
        ioa,
        control_kind,
        desired,
        phase,
        matches!(decision, ControlDecision::ActConOnly),
        station,
        session,
        settings,
        events,
        view,
    )
    .await?;
    Ok(false)
}

#[derive(Debug)]
struct DelayedControlResponse {
    due: Instant,
    request: Asdu,
    ioa: u32,
    control_kind: ControlKind,
    desired: f64,
    phase: ControlPhase,
}

#[allow(clippy::too_many_arguments)]
async fn complete_success_control(
    request: Asdu,
    ioa: u32,
    control_kind: ControlKind,
    desired: f64,
    phase: ControlPhase,
    actcon_only: bool,
    station: &mut Station,
    session: &mut LinkSession,
    settings: &CollectSettings,
    events: &mpsc::Sender<RuntimeEvent>,
    view: &mut ConnectionView,
) -> Result<(), String> {
    send_asdu(
        session,
        confirmation(
            &request,
            Cot::ActivationConfirmation,
            false,
            settings.common_address,
        ),
        events,
        view,
    )
    .await?;
    if phase == ControlPhase::Select || actcon_only {
        return Ok(());
    }
    send_asdu(
        session,
        confirmation(
            &request,
            Cot::ActivationTermination,
            false,
            settings.common_address,
        ),
        events,
        view,
    )
    .await?;
    station.commit_control(ioa, control_kind, desired)?;
    emit(events, RuntimeEvent::CollectValues(station.views())).await;
    emit_log(
        events,
        Side::Collect,
        Direction::Internal,
        Severity::Success,
        LogCategory::Control,
        format!("控制完成并提交点值：IOA={ioa} value={desired}"),
    )
    .await;
    Ok(())
}

async fn flush_delayed_controls(
    pending: &mut Vec<DelayedControlResponse>,
    station: &mut Station,
    session: &mut LinkSession,
    settings: &CollectSettings,
    events: &mpsc::Sender<RuntimeEvent>,
    view: &mut ConnectionView,
) -> Result<(), String> {
    while pending
        .first()
        .is_some_and(|response| response.due <= Instant::now())
    {
        let response = pending.remove(0);
        complete_success_control(
            response.request,
            response.ioa,
            response.control_kind,
            response.desired,
            response.phase,
            false,
            station,
            session,
            settings,
            events,
            view,
        )
        .await?;
    }
    Ok(())
}

#[derive(Debug, Clone, Copy)]
enum ControlDecision {
    Success,
    Reject,
    ActConOnly,
    Silent,
    Delay(Duration),
    Disconnect,
}

#[derive(Debug, Clone)]
struct PointState {
    config: CollectPointConfig,
    value: f64,
    counter_sequence: u8,
    updated_ms: u64,
}

#[derive(Debug, Clone)]
struct Station {
    points: Vec<PointState>,
}

impl Station {
    fn new(points: Vec<CollectPointConfig>) -> Self {
        let updated_ms = now_millis();
        Self {
            points: points
                .into_iter()
                .map(|config| PointState {
                    value: config.initial,
                    config,
                    counter_sequence: 0,
                    updated_ms,
                })
                .collect(),
        }
    }

    fn reconfigured(&self, points: Vec<CollectPointConfig>) -> Self {
        let now = now_millis();
        Self {
            points: points
                .into_iter()
                .map(|config| {
                    let previous = self.points.iter().find(|point| {
                        point.config.ioa == config.ioa && point.config.kind == config.kind
                    });
                    let reusable = previous
                        .filter(|point| point.value >= config.min && point.value <= config.max);
                    PointState {
                        value: reusable.map_or(config.initial, |point| point.value),
                        counter_sequence: reusable.map_or(0, |point| point.counter_sequence),
                        updated_ms: reusable.map_or(now, |point| point.updated_ms),
                        config,
                    }
                })
                .collect(),
        }
    }

    fn views(&self) -> Vec<PointView> {
        let mut rows: Vec<_> = self
            .points
            .iter()
            .map(|point| PointView {
                ioa: point.config.ioa,
                name: point.config.name.clone(),
                type_id: point.config.kind.type_id().to_owned(),
                type_name: point.config.kind.label().to_owned(),
                value: Some(display_value(point)),
                quality: point.config.quality.to_ascii_uppercase(),
                updated_ms: Some(point.updated_ms),
                configured: true,
            })
            .collect();
        rows.sort_by_key(|row| row.ioa);
        rows
    }

    fn refresh(&mut self, upload: UploadKind) -> usize {
        let mut rng = fastrand::Rng::with_seed(fastrand::u64(..));
        self.refresh_with_rng(upload, &mut rng)
    }

    fn refresh_with_rng(&mut self, upload: UploadKind, rng: &mut fastrand::Rng) -> usize {
        if upload == UploadKind::CurrentValues {
            return self.count_for_upload(upload);
        }
        let now = now_millis();
        let mut count = 0;
        for point in &mut self.points {
            if !point.config.purpose.includes_data() || !upload_matches(upload, point) {
                continue;
            }
            // 每个点在循环内独立取样，避免同一批次复用一个随机值。
            point.value = random_value(point, rng);
            if point.config.kind == CollectPointKind::Counter {
                point.counter_sequence = (point.counter_sequence + 1) & 0x1f;
            }
            point.updated_ms = now;
            count += 1;
        }
        count
    }

    fn count_for_upload(&self, upload: UploadKind) -> usize {
        self.points
            .iter()
            .filter(|point| point.config.purpose.includes_data() && upload_matches(upload, point))
            .count()
    }

    fn set_value(&mut self, ioa: u32, type_id: &str, value: f64) -> Result<String, String> {
        let Some(point) = self
            .points
            .iter_mut()
            .find(|point| point.config.ioa == ioa && point.config.kind.type_id() == type_id)
        else {
            return Err(format!("点表中不存在 {type_id}/IOA={ioa}"));
        };
        if !value.is_finite() || value < point.config.min || value > point.config.max {
            return Err(format!(
                "IOA={ioa} 的值必须在 [{}, {}] 范围内",
                point.config.min, point.config.max
            ));
        }
        match point.config.kind {
            CollectPointKind::SinglePoint | CollectPointKind::DoublePoint => {
                if !matches!(value, 0.0 | 1.0) {
                    return Err(format!("IOA={ioa} 的离散值只能是 0 或 1"));
                }
            }
            CollectPointKind::Normalized | CollectPointKind::NormalizedNoQuality => {
                if value.fract() != 0.0 || value < i16::MIN as f64 || value > i16::MAX as f64 {
                    return Err(format!("IOA={ioa} 的归一化值必须是 i16 范围整数"));
                }
            }
            CollectPointKind::Counter => {
                if value.fract() != 0.0 || value < i32::MIN as f64 || value > i32::MAX as f64 {
                    return Err(format!("IOA={ioa} 的电度值必须是 i32 范围整数"));
                }
            }
            CollectPointKind::Float => {}
        }
        point.value = value;
        point.updated_ms = now_millis();
        Ok(display_value(point))
    }

    fn point(&self, ioa: u32, kind: ControlKind) -> Option<&PointState> {
        self.points.iter().find(|point| {
            point.config.ioa == ioa
                && point.config.purpose.includes_control()
                && point.config.control == Some(kind)
        })
    }

    fn control_value(&self, ioa: u32, kind: ControlKind, requested: f64) -> Result<f64, String> {
        let Some(point) = self.point(ioa, kind) else {
            return Err(format!("没有匹配的控制目标：IOA={ioa} type={kind:?}"));
        };
        let desired = match kind {
            ControlKind::Regulating => {
                if !matches!(requested, -1.0 | 1.0) {
                    return Err(format!("IOA={ioa} 升降值只能为 -1 或 1"));
                }
                point.value + requested * point.config.step
            }
            ControlKind::Single | ControlKind::Double => {
                if !matches!(requested, 0.0 | 1.0) {
                    return Err(format!("IOA={ioa} 离散控制值只能为 0 或 1"));
                }
                requested
            }
            ControlKind::Setpoint => requested,
        };
        if !desired.is_finite() || desired < point.config.min || desired > point.config.max {
            return Err(format!(
                "IOA={ioa} 控制目标值 {desired} 超出 [{}, {}]",
                point.config.min, point.config.max
            ));
        }
        Ok(desired)
    }

    fn commit_control(&mut self, ioa: u32, kind: ControlKind, value: f64) -> Result<(), String> {
        let Some(point) = self.points.iter_mut().find(|point| {
            point.config.ioa == ioa
                && point.config.purpose.includes_control()
                && point.config.control == Some(kind)
        }) else {
            return Err(format!("控制提交时未找到 IOA={ioa}"));
        };
        point.value = value;
        point.updated_ms = now_millis();
        Ok(())
    }

    fn frames_for_upload(
        &self,
        upload: UploadKind,
        common_address: u16,
        originator_address: u8,
    ) -> Result<Vec<Asdu>, String> {
        let cot = Cot::SpontaneousData;
        match upload {
            UploadKind::CurrentValues | UploadKind::RefreshAll => {
                let mut frames = Vec::new();
                for kind in all_collect_kinds() {
                    frames.extend(self.frames_for_kind(
                        kind,
                        None,
                        cot,
                        common_address,
                        originator_address,
                        false,
                        None,
                    )?);
                }
                Ok(frames)
            }
            UploadKind::SingleSoe => self.frames_for_kind(
                CollectPointKind::SinglePoint,
                Some(SoeKind::Single),
                cot,
                common_address,
                originator_address,
                false,
                None,
            ),
            UploadKind::DoubleSoe => self.frames_for_kind(
                CollectPointKind::DoublePoint,
                Some(SoeKind::Double),
                cot,
                common_address,
                originator_address,
                false,
                None,
            ),
            _ => {
                let kind = upload_kind(upload)
                    .ok_or_else(|| format!("不支持的主动上送类型: {upload:?}"))?;
                self.frames_for_kind(
                    kind,
                    None,
                    cot,
                    common_address,
                    originator_address,
                    false,
                    None,
                )
            }
        }
    }

    fn general_frames(
        &self,
        group: Option<u8>,
        cot: Cot,
        common_address: u16,
        originator_address: u8,
        test: bool,
    ) -> Result<Vec<Asdu>, String> {
        let mut frames = Vec::new();
        for kind in all_collect_kinds()
            .into_iter()
            .filter(|kind| *kind != CollectPointKind::Counter)
        {
            frames.extend(self.frames_for_kind(
                kind,
                None,
                cot,
                common_address,
                originator_address,
                test,
                group,
            )?);
        }
        Ok(frames)
    }

    fn counter_frames(
        &self,
        group: Option<u8>,
        cot: Cot,
        common_address: u16,
        originator_address: u8,
        test: bool,
    ) -> Result<Vec<Asdu>, String> {
        self.frames_for_kind(
            CollectPointKind::Counter,
            None,
            cot,
            common_address,
            originator_address,
            test,
            group,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn frames_for_kind(
        &self,
        kind: CollectPointKind,
        soe: Option<SoeKind>,
        cot: Cot,
        common_address: u16,
        originator_address: u8,
        test: bool,
        group: Option<u8>,
    ) -> Result<Vec<Asdu>, String> {
        let mut points: Vec<_> = self
            .points
            .iter()
            .filter(|point| {
                point.config.purpose.includes_data()
                    && point.config.kind == kind
                    && group.is_none_or(|group| point.config.group == group)
                    && soe.is_none_or(|_| point.config.soe)
            })
            .cloned()
            .collect();
        points.sort_by_key(|point| point.config.ioa);
        let type_id = collect_type_id(kind, soe)?;
        let chunks = addressing_chunks(points, type_id);
        chunks
            .into_iter()
            .map(|(sequence, points)| {
                point_asdu(
                    kind,
                    soe,
                    sequence,
                    points,
                    cot,
                    common_address,
                    originator_address,
                    test,
                )
            })
            .collect()
    }
}

#[derive(Debug, Clone, Copy)]
enum SoeKind {
    Single,
    Double,
}

#[allow(clippy::too_many_arguments)]
fn point_asdu(
    kind: CollectPointKind,
    soe: Option<SoeKind>,
    sequence: bool,
    points: Vec<PointState>,
    cot: Cot,
    common_address: u16,
    originator_address: u8,
    test: bool,
) -> Result<Asdu, String> {
    let (type_id, information_objects) = match (kind, soe) {
        (CollectPointKind::SinglePoint, Some(SoeKind::Single)) => {
            let time = current_cp56_time()?;
            (
                TypeId::M_SP_TB_1,
                InformationObjects::MSpTb1(
                    points
                        .into_iter()
                        .map(|point| GenericObject {
                            address: point.config.ioa,
                            object: MSpTb1 {
                                siq: make_siq(&point),
                                time: time.clone(),
                            },
                        })
                        .collect(),
                ),
            )
        }
        (CollectPointKind::DoublePoint, Some(SoeKind::Double)) => {
            let time = current_cp56_time()?;
            (
                TypeId::M_DP_TB_1,
                InformationObjects::MDpTb1(
                    points
                        .into_iter()
                        .map(|point| GenericObject {
                            address: point.config.ioa,
                            object: MDpTb1 {
                                diq: make_diq(&point),
                                time: time.clone(),
                            },
                        })
                        .collect(),
                ),
            )
        }
        (CollectPointKind::SinglePoint, None) => (
            TypeId::M_SP_NA_1,
            InformationObjects::MSpNa1(
                points
                    .into_iter()
                    .map(|point| GenericObject {
                        address: point.config.ioa,
                        object: MSpNa1 {
                            siq: make_siq(&point),
                        },
                    })
                    .collect(),
            ),
        ),
        (CollectPointKind::DoublePoint, None) => (
            TypeId::M_DP_NA_1,
            InformationObjects::MDpNa1(
                points
                    .into_iter()
                    .map(|point| GenericObject {
                        address: point.config.ioa,
                        object: MDpNa1 {
                            diq: make_diq(&point),
                        },
                    })
                    .collect(),
            ),
        ),
        (CollectPointKind::Normalized, None) => (
            TypeId::M_ME_NA_1,
            InformationObjects::MMeNa1(
                points
                    .into_iter()
                    .map(|point| GenericObject {
                        address: point.config.ioa,
                        object: MMeNa1 {
                            nva: point.value as i16,
                            qds: make_qds(&point.config.quality),
                        },
                    })
                    .collect(),
            ),
        ),
        (CollectPointKind::NormalizedNoQuality, None) => (
            TypeId::M_ME_ND_1,
            InformationObjects::MMeNd1(
                points
                    .into_iter()
                    .map(|point| GenericObject {
                        address: point.config.ioa,
                        object: MMeNd1 {
                            nva: point.value as i16,
                        },
                    })
                    .collect(),
            ),
        ),
        (CollectPointKind::Float, None) => (
            TypeId::M_ME_NC_1,
            InformationObjects::MMeNc1(
                points
                    .into_iter()
                    .map(|point| GenericObject {
                        address: point.config.ioa,
                        object: MMeNc1 {
                            value: point.value as f32,
                            qds: make_qds(&point.config.quality),
                        },
                    })
                    .collect(),
            ),
        ),
        (CollectPointKind::Counter, None) => (
            TypeId::M_IT_NA_1,
            InformationObjects::MItNa1(
                points
                    .into_iter()
                    .map(|point| GenericObject {
                        address: point.config.ioa,
                        object: MItNa1 {
                            bcr: point.value as i32,
                            sqd: SeqQd {
                                iv: point.config.quality == "invalid",
                                ca: point.config.quality == "substituted",
                                cy: false,
                                seq: point.counter_sequence,
                            },
                        },
                    })
                    .collect(),
            ),
        ),
        _ => return Err(format!("点类型 {kind:?} 与 SOE 模式 {soe:?} 不匹配")),
    };
    let mut asdu = make_asdu(
        type_id,
        cot,
        common_address,
        originator_address,
        test,
        information_objects,
    );
    asdu.sequence = sequence;
    Ok(asdu)
}

fn addressing_chunks(points: Vec<PointState>, type_id: TypeId) -> Vec<(bool, Vec<PointState>)> {
    let mut chunks = Vec::new();
    let individual: Vec<_> = points
        .iter()
        .filter(|point| point.config.addressing == AddressingMode::Individual)
        .cloned()
        .collect();
    for chunk in individual.chunks(frame_object_limit(type_id, false)) {
        chunks.push((false, chunk.to_vec()));
    }

    let sequential: Vec<_> = points
        .into_iter()
        .filter(|point| point.config.addressing != AddressingMode::Individual)
        .collect();
    let mut start = 0;
    while start < sequential.len() {
        let mut end = start + 1;
        while end < sequential.len()
            && end - start < frame_object_limit(type_id, true)
            && sequential[end].config.ioa == sequential[end - 1].config.ioa + 1
        {
            end += 1;
        }
        let chunk = sequential[start..end].to_vec();
        chunks.push((chunk.len() > 1, chunk));
        start = end;
    }
    chunks
}

fn collect_type_id(kind: CollectPointKind, soe: Option<SoeKind>) -> Result<TypeId, String> {
    match (kind, soe) {
        (CollectPointKind::SinglePoint, Some(SoeKind::Single)) => Ok(TypeId::M_SP_TB_1),
        (CollectPointKind::DoublePoint, Some(SoeKind::Double)) => Ok(TypeId::M_DP_TB_1),
        (CollectPointKind::SinglePoint, None) => Ok(TypeId::M_SP_NA_1),
        (CollectPointKind::DoublePoint, None) => Ok(TypeId::M_DP_NA_1),
        (CollectPointKind::Normalized, None) => Ok(TypeId::M_ME_NA_1),
        (CollectPointKind::NormalizedNoQuality, None) => Ok(TypeId::M_ME_ND_1),
        (CollectPointKind::Float, None) => Ok(TypeId::M_ME_NC_1),
        (CollectPointKind::Counter, None) => Ok(TypeId::M_IT_NA_1),
        _ => Err(format!("点类型 {kind:?} 与 SOE 模式 {soe:?} 不匹配")),
    }
}

fn frame_object_limit(type_id: TypeId, sequence: bool) -> usize {
    let object_size = type_id.size();
    let available = MAX_ASDU_BYTES.saturating_sub(6 + usize::from(sequence) * 3);
    let per_object = object_size + if sequence { 0 } else { 3 };
    (available / per_object).clamp(1, 127)
}

fn upload_matches(upload: UploadKind, point: &PointState) -> bool {
    match upload {
        UploadKind::CurrentValues | UploadKind::RefreshAll => true,
        UploadKind::SinglePoint => point.config.kind == CollectPointKind::SinglePoint,
        UploadKind::DoublePoint => point.config.kind == CollectPointKind::DoublePoint,
        UploadKind::Normalized => point.config.kind == CollectPointKind::Normalized,
        UploadKind::NormalizedNoQuality => {
            point.config.kind == CollectPointKind::NormalizedNoQuality
        }
        UploadKind::Float => point.config.kind == CollectPointKind::Float,
        UploadKind::Counter => point.config.kind == CollectPointKind::Counter,
        UploadKind::SingleSoe => {
            point.config.kind == CollectPointKind::SinglePoint && point.config.soe
        }
        UploadKind::DoubleSoe => {
            point.config.kind == CollectPointKind::DoublePoint && point.config.soe
        }
        UploadKind::InitializationEnd => false,
    }
}

fn upload_kind(upload: UploadKind) -> Option<CollectPointKind> {
    match upload {
        UploadKind::SinglePoint => Some(CollectPointKind::SinglePoint),
        UploadKind::DoublePoint => Some(CollectPointKind::DoublePoint),
        UploadKind::Normalized => Some(CollectPointKind::Normalized),
        UploadKind::NormalizedNoQuality => Some(CollectPointKind::NormalizedNoQuality),
        UploadKind::Float => Some(CollectPointKind::Float),
        UploadKind::Counter => Some(CollectPointKind::Counter),
        _ => None,
    }
}

const fn all_collect_kinds() -> [CollectPointKind; 6] {
    [
        CollectPointKind::SinglePoint,
        CollectPointKind::DoublePoint,
        CollectPointKind::Normalized,
        CollectPointKind::NormalizedNoQuality,
        CollectPointKind::Float,
        CollectPointKind::Counter,
    ]
}

fn random_value(point: &PointState, rng: &mut fastrand::Rng) -> f64 {
    match point.config.kind {
        CollectPointKind::SinglePoint | CollectPointKind::DoublePoint => {
            if point.value == 0.0 {
                1.0
            } else {
                0.0
            }
        }
        CollectPointKind::Normalized | CollectPointKind::NormalizedNoQuality => {
            let min = point.config.min as i16;
            let max = point.config.max as i16;
            let sampled = rng.i16(min..=max);
            if sampled as f64 != point.value || min == max {
                sampled as f64
            } else if sampled < max {
                (sampled + 1) as f64
            } else {
                min as f64
            }
        }
        CollectPointKind::Counter => {
            let min = point.config.min as i32;
            let max = point.config.max as i32;
            let sampled = rng.i32(min..=max);
            if sampled as f64 != point.value || min == max {
                sampled as f64
            } else if sampled < max {
                (sampled + 1) as f64
            } else {
                min as f64
            }
        }
        CollectPointKind::Float => {
            if point.config.min == point.config.max {
                return point.config.min;
            }
            let ratio = rng.f64();
            let sampled = point
                .config
                .min
                .mul_add(1.0 - ratio, point.config.max * ratio)
                .clamp(point.config.min, point.config.max);
            if sampled != point.value {
                sampled
            } else if point.value != point.config.min {
                point.config.min
            } else {
                point.config.max
            }
        }
    }
}

fn display_value(point: &PointState) -> String {
    match point.config.kind {
        CollectPointKind::SinglePoint => {
            if point.value == 0.0 {
                "0 / 分".to_owned()
            } else {
                "1 / 合".to_owned()
            }
        }
        CollectPointKind::DoublePoint => {
            if point.value == 0.0 {
                "0 / 分（DPI=1）".to_owned()
            } else {
                "1 / 合（DPI=2）".to_owned()
            }
        }
        CollectPointKind::Normalized
        | CollectPointKind::NormalizedNoQuality
        | CollectPointKind::Counter => format!("{:.0}", point.value),
        CollectPointKind::Float => format!("{:.3}", point.value),
    }
}

fn make_siq(point: &PointState) -> Siq {
    let qds = make_qds(&point.config.quality);
    Siq {
        iv: qds.iv,
        nt: qds.nt,
        sb: qds.sb,
        bl: qds.bl,
        spi: if point.value == 0.0 {
            Spi::Off
        } else {
            Spi::On
        },
    }
}

fn make_diq(point: &PointState) -> Diq {
    let qds = make_qds(&point.config.quality);
    Diq {
        iv: qds.iv,
        nt: qds.nt,
        sb: qds.sb,
        bl: qds.bl,
        dpi: if point.value == 0.0 {
            Dpi::Off
        } else {
            Dpi::On
        },
    }
}

fn make_qds(quality: &str) -> Qds {
    Qds {
        iv: quality == "invalid",
        nt: false,
        sb: quality == "substituted",
        bl: quality == "blocked",
        ov: false,
    }
}

fn control_phase(value: SelectExecute) -> ControlPhase {
    match value {
        SelectExecute::Select => ControlPhase::Select,
        SelectExecute::Execute => ControlPhase::Execute,
    }
}

fn counter_group(request: u8) -> Option<u8> {
    match request {
        1..=4 => Some(request),
        _ => None,
    }
}

async fn send_asdu(
    session: &mut LinkSession,
    asdu: Asdu,
    events: &mpsc::Sender<RuntimeEvent>,
    view: &mut ConnectionView,
) -> Result<(), String> {
    let event = session.send_asdu(asdu).await?;
    record_wire(events, view, event).await;
    Ok(())
}

async fn commit_station_reload(
    generation: u64,
    prepared: &mut Option<(u64, Station)>,
    station: &mut Station,
    events: &mpsc::Sender<RuntimeEvent>,
) -> Result<(), String> {
    let Some((prepared_generation, fresh)) = prepared.take() else {
        return Err(format!("采集侧没有待提交的重载版本 {generation}"));
    };
    if prepared_generation != generation {
        *prepared = Some((prepared_generation, fresh));
        return Err(format!(
            "采集侧待提交版本为 {prepared_generation}，收到版本 {generation}"
        ));
    }
    *station = fresh;
    emit(events, RuntimeEvent::CollectValues(station.views())).await;
    emit_log(
        events,
        Side::Collect,
        Direction::Internal,
        Severity::Success,
        LogCategory::Configuration,
        format!("采集点表已提交重载版本 {generation}"),
    )
    .await;
    Ok(())
}

fn abort_prepared_reload(generation: u64, prepared: &mut Option<(u64, Station)>) -> bool {
    if prepared
        .as_ref()
        .is_some_and(|(prepared_generation, _)| *prepared_generation == generation)
    {
        *prepared = None;
        true
    } else {
        false
    }
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
                Side::Collect,
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
            side: Side::Collect,
            view: view.clone(),
        },
    )
    .await;
}

async fn emit_log(
    events: &mpsc::Sender<RuntimeEvent>,
    side: Side,
    direction: Direction,
    severity: Severity,
    category: LogCategory,
    summary: impl Into<String>,
) {
    emit(
        events,
        RuntimeEvent::Log(LogEntry::new(side, direction, severity, category, summary)),
    )
    .await;
}

async fn emit(events: &mpsc::Sender<RuntimeEvent>, event: RuntimeEvent) {
    let _ = events.send(event).await;
}

#[cfg(test)]
mod tests {
    use iec104::types::{
        CScNa1, GenericObject, InformationObjects,
        commands::{Qu, Sco},
        information_elements::{SelectExecute, Spi},
    };

    use super::*;
    use crate::{
        config::{load_collect_points, load_from},
        protocol::asdu::make_asdu,
    };

    #[test]
    fn refresh_samples_each_matching_point_independently() {
        let points = load_collect_points(crate::config::DEFAULT_COLLECT_POINTS_PATH)
            .expect("collect point config");
        let mut station = Station::new(points);
        let mut rng = fastrand::Rng::with_seed(0x104);

        station.refresh_with_rng(UploadKind::RefreshAll, &mut rng);

        let values: Vec<_> = station
            .points
            .iter()
            .filter(|point| point.config.kind == CollectPointKind::Float)
            .map(|point| point.value)
            .collect();
        assert!(values.len() > 1, "test requires multiple float points");
        assert!(
            values.iter().skip(1).any(|value| *value != values[0]),
            "all matching points received the same value"
        );
    }

    #[test]
    fn refresh_all_changes_each_data_point_once_and_keeps_control_only_points() {
        let points = load_collect_points(crate::config::DEFAULT_COLLECT_POINTS_PATH)
            .expect("collect point config");
        let mut station = Station::new(points);
        let before = station.clone();
        let expected = before
            .points
            .iter()
            .filter(|point| point.config.purpose.includes_data())
            .count();
        assert_eq!(station.refresh(UploadKind::RefreshAll), expected);

        for (old, new) in before.points.iter().zip(&station.points) {
            if old.config.purpose.includes_data() {
                assert_ne!(old.value, new.value, "{} should change", old.config.name);
            } else {
                assert_eq!(old.value, new.value, "{} should stay", old.config.name);
            }
        }

        let frames = station
            .frames_for_upload(UploadKind::RefreshAll, 1, 0)
            .expect("frames");
        for expected_type in station
            .points
            .iter()
            .filter(|point| point.config.purpose.includes_data())
            .map(|point| point.config.kind.type_id())
        {
            assert!(
                frames
                    .iter()
                    .any(|asdu| format!("{:?}", asdu.type_id) == expected_type)
            );
        }
    }

    #[test]
    fn current_values_upload_generates_all_data_types_without_changing_points() {
        let points = load_collect_points(crate::config::DEFAULT_COLLECT_POINTS_PATH)
            .expect("collect point config");
        let mut station = Station::new(points);
        let before = station.views();
        let expected = station
            .points
            .iter()
            .filter(|point| point.config.purpose.includes_data())
            .count();

        assert_eq!(station.refresh(UploadKind::CurrentValues), expected);
        assert_eq!(
            station
                .views()
                .iter()
                .map(|point| (&point.value, point.updated_ms))
                .collect::<Vec<_>>(),
            before
                .iter()
                .map(|point| (&point.value, point.updated_ms))
                .collect::<Vec<_>>()
        );
        let frames = station
            .frames_for_upload(UploadKind::CurrentValues, 1, 0)
            .expect("current frames");
        assert!(!frames.is_empty());
    }

    #[test]
    fn manual_point_value_change_validates_type_and_range() {
        let points = load_collect_points(crate::config::DEFAULT_COLLECT_POINTS_PATH)
            .expect("collect point config");
        let mut station = Station::new(points);
        let float_ioa = station
            .points
            .iter()
            .find(|point| {
                point.config.kind == CollectPointKind::Float && point.config.purpose.includes_data()
            })
            .expect("float data point")
            .config
            .ioa;
        let single_ioa = station
            .points
            .iter()
            .find(|point| {
                point.config.kind == CollectPointKind::SinglePoint
                    && point.config.purpose.includes_data()
            })
            .expect("single data point")
            .config
            .ioa;

        assert_eq!(
            station
                .set_value(float_ioa, "M_ME_NC_1", 91.25)
                .expect("float value"),
            "91.250"
        );
        assert!(station.set_value(single_ioa, "M_SP_NA_1", 0.5).is_err());
        assert!(station.set_value(float_ioa, "M_ME_NA_1", 100.5).is_err());
        assert!(station.set_value(9_999_999, "M_SP_NA_1", 1.0).is_err());
    }

    #[tokio::test]
    async fn manual_point_value_change_emits_fresh_collect_values() {
        let points = load_collect_points(crate::config::DEFAULT_COLLECT_POINTS_PATH)
            .expect("collect point config");
        let mut station = Station::new(points);
        let (events_tx, mut events_rx) = mpsc::channel(4);
        let float_ioa = station
            .points
            .iter()
            .find(|point| {
                point.config.kind == CollectPointKind::Float && point.config.purpose.includes_data()
            })
            .expect("float data point")
            .config
            .ioa;

        update_station_value(float_ioa, "M_ME_NC_1", 91.25, &mut station, &events_tx).await;

        let RuntimeEvent::CollectValues(values) = events_rx.recv().await.expect("values event")
        else {
            panic!("expected collect values before the audit log");
        };
        let value = values
            .iter()
            .find(|row| row.ioa == float_ioa)
            .expect("float point");
        assert_eq!(value.value.as_deref(), Some("91.250"));
        assert!(value.updated_ms.is_some());
    }

    #[test]
    fn soe_refresh_uses_the_same_discrete_state() {
        let points = load_collect_points(crate::config::DEFAULT_COLLECT_POINTS_PATH)
            .expect("collect point config");
        let mut station = Station::new(points);
        let ioa = station
            .points
            .iter()
            .find(|point| point.config.kind == CollectPointKind::SinglePoint && point.config.soe)
            .expect("SOE point")
            .config
            .ioa;
        station.refresh(UploadKind::SingleSoe);
        let point = station
            .points
            .iter()
            .find(|point| point.config.ioa == ioa)
            .unwrap();
        let expected_spi = if point.value == 0.0 {
            Spi::Off
        } else {
            Spi::On
        };
        let frames = station
            .frames_for_upload(UploadKind::SingleSoe, 1, 0)
            .expect("SOE frames");
        let InformationObjects::MSpTb1(values) = &frames[0].information_objects else {
            panic!("expected M_SP_TB_1");
        };
        assert_eq!(values[0].address, ioa);
        assert_eq!(values[0].object.siq.spi, expected_spi);
    }

    #[test]
    fn frame_limits_use_the_real_asdu_object_size() {
        for type_id in [TypeId::M_SP_NA_1, TypeId::M_ME_NC_1, TypeId::M_SP_TB_1] {
            for sequence in [false, true] {
                let limit = frame_object_limit(type_id, sequence);
                let encoded = 6
                    + usize::from(sequence) * 3
                    + limit * (type_id.size() + if sequence { 0 } else { 3 });
                assert!(encoded <= MAX_ASDU_BYTES);
                if limit < 127 {
                    let next = 6
                        + usize::from(sequence) * 3
                        + (limit + 1) * (type_id.size() + if sequence { 0 } else { 3 });
                    assert!(next > MAX_ASDU_BYTES);
                }
            }
        }
        assert!(frame_object_limit(TypeId::M_SP_NA_1, false) > 20);
    }

    #[tokio::test]
    async fn delayed_control_response_does_not_pause_the_link_runtime() {
        let config = load_from(crate::config::MAIN_CONFIG_PATH).expect("config");
        let control_ioa = config
            .collect_points
            .iter()
            .find(|point| point.control == Some(ControlKind::Single))
            .expect("single control target")
            .ioa;
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("address");
        let client_stream = TcpStream::connect(address).await.expect("connect");
        let (server_stream, _) = listener.accept().await.expect("accept");
        let (mut client_reader, client_writer) = client_stream.into_split();
        let (mut server_reader, server_writer) = server_stream.into_split();
        let mut client =
            LinkSession::new(LinkRole::Client, client_writer, config.protocol.to_iec104());
        let mut server =
            LinkSession::new(LinkRole::Server, server_writer, config.protocol.to_iec104());
        client.start_dt().await.expect("STARTDT act");
        let frame = read_apdu(&mut server_reader).await.expect("read STARTDT");
        server.process(frame.frame).await.expect("confirm STARTDT");
        let frame = read_apdu(&mut client_reader)
            .await
            .expect("read STARTDT con");
        client.process(frame.frame).await.expect("activate client");

        let request = make_asdu(
            TypeId::C_SC_NA_1,
            Cot::Activation,
            1,
            0,
            false,
            InformationObjects::CScNa1(vec![GenericObject {
                address: control_ioa,
                object: CScNa1 {
                    sco: Sco {
                        se: SelectExecute::Execute,
                        qu: Qu::from_byte(0),
                        scs: Spi::On,
                    },
                },
            }]),
        );
        let mut station = Station::new(config.collect_points);
        let mut settings = config.collect;
        settings.fault_delay = Duration::from_millis(80);
        let mut policy = Some(FaultPolicy::DelaySuccess);
        let mut delayed = Vec::new();
        let (events_tx, _events_rx) = mpsc::channel(64);
        let mut view = ConnectionView::new(ConnectionPhase::Active, "test");

        let result = tokio::time::timeout(
            Duration::from_millis(20),
            control_response(
                request,
                control_ioa,
                ControlKind::Single,
                1.0,
                ControlPhase::Execute,
                &mut station,
                &mut server,
                &settings,
                &mut policy,
                &mut delayed,
                &events_tx,
                &mut view,
            ),
        )
        .await
        .expect("scheduling a delayed reply must return immediately")
        .expect("schedule delayed reply");
        assert!(!result);
        assert_eq!(delayed.len(), 1);
        assert!(server.tick().await.is_ok());

        tokio::time::sleep(Duration::from_millis(90)).await;
        flush_delayed_controls(
            &mut delayed,
            &mut station,
            &mut server,
            &settings,
            &events_tx,
            &mut view,
        )
        .await
        .expect("flush delayed reply");
        assert!(delayed.is_empty());
        assert_eq!(
            station
                .point(control_ioa, ControlKind::Single)
                .expect("control point")
                .value,
            1.0
        );
    }
}
