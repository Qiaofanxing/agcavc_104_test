use std::{collections::VecDeque, time::Duration};

use iec104::{
    apdu::{Apdu, Frame, SFrame, UFrame},
    asdu::Asdu,
    config::ProtocolConfig,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::tcp::{OwnedReadHalf, OwnedWriteHalf},
    time::Instant,
};

use crate::{
    model::{ConnectionPhase, Direction, LogCategory, ProtocolMeta, Severity},
    protocol::asdu::{asdu_details, asdu_summary, protocol_meta},
};

const TELEGRAM_HEADER: u8 = 0x68;
const MAX_APDU_PAYLOAD: usize = 253;
const SEQUENCE_MODULUS: u16 = 32_768;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LinkRole {
    Client,
    Server,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PendingU {
    Start,
    Stop,
    Test,
}

#[derive(Debug, Clone)]
pub struct WireEvent {
    pub direction: Direction,
    pub severity: Severity,
    pub category: LogCategory,
    pub summary: String,
    pub details: Vec<String>,
    pub protocol: Option<ProtocolMeta>,
}

impl WireEvent {
    fn protocol(
        direction: Direction,
        summary: impl Into<String>,
        details: Vec<String>,
        asdu: Option<&Asdu>,
    ) -> Self {
        Self {
            direction,
            severity: Severity::Normal,
            category: LogCategory::Protocol,
            summary: summary.into(),
            details,
            protocol: asdu.map(protocol_meta),
        }
    }

    fn keepalive(direction: Direction, summary: impl Into<String>) -> Self {
        Self {
            direction,
            severity: Severity::Normal,
            category: LogCategory::KeepAlive,
            summary: summary.into(),
            details: Vec::new(),
            protocol: None,
        }
    }
}

#[derive(Debug)]
pub struct ProcessedFrame {
    pub asdu: Option<Asdu>,
    pub events: Vec<WireEvent>,
    pub phase_changed: Option<ConnectionPhase>,
}

pub struct LinkSession {
    role: LinkRole,
    writer: OwnedWriteHalf,
    config: ProtocolConfig,
    phase: ConnectionPhase,
    sent_counter: u16,
    peer_acknowledged: u16,
    received_counter: u16,
    unacknowledged_sent: VecDeque<(u16, Instant)>,
    unacknowledged_received: u16,
    t2_deadline: Option<Instant>,
    last_received: Instant,
    pending_u: Option<(PendingU, Instant)>,
}

impl LinkSession {
    pub fn new(role: LinkRole, writer: OwnedWriteHalf, config: ProtocolConfig) -> Self {
        Self {
            role,
            writer,
            config,
            phase: ConnectionPhase::ConnectedStopped,
            sent_counter: 0,
            peer_acknowledged: 0,
            received_counter: 0,
            unacknowledged_sent: VecDeque::new(),
            unacknowledged_received: 0,
            t2_deadline: None,
            last_received: Instant::now(),
            pending_u: None,
        }
    }

    pub const fn phase(&self) -> ConnectionPhase {
        self.phase
    }

    pub const fn is_active(&self) -> bool {
        matches!(self.phase, ConnectionPhase::Active)
    }

    pub async fn start_dt(&mut self) -> Result<WireEvent, String> {
        if self.role != LinkRole::Client {
            return Err("只有调度客户端可以主动发送 STARTDT".to_owned());
        }
        if self.phase != ConnectionPhase::ConnectedStopped {
            return Err(format!("当前状态 {} 不能发送 STARTDT", self.phase.label()));
        }
        let event = self
            .send_u(UFrame {
                start_dt_activation: true,
                ..UFrame::default()
            })
            .await?;
        self.phase = ConnectionPhase::Starting;
        self.pending_u = Some((PendingU::Start, Instant::now()));
        Ok(event)
    }

    pub async fn stop_dt(&mut self) -> Result<WireEvent, String> {
        if self.role != LinkRole::Client {
            return Err("只有调度客户端可以主动发送 STOPDT".to_owned());
        }
        if !self.is_active() {
            return Err("只有 ACTIVE 状态可以发送 STOPDT".to_owned());
        }
        let event = self
            .send_u(UFrame {
                stop_dt_activation: true,
                ..UFrame::default()
            })
            .await?;
        self.phase = ConnectionPhase::Stopping;
        self.pending_u = Some((PendingU::Stop, Instant::now()));
        Ok(event)
    }

    pub async fn manual_test(&mut self) -> Result<WireEvent, String> {
        if !self.is_active() {
            return Err("只有 ACTIVE 状态可以发送 TESTFR".to_owned());
        }
        if self.pending_u.is_some() {
            return Err("已有 U 帧等待确认".to_owned());
        }
        self.send_test_activation().await
    }

    pub async fn send_asdu(&mut self, asdu: Asdu) -> Result<WireEvent, String> {
        if !self.is_active() {
            return Err(format!("链路状态 {}，不能发送 I 帧", self.phase.label()));
        }
        if self.unacknowledged_sent.len() >= self.config.k as usize {
            return Err(format!("发送窗口已满 K={}", self.config.k));
        }
        let sequence = self.sent_counter;
        let bytes = encode_i_frame(sequence, self.received_counter, &asdu)?;
        self.writer
            .write_all(&bytes)
            .await
            .map_err(|error| format!("发送 I 帧失败: {error}"))?;
        self.sent_counter = (self.sent_counter + 1) % SEQUENCE_MODULUS;
        self.unacknowledged_sent
            .push_back((self.sent_counter, Instant::now()));
        self.unacknowledged_received = 0;
        self.t2_deadline = None;
        Ok(WireEvent::protocol(
            Direction::Outgoing,
            format!(
                "I帧 N(S)={sequence} N(R)={} · {}",
                self.received_counter,
                asdu_summary(&asdu)
            ),
            asdu_details(&asdu),
            Some(&asdu),
        ))
    }

    pub async fn process(&mut self, frame: Frame) -> Result<ProcessedFrame, String> {
        let before = self.phase;
        self.last_received = Instant::now();
        let mut asdu = None;
        let mut events = Vec::new();
        match frame {
            Frame::I(frame) => {
                events.push(WireEvent::protocol(
                    Direction::Incoming,
                    format!(
                        "I帧 N(S)={} N(R)={} · {}",
                        frame.send_sequence_number,
                        frame.receive_sequence_number,
                        asdu_summary(&frame.asdu)
                    ),
                    asdu_details(&frame.asdu),
                    Some(&frame.asdu),
                ));
                self.process_ack(frame.receive_sequence_number)?;
                if frame.send_sequence_number != self.received_counter {
                    return Err(format!(
                        "接收序号错误：期望 N(S)={}，实际 {}",
                        self.received_counter, frame.send_sequence_number
                    ));
                }
                self.received_counter = (self.received_counter + 1) % SEQUENCE_MODULUS;
                self.unacknowledged_received = self.unacknowledged_received.saturating_add(1);
                self.t2_deadline
                    .get_or_insert(Instant::now() + self.config.t2);
                if self.unacknowledged_received >= self.config.w {
                    events.push(self.send_s_frame().await?);
                }
                if self.is_active() {
                    asdu = Some(frame.asdu);
                } else {
                    events.push(WireEvent {
                        direction: Direction::Internal,
                        severity: Severity::Warning,
                        category: LogCategory::Protocol,
                        summary: "未激活状态收到 I 帧，已做链路确认但未交给业务".to_owned(),
                        details: Vec::new(),
                        protocol: None,
                    });
                }
            }
            Frame::S(frame) => {
                events.push(WireEvent::protocol(
                    Direction::Incoming,
                    format!("S帧 N(R)={}", frame.receive_sequence_number),
                    Vec::new(),
                    None,
                ));
                self.process_ack(frame.receive_sequence_number)?;
            }
            Frame::U(frame) => {
                let flag_count = [
                    frame.test_fr_activation,
                    frame.test_fr_confirmation,
                    frame.start_dt_activation,
                    frame.start_dt_confirmation,
                    frame.stop_dt_activation,
                    frame.stop_dt_confirmation,
                ]
                .into_iter()
                .filter(|flag| *flag)
                .count();
                if flag_count != 1 {
                    return Err(format!("U 帧必须且只能设置一个功能位，实际 {flag_count}"));
                }
                events.push(WireEvent::keepalive(
                    Direction::Incoming,
                    format!("U帧 {}", u_frame_label(&frame)),
                ));
                if frame.test_fr_activation {
                    events.push(
                        self.send_u(UFrame {
                            test_fr_confirmation: true,
                            ..UFrame::default()
                        })
                        .await?,
                    );
                }
                if frame.test_fr_confirmation
                    && self
                        .pending_u
                        .is_some_and(|(kind, _)| kind == PendingU::Test)
                {
                    self.pending_u = None;
                }
                if frame.start_dt_activation {
                    events.push(
                        self.send_u(UFrame {
                            start_dt_confirmation: true,
                            ..UFrame::default()
                        })
                        .await?,
                    );
                    self.phase = ConnectionPhase::Active;
                    self.pending_u = None;
                }
                if frame.start_dt_confirmation
                    && self
                        .pending_u
                        .is_some_and(|(kind, _)| kind == PendingU::Start)
                {
                    self.phase = ConnectionPhase::Active;
                    self.pending_u = None;
                }
                if frame.stop_dt_activation {
                    events.push(
                        self.send_u(UFrame {
                            stop_dt_confirmation: true,
                            ..UFrame::default()
                        })
                        .await?,
                    );
                    self.phase = ConnectionPhase::ConnectedStopped;
                    self.pending_u = None;
                }
                if frame.stop_dt_confirmation
                    && self
                        .pending_u
                        .is_some_and(|(kind, _)| kind == PendingU::Stop)
                {
                    self.phase = ConnectionPhase::ConnectedStopped;
                    self.pending_u = None;
                }
            }
        }
        Ok(ProcessedFrame {
            asdu,
            events,
            phase_changed: (self.phase != before).then_some(self.phase),
        })
    }

    pub async fn tick(&mut self) -> Result<Vec<WireEvent>, String> {
        let now = Instant::now();
        if let Some((_, sent_at)) = self.pending_u
            && now.duration_since(sent_at) >= self.config.t1
        {
            return Err("T1 超时：U 帧未获确认".to_owned());
        }
        if let Some((_, sent_at)) = self.unacknowledged_sent.front()
            && now.duration_since(*sent_at) >= self.config.t1
        {
            return Err("T1 超时：I 帧未获确认".to_owned());
        }
        let mut events = Vec::new();
        if self
            .t2_deadline
            .is_some_and(|deadline| now >= deadline && self.unacknowledged_received > 0)
        {
            events.push(self.send_s_frame().await?);
        }
        if self.is_active()
            && self.pending_u.is_none()
            && now.duration_since(self.last_received) >= self.config.t3
        {
            events.push(self.send_test_activation().await?);
        }
        Ok(events)
    }

    fn process_ack(&mut self, acknowledgement: u16) -> Result<(), String> {
        if acknowledgement >= SEQUENCE_MODULUS {
            return Err(format!("N(R) 超出 15 位序号范围: {acknowledgement}"));
        }
        let outstanding = sequence_distance(self.peer_acknowledged, self.sent_counter);
        let advance = sequence_distance(self.peer_acknowledged, acknowledgement);
        if advance > outstanding {
            return Err(format!(
                "N(R) 非法：V(A)={} V(S)={} 收到 {}",
                self.peer_acknowledged, self.sent_counter, acknowledgement
            ));
        }
        if usize::from(advance) > self.unacknowledged_sent.len() {
            return Err(format!(
                "发送确认状态不一致：需移除 {advance} 帧，队列仅 {} 帧",
                self.unacknowledged_sent.len()
            ));
        }
        for _ in 0..advance {
            self.unacknowledged_sent.pop_front();
        }
        self.peer_acknowledged = acknowledgement;
        Ok(())
    }

    async fn send_s_frame(&mut self) -> Result<WireEvent, String> {
        let frame = Frame::S(SFrame {
            receive_sequence_number: self.received_counter,
        });
        self.write_frame(&frame).await?;
        self.unacknowledged_received = 0;
        self.t2_deadline = None;
        Ok(WireEvent::protocol(
            Direction::Outgoing,
            format!("S帧 N(R)={}", self.received_counter),
            Vec::new(),
            None,
        ))
    }

    async fn send_test_activation(&mut self) -> Result<WireEvent, String> {
        let event = self
            .send_u(UFrame {
                test_fr_activation: true,
                ..UFrame::default()
            })
            .await?;
        self.pending_u = Some((PendingU::Test, Instant::now()));
        Ok(event)
    }

    async fn send_u(&mut self, frame: UFrame) -> Result<WireEvent, String> {
        let label = u_frame_label(&frame);
        self.write_frame(&Frame::U(frame)).await?;
        Ok(WireEvent::keepalive(
            Direction::Outgoing,
            format!("U帧 {label}"),
        ))
    }

    async fn write_frame(&mut self, frame: &Frame) -> Result<(), String> {
        let bytes = frame
            .to_apdu_bytes()
            .map_err(|error| format!("编码 APDU 失败: {error}"))?;
        self.writer
            .write_all(&bytes)
            .await
            .map_err(|error| format!("发送 APDU 失败: {error}"))
    }
}

pub async fn read_apdu(reader: &mut OwnedReadHalf) -> Result<Apdu, String> {
    let mut header = [0_u8; 2];
    reader
        .read_exact(&mut header)
        .await
        .map_err(|error| format!("读取 APDU 头失败: {error}"))?;
    if header[0] != TELEGRAM_HEADER {
        return Err(format!("APDU 启动字符非法: 0x{:02X}", header[0]));
    }
    let length = header[1] as usize;
    if !(4..=MAX_APDU_PAYLOAD).contains(&length) {
        return Err(format!("APDU 长度非法: {length}"));
    }
    let mut bytes = Vec::with_capacity(length + 2);
    bytes.extend_from_slice(&header);
    bytes.resize(length + 2, 0);
    reader
        .read_exact(&mut bytes[2..])
        .await
        .map_err(|error| format!("读取 APDU 载荷失败: {error}"))?;
    Apdu::from_bytes(&bytes).map_err(|error| format!("解析 APDU 失败: {error}"))
}

pub fn encode_asdu_bytes(asdu: &Asdu) -> Result<Vec<u8>, String> {
    let mut expanded = asdu.clone();
    expanded.sequence = false;
    let mut bytes = Vec::new();
    expanded
        .to_bytes(&mut bytes)
        .map_err(|error| format!("编码 ASDU 失败: {error}"))?;
    if !asdu.sequence || asdu.information_objects.len() <= 1 {
        if asdu.sequence && bytes.len() > 1 {
            bytes[1] |= 0x80;
        }
        return Ok(bytes);
    }
    if !asdu.type_id.is_standard() {
        return Err("非标准 TypeID 不支持顺序编址".to_owned());
    }
    let object_size = asdu.type_id.size();
    let count = asdu.information_objects.len();
    let expected = 6 + count * (3 + object_size);
    if bytes.len() != expected {
        return Err(format!(
            "顺序编址转换长度不匹配：实际 {}，期望 {expected}",
            bytes.len()
        ));
    }
    let first_address = ioa_at(&bytes, 0, object_size);
    let mut compact = bytes[..6].to_vec();
    compact[1] |= 0x80;
    for index in 0..count {
        let offset = 6 + index * (3 + object_size);
        let address = ioa_at(&bytes, index, object_size);
        if address != first_address + index as u32 {
            return Err(format!(
                "顺序编址要求 IOA 连续：起始 {first_address}，第 {index} 个为 {address}"
            ));
        }
        if index == 0 {
            compact.extend_from_slice(&bytes[offset..offset + 3]);
        }
        compact.extend_from_slice(&bytes[offset + 3..offset + 3 + object_size]);
    }
    Ok(compact)
}

fn encode_i_frame(send: u16, receive: u16, asdu: &Asdu) -> Result<Vec<u8>, String> {
    let asdu_bytes = encode_asdu_bytes(asdu)?;
    let payload_length = 4 + asdu_bytes.len();
    if payload_length > MAX_APDU_PAYLOAD {
        return Err(format!("APDU 超长: {payload_length} > {MAX_APDU_PAYLOAD}"));
    }
    let mut bytes = Vec::with_capacity(payload_length + 2);
    bytes.push(TELEGRAM_HEADER);
    bytes.push(payload_length as u8);
    bytes.extend_from_slice(&(send << 1).to_le_bytes());
    bytes.extend_from_slice(&(receive << 1).to_le_bytes());
    bytes.extend_from_slice(&asdu_bytes);
    Ok(bytes)
}

fn ioa_at(bytes: &[u8], index: usize, object_size: usize) -> u32 {
    let offset = 6 + index * (3 + object_size);
    u32::from_le_bytes([bytes[offset], bytes[offset + 1], bytes[offset + 2], 0])
}

fn sequence_distance(from: u16, to: u16) -> u16 {
    ((u32::from(to) + u32::from(SEQUENCE_MODULUS) - u32::from(from)) % u32::from(SEQUENCE_MODULUS))
        as u16
}

fn u_frame_label(frame: &UFrame) -> &'static str {
    if frame.start_dt_activation {
        "STARTDT act"
    } else if frame.start_dt_confirmation {
        "STARTDT con"
    } else if frame.stop_dt_activation {
        "STOPDT act"
    } else if frame.stop_dt_confirmation {
        "STOPDT con"
    } else if frame.test_fr_activation {
        "TESTFR act"
    } else if frame.test_fr_confirmation {
        "TESTFR con"
    } else {
        "未知 U 帧"
    }
}

pub fn tick_interval() -> Duration {
    Duration::from_millis(50)
}

#[cfg(test)]
mod tests {
    use iec104::{
        asdu::Asdu,
        config::ProtocolConfig,
        cot::Cot,
        types::{
            CRdNa1, GenericObject, InformationObjects, MSpNa1,
            information_elements::{Siq, Spi},
        },
        types_id::TypeId,
    };
    use tokio::{net::TcpListener, time::sleep};

    use super::*;
    use crate::protocol::asdu::make_asdu;

    fn test_protocol() -> ProtocolConfig {
        ProtocolConfig {
            t0: Duration::from_millis(100),
            t1: Duration::from_millis(60),
            t2: Duration::from_millis(20),
            t3: Duration::from_millis(100),
            k: 12,
            w: 1,
            max_pending_outgoing_asdu: 32,
            originator_address: 0,
        }
    }

    fn read_request(ioa: u32) -> Asdu {
        make_asdu(
            TypeId::C_RD_NA_1,
            Cot::Request,
            1,
            0,
            false,
            InformationObjects::CRdNa1(vec![GenericObject {
                address: ioa,
                object: CRdNa1 {},
            }]),
        )
    }

    async fn start_data_transfer(
        client: &mut LinkSession,
        client_reader: &mut OwnedReadHalf,
        server: &mut LinkSession,
        server_reader: &mut OwnedReadHalf,
    ) {
        client.start_dt().await.expect("send STARTDT");
        let frame = read_apdu(server_reader)
            .await
            .expect("server reads STARTDT");
        server
            .process(frame.frame)
            .await
            .expect("server confirms STARTDT");
        let frame = read_apdu(client_reader)
            .await
            .expect("client reads STARTDT con");
        client.process(frame.frame).await.expect("client activates");
        assert!(client.is_active());
        assert!(server.is_active());
    }

    async fn send_and_ack(
        client: &mut LinkSession,
        client_reader: &mut OwnedReadHalf,
        server: &mut LinkSession,
        server_reader: &mut OwnedReadHalf,
        ioa: u32,
    ) {
        client
            .send_asdu(read_request(ioa))
            .await
            .expect("client sends I frame");
        let frame = read_apdu(server_reader)
            .await
            .expect("server reads I frame");
        let processed = server
            .process(frame.frame)
            .await
            .expect("server processes I frame");
        assert_eq!(
            processed.asdu.expect("business ASDU").type_id,
            TypeId::C_RD_NA_1
        );
        let frame = read_apdu(client_reader)
            .await
            .expect("client reads S frame");
        client
            .process(frame.frame)
            .await
            .expect("client processes S frame");
    }

    #[tokio::test]
    async fn stop_start_keeps_sequence_and_t3_testfr_keeps_link_active() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("address");
        let client_stream = tokio::net::TcpStream::connect(address)
            .await
            .expect("connect");
        let (server_stream, _) = listener.accept().await.expect("accept");
        let (mut client_reader, client_writer) = client_stream.into_split();
        let (mut server_reader, server_writer) = server_stream.into_split();
        let mut client = LinkSession::new(LinkRole::Client, client_writer, test_protocol());
        let mut server = LinkSession::new(LinkRole::Server, server_writer, test_protocol());

        start_data_transfer(
            &mut client,
            &mut client_reader,
            &mut server,
            &mut server_reader,
        )
        .await;
        send_and_ack(
            &mut client,
            &mut client_reader,
            &mut server,
            &mut server_reader,
            1,
        )
        .await;

        client.stop_dt().await.expect("send STOPDT");
        let frame = read_apdu(&mut server_reader)
            .await
            .expect("server reads STOPDT");
        server
            .process(frame.frame)
            .await
            .expect("server confirms STOPDT");
        let frame = read_apdu(&mut client_reader)
            .await
            .expect("client reads STOPDT con");
        client.process(frame.frame).await.expect("client stops");
        assert_eq!(client.phase(), ConnectionPhase::ConnectedStopped);
        assert_eq!(server.phase(), ConnectionPhase::ConnectedStopped);

        start_data_transfer(
            &mut client,
            &mut client_reader,
            &mut server,
            &mut server_reader,
        )
        .await;
        send_and_ack(
            &mut client,
            &mut client_reader,
            &mut server,
            &mut server_reader,
            2,
        )
        .await;

        sleep(Duration::from_millis(120)).await;
        let events = client.tick().await.expect("T3 tick");
        assert!(
            events
                .iter()
                .any(|event| event.summary.contains("TESTFR act"))
        );
        let frame = read_apdu(&mut server_reader)
            .await
            .expect("server reads TESTFR");
        server
            .process(frame.frame)
            .await
            .expect("server confirms TESTFR");
        let frame = read_apdu(&mut client_reader)
            .await
            .expect("client reads TESTFR con");
        client
            .process(frame.frame)
            .await
            .expect("client clears T1-U");
        assert!(client.is_active());
        assert!(client.tick().await.is_ok());
    }

    #[tokio::test]
    async fn acknowledgements_reject_future_and_stale_sequence_numbers() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("address");
        let client_stream = tokio::net::TcpStream::connect(address)
            .await
            .expect("connect");
        let (server_stream, _) = listener.accept().await.expect("accept");
        let (mut client_reader, client_writer) = client_stream.into_split();
        let (mut server_reader, server_writer) = server_stream.into_split();
        let mut client = LinkSession::new(LinkRole::Client, client_writer, test_protocol());
        let mut server = LinkSession::new(LinkRole::Server, server_writer, test_protocol());
        start_data_transfer(
            &mut client,
            &mut client_reader,
            &mut server,
            &mut server_reader,
        )
        .await;

        client
            .send_asdu(read_request(7))
            .await
            .expect("send I frame");
        let _ = read_apdu(&mut server_reader).await.expect("read I frame");
        let error = client
            .process(Frame::S(SFrame {
                receive_sequence_number: 100,
            }))
            .await
            .expect_err("future acknowledgement must fail");
        assert!(error.contains("N(R) 非法"));
        assert_eq!(client.unacknowledged_sent.len(), 1);

        client
            .process(Frame::S(SFrame {
                receive_sequence_number: 1,
            }))
            .await
            .expect("valid acknowledgement");
        assert!(client.unacknowledged_sent.is_empty());
        assert!(
            client
                .process(Frame::S(SFrame {
                    receive_sequence_number: 0,
                }))
                .await
                .is_err()
        );
        assert_eq!(sequence_distance(32_767, 0), 1);
    }

    #[test]
    fn sequence_encoding_is_compact_and_rejects_gaps() {
        let object = |address| GenericObject {
            address,
            object: MSpNa1 {
                siq: Siq {
                    spi: Spi::On,
                    ..Siq::default()
                },
            },
        };
        let mut sequential = make_asdu(
            TypeId::M_SP_NA_1,
            Cot::SpontaneousData,
            1,
            0,
            false,
            InformationObjects::MSpNa1(vec![object(100), object(101)]),
        );
        sequential.sequence = true;
        let compact = encode_asdu_bytes(&sequential).expect("sequence encode");
        let mut individual = sequential.clone();
        individual.sequence = false;
        let expanded = encode_asdu_bytes(&individual).expect("individual encode");
        assert_eq!(expanded.len() - compact.len(), 3);
        let parsed = Asdu::parse(&compact).expect("sequence parse");
        assert!(parsed.sequence);
        let InformationObjects::MSpNa1(values) = parsed.information_objects else {
            panic!("wrong information object type");
        };
        assert_eq!(values[0].address, 100);
        assert_eq!(values[1].address, 101);

        sequential.information_objects = InformationObjects::MSpNa1(vec![object(100), object(102)]);
        assert!(encode_asdu_bytes(&sequential).is_err());
    }
}
