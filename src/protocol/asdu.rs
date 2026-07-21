use chrono::{DateTime, Datelike, Local, Timelike, Utc};

use iec104::{
    asdu::Asdu,
    cot::Cot,
    types::{
        CCiNa1, CCsNa1, CIcNa1, CRdNa1, CScNa1, CSeNa1, CSeNb1, CSeNc1, CdcNa1, CrcNa1,
        GenericObject, InformationObjects,
        commands::{Dco, Frz, Qoi, Qu, Rco, Rcs, Rqt, Sco},
        information_elements::{Dpi, SelectExecute, Spi},
        quality_descriptors::Qos,
        time::Cp56Time2a,
    },
    types_id::TypeId,
};

use crate::model::{
    ControlPhase, DispatchAction, DispatchRequest, PointView, ProtocolMeta, now_millis,
};

pub fn protocol_meta(asdu: &Asdu) -> ProtocolMeta {
    macro_rules! addresses {
        ($values:expr) => {
            $values.iter().map(|value| value.address).collect()
        };
    }
    let ioas = match &asdu.information_objects {
        InformationObjects::MSpNa1(values) => addresses!(values),
        InformationObjects::MDpNa1(values) => addresses!(values),
        InformationObjects::MMeNa1(values) => addresses!(values),
        InformationObjects::MMeNd1(values) => addresses!(values),
        InformationObjects::MMeNc1(values) => addresses!(values),
        InformationObjects::MItNa1(values) => addresses!(values),
        InformationObjects::MSpTb1(values) => addresses!(values),
        InformationObjects::MDpTb1(values) => addresses!(values),
        InformationObjects::MEiNa1(values) => addresses!(values),
        InformationObjects::CIcNa1(values) => addresses!(values),
        InformationObjects::CCiNa1(values) => addresses!(values),
        InformationObjects::CCsNa1(values) => addresses!(values),
        InformationObjects::CRdNa1(values) => addresses!(values),
        InformationObjects::CScNa1(values) => addresses!(values),
        InformationObjects::CdcNa1(values) => addresses!(values),
        InformationObjects::CrcNa1(values) => addresses!(values),
        InformationObjects::CSeNa1(values) => addresses!(values),
        InformationObjects::CSeNb1(values) => addresses!(values),
        InformationObjects::CSeNc1(values) => addresses!(values),
        _ => Vec::new(),
    };
    ProtocolMeta {
        type_id: format!("{:?}", asdu.type_id),
        cot: format!("{:?}", asdu.cot),
        common_address: asdu.address_field,
        originator_address: asdu.originator_address,
        sequence: asdu.sequence,
        test: asdu.test,
        negative: asdu.negative,
        ioas,
    }
}

pub fn make_asdu(
    type_id: TypeId,
    cot: Cot,
    common_address: u16,
    originator_address: u8,
    test: bool,
    information_objects: InformationObjects,
) -> Asdu {
    Asdu {
        type_id,
        cot,
        originator_address,
        address_field: common_address,
        sequence: false,
        test,
        negative: false,
        information_objects,
    }
}

pub fn confirmation(request: &Asdu, cot: Cot, negative: bool, concrete_ca: u16) -> Asdu {
    Asdu {
        type_id: request.type_id,
        cot,
        originator_address: request.originator_address,
        address_field: concrete_ca,
        sequence: request.sequence,
        test: request.test,
        negative,
        information_objects: request.information_objects.clone(),
    }
}

pub fn build_dispatch_request(request: &DispatchRequest) -> Result<Option<Asdu>, String> {
    validate_dispatch_request_fields(request)?;
    let common_address = request.common_address;
    let oa = request.originator_address;
    let test = request.test;
    let se = match request.phase {
        ControlPhase::Select => SelectExecute::Select,
        ControlPhase::Execute => SelectExecute::Execute,
    };
    let qu = Qu::from_byte(request.qualifier);
    let qos = Qos {
        se,
        ql: request.qualifier,
    };

    let asdu = match request.action {
        DispatchAction::StartDt | DispatchAction::StopDt | DispatchAction::TestFr => {
            return Ok(None);
        }
        DispatchAction::GeneralInterrogation => make_asdu(
            TypeId::C_IC_NA_1,
            Cot::Activation,
            common_address,
            oa,
            test,
            InformationObjects::CIcNa1(vec![GenericObject {
                address: 0,
                object: CIcNa1 {
                    qoi: Qoi::from_byte(request.qoi),
                },
            }]),
        ),
        DispatchAction::CounterInterrogation => make_asdu(
            TypeId::C_CI_NA_1,
            Cot::Activation,
            common_address,
            oa,
            test,
            InformationObjects::CCiNa1(vec![GenericObject {
                address: 0,
                object: CCiNa1 {
                    rqt: Rqt::from_byte(request.qcc_request),
                    frz: Frz::from_byte(request.qcc_freeze),
                },
            }]),
        ),
        DispatchAction::ClockSync => make_asdu(
            TypeId::C_CS_NA_1,
            Cot::Activation,
            common_address,
            oa,
            test,
            InformationObjects::CCsNa1(vec![GenericObject {
                address: 0,
                object: CCsNa1 {
                    time: match request.clock_time_ms {
                        Some(value) => cp56_from_unix_millis(value)?,
                        None => current_cp56_time()?,
                    },
                },
            }]),
        ),
        DispatchAction::Read => make_asdu(
            TypeId::C_RD_NA_1,
            Cot::Request,
            common_address,
            oa,
            test,
            InformationObjects::CRdNa1(vec![GenericObject {
                address: request.ioa,
                object: CRdNa1 {},
            }]),
        ),
        DispatchAction::SingleControl => {
            let value = bool_value(request.value, "单点遥控")?;
            make_asdu(
                TypeId::C_SC_NA_1,
                Cot::Activation,
                common_address,
                oa,
                test,
                InformationObjects::CScNa1(vec![GenericObject {
                    address: request.ioa,
                    object: CScNa1 {
                        sco: Sco {
                            se,
                            qu,
                            scs: if value { Spi::On } else { Spi::Off },
                        },
                    },
                }]),
            )
        }
        DispatchAction::DoubleControl => {
            let value = bool_value(request.value, "双点遥控")?;
            make_asdu(
                TypeId::C_DC_NA_1,
                Cot::Activation,
                common_address,
                oa,
                test,
                InformationObjects::CdcNa1(vec![GenericObject {
                    address: request.ioa,
                    object: CdcNa1 {
                        dco: Dco {
                            se,
                            qu,
                            dcs: if value { Dpi::On } else { Dpi::Off },
                        },
                    },
                }]),
            )
        }
        DispatchAction::RegulatingStep => {
            let value = integer_value(request.value, -1, 1, "升降控制")?;
            let rcs = match value {
                -1 => Rcs::Decrement,
                1 => Rcs::Increment,
                _ => Rcs::None,
            };
            make_asdu(
                TypeId::C_RC_NA_1,
                Cot::Activation,
                common_address,
                oa,
                test,
                InformationObjects::CrcNa1(vec![GenericObject {
                    address: request.ioa,
                    object: CrcNa1 {
                        rco: Rco { se, qu, rcs },
                    },
                }]),
            )
        }
        DispatchAction::NormalizedSetpoint => {
            let value = integer_value(
                request.value,
                i16::MIN as i64,
                i16::MAX as i64,
                "归一化遥调",
            )?;
            make_asdu(
                TypeId::C_SE_NA_1,
                Cot::Activation,
                common_address,
                oa,
                test,
                InformationObjects::CSeNa1(vec![GenericObject {
                    address: request.ioa,
                    object: CSeNa1 {
                        nva: value as i16,
                        qos,
                    },
                }]),
            )
        }
        DispatchAction::ScaledSetpoint => {
            let value = integer_value(
                request.value,
                i16::MIN as i64,
                i16::MAX as i64,
                "标度化遥调",
            )?;
            make_asdu(
                TypeId::C_SE_NB_1,
                Cot::Activation,
                common_address,
                oa,
                test,
                InformationObjects::CSeNb1(vec![GenericObject {
                    address: request.ioa,
                    object: CSeNb1 {
                        sva: value as i16,
                        qos,
                    },
                }]),
            )
        }
        DispatchAction::FloatSetpoint => {
            if !request.value.is_finite()
                || request.value < f32::MIN as f64
                || request.value > f32::MAX as f64
            {
                return Err("短浮点遥调值必须是有限 f32".to_owned());
            }
            make_asdu(
                TypeId::C_SE_NC_1,
                Cot::Activation,
                common_address,
                oa,
                test,
                InformationObjects::CSeNc1(vec![GenericObject {
                    address: request.ioa,
                    object: CSeNc1 {
                        value: request.value as f32,
                        qos,
                    },
                }]),
            )
        }
    };
    Ok(Some(asdu))
}

fn validate_dispatch_request_fields(request: &DispatchRequest) -> Result<(), String> {
    if !matches!(
        request.action,
        DispatchAction::StartDt | DispatchAction::StopDt | DispatchAction::TestFr
    ) && request.common_address == 0
    {
        return Err("CA 不能为 0".to_owned());
    }
    if request.action.uses_ioa() && request.ioa > 0xFF_FFFF {
        return Err(format!("IOA={} 超出三字节范围", request.ioa));
    }
    if request.action == DispatchAction::GeneralInterrogation && !(20..=36).contains(&request.qoi) {
        return Err("QOI 必须在 20..=36".to_owned());
    }
    if request.action == DispatchAction::CounterInterrogation {
        if !(1..=5).contains(&request.qcc_request) {
            return Err("QCC 请求组必须在 1..=5".to_owned());
        }
        if request.qcc_freeze > 3 {
            return Err("QCC 冻结限定词必须在 0..=3".to_owned());
        }
    }
    if request.action.is_control() {
        let maximum = if matches!(
            request.action,
            DispatchAction::NormalizedSetpoint
                | DispatchAction::ScaledSetpoint
                | DispatchAction::FloatSetpoint
        ) {
            127
        } else {
            31
        };
        if request.qualifier > maximum {
            return Err(format!("限定词必须在 0..={maximum}"));
        }
    }
    Ok(())
}

pub fn interrogation_cot(qoi: Qoi) -> Option<Cot> {
    match qoi.to_byte() {
        20..=36 => Cot::try_from(qoi.to_byte()).ok(),
        _ => None,
    }
}

pub fn counter_interrogation_cot(rqt: Rqt) -> Cot {
    match rqt {
        Rqt::ReqCo1 => Cot::CounterInterrogationGroup1,
        Rqt::ReqCo2 => Cot::CounterInterrogationGroup2,
        Rqt::ReqCo3 => Cot::CounterInterrogationGroup3,
        Rqt::ReqCo4 => Cot::CounterInterrogationGroup4,
        Rqt::ReqCoGen | Rqt::None | Rqt::Other(_) => Cot::CounterInterrogationGeneral,
    }
}

pub fn qoi_group(qoi: Qoi) -> Option<u8> {
    match qoi.to_byte() {
        20 => None,
        21..=36 => Some(qoi.to_byte() - 20),
        _ => None,
    }
}

pub fn asdu_summary(asdu: &Asdu) -> String {
    let preview = point_preview(&asdu.information_objects);
    format!(
        "{} COT={} CA={} OA={} SQ={} 对象={}{}",
        type_label(asdu.type_id),
        cot_label(asdu.cot),
        asdu.address_field,
        asdu.originator_address,
        if asdu.sequence { 1 } else { 0 },
        asdu.information_objects.len(),
        preview.map_or_else(String::new, |value| format!(" · {value}"))
    )
}

pub fn asdu_details(asdu: &Asdu) -> Vec<String> {
    let mut details = vec![
        format!("TypeID: {:?} ({})", asdu.type_id, asdu.type_id as u8),
        format!("COT: {:?} ({})", asdu.cot, asdu.cot as u8),
        format!(
            "CA={} OA={} SQ={} Test={} Negative={}",
            asdu.address_field, asdu.originator_address, asdu.sequence, asdu.test, asdu.negative
        ),
    ];
    details.extend(object_details(&asdu.information_objects));
    details
}

pub fn rows_from_asdu(asdu: &Asdu) -> Vec<PointView> {
    let updated = now_millis();
    match &asdu.information_objects {
        InformationObjects::MSpNa1(objects) => objects
            .iter()
            .map(|object| {
                row(
                    object.address,
                    "M_SP_NA_1",
                    "单点",
                    spi_label(object.object.siq.spi),
                    quality_bits(
                        object.object.siq.iv,
                        object.object.siq.nt,
                        object.object.siq.sb,
                        object.object.siq.bl,
                        false,
                    ),
                    updated,
                )
            })
            .collect(),
        InformationObjects::MDpNa1(objects) => objects
            .iter()
            .map(|object| {
                row(
                    object.address,
                    "M_DP_NA_1",
                    "双点",
                    dpi_label(object.object.diq.dpi),
                    quality_bits(
                        object.object.diq.iv,
                        object.object.diq.nt,
                        object.object.diq.sb,
                        object.object.diq.bl,
                        false,
                    ),
                    updated,
                )
            })
            .collect(),
        InformationObjects::MMeNc1(objects) => objects
            .iter()
            .map(|object| {
                row(
                    object.address,
                    "M_ME_NC_1",
                    "短浮点遥测",
                    format!("{:.6}", object.object.value),
                    quality_bits(
                        object.object.qds.iv,
                        object.object.qds.nt,
                        object.object.qds.sb,
                        object.object.qds.bl,
                        object.object.qds.ov,
                    ),
                    updated,
                )
            })
            .collect(),
        InformationObjects::MMeNa1(objects) => objects
            .iter()
            .map(|object| {
                row(
                    object.address,
                    "M_ME_NA_1",
                    "归一化遥测",
                    object.object.nva.to_string(),
                    quality_bits(
                        object.object.qds.iv,
                        object.object.qds.nt,
                        object.object.qds.sb,
                        object.object.qds.bl,
                        object.object.qds.ov,
                    ),
                    updated,
                )
            })
            .collect(),
        InformationObjects::MMeNd1(objects) => objects
            .iter()
            .map(|object| {
                row(
                    object.address,
                    "M_ME_ND_1",
                    "无品质遥测",
                    object.object.nva.to_string(),
                    "无品质位".to_owned(),
                    updated,
                )
            })
            .collect(),
        InformationObjects::MItNa1(objects) => objects
            .iter()
            .map(|object| {
                row(
                    object.address,
                    "M_IT_NA_1",
                    "电度",
                    object.object.bcr.to_string(),
                    counter_quality(
                        object.object.sqd.iv,
                        object.object.sqd.ca,
                        object.object.sqd.cy,
                        object.object.sqd.seq,
                    ),
                    updated,
                )
            })
            .collect(),
        InformationObjects::MSpTb1(objects) => objects
            .iter()
            .map(|object| {
                row(
                    object.address,
                    "M_SP_TB_1",
                    "单点 SOE",
                    spi_label(object.object.siq.spi),
                    quality_bits(
                        object.object.siq.iv,
                        object.object.siq.nt,
                        object.object.siq.sb,
                        object.object.siq.bl,
                        false,
                    ),
                    updated,
                )
            })
            .collect(),
        InformationObjects::MDpTb1(objects) => objects
            .iter()
            .map(|object| {
                row(
                    object.address,
                    "M_DP_TB_1",
                    "双点 SOE",
                    dpi_label(object.object.diq.dpi),
                    quality_bits(
                        object.object.diq.iv,
                        object.object.diq.nt,
                        object.object.diq.sb,
                        object.object.diq.bl,
                        false,
                    ),
                    updated,
                )
            })
            .collect(),
        _ => Vec::new(),
    }
}

pub const fn is_general_data_cot(cot: Cot) -> bool {
    matches!(
        cot,
        Cot::InterrogationGeneral
            | Cot::InterrogationGroup1
            | Cot::InterrogationGroup2
            | Cot::InterrogationGroup3
            | Cot::InterrogationGroup4
            | Cot::InterrogationGroup5
            | Cot::InterrogationGroup6
            | Cot::InterrogationGroup7
            | Cot::InterrogationGroup8
            | Cot::InterrogationGroup9
            | Cot::InterrogationGroup10
            | Cot::InterrogationGroup11
            | Cot::InterrogationGroup12
            | Cot::InterrogationGroup13
            | Cot::InterrogationGroup14
            | Cot::InterrogationGroup15
            | Cot::InterrogationGroup16
    )
}

pub const fn is_counter_data_cot(cot: Cot) -> bool {
    matches!(
        cot,
        Cot::CounterInterrogationGeneral
            | Cot::CounterInterrogationGroup1
            | Cot::CounterInterrogationGroup2
            | Cot::CounterInterrogationGroup3
            | Cot::CounterInterrogationGroup4
    )
}

pub fn current_cp56_time() -> Result<Cp56Time2a, String> {
    cp56_from_local_datetime(&Local::now())
}

pub fn cp56_from_unix_millis(unix_millis: u64) -> Result<Cp56Time2a, String> {
    let timestamp =
        i64::try_from(unix_millis).map_err(|_| format!("Unix 毫秒超出 i64 范围: {unix_millis}"))?;
    let utc = DateTime::<Utc>::from_timestamp_millis(timestamp)
        .ok_or_else(|| format!("Unix 毫秒无法转换为时间: {unix_millis}"))?;
    let local = utc.with_timezone(&Local);
    cp56_from_local_datetime(&local)
}

fn cp56_from_local_datetime<Tz: chrono::TimeZone>(
    value: &DateTime<Tz>,
) -> Result<Cp56Time2a, String> {
    if !(2000..=2099).contains(&value.year()) {
        return Err(format!(
            "CP56Time2a 年份必须在 2000..=2099，实际 {}",
            value.year()
        ));
    }
    let milliseconds = value
        .second()
        .saturating_mul(1_000)
        .saturating_add(value.timestamp_subsec_millis());
    Ok(Cp56Time2a {
        ms: milliseconds.min(59_999) as u16,
        iv: false,
        min: value.minute() as u8,
        // 当前联调环境为 Asia/Shanghai，无夏令时。CP56 本身不携带时区。
        summer_time: false,
        hour: value.hour() as u8,
        weekday: value.weekday().number_from_monday() as u8,
        day: value.day() as u8,
        month: value.month() as u8,
        year: (value.year() - 2000) as u8,
    })
}

fn bool_value(value: f64, label: &str) -> Result<bool, String> {
    match value {
        0.0 => Ok(false),
        1.0 => Ok(true),
        _ => Err(format!("{label}值只能为 0 或 1")),
    }
}

fn integer_value(value: f64, min: i64, max: i64, label: &str) -> Result<i64, String> {
    if !value.is_finite() || value.fract() != 0.0 || value < min as f64 || value > max as f64 {
        return Err(format!("{label}值必须是 {min}..={max} 的整数"));
    }
    Ok(value as i64)
}

fn row(
    ioa: u32,
    type_id: &str,
    type_name: &str,
    value: impl Into<String>,
    quality: String,
    updated: u64,
) -> PointView {
    PointView {
        ioa,
        name: String::new(),
        type_id: type_id.to_owned(),
        type_name: type_name.to_owned(),
        value: Some(value.into()),
        quality,
        updated_ms: Some(updated),
        configured: false,
    }
}

fn quality_bits(iv: bool, nt: bool, sb: bool, bl: bool, ov: bool) -> String {
    let mut flags = Vec::new();
    if iv {
        flags.push("IV");
    }
    if nt {
        flags.push("NT");
    }
    if sb {
        flags.push("SB");
    }
    if bl {
        flags.push("BL");
    }
    if ov {
        flags.push("OV");
    }
    if flags.is_empty() {
        "GOOD".to_owned()
    } else {
        flags.join("|")
    }
}

fn counter_quality(iv: bool, ca: bool, cy: bool, sequence: u8) -> String {
    let mut flags = Vec::new();
    if iv {
        flags.push("IV".to_owned());
    }
    if ca {
        flags.push("CA".to_owned());
    }
    if cy {
        flags.push("CY".to_owned());
    }
    if flags.is_empty() {
        flags.push("GOOD".to_owned());
    }
    flags.push(format!("S{sequence}"));
    flags.join("|")
}

fn point_preview(objects: &InformationObjects) -> Option<String> {
    rows_from_objects(objects)
        .into_iter()
        .take(4)
        .reduce(|left, right| format!("{left}, {right}"))
}

fn rows_from_objects(objects: &InformationObjects) -> Vec<String> {
    match objects {
        InformationObjects::MSpNa1(values) => values
            .iter()
            .map(|value| format!("IOA{}={}", value.address, spi_label(value.object.siq.spi)))
            .collect(),
        InformationObjects::MDpNa1(values) => values
            .iter()
            .map(|value| format!("IOA{}={}", value.address, dpi_label(value.object.diq.dpi)))
            .collect(),
        InformationObjects::MMeNa1(values) => values
            .iter()
            .map(|value| format!("IOA{}={}", value.address, value.object.nva))
            .collect(),
        InformationObjects::MMeNd1(values) => values
            .iter()
            .map(|value| format!("IOA{}={}", value.address, value.object.nva))
            .collect(),
        InformationObjects::MMeNc1(values) => values
            .iter()
            .map(|value| format!("IOA{}={:.3}", value.address, value.object.value))
            .collect(),
        InformationObjects::MItNa1(values) => values
            .iter()
            .map(|value| format!("IOA{}={}", value.address, value.object.bcr))
            .collect(),
        InformationObjects::MSpTb1(values) => values
            .iter()
            .map(|value| {
                format!(
                    "IOA{}={}@{:?}",
                    value.address,
                    spi_label(value.object.siq.spi),
                    value.object.time
                )
            })
            .collect(),
        InformationObjects::MDpTb1(values) => values
            .iter()
            .map(|value| {
                format!(
                    "IOA{}={}@{:?}",
                    value.address,
                    dpi_label(value.object.diq.dpi),
                    value.object.time
                )
            })
            .collect(),
        InformationObjects::CScNa1(values) => values
            .iter()
            .map(|value| {
                format!(
                    "IOA{}={} {:?}",
                    value.address,
                    spi_label(value.object.sco.scs),
                    value.object.sco.se
                )
            })
            .collect(),
        InformationObjects::CdcNa1(values) => values
            .iter()
            .map(|value| {
                format!(
                    "IOA{}={} {:?}",
                    value.address,
                    dpi_label(value.object.dco.dcs),
                    value.object.dco.se
                )
            })
            .collect(),
        InformationObjects::CrcNa1(values) => values
            .iter()
            .map(|value| {
                format!(
                    "IOA{}={:?} {:?}",
                    value.address, value.object.rco.rcs, value.object.rco.se
                )
            })
            .collect(),
        InformationObjects::CSeNa1(values) => values
            .iter()
            .map(|value| {
                format!(
                    "IOA{}={} {:?}",
                    value.address, value.object.nva, value.object.qos.se
                )
            })
            .collect(),
        InformationObjects::CSeNb1(values) => values
            .iter()
            .map(|value| {
                format!(
                    "IOA{}={} {:?}",
                    value.address, value.object.sva, value.object.qos.se
                )
            })
            .collect(),
        InformationObjects::CSeNc1(values) => values
            .iter()
            .map(|value| {
                format!(
                    "IOA{}={:.3} {:?}",
                    value.address, value.object.value, value.object.qos.se
                )
            })
            .collect(),
        InformationObjects::CIcNa1(values) => values
            .iter()
            .map(|value| format!("QOI={}", value.object.qoi.to_byte()))
            .collect(),
        InformationObjects::CCiNa1(values) => values
            .iter()
            .map(|value| {
                format!(
                    "QCC={}/{}",
                    value.object.rqt.to_byte(),
                    value.object.frz as u8
                )
            })
            .collect(),
        InformationObjects::CRdNa1(values) => values
            .iter()
            .map(|value| format!("IOA{}", value.address))
            .collect(),
        _ => Vec::new(),
    }
}

fn object_details(objects: &InformationObjects) -> Vec<String> {
    let mut rows = rows_from_objects(objects);
    if rows.is_empty() {
        rows.push(format!("对象: {objects:?}"));
    } else {
        for (index, row) in rows.iter_mut().enumerate() {
            *row = format!("对象 #{index}: {row}");
        }
    }
    rows
}

pub const fn type_label(type_id: TypeId) -> &'static str {
    match type_id {
        TypeId::M_SP_NA_1 => "M_SP_NA_1 单点遥信",
        TypeId::M_DP_NA_1 => "M_DP_NA_1 双点遥信",
        TypeId::M_ME_NA_1 => "M_ME_NA_1 归一化遥测",
        TypeId::M_ME_ND_1 => "M_ME_ND_1 无品质遥测",
        TypeId::M_ME_NC_1 => "M_ME_NC_1 短浮点遥测",
        TypeId::M_IT_NA_1 => "M_IT_NA_1 电度",
        TypeId::M_SP_TB_1 => "M_SP_TB_1 单点 SOE",
        TypeId::M_DP_TB_1 => "M_DP_TB_1 双点 SOE",
        TypeId::M_EI_NA_1 => "M_EI_NA_1 初始化结束",
        TypeId::C_SC_NA_1 => "C_SC_NA_1 单点遥控",
        TypeId::C_DC_NA_1 => "C_DC_NA_1 双点遥控",
        TypeId::C_RC_NA_1 => "C_RC_NA_1 升降控制",
        TypeId::C_SE_NA_1 => "C_SE_NA_1 归一化遥调",
        TypeId::C_SE_NB_1 => "C_SE_NB_1 标度化遥调",
        TypeId::C_SE_NC_1 => "C_SE_NC_1 短浮点遥调",
        TypeId::C_IC_NA_1 => "C_IC_NA_1 总召",
        TypeId::C_CI_NA_1 => "C_CI_NA_1 电度总召",
        TypeId::C_RD_NA_1 => "C_RD_NA_1 读命令",
        TypeId::C_CS_NA_1 => "C_CS_NA_1 校时",
        _ => "其他 ASDU",
    }
}

pub const fn cot_label(cot: Cot) -> &'static str {
    match cot {
        Cot::SpontaneousData => "主动上送",
        Cot::Initiated => "初始化",
        Cot::Request => "请求",
        Cot::Activation => "激活",
        Cot::ActivationConfirmation => "激活确认",
        Cot::ActivationTermination => "激活结束",
        Cot::InterrogationGeneral => "总召响应",
        Cot::CounterInterrogationGeneral => "电度总召响应",
        Cot::UnknownType => "未知类型",
        Cot::UnknownCause => "未知原因",
        Cot::UnknownAsduAddress => "未知公共地址",
        Cot::UnknownObjectAddress => "未知 IOA",
        _ => "分组/其他",
    }
}

pub const fn spi_label(spi: Spi) -> &'static str {
    match spi {
        Spi::Off => "0/分",
        Spi::On => "1/合",
    }
}

pub const fn dpi_label(dpi: Dpi) -> &'static str {
    match dpi {
        Dpi::Indeterminate => "0/不确定",
        Dpi::Off => "1/分",
        Dpi::On => "2/合",
        Dpi::Invalid => "3/无效",
    }
}

#[cfg(test)]
mod tests {
    use chrono::{Datelike, Timelike};

    use super::*;

    fn request(action: DispatchAction) -> DispatchRequest {
        DispatchRequest {
            action,
            ioa: 123,
            value: 1.0,
            phase: ControlPhase::Execute,
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

    #[test]
    fn all_dispatch_application_actions_build_expected_type_ids() {
        let cases = [
            (DispatchAction::GeneralInterrogation, TypeId::C_IC_NA_1),
            (DispatchAction::CounterInterrogation, TypeId::C_CI_NA_1),
            (DispatchAction::ClockSync, TypeId::C_CS_NA_1),
            (DispatchAction::Read, TypeId::C_RD_NA_1),
            (DispatchAction::SingleControl, TypeId::C_SC_NA_1),
            (DispatchAction::DoubleControl, TypeId::C_DC_NA_1),
            (DispatchAction::RegulatingStep, TypeId::C_RC_NA_1),
            (DispatchAction::NormalizedSetpoint, TypeId::C_SE_NA_1),
            (DispatchAction::ScaledSetpoint, TypeId::C_SE_NB_1),
            (DispatchAction::FloatSetpoint, TypeId::C_SE_NC_1),
        ];
        for (action, type_id) in cases {
            let asdu = build_dispatch_request(&request(action))
                .expect("valid request")
                .expect("application ASDU");
            assert_eq!(asdu.type_id, type_id);
            assert_eq!(asdu.address_field, 1);
            assert_eq!(asdu.originator_address, 7);
        }
    }

    #[test]
    fn clock_sync_can_use_an_explicit_unix_millisecond_value() {
        let mut request = request(DispatchAction::ClockSync);
        request.clock_time_ms = Some(1_767_225_600_123);
        let asdu = build_dispatch_request(&request)
            .expect("clock request")
            .expect("clock ASDU");
        let InformationObjects::CCsNa1(values) = asdu.information_objects else {
            panic!("expected C_CS_NA_1");
        };
        let time = &values[0].object.time;
        let expected = DateTime::<Utc>::from_timestamp_millis(1_767_225_600_123)
            .expect("timestamp")
            .with_timezone(&Local);
        assert_eq!(time.year, (expected.year() - 2000) as u8);
        assert_eq!(time.month, expected.month() as u8);
        assert_eq!(time.day, expected.day() as u8);
        assert_eq!(time.hour, expected.hour() as u8);
        assert_eq!(time.ms, 123);
    }

    #[test]
    fn protocol_metadata_contains_direct_fields_and_ioas() {
        let asdu = build_dispatch_request(&request(DispatchAction::SingleControl))
            .expect("control")
            .expect("ASDU");
        let meta = protocol_meta(&asdu);
        assert_eq!(meta.type_id, "C_SC_NA_1");
        assert_eq!(meta.common_address, 1);
        assert_eq!(meta.originator_address, 7);
        assert_eq!(meta.ioas, vec![123]);
    }

    #[test]
    fn double_control_ui_zero_one_maps_to_iec_off_on_states() {
        let mut on = request(DispatchAction::DoubleControl);
        on.value = 1.0;
        let asdu = build_dispatch_request(&on)
            .expect("double on")
            .expect("ASDU");
        let InformationObjects::CdcNa1(values) = asdu.information_objects else {
            panic!("expected C_DC_NA_1");
        };
        assert_eq!(values[0].object.dco.dcs, Dpi::On);

        let mut off = request(DispatchAction::DoubleControl);
        off.value = 0.0;
        let asdu = build_dispatch_request(&off)
            .expect("double off")
            .expect("ASDU");
        let InformationObjects::CdcNa1(values) = asdu.information_objects else {
            panic!("expected C_DC_NA_1");
        };
        assert_eq!(values[0].object.dco.dcs, Dpi::Off);
    }

    #[test]
    fn dispatch_rows_preserve_quality_bits() {
        let asdu = make_asdu(
            TypeId::M_SP_NA_1,
            Cot::InterrogationGeneral,
            1,
            0,
            false,
            InformationObjects::MSpNa1(vec![GenericObject {
                address: 9,
                object: iec104::types::MSpNa1 {
                    siq: iec104::types::information_elements::Siq {
                        iv: true,
                        sb: true,
                        spi: Spi::On,
                        ..iec104::types::information_elements::Siq::default()
                    },
                },
            }]),
        );
        let rows = rows_from_asdu(&asdu);
        assert_eq!(rows[0].type_id, "M_SP_NA_1");
        assert_eq!(rows[0].quality, "IV|SB");
    }

    #[test]
    fn request_builder_rejects_fields_that_cannot_be_encoded_losslessly() {
        let mut control = request(DispatchAction::SingleControl);
        control.qualifier = 32;
        assert!(build_dispatch_request(&control).is_err());

        let mut general = request(DispatchAction::GeneralInterrogation);
        general.qoi = 19;
        assert!(build_dispatch_request(&general).is_err());

        let mut counter = request(DispatchAction::CounterInterrogation);
        counter.qcc_freeze = 4;
        assert!(build_dispatch_request(&counter).is_err());
    }
}
