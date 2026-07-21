# agcavc104test 使用说明

`agcavc104test` 是一个带终端界面（TUI）的 IEC 60870-5-104 双端人工联调工具。程序启动后会同时运行以下两侧：

| 工具侧 | IEC 104 角色 | 用途 |
| --- | --- | --- |
| 采集侧 | 子站（服务端） | 监听端口，供 AGCAVC 的采集主站连接；可应答总召、校时、遥控和短浮点遥调，并可主动上送模拟点值 |
| 调度侧 | 主站（客户端） | 主动连接 AGCAVC 的调度子站；可手工发送 STARTDT、总召、读命令、遥控、遥调等指令 |

程序只通过配置文件控制，不接受任何命令行参数。正常启动命令中不要添加 `--host`、`--port`、`--help` 等参数。

## 1. 使用前准备

### 1.1 使用 GitHub Release 发布包

发布包通过 GitHub Release 提供：

- [下载 agcavc104test 0.1.0 Windows x86_64 发布包](https://github.com/Qiaofanxing/agcavc_104_test/releases/download/v0.1.0/agcavc104test-0.1.0-windows-x86_64.zip)
- [下载 agcavc104test 0.1.0 macOS aarch64 发布包](https://github.com/Qiaofanxing/agcavc_104_test/releases/download/v0.1.0/agcavc104test-0.1.0-macos-aarch64.tar.gz)

使用发布包不需要安装 Rust。解压后应保留以下结构：

```text
agcavc104test-0.1.0-windows-x86_64/
├── agcavc104test.exe
└── config/
    ├── agcavc104test.toml
    ├── collect-points.toml
    └── dispatch-points.toml
```

macOS 发布包中的目录结构相同，但根目录名为 `agcavc104test-0.1.0-macos-aarch64`，可执行文件名为 `agcavc104test`。

不要只复制可执行文件。程序运行时必须能够从当前工作目录读取 `config` 目录。

### 1.2 从源码运行

从源码运行需要：

- 可用的 Rust stable 工具链和 Cargo；
- 支持 UTF-8、颜色和鼠标事件的终端；
- 至少 `80 × 24` 的终端尺寸；
- 可用的本地监听端口，以及到目标 IEC 104 设备的网络连通性。

可先确认 Rust 环境：

```bash
rustc --version
cargo --version
```

## 2. 快速启动

### 2.1 Windows 发布包

先修改解压目录下的配置文件，然后在该目录打开 PowerShell：

```powershell
.\agcavc104test.exe
```

建议从 PowerShell 或 Windows Terminal 启动，不要直接双击 EXE。这样既能保证工作目录正确，也便于查看启动失败信息。

### 2.2 macOS 发布包

在终端中解压并进入发布包目录：

```bash
tar -xzf agcavc104test-0.1.0-macos-aarch64.tar.gz
cd agcavc104test-0.1.0-macos-aarch64
./agcavc104test
```

当前 macOS 发布包未使用 Apple Developer ID 签名。如果系统阻止首次运行，请在“系统设置 → 隐私与安全性”中确认允许后再次启动。

### 2.3 从源码启动

在项目根目录执行：

```bash
cargo run --locked
```

程序会固定读取：

```text
config/agcavc104test.toml
```

以下启动方式会被拒绝：

```bash
cargo run -- --help
cargo run -- --host 127.0.0.1
cargo run -- master
```

### 2.4 构建本机发布版本

```bash
cargo build --release --locked
```

构建完成后，在项目根目录启动：

```bash
./target/release/agcavc104test
```

Windows 本机构建后可在项目根目录执行：

```powershell
.\target\release\agcavc104test.exe
```

无论以哪种方式启动，都要让当前工作目录中存在 `config/agcavc104test.toml` 及其引用的点表文件。

## 3. 首次联调步骤

### 3.1 联调 AGCAVC 采集主站

此时本工具作为 IEC 104 子站。

1. 在 `[collect]` 中设置监听地址、端口和公共地址。
2. 在 `config/collect-points.toml` 中准备需要模拟的遥信、遥测、电度和控制目标。
3. 启动本工具。
4. 让 AGCAVC 采集主站连接 `collect.bind_host:collect.bind_port`。
5. 等待对端发送 STARTDT；状态页显示“数据传输激活”后即可交互。
6. 在“采集日志”页查看对端总召、校时、遥控或遥调。
7. 在“采集指令”页主动上送点值，或在“采集值”页手工修改点值。

若其他机器需要连接本工具，通常应将 `collect.bind_host` 改为本机实际网卡地址或 `0.0.0.0`，并放行对应防火墙端口。仅在可信测试网络中使用 `0.0.0.0`。

采集子站当前处理的应用层命令如下：

| TypeID | 处理范围 |
| --- | --- |
| `C_IC_NA_1` | 全站总召和第 1～16 组分组召唤 |
| `C_CI_NA_1` | 全电度召唤和第 1～4 组电度召唤 |
| `C_CS_NA_1` | 校时激活确认 |
| `C_SC_NA_1` | 单点遥控 |
| `C_DC_NA_1` | 双点遥控 |
| `C_RC_NA_1` | 升降控制 |
| `C_SE_NC_1` | 短浮点遥调 |

`C_RD_NA_1`、`C_SE_NA_1`、`C_SE_NB_1` 及其他未列出的应用层 TypeID 会显示在采集日志中，但采集子站当前不会为其生成业务响应。

### 3.2 联调 AGCAVC 调度子站

此时本工具作为 IEC 104 主站。

1. 在 `[dispatch]` 中填写调度子站的 IP、端口和公共地址。
2. 在 `config/dispatch-points.toml` 中配置预期遥信、遥测、电度点和命令提示点。
3. 先启动或确认 AGCAVC 调度子站可连接，再启动本工具。
4. 调度侧会自动发起 TCP 连接；连接失败时按 `reconnect_ms` 周期重试。
5. TCP 连接成功后，调度侧会保持“已连接/未启动传输”。进入“调度指令”页手工执行 `STARTDT`。
6. 状态变为“数据传输激活”后，再执行总召、读命令、遥控或遥调。
7. 在“调度日志”页检查收发报文，在“调度值”页查看实时收到的遥信、遥测和电度。

### 3.3 本机双端回环

如需让本工具的调度主站直接连接本工具自己的采集子站，可将两侧配置成同一个端点和公共地址：

```toml
[collect]
bind_host = "127.0.0.1"
bind_port = 2405
common_address = 1

[dispatch]
target_host = "127.0.0.1"
target_port = 2405
common_address = 1
```

启动后进入“调度指令”页执行 `STARTDT`。随后可从调度侧发送总召、校时和受支持的控制命令，并在采集、调度两页日志中同时观察报文。调度侧可以发送读命令，但本机回环中的采集子站只记录该命令，不会返回读响应。

## 4. 主配置文件

主配置文件是 `config/agcavc104test.toml`。随项目提供的默认配置如下：

```toml
[protocol]
t0_ms = 10000
t1_ms = 15000
t2_ms = 10000
t3_ms = 20000
k = 12
w = 8
max_pending_outgoing_asdu = 1024
originator_address = 0

[collect]
bind_host = "127.0.0.1"
bind_port = 2405
common_address = 1
fault_delay_ms = 2000

[dispatch]
target_host = "127.0.0.1"
target_port = 2404
common_address = 1
reconnect_ms = 3000

[ui]
tick_ms = 100
log_capacity = 2000
command_capacity = 256
event_capacity = 4096

[points]
collect = "config/collect-points.toml"
dispatch = "config/dispatch-points.toml"
```

### 4.1 `[protocol]`

| 字段 | 默认值 | 使用说明 |
| --- | ---: | --- |
| `t0_ms` | `10000` | TCP 建连等待时间，单位毫秒 |
| `t1_ms` | `15000` | 已发送 APDU 等待确认的超时时间，单位毫秒 |
| `t2_ms` | `10000` | 无反向数据时延迟发送确认的时间，单位毫秒 |
| `t3_ms` | `20000` | 空闲链路测试周期，单位毫秒 |
| `k` | `12` | 未确认 I 帧发送窗口大小 |
| `w` | `8` | 接收 I 帧确认阈值 |
| `max_pending_outgoing_asdu` | `1024` | 待发送应用报文的容量 |
| `originator_address` | `0` | 默认源发地址 OA，取值范围 `0..=255` |

注意：

- `t0_ms`、`t1_ms`、`t2_ms`、`t3_ms` 必须大于 `0`；
- 必须满足 `t2_ms < t1_ms < t3_ms`；
- `k`、`w` 必须满足 IEC 104 协议库的窗口参数校验；
- 没有明确联调要求时，建议保留默认值。

### 4.2 `[collect]`

| 字段 | 默认值 | 使用说明 |
| --- | ---: | --- |
| `bind_host` | `127.0.0.1` | 采集子站监听地址。跨主机联调时填写实际网卡地址或 `0.0.0.0` |
| `bind_port` | `2405` | 采集子站监听端口，不能为 `0` |
| `common_address` | `1` | 采集子站公共地址 CA，必须在 `1..=65534` 内 |
| `fault_delay_ms` | `2000` | 通过界面选择“一次性延迟成功”时采用的延迟时间 |

`bind_host` 对应本机监听地址，不是远端 AGCAVC 主站地址。

### 4.3 `[dispatch]`

| 字段 | 默认值 | 使用说明 |
| --- | ---: | --- |
| `target_host` | `127.0.0.1` | 调度子站的目标 IP 或主机名 |
| `target_port` | `2404` | 调度子站端口，不能为 `0` |
| `common_address` | `1` | 默认目标公共地址 CA，必须在 `1..=65534` 内 |
| `reconnect_ms` | `3000` | 连接失败或断开后的重连间隔，必须大于 `0` |

TCP 建立后不会自动发送 STARTDT。必须在“调度指令”页手工执行 `STARTDT`，以便人工控制测试时序。

### 4.4 `[ui]`

| 字段 | 默认值 | 使用说明 |
| --- | ---: | --- |
| `tick_ms` | `100` | 界面刷新和输入轮询间隔，必须大于 `0` |
| `log_capacity` | `2000` | 每侧日志在内存中保留的最大条数，必须大于 `0` |
| `command_capacity` | `256` | 界面命令队列容量，必须大于 `0` |
| `event_capacity` | `4096` | 协议事件队列容量，必须大于 `0` |

大量连续上送或压力测试时，可以适当提高日志和队列容量，但会增加内存占用。

### 4.5 `[points]`

| 字段 | 默认值 | 使用说明 |
| --- | --- | --- |
| `collect` | `config/collect-points.toml` | 采集子站使用的模拟点表 |
| `dispatch` | `config/dispatch-points.toml` | 调度主站使用的预期点表和命令提示点表 |

路径按启动时的当前工作目录解析。修改路径后，确保目标文件存在且为合法 TOML。

## 5. 采集侧点表

采集点表默认为 `config/collect-points.toml`。每个点使用一个 `[[points]]` 段，例如：

```toml
[[points]]
name = "逆变器1 yc1"
ioa = 16385
type = "float"
purpose = "data"
group = 1
initial = 0
min = -100
max = 100
step = 0.5
quality = "good"
addressing = "auto"
```

控制目标示例：

```toml
[[points]]
name = "逆变器1 yk1"
ioa = 24577
type = "single_point"
purpose = "control_target"
control = "single"
group = 1
initial = 0
min = 0
max = 1
step = 1
quality = "good"
response = "success"
```

### 5.1 字段说明

| 字段 | 是否必填 | 使用说明 |
| --- | --- | --- |
| `name` | 是 | 点名，不能为空 |
| `ioa` | 是 | 信息对象地址，范围 `0..=16777215` |
| `type` | 是 | 点类型，见下一节 |
| `purpose` | 否 | `data`、`control_target` 或 `both`；默认 `data` |
| `control` | 条件必填 | 控制目标类型：`single`、`double`、`setpoint`、`regulating` |
| `group` | 否 | 总召组号，默认 `1`；普通点范围 `1..=16`，电度点范围 `1..=4` |
| `initial` | 是 | 启动初值 |
| `min` | 否 | 随机刷新、手工修改和控制写入允许的最小值 |
| `max` | 否 | 随机刷新、手工修改和控制写入允许的最大值 |
| `step` | 否 | 点值步长；必须大于 `0` |
| `soe` | 否 | 是否允许该单点或双点以带 CP56Time2a 的 SOE 类型主动上送；默认 `false` |
| `quality` | 否 | `good`、`invalid`、`blocked`、`substituted`；默认 `good` |
| `addressing` | 否 | `individual`、`sequence`、`auto`；默认 `individual` |
| `response` | 否 | 控制响应策略；默认 `success` |
| `response_delay_ms` | 否 | `response = "delay_success"` 时的点位延迟；为 `0` 时使用 `collect.fault_delay_ms` |

同一 `type + ioa` 不能重复。所有控制目标的 IOA 必须唯一。

### 5.2 采集点类型

| `type` | IEC 104 TypeID | 值要求 |
| --- | --- | --- |
| `single_point` | `M_SP_NA_1` | 初值只能是 `0` 或 `1` |
| `double_point` | `M_DP_NA_1` | 初值只能是 `0` 或 `1`；界面中的 `0/1` 分别映射 IEC 双点分/合状态 |
| `normalized` | `M_ME_NA_1` | `initial/min/max/step` 必须是 i16 范围整数 |
| `normalized_no_quality` | `M_ME_ND_1` | `initial/min/max/step` 必须是 i16 范围整数 |
| `float` | `M_ME_NC_1` | 必须是有限数值 |
| `counter` | `M_IT_NA_1` | `initial/min/max/step` 必须是 i32 范围整数 |

所有点均需满足：

```text
min <= initial <= max
step > 0
```

省略 `min`、`max`、`step` 时，程序会按点类型使用默认范围。

### 5.3 `purpose` 与 `control`

- `purpose = "data"`：参与总召和主动上送，不作为控制目标；
- `purpose = "control_target"`：作为遥控或遥调目标；
- `purpose = "both"`：同时作为数据点和控制目标。

控制目标未显式填写 `control` 时，可按以下类型推断：

| 点类型 | 推断的 `control` |
| --- | --- |
| `single_point` | `single` |
| `double_point` | `double` |
| `float` | `setpoint` |
| `normalized` | `regulating` |

`normalized_no_quality` 和 `counter` 不能自动推断控制类型。建议控制目标始终显式填写 `control`，避免点表含义不清。

### 5.4 编址方式

- `individual`：使用非顺序编址（SQ=0）；
- `sequence` 或 `auto`：连续 IOA 会尽量合并为顺序编址（SQ=1），不连续点仍会拆分发送。

### 5.5 控制响应策略

| `response` | 收到匹配控制后的行为 |
| --- | --- |
| `success` | 返回正向 ACTCON；执行命令再返回 ACTTERM，并提交新点值 |
| `reject` | 返回否定 ACTCON |
| `act_con_only` | 只返回正向 ACTCON，不返回 ACTTERM，也不提交执行值 |
| `silent` | 不返回控制响应 |
| `delay_success` | 延迟后按成功流程响应 |

界面中的一次性故障策略会覆盖点位默认响应一次；命中后自动清除。

## 6. 调度侧点表

调度点表默认为 `config/dispatch-points.toml`。它用于标识期望接收的点，并为调度命令表单提供 IOA 和默认值提示。

预期遥测点示例：

```toml
[[points]]
name = "逆变器1 yc1"
ioa = 16385
type = "float"
purpose = "expected_general"
```

命令提示点示例：

```toml
[[points]]
name = "逆变器1 yk1"
ioa = 24577
type = "single_control"
purpose = "command"
min = 0
max = 1
default_value = 1
```

### 6.1 字段说明

| 字段 | 是否必填 | 使用说明 |
| --- | --- | --- |
| `name` | 是 | 点名，不能为空 |
| `ioa` | 是 | 信息对象地址，范围 `0..=16777215` |
| `type` | 是 | 预期数据类型或命令类型 |
| `purpose` | 否 | `expected_general`、`expected_energy` 或 `command`；默认 `command` |
| `min` | 否 | 命令提示范围下限 |
| `max` | 否 | 命令提示范围上限 |
| `default_value` | 否 | 打开对应命令表单时优先填入的值 |

同一 `purpose + type + ioa` 不能重复。`min`、`max` 和 `default_value` 必须是有限数值；同时填写上下限时必须满足 `min <= max`。

`min` 和 `max` 用于配置校验与表单提示，不替代发送时的协议值校验。发送前仍应确认目标设备允许的工程值范围。

### 6.2 调度点类型

用于 `expected_general` 或 `expected_energy` 的数据类型：

| `type` | IEC 104 TypeID |
| --- | --- |
| `single_point` | `M_SP_NA_1` |
| `double_point` | `M_DP_NA_1` |
| `float` | `M_ME_NC_1` |
| `counter` | `M_IT_NA_1` |

用于 `command` 的命令类型：

| `type` | IEC 104 TypeID | `default_value` 要求 |
| --- | --- | --- |
| `single_control` | `C_SC_NA_1` | `0` 或 `1` |
| `double_control` | `C_DC_NA_1` | `0` 或 `1` |
| `regulating_step` | `C_RC_NA_1` | `-1` 或 `1` |
| `normalized_setpoint` | `C_SE_NA_1` | i16 范围整数 |
| `scaled_setpoint` | `C_SE_NB_1` | i16 范围整数 |
| `float_setpoint` | `C_SE_NC_1` | 有限数值 |

数据类型不能配置为 `purpose = "command"`，命令类型也不能配置为 `expected_general` 或 `expected_energy`。

## 7. TUI 页面与操作

程序共有七个页面：

| 页面 | 内容 | 主要用途 |
| ---: | --- | --- |
| `1` | 状态 | 查看两侧连接状态、端点、对端、总召事务、待处理应用回执和最近系统事件 |
| `2` | 采集日志 | 查看采集子站接收、发送和内部状态日志 |
| `3` | 调度日志 | 查看调度主站接收、发送和内部状态日志 |
| `4` | 采集指令 | 触发采集点主动上送 |
| `5` | 调度指令 | 发送链路命令、召唤、读命令、遥控和遥调 |
| `6` | 采集值 | 查看并手工修改采集侧当前点值 |
| `7` | 调度值 | 查看调度侧实际收到的遥信、遥测和电度 |

终端小于 `80 × 24` 时只会显示尺寸不足提示。请先放大窗口。

### 7.1 全局快捷键

| 按键 | 作用 |
| --- | --- |
| `1`～`7` | 直接切换页面 |
| `←` / `→` | 切换上一页或下一页 |
| 鼠标单击页签 | 切换页面 |
| `f` | 打开“下一条采集控制响应”一次性故障策略 |
| `r` | 重新读取主配置并热重载两侧点表 |
| `q` | 正常退出 |
| `Ctrl+C` | 正常退出 |

### 7.2 日志页

采集日志和调度日志均分为“接收”和“发送/状态”两个窗格。

| 操作 | 作用 |
| --- | --- |
| `↑` / `PageUp` | 向较早日志滚动 |
| `↓` / `PageDown` | 向较新日志滚动 |
| `Home` | 跳到最早日志 |
| `End` | 回到最新日志 |
| `p` | 在“全部、协议、控制、召唤、连接”过滤器之间循环 |
| `c` | 清空当前侧日志 |
| `Enter` | 打开当前偏移位置的结构化日志详情 |
| 鼠标单击日志行 | 打开该行的结构化详情 |
| 鼠标滚轮 | 滚动日志 |
| 详情窗口中的 `Enter` / `Esc` / `q` | 关闭详情窗口 |

结构化详情会显示 TypeID、COT、CA、OA、SQ、Test、Negative、IOA 以及信息体内容。链路命令和连接状态也会记录在对应日志页中。

### 7.3 采集指令页

使用 `↑`、`↓` 选择，按 `Enter` 执行，也可以直接单击项目。

| 指令 | 行为 |
| --- | --- |
| 主动上送全部当前值 | 不改变点值，按当前值发送所有可上送点 |
| 全部点变化并主动上送 | 先刷新所有点值，再主动上送 |
| 全部单点遥信变化上送 | 切换所有单点值并上送 |
| 全部双点遥信变化上送 | 切换所有双点值并上送 |
| 全部归一化遥测变化上送 | 刷新并上送 `normalized` 点 |
| 全部无品质遥测变化上送 | 刷新并上送 `normalized_no_quality` 点 |
| 全部短浮点遥测变化上送 | 刷新并上送 `float` 点 |
| 全部电度变化上送 | 刷新并上送 `counter` 点 |
| 全部单点 SOE 变化上送 | 刷新并上送 `soe = true` 的单点，使用 `M_SP_TB_1` |
| 全部双点 SOE 变化上送 | 刷新并上送 `soe = true` 的双点，使用 `M_DP_TB_1` |
| 初始化结束主动上送 | 发送 `M_EI_NA_1` |

主动上送前，采集链路必须已经完成 STARTDT。若点表中没有匹配类型，操作会在采集日志中显示失败原因。

### 7.4 采集值页

- 使用鼠标滚轮浏览点表；
- 单击一行打开“手工修改采集点值”窗口；
- 使用 `Backspace` 删除原值后输入新值；
- 按 `Enter` 或单击“确认修改”提交；
- 按 `Esc` 或单击弹窗外取消。

手工修改只更新工具中的当前模拟值，不会立即主动发送。需要发送时，再进入“采集指令”页执行“主动上送全部当前值”或对应类型的变化上送。

### 7.5 调度指令页

使用 `↑`、`↓` 或 `j`、`k` 选择，按 `Enter` 打开表单或立即执行，也可以直接单击指令。

支持以下指令：

| 指令 | TypeID / 作用 |
| --- | --- |
| `STARTDT` | 启动数据传输 |
| `STOPDT` | 停止数据传输 |
| 手工 `TESTFR` | 发送链路测试 |
| 总召 | `C_IC_NA_1` |
| 电度总召 | `C_CI_NA_1` |
| 校时 | `C_CS_NA_1` |
| 读命令 | `C_RD_NA_1` |
| 单点遥控 | `C_SC_NA_1` |
| 双点遥控 | `C_DC_NA_1` |
| 升降控制 | `C_RC_NA_1` |
| 归一化遥调 | `C_SE_NA_1` |
| 标度化遥调 | `C_SE_NB_1` |
| 短浮点遥调 | `C_SE_NC_1` |

`STARTDT`、`STOPDT` 和 `TESTFR` 会直接执行。其他指令会打开参数表单。

表单操作：

| 按键 | 作用 |
| --- | --- |
| `Tab` / `Shift+Tab` | 切换字段 |
| `Backspace` | 删除当前字段末尾字符 |
| 数字键 | 输入数值 |
| `s` | 在“选择”和“执行/直接执行”之间切换控制阶段 |
| `t` | 切换 Test 位 |
| `Enter` | 校验并发送 |
| `Esc` | 取消 |

字段已有默认值时，直接输入会追加字符；如需替换，请先用 `Backspace` 删除旧值。

### 7.6 调度表单参数

| 参数 | 说明 |
| --- | --- |
| `IOA` | 信息对象地址，范围 `0..=16777215` |
| `值/状态` | 控制或遥调值，见下表 |
| `控制阶段` | 选择（Select）或执行（Execute） |
| `CA` | 目标公共地址；可填写具体地址，也可填写 `65535` 广播地址 |
| `OA` | 源发地址，范围 `0..=255` |
| `限定词` | 遥控类范围 `0..=31`，遥调类范围 `0..=127` |
| `QOI` | 总召限定词，范围 `20..=36`；`20` 表示全站总召，`21..=36` 表示第 1～16 组 |
| `QCC 请求组` | 电度召唤组，范围 `1..=5`；`5` 表示全局 |
| `QCC 冻结` | 电度冻结限定词，范围 `0..=3` |
| `重复次数` | 同一请求的计划发送次数，范围 `1..=1000` |
| `间隔 ms` | 重复发送间隔，单位毫秒 |
| `校时时间` | `now` 或 Unix 毫秒时间戳；可表示的年份须在 `2000..=2099` |
| `测试位` | 是否设置 ASDU Test 标志 |

控制值要求：

| 指令 | 允许值 |
| --- | --- |
| 单点遥控 | `0` 或 `1` |
| 双点遥控 | `0` 或 `1` |
| 升降控制 | `-1`、`0` 或 `1`；分别表示降、无动作、升 |
| 归一化遥调 | i16 范围整数 |
| 标度化遥调 | i16 范围整数 |
| 短浮点遥调 | 有限 f32 范围数值 |

发送应用层指令前，调度链路必须处于“数据传输激活”状态。若未执行 STARTDT，调度日志会显示链路状态不允许发送。

### 7.7 调度值页

“调度值”页分为：

- 调度当前遥信/遥测；
- 调度当前电度。

收到总召、电度总召或自发上送数据后，表格会实时更新。使用鼠标滚轮分别滚动两个区域。

若实际收到的 IOA 未配置，点名会显示“未配置 IOA”；若同一 IOA 的类型与点表不一致，会显示“类型不匹配”。这两种情况应优先检查 `config/dispatch-points.toml` 和对端点表。

## 8. 故障注入

按 `f` 可设置下一条匹配的采集控制响应。可选策略如下：

| 策略 | 行为 |
| --- | --- |
| 清除等待策略 / 使用点位默认 | 取消一次性策略，恢复点表中的 `response` |
| 强制正常成功 | 下一条控制按成功流程处理 |
| 下一条选择：拒绝 | 下一条 Select 返回否定 ACTCON |
| 下一条执行：拒绝 | 下一条 Execute 返回否定 ACTCON |
| 下一条执行：仅 ACTCON | 下一条 Execute 只返回正向 ACTCON |
| 下一条控制：完全静默 | 不返回响应 |
| 下一条控制：延迟成功 | 按 `collect.fault_delay_ms` 延迟后成功响应 |
| 下一条控制：立即断链 | 收到匹配控制后立即断开采集连接 |

使用 `↑`、`↓` 选择，按 `Enter` 确认，按 `Esc` 取消。一次性策略只有在收到匹配阶段的控制时才会消费。

## 9. 运行中重载配置

修改点表后可按 `r` 重载。重载会重新读取主配置中 `[points]` 指向的两个点表，并同时更新采集侧和调度侧点表。

以下变更按 `r` 不会替换当前运行参数，必须退出并重新启动程序：

- `[protocol]` 的计时、窗口和 OA；
- `[collect]` 的监听地址、端口、CA 和故障延迟；
- `[dispatch]` 的目标地址、端口、CA 和重连周期；
- `[ui]` 的刷新周期和容量。

若新点表读取、解析或校验失败，程序会保留当前运行中的点表，并在状态页的系统事件中显示失败原因。

## 10. 退出与中断

推荐使用以下任一方式正常退出：

```text
q
Ctrl+C
```

正常退出会离开备用屏幕并恢复终端状态。若进程被强制终止后终端显示异常，可在 Unix 终端执行 `reset`，或关闭后重新打开终端窗口。

## 11. 常见问题

### 11.1 启动时报“读取主配置失败”

确认当前工作目录中存在：

```text
config/agcavc104test.toml
```

请从项目根目录或发布包解压根目录启动，不要在其他目录中直接执行绝对路径下的二进制。

### 11.2 启动时报“不接受命令行参数”

删除所有运行参数，修改 TOML 后直接启动：

```bash
cargo run --locked
```

### 11.3 终端只显示“尺寸过小”

将终端窗口调整到至少 `80 × 24`。

### 11.4 采集侧监听失败

常见原因：

- `collect.bind_port` 已被其他程序占用；
- `bind_host` 不是本机可绑定地址；
- 当前用户无权监听该端口；
- 防火墙或安全软件阻止监听。

修改为可用端口后重新启动。主配置中的监听参数不能热重载。

### 11.5 调度侧一直显示“等待重连”

检查：

- `dispatch.target_host` 和 `target_port` 是否正确；
- 对端调度子站是否已启动并监听；
- 本机到目标端口是否可达；
- 防火墙、VPN 或路由是否拦截连接。

### 11.6 TCP 已连接但不能发送总召或控制

如果状态为“已连接/未启动传输”，进入“调度指令”页执行 `STARTDT`。只有状态变为“数据传输激活”后才能发送应用层 ASDU。

### 11.7 对端返回未知站地址或本工具拒绝站地址

确认双方 CA 一致：

- 采集联调检查 `collect.common_address`；
- 调度联调检查 `dispatch.common_address` 和表单中的 CA；
- 广播地址 `65535` 只应在明确需要时使用。

### 11.8 主动上送提示“没有匹配的可上送点”

检查采集点表是否包含对应 `type`。SOE 上送还要求单点或双点配置 `soe = true`。

### 11.9 按 `r` 后地址或端口没有变化

`r` 只让新点表和新的点表路径生效。协议地址、端口、计时和界面容量变更需要重启程序。

### 11.10 点表修改后重载失败

查看状态页最近系统事件，并检查：

- TOML 引号、数组段和字段名是否正确；
- IOA、类型和用途组合是否重复；
- CA、组号、初值和范围是否合法；
- 控制目标是否有可用的 `control` 类型；
- 两个点表文件是否都可读取。

## 12. 源码环境自检

修改配置或升级依赖后，可在项目根目录运行：

```bash
cargo check --locked
cargo test --locked
```

`cargo check` 用于检查编译，`cargo test` 用于运行项目测试。真实网络联调仍需启动工具，并实际完成 TCP、STARTDT 和业务报文交互。

## 13. 使用注意事项

- 调度侧可向配置目标发送真实 IEC 104 控制和遥调命令，发送前务必核对 IP、端口、CA、IOA 和值；
- 生产设备联调前应先确认现场安全措施和操作授权；
- 不要仅依赖界面表单中的默认值判断目标设备允许范围；
- 非必要不要将采集监听地址暴露到公共网络；
- 压力测试前适当提高日志和队列容量，并关注终端刷新与内存占用。
