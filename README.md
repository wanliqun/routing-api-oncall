# RoutingAPI 告警转发 V1

正常情况下每 60 秒只读查询 CloudWatch Alarm，用一个 `state.json` 保存最新观测和逐项通知基线，将当前仍有效的告警变化及持续异常提醒发送到钉钉。不保存历史消息队列。运行时只有一个 Python 程序和一个 systemd Service。

默认排除 `RoutingAPI-*-Latency`。没有启动通知、每日心跳或独立自检服务，不修改线上 CloudWatch Alarm，也不需要新建 AWS 资源。

## 文件与运行位置

| 文件 | 用途 | 安装位置 |
|---|---|---|
| `agent.py` | 采集、过滤、逐项状态对比、持久化确认、发送 | `/opt/routing-oncall/agent.py` |
| `agent.env.example` | 配置模板，部署时填写 Webhook | `/etc/routing-oncall/agent.env` |
| `routing-oncall.service` | 启动与故障重启 | `/etc/systemd/system/routing-oncall.service` |
| `requirements.txt` | 唯一直接依赖 boto3 | 安装到 `/opt/routing-oncall/venv/` |
| `tests/test_agent.py` | 离线回归测试 | 无需安装到生产 |

状态文件由程序创建在 `/var/lib/routing-oncall/state.json`，不含 AWS 密钥或 Webhook。

## 通知规则

| 情况 | 行为 |
|---|---|
| 首次运行，当前没有 ALARM | 建立基线，静默 |
| 首次运行，已有未排除的 ALARM | 发一条实际异常摘要 |
| 新发现的告警当前为 ALARM | 通知 |
| 进入或离开 ALARM | 通知，包括 `ALARM ↔ INSUFFICIENT_DATA` |
| `OK ↔ INSUFFICIENT_DATA` | 静默 |
| 被监控的告警从查询结果中消失 | 通知“Alarm 已被删除” |
| 同轮多条变化 | 合并发送，超长时按完整告警项分批 |
| 存在 ALARM，距上次变化摘要或持续提醒已满 30 分钟 | 合并发送持续异常提醒 |
| 命中排除规则 | 不参与变化、删除、提醒和当前 ALARM 数量统计 |

30 分钟是全局提醒计时，不是每条 Alarm 的独立计时。本次变化或提醒的全部有效告警项确认后，以最后一条消息成功投递的实际时间重置计时。存在待通知的状态变化时优先处理变化；提醒只保存一份未确认的告警名称集合，不重复追加。

`ALARM → INSUFFICIENT_DATA` 表示数据不足，不能视为业务恢复。消息标注完整观测时间及“观测时 ALARM”，避免延迟投递被误认为当前状态。只有 `ALARM → OK` 表示该 Alarm 在观测时恢复正常。

## 当前状态、逐项确认与发送预算

采用“当前观测与通知基线对比”，不补发已经过时的中间变化：

| 场景 | 行为 |
|---|---|
| 异常尚未通知，下一轮已恢复 | 取消异常，不发送恢复 |
| 异常尚未通知，下一轮仍异常 | 继续发送异常 |
| 异常已通知，恢复发送失败且仍为 OK | 持续尝试发送恢复，重启后继续 |
| 恢复尚未通知，下一轮再次异常 | 取消待发恢复，按原持续异常提醒计时处理 |
| 发送失败期间反复抖动 | 根据最新状态和通知基线重新计算，不按顺序回放历史 |
| A、B 异常合并发送失败，随后 A 恢复、B 仍异常 | 只通知 B |
| 持续提醒发送失败，随后恢复 | 取消旧提醒，发送恢复 |
| 已通知 ALARM，数据不足通知失败，随后 OK | 仍发送 ALARM → OK |

状态字段如下：

| 字段 | 含义 |
|---|---|
| `schema_version` | 当前为 3 |
| `alarms` | 最近一次成功保存的完整采集结果 |
| `notified` | 每个告警的通知基线，只保存状态枚举；需通知的变化仅在成功后推进，静默变化直接推进 |
| `reminder_remaining` | 本次持续提醒尚未确认的告警名称，未开始或已完成时为空值 |
| `last_reminder_at` | 最近一次变化/提醒全部确认的实际投递时间；没有未结束的异常时为空 |

每轮先重新采集并保存最新观测，再计算本轮消息。同轮 15 项变化在长度允许时仍合并发送。消息过长时按完整告警项分批，每项只出现在一个批次；每条消息成功后只确认其中的告警，再保存状态。下一轮重新计算未确认项，正文和分批边界可以变化。

例如，15 项异常的第一条消息已发送、第二条失败，随后全部恢复：第一条中的告警需要发送恢复，未发送的告警直接取消。消息末尾只列观测时 ALARM 总数，避免其他批次的告警名称提前出现在已成功消息中。

| 配置 | 默认值 | 规则 |
|---|---:|---|
| `MAX_STATE_BYTES` | 2097152 | 整个状态文件序列化预算 2 MiB，包含观测、通知基线和提醒进度 |
| `MAX_SEND_ATTEMPTS_PER_ROUND` | 5 | 每轮全部 HTTP 尝试总数，包含首次、各批次及重试 |
| `SEND_BUDGET_SECONDS` | 20 | 每轮发送阶段时间预算，并受本轮剩余轮询时间限制 |

这些配置必须为正整数。

- 不保留历史快照和消息正文。状态量随当前告警及尚未确认删除的告警数量变化，不随抖动次数增长。已通知的告警删除后，其通知基线在删除消息成功时移除。
- 保存新快照前预留本轮全部确认和提醒进度的空间。超出状态预算时保留旧文件、记录 ERROR，并跳过本轮发送；下一轮仍重新查询。不会静默删除旧通知基线，也不会按旧快照发送。需要提高 `MAX_STATE_BYTES` 或缩小监控范围。
- 旧配置 `MAX_PENDING_NOTIFICATIONS`、`MAX_PENDING_BYTES` 已废弃，存在时启动会记录警告；请移除，并按需要设置 `MAX_STATE_BYTES`。
- 请求至少间隔 3.1 秒，重试同样遵守；次数或时间预算用完即结束，下轮继续采集和计算。
- 单次 HTTP 整体超时取 10 秒与剩余预算的较小值，包含连接与读取响应。使用 POSIX 定时信号，因此程序应在 Ubuntu/Linux 或 macOS 主线程运行。AWS 查询、落盘及系统调度仍可能延长轮询周期，不能承诺严格每 60 秒完成一次查询。
- AWS 临时查询失败时保留通知基线，跳过本轮发送，避免投递无法重新核实的旧状态；下次查询成功后继续处理。

## 告警排除配置

```ini
ALARM_PREFIX=RoutingAPI
ALARM_EXCLUDE_PATTERNS=RoutingAPI-*-Latency
```

`ALARM_PREFIX` 是 CloudWatch 的名称前缀，不是通配符。排除模式用标准库 `fnmatchcase` 匹配完整 Alarm 名称，区分大小写；`*` 匹配任意长度字符，`?` 匹配单个字符，也支持 Glob 的 `[]` 字符集合。多个模式用逗号分隔，忽略各项两端空格及空项；任意一个匹配就排除。

```ini
# 同时排除延迟和指定的 2XX 告警
ALARM_EXCLUDE_PATTERNS=RoutingAPI-*-Latency,RoutingAPI-SEV3-2XXAlarm-*
```

留空表示不排除。修改后执行 `sudo systemctl restart routing-oncall.service`，无需删除状态文件。

观测、通知基线和提醒进度都按当前排除规则过滤。增加排除项不会误发“已删除”，也不再投递该项尚未发送的变化。撤销排除后，当前为 ALARM 的项按新发现的异常通知。CloudWatch 自身及现有邮件通知不受影响。

## 首次部署

以下命令在目标 Ubuntu 服务器执行。默认已有 `ubuntu` 用户及可用的 AWS CLI 共享凭证；需要 Python 3.8+、venv、到 CloudWatch 和钉钉的 HTTPS 网络。新部署优先使用仍获维护的 Ubuntu/Python 版本。

### 1. 解压并验证现有 AWS 身份

```bash
unzip routing-oncall-v1.zip
cd routing-oncall

sudo -u ubuntu -H aws sts get-caller-identity
sudo -u ubuntu -H aws cloudwatch describe-alarms \
  --alarm-name-prefix RoutingAPI \
  --alarm-types MetricAlarm CompositeAlarm \
  --region ap-southeast-1 \
  --query 'MetricAlarms[*].[AlarmName,StateValue]' \
  --output table
```

使用当前账号已有的 `cloudwatch:DescribeAlarms` 读权限；没有该权限时需要由运维处理。查询包含 Composite Alarm，AWS 要求相应 DescribeAlarms 权限的资源范围为 `*`。Agent 不调用 DescribeAlarmHistory 或 SetAlarmState。

默认读取 `/home/ubuntu/.aws/credentials` 的 `default` profile；使用命名 profile 时，在 `agent.env` 增加 `AWS_PROFILE=实际名称`，验证命令也使用同一 profile。不要把 Access Key 复制到 `agent.env`。Boto3 的默认凭证链会优先读取已存在的环境凭证，因此升级时要移除旧环境文件中的密钥配置。

### 2. 安装代码与依赖

```bash
sudo apt-get update
sudo apt-get install -y python3-venv
sudo install -d -m 0755 /opt/routing-oncall
sudo python3 -m venv /opt/routing-oncall/venv
sudo /opt/routing-oncall/venv/bin/pip install -r requirements.txt
sudo install -m 0644 agent.py /opt/routing-oncall/agent.py

sudo install -d -o root -g ubuntu -m 0750 /etc/routing-oncall
sudo install -o root -g ubuntu -m 0640 agent.env.example /etc/routing-oncall/agent.env
sudoedit /etc/routing-oncall/agent.env
```

填写真实 `DINGTALK_WEBHOOK`。机器人使用自定义关键词时，关键词设置为 `RoutingAPI`，`DINGTALK_SECRET` 留空；使用加签时填写对应 Secret。配置采用 `KEY=value`，注释放在独立行，不要在值后追加注释。

安装模板会覆盖目标配置，以上步骤适用于首次安装；已有部署先看下方升级说明。

### 3. 启动并验收

```bash
sudo install -m 0644 routing-oncall.service /etc/systemd/system/routing-oncall.service
sudo systemctl daemon-reload
sudo systemctl enable --now routing-oncall.service
sudo systemctl status routing-oncall.service --no-pager
sudo journalctl -u routing-oncall.service -n 50 --no-pager
```

Service 以 `ubuntu` 运行，`HOME=/home/ubuntu`；`ProtectHome=read-only` 允许读取已有 AWS 凭证，`StateDirectory` 提供可写状态目录。不要改为 `ProtectHome=true`，它会屏蔽 home 目录。

等待至少一个轮询周期后检查：

```bash
sudo -u ubuntu python3 - <<'PY'
import json
from fnmatch import fnmatchcase
from pathlib import Path
p = Path('/var/lib/routing-oncall/state.json')
st = json.loads(p.read_text())
alarms = st['alarms']
print('state updated:', p.stat().st_mtime)
print('monitored:', len(alarms))
print('ALARM:', sum(a['state'] == 'ALARM' for a in alarms.values()))
print('Latency still included:', any(fnmatchcase(n, 'RoutingAPI-*-Latency') for n in alarms))
print('schema version:', st.get('schema_version'))
print('notification baselines:', len(st.get('notified', {})))
print('remaining reminders:', len(st.get('reminder_remaining') or []))
PY
```

默认配置下 `Latency still included` 应为 `False`。无实际异常时群内静默是预期行为，不能用“没收到消息”判断 Webhook 已验证。真实钉钉投递需在部署后通过实际告警验证；此交付包没有发送测试消息或修改线上告警。

## 从旧版升级

保留 `state.json`，先备份现有代码、配置和状态。**若现有文件为 schema 2 且 `pending` 非空，先使用原程序完成投递，确认队列为空，再停止服务升级。** 旧文件没有逐项确认映射，新程序无法安全推断已送达的告警，因此会保留原文件并退出 2，不自动丢弃或回放旧队列。无版本的旧基线文件、以及队列为空的 schema 2 文件会自动转换为 schema 3。回滚前保留 schema 3 状态备份；旧程序不理解逐项通知基线，不能保证未确认恢复通知的延续。

若旧版额外的 unit 已安装，先停用：

```bash
sudo systemctl stop routing-oncall.service
sudo systemctl disable --now routing-oncall-stale.timer
sudo systemctl stop routing-oncall-stale.service routing-oncall-dead.service
```

不存在的旧 unit 会提示未找到，按实际安装情况跳过。随后按首次部署的依赖和代码安装步骤更新 `agent.py`，用新文件覆盖主 Service；不要直接用模板覆盖现有 `agent.env`。编辑配置完成以下改动：

- 删除 `AWS_ACCESS_KEY_ID`、`AWS_SECRET_ACCESS_KEY`、`AWS_SESSION_TOKEN`、`HEARTBEAT_HOUR`，确认不再从其他 systemd drop-in 注入旧凭证。
- 保留现有钉钉 Webhook/Secret，增加 `ALARM_EXCLUDE_PATTERNS=RoutingAPI-*-Latency`。
- 使用 `AWS_DEFAULT_REGION=ap-southeast-1`；旧配置中的 `AWS_REGION` 仍兼容，若同时设置则以 `AWS_REGION` 为准。
- 保留 60 秒轮询、30 分钟提醒和 `MAX_SEND_FAILURES=5`。
- 删除 `MAX_PENDING_NOTIFICATIONS`、`MAX_PENDING_BYTES`，改用 `MAX_STATE_BYTES=2097152`；发送预算参数继续生效。不要覆盖真实 Webhook。

调整旧目录所有权、移除不再需要的已知文件，再启动：

```bash
sudo chown root:ubuntu /etc/routing-oncall/agent.env
sudo chmod 0640 /etc/routing-oncall/agent.env
sudo chown -R ubuntu:ubuntu /var/lib/routing-oncall
sudo chmod 0700 /var/lib/routing-oncall
sudo rm -f /opt/routing-oncall/notify-dead.sh /opt/routing-oncall/check-stale.sh
sudo rm -f /etc/systemd/system/routing-oncall-dead.service \
  /etc/systemd/system/routing-oncall-stale.service \
  /etc/systemd/system/routing-oncall-stale.timer
sudo systemctl daemon-reload
sudo systemctl reset-failed routing-oncall.service
sudo systemctl enable --now routing-oncall.service
```

旧状态中的 `last_heartbeat_date` 自动移除。若曾使用另一套带 `routing-oncall.timer` 的定时启动方案，先停用那个 timer，避免它与本方案的常驻 Service 同时使用。旧备份中的 Webhook 和密钥也应保持原有访问权限。

## 消息示例

```text
[RoutingAPI] 告警状态变化 (1)  观测于 2026-10-29 16:30:00 +0800
🔴 RoutingAPI-SEV2-5XX: OK → ALARM
    Threshold Crossed: ...
观测时 ALARM (1)
```

恢复消息显示 `🟢 ... ALARM → OK`；数据不足显示 `⚪ ... ALARM → INSUFFICIENT_DATA`。触发原因保留 AWS 原文，最多 160 字符，便于对照排查。

```text
[RoutingAPI] ⏰ 持续异常 · 本条 ALARM 1 个  观测于 2026-10-29 17:00:00 +0800
🔴 RoutingAPI-SEV2-5XX  已持续 30m
观测时 ALARM (1)
```

持续时长优先根据 CloudWatch 的 `StateTransitionedTimestamp`（状态实际变化时间）计算；该字段缺失时回退到 `StateUpdatedTimestamp`，此时可能低估持续时长。时间按配置的 `Asia/Shanghai` 显示。

消息按完整 JSON 请求体的 UTF-8 字节数限制在 **18,000 字节以内**，包含标题、中文、emoji 和 JSON 转义。**这是实现预算，不是已验证的钉钉服务端接受上限。** 已核对对应自定义机器人接口及消息类型页面，但未获得可核实的消息体字节上限；本轮按用户要求暂不真实联调，仍需验证短消息、接近预算的中文/emoji 内容以及多段消息的 API 响应和群内完整到达情况。离线测试通过不代表该项验收完成。

超长消息按完整告警项分批，每条消息都有关键词前缀、独立标题、数量和观测时间，不截断告警项，也不把同一项拆成多条。原因仍最多保留 160 字符。单项正文自身超出预算时明确报错，不提前确认，也不发送不完整告警。发送前再次检查实际请求体长度。实际群内可读性和完整性仍需真实联调确认。

同一进程内所有 HTTP 发送尝试（含分段及重试）至少间隔 3.1 秒；钉钉官方规定每个机器人每分钟最多发送 20 条。该间隔不能协调其他进程或共用机器人的发送方，也不跨进程重启保留。

## 失败、恢复与排查

- 网络错误、HTTP 429/5xx、无效 JSON 响应可在剩余预算内重试；每轮最多 5 次 HTTP 尝试，共用 20 秒发送预算。连续失败退避为 1、2、4、8…秒（上限 30 秒），同时遵守 3.1 秒最小请求间隔；没有剩余预算时留到下一轮重新观测。签名在等待结束后、每次请求前重新生成。
- HTTP 其他错误或钉钉非零 `errcode` 直接结束本轮发送，下一轮根据最新观测再试；成功必须同时满足 HTTP 成功和 `errcode == 0`。日志记录错误码，不输出 Webhook、token 或签名。
- 最新观测先保存，发送成功后按消息中的告警逐项确认；确认写盘失败立即停止后续发送。连续 5 个投递错误/写入错误轮次退出 1，由 systemd 重启。正常预算耗尽不算错误，单独的 SDK 查询失败也不算发送错误。
- AWS 临时错误跳过本轮，凭证缺失、常见认证或权限错误退出 2，不自动重启。修复配置后手动 `sudo systemctl restart routing-oncall.service`；遇启动频率限制时先 `reset-failed`。
- 状态文件采用临时文件写入、文件 fsync、原子替换和目录 fsync；读取时校验 JSON、UTF-8、结构、状态枚举、时区、逐项通知状态和提醒进度。损坏文件备份为 `state.json.corrupt`，随后重新建立基线；损坏文件中的通知基线不会自动恢复，需人工检查备份。未知未来版本及有积压的 schema 2 文件保留原文件并退出 2。权限、磁盘读取或备份错误不会被当成损坏文件静默重建。
- 钉钉已收到消息，但成功响应丢失，或确认落盘前崩溃时，未确认告警可能重复发送。此时无法保证恢复通知完整；逐项确认以本地成功响应和落盘为准。没有严格去重、事件补录、独立进程故障通知或整机故障通知。正常轮询间隙的短暂异常可能漏掉；未成功通知便恢复的异常按设计取消。

常用排查命令：

```bash
sudo systemctl status routing-oncall.service --no-pager
sudo journalctl -u routing-oncall.service -n 100 --no-pager
sudo journalctl -fu routing-oncall.service
```

### 日志级别

在 `/etc/routing-oncall/agent.env` 设置 `LOG_LEVEL=INFO`，修改后执行
`sudo systemctl restart routing-oncall.service` 生效。默认 `INFO`；支持 `DEBUG`、
`INFO`、`WARNING`、`ERROR`、`CRITICAL`，不区分大小写并忽略两端空格。
非法值会在启动时明确报错并退出 2。

| 级别 | 内容 |
|---|---|
| INFO | 启动参数；每轮保存的监控数、异常数、待通知变化数、待提醒数及过时变化取消数；消息发送并确认成功；发送预算耗尽后的剩余项数 |
| DEBUG | 额外记录采集开始、待通知告警名称及新旧状态、每次发送的项数和 JSON 字节数、成功确认的告警名称 |
| WARNING | AWS 临时查询失败及错误码、可重试投递错误、废弃配置 |
| ERROR / CRITICAL | 通知或落盘失败、损坏状态文件、容量不足、无法安全升级、认证错误及退出原因 |

日志写入标准输出，带时间、级别和 logger 名称；本服务下可用上面的 `journalctl`
命令查看。正常运行时每轮有一条 INFO 采集摘要，便于区分“没有告警”和“没有运行”。
`superseded` 只统计被最新观测取消或替代的待通知变化，不包含已成功确认的变化。
DEBUG 只开启 oncall 自身日志；boto3/botocore 仍为 WARNING，避免输出详细请求与认证信息。
不记录消息正文、告警原因、Webhook、Secret 或签名。

`fatal AWS error`：核对 region、profile、权限和凭证有效期。`DingTalk rejected message`：核对错误码、关键词、加签和 Webhook。`state capacity exceeded`：本轮快照未接纳且未发送消息，检查监控范围和 `MAX_STATE_BYTES`。`state upgrade required`：按升级说明处理原状态文件，不要直接删除。`Permission denied`：核对 `/var/lib/routing-oncall` 所有权与配置文件权限。日志不输出 Webhook URL、token 或签名。

## 本地验证

在解压后的 `routing-oncall` 目录运行：

```bash
python3 -m venv venv
venv/bin/pip install -r requirements.txt
venv/bin/python -m unittest discover -s tests -v
```

测试使用 SDK Stubber、临时状态文件、模拟时钟和 mock，不访问真实 AWS、不发送钉钉消息；HTTP 整体超时另用真实 POSIX 定时信号测试阻塞读取。覆盖状态转换、Glob、旧状态迁移、状态损坏、分页、状态变化时间及回退、UTF-8/JSON 字节分批、签名、退出码，以及：

- 已对齐的 10 个场景：未送达异常取消、持续异常重试、持续恢复重试、未送达恢复取消、抖动合并、多项独立处理、重启、提醒取消、数据不足中间状态，以及分批部分成功后的恢复。
- 首次 HTTP 前观测已落盘，逐项确认、重启续发、确认写盘失败后的有限重复。
- 持续提醒去重、分批进度、全局提醒计时、每轮次数/时间预算和重试限速。
- 连续 120 轮抖动与投递失败不积累历史；状态容量超限不覆盖文件、不发送旧快照。
- 原状态兼容、schema 2 非空队列升级保护、排除规则和删除后重现。
- AWS 临时查询失败时跳过旧状态投递。

本轮 72 项离线测试通过（开启 `ResourceWarning` 错误检查）。验证使用 Python 3.12、boto3 1.43.121；主程序另做 Python 3.8 语法兼容检查，不等同于 Python 3.8 运行时验证。未修改 systemd unit，未部署到目标 Ubuntu，未完成钉钉服务端长度和真实投递验收。

参考：[Boto3 凭证链](https://docs.aws.amazon.com/boto3/latest/guide/credentials.html)、[DescribeAlarms](https://docs.aws.amazon.com/AmazonCloudWatch/latest/APIReference/API_DescribeAlarms.html)、[MetricAlarm 时间字段](https://docs.aws.amazon.com/AmazonCloudWatch/latest/APIReference/API_MetricAlarm.html)、[钉钉自定义机器人发送群消息](https://open.dingtalk.com/document/orgapp/custom-robots-send-group-messages)、[systemd.exec](https://www.freedesktop.org/software/systemd/man/256/systemd.exec.html)。
