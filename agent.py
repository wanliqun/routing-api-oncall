#!/usr/bin/env python3
"""
routing-oncall agent

每 POLL_SECONDS 拉一次 CloudWatch Alarm 当前状态（只读），与本地 state.json 做 diff：
  - 进入/离开 ALARM            → 钉钉（同一轮内多个变化合并，超长按完整项分批）
  - 仍有 ALARM 且距上次 ≥ 30min → 钉钉提醒
Alarm 删除也通知；OK <-> INSUFFICIENT_DATA 静默。
ALARM_EXCLUDE_PATTERNS 中的 Glob 模式同时过滤采集和通知基线。

失败语义：
  - 保存最新观测与逐项通知基线；只重试当前仍有效的变化
  - 不积累历史通知；状态文件大小、每轮发送次数与时间均有上限
  - AWS 临时错误：跳过本轮，避免发送无法重新核实的旧状态
  - AWS 凭证/权限错误：退出(2)，systemd 不重启
"""
import base64
import copy
import hashlib
import hmac
import json
import logging
import os
import signal
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from contextlib import contextmanager
from datetime import datetime, timezone
from fnmatch import fnmatchcase
from typing import Dict, List, Optional

import boto3
from botocore.config import Config
from botocore.exceptions import (
    BotoCoreError, ClientError, NoCredentialsError, NoRegionError, PartialCredentialsError,
)

# ---------------------------------------------------------------- 配置


def env(name: str, default: Optional[str] = None, required: bool = False) -> str:
    v = os.environ.get(name, default)
    if required and not v:
        sys.stderr.write(f"missing required env {name}\n")
        sys.exit(2)
    return v  # type: ignore[return-value]


def positive_int(name: str, default: str) -> int:
    try:
        value = int(env(name, default))
        if value > 0:
            return value
    except ValueError:
        pass
    sys.stderr.write(f"{name} must be a positive integer\n")
    sys.exit(2)


WEBHOOK = env("DINGTALK_WEBHOOK", required=True)
SECRET = env("DINGTALK_SECRET", "")
PREFIX = env("ALARM_PREFIX", "RoutingAPI")
STATE_FILE = env("STATE_FILE", "/var/lib/routing-oncall/state.json")
POLL_SECONDS = positive_int("POLL_SECONDS", "60")
REMINDER_SECONDS = positive_int("REMINDER_MINUTES", "30") * 60
MAX_SEND_FAILURES = positive_int("MAX_SEND_FAILURES", "5")
MAX_STATE_BYTES = positive_int("MAX_STATE_BYTES", "2097152")
MAX_SEND_ATTEMPTS_PER_ROUND = positive_int("MAX_SEND_ATTEMPTS_PER_ROUND", "5")
SEND_BUDGET_SECONDS = positive_int("SEND_BUDGET_SECONDS", "20")
MAX_MESSAGE_BYTES = 18000  # 完整 JSON 请求体的保守预算，包含 UTF-8 和 JSON 转义。
MIN_SEND_INTERVAL = 3.1  # 单进程请求间隔，避免分批触发每分钟 20 条的频控。
_last_send_at: Optional[float] = None
LOG_LEVEL = env("LOG_LEVEL", "INFO").strip().upper()
if LOG_LEVEL not in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"):
    sys.stderr.write("LOG_LEVEL must be DEBUG, INFO, WARNING, ERROR or CRITICAL\n")
    sys.exit(2)
EXCLUDE_PATTERNS = [p.strip() for p in env("ALARM_EXCLUDE_PATTERNS", "").split(",") if p.strip()]

TAG = f"[{PREFIX}]"  # 钉钉机器人"自定义关键词"用这个
ICON = {"ALARM": "🔴", "OK": "🟢", "INSUFFICIENT_DATA": "⚪"}
FATAL_AWS_CODES = {
    "AccessDenied", "AccessDeniedException", "UnauthorizedOperation",
    "InvalidClientTokenId", "UnrecognizedClientException",
    "ExpiredToken", "ExpiredTokenException", "SignatureDoesNotMatch",
}

logging.basicConfig(level=logging.WARNING, stream=sys.stdout,
                    format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("routing-oncall")
log.setLevel(LOG_LEVEL)  # DEBUG 只作用于本程序，不开启 SDK 请求/认证调试日志。
for obsolete in ("MAX_PENDING_NOTIFICATIONS", "MAX_PENDING_BYTES"):
    if obsolete in os.environ:
        log.warning("%s is obsolete; use MAX_STATE_BYTES for the state size limit", obsolete)

# ---------------------------------------------------------------- 工具


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat(timespec="seconds")


def parse_iso(s: str) -> datetime:
    return datetime.fromisoformat(s)


def local_str(dt: datetime) -> str:
    return dt.astimezone().strftime("%Y-%m-%d %H:%M:%S %z")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def should_monitor(name: str) -> bool:
    return name.startswith(PREFIX) and not any(fnmatchcase(name, p) for p in EXCLUDE_PATTERNS)


def filter_alarms(alarms: Dict[str, dict]) -> Dict[str, dict]:
    return {name: alarm for name, alarm in alarms.items() if should_monitor(name)}


def fmt_dur(since_iso: str, now: datetime) -> str:
    s = max(0, int((now - parse_iso(since_iso)).total_seconds()))
    if s < 3600:
        return f"{s // 60}m"
    if s < 86400:
        return f"{s // 3600}h{(s % 3600) // 60:02d}m"
    return f"{s // 86400}d{(s % 86400) // 3600}h"


def active_names(alarms: Dict[str, dict]) -> List[str]:
    return sorted(n for n, a in alarms.items() if a["state"] == "ALARM")

# ---------------------------------------------------------------- state.json


def validate_timestamp(value: object) -> None:
    if not isinstance(value, str) or parse_iso(value).utcoffset() is None:
        raise ValueError("state timestamp must include a timezone")


def validate_state(st: object) -> None:
    if not isinstance(st, dict):
        raise ValueError("state must be an object")
    version = st.get("schema_version", 2)
    if type(version) is not int or version not in (2, 3):
        raise RuntimeError("unsupported state schema version")
    if not isinstance(st.get("alarms", {}), dict):
        raise ValueError("alarms must be an object")
    for name, alarm in st.get("alarms", {}).items():
        if not isinstance(name, str) or not isinstance(alarm, dict):
            raise ValueError("invalid alarm record")
        if not isinstance(alarm.get("state"), str) or alarm["state"] not in ICON:
            raise ValueError("invalid alarm state")
        if not isinstance(alarm.get("reason", ""), str):
            raise ValueError("invalid alarm reason")
        validate_timestamp(alarm.get("since"))
    if st.get("last_reminder_at") is not None:
        validate_timestamp(st["last_reminder_at"])
    if version == 2:
        if not isinstance(st.get("pending", []), list):
            raise ValueError("pending must be a list")
        if st.get("pending"):
            # 旧正文没有逐项确认信息，不能可靠推断哪些告警已经送达。
            raise RuntimeError("schema 2 has pending notifications; drain with the previous "
                               "agent before upgrading; state file preserved")
        return
    notified = st.get("notified")
    if not isinstance(notified, dict) or any(
            not isinstance(n, str) or not isinstance(v, str) or v not in ICON
            for n, v in notified.items()):
        raise ValueError("invalid notified states")
    remaining = st.get("reminder_remaining")
    if remaining is not None and (not isinstance(remaining, list) or
            not all(isinstance(n, str) for n in remaining) or
            len(set(remaining)) != len(remaining)):
        raise ValueError("invalid reminder progress")


def normalize_state(st: dict) -> dict:
    st = copy.deepcopy(st)
    if st.get("schema_version", 2) == 2:
        validate_state(st)
        st["notified"] = {n: a["state"] for n, a in st.get("alarms", {}).items()}
        st["reminder_remaining"] = None
        st["schema_version"] = 3
    st["alarms"] = filter_alarms(st.get("alarms", {}))
    st["notified"] = {n: v for n, v in st["notified"].items() if should_monitor(n)}
    if st.get("reminder_remaining") is not None:
        st["reminder_remaining"] = [n for n in st["reminder_remaining"] if should_monitor(n)]
    st.setdefault("reminder_remaining", None)
    if st["reminder_remaining"] == []:
        st["reminder_remaining"] = None
    st.setdefault("last_reminder_at", None)
    for old in ("pending", "collection_paused", "last_heartbeat_date"):
        st.pop(old, None)
    if not active_names(st["alarms"]) and "ALARM" not in st["notified"].values():
        st["last_reminder_at"] = None
    return st


def load_state() -> Optional[dict]:
    try:
        with open(STATE_FILE, encoding="utf-8") as f:
            st = json.load(f)
        validate_state(st)
        return normalize_state(st)
    except FileNotFoundError:
        return None
    except ValueError as e:  # 包含 JSONDecodeError 和 UnicodeDecodeError。
        # 文件损坏：挪走后重新建立基线，当前实际 ALARM 仍会通知。
        bad = STATE_FILE + ".corrupt"
        log.error("state file invalid (%s), moving to %s", type(e).__name__, bad)
        try:
            os.replace(STATE_FILE, bad)
        except OSError:
            log.error("could not back up invalid state file")
            raise
        return None


def save_state(st: dict) -> None:
    if len(json.dumps(st, ensure_ascii=False, indent=1).encode("utf-8")) > MAX_STATE_BYTES:
        raise RuntimeError("state capacity exceeded: increase MAX_STATE_BYTES or reduce monitored alarms")
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(st, f, ensure_ascii=False, indent=1)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, STATE_FILE)  # 原子替换
    directory = os.open(os.path.dirname(STATE_FILE) or ".", os.O_RDONLY)
    try:
        os.fsync(directory)  # 持久化 rename，保存观测和逐项通知确认。
    finally:
        os.close(directory)

# ---------------------------------------------------------------- AWS


def fetch_alarms(cw) -> Dict[str, dict]:
    out: Dict[str, dict] = {}
    paginator = cw.get_paginator("describe_alarms")
    pages = paginator.paginate(AlarmNamePrefix=PREFIX,
                               AlarmTypes=["MetricAlarm", "CompositeAlarm"])
    for page in pages:
        for a in page.get("MetricAlarms", []) + page.get("CompositeAlarms", []):
            if not should_monitor(a["AlarmName"]):
                continue
            out[a["AlarmName"]] = {
                "state": a["StateValue"],
                "reason": (a.get("StateReason") or "").strip()[:160],
                "since": iso(a.get("StateTransitionedTimestamp") or a["StateUpdatedTimestamp"]),
            }
    return out

# ---------------------------------------------------------------- diff


def diff(prev: Dict[str, dict], now: Dict[str, dict]) -> List[dict]:
    events: List[dict] = []
    for name in sorted(set(prev) | set(now)):
        p, n = prev.get(name), now.get(name)
        if p is None:
            # 新出现的 Alarm：只有当前就是 ALARM 才通知
            if n["state"] == "ALARM":
                events.append({"kind": "change", "name": name,
                               "old": "(新建)", "new": "ALARM", "reason": n.get("reason", "")})
        elif n is None:
            events.append({"kind": "deleted", "name": name, "old": p["state"]})
        elif p["state"] != n["state"] and "ALARM" in (p["state"], n["state"]):
            events.append({"kind": "change", "name": name,
                           "old": p["state"], "new": n["state"], "reason": n.get("reason", "")})
    return events

# ---------------------------------------------------------------- 消息


def footer(alarms: Dict[str, dict]) -> str:
    act = active_names(alarms)
    return f"观测时 ALARM ({len(act)})"


def change_msg(events: List[dict], alarms: Dict[str, dict], now: datetime) -> str:
    lines = [f"{TAG} 告警状态变化 ({len(events)})  观测于 {local_str(now)}"]
    for e in events:
        if e["kind"] == "deleted":
            lines.append(f"🗑 {e['name']}: Alarm 已被删除 (原状态 {e['old']})")
        else:
            lines.append(f"{ICON.get(e['new'], '❓')} {e['name']}: {e['old']} → {e['new']}")
            if e["new"] == "ALARM" and e["reason"]:
                lines.append(f"    {e['reason']}")
    lines.append(footer(alarms))
    return "\n".join(lines)


def reminder_msg(alarms: Dict[str, dict], now: datetime) -> str:
    act = active_names(alarms)
    lines = [f"{TAG} ⏰ 持续异常 · 本条 ALARM {len(act)} 个  观测于 {local_str(now)}"]
    for n in act:
        lines.append(f"🔴 {n}  已持续 {fmt_dur(alarms[n]['since'], now)}")
    return "\n".join(lines)


# ---------------------------------------------------------------- 钉钉


def message_body(text: str) -> bytes:
    return json.dumps({"msgtype": "text", "text": {"content": text}},
                      ensure_ascii=False).encode("utf-8")


def batch_notifications(events: List[dict], alarms: Dict[str, dict],
                        now: datetime, kind: str) -> List[dict]:
    """按完整告警项装箱，每条正文与它能够确认的告警一一对应。"""
    def render(batch):
        if kind == "reminder":
            return reminder_msg({e["name"]: alarms[e["name"]] for e in batch}, now) + "\n" + footer(alarms)
        return change_msg(batch, alarms, now)

    batches = []
    batch: List[dict] = []
    for original in events:
        event = dict(original)
        # 正常 AWS Alarm 名称和 160 字符原因远小于预算。配置前缀过长等
        # 极端输入仍检查完整单项，绝不把同一项拆到两条消息再提前确认。
        if len(message_body(render([event]))) > MAX_MESSAGE_BYTES:
            raise ValueError("single alarm notification exceeds byte budget")
        if batch and len(message_body(render(batch + [event]))) > MAX_MESSAGE_BYTES:
            batches.append({"kind": kind, "events": batch, "text": render(batch)})
            batch = []
        batch.append(event)
    if batch:
        batches.append({"kind": kind, "events": batch, "text": render(batch)})
    return batches


class SendBudgetExhausted(Exception):
    pass


@contextmanager
def request_timeout(seconds: float):
    """Ubuntu/macOS 主线程：限制整个 HTTP 请求，避免慢响应逐字节延长超时。"""
    def expired(*_):
        raise TimeoutError("DingTalk request timed out")

    previous = signal.signal(signal.SIGALRM, expired)
    signal.setitimer(signal.ITIMER_REAL, seconds)
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


def send_once(text: str, deadline: Optional[float] = None) -> None:
    global _last_send_at
    body = message_body(text)
    if len(body) > MAX_MESSAGE_BYTES:
        raise ValueError("DingTalk payload exceeds byte budget")
    delay = 0.0 if _last_send_at is None else max(
        0.0, MIN_SEND_INTERVAL - (time.monotonic() - _last_send_at))
    if deadline is not None and time.monotonic() + delay + 0.1 >= deadline:
        raise SendBudgetExhausted()
    if delay:
        time.sleep(delay)
    timeout = 10.0 if deadline is None else min(10.0, deadline - time.monotonic())
    if timeout <= 0:
        raise SendBudgetExhausted()
    url = WEBHOOK
    if SECRET:
        ts = str(int(time.time() * 1000))
        digest = hmac.new(SECRET.encode(), f"{ts}\n{SECRET}".encode(), hashlib.sha256).digest()
        sign = urllib.parse.quote_plus(base64.b64encode(digest).decode())
        url += f"&timestamp={ts}&sign={sign}"
    req = urllib.request.Request(url, data=body,
                                 headers={"Content-Type": "application/json; charset=utf-8"})
    _last_send_at = time.monotonic()
    with request_timeout(timeout):
        with urllib.request.urlopen(req, timeout=timeout) as r:
            resp = json.loads(r.read(65537).decode("utf-8"))
    if not isinstance(resp, dict):
        raise ValueError("invalid DingTalk response object")
    if resp.get("errcode") != 0:
        # 业务拒绝（关键词、签名、参数等）直接交给下一轮，避免密集重试。
        raise RuntimeError(f"DingTalk rejected message: errcode={resp.get('errcode')}")


# ---------------------------------------------------------------- 当前状态投递


def notification_events(state: dict) -> List[dict]:
    return diff({n: {"state": value} for n, value in state["notified"].items()}, state["alarms"])


def collect(cw, state: dict, now: datetime) -> dict:
    log.debug("collection started: prefix=%s", PREFIX)
    alarms = fetch_alarms(cw)
    candidate = copy.deepcopy(state)
    candidate["alarms"] = alarms
    # 静默变化直接推进通知基线；进入/离开 ALARM 和删除只在发送成功后推进。
    for name, alarm in alarms.items():
        old = candidate["notified"].get(name)
        new = alarm["state"]
        if new != "ALARM" and old != "ALARM":
            candidate["notified"][name] = new
    if not active_names(alarms) and "ALARM" not in candidate["notified"].values():
        candidate["last_reminder_at"] = None
    events = notification_events(candidate)
    if events:
        candidate["reminder_remaining"] = None
    elif candidate["reminder_remaining"] is not None:
        candidate["reminder_remaining"] = [n for n in candidate["reminder_remaining"]
                                           if n in alarms and alarms[n]["state"] == "ALARM"]
    else:
        last = candidate["last_reminder_at"]
        due = last is None or (now - parse_iso(last)).total_seconds() >= REMINDER_SECONDS
        if active_names(alarms) and due:
            candidate["reminder_remaining"] = active_names(alarms)
    # 预留本快照全部确认及提醒进度的空间，避免正常确认使文件越过上限。
    reserved = copy.deepcopy(candidate)
    reserved["notified"] = {n: "INSUFFICIENT_DATA"
                            for n in set(candidate["notified"]) | set(alarms)}
    reserved["reminder_remaining"] = active_names(alarms)
    reserved["last_reminder_at"] = iso(now)
    if len(json.dumps(reserved, ensure_ascii=False, indent=1).encode("utf-8")) > MAX_STATE_BYTES:
        raise RuntimeError("state capacity exceeded: snapshot not committed; "
                           "increase MAX_STATE_BYTES or reduce monitored alarms")
    save_state(candidate)  # 先保存最新观测，再允许 HTTP；不保存历史正文。
    def event_key(event):
        return (event["name"], event["kind"], event["old"], event.get("new"))

    superseded = len({event_key(e) for e in notification_events(state)} -
                     {event_key(e) for e in events})
    log.debug("collection saved: monitored=%d alarm=%d changes=%d reminders=%d superseded=%d",
              len(alarms), len(active_names(alarms)), len(events),
              len(candidate["reminder_remaining"] or []), superseded)
    if superseded:
        log.info("pending changes superseded by latest observation: count=%d", superseded)
    for event in events:
        log.debug("pending change: alarm=%s old=%s new=%s", event["name"],
                  event["old"], event.get("new", "deleted"))
    return candidate


def acknowledge(state: dict, part: dict, delivered_at: datetime) -> dict:
    candidate = copy.deepcopy(state)
    for event in part["events"]:
        name = event["name"]
        if part["kind"] == "reminder":
            candidate["reminder_remaining"].remove(name)
        elif event["kind"] == "deleted":
            candidate["notified"].pop(name, None)
        else:
            candidate["notified"][name] = event["new"]
    if not notification_events(candidate) and not candidate["reminder_remaining"]:
        candidate["reminder_remaining"] = None
        candidate["last_reminder_at"] = iso(delivered_at) if active_names(candidate["alarms"]) else None
    save_state(candidate)  # 成功消息逐条确认；写盘失败立即停止发送。
    return candidate


def deliver_notifications(state: dict, deadline: float, now: datetime) -> dict:
    events = notification_events(state)
    kind = "change" if events else "reminder"
    if not events:
        events = [{"name": n} for n in (state["reminder_remaining"] or [])]
    parts = iter(batch_notifications(events, state["alarms"], now, kind))
    part = next(parts, None)
    failure = None
    consecutive_failures = 0
    for attempt in range(MAX_SEND_ATTEMPTS_PER_ROUND):
        if part is None or time.monotonic() >= deadline:
            break
        log.debug("delivery attempt %d/%d: kind=%s alarms=%d bytes=%d",
                  attempt + 1, MAX_SEND_ATTEMPTS_PER_ROUND, part["kind"],
                  len(part["events"]), len(message_body(part["text"])))
        try:
            send_once(part["text"], deadline=deadline)
        except SendBudgetExhausted:
            break
        except urllib.error.HTTPError as e:
            if e.code != 429 and not 500 <= e.code < 600:
                raise RuntimeError(f"DingTalk HTTP {e.code}") from None
            failure = f"HTTP {e.code}"
        except (urllib.error.URLError, OSError, ValueError) as e:
            failure = type(e).__name__
        else:
            state = acknowledge(state, part, utc_now())
            log.info("sent %s: %d alarms observed_at=%s", part["kind"], len(part["events"]), iso(now))
            log.debug("delivery acknowledged: alarms=%s", [e["name"] for e in part["events"]])
            part = next(parts, None)
            failure = None
            consecutive_failures = 0
            continue
        # 只记录错误类型，不输出可能含 Webhook token 的异常内容。
        consecutive_failures += 1
        log.warning("DingTalk round attempt %d/%d failed: %s", attempt + 1,
                    MAX_SEND_ATTEMPTS_PER_ROUND, failure)
        delay = min(2 ** (consecutive_failures - 1), 30)
        if attempt + 1 == MAX_SEND_ATTEMPTS_PER_ROUND or time.monotonic() + delay + 0.1 >= deadline:
            break
        time.sleep(delay)
    if failure is not None:
        raise RuntimeError(f"DingTalk delivery deferred: {failure}") from None
    if part is not None:
        log.info("delivery budget reached: remaining_changes=%d remaining_reminders=%d; "
                 "recheck next round", len(notification_events(state)),
                 len(state["reminder_remaining"] or []))
    return state

# ---------------------------------------------------------------- 主循环


def one_round(cw, state: Optional[dict], now: datetime) -> dict:
    """每轮先重新核实当前状态，再在预算内投递仍有效的逐项变化。"""
    round_deadline = time.monotonic() + POLL_SECONDS
    state = normalize_state(state) if state is not None else {
        "schema_version": 3, "alarms": {}, "notified": {},
        "last_reminder_at": None, "reminder_remaining": None}
    try:
        state = collect(cw, state, now)
    except ClientError as e:
        if e.response.get("Error", {}).get("Code", "") in FATAL_AWS_CODES:
            raise
        log.warning("AWS collection failed; delivery skipped: code=%s",
                    e.response.get("Error", {}).get("Code", "unknown"))
        return state
    except (NoCredentialsError, PartialCredentialsError, NoRegionError):
        raise
    except BotoCoreError as e:
        log.warning("AWS collection failed; delivery skipped: %s", type(e).__name__)
        return state
    deadline = min(round_deadline, time.monotonic() + SEND_BUDGET_SECONDS)
    return deliver_notifications(state, deadline, now)


def main() -> None:
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    signal.signal(signal.SIGINT, lambda *_: sys.exit(0))

    try:
        # 复用 Boto3 默认凭证链，不复制或传入 Access Key。
        cw = boto3.client("cloudwatch", region_name=env("AWS_REGION", None) or None,
                          config=Config(connect_timeout=10, read_timeout=10,
                                        retries={"mode": "standard", "total_max_attempts": 3}))
    except (BotoCoreError, ValueError) as e:
        log.critical("AWS client initialization failed: %s", type(e).__name__)
        sys.exit(2)
    try:
        state = load_state()
    except RuntimeError as e:
        log.critical("state upgrade required: %s", e)
        sys.exit(2)
    log.info("starting: prefix=%s state=%s (%s) log_level=%s poll_seconds=%d "
             "reminder_seconds=%d send_attempts=%d send_budget_seconds=%d max_state_bytes=%d",
             PREFIX, STATE_FILE, "found" if state else "bootstrap", LOG_LEVEL,
             POLL_SECONDS, REMINDER_SECONDS, MAX_SEND_ATTEMPTS_PER_ROUND,
             SEND_BUDGET_SECONDS, MAX_STATE_BYTES)
    send_failures = 0

    while True:
        t0 = time.monotonic()
        now = utc_now()
        try:
            state = one_round(cw, state, now)
            send_failures = 0
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if code in FATAL_AWS_CODES:
                log.critical("fatal AWS error %s: %s", code, e)
                sys.exit(2)
            log.warning("AWS error (will retry next round): %s", e)
        except (NoCredentialsError, PartialCredentialsError, NoRegionError) as e:
            log.critical("AWS configuration error: %s", type(e).__name__)
            sys.exit(2)
        except BotoCoreError as e:
            log.warning("AWS connection error (will retry next round): %s", e)
        except (urllib.error.URLError, RuntimeError, OSError, ValueError) as e:
            send_failures += 1
            log.error("notify failed (%d/%d): %s", send_failures, MAX_SEND_FAILURES, e)
            state = load_state()  # 恢复最新观测及逐项通知确认。
            if send_failures >= MAX_SEND_FAILURES:
                log.critical("too many consecutive notify failures, exiting")
                sys.exit(1)

        elapsed = time.monotonic() - t0
        time.sleep(max(1.0, POLL_SECONDS - elapsed))


if __name__ == "__main__":
    main()
