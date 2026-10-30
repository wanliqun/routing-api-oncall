"""离线行为测试：不访问 AWS，不发送钉钉消息。"""
import base64
import copy
import signal
import time
import hashlib
import hmac
import json
import os
import subprocess
import sys
import tempfile
import unittest
import urllib.error
import urllib.parse
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

os.environ.setdefault("DINGTALK_WEBHOOK", "https://oapi.dingtalk.com/robot/send?access_token=TEST_ONLY")

import agent
import boto3
from botocore.stub import Stubber
from botocore.exceptions import NoCredentialsError, EndpointConnectionError, ClientError


NOW = datetime(2026, 10, 29, 8, 0, tzinfo=timezone.utc)
ERROR = "RoutingAPI-SEV2-5XX"
LATENCY = "RoutingAPI-SEV2-Latency"


def alarm(state="ALARM", since=NOW):
    return {"state": state, "since": agent.iso(since), "reason": "test reason"}


def queued(parts=None, kind="change"):
    return {"kind": kind, "observed_at": agent.iso(NOW),
            "parts": parts or ["[RoutingAPI] test"], "next_part": 0}


class FakeClock:
    def __init__(self):
        self.seconds = 0.0

    def monotonic(self):
        return self.seconds

    def sleep(self, seconds):
        self.seconds += seconds


def make_state(alarms, notified=None, last=None):
    return {"schema_version": 3, "alarms": copy.deepcopy(alarms),
            "notified": ({n: a["state"] for n, a in alarms.items()} if notified is None else dict(notified)),
            "last_reminder_at": agent.iso(last) if last else None, "reminder_remaining": None}


class RoundTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = str(Path(self.tmp.name) / "state.json")
        for name, value in (("STATE_FILE", self.path), ("PREFIX", "RoutingAPI"),
                            ("EXCLUDE_PATTERNS", ["RoutingAPI-*-Latency"])):
            p = patch.object(agent, name, value)
            p.start()
            self.addCleanup(p.stop)


    def run_round(self, alarms, state, now=NOW):
        with patch.object(agent, "fetch_alarms", return_value=agent.filter_alarms(alarms)), \
                patch.object(agent, "send_once") as send, patch.object(agent, "utc_now", return_value=now):
            result = agent.one_round(None, state, now)
        return result, send


    def test_glob_case_and_multiple_patterns(self):
        with patch.object(agent, "EXCLUDE_PATTERNS", ["RoutingAPI-*-Latency", "RoutingAPI-?-4XX*"]):
            self.assertFalse(agent.should_monitor(LATENCY))
            self.assertFalse(agent.should_monitor("RoutingAPI-X-4XXAlarm"))
            self.assertTrue(agent.should_monitor(ERROR))
            self.assertTrue(agent.should_monitor("RoutingAPI-SEV2-latency"))
            self.assertFalse(agent.should_monitor("OtherService-5XX"))
        with patch.object(agent, "EXCLUDE_PATTERNS", []):
            self.assertTrue(agent.should_monitor(LATENCY))


    def test_initial_healthy_and_unknown_are_silent(self):
        state, send = self.run_round({ERROR: alarm("OK"), LATENCY: alarm()}, None)
        send.assert_not_called()
        self.assertEqual(state, agent.load_state())
        self.assertNotIn(LATENCY, state["alarms"])
        _, send = self.run_round({ERROR: alarm("INSUFFICIENT_DATA")}, state, NOW + timedelta(days=1))
        send.assert_not_called()  # 跨天也无心跳


    def test_initial_actual_alarms_are_merged(self):
        state, send = self.run_round({ERROR: alarm(), ERROR + "-2": alarm(), LATENCY: alarm()}, None)
        send.assert_called_once()
        text = send.call_args.args[0]
        self.assertIn("观测时 ALARM (2)", text)
        self.assertNotIn(LATENCY, text)
        self.assertNotIn("启动", text)
        self.assertEqual(state["last_reminder_at"], agent.iso(NOW))


    def test_transition_matrix_is_preserved(self):
        for old in ("OK", "ALARM", "INSUFFICIENT_DATA"):
            for new in ("OK", "ALARM", "INSUFFICIENT_DATA"):
                with self.subTest(old=old, new=new):
                    events = agent.diff({ERROR: alarm(old)}, {ERROR: alarm(new)})
                    self.assertEqual(bool(events), old != new and "ALARM" in (old, new))


    def test_excluded_old_state_is_removed_without_deleted_notification(self):
        old = {"alarms": {ERROR: alarm("OK"), LATENCY: alarm()},
               "last_reminder_at": agent.iso(NOW - timedelta(hours=2)),
               "last_heartbeat_date": "2026-10-28"}
        agent.save_state(old)
        loaded = agent.load_state()
        self.assertNotIn(LATENCY, loaded["alarms"])
        self.assertNotIn("last_heartbeat_date", loaded)
        self.assertIsNone(loaded["last_reminder_at"])
        state, send = self.run_round({ERROR: alarm("OK"), LATENCY: alarm()}, old)
        send.assert_not_called()
        self.assertEqual(set(state["alarms"]), {ERROR})


    def test_actual_deleted_alarm_still_notifies(self):
        old = {"alarms": {ERROR: alarm("OK")}, "last_reminder_at": None}
        _, send = self.run_round({}, old)
        send.assert_called_once()
        self.assertIn("Alarm 已被删除", send.call_args.args[0])


    def test_reenabled_active_alarm_notifies_as_new(self):
        old = {"alarms": {}, "last_reminder_at": None}
        with patch.object(agent, "EXCLUDE_PATTERNS", []):
            _, send = self.run_round({LATENCY: alarm()}, old)
        send.assert_called_once()


    def test_reminder_boundary_and_timer_reset(self):
        state, _ = self.run_round({ERROR: alarm()}, None)
        state, send = self.run_round({ERROR: alarm()}, state, NOW + timedelta(minutes=29, seconds=59))
        send.assert_not_called()
        state, send = self.run_round({ERROR: alarm()}, state, NOW + timedelta(minutes=30))
        send.assert_called_once()
        self.assertIn("持续异常", send.call_args.args[0])
        state, send = self.run_round({ERROR: alarm(), ERROR + "-2": alarm()}, state,
                                     NOW + timedelta(minutes=40))
        send.assert_called_once()  # 状态变化摘要重置全局提醒计时
        _, send = self.run_round(state["alarms"], state, NOW + timedelta(minutes=60))
        send.assert_not_called()


    def test_reminder_failure_preserves_timer_for_retry(self):
        previous = {"alarms": {ERROR: alarm()}, "last_reminder_at": agent.iso(NOW)}
        agent.save_state(previous)
        with patch.object(agent, "fetch_alarms", return_value=previous["alarms"]), \
                patch.object(agent, "send_once", side_effect=RuntimeError("failed")):
            with self.assertRaises(RuntimeError):
                agent.one_round(None, previous, NOW + timedelta(minutes=30))
        self.assertEqual(agent.load_state()["last_reminder_at"], agent.iso(NOW))


    def test_atomic_save_failure_does_not_advance_input(self):
        previous = {"alarms": {ERROR: alarm("OK")}, "last_reminder_at": None}
        agent.save_state(previous)
        with patch.object(agent, "fetch_alarms", return_value={ERROR: alarm()}), \
                patch.object(agent, "send_once"), patch.object(agent.os, "replace", side_effect=OSError("disk")):
            with self.assertRaises(OSError):
                agent.one_round(None, previous, NOW)
        self.assertEqual(previous["alarms"], agent.load_state()["alarms"])


    def test_corrupt_state_is_backed_up(self):
        Path(self.path).write_text("{broken")
        self.assertIsNone(agent.load_state())
        self.assertTrue(Path(self.path + ".corrupt").exists())


    def test_invalid_state_structure_and_timestamps_rebuild_baseline(self):
        cases = [[], None, {"alarms": None}, {"alarms": []},
                 {"alarms": {ERROR: {}}},
                 {"alarms": {ERROR: dict(alarm(), state=[])}},
                 {"alarms": {ERROR: dict(alarm(), state="BROKEN")}},
                 {"alarms": {ERROR: dict(alarm(), reason=12)}},
                 {"alarms": {ERROR: dict(alarm(), since=None)}},
                 {"alarms": {ERROR: dict(alarm(), since="2026-10-29T08:00:00")}},
                 {"alarms": {ERROR: alarm()}, "last_reminder_at": "bad"},
                 {"alarms": {}, "last_reminder_at": 12}]
        for payload in cases:
            with self.subTest(payload=payload), self.assertLogs(agent.log):
                Path(self.path).write_text(json.dumps(payload), encoding="utf-8")
                self.assertIsNone(agent.load_state())
                self.assertEqual(json.loads(Path(self.path + ".corrupt").read_text()), payload)
        state, send = self.run_round({ERROR: alarm()}, agent.load_state())
        send.assert_called_once()
        self.assertEqual(state, agent.load_state())


    def test_invalid_utf8_state_is_backed_up(self):
        Path(self.path).write_bytes(b'\xff')
        with self.assertLogs(agent.log):
            self.assertIsNone(agent.load_state())
        self.assertEqual(Path(self.path + ".corrupt").read_bytes(), b'\xff')


    def test_unreadable_state_is_not_treated_as_corrupt(self):
        with patch("builtins.open", side_effect=PermissionError("denied")), \
                patch.object(agent.os, "replace") as replace:
            with self.assertRaises(PermissionError):
                agent.load_state()
        replace.assert_not_called()


    def test_legacy_state_defaults_are_preserved(self):
        Path(self.path).write_text(json.dumps({"alarms": {ERROR: alarm()},
                                              "last_heartbeat_date": "2026-10-28"}))
        state = agent.load_state()
        self.assertIsNone(state["last_reminder_at"])
        self.assertNotIn("last_heartbeat_date", state)


    def failed_round(self, alarms, state, now=NOW):
        with patch.object(agent, "fetch_alarms", return_value=alarms), \
                patch.object(agent, "send_once", side_effect=RuntimeError("rejected")):
            with self.assertRaises(RuntimeError):
                agent.one_round(None, state, now)
        return agent.load_state()

    def test_unnotified_alarm_recovers_without_recovery_message(self):
        previous = make_state({ERROR: alarm("OK")})
        saved = self.failed_round({ERROR: alarm()}, previous)
        self.assertEqual(saved["alarms"][ERROR]["state"], "ALARM")
        self.assertEqual(saved["notified"][ERROR], "OK")
        self.assertEqual(previous["alarms"][ERROR]["state"], "OK")
        state, send = self.run_round({ERROR: alarm("OK")}, saved, NOW + timedelta(minutes=1))
        send.assert_not_called()
        self.assertEqual(state["notified"][ERROR], "OK")

    def test_logs_show_healthy_collection_and_superseded_changes_without_payload(self):
        with self.assertLogs(agent.log, level="INFO") as logs:
            state, _ = self.run_round({ERROR: alarm("OK")}, None)
        self.assertIn("monitored=1 alarm=0 changes=0 reminders=0 superseded=0", "\n".join(logs.output))
        active = {ERROR: dict(alarm(), reason="PRIVATE_REASON_NOT_FOR_LOGS")}
        with self.assertLogs(agent.log, level="DEBUG") as logs:
            state = self.failed_round(active, state)
        output = "\n".join(logs.output)
        self.assertIn("pending change: alarm=" + ERROR + " old=OK new=ALARM", output)
        self.assertIn("bytes=", output)
        self.assertNotIn("PRIVATE_REASON_NOT_FOR_LOGS", output)
        self.assertNotIn(agent.WEBHOOK, output)
        with self.assertLogs(agent.log, level="INFO") as logs:
            _, send = self.run_round({ERROR: alarm("OK")}, state)
        send.assert_not_called()
        self.assertIn("changes=0 reminders=0 superseded=1", "\n".join(logs.output))

    def test_persistent_alarm_retries_after_restart(self):
        state = self.failed_round({ERROR: alarm()}, make_state({ERROR: alarm("OK")}))
        state, send = self.run_round({ERROR: alarm()}, state, NOW + timedelta(minutes=1))
        send.assert_called_once()
        self.assertIn("OK → ALARM", send.call_args.args[0])
        self.assertEqual(state["notified"][ERROR], "ALARM")

    def test_persistent_recovery_retries_across_multiple_restarts(self):
        state = make_state({ERROR: alarm()}, last=NOW)
        for minute in (1, 2):
            state = self.failed_round({ERROR: alarm("OK")}, state, NOW + timedelta(minutes=minute))
            self.assertEqual(state["notified"][ERROR], "ALARM")
        state, send = self.run_round({ERROR: alarm("OK")}, state, NOW + timedelta(minutes=3))
        send.assert_called_once()
        self.assertIn("ALARM → OK", send.call_args.args[0])
        self.assertEqual(state["notified"][ERROR], "OK")

    def test_unnotified_recovery_is_cancelled_on_relapse(self):
        state = self.failed_round({ERROR: alarm("OK")}, make_state({ERROR: alarm()}, last=NOW),
                                  NOW + timedelta(minutes=1))
        state, send = self.run_round({ERROR: alarm()}, state, NOW + timedelta(minutes=2))
        send.assert_not_called()
        self.assertEqual(state["last_reminder_at"], agent.iso(NOW))
        _, send = self.run_round({ERROR: alarm()}, state, NOW + timedelta(minutes=30))
        self.assertIn("持续异常", send.call_args.args[0])

    def test_flapping_sends_only_latest_alarm(self):
        state = make_state({ERROR: alarm("OK")})
        state = self.failed_round({ERROR: alarm()}, state)
        state, send = self.run_round({ERROR: alarm("OK")}, state, NOW + timedelta(minutes=1))
        send.assert_not_called()
        state, send = self.run_round({ERROR: alarm()}, state, NOW + timedelta(minutes=2))
        send.assert_called_once()
        self.assertIn("OK → ALARM", send.call_args.args[0])

    def test_failed_merged_batch_drops_recovered_alarm_only(self):
        other = ERROR + "-2"
        state = make_state({ERROR: alarm("OK"), other: alarm("OK")})
        state = self.failed_round({ERROR: alarm(), other: alarm()}, state)
        _, send = self.run_round({ERROR: alarm("OK"), other: alarm()}, state, NOW + timedelta(minutes=1))
        send.assert_called_once()
        text = send.call_args.args[0]
        self.assertIn(other + ": OK → ALARM", text)
        self.assertNotIn(ERROR + ":", text)

    def test_failed_reminder_is_cancelled_on_recovery(self):
        state = self.failed_round({ERROR: alarm()}, make_state({ERROR: alarm()}, last=NOW),
                                  NOW + timedelta(minutes=30))
        self.assertEqual(state["reminder_remaining"], [ERROR])
        _, send = self.run_round({ERROR: alarm("OK")}, state, NOW + timedelta(minutes=31))
        send.assert_called_once()
        self.assertIn("ALARM → OK", send.call_args.args[0])
        self.assertNotIn("持续异常", send.call_args.args[0])

    def test_failed_insufficient_data_does_not_hide_recovery(self):
        state = self.failed_round({ERROR: alarm("INSUFFICIENT_DATA")},
                                  make_state({ERROR: alarm()}, last=NOW))
        _, send = self.run_round({ERROR: alarm("OK")}, state, NOW + timedelta(minutes=1))
        send.assert_called_once()
        self.assertIn("ALARM → OK", send.call_args.args[0])

    def test_delivered_insufficient_data_then_OK_remains_silent(self):
        state, send = self.run_round({ERROR: alarm("INSUFFICIENT_DATA")},
                                     make_state({ERROR: alarm()}, last=NOW))
        self.assertIn("ALARM → INSUFFICIENT_DATA", send.call_args.args[0])
        _, send = self.run_round({ERROR: alarm("OK")}, state, NOW + timedelta(minutes=1))
        send.assert_not_called()

    def partially_send(self, alarms, state, now=NOW):
        with patch.object(agent, "MAX_MESSAGE_BYTES", 500), \
                patch.object(agent, "fetch_alarms", return_value=alarms), \
                patch.object(agent, "utc_now", return_value=now), \
                patch.object(agent, "send_once", side_effect=[None, RuntimeError("rejected")]) as send:
            with self.assertRaises(RuntimeError):
                agent.one_round(None, state, now)
        self.assertEqual(send.call_count, 2)
        return agent.load_state(), send.call_args_list[0].args[0]

    def test_partial_batch_then_all_recover_only_notified_alarms_get_recovery(self):
        names = ["RoutingAPI-A%02d" % i for i in range(1, 16)]
        healthy = {n: alarm("OK") for n in names}
        active = {n: alarm() for n in names}
        saved, first = self.partially_send(active, make_state(healthy))
        delivered = {n for n in names if n + ": OK → ALARM" in first}
        self.assertTrue(delivered)
        self.assertLess(len(delivered), len(names))
        self.assertEqual({n for n, value in saved["notified"].items() if value == "ALARM"}, delivered)
        self.assertNotIn(names[-1], first)  # 页脚不泄露其他分段的告警名称。
        state, send = self.run_round(healthy, saved, NOW + timedelta(minutes=1))
        text = "\n".join(c.args[0] for c in send.call_args_list)
        for name in names:
            self.assertEqual(name + ": ALARM → OK" in text, name in delivered)
        self.assertTrue(all(v == "OK" for v in state["notified"].values()))

    def test_partial_batch_rebuilds_only_unconfirmed_items_after_restart(self):
        active = {"RoutingAPI-A%02d" % i: alarm() for i in range(15)}
        healthy = {n: alarm("OK") for n in active}
        saved, first = self.partially_send(active, make_state(healthy))
        delivered = {n for n in active if n + ": OK → ALARM" in first}
        state, send = self.run_round(active, saved, NOW + timedelta(minutes=1))
        text = "\n".join(c.args[0] for c in send.call_args_list)
        for name in active:
            self.assertEqual(name + ": OK → ALARM" in text, name not in delivered)
        self.assertTrue(all(v == "ALARM" for v in state["notified"].values()))

    def test_notification_snapshot_is_durable_before_first_http(self):
        def send(text, **kwargs):
            saved = agent.load_state()
            self.assertEqual(saved["alarms"][ERROR]["state"], "ALARM")
            self.assertNotIn(ERROR, saved["notified"])
            self.assertNotIn("pending", saved)
        with patch.object(agent, "fetch_alarms", return_value={ERROR: alarm()}), \
                patch.object(agent, "send_once", side_effect=send):
            agent.one_round(None, None, NOW)
        self.assertEqual(agent.load_state()["notified"][ERROR], "ALARM")

    def test_repeated_failed_reminders_keep_one_progress_set(self):
        state = make_state({ERROR: alarm()}, last=NOW)
        for minute in range(30, 40):
            state = self.failed_round({ERROR: alarm()}, state, NOW + timedelta(minutes=minute))
            self.assertEqual(state["reminder_remaining"], [ERROR])
            self.assertEqual(state["last_reminder_at"], agent.iso(NOW))
            self.assertNotIn("pending", state)

    def test_partial_reminder_resumes_unconfirmed_names_only(self):
        active = {"RoutingAPI-A%02d" % i: alarm() for i in range(15)}
        saved, first = self.partially_send(active, make_state(active, last=NOW),
                                           NOW + timedelta(minutes=30))
        delivered = {n for n in active if n + "  已持续" in first}
        self.assertTrue(delivered)
        self.assertEqual(set(saved["reminder_remaining"]), set(active) - delivered)
        self.assertEqual(saved["last_reminder_at"], agent.iso(NOW))
        state, send = self.run_round(active, saved, NOW + timedelta(minutes=31))
        text = "\n".join(c.args[0] for c in send.call_args_list)
        for name in active:
            self.assertEqual(name in text, name not in delivered)
        self.assertIsNone(state["reminder_remaining"])
        self.assertEqual(state["last_reminder_at"], agent.iso(NOW + timedelta(minutes=31)))

    def test_change_timer_resets_at_final_successful_batch_only(self):
        active = {"RoutingAPI-A%02d" % i: alarm() for i in range(15)}
        healthy = {n: alarm("OK") for n in active}
        saved, _ = self.partially_send(active, make_state(healthy, last=NOW))
        self.assertIsNone(saved["last_reminder_at"])  # 健康基线没有异常提醒计时。
        state, _ = self.run_round(active, saved, NOW + timedelta(hours=2))
        self.assertEqual(state["last_reminder_at"], agent.iso(NOW + timedelta(hours=2)))

    def test_capacity_rejects_snapshot_without_sending_old_observation(self):
        previous = make_state({ERROR: alarm("OK")})
        agent.save_state(previous)
        original = Path(self.path).read_bytes()
        with patch.object(agent, "MAX_STATE_BYTES", len(original)), \
                patch.object(agent, "fetch_alarms", return_value={ERROR: alarm()}), \
                patch.object(agent, "send_once") as send:
            with self.assertRaisesRegex(RuntimeError, "state capacity exceeded"):
                agent.one_round(None, previous, NOW)
        send.assert_not_called()
        self.assertEqual(Path(self.path).read_bytes(), original)

    def test_ack_failure_stops_then_only_unconfirmed_items_repeat(self):
        active = {"RoutingAPI-A%02d" % i: alarm() for i in range(15)}
        healthy = {n: alarm("OK") for n in active}
        real_save = agent.save_state
        ack_count = 0
        def save(candidate):
            nonlocal ack_count
            if any(v == "ALARM" for v in candidate["notified"].values()):
                ack_count += 1
                if ack_count == 2:
                    raise OSError("disk")
            real_save(candidate)
        with patch.object(agent, "MAX_MESSAGE_BYTES", 500), \
                patch.object(agent, "fetch_alarms", return_value=active), \
                patch.object(agent, "send_once") as send, \
                patch.object(agent, "save_state", side_effect=save):
            with self.assertRaises(OSError):
                agent.one_round(None, make_state(healthy), NOW)
        self.assertEqual(send.call_count, 2)
        first, second = [c.args[0] for c in send.call_args_list]
        state, send = self.run_round(active, agent.load_state(), NOW + timedelta(minutes=1))
        text = "\n".join(c.args[0] for c in send.call_args_list)
        for name in active:
            if name + ": OK → ALARM" in first:
                self.assertNotIn(name + ":", text)
            if name + ": OK → ALARM" in second:
                self.assertIn(name + ": OK → ALARM", text)

    def test_per_round_attempt_budget_keeps_collecting_and_makes_progress(self):
        active = {"RoutingAPI-A%02d" % i: alarm() for i in range(15)}
        with patch.object(agent, "MAX_SEND_ATTEMPTS_PER_ROUND", 1), \
                patch.object(agent, "MAX_MESSAGE_BYTES", 500), \
                patch.object(agent, "fetch_alarms", return_value=active) as fetch, \
                patch.object(agent, "send_once") as send, self.assertLogs(agent.log, level="INFO") as logs:
            state = agent.one_round(None, None, NOW)
            first_count = len(state["notified"])
            self.assertGreater(first_count, 0)
            self.assertLess(first_count, len(active))
            state = agent.one_round(None, agent.load_state(), NOW + timedelta(minutes=1))
            self.assertGreater(len(state["notified"]), first_count)
        self.assertEqual(send.call_count, 2)
        self.assertEqual(fetch.call_count, 2)
        self.assertIn("delivery budget reached: remaining_changes=", "\n".join(logs.output))

    def test_temporary_aws_failure_preserves_baseline_and_skips_stale_delivery(self):
        state = make_state({ERROR: alarm()}, notified={ERROR: "OK"})
        agent.save_state(state)
        errors = [EndpointConnectionError(endpoint_url="https://example.invalid"),
                  ClientError({"Error": {"Code": "Throttling"}}, "DescribeAlarms")]
        for error in errors:
            with self.subTest(error=type(error).__name__), \
                    patch.object(agent, "fetch_alarms", side_effect=error), \
                    patch.object(agent, "send_once") as send, self.assertLogs(agent.log):
                result = agent.one_round(None, state, NOW)
            send.assert_not_called()
            self.assertEqual(result, state)
            self.assertEqual(agent.load_state(), state)

    def test_exclusion_removes_observations_and_notification_baseline(self):
        state = make_state({ERROR: alarm(), LATENCY: alarm()}, last=NOW)
        agent.save_state(state)
        loaded = agent.load_state()
        self.assertNotIn(LATENCY, loaded["notified"])
        self.assertNotIn(LATENCY, loaded["alarms"])
        _, send = self.run_round({ERROR: alarm()}, loaded)
        send.assert_not_called()

    def test_invalid_notification_state_rebuilds_baseline(self):
        for fields in ({"notified": []}, {"notified": {ERROR: []}},
                       {"notified": {ERROR: "BAD"}}, {"reminder_remaining": {}},
                       {"reminder_remaining": [ERROR, ERROR]}, {"reminder_remaining": [12]}):
            with self.subTest(fields=fields), self.assertLogs(agent.log):
                payload = dict(make_state({ERROR: alarm()}), **fields)
                agent.save_state(payload)
                self.assertIsNone(agent.load_state())

    def test_unknown_state_version_is_not_silently_rebuilt(self):
        Path(self.path).write_text(json.dumps({"schema_version": 4, "alarms": []}))
        with self.assertRaisesRegex(RuntimeError, "schema version"):
            agent.load_state()
        self.assertTrue(Path(self.path).exists())
        self.assertFalse(Path(self.path + ".corrupt").exists())

    def test_schema_two_with_pending_requires_explicit_safe_upgrade(self):
        payload = {"schema_version": 2, "alarms": {ERROR: alarm()}, "pending": [queued()]}
        agent.save_state(payload)
        original = Path(self.path).read_bytes()
        with self.assertRaisesRegex(RuntimeError, "drain with the previous agent"):
            agent.load_state()
        self.assertEqual(Path(self.path).read_bytes(), original)
        self.assertFalse(Path(self.path + ".corrupt").exists())

    def test_schema_two_empty_queue_migrates_notification_baseline(self):
        agent.save_state({"schema_version": 2, "alarms": {ERROR: alarm()},
                          "pending": [], "collection_paused": True})
        state = agent.load_state()
        self.assertEqual(state["schema_version"], 3)
        self.assertEqual(state["notified"], {ERROR: "ALARM"})
        self.assertNotIn("pending", state)
        self.assertNotIn("collection_paused", state)

    def test_long_outage_and_flapping_do_not_accumulate_history(self):
        state = make_state({ERROR: alarm("OK")})
        sizes = []
        with patch.object(agent, "fetch_alarms") as fetch, \
                patch.object(agent, "send_once", side_effect=RuntimeError("rejected")) as send:
            for minute in range(120):
                fetch.return_value = {ERROR: alarm("ALARM" if minute % 2 == 0 else "OK")}
                try:
                    agent.one_round(None, state, NOW + timedelta(minutes=minute))
                except RuntimeError:
                    pass
                state = agent.load_state()
                self.assertEqual(len(state["notified"]), 1)
                self.assertEqual(len(state["alarms"]), 1)
                self.assertNotIn("pending", state)
                sizes.append(Path(self.path).stat().st_size)
        self.assertEqual(fetch.call_count, 120)
        self.assertEqual(send.call_count, 60)
        self.assertLess(max(sizes) - min(sizes), 20)

    def test_failed_deletion_is_retried_but_cancelled_if_alarm_reappears(self):
        state = self.failed_round({}, make_state({ERROR: alarm()}, last=NOW))
        state, send = self.run_round({ERROR: alarm()}, state, NOW + timedelta(minutes=1))
        send.assert_not_called()
        state = self.failed_round({}, state, NOW + timedelta(minutes=2))
        state, send = self.run_round({}, state, NOW + timedelta(minutes=3))
        self.assertIn("Alarm 已被删除", send.call_args.args[0])
        self.assertNotIn(ERROR, state["notified"])

    def test_unnotified_new_alarm_disappears_silently(self):
        state = self.failed_round({ERROR: alarm()}, None)
        state, send = self.run_round({}, state, NOW + timedelta(minutes=1))
        send.assert_not_called()
        self.assertEqual(state["notified"], {})

    def test_slow_collection_consumes_poll_budget_before_sending(self):
        clock = FakeClock()
        def fetch(_):
            clock.seconds += agent.POLL_SECONDS + 1
            return {ERROR: alarm()}
        with patch.object(agent.time, "monotonic", side_effect=clock.monotonic), \
                patch.object(agent, "fetch_alarms", side_effect=fetch), \
                patch.object(agent, "send_once") as send:
            state = agent.one_round(None, None, NOW)
        send.assert_not_called()
        self.assertEqual(state["notified"], {})
        self.assertEqual(state["alarms"][ERROR]["state"], "ALARM")

    def test_alarm_churn_hits_capacity_without_discarding_old_baselines(self):
        state = None
        exceeded = 0
        with patch.object(agent, "MAX_STATE_BYTES", 1000), \
                patch.object(agent, "fetch_alarms") as fetch, \
                patch.object(agent, "send_once", side_effect=RuntimeError("rejected")):
            for minute in range(60):
                fetch.return_value = {"RoutingAPI-%03d" % minute: alarm("OK")}
                prior = set(state["notified"]) if state else set()
                try:
                    agent.one_round(None, state, NOW + timedelta(minutes=minute))
                except RuntimeError as e:
                    exceeded += "state capacity exceeded" in str(e)
                state = agent.load_state()
                self.assertTrue(prior.issubset(state["notified"]))
                self.assertLessEqual(Path(self.path).stat().st_size, 1000)
        self.assertGreater(exceeded, 0)
        self.assertEqual(fetch.call_count, 60)

    def test_capacity_reserves_mixed_old_and_new_ack_states(self):
        first, last = "RoutingAPI-A", "RoutingAPI-Z"
        state = make_state({last: alarm("INSUFFICIENT_DATA")})
        current = {first: alarm(), last: alarm()}
        intermediate = copy.deepcopy(state)
        intermediate["alarms"] = current
        intermediate["notified"][first] = "ALARM"
        limit = len(json.dumps(intermediate, ensure_ascii=False, indent=1).encode("utf-8")) - 1
        agent.save_state(state)
        original = Path(self.path).read_bytes()
        with patch.object(agent, "MAX_STATE_BYTES", limit), \
                patch.object(agent, "fetch_alarms", return_value=current), \
                patch.object(agent, "send_once") as send:
            with self.assertRaisesRegex(RuntimeError, "state capacity exceeded"):
                agent.one_round(None, state, NOW)
        send.assert_not_called()
        self.assertEqual(Path(self.path).read_bytes(), original)

    def test_empty_reminder_progress_after_exclusion_does_not_block_future_reminders(self):
        state = make_state({ERROR: alarm(), LATENCY: alarm()}, last=NOW)
        state["reminder_remaining"] = [LATENCY]
        agent.save_state(state)
        state, send = self.run_round({ERROR: alarm()}, agent.load_state(), NOW + timedelta(minutes=30))
        send.assert_called_once()
        self.assertIn(ERROR, send.call_args.args[0])
        self.assertIsNone(state["reminder_remaining"])


class AWSTests(unittest.TestCase):
    def test_real_sdk_pagination_filters_metric_and_composite(self):
        cw = boto3.client("cloudwatch", region_name="ap-southeast-1",
                          aws_access_key_id="TEST_ONLY", aws_secret_access_key="TEST_ONLY")
        def aws_alarm(name):
            return {"AlarmName": name, "StateValue": "ALARM", "StateReason": "x" * 200,
                    "StateUpdatedTimestamp": NOW}
        args = {"AlarmNamePrefix": "RoutingAPI", "AlarmTypes": ["MetricAlarm", "CompositeAlarm"]}
        with Stubber(cw) as stub, patch.object(agent, "PREFIX", "RoutingAPI"), \
                patch.object(agent, "EXCLUDE_PATTERNS", ["RoutingAPI-*-Latency"]):
            stub.add_response("describe_alarms", {"MetricAlarms": [aws_alarm(LATENCY)],
                                                 "NextToken": "page2"}, args)
            stub.add_response("describe_alarms", {"MetricAlarms": [aws_alarm(ERROR)],
                              "CompositeAlarms": [aws_alarm(ERROR + "-composite")]},
                              dict(args, NextToken="page2"))
            alarms = agent.fetch_alarms(cw)
            stub.assert_no_pending_responses()
        self.assertEqual(set(alarms), {ERROR, ERROR + "-composite"})
        self.assertEqual(len(alarms[ERROR]["reason"]), 160)

    def test_transition_timestamp_is_preferred_with_updated_fallback(self):
        cw = boto3.client("cloudwatch", region_name="ap-southeast-1",
                          aws_access_key_id="TEST_ONLY", aws_secret_access_key="TEST_ONLY")
        entered = NOW - timedelta(hours=2)
        updated = NOW - timedelta(minutes=1)
        metric = {"AlarmName": ERROR, "StateValue": "ALARM",
                  "StateUpdatedTimestamp": updated, "StateTransitionedTimestamp": entered}
        composite = dict(metric, AlarmName=ERROR + "-composite")
        fallback = dict(metric, AlarmName=ERROR + "-fallback")
        del fallback["StateTransitionedTimestamp"]
        with Stubber(cw) as stub:
            stub.add_response("describe_alarms", {"MetricAlarms": [metric, fallback],
                                                 "CompositeAlarms": [composite]})
            alarms = agent.fetch_alarms(cw)
        self.assertEqual(alarms[ERROR]["since"], agent.iso(entered))
        self.assertEqual(alarms[ERROR + "-composite"]["since"], agent.iso(entered))
        self.assertEqual(alarms[ERROR + "-fallback"]["since"], agent.iso(updated))
        self.assertIn("已持续 2h00m", agent.reminder_msg(alarms, NOW))


class SendTests(unittest.TestCase):
    def setUp(self):
        for target, value in (("_last_send_at", None), ("TAG", "[RoutingAPI]")):
            p = patch.object(agent, target, value)
            p.start()
            self.addCleanup(p.stop)
        p = patch.object(agent.time, "sleep")
        p.start()
        self.addCleanup(p.stop)


    def deliver(self, text):
        state = make_state({ERROR: alarm()}, notified={})
        part = {"kind": "change", "events": agent.notification_events(state), "text": text}
        with patch.object(agent, "save_state"), patch.object(agent, "batch_notifications", return_value=[part]):
            return agent.deliver_notifications(state, time.monotonic() + 20, NOW)

    def test_whole_alarm_batches_preserve_every_event_and_byte_budget(self):
        alarms = {f"{ERROR}-{i}": dict(alarm(), reason='中文🔴"\\\n\t\x00' * 15) for i in range(100)}
        events = agent.diff({}, alarms)
        parts = agent.batch_notifications(events, alarms, NOW, "change")
        self.assertGreater(len(parts), 1)
        self.assertEqual([e for part in parts for e in part["events"]], events)
        for part in parts:
            self.assertLessEqual(len(agent.message_body(part["text"])), agent.MAX_MESSAGE_BYTES)
            self.assertTrue(part["text"].startswith(agent.TAG))
            for event in part["events"]:
                self.assertIn(event["name"] + ": (新建) → ALARM", part["text"])
                self.assertIn(event["reason"], part["text"])
            for event in events:
                self.assertEqual(event["name"] + ":" in part["text"], event in part["events"])

    def test_exact_byte_boundary_and_single_item_oversize(self):
        alarms = {ERROR: alarm()}
        events = agent.diff({}, alarms)
        size = len(agent.message_body(agent.change_msg(events, alarms, NOW)))
        with patch.object(agent, "MAX_MESSAGE_BYTES", size):
            self.assertEqual(len(agent.batch_notifications(events, alarms, NOW, "change")), 1)
        with patch.object(agent, "MAX_MESSAGE_BYTES", size - 1):
            with self.assertRaisesRegex(ValueError, "single alarm"):
                agent.batch_notifications(events, alarms, NOW, "change")

    def test_large_alarm_batch_sends_bounded_requests_and_confirms_names(self):
        alarms = {f"{ERROR}-{i}": dict(alarm(), reason="异常🔴" * 40) for i in range(100)}
        state = make_state(alarms, notified={})
        with patch.object(agent.urllib.request, "urlopen") as open_url, \
                patch.object(agent, "save_state"), patch.object(agent, "utc_now", return_value=NOW):
            open_url.return_value.__enter__.return_value.read.return_value = b'{"errcode":0}'
            state = agent.deliver_notifications(state, time.monotonic() + 20, NOW)
        self.assertGreater(open_url.call_count, 1)
        self.assertLessEqual(open_url.call_count, agent.MAX_SEND_ATTEMPTS_PER_ROUND)
        contents = []
        for call in open_url.call_args_list:
            body = call.args[0].data
            self.assertLessEqual(len(body), agent.MAX_MESSAGE_BYTES)
            contents.append(json.loads(body)["text"]["content"])
        text = "\n".join(contents)
        for name in alarms:
            self.assertEqual(name in state["notified"], name + ":" in text)

    def test_send_once_blocks_oversized_payload_before_network(self):
        with patch.object(agent.urllib.request, "urlopen") as open_url:
            with self.assertRaisesRegex(ValueError, "byte budget"):
                agent.send_once("x" * agent.MAX_MESSAGE_BYTES)
        open_url.assert_not_called()


    def test_requests_are_paced_even_after_failed_attempt(self):
        with patch.object(agent.urllib.request, "urlopen") as open_url, \
                patch.object(agent.time, "monotonic", side_effect=[10.0, 11.0, 13.1]), \
                patch.object(agent.time, "sleep") as sleep:
            open_url.side_effect = [TimeoutError(), open_url.return_value]
            open_url.return_value.__enter__.return_value.read.return_value = b'{"errcode":0}'
            with self.assertRaises(TimeoutError):
                agent.send_once("test")
            agent.send_once("test")
        sleep.assert_called_once()
        self.assertAlmostEqual(sleep.call_args.args[0], 2.1)


    def test_expired_budget_does_not_start_network_request(self):
        with patch.object(agent.time, "monotonic", return_value=20), \
                patch.object(agent.urllib.request, "urlopen") as open_url:
            with self.assertRaises(agent.SendBudgetExhausted):
                agent.send_once("test", deadline=20)
        open_url.assert_not_called()


    def test_timeout_covers_body_read_and_restores_signal_handler(self):
        previous = signal.getsignal(signal.SIGALRM)
        with patch.object(agent.urllib.request, "urlopen") as open_url:
            # signal.pause blocks until SIGALRM; unlike mock sleeps, this exercises the real deadline.
            open_url.return_value.__enter__.return_value.read.side_effect = lambda *_: signal.pause()
            with self.assertRaises(TimeoutError):
                agent.send_once("test", deadline=time.monotonic() + 0.2)
        self.assertEqual(signal.getsignal(signal.SIGALRM), previous)
        self.assertEqual(signal.getitimer(signal.ITIMER_REAL)[0], 0)


    def test_invalid_json_shape_is_a_retryable_failure(self):
        with patch.object(agent.urllib.request, "urlopen") as open_url:
            open_url.return_value.__enter__.return_value.read.return_value = b'[]'
            with self.assertRaisesRegex(ValueError, "response object"):
                agent.send_once("test")


    def test_transient_failure_then_success(self):
        with patch.object(agent, "send_once", side_effect=[TimeoutError(), TimeoutError(), None]) as once, \
                patch.object(agent.time, "sleep") as sleep:
            self.deliver("test")
        self.assertEqual(once.call_count, 3)
        self.assertEqual([x.args[0] for x in sleep.call_args_list], [1, 2])


    def test_retry_limit_and_no_webhook_in_errors(self):
        error = urllib.error.URLError("https://secret-token.example")
        with patch.object(agent, "send_once", side_effect=error) as once, \
                patch.object(agent.time, "sleep") as sleep, self.assertLogs(agent.log) as logs:
            with self.assertRaisesRegex(RuntimeError, "delivery deferred") as result:
                self.deliver("test")
        self.assertEqual(once.call_count, 5)
        self.assertEqual([x.args[0] for x in sleep.call_args_list], [1, 2, 4, 8])
        self.assertNotIn("secret-token", str(result.exception) + "".join(logs.output))


    def test_http_errors_are_classified(self):
        for code, attempts in ((429, 5), (500, 5), (503, 5), (400, 1), (403, 1)):
            with self.subTest(code=code):
                error = urllib.error.HTTPError("https://secret-token.example", code, "test", {}, None)
                with patch.object(agent, "send_once", side_effect=error) as once, \
                        patch.object(agent.time, "sleep"), patch.object(agent.log, "warning"):
                    with self.assertRaises(RuntimeError):
                        self.deliver("test")
                self.assertEqual(once.call_count, attempts)


    def test_business_rejection_is_not_retried_in_same_send(self):
        with patch.object(agent, "send_once", side_effect=RuntimeError("DingTalk rejected")) as once, \
                patch.object(agent.time, "sleep") as sleep:
            with self.assertRaises(RuntimeError):
                self.deliver("test")
        once.assert_called_once()
        sleep.assert_not_called()


    def test_text_payload_and_signature(self):
        webhook = "https://oapi.dingtalk.com/robot/send?access_token=TEST_ONLY"
        for secret in ("", "SECtest"):
            with self.subTest(signed=bool(secret)), patch.object(agent, "WEBHOOK", webhook), \
                    patch.object(agent, "SECRET", secret), patch.object(agent.time, "time", return_value=123), \
                    patch.object(agent.urllib.request, "urlopen") as open_url:
                open_url.return_value.__enter__.return_value.read.return_value = b'{"errcode":0}'
                agent.send_once("[RoutingAPI] 测试")
                request = open_url.call_args.args[0]
                self.assertEqual(open_url.call_args.kwargs["timeout"], 10)
                self.assertEqual(json.loads(request.data)["text"]["content"], "[RoutingAPI] 测试")
                query = urllib.parse.parse_qs(urllib.parse.urlsplit(request.full_url).query)
                if secret:
                    expected = base64.b64encode(hmac.new(secret.encode(), b"123000\nSECtest",
                                                         hashlib.sha256).digest()).decode()
                    self.assertEqual(query["sign"], [expected])
                    self.assertEqual(query["timestamp"], ["123000"])
                else:
                    self.assertEqual(request.full_url, webhook)


    def test_http_success_with_business_error_is_failure(self):
        with patch.object(agent.urllib.request, "urlopen") as open_url:
            open_url.return_value.__enter__.return_value.read.return_value = b'{"errcode":310000}'
            with self.assertRaisesRegex(RuntimeError, "310000"):
                agent.send_once("test")


    def test_spacing_includes_retries_and_time_budget_stops_sending(self):
        clock = FakeClock()
        alarms = {"RoutingAPI-A%02d" % i: alarm() for i in range(15)}
        state = make_state(alarms, notified={})
        starts = []
        timeouts = []
        with patch.object(agent, "MAX_MESSAGE_BYTES", 500), \
                patch.object(agent.time, "monotonic", side_effect=clock.monotonic), \
                patch.object(agent.time, "sleep", side_effect=clock.sleep), \
                patch.object(agent.urllib.request, "urlopen") as open_url, \
                patch.object(agent, "save_state"):
            response = open_url.return_value
            response.__enter__.return_value.read.return_value = b'{"errcode":0}'
            def request(*args, **kwargs):
                starts.append(clock.seconds)
                timeouts.append(kwargs["timeout"])
                if len(starts) == 1:
                    raise TimeoutError("test")
                return response
            open_url.side_effect = request
            result = agent.deliver_notifications(state, deadline=6.4, now=NOW)
        self.assertEqual(len(starts), 3)
        for previous, current in zip(starts, starts[1:]):
            self.assertGreaterEqual(current - previous, agent.MIN_SEND_INTERVAL - 1e-9)
        self.assertGreater(len(result["notified"]), 0)
        self.assertLess(len(result["notified"]), len(alarms))
        self.assertLessEqual(clock.seconds, 6.4)
        self.assertAlmostEqual(timeouts[-1], 0.2)

    def test_retry_backoff_cannot_exceed_remaining_budget(self):
        clock = FakeClock()
        state = make_state({ERROR: alarm()}, notified={})
        with patch.object(agent.time, "monotonic", side_effect=clock.monotonic), \
                patch.object(agent.time, "sleep", side_effect=clock.sleep), \
                patch.object(agent, "send_once", side_effect=TimeoutError()) as once, \
                self.assertLogs(agent.log):
            with self.assertRaises(RuntimeError):
                agent.deliver_notifications(state, deadline=0.5, now=NOW)
        once.assert_called_once()
        self.assertEqual(clock.seconds, 0)


class MainTests(unittest.TestCase):
    def test_log_level_defaults_normalization_filtering_and_sdk_isolation(self):
        code = ("import agent,logging; print('level='+agent.LOG_LEVEL); "
                "agent.log.debug('APP_DEBUG'); agent.log.info('APP_INFO'); "
                "logging.getLogger('botocore.endpoint').debug('SDK_DEBUG_PRIVATE')")
        for value, expected in ((None, "INFO"), (" debug ", "DEBUG"), ("warning", "WARNING")):
            with self.subTest(value=value):
                env = dict(os.environ)
                env.pop("LOG_LEVEL", None)
                if value is not None:
                    env["LOG_LEVEL"] = value
                result = subprocess.run([sys.executable, "-c", code], env=env,
                                        capture_output=True, text=True, check=True)
                self.assertIn("level=" + expected, result.stdout)
                self.assertEqual("APP_DEBUG" in result.stdout, expected == "DEBUG")
                self.assertEqual("APP_INFO" in result.stdout, expected != "WARNING")
                self.assertNotIn("SDK_DEBUG_PRIVATE", result.stdout + result.stderr)

    def test_invalid_log_level_exits_two_before_network(self):
        result = subprocess.run([sys.executable, "-c", "import agent"],
                                env=dict(os.environ, LOG_LEVEL="verbose"), capture_output=True, text=True)
        self.assertEqual(result.returncode, 2)
        self.assertIn("LOG_LEVEL must be", result.stderr)

    def test_five_failed_rounds_exit_one_and_reload_saved_state(self):
        with patch.object(agent.boto3, "client"), patch.object(agent.signal, "signal"), \
                patch.object(agent, "load_state", return_value=None) as load, \
                patch.object(agent, "one_round", side_effect=RuntimeError("failed")) as one, \
                patch.object(agent.time, "sleep"), self.assertLogs(agent.log), \
                patch.object(agent, "MAX_SEND_FAILURES", 5):
            with self.assertRaises(SystemExit) as result:
                agent.main()
        self.assertEqual(result.exception.code, 1)
        self.assertEqual(one.call_count, 5)
        self.assertEqual(load.call_count, 6)

    def test_unsafe_state_upgrade_exits_two_without_polling(self):
        with patch.object(agent.boto3, "client"), patch.object(agent.signal, "signal"), \
                patch.object(agent, "load_state", side_effect=RuntimeError("schema 2 has pending notifications")), \
                patch.object(agent, "one_round") as one, self.assertLogs(agent.log):
            with self.assertRaises(SystemExit) as result:
                agent.main()
        self.assertEqual(result.exception.code, 2)
        one.assert_not_called()

    def test_missing_credentials_exit_two(self):
        with patch.object(agent.boto3, "client"), patch.object(agent.signal, "signal"), \
                patch.object(agent, "load_state", return_value=None), \
                patch.object(agent, "one_round", side_effect=NoCredentialsError()), \
                self.assertLogs(agent.log):
            with self.assertRaises(SystemExit) as result:
                agent.main()
        self.assertEqual(result.exception.code, 2)

    def test_config_strips_empty_patterns_and_spaces(self):
        env = dict(os.environ, ALARM_EXCLUDE_PATTERNS=" RoutingAPI-*-Latency, , RoutingAPI-*-4XX* ,")
        result = subprocess.run([sys.executable, "-c", "import agent,json;print(json.dumps(agent.EXCLUDE_PATTERNS))"],
                                env=env, capture_output=True, text=True, check=True)
        self.assertEqual(json.loads(result.stdout), ["RoutingAPI-*-Latency", "RoutingAPI-*-4XX*"])

    def test_invalid_interval_exits_two_before_network(self):
        for name, value in (("POLL_SECONDS", "0"), ("POLL_SECONDS", "-1"),
                            ("POLL_SECONDS", "oops"), ("MAX_STATE_BYTES", "0"),
                            ("MAX_STATE_BYTES", "-1"), ("MAX_SEND_ATTEMPTS_PER_ROUND", "oops"),
                            ("SEND_BUDGET_SECONDS", "0")):
            with self.subTest(name=name, value=value):
                result = subprocess.run([sys.executable, "-c", "import agent"],
                                        env=dict(os.environ, **{name: value}), capture_output=True, text=True)
                self.assertEqual(result.returncode, 2)
                self.assertIn(f"{name} must be a positive integer", result.stderr)


if __name__ == "__main__":
    unittest.main()
