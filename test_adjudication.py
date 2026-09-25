"""中止封存与解封裁决的全链路测试。

覆盖：封存默认隔离、双阶段顺序门禁、分歧伦理复核、同内容重试安全、
并发单一当前版本、重启续裁、时钟校正只增修订、参与者撤回、审计溯源。
"""

import json
import os
import tempfile
import threading
import unittest
from http.server import ThreadingHTTPServer
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from core import AdjudicationService, DomainError
from service import make_handler
from storage import Store


# ---------------------------------------------------------------------------
# 领域级直接测试
# ---------------------------------------------------------------------------

class DomainTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "test.db")
        self.svc = AdjudicationService(Store(self.db_path))
        self.actor = "tester"

    def tearDown(self):
        self.tmp.cleanup()

    def call(self, fn, *args):
        return self.svc.store.write(lambda c: fn(c, self.actor, *args))

    # -- 造数据 ---------------------------------------------------------

    def seed_case(self):
        participant, _ = self.call(
            self.svc.register_participant,
            {"participant_id": "pt-1",
             "consent_scope": {"research": True, "safety_review": True,
                               "version": "v1"}},
        )
        session, _ = self.call(
            self.svc.start_session,
            {"session_id": "ss-1", "participant_id": "pt-1", "started_at": 0.0},
        )
        segments = []
        for sid, kind, t0, t1 in [
            ("seg-pre", "brain_signal", 0.0, 10.0),
            ("seg-at", "stimulus", 10.0, 20.0),
            ("seg-post", "behavior", 20.0, 30.0),
        ]:
            seg, _ = self.call(self.svc.register_segment, {
                "segment_id": sid, "session_id": "ss-1", "kind": kind,
                "t_start": t0, "t_end": t1,
                "data_ref": f"s3://bucket/{sid}", "checksum": f"sha:{sid}",
            })
            segments.append(seg)
        self.call(self.svc.register_device_state, {
            "device_state_id": "dev-1", "session_id": "ss-1",
            "recorded_at": 14.0, "payload": {"voltage": 4.2}, "checksum": "sha:dev-1",
        })
        self.call(self.svc.register_medical_order, {
            "order_id": "ord-1", "session_id": "ss-1", "issued_at": 15.5,
            "payload": {"action": "stop_stimulation", "by": "clinician-on-call"},
        })
        return participant, session, segments

    def seal_event(self, event_id="ev-1"):
        event, _ = self.call(self.svc.declare_safety_event, {
            "event_id": event_id, "session_id": "ss-1",
            "occurred_at": 15.0, "adverse_effect": "短暂运动性发作后自行缓解",
            "timeline": {"clock": "study-master-clock", "tz": "UTC"},
        })
        return event

    def reviews_consensus_and_dispute(self, event_id="ev-1"):
        # pre：未受影响且可解释 → release；at：受影响且不可解释 → exclude；
        # post：受影响但仍可解释 → 分歧 → 需伦理
        self.call(self.svc.submit_review, event_id, {
            "role": "clinical_safety", "reviewer": "dr-clinical",
            "signature": "sig/clinical/1",
            "payload": {"segments": {
                "seg-pre": {"affected": False, "note": "事件前基线"},
                "seg-at": {"affected": True, "note": "跨越中止时刻"},
                "seg-post": {"affected": True, "note": "异常状态窗口内"},
            }}})
        self.call(self.svc.submit_review, event_id, {
            "role": "methodology", "reviewer": "dr-method",
            "signature": "sig/method/1",
            "payload": {"segments": {
                "seg-pre": {"interpretable": True, "note": "预注册基线有效"},
                "seg-at": {"interpretable": False, "note": "治疗污染终点"},
                "seg-post": {"interpretable": True, "note": "行为终点仍可估计"},
            }}})

    def ethics_and_apply(self, event_id="ev-1", applied_by="data-steward-1"):
        self.call(self.svc.submit_review, event_id, {
            "role": "ethics", "reviewer": "ethics-board",
            "signature": "sig/ethics/1",
            "payload": {"segments": {
                "seg-post": {"decision": "safety_only",
                             "note": "仅限安全复盘，不进研究终点"},
            }}})
        version, _ = self.call(self.svc.apply_release, event_id,
                               {"applied_by": applied_by})
        return version


class SealingAndVisibilityTest(DomainTestBase):
    def test_event_seals_all_context_on_unified_timeline(self):
        _, _, segments = self.seed_case()
        event = self.seal_event()
        self.assertEqual(event["occurred_at"], 15.0)
        positions = {(r["category"], r["ref_id"]): r["position"]
                     for r in event["sealed_records"]}
        self.assertEqual(positions[("segment", "seg-pre")], "pre")
        self.assertEqual(positions[("segment", "seg-at")], "at")
        self.assertEqual(positions[("segment", "seg-post")], "post")
        self.assertEqual(positions[("device_state", "dev-1")], "pre")
        self.assertEqual(positions[("medical_order", "ord-1")], "post")
        # 封存时同意范围被冻结
        self.assertTrue(event["consent_snapshot"]["research"])
        # 首轮裁决立即进入待临床阶段
        self.assertEqual(event["rounds"][0]["stage"], "await_clinical")
        self.assertEqual(event["rounds"][0]["round_no"], 1)

    def test_sealed_segments_default_quarantined_before_adjudication(self):
        self.seed_case()
        self.seal_event()
        task, _ = self.call(self.svc.create_analysis_task, {
            "kind": "research", "endpoint_id": "ep-primary",
            "creator": "analyst-1",
            "scope": {"session_ids": ["ss-1"], "kinds": ["brain_signal"]},
        })
        self.assertEqual(task["readset"]["readable"], [])
        self.assertEqual(
            task["readset"]["excluded"]["seg-pre"], "quarantined_pending_adjudication")

    def test_research_result_cannot_use_quarantined_segment(self):
        self.seed_case()
        self.seal_event()
        task, _ = self.call(self.svc.create_analysis_task, {
            "kind": "research", "endpoint_id": "ep-primary",
            "creator": "analyst-1", "scope": {"session_ids": ["ss-1"]},
        })
        with self.assertRaises(DomainError) as ctx:
            self.call(self.svc.record_analysis_result, {
                "task_id": task["task_id"], "endpoint_id": "ep-primary",
                "producer": "batch-1", "segment_ids": ["seg-pre"],
                "values": {"estimate": 0.12},
            })
        self.assertEqual(ctx.exception.code, "segment_not_readable")


class AdjudicationFlowTest(DomainTestBase):
    def test_stage_order_is_enforced(self):
        self.seed_case()
        self.seal_event()
        # 方法学不能早于临床
        with self.assertRaises(DomainError) as ctx:
            self.call(self.svc.submit_review, "ev-1", {
                "role": "methodology", "reviewer": "x", "signature": "s",
                "payload": {"segments": {
                    "seg-pre": {"interpretable": True},
                    "seg-at": {"interpretable": True},
                    "seg-post": {"interpretable": True}}}})
        self.assertEqual(ctx.exception.code, "stage_order")
        # 两阶段未完成，管理员不能应用
        with self.assertRaises(DomainError) as ctx:
            self.call(self.svc.apply_release, "ev-1", {"applied_by": "steward"})
        self.assertEqual(ctx.exception.code, "not_ready")

    def test_review_must_cover_every_segment_in_window(self):
        self.seed_case()
        self.seal_event()
        with self.assertRaises(DomainError) as ctx:
            self.call(self.svc.submit_review, "ev-1", {
                "role": "clinical_safety", "reviewer": "x", "signature": "s",
                "payload": {"segments": {"seg-pre": {"affected": False}}}})
        self.assertEqual(ctx.exception.code, "review_coverage")

    def test_full_flow_release_exclude_safety_only(self):
        self.seed_case()
        self.seal_event()
        self.reviews_consensus_and_dispute()
        # 存在分歧，未走伦理前不能应用
        with self.assertRaises(DomainError) as ctx:
            self.call(self.svc.apply_release, "ev-1", {"applied_by": "steward"})
        self.assertEqual(ctx.exception.code, "not_ready")
        event = self.svc.store.read(lambda c: self.svc.get_event(c, "ev-1"))
        self.assertEqual(event["rounds"][-1]["stage"], "await_ethics")

        version = self.ethics_and_apply()
        self.assertEqual(version["agreement"], "ethics_tiebreak")
        self.assertEqual(version["decisions"]["seg-pre"]["decision"], "release")
        self.assertEqual(version["decisions"]["seg-at"]["decision"], "exclude")
        self.assertEqual(version["decisions"]["seg-post"]["decision"], "safety_only")
        for seg_id, entry in version["decisions"].items():
            self.assertIn("basis", entry)

        # 研究任务只能读到 release
        research, _ = self.call(self.svc.create_analysis_task, {
            "kind": "research", "endpoint_id": "ep-primary",
            "creator": "analyst-1", "scope": {"session_ids": ["ss-1"]}})
        readable = {e["segment_id"] for e in research["readset"]["readable"]}
        self.assertEqual(readable, {"seg-pre"})
        self.assertEqual(research["readset"]["excluded"]["seg-at"],
                         "permanently_excluded")
        self.assertEqual(research["readset"]["excluded"]["seg-post"],
                         "safety_review_only")

        # 安全复盘可读 release 与 safety_only，但读不到永久排除
        safety, _ = self.call(self.svc.create_analysis_task, {
            "kind": "safety_review", "creator": "safety-officer",
            "scope": {"session_ids": ["ss-1"]}})
        safety_readable = {e["segment_id"] for e in safety["readset"]["readable"]}
        self.assertEqual(safety_readable, {"seg-pre", "seg-post"})

        # 研究结果可登记，且带版本血缘
        result, _ = self.call(self.svc.record_analysis_result, {
            "task_id": research["task_id"], "endpoint_id": "ep-primary",
            "producer": "batch-1", "segment_ids": ["seg-pre"],
            "values": {"estimate": 0.12}})
        self.assertEqual(result["segment_inputs"][0]["versions"]["ev-1"],
                         version["version_id"])

    def test_consensus_does_not_require_ethics(self):
        self.seed_case()
        self.seal_event()
        self.call(self.svc.submit_review, "ev-1", {
            "role": "clinical_safety", "reviewer": "c", "signature": "s",
            "payload": {"segments": {
                "seg-pre": {"affected": False}, "seg-at": {"affected": False},
                "seg-post": {"affected": False}}}})
        self.call(self.svc.submit_review, "ev-1", {
            "role": "methodology", "reviewer": "m", "signature": "s",
            "payload": {"segments": {
                "seg-pre": {"interpretable": True}, "seg-at": {"interpretable": True},
                "seg-post": {"interpretable": True}}}})
        version, replayed = self.call(
            self.svc.apply_release, "ev-1", {"applied_by": "steward"})
        self.assertFalse(replayed)
        self.assertEqual(version["agreement"], "consensus")

    def test_same_content_retries_are_safe(self):
        self.seed_case()
        self.seal_event()
        clinical = {
            "role": "clinical_safety", "reviewer": "dr-clinical",
            "signature": "sig/clinical/1",
            "payload": {"segments": {
                "seg-pre": {"affected": False}, "seg-at": {"affected": True},
                "seg-post": {"affected": True}}}}
        first, replayed1 = self.call(self.svc.submit_review, "ev-1", clinical)
        second, replayed2 = self.call(self.svc.submit_review, "ev-1", dict(clinical))
        self.assertFalse(replayed1)
        self.assertTrue(replayed2)
        self.assertEqual(first["review_id"], second["review_id"])

        # 同轮次同角色不同内容 → 冲突
        conflict = dict(clinical, signature="sig/clinical/forged")
        with self.assertRaises(DomainError) as ctx:
            self.call(self.svc.submit_review, "ev-1", conflict)
        self.assertEqual(ctx.exception.code, "review_conflict")

    def test_only_one_current_version_under_concurrency(self):
        self.seed_case()
        self.seal_event()
        self.reviews_consensus_and_dispute()
        self.ethics_and_apply()

        errors, versions = [], []

        def worker():
            try:
                v, _ = self.svc.store.write(
                    lambda c: self.svc.apply_release(c, "w", "ev-1",
                                                     {"applied_by": "steward"}))
                versions.append(v["version_id"])
            except DomainError as exc:
                errors.append(exc.code)

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        # 所有重试都收敛到同一个版本
        self.assertEqual(set(versions) - set(errors), set(versions))
        self.assertEqual(len(set(versions)), 1)
        current = self.svc.store.read(lambda c: c.execute(
            "SELECT COUNT(*) AS n FROM decision_versions"
            " WHERE event_id='ev-1' AND superseded_at IS NULL").fetchone()["n"])
        self.assertEqual(current, 1)


class RestartTest(DomainTestBase):
    def test_pending_work_resumes_after_restart(self):
        self.seed_case()
        self.seal_event()
        self.reviews_consensus_and_dispute()  # 停在 await_ethics

        # 模拟服务重启：用同一数据库文件新建存储与服务
        del self.svc
        restarted = AdjudicationService(Store(self.db_path))
        pending = restarted.store.read(restarted.list_pending)
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0]["event_id"], "ev-1")
        self.assertEqual(pending[0]["stage"], "await_ethics")

        def call(fn, *a):
            return restarted.store.write(lambda c: fn(c, self.actor, *a))

        call(restarted.submit_review, "ev-1", {
            "role": "ethics", "reviewer": "ethics-board", "signature": "s",
            "payload": {"segments": {
                "seg-post": {"decision": "safety_only"}}}})
        v, _ = call(restarted.apply_release, "ev-1", {"applied_by": "steward"})
        self.assertEqual(v["seq"], 1)
        self.assertEqual(restarted.store.read(restarted.list_pending), [])


class ClockCorrectionTest(DomainTestBase):
    def _v1(self):
        self.seed_case()
        self.seal_event()
        self.reviews_consensus_and_dispute()
        return self.ethics_and_apply()

    def test_correction_cannot_be_accepted_mid_adjudication(self):
        self.seed_case()
        self.seal_event()
        self.reviews_consensus_and_dispute()  # await_ethics
        cc, _ = self.call(self.svc.propose_clock_correction, "ev-1", {
            "correction_id": "cc-1", "delta": -1.5,
            "source": "device-clock-log", "basis": "刺激器时钟漂移",
            "impact_window": {"t_start": 13.5, "t_end": 28.5}})
        with self.assertRaises(DomainError) as ctx:
            self.call(self.svc.decide_clock_correction, "cc-1",
                      {"accept": True, "decided_by": "ethics-board"})
        self.assertEqual(ctx.exception.code, "adjudication_in_progress")

    def test_accepted_correction_opens_round_two_without_rewriting_v1(self):
        v1 = self._v1()

        # v1 之后的研究任务可读集被冻结在 v1
        task_before, _ = self.call(self.svc.create_analysis_task, {
            "kind": "research", "endpoint_id": "ep-primary",
            "creator": "analyst-1", "scope": {"session_ids": ["ss-1"]}})
        before_readable = {e["segment_id"] for e in task_before["readset"]["readable"]}
        self.assertEqual(before_readable, {"seg-pre"})

        # v1 之后登记、当时未封存的片段（迟到补传）：校正出现前在默认隔离之外
        self.call(self.svc.register_segment, {
            "segment_id": "seg-late", "session_id": "ss-1",
            "kind": "behavior", "t_start": 30.0, "t_end": 40.0,
            "data_ref": "s3://bucket/late", "checksum": "sha:late"})
        task_pre_cc, _ = self.call(self.svc.create_analysis_task, {
            "kind": "research", "endpoint_id": "ep-primary",
            "creator": "analyst-1b", "scope": {"session_ids": ["ss-1"]}})
        pre_cc_readable = {e["segment_id"] for e in task_pre_cc["readset"]["readable"]}
        self.assertEqual(pre_cc_readable, {"seg-pre", "seg-late"})

        self.call(self.svc.propose_clock_correction, "ev-1", {
            "correction_id": "cc-1", "delta": -1.5,
            "source": "device-clock-log", "basis": "刺激器时钟漂移",
            "impact_window": {"t_start": 25.0, "t_end": 35.0}})
        cc, _ = self.call(self.svc.decide_clock_correction, "cc-1",
                          {"accept": True, "decided_by": "ethics-board"})
        self.assertEqual(cc["status"], "accepted")
        self.assertEqual(cc["round_no"], 2)

        event = self.svc.store.read(lambda c: self.svc.get_event(c, "ev-1"))
        round2 = event["rounds"][-1]
        self.assertEqual(round2["stage"], "await_clinical")
        self.assertEqual(round2["triggered_by"], "clock_correction:cc-1")
        self.assertEqual(round2["proposed_window"],
                         {"t_start": 25.0, "t_end": 35.0})
        # 影响区间内的新片段被补封进入默认隔离；区间外（seg-pre/seg-at）不重审
        sealed_ids = {r["ref_id"] for r in event["sealed_records"]
                      if r["category"] == "segment"}
        self.assertIn("seg-late", sealed_ids)

        # 新轮待裁期间，seg-late 与区间内 seg-post 均不可读
        task_pending, _ = self.call(self.svc.create_analysis_task, {
            "kind": "research", "endpoint_id": "ep-primary",
            "creator": "analyst-1c", "scope": {"session_ids": ["ss-1"]}})
        self.assertEqual(
            {e["segment_id"] for e in task_pending["readset"]["readable"]},
            {"seg-pre"})
        self.assertEqual(task_pending["readset"]["excluded"]["seg-late"],
                         "quarantined_pending_adjudication")

        # 新轮只审议区间内片段：post 与 late
        self.call(self.svc.submit_review, "ev-1", {
            "role": "clinical_safety", "reviewer": "dr-clinical-2",
            "signature": "sig/c/2",
            "payload": {"segments": {
                "seg-post": {"affected": False, "note": "校正后落在安全窗口外"},
                "seg-late": {"affected": False, "note": "事件后很久"},
            }}})
        self.call(self.svc.submit_review, "ev-1", {
            "role": "methodology", "reviewer": "dr-method-2",
            "signature": "sig/m/2",
            "payload": {"segments": {
                "seg-post": {"interpretable": True},
                "seg-late": {"interpretable": True},
            }}})
        v2, replayed = self.call(self.svc.apply_release, "ev-1",
                                 {"applied_by": "steward-2"})
        self.assertFalse(replayed)
        self.assertEqual(v2["seq"], 2)
        self.assertEqual(v2["predecessor_id"], v1["version_id"])
        self.assertEqual(v2["decisions"]["seg-post"]["decision"], "release")
        self.assertEqual(v2["decisions"]["seg-late"]["decision"], "release")
        # 区间外决定沿用上一版本
        self.assertEqual(v2["decisions"]["seg-pre"]["decision"], "release")
        self.assertEqual(v2["decisions"]["seg-at"]["decision"], "exclude")
        self.assertEqual(v2["decisions"]["seg-at"]["basis"]["carried_from"],
                         v1["version_id"])

        # v1 未被静默改写：保留且标记被 v2 取代
        old = self.svc.store.read(lambda c: self.svc.get_event(c, "ev-1"))
        v1_rows = [v for v in old["versions"] if v["version_id"] == v1["version_id"]]
        self.assertEqual(len(v1_rows), 1)
        self.assertIsNotNone(v1_rows[0]["superseded_at"])
        self.assertEqual(v1_rows[0]["superseded_by"], v2["version_id"])

        # 旧任务快照不变；新任务读到 v2 的放行集
        task_after, _ = self.call(self.svc.create_analysis_task, {
            "kind": "research", "endpoint_id": "ep-primary",
            "creator": "analyst-2", "scope": {"session_ids": ["ss-1"]}})
        after_readable = {e["segment_id"] for e in task_after["readset"]["readable"]}
        self.assertEqual(after_readable, {"seg-pre", "seg-post", "seg-late"})

        # 校正记录指向新版本
        cc_row = self.svc.store.read(lambda c: c.execute(
            "SELECT * FROM clock_corrections WHERE correction_id='cc-1'").fetchone())
        self.assertEqual(cc_row["resulting_version_id"], v2["version_id"])

    def test_rejected_correction_leaves_current_version_untouched(self):
        v1 = self._v1()
        self.call(self.svc.propose_clock_correction, "ev-1", {
            "correction_id": "cc-r", "delta": 0.1,
            "source": "unverified", "basis": "传闻",
            "impact_window": {"t_start": 14.0, "t_end": 16.0}})
        cc, _ = self.call(self.svc.decide_clock_correction, "cc-r",
                          {"accept": False, "decided_by": "ethics-board"})
        self.assertEqual(cc["status"], "rejected")
        event = self.svc.store.read(lambda c: self.svc.get_event(c, "ev-1"))
        self.assertEqual(len(event["versions"]), 1)
        self.assertIsNone(event["versions"][0]["superseded_at"])


class WithdrawalTest(DomainTestBase):
    def test_withdrawal_stops_new_research_use_but_keeps_safety_records(self):
        self.seed_case()
        self.seal_event()
        self.reviews_consensus_and_dispute()
        self.ethics_and_apply()

        participant, _ = self.call(self.svc.withdraw_participant, "pt-1")
        self.assertTrue(participant["withdrawn"])

        # 重复撤回是幂等重放
        _, again = self.call(self.svc.withdraw_participant, "pt-1")
        self.assertTrue(again)

        # 不能开新会话
        with self.assertRaises(DomainError) as ctx:
            self.call(self.svc.start_session,
                      {"session_id": "ss-2", "participant_id": "pt-1",
                       "started_at": 99.0})
        self.assertEqual(ctx.exception.code, "participant_withdrawn")

        # 显式指定撤回参与者的研究任务被拒绝
        with self.assertRaises(DomainError) as ctx:
            self.call(self.svc.create_analysis_task, {
                "kind": "research", "endpoint_id": "ep",
                "creator": "a", "scope": {"participant_ids": ["pt-1"]}})
        self.assertEqual(ctx.exception.code, "participant_withdrawn")

        # 安全复盘照常：safety_only/release 片段仍可读
        safety, _ = self.call(self.svc.create_analysis_task, {
            "kind": "safety_review", "creator": "safety-officer",
            "scope": {"session_ids": ["ss-1"]}})
        safety_readable = {e["segment_id"] for e in safety["readset"]["readable"]}
        self.assertEqual(safety_readable, {"seg-pre", "seg-post"})
        result, _ = self.call(self.svc.record_analysis_result, {
            "task_id": safety["task_id"], "endpoint_id": "safety-audit",
            "producer": "safety-batch", "segment_ids": ["seg-post"],
            "values": {"review": "no-device-malfunction"}})
        self.assertTrue(result["result_id"])

        # 依法必须保存的安全记录与审计仍在
        event = self.svc.store.read(lambda c: self.svc.get_event(c, "ev-1"))
        self.assertEqual(event["adverse_effect"], "短暂运动性发作后自行缓解")
        audit = self.svc.store.read(
            lambda c: self.svc.audit_for(c, "participant", "pt-1"))
        actions = {a["action"] for a in audit}
        self.assertIn("participant_withdrawn", actions)


class AuditTraceTest(DomainTestBase):
    def test_result_traces_back_to_event_window_signatures_and_revisions(self):
        v1 = self._v1_with_release_post()
        # 登记研究结果
        task, _ = self.call(self.svc.create_analysis_task, {
            "kind": "research", "endpoint_id": "ep-primary",
            "creator": "analyst-1", "scope": {"session_ids": ["ss-1"]}})
        result, _ = self.call(self.svc.record_analysis_result, {
            "task_id": task["task_id"], "endpoint_id": "ep-primary",
            "producer": "batch-1", "segment_ids": ["seg-pre", "seg-post"],
            "values": {"estimate": 0.3}})

        # 时钟校正修订 seg-post（区间避开 seg-at 的边界 t_end=20）
        self.call(self.svc.propose_clock_correction, "ev-1", {
            "correction_id": "cc-1", "delta": -1.0, "source": "clk",
            "basis": "漂移", "impact_window": {"t_start": 21.0, "t_end": 30.0}})
        self.call(self.svc.decide_clock_correction, "cc-1",
                  {"accept": True, "decided_by": "board"})
        self.call(self.svc.submit_review, "ev-1", {
            "role": "clinical_safety", "reviewer": "c2", "signature": "s2",
            "payload": {"segments": {"seg-post": {"affected": True}}}})
        self.call(self.svc.submit_review, "ev-1", {
            "role": "methodology", "reviewer": "m2", "signature": "s2",
            "payload": {"segments": {"seg-post": {"interpretable": False}}}})
        self.call(self.svc.apply_release, "ev-1", {"applied_by": "steward2"})

        trace = self.svc.store.read(
            lambda c: self.svc.trace_result(c, result["result_id"]))
        self.assertEqual(trace["result"]["result_id"], result["result_id"])
        lineage = {x["segment_id"]: x for x in trace["segment_lineage"]}
        post_hist = lineage["seg-post"]["quarantine_history"][0]
        self.assertEqual(post_hist["event_id"], "ev-1")
        # 结果所依据的放行版本
        self.assertEqual(post_hist["releasing_version"]["version_id"], v1["version_id"])
        # 区间选择（两轮）与签署决定
        windows = [r["proposed_window"] for r in post_hist["rounds"]]
        self.assertEqual(windows[0], {"t_start": 0.0, "t_end": 30.0})
        self.assertEqual(windows[1], {"t_start": 21.0, "t_end": 30.0})
        roles = {r["role"] for r in post_hist["signed_reviews"]}
        self.assertEqual(roles, {"clinical_safety", "methodology"})
        for r in post_hist["signed_reviews"]:
            self.assertTrue(r["signature"])
        # 后来修订可追
        self.assertEqual(len(post_hist["later_revisions"]), 1)
        self.assertEqual(post_hist["later_revisions"][0]["decisions"]["seg-post"]
                         ["decision"], "exclude")
        self.assertEqual(len(post_hist["clock_corrections"]), 1)
        # 审计条目挂链
        self.assertTrue(trace["audit"])
        ok, broken = self.svc.store.read(self.svc.store.verify_chain)
        self.assertTrue(ok, f"审计链断裂于 {broken}")

    def _v1_with_release_post(self):
        self.seed_case()
        self.seal_event()
        self.call(self.svc.submit_review, "ev-1", {
            "role": "clinical_safety", "reviewer": "c", "signature": "s",
            "payload": {"segments": {
                "seg-pre": {"affected": False}, "seg-at": {"affected": True},
                "seg-post": {"affected": False}}}})
        self.call(self.svc.submit_review, "ev-1", {
            "role": "methodology", "reviewer": "m", "signature": "s",
            "payload": {"segments": {
                "seg-pre": {"interpretable": True},
                "seg-at": {"interpretable": False},
                "seg-post": {"interpretable": True}}}})
        v, _ = self.call(self.svc.apply_release, "ev-1",
                         {"applied_by": "steward"})
        return v


# ---------------------------------------------------------------------------
# HTTP 层测试：幂等键、并发、错误码
# ---------------------------------------------------------------------------

class HttpTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        svc = AdjudicationService(Store(os.path.join(cls.tmp.name, "http.db")))
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(svc))
        cls.svc = svc
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.base = f"http://127.0.0.1:{cls.server.server_port}"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(timeout=2)
        cls.tmp.cleanup()

    def request(self, method, path, body=None, headers=None, expect=None):
        data = json.dumps(body).encode("utf-8") if body is not None else None
        req = Request(self.base + path, data=data, method=method, headers={
            "Content-Type": "application/json", "X-Actor": "http-tester",
            **(headers or {})})
        try:
            with urlopen(req, timeout=5) as resp:
                payload = json.load(resp)
                if expect is not None:
                    self.assertEqual(resp.status, expect)
                return resp.status, payload, dict(resp.headers)
        except HTTPError as exc:
            payload = json.load(exc)
            if expect is not None:
                self.assertEqual(exc.code, expect)
            return exc.code, payload, dict(exc.headers)

    def test_idempotency_key_replays_same_response(self):
        body = {"participant_id": "pt-http",
                "consent_scope": {"research": True}}
        s1, b1, h1 = self.request("POST", "/participants", body,
                                  {"Idempotency-Key": "key-1"}, expect=201)
        s2, b2, h2 = self.request("POST", "/participants", body,
                                  {"Idempotency-Key": "key-1"}, expect=201)
        self.assertEqual(b1["participant_id"], b2["participant_id"])
        self.assertTrue(b2["idempotent_replay"])
        self.assertEqual(h2.get("Idempotent-Replay"), "true")

        # 同键不同路径 → 冲突
        status, payload, _ = self.request(
            "POST", "/sessions",
            {"session_id": "x", "participant_id": "pt-http", "started_at": 1.0},
            {"Idempotency-Key": "key-1"})
        self.assertEqual(status, 409)
        self.assertEqual(payload["error"]["code"], "idempotency_conflict")

    def test_concurrent_applications_return_single_version(self):
        # 准备：封存并完成两阶段（无分歧）
        self.request("POST", "/participants",
                     {"participant_id": "pt-c", "consent_scope": {"r": True}})
        self.request("POST", "/sessions",
                     {"session_id": "ss-c", "participant_id": "pt-c",
                      "started_at": 0.0})
        self.request("POST", "/segments", {
            "segment_id": "seg-c", "session_id": "ss-c", "kind": "stimulus",
            "t_start": 0.0, "t_end": 5.0,
            "data_ref": "d", "checksum": "c"})
        self.request("POST", "/safety-events", {
            "event_id": "ev-c", "session_id": "ss-c", "occurred_at": 4.0,
            "adverse_effect": "x", "timeline": {"clock": "master"}})
        clinical = {"role": "clinical_safety", "reviewer": "c", "signature": "s",
                    "payload": {"segments": {"seg-c": {"affected": False}}}}
        method = {"role": "methodology", "reviewer": "m", "signature": "s",
                  "payload": {"segments": {"seg-c": {"interpretable": True}}}}
        self.request("POST", "/safety-events/ev-c/reviews", clinical)
        self.request("POST", "/safety-events/ev-c/reviews", method)

        results = []

        def worker():
            status, payload, _ = self.request(
                "POST", "/safety-events/ev-c/apply", {"applied_by": "steward"})
            results.append((status, payload["version_id"]))

        threads = [threading.Thread(target=worker) for _ in range(6)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(len({vid for _, vid in results}), 1)
        self.assertTrue(all(s in (200, 201) for s, _ in results))

    def test_pending_and_trace_endpoints(self):
        status, payload, _ = self.request("GET", "/safety-events/pending")
        self.assertEqual(status, 200)
        self.assertIsInstance(payload["pending"], list)
        status, payload, _ = self.request("GET", "/audit/verify")
        self.assertTrue(payload["chain_intact"])
        status, payload, _ = self.request("GET", "/safety-events/missing")
        self.assertEqual(status, 404)
        self.assertEqual(payload["error"]["code"], "not_found")


if __name__ == "__main__":
    unittest.main()
