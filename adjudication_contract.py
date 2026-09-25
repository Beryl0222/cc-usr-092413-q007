"""验证临床中止数据解封裁决的领域行为。

覆盖：封存与默认隔离、两阶段裁决顺序与角色、最小范围解封、同内容重试、
分歧进入伦理复核、时钟校正不改写已裁决版本、并发单版本、参与者撤回、
重启后待裁决工作继续、审计追溯与迟到片段补封。
"""

import json
import os
import shutil
import tempfile
import threading
import unittest
from http.server import ThreadingHTTPServer
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from adjudication import Store
from service import make_handler

SAFETY = "clinical_safety"
METHOD = "research_methodology"
ADMIN = "data_administrator"
ETHICS = "ethics_coordinator"
DEVICE = "device_engineer"
RESEARCH = "research_staff"


class AdjudicationApiTest(unittest.TestCase):
    """每个用例一套独立状态文件与 HTTP 服务。"""

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="adjudication-test-")
        self.state_path = os.path.join(self.tmp, "state.json")
        self.store = Store(self.state_path)
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(self.store))
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base = f"http://127.0.0.1:{self.server.server_port}"

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)
        shutil.rmtree(self.tmp, ignore_errors=True)

    # ---- 基础工具 -----------------------------------------------------------

    def call(self, method, path, body=None, role=RESEARCH, actor="tester"):
        data = None if body is None else json.dumps(body).encode("utf-8")
        request = Request(self.base + path, data=data, method=method)
        request.add_header("X-Actor-Role", role)
        request.add_header("X-Actor-Id", actor)
        if data is not None:
            request.add_header("Content-Type", "application/json")
        try:
            with urlopen(request, timeout=5) as response:
                return response.status, json.load(response)
        except HTTPError as error:
            payload = json.loads(error.read().decode("utf-8"))
            error.close()
            return error.code, payload

    def register_participant(self, pid="P-1"):
        status, body = self.call("POST", "/participants", {
            "participant_id": pid,
            "consent_scope": {"uses": ["research", "safety"], "version": 3},
            "at_ms": 10,
        })
        self.assertEqual(status, 201, body)
        return body["participant"]

    def ingest_segment(self, segment_id, start, end, kind="neural", pid="P-1"):
        status, body = self.call("POST", "/segments", {
            "segment_id": segment_id, "participant_id": pid, "kind": kind,
            "start_ms": start, "end_ms": end, "at_ms": 20,
        })
        self.assertEqual(status, 201, body)
        return body["segment"]

    def seal_event(self, pid="P-1", occurred=1500, pre=200, post=200):
        status, body = self.call("POST", "/suspension-events", {
            "participant_id": pid, "occurred_at_ms": occurred,
            "pre_window_ms": pre, "post_window_ms": post,
            "device_status": {"amplifier": "ok", "stimulator": "halted"},
            "medical_orders": [{"order": "stop-stimulation", "at_ms": occurred}],
            "reason": "短暂不良反应", "at_ms": 30,
        }, role=SAFETY, actor="safety-1")
        self.assertEqual(status, 201, body)
        return body["event"]

    def standard_case(self):
        """三个片段，中间一个落入封存窗口。"""
        self.register_participant()
        self.ingest_segment("S-pre", 0, 1000, "stimulation")
        self.ingest_segment("S-mid", 1000, 2000, "neural")
        self.ingest_segment("S-post", 2000, 3000, "behavior")
        return self.seal_event()

    def assess_safety(self, event_id, intervals=None, **overrides):
        payload = {
            "affected_intervals": intervals
            if intervals is not None else
            [{"start_ms": 1300, "end_ms": 1700, "reason": "可能受停药影响"}],
            "rationale": "安全区间判定", "at_ms": 40,
        }
        payload.update(overrides)
        return self.call("POST", f"/suspension-events/{event_id}/safety-assessment",
                         payload, role=SAFETY, actor="safety-1")

    def assess_methodology(self, event_id, interpretable=True, **overrides):
        payload = {
            "endpoint_interpretability": {"primary": interpretable},
            "rationale": "终点可解释性判定", "at_ms": 50,
        }
        payload.update(overrides)
        return self.call("POST",
                         f"/suspension-events/{event_id}/methodology-assessment",
                         payload, role=METHOD, actor="method-1")

    def dispose(self, event_id, dispositions, expected=0, **overrides):
        payload = {
            "dispositions": dispositions,
            "expected_version": expected,
            "rationale": "按最小范围处置", "at_ms": 60,
        }
        payload.update(overrides)
        return self.call("POST", f"/suspension-events/{event_id}/dispositions",
                         payload, role=ADMIN, actor="admin-1")

    def adjudicate(self, event_id, dispositions, expected=0):
        status, body = self.assess_safety(event_id)
        self.assertEqual(status, 201, body)
        status, body = self.assess_methodology(event_id)
        self.assertEqual(status, 201, body)
        status, body = self.dispose(event_id, dispositions, expected)
        self.assertEqual(status, 201, body)
        return body

    def analyze(self, pid="P-1", purpose="research", **overrides):
        payload = {"participant_id": pid, "start_ms": 0, "end_ms": 3000,
                   "purpose": purpose, "at_ms": 70}
        payload.update(overrides)
        role = RESEARCH if purpose == "research" else SAFETY
        return self.call("POST", "/analysis-tasks", payload, role=role)

    # ---- 封存与默认隔离 -------------------------------------------------------

    def test_sealing_snapshots_context_and_quarantines_by_default(self):
        event = self.standard_case()
        self.assertEqual(event["status"], "sealed")
        self.assertEqual(event["sealed_segment_ids"], ["S-mid"])
        # 封存快照包含设备状态、医嘱与当时同意范围
        self.assertEqual(event["device_status"]["stimulator"], "halted")
        self.assertEqual(event["medical_orders"][0]["order"], "stop-stimulation")
        self.assertEqual(event["consent_scope"], {"uses": ["research", "safety"],
                                                  "version": 3})
        # 默认隔离：分析任务只读到隔离之外的片段
        status, body = self.analyze()
        self.assertEqual(status, 201, body)
        result = body["analysis_result"]
        self.assertEqual(result["included"], ["S-pre", "S-post"])
        self.assertEqual([e["segment_id"] for e in result["excluded"]], ["S-mid"])
        self.assertEqual(result["excluded"][0]["reason"],
                         "quarantined_pending_adjudication")

    def test_analysis_requires_known_role_and_actor(self):
        self.standard_case()
        request = Request(self.base + "/analysis-tasks", method="POST",
                          data=json.dumps({}).encode("utf-8"))
        with self.assertRaises(HTTPError) as ctx:
            urlopen(request, timeout=5)
        self.assertEqual(ctx.exception.code, 403)
        ctx.exception.close()
        status, body = self.call("POST", "/analysis-tasks",
                                 {"participant_id": "P-1", "start_ms": 0,
                                  "end_ms": 10}, role="not-a-role")
        self.assertEqual(status, 403)
        self.assertEqual(body["error"]["code"], "unknown_role")

    # ---- 两阶段顺序、角色与最小范围处置 -----------------------------------------

    def test_assessment_order_and_roles_are_enforced(self):
        event = self.standard_case()
        event_id = event["event_id"]
        # 方法评估不能先于安全评估
        status, body = self.assess_methodology(event_id)
        self.assertEqual(status, 409)
        self.assertEqual(body["error"]["code"], "invalid_state")
        # 角色校验
        status, body = self.call(
            "POST", f"/suspension-events/{event_id}/safety-assessment",
            {"affected_intervals": [], "rationale": "x", "at_ms": 40},
            role=METHOD, actor="method-1")
        self.assertEqual(status, 403)
        self.assertEqual(body["error"]["code"], "forbidden_role")
        # 处置不能先于两阶段评估
        status, body = self.dispose(event_id, {"S-mid": "unseal"})
        self.assertEqual(status, 409)
        self.assertEqual(body["error"]["code"], "invalid_state")
        # 正常顺序
        status, _ = self.assess_safety(event_id)
        self.assertEqual(status, 201)
        status, body = self.dispose(event_id, {"S-mid": "unseal"})
        self.assertEqual(status, 409)  # 仍缺方法评估
        status, _ = self.assess_methodology(event_id)
        self.assertEqual(status, 201)
        status, body = self.call(
            "POST", f"/suspension-events/{event_id}/dispositions",
            {"dispositions": {"S-mid": "unseal"}, "expected_version": 0,
             "rationale": "x", "at_ms": 60}, role=SAFETY, actor="safety-1")
        self.assertEqual(status, 403)  # 仅数据管理员可处置

    def test_disposition_must_cover_all_sealed_segments(self):
        event = self.standard_case()
        event_id = event["event_id"]
        self.assess_safety(event_id)
        self.assess_methodology(event_id)
        status, body = self.dispose(event_id, {"S-mid": "unseal",
                                               "S-other": "unseal"})
        self.assertEqual(status, 422)
        self.assertEqual(body["error"]["code"], "incomplete_disposition")
        self.assertEqual(body["error"]["details"]["unexpected"], ["S-other"])

    def test_three_disposition_actions_drive_analysis_access(self):
        self.register_participant()
        self.ingest_segment("S-a", 1100, 1400, "neural")
        self.ingest_segment("S-b", 1400, 1600, "neural")
        self.ingest_segment("S-c", 1600, 1900, "neural")
        event = self.seal_event()
        event_id = event["event_id"]
        self.assertEqual(sorted(event["sealed_segment_ids"]),
                         ["S-a", "S-b", "S-c"])
        self.adjudicate(event_id, {
            "S-a": "unseal",
            "S-b": "safety_review_only",
            "S-c": "exclude_permanently",
        })
        # 研究用途：仅解封片段可读，并带裁决血缘
        status, body = self.analyze()
        result = body["analysis_result"]
        self.assertEqual(result["included"], ["S-a"])
        self.assertEqual(result["lineage"],
                         {"S-a": [{"event_id": event_id, "version": 1}]})
        excluded = {e["segment_id"]: e["reason"] for e in result["excluded"]}
        self.assertEqual(excluded, {"S-b": "safety_review_only",
                                    "S-c": "excluded_permanently"})
        # 安全复盘：仅安全复盘片段可读，永久排除仍不可读
        status, body = self.analyze(purpose="safety_review")
        result = body["analysis_result"]
        self.assertEqual(result["included"], ["S-a", "S-b"])
        self.assertEqual([e["segment_id"] for e in result["excluded"]], ["S-c"])

    # ---- 同内容重试与分歧 -------------------------------------------------------

    def test_same_content_retry_is_idempotent(self):
        event = self.standard_case()
        event_id = event["event_id"]
        status, first = self.assess_safety(event_id)
        self.assertEqual(status, 201)
        status, replay = self.assess_safety(event_id)
        self.assertEqual(status, 200)
        self.assertEqual(first, replay)
        # request_id 级幂等（创建类端点）
        payload = {"participant_id": "P-1", "start_ms": 0, "end_ms": 3000,
                   "purpose": "research", "request_id": "req-1", "at_ms": 70}
        status, first = self.call("POST", "/analysis-tasks", payload)
        self.assertEqual(status, 201)
        status, replay = self.call("POST", "/analysis-tasks", payload)
        self.assertEqual(status, 200)
        self.assertEqual(first, replay)
        # 同 request_id 不同内容 → 409
        payload["end_ms"] = 3001
        status, body = self.call("POST", "/analysis-tasks", payload)
        self.assertEqual(status, 409)
        self.assertEqual(body["error"]["code"], "request_id_conflict")

    def test_disposition_retry_does_not_create_new_version(self):
        event = self.standard_case()
        event_id = event["event_id"]
        self.assess_safety(event_id)
        self.assess_methodology(event_id)
        status, first = self.dispose(event_id, {"S-mid": "unseal"})
        self.assertEqual(status, 201)
        status, replay = self.dispose(event_id, {"S-mid": "unseal"})
        self.assertEqual(status, 200)
        self.assertEqual(first, replay)
        status, body = self.call("GET", f"/suspension-events/{event_id}")
        self.assertEqual(body["event"]["current_version"], 1)
        self.assertEqual(len(body["event"]["versions"]), 1)

    def test_divergence_goes_to_ethics_review_and_can_be_dismissed(self):
        event = self.standard_case()
        event_id = event["event_id"]
        self.assess_safety(event_id)
        # 不同的安全评估结论 → 分歧 → 伦理复核
        status, body = self.assess_safety(
            event_id, intervals=[{"start_ms": 0, "end_ms": 3000,
                                  "reason": "另一意见"}])
        self.assertEqual(status, 409)
        self.assertEqual(body["error"]["code"], "divergence_recorded")
        review_id = body["error"]["details"]["ethics_review_id"]
        status, body = self.call("GET", "/ethics-reviews?status=open")
        self.assertEqual([r["review_id"] for r in body["ethics_reviews"]],
                         [review_id])
        # 伦理复核驳回分歧，原评估继续生效
        status, body = self.call("POST", f"/ethics-reviews/{review_id}/resolve",
                                 {"decision": "dismiss", "note": "维持原判",
                                  "at_ms": 55}, role=ETHICS, actor="ethics-1")
        self.assertEqual(status, 200)
        self.assertEqual(body["ethics_review"]["status"], "resolved")
        # 流程继续：首份安全评估仍然有效
        status, _ = self.assess_methodology(event_id)
        self.assertEqual(status, 201)
        status, body = self.dispose(event_id, {"S-mid": "unseal"})
        self.assertEqual(status, 201)

    # ---- 时钟校正与修订 ---------------------------------------------------------

    def test_clock_correction_proposes_revision_without_rewriting(self):
        event = self.standard_case()
        event_id = event["event_id"]
        self.adjudicate(event_id, {"S-mid": "unseal"})
        # 迟到的时钟校正：提出影响区间
        status, body = self.call("POST", "/clock-corrections", {
            "participant_id": "P-1",
            "affected_interval": {"start_ms": 1200, "end_ms": 1800},
            "shift_ms": -35, "reason": "设备时钟漂移", "at_ms": 80,
        }, role=DEVICE, actor="dev-1")
        self.assertEqual(status, 201, body)
        self.assertEqual(body["affected_event_ids"], [event_id])
        # 已裁决版本不被静默改写：当前版本仍是 v1，访问不变
        status, body = self.call("GET", f"/suspension-events/{event_id}")
        event_view = body["event"]
        self.assertEqual(event_view["status"], "revision_pending")
        self.assertEqual(event_view["current_version"], 1)
        self.assertEqual(len(event_view["versions"]), 1)
        self.assertEqual(len(event_view["revision_proposals"]), 1)
        status, body = self.analyze()
        self.assertEqual(body["analysis_result"]["included"],
                         ["S-pre", "S-mid", "S-post"])
        # 修订仍需完整两阶段流程，落为 v2
        self.adjudicate(event_id, {"S-mid": "safety_review_only"}, expected=1)
        status, body = self.call("GET", f"/suspension-events/{event_id}")
        event_view = body["event"]
        self.assertEqual(event_view["current_version"], 2)
        self.assertEqual(len(event_view["versions"]), 2)
        self.assertEqual(event_view["versions"][0]["dispositions"],
                         {"S-mid": "unseal"})  # v1 原样保留
        self.assertEqual(event_view["revision_proposals"][0]["status"],
                         "incorporated")
        # 新访问按 v2 生效
        status, body = self.analyze()
        self.assertEqual(body["analysis_result"]["included"],
                         ["S-pre", "S-post"])

    def test_version_conflict_goes_to_ethics_review(self):
        event = self.standard_case()
        event_id = event["event_id"]
        self.adjudicate(event_id, {"S-mid": "unseal"})
        # 基于过期版本的处置 → 分歧 → 伦理复核
        status, body = self.call("POST", "/clock-corrections", {
            "participant_id": "P-1",
            "affected_interval": {"start_ms": 1200, "end_ms": 1800},
            "reason": "时钟漂移", "at_ms": 80,
        }, role=DEVICE, actor="dev-1")
        self.assertEqual(status, 201)
        self.assess_safety(event_id)
        self.assess_methodology(event_id)
        # 内容不同但基于过期版本号 → 并发分歧 → 伦理复核
        status, body = self.dispose(event_id, {"S-mid": "exclude_permanently"},
                                    expected=0)
        self.assertEqual(status, 409)
        self.assertEqual(body["error"]["code"], "divergence_recorded")
        # 正确版本号可完成修订
        status, body = self.dispose(event_id, {"S-mid": "unseal"}, expected=1)
        self.assertEqual(status, 201)
        self.assertEqual(body["current_version"], 2)

    def test_concurrent_dispositions_yield_single_current_version(self):
        event = self.standard_case()
        event_id = event["event_id"]
        self.assess_safety(event_id)
        self.assess_methodology(event_id)
        outcomes = []

        def attempt(action, tag):
            outcomes.append(self.dispose(
                event_id, {"S-mid": action}, expected=0,
                rationale=f"并发处置 {tag}"))

        threads = [threading.Thread(target=attempt, args=(action, i))
                   for i, action in enumerate(
                       ["unseal", "exclude_permanently", "safety_review_only",
                        "unseal", "exclude_permanently"])]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=5)
        successes = [o for o in outcomes if o[0] == 201]
        conflicts = [o for o in outcomes if o[0] == 409]
        self.assertEqual(len(successes), 1, outcomes)
        self.assertEqual(len(conflicts), len(outcomes) - 1, outcomes)
        for _, body in conflicts:
            self.assertEqual(body["error"]["code"], "divergence_recorded")
        status, body = self.call("GET", f"/suspension-events/{event_id}")
        self.assertEqual(body["event"]["current_version"], 1)
        self.assertEqual(len(body["event"]["versions"]), 1)
        # 每个分歧都进入伦理复核
        status, body = self.call("GET", "/ethics-reviews?status=open")
        self.assertEqual(len(body["ethics_reviews"]), len(conflicts))

    # ---- 参与者撤回 ---------------------------------------------------------------

    def test_withdrawal_stops_research_but_keeps_safety_records(self):
        event = self.standard_case()
        event_id = event["event_id"]
        self.adjudicate(event_id, {"S-mid": "unseal"})
        status, before = self.analyze()
        self.assertEqual(len(before["analysis_result"]["included"]), 3)
        # 撤回
        status, body = self.call("POST", "/participants/P-1/withdrawal",
                                 {"reason": "参与者撤回同意", "at_ms": 90},
                                 role=ETHICS, actor="ethics-1")
        self.assertEqual(status, 201)
        self.assertIsNotNone(body["participant"]["withdrawal"])
        # 同内容重试安全
        status, replay = self.call("POST", "/participants/P-1/withdrawal",
                                   {"reason": "参与者撤回同意", "at_ms": 90},
                                   role=ETHICS, actor="ethics-1")
        self.assertEqual(status, 200)
        # 新的研究使用被停止
        status, body = self.analyze()
        result = body["analysis_result"]
        self.assertEqual(result["included"], [])
        self.assertTrue(all(e["reason"] == "participant_withdrawn"
                            for e in result["excluded"]))
        # 依法必须保存的安全记录仍可安全复盘
        status, body = self.analyze(purpose="safety_review")
        self.assertEqual(len(body["analysis_result"]["included"]), 3)
        # 撤回前的分析结果仍可追溯（历史不被改写）
        result_id = before["analysis_result"]["analysis_result_id"]
        status, body = self.call("GET", f"/audit/analysis-results/{result_id}")
        self.assertEqual(status, 200)
        self.assertIsNotNone(body["participant_withdrawal"])
        self.assertEqual(body["analysis_result"]["included"],
                         ["S-pre", "S-mid", "S-post"])

    # ---- 重启恢复 -----------------------------------------------------------------

    def test_restart_continues_pending_adjudication(self):
        event = self.standard_case()
        event_id = event["event_id"]
        self.assess_safety(event_id)
        # 模拟重启：同一状态文件打开新 Store 与新服务
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)
        reopened = Store(self.state_path)
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(reopened))
        self.thread = threading.Thread(target=self.server.serve_forever,
                                       daemon=True)
        self.thread.start()
        self.base = f"http://127.0.0.1:{self.server.server_port}"
        # 待裁决工作仍在队列中
        status, body = self.call("GET", "/adjudication-queue")
        self.assertEqual(body["queue"]["needs_methodology_assessment"],
                         [event_id])
        # 流程可继续走完
        status, _ = self.assess_methodology(event_id)
        self.assertEqual(status, 201)
        status, body = self.dispose(event_id, {"S-mid": "unseal"})
        self.assertEqual(status, 201)
        status, body = self.call("GET", "/adjudication-queue")
        self.assertEqual(body["queue"]["needs_disposition"], [])

    # ---- 审计追溯 -----------------------------------------------------------------

    def test_audit_traces_result_to_event_decisions_and_revisions(self):
        event = self.standard_case()
        event_id = event["event_id"]
        self.adjudicate(event_id, {"S-mid": "unseal"})
        status, body = self.analyze()
        result_id = body["analysis_result"]["analysis_result_id"]
        # 之后发生修订（v2），审计应能从 v1 的结果看到后来修订
        self.call("POST", "/clock-corrections", {
            "participant_id": "P-1",
            "affected_interval": {"start_ms": 1200, "end_ms": 1800},
            "reason": "时钟漂移", "at_ms": 80,
        }, role=DEVICE, actor="dev-1")
        self.adjudicate(event_id, {"S-mid": "exclude_permanently"}, expected=1)
        status, body = self.call("GET", f"/audit/analysis-results/{result_id}")
        self.assertEqual(status, 200, body)
        trace = next(t for t in body["traces"] if t["segment_id"] == "S-mid")
        # 追到中止事件
        self.assertEqual(trace["event_id"], event_id)
        self.assertEqual(trace["event"]["occurred_at_ms"], 1500)
        # 区间选择（安全评估）
        self.assertEqual(trace["interval_selection"],
                         [{"start_ms": 1300, "end_ms": 1700,
                           "reason": "可能受停药影响"}])
        # 签署决定：安全、方法、处置三方签署
        signers = {d["step"]: d["signed_by"] for d in trace["signed_decisions"]}
        self.assertEqual(signers, {"safety_assessment": "safety-1",
                                   "methodology_assessment": "method-1",
                                   "disposition": "admin-1"})
        # 后来修订
        self.assertEqual([v["version"] for v in trace["later_revisions"]], [2])
        self.assertEqual(trace["later_revisions"][0]["dispositions"],
                         {"S-mid": "exclude_permanently"})
        # 审计日志覆盖封存与历次裁决
        actions = [e["action"] for e in body["audit_entries"]]
        for expected in ("suspension_event_sealed", "safety_assessment_recorded",
                         "methodology_assessment_recorded",
                         "disposition_finalized", "analysis_executed"):
            self.assertIn(expected, actions)
        # 事件视角审计同样可用
        status, body = self.call("GET", f"/audit/suspension-events/{event_id}")
        self.assertEqual(status, 200)
        self.assertEqual(len(body["event"]["versions"]), 2)

    # ---- 迟到片段补封 ---------------------------------------------------------------

    def test_late_segment_reopens_adjudicated_event(self):
        event = self.standard_case()
        event_id = event["event_id"]
        self.adjudicate(event_id, {"S-mid": "unseal"})
        # 迟到片段落入封存窗口 → 补封并重开修订，当前版本不变
        segment = self.ingest_segment("S-late", 1400, 1600, "neural")
        self.assertEqual(segment["sealed_by"], [event_id])
        status, body = self.call("GET", f"/suspension-events/{event_id}")
        self.assertEqual(body["event"]["status"], "revision_pending")
        self.assertEqual(body["event"]["current_version"], 1)
        # 迟到片段默认隔离
        status, body = self.analyze()
        excluded = {e["segment_id"]: e["reason"]
                    for e in body["analysis_result"]["excluded"]}
        self.assertEqual(excluded.get("S-late"),
                         "quarantined_pending_adjudication")
        # 修订处置须覆盖全部封存片段（含迟到片段）
        self.assess_safety(event_id)
        self.assess_methodology(event_id)
        status, body = self.dispose(
            event_id, {"S-mid": "unseal", "S-late": "unseal"}, expected=1)
        self.assertEqual(status, 201)
        status, body = self.analyze()
        self.assertEqual(body["analysis_result"]["included"],
                         ["S-pre", "S-mid", "S-late", "S-post"])


if __name__ == "__main__":
    unittest.main()
