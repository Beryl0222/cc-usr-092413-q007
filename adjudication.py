"""临床中止数据解封裁决的领域核心。

职责边界：
- 安全事件发生时，按统一时间轴封存前后片段、设备状态、医嘱与当时同意范围；
- 已封存片段默认隔离，任何分析任务只能读到隔离之外（或经裁决解封）的数据；
- 临床安全人员先判定可能受治疗或异常状态影响的区间，研究方法人员再判定
  预注册终点是否仍可解释，两者齐备后数据管理员才能按最小范围解封、
  永久排除或仅允许安全复盘；
- 迟到的时钟校正只能提出影响区间并开启修订，不能越过已裁决版本静默改写；
- 参与者撤回后停止新的研究使用，但依法必须保存的安全记录仍可安全复盘；
- 并发裁决只产生一个当前版本，同内容重试安全，分歧进入伦理复核；
- 状态持久化到单个 JSON 文件，服务重启后待裁决工作继续；
- 审计查询可从任一分析结果追到中止事件、区间选择、签署决定和后来修订。
"""

from __future__ import annotations

import copy
import hashlib
import json
import os
import threading
import time

# ---- 角色 -----------------------------------------------------------------

ROLE_CLINICAL_SAFETY = "clinical_safety"        # 临床安全人员
ROLE_METHODOLOGY = "research_methodology"       # 研究方法人员
ROLE_DATA_ADMIN = "data_administrator"          # 数据管理员
ROLE_ETHICS = "ethics_coordinator"              # 伦理协调员
ROLE_DEVICE = "device_engineer"                 # 设备工程师（时钟校正）
ROLE_RESEARCH = "research_staff"                # 研究成员（登记与分析）

KNOWN_ROLES = frozenset({
    ROLE_CLINICAL_SAFETY,
    ROLE_METHODOLOGY,
    ROLE_DATA_ADMIN,
    ROLE_ETHICS,
    ROLE_DEVICE,
    ROLE_RESEARCH,
})

# ---- 片段类型与处置动作 ----------------------------------------------------

SEGMENT_KINDS = ("stimulation", "behavior", "neural")

ACTION_UNSEAL = "unseal"                        # 按最小范围解封
ACTION_EXCLUDE_PERMANENTLY = "exclude_permanently"  # 永久排除
ACTION_SAFETY_REVIEW_ONLY = "safety_review_only"    # 仅允许安全复盘
DISPOSITION_ACTIONS = (
    ACTION_UNSEAL,
    ACTION_EXCLUDE_PERMANENTLY,
    ACTION_SAFETY_REVIEW_ONLY,
)

PURPOSE_RESEARCH = "research"
PURPOSE_SAFETY_REVIEW = "safety_review"
PURPOSES = (PURPOSE_RESEARCH, PURPOSE_SAFETY_REVIEW)

# ---- 裁决案件状态 ----------------------------------------------------------

STATUS_SEALED = "sealed"                        # 已封存，待临床安全评估
STATUS_SAFETY_ASSESSED = "safety_assessed"      # 待研究方法评估
STATUS_METHODOLOGY_ASSESSED = "methodology_assessed"  # 待数据管理员处置
STATUS_ADJUDICATED = "adjudicated"              # 已有当前裁决版本
STATUS_REVISION_PENDING = "revision_pending"    # 已裁决但有待完成的修订

# ---- 分析排除原因 ----------------------------------------------------------

REASON_WITHDRAWN = "participant_withdrawn"
REASON_QUARANTINED = "quarantined_pending_adjudication"
REASON_EXCLUDED = "excluded_permanently"
REASON_SAFETY_ONLY = "safety_review_only"

_EXCLUDE_PRIORITY = (REASON_EXCLUDED, REASON_QUARANTINED, REASON_SAFETY_ONLY)


class ApiError(Exception):
    """携带 HTTP 状态码与稳定错误码的领域错误。"""

    def __init__(self, status, code, message, details=None):
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message
        self.details = details or {}


def now_ms():
    return int(time.time() * 1000)


def payload_hash(payload):
    """规范化 JSON 的内容指纹，用于同内容重试判定。"""
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _require_role(actor, allowed):
    if actor["role"] not in allowed:
        raise ApiError(
            403,
            "forbidden_role",
            f"角色 {actor['role']} 无权执行该操作",
            {"allowed": sorted(allowed)},
        )


def _require_str(payload, field):
    value = payload.get(field)
    if not isinstance(value, str) or not value.strip():
        raise ApiError(400, "invalid_field", f"{field} 必须是非空字符串")
    return value


def _require_int(payload, field, minimum=None):
    value = payload.get(field)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ApiError(400, "invalid_field", f"{field} 必须是整数")
    if minimum is not None and value < minimum:
        raise ApiError(400, "invalid_field", f"{field} 不能小于 {minimum}")
    return value


def _at(payload):
    """领域时间以统一时间轴毫秒为准；缺省取墙钟，测试可显式传入。"""
    value = payload.get("at_ms")
    if value is None:
        return now_ms()
    if isinstance(value, bool) or not isinstance(value, int):
        raise ApiError(400, "invalid_field", "at_ms 必须是整数")
    return value


def _validate_interval(payload, field):
    interval = payload.get(field)
    if not isinstance(interval, dict):
        raise ApiError(400, "invalid_field", f"{field} 必须是包含 start_ms/end_ms 的对象")
    start = _require_int(interval, "start_ms", 0)
    end = _require_int(interval, "end_ms", 0)
    if end <= start:
        raise ApiError(400, "invalid_field", f"{field} 的 end_ms 必须大于 start_ms")
    return {"start_ms": start, "end_ms": end}


def _overlaps(a_start, a_end, b_start, b_end):
    return a_start < b_end and b_start < a_end


class Store:
    """裁决状态的唯一持有者。

    所有状态迁移都在 ``self.lock`` 内完成并原子落盘，因此并发裁决只会
    产生一个当前版本；重启后从同一文件恢复，待裁决工作可继续。
    """

    def __init__(self, path=None):
        self.path = path
        self.lock = threading.RLock()
        self.state = self._empty_state()
        if path and os.path.exists(path):
            with open(path, "r", encoding="utf-8") as fh:
                self.state = json.load(fh)

    @staticmethod
    def _empty_state():
        return {
            "participants": {},
            "segments": {},
            "events": {},
            "clock_corrections": {},
            "analysis_results": {},
            "ethics_reviews": {},
            "requests": {},
            "audit_log": [],
            "counters": {},
        }

    # ---- 基础设施 ---------------------------------------------------------

    def _save_locked(self):
        if not self.path:
            return
        directory = os.path.dirname(os.path.abspath(self.path))
        os.makedirs(directory, exist_ok=True)
        tmp_path = self.path + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as fh:
            json.dump(self.state, fh, ensure_ascii=False, indent=1)
        os.replace(tmp_path, self.path)

    def _next_id_locked(self, prefix):
        counters = self.state["counters"]
        counters[prefix] = counters.get(prefix, 0) + 1
        return f"{prefix}-{counters[prefix]}"

    def _audit_locked(self, action, actor, entity_type, entity_id, detail, at_ms):
        entry = {
            "seq": len(self.state["audit_log"]) + 1,
            "at_ms": at_ms,
            "actor_id": actor["actor_id"],
            "role": actor["role"],
            "action": action,
            "entity_type": entity_type,
            "entity_id": entity_id,
            "detail": detail,
        }
        self.state["audit_log"].append(entry)
        return entry

    def _idempotent_locked(self, scope, payload, produce):
        """request_id 级幂等：同键同内容回放，同键不同内容报 409。"""
        request_id = payload.get("request_id")
        if request_id is None:
            return produce()
        if not isinstance(request_id, str) or not request_id:
            raise ApiError(400, "invalid_field", "request_id 必须是非空字符串")
        key = f"{scope}:{request_id}"
        digest = payload_hash(payload)
        existing = self.state["requests"].get(key)
        if existing is not None:
            if existing["hash"] != digest:
                raise ApiError(
                    409,
                    "request_id_conflict",
                    "同一 request_id 提交了不同内容",
                    {"request_id": request_id},
                )
            # 重试已成功的请求：内容一致，按 200 回放，不产生新效果。
            return 200, copy.deepcopy(existing["body"])
        status, body = produce()
        self.state["requests"][key] = {
            "hash": digest,
            "status": status,
            "body": copy.deepcopy(body),
        }
        return status, body

    # ---- 参与者与片段 ------------------------------------------------------

    def register_participant(self, actor, payload):
        _require_role(actor, {ROLE_RESEARCH, ROLE_CLINICAL_SAFETY, ROLE_DATA_ADMIN, ROLE_ETHICS})
        participant_id = _require_str(payload, "participant_id")
        consent = payload.get("consent_scope")
        if not isinstance(consent, dict) or not consent:
            raise ApiError(400, "invalid_consent_scope", "consent_scope 必须是非空对象")
        at = _at(payload)
        digest = payload_hash({"participant_id": participant_id, "consent_scope": consent})
        with self.lock:
            existing = self.state["participants"].get(participant_id)
            if existing is not None:
                if existing["content_hash"] == digest:
                    return 200, {"participant": self._participant_view(existing)}
                raise ApiError(
                    409,
                    "participant_conflict",
                    "参与者已存在且登记内容不同",
                    {"participant_id": participant_id},
                )
            participant = {
                "participant_id": participant_id,
                "consent_scope": copy.deepcopy(consent),
                "content_hash": digest,
                "registered_by": actor["actor_id"],
                "registered_at_ms": at,
                "withdrawal": None,
            }
            self.state["participants"][participant_id] = participant
            self._audit_locked(
                "participant_registered", actor, "participant", participant_id,
                {"consent_scope": consent}, at,
            )
            self._save_locked()
            return 201, {"participant": self._participant_view(participant)}

    def _participant_view(self, participant):
        view = copy.deepcopy(participant)
        view.pop("content_hash", None)
        return view

    def get_participant(self, actor, participant_id):
        _require_role(actor, KNOWN_ROLES)
        with self.lock:
            participant = self.state["participants"].get(participant_id)
            if participant is None:
                raise ApiError(404, "participant_not_found", "参与者不存在",
                               {"participant_id": participant_id})
            return 200, {"participant": self._participant_view(participant)}

    def withdraw_participant(self, actor, participant_id, payload):
        """撤回：停止新的研究使用，但安全记录依法保留、仍可安全复盘。"""
        _require_role(actor, {ROLE_ETHICS})
        reason = payload.get("reason", "")
        if not isinstance(reason, str):
            raise ApiError(400, "invalid_field", "reason 必须是字符串")
        at = _at(payload)
        # 撤回是事实记录：指纹不含时间，同内容重试安全。
        digest = payload_hash({"participant_id": participant_id, "reason": reason})
        with self.lock:
            participant = self.state["participants"].get(participant_id)
            if participant is None:
                raise ApiError(404, "participant_not_found", "参与者不存在",
                               {"participant_id": participant_id})
            existing = participant["withdrawal"]
            if existing is not None:
                if existing["content_hash"] == digest:
                    return 200, {"participant": self._participant_view(participant)}
                raise ApiError(
                    409,
                    "withdrawal_conflict",
                    "参与者已撤回，且本次提交与已记录撤回不一致",
                    {"participant_id": participant_id},
                )
            participant["withdrawal"] = {
                "withdrawn_at_ms": at,
                "recorded_by": actor["actor_id"],
                "reason": reason,
                "content_hash": digest,
            }
            self._audit_locked(
                "participant_withdrawn", actor, "participant", participant_id,
                {"reason": reason}, at,
            )
            self._save_locked()
            return 201, {"participant": self._participant_view(participant)}

    def ingest_segment(self, actor, payload):
        """登记统一时间轴上的数据片段；与已封存窗口重叠的片段会被一并封存。"""
        _require_role(actor, {ROLE_RESEARCH, ROLE_CLINICAL_SAFETY, ROLE_DATA_ADMIN})
        segment_id = _require_str(payload, "segment_id")
        participant_id = _require_str(payload, "participant_id")
        kind = payload.get("kind")
        if kind not in SEGMENT_KINDS:
            raise ApiError(400, "invalid_field",
                           f"kind 必须是 {sorted(SEGMENT_KINDS)} 之一")
        start = _require_int(payload, "start_ms", 0)
        end = _require_int(payload, "end_ms", 0)
        if end <= start:
            raise ApiError(400, "invalid_field", "end_ms 必须大于 start_ms")
        at = _at(payload)
        digest = payload_hash({
            "segment_id": segment_id, "participant_id": participant_id,
            "kind": kind, "start_ms": start, "end_ms": end,
        })
        with self.lock:
            participant = self.state["participants"].get(participant_id)
            if participant is None:
                raise ApiError(404, "participant_not_found", "参与者不存在",
                               {"participant_id": participant_id})
            existing = self.state["segments"].get(segment_id)
            if existing is not None:
                if existing["content_hash"] == digest:
                    return 200, {"segment": self._segment_view(existing)}
                raise ApiError(409, "segment_conflict", "片段已存在且内容不同",
                               {"segment_id": segment_id})
            segment = {
                "segment_id": segment_id,
                "participant_id": participant_id,
                "kind": kind,
                "start_ms": start,
                "end_ms": end,
                "content_hash": digest,
                "ingested_by": actor["actor_id"],
                "ingested_at_ms": at,
                "sealed_by": [],
            }
            self.state["segments"][segment_id] = segment
            self._audit_locked(
                "segment_ingested", actor, "segment", segment_id,
                {"participant_id": participant_id, "kind": kind,
                 "start_ms": start, "end_ms": end}, at,
            )
            # 迟到的片段若落入既有事件的封存窗口，按统一时间轴补封；
            # 已裁决事件因此进入修订，但当前版本保持不变。
            for event in self.state["events"].values():
                if event["participant_id"] != participant_id:
                    continue
                if not _overlaps(start, end, event["window_start_ms"], event["window_end_ms"]):
                    continue
                self._seal_segment_locked(event, segment)
                if event["status"] == STATUS_ADJUDICATED:
                    self._open_revision_locked(
                        event, actor, kind="late_segment", at_ms=at,
                        detail={
                            "reason": "迟到片段落入已裁决封存窗口",
                            "segment_id": segment_id,
                            "affected_interval": {"start_ms": start, "end_ms": end},
                        },
                    )
            self._save_locked()
            return 201, {"segment": self._segment_view(segment)}

    def _segment_view(self, segment):
        view = copy.deepcopy(segment)
        view.pop("content_hash", None)
        with self.lock:
            view["dispositions"] = {
                event_id: self._current_action_locked(event_id, segment["segment_id"])
                for event_id in segment["sealed_by"]
            }
        return view

    def _current_action_locked(self, event_id, segment_id):
        event = self.state["events"][event_id]
        if event["current_version"] is None:
            return None
        version = event["versions"][event["current_version"] - 1]
        return version["dispositions"].get(segment_id)

    def get_segment(self, actor, segment_id):
        _require_role(actor, KNOWN_ROLES)
        with self.lock:
            segment = self.state["segments"].get(segment_id)
            if segment is None:
                raise ApiError(404, "segment_not_found", "片段不存在",
                               {"segment_id": segment_id})
            return 200, {"segment": self._segment_view(segment)}

    # ---- 安全事件封存 ------------------------------------------------------

    def create_suspension_event(self, actor, payload):
        """安全事件发生：封存前后片段、设备状态、医嘱与当时同意范围。"""
        _require_role(actor, {ROLE_CLINICAL_SAFETY, ROLE_ETHICS})
        participant_id = _require_str(payload, "participant_id")
        occurred_at = _require_int(payload, "occurred_at_ms", 0)
        pre_window = _require_int(payload, "pre_window_ms", 0)
        post_window = _require_int(payload, "post_window_ms", 0)
        device_status = payload.get("device_status")
        if not isinstance(device_status, dict):
            raise ApiError(400, "invalid_field", "device_status 必须是对象")
        medical_orders = payload.get("medical_orders")
        if not isinstance(medical_orders, list):
            raise ApiError(400, "invalid_field", "medical_orders 必须是数组")
        reason = payload.get("reason", "")
        if not isinstance(reason, str):
            raise ApiError(400, "invalid_field", "reason 必须是字符串")
        at = _at(payload)
        with self.lock:
            participant = self.state["participants"].get(participant_id)
            if participant is None:
                raise ApiError(404, "participant_not_found", "参与者不存在",
                               {"participant_id": participant_id})

            def produce():
                event_id = self._next_id_locked("E")
                event = {
                    "event_id": event_id,
                    "participant_id": participant_id,
                    "occurred_at_ms": occurred_at,
                    "pre_window_ms": pre_window,
                    "post_window_ms": post_window,
                    "window_start_ms": occurred_at - pre_window,
                    "window_end_ms": occurred_at + post_window,
                    "device_status": copy.deepcopy(device_status),
                    "medical_orders": copy.deepcopy(medical_orders),
                    # 当时的同意范围随事件一并封存，之后不可更改。
                    "consent_scope": copy.deepcopy(participant["consent_scope"]),
                    "reason": reason,
                    "status": STATUS_SEALED,
                    "sealed_segment_ids": [],
                    "versions": [],
                    "current_version": None,
                    "working": {"version": 1, "safety": None, "methodology": None},
                    "revision_proposals": [],
                    "divergences": [],
                    "receipts": {},
                    "created_by": actor["actor_id"],
                    "created_at_ms": at,
                }
                self.state["events"][event_id] = event
                for segment in self.state["segments"].values():
                    if segment["participant_id"] != participant_id:
                        continue
                    if _overlaps(segment["start_ms"], segment["end_ms"],
                                 event["window_start_ms"], event["window_end_ms"]):
                        self._seal_segment_locked(event, segment)
                self._audit_locked(
                    "suspension_event_sealed", actor, "suspension_event", event_id,
                    {"participant_id": participant_id,
                     "window_start_ms": event["window_start_ms"],
                     "window_end_ms": event["window_end_ms"],
                     "sealed_segment_ids": list(event["sealed_segment_ids"])}, at,
                )
                self._save_locked()
                return 201, {"event": copy.deepcopy(event)}

            return self._idempotent_locked("create_event", payload, produce)

    def _seal_segment_locked(self, event, segment):
        if segment["segment_id"] not in event["sealed_segment_ids"]:
            event["sealed_segment_ids"].append(segment["segment_id"])
        if event["event_id"] not in segment["sealed_by"]:
            segment["sealed_by"].append(event["event_id"])

    def get_event(self, actor, event_id):
        _require_role(actor, KNOWN_ROLES)
        with self.lock:
            event = self.state["events"].get(event_id)
            if event is None:
                raise ApiError(404, "event_not_found", "中止事件不存在",
                               {"event_id": event_id})
            return 200, {"event": copy.deepcopy(event)}

    # ---- 两阶段评估与处置 --------------------------------------------------

    def submit_safety_assessment(self, actor, event_id, payload):
        """第一步：临床安全人员判定可能受治疗或异常状态影响的区间。"""
        _require_role(actor, {ROLE_CLINICAL_SAFETY})
        intervals = payload.get("affected_intervals")
        if not isinstance(intervals, list):
            raise ApiError(400, "invalid_field", "affected_intervals 必须是数组")
        normalized = []
        for item in intervals:
            if not isinstance(item, dict):
                raise ApiError(400, "invalid_field", "affected_intervals 元素必须是对象")
            interval = _validate_interval(item, "interval") if "interval" in item else {
                "start_ms": _require_int(item, "start_ms", 0),
                "end_ms": _require_int(item, "end_ms", 0),
            }
            if interval["end_ms"] <= interval["start_ms"]:
                raise ApiError(400, "invalid_field", "区间 end_ms 必须大于 start_ms")
            normalized.append({
                "start_ms": interval["start_ms"],
                "end_ms": interval["end_ms"],
                "reason": item.get("reason", ""),
            })
        rationale = _require_str(payload, "rationale")
        at = _at(payload)
        content = {"affected_intervals": normalized, "rationale": rationale}
        with self.lock:
            event = self._event_or_404(event_id)
            receipt_key = f"safety:v{event['working']['version']}"
            replay = self._replay_locked(event, receipt_key, content)
            if replay is not None:
                return replay
            if event["working"]["safety"] is None and event["status"] in (
                    STATUS_SEALED, STATUS_REVISION_PENDING):
                assessment = {
                    "affected_intervals": normalized,
                    "rationale": rationale,
                    "assessed_by": actor["actor_id"],
                    "assessed_role": actor["role"],
                    "assessed_at_ms": at,
                    "version": event["working"]["version"],
                }
                event["working"]["safety"] = assessment
                if event["status"] == STATUS_SEALED:
                    event["status"] = STATUS_SAFETY_ASSESSED
                self._audit_locked(
                    "safety_assessment_recorded", actor, "suspension_event", event_id,
                    {"version": assessment["version"],
                     "affected_intervals": normalized}, at,
                )
                body = {"event_id": event_id, "status": event["status"],
                        "safety_assessment": copy.deepcopy(assessment)}
                self._record_receipt_locked(event, receipt_key, content, 201, body)
                self._save_locked()
                return 201, body
            self._diverge_locked(event, "safety_assessment", actor, content, at)

    def submit_methodology_assessment(self, actor, event_id, payload):
        """第二步：研究方法人员判定预注册终点是否仍可解释。"""
        _require_role(actor, {ROLE_METHODOLOGY})
        endpoints = payload.get("endpoint_interpretability")
        if not isinstance(endpoints, dict) or not endpoints:
            raise ApiError(400, "invalid_field",
                           "endpoint_interpretability 必须是非空对象")
        for name, interpretable in endpoints.items():
            if not isinstance(name, str) or not isinstance(interpretable, bool):
                raise ApiError(400, "invalid_field",
                               "endpoint_interpretability 的键值必须是 终点名: 布尔")
        rationale = _require_str(payload, "rationale")
        at = _at(payload)
        content = {"endpoint_interpretability": endpoints, "rationale": rationale}
        with self.lock:
            event = self._event_or_404(event_id)
            receipt_key = f"methodology:v{event['working']['version']}"
            replay = self._replay_locked(event, receipt_key, content)
            if replay is not None:
                return replay
            if event["working"]["safety"] is None and event["status"] != STATUS_ADJUDICATED:
                raise ApiError(
                    409, "invalid_state",
                    "需先完成临床安全评估，再进行研究方法评估",
                    {"event_id": event_id, "status": event["status"]},
                )
            if event["working"]["methodology"] is None and event["status"] in (
                    STATUS_SAFETY_ASSESSED, STATUS_REVISION_PENDING):
                assessment = {
                    "endpoint_interpretability": copy.deepcopy(endpoints),
                    "rationale": rationale,
                    "assessed_by": actor["actor_id"],
                    "assessed_role": actor["role"],
                    "assessed_at_ms": at,
                    "version": event["working"]["version"],
                }
                event["working"]["methodology"] = assessment
                if event["status"] == STATUS_SAFETY_ASSESSED:
                    event["status"] = STATUS_METHODOLOGY_ASSESSED
                self._audit_locked(
                    "methodology_assessment_recorded", actor, "suspension_event",
                    event_id,
                    {"version": assessment["version"],
                     "endpoint_interpretability": endpoints}, at,
                )
                body = {"event_id": event_id, "status": event["status"],
                        "methodology_assessment": copy.deepcopy(assessment)}
                self._record_receipt_locked(event, receipt_key, content, 201, body)
                self._save_locked()
                return 201, body
            self._diverge_locked(event, "methodology_assessment", actor, content, at)

    def submit_disposition(self, actor, event_id, payload):
        """第三步：数据管理员按最小范围解封、永久排除或仅允许安全复盘。

        两阶段评估齐备后才能处置；expected_version 提供乐观并发控制，
        并发处置只产生一个当前版本，分歧进入伦理复核。
        """
        _require_role(actor, {ROLE_DATA_ADMIN})
        dispositions = payload.get("dispositions")
        if not isinstance(dispositions, dict) or not dispositions:
            raise ApiError(400, "invalid_field", "dispositions 必须是非空对象")
        for segment_id, action in dispositions.items():
            if action not in DISPOSITION_ACTIONS:
                raise ApiError(400, "invalid_field",
                               f"片段 {segment_id} 的处置必须是 "
                               f"{sorted(DISPOSITION_ACTIONS)} 之一")
        rationale = _require_str(payload, "rationale")
        expected = payload.get("expected_version")
        if isinstance(expected, bool) or not isinstance(expected, int) or expected < 0:
            raise ApiError(400, "invalid_field",
                           "expected_version 必须是非负整数（当前版本号，未裁决时为 0）")
        at = _at(payload)
        content = {"dispositions": dispositions, "rationale": rationale,
                   "expected_version": expected}
        with self.lock:
            event = self._event_or_404(event_id)
            for key, receipt in event["receipts"].items():
                if key.startswith("disposition:") and receipt["hash"] == payload_hash(content):
                    return 200, copy.deepcopy(receipt["body"])
            current = event["current_version"] or 0
            ready = (event["working"]["safety"] is not None
                     and event["working"]["methodology"] is not None
                     and event["status"] in (
                         STATUS_METHODOLOGY_ASSESSED, STATUS_REVISION_PENDING))
            if not ready:
                if event["status"] == STATUS_ADJUDICATED:
                    self._diverge_locked(event, "disposition", actor, content, at)
                raise ApiError(
                    409, "invalid_state",
                    "需依次完成临床安全评估与研究方法评估后才能处置",
                    {"event_id": event_id, "status": event["status"]},
                )
            if expected != current:
                self._diverge_locked(
                    event, "disposition", actor, content, at,
                    detail={"expected_version": expected, "current_version": current},
                )
            sealed = set(event["sealed_segment_ids"])
            decided = set(dispositions)
            if decided != sealed:
                raise ApiError(
                    422, "incomplete_disposition",
                    "处置必须恰好覆盖全部封存片段（最小范围逐一决定）",
                    {"missing": sorted(sealed - decided),
                     "unexpected": sorted(decided - sealed)},
                )
            version_no = current + 1
            open_proposals = [p for p in event["revision_proposals"]
                              if p["status"] == "open"]
            version = {
                "version": version_no,
                "dispositions": dict(dispositions),
                "rationale": rationale,
                "signed_by": actor["actor_id"],
                "signed_role": actor["role"],
                "signed_at_ms": at,
                "safety_assessment": copy.deepcopy(event["working"]["safety"]),
                "methodology_assessment": copy.deepcopy(event["working"]["methodology"]),
                "incorporates": [p["proposal_id"] for p in open_proposals],
            }
            event["versions"].append(version)
            event["current_version"] = version_no
            for proposal in open_proposals:
                proposal["status"] = "incorporated"
                proposal["incorporated_in_version"] = version_no
            event["working"] = {"version": version_no + 1,
                                "safety": None, "methodology": None}
            event["status"] = STATUS_ADJUDICATED
            self._audit_locked(
                "disposition_finalized", actor, "suspension_event", event_id,
                {"version": version_no, "dispositions": dict(dispositions),
                 "incorporates": version["incorporates"]}, at,
            )
            body = {"event_id": event_id, "status": event["status"],
                    "current_version": version_no,
                    "version": copy.deepcopy(version)}
            self._record_receipt_locked(
                event, f"disposition:v{version_no}", content, 201, body)
            self._save_locked()
            return 201, body

    # ---- 评估/处置的幂等与分歧 ---------------------------------------------

    def _replay_locked(self, event, receipt_key, content):
        receipt = event["receipts"].get(receipt_key)
        if receipt is not None and receipt["hash"] == payload_hash(content):
            return 200, copy.deepcopy(receipt["body"])
        return None

    def _record_receipt_locked(self, event, receipt_key, content, status, body):
        event["receipts"][receipt_key] = {
            "hash": payload_hash(content),
            "status": status,
            "body": copy.deepcopy(body),
        }

    def _diverge_locked(self, event, step, actor, content, at_ms, detail=None):
        """分歧不覆盖已记录结论，转入伦理复核。"""
        review_id = self._next_id_locked("ER")
        divergence = {
            "step": step,
            "actor_id": actor["actor_id"],
            "role": actor["role"],
            "content": copy.deepcopy(content),
            "content_hash": payload_hash(content),
            "at_ms": at_ms,
            "detail": detail or {},
            "ethics_review_id": review_id,
        }
        event["divergences"].append(divergence)
        review = {
            "review_id": review_id,
            "event_id": event["event_id"],
            "kind": "adjudication_divergence",
            "step": step,
            "status": "open",
            "opened_at_ms": at_ms,
            "divergence": copy.deepcopy(divergence),
            "resolution": None,
        }
        self.state["ethics_reviews"][review_id] = review
        self._audit_locked(
            "divergence_recorded", actor, "suspension_event", event["event_id"],
            {"step": step, "ethics_review_id": review_id}, at_ms,
        )
        self._audit_locked(
            "ethics_review_opened", actor, "ethics_review", review_id,
            {"event_id": event["event_id"], "step": step}, at_ms,
        )
        self._save_locked()
        raise ApiError(
            409, "divergence_recorded",
            "与已记录结论存在分歧，已转入伦理复核",
            {"ethics_review_id": review_id, "event_id": event["event_id"]},
        )

    # ---- 时钟校正与修订 ------------------------------------------------------

    def propose_clock_correction(self, actor, payload):
        """迟到的时钟校正只提出影响区间并开启修订，不改写已裁决版本。"""
        _require_role(actor, {ROLE_DEVICE, ROLE_CLINICAL_SAFETY})
        participant_id = _require_str(payload, "participant_id")
        interval = _validate_interval(payload, "affected_interval")
        reason = _require_str(payload, "reason")
        shift_ms = payload.get("shift_ms")
        if shift_ms is not None and (isinstance(shift_ms, bool)
                                     or not isinstance(shift_ms, int)):
            raise ApiError(400, "invalid_field", "shift_ms 必须是整数")
        at = _at(payload)
        with self.lock:
            if participant_id not in self.state["participants"]:
                raise ApiError(404, "participant_not_found", "参与者不存在",
                               {"participant_id": participant_id})

            def produce():
                correction_id = self._next_id_locked("C")
                correction = {
                    "correction_id": correction_id,
                    "participant_id": participant_id,
                    "affected_interval": interval,
                    "shift_ms": shift_ms,
                    "reason": reason,
                    "proposed_by": actor["actor_id"],
                    "proposed_at_ms": at,
                }
                self.state["clock_corrections"][correction_id] = correction
                self._audit_locked(
                    "clock_correction_proposed", actor, "clock_correction",
                    correction_id,
                    {"participant_id": participant_id,
                     "affected_interval": interval, "shift_ms": shift_ms}, at,
                )
                affected = []
                for event in self.state["events"].values():
                    if event["participant_id"] != participant_id:
                        continue
                    if not _overlaps(interval["start_ms"], interval["end_ms"],
                                     event["window_start_ms"], event["window_end_ms"]):
                        continue
                    affected.append(event["event_id"])
                    self._open_revision_locked(
                        event, actor, kind="clock_correction", at_ms=at,
                        detail={"reason": reason,
                                "correction_id": correction_id,
                                "affected_interval": interval},
                    )
                self._save_locked()
                return 201, {"correction": copy.deepcopy(correction),
                             "affected_event_ids": affected}

            return self._idempotent_locked("clock_correction", payload, produce)

    def _open_revision_locked(self, event, actor, kind, at_ms, detail):
        """在已裁决事件上开启修订：当前版本保持有效，新版本走完整两阶段流程。"""
        proposal = {
            "proposal_id": self._next_id_locked("P"),
            "kind": kind,
            "status": "open",
            "proposed_by": actor["actor_id"],
            "proposed_at_ms": at_ms,
            "detail": copy.deepcopy(detail),
        }
        event["revision_proposals"].append(proposal)
        if event["status"] == STATUS_ADJUDICATED:
            event["status"] = STATUS_REVISION_PENDING
            self._audit_locked(
                "revision_opened", actor, "suspension_event", event["event_id"],
                {"proposal_id": proposal["proposal_id"], "kind": kind,
                 "current_version": event["current_version"]}, at_ms,
            )
        return proposal

    # ---- 分析任务与读取隔离 --------------------------------------------------

    def run_analysis(self, actor, payload):
        """执行分析任务：只读到默认隔离之外（或经裁决解封）的数据。"""
        purpose = payload.get("purpose", PURPOSE_RESEARCH)
        if purpose not in PURPOSES:
            raise ApiError(400, "invalid_field",
                           f"purpose 必须是 {sorted(PURPOSES)} 之一")
        if purpose == PURPOSE_RESEARCH:
            _require_role(actor, {ROLE_RESEARCH, ROLE_METHODOLOGY, ROLE_DATA_ADMIN})
        else:
            _require_role(actor, {ROLE_CLINICAL_SAFETY, ROLE_ETHICS, ROLE_DATA_ADMIN})
        participant_id = _require_str(payload, "participant_id")
        start = _require_int(payload, "start_ms", 0)
        end = _require_int(payload, "end_ms", 0)
        if end <= start:
            raise ApiError(400, "invalid_field", "end_ms 必须大于 start_ms")
        kinds = payload.get("kinds")
        if kinds is not None:
            if (not isinstance(kinds, list)
                    or any(k not in SEGMENT_KINDS for k in kinds)):
                raise ApiError(400, "invalid_field",
                               f"kinds 必须是 {sorted(SEGMENT_KINDS)} 的子集")
        at = _at(payload)
        with self.lock:
            participant = self.state["participants"].get(participant_id)
            if participant is None:
                raise ApiError(404, "participant_not_found", "参与者不存在",
                               {"participant_id": participant_id})

            def produce():
                included, excluded, lineage = [], [], {}
                for segment in sorted(self.state["segments"].values(),
                                      key=lambda s: (s["start_ms"], s["segment_id"])):
                    if segment["participant_id"] != participant_id:
                        continue
                    if not _overlaps(segment["start_ms"], segment["end_ms"], start, end):
                        continue
                    if kinds is not None and segment["kind"] not in kinds:
                        continue
                    access = self._segment_access_locked(segment, participant, purpose)
                    if access["allowed"]:
                        included.append(segment["segment_id"])
                        if access["refs"]:
                            lineage[segment["segment_id"]] = access["refs"]
                    else:
                        excluded.append({
                            "segment_id": segment["segment_id"],
                            "reason": access["reason"],
                            "blockers": access["blockers"],
                        })
                result_id = self._next_id_locked("AR")
                result = {
                    "analysis_result_id": result_id,
                    "participant_id": participant_id,
                    "purpose": purpose,
                    "start_ms": start,
                    "end_ms": end,
                    "kinds": kinds,
                    "included": included,
                    "excluded": excluded,
                    "lineage": lineage,
                    "created_by": actor["actor_id"],
                    "created_at_ms": at,
                }
                self.state["analysis_results"][result_id] = result
                self._audit_locked(
                    "analysis_executed", actor, "analysis_result", result_id,
                    {"participant_id": participant_id, "purpose": purpose,
                     "included": included,
                     "excluded": [e["segment_id"] for e in excluded]}, at,
                )
                self._save_locked()
                return 201, {"analysis_result": copy.deepcopy(result)}

            return self._idempotent_locked("analysis", payload, produce)

    def _segment_access_locked(self, segment, participant, purpose):
        """计算片段对指定用途的可读性。

        研究用途：撤回即全量停止；封存片段须每个封存事件的当前版本都解封。
        安全复盘：安全记录依法保留，仅“永久排除”不可读。
        """
        refs, blockers = [], []
        for event_id in segment["sealed_by"]:
            event = self.state["events"][event_id]
            if event["current_version"] is None:
                blockers.append({"event_id": event_id, "reason": REASON_QUARANTINED})
                continue
            version = event["versions"][event["current_version"] - 1]
            action = version["dispositions"].get(segment["segment_id"])
            if action is None:
                blockers.append({"event_id": event_id, "reason": REASON_QUARANTINED})
            elif action == ACTION_EXCLUDE_PERMANENTLY:
                blockers.append({"event_id": event_id, "reason": REASON_EXCLUDED})
            elif action == ACTION_SAFETY_REVIEW_ONLY:
                blockers.append({"event_id": event_id, "reason": REASON_SAFETY_ONLY})
            else:
                refs.append({"event_id": event_id,
                             "version": event["current_version"]})
        if purpose == PURPOSE_SAFETY_REVIEW:
            hard = [b for b in blockers if b["reason"] == REASON_EXCLUDED]
            if hard:
                return {"allowed": False, "reason": REASON_EXCLUDED,
                        "blockers": hard, "refs": refs}
            return {"allowed": True, "reason": None, "blockers": [], "refs": refs}
        if participant["withdrawal"] is not None:
            return {"allowed": False, "reason": REASON_WITHDRAWN,
                    "blockers": [], "refs": refs}
        if blockers:
            reason = next(r for r in _EXCLUDE_PRIORITY
                          if any(b["reason"] == r for b in blockers))
            return {"allowed": False, "reason": reason,
                    "blockers": blockers, "refs": refs}
        return {"allowed": True, "reason": None, "blockers": [], "refs": refs}

    def get_analysis_result(self, actor, result_id):
        _require_role(actor, KNOWN_ROLES)
        with self.lock:
            result = self.state["analysis_results"].get(result_id)
            if result is None:
                raise ApiError(404, "analysis_result_not_found", "分析结果不存在",
                               {"analysis_result_id": result_id})
            return 200, {"analysis_result": copy.deepcopy(result)}

    # ---- 审计追溯 ------------------------------------------------------------

    def trace_analysis_result(self, actor, result_id):
        """从分析结果追到中止事件、区间选择、签署决定和后来修订。"""
        _require_role(actor, KNOWN_ROLES)
        with self.lock:
            result = self.state["analysis_results"].get(result_id)
            if result is None:
                raise ApiError(404, "analysis_result_not_found", "分析结果不存在",
                               {"analysis_result_id": result_id})
            references = []
            for segment_id, refs in result["lineage"].items():
                for ref in refs:
                    references.append((segment_id, ref["event_id"], ref["version"], True))
            for excluded in result["excluded"]:
                for blocker in excluded["blockers"]:
                    event = self.state["events"][blocker["event_id"]]
                    references.append((excluded["segment_id"], blocker["event_id"],
                                       event["current_version"], False))
            traces = []
            involved_event_ids = set()
            for segment_id, event_id, version_no, was_included in references:
                event = self.state["events"][event_id]
                involved_event_ids.add(event_id)
                version = (event["versions"][version_no - 1]
                           if version_no is not None else None)
                traces.append({
                    "segment_id": segment_id,
                    "included": was_included,
                    "event_id": event_id,
                    "event": {
                        "participant_id": event["participant_id"],
                        "occurred_at_ms": event["occurred_at_ms"],
                        "window_start_ms": event["window_start_ms"],
                        "window_end_ms": event["window_end_ms"],
                        "status": event["status"],
                        "current_version": event["current_version"],
                    },
                    "adjudication_version": version_no,
                    "interval_selection": (
                        copy.deepcopy(version["safety_assessment"]["affected_intervals"])
                        if version else None),
                    "signed_decisions": self._signed_decisions(version),
                    "later_revisions": [
                        self._version_summary(v) for v in event["versions"]
                        if version_no is not None and v["version"] > version_no
                    ],
                    "open_revision_proposals": [
                        copy.deepcopy(p) for p in event["revision_proposals"]
                        if p["status"] == "open"
                    ],
                    "divergences": copy.deepcopy(event["divergences"]),
                })
            participant = self.state["participants"][result["participant_id"]]
            audit_entries = [
                copy.deepcopy(e) for e in self.state["audit_log"]
                if e["entity_id"] in involved_event_ids | {result_id}
            ]
            return 200, {
                "analysis_result": copy.deepcopy(result),
                "participant_withdrawal": copy.deepcopy(participant["withdrawal"]),
                "traces": traces,
                "audit_entries": audit_entries,
            }

    def _signed_decisions(self, version):
        if version is None:
            return []
        decisions = []
        safety = version["safety_assessment"]
        decisions.append({
            "step": "safety_assessment",
            "signed_by": safety["assessed_by"],
            "signed_role": safety["assessed_role"],
            "signed_at_ms": safety["assessed_at_ms"],
        })
        methodology = version["methodology_assessment"]
        decisions.append({
            "step": "methodology_assessment",
            "signed_by": methodology["assessed_by"],
            "signed_role": methodology["assessed_role"],
            "signed_at_ms": methodology["assessed_at_ms"],
        })
        decisions.append({
            "step": "disposition",
            "signed_by": version["signed_by"],
            "signed_role": version["signed_role"],
            "signed_at_ms": version["signed_at_ms"],
        })
        return decisions

    def _version_summary(self, version):
        return {
            "version": version["version"],
            "dispositions": copy.deepcopy(version["dispositions"]),
            "signed_by": version["signed_by"],
            "signed_at_ms": version["signed_at_ms"],
            "incorporates": list(version["incorporates"]),
        }

    def trace_suspension_event(self, actor, event_id):
        """事件视角的审计：封存快照、全部版本、修订提议、分歧与日志。"""
        _require_role(actor, KNOWN_ROLES)
        with self.lock:
            event = self.state["events"].get(event_id)
            if event is None:
                raise ApiError(404, "event_not_found", "中止事件不存在",
                               {"event_id": event_id})
            entries = [copy.deepcopy(e) for e in self.state["audit_log"]
                       if e["entity_id"] == event_id]
            return 200, {"event": copy.deepcopy(event), "audit_entries": entries}

    # ---- 待裁决队列与伦理复核 --------------------------------------------------

    def adjudication_queue(self, actor):
        """待裁决工作清单；重启后据此继续。"""
        _require_role(actor, KNOWN_ROLES)
        with self.lock:
            queue = {
                "needs_safety_assessment": [],
                "needs_methodology_assessment": [],
                "needs_disposition": [],
                "needs_revision": [],
            }
            for event in self.state["events"].values():
                if event["status"] == STATUS_SEALED:
                    queue["needs_safety_assessment"].append(event["event_id"])
                elif event["status"] == STATUS_SAFETY_ASSESSED:
                    queue["needs_methodology_assessment"].append(event["event_id"])
                elif event["status"] == STATUS_METHODOLOGY_ASSESSED:
                    queue["needs_disposition"].append(event["event_id"])
                elif event["status"] == STATUS_REVISION_PENDING:
                    working = event["working"]
                    step = ("safety_assessment" if working["safety"] is None
                            else "methodology_assessment"
                            if working["methodology"] is None else "disposition")
                    queue["needs_revision"].append({
                        "event_id": event["event_id"],
                        "awaiting": step,
                        "current_version": event["current_version"],
                    })
            open_reviews = [r["review_id"] for r in
                            self.state["ethics_reviews"].values()
                            if r["status"] == "open"]
            return 200, {"queue": queue, "open_ethics_reviews": open_reviews}

    def list_ethics_reviews(self, actor, status=None):
        _require_role(actor, KNOWN_ROLES)
        with self.lock:
            reviews = [copy.deepcopy(r) for r in
                       self.state["ethics_reviews"].values()
                       if status is None or r["status"] == status]
            reviews.sort(key=lambda r: r["review_id"])
            return 200, {"ethics_reviews": reviews}

    def resolve_ethics_review(self, actor, review_id, payload):
        """伦理复核结论：驳回分歧，或在已裁决事件上开启修订。"""
        _require_role(actor, {ROLE_ETHICS})
        decision = payload.get("decision")
        if decision not in ("dismiss", "open_revision"):
            raise ApiError(400, "invalid_field",
                           "decision 必须是 dismiss 或 open_revision")
        note = payload.get("note", "")
        if not isinstance(note, str):
            raise ApiError(400, "invalid_field", "note 必须是字符串")
        at = _at(payload)
        with self.lock:
            review = self.state["ethics_reviews"].get(review_id)
            if review is None:
                raise ApiError(404, "ethics_review_not_found", "伦理复核不存在",
                               {"review_id": review_id})
            if review["status"] != "open":
                raise ApiError(409, "invalid_state", "该伦理复核已有结论",
                               {"review_id": review_id, "status": review["status"]})
            event = self.state["events"][review["event_id"]]
            if decision == "open_revision":
                if event["status"] != STATUS_ADJUDICATED:
                    raise ApiError(
                        422, "invalid_state",
                        "仅已裁决事件可因伦理复核开启修订",
                        {"event_id": event["event_id"], "status": event["status"]},
                    )
                self._open_revision_locked(
                    event, actor, kind="ethics_review", at_ms=at,
                    detail={"reason": note, "review_id": review_id},
                )
            review["status"] = "resolved"
            review["resolution"] = {
                "decision": decision,
                "note": note,
                "resolved_by": actor["actor_id"],
                "resolved_at_ms": at,
            }
            self._audit_locked(
                "ethics_review_resolved", actor, "ethics_review", review_id,
                {"decision": decision, "event_id": event["event_id"]}, at,
            )
            self._save_locked()
            return 200, {"ethics_review": copy.deepcopy(review)}

    # ---- 内部工具 --------------------------------------------------------------

    def _event_or_404(self, event_id):
        event = self.state["events"].get(event_id)
        if event is None:
            raise ApiError(404, "event_not_found", "中止事件不存在",
                           {"event_id": event_id})
        return event
