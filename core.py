"""中止后数据隔离与解封裁决的领域核心。

不变量：

* 封存即隔离：安全事件封存的所有片段默认隔离，任何分析任务只能
  读到默认隔离之外、或经当前裁决版本明确放行的数据。
* 裁决顺序：临床安全 → 方法学 →（分歧时）伦理 → 数据管理员应用。
* 版本只增不改：时钟校正只能开启新裁决轮产生新版本，旧版本保留。
* 每个事件任一时刻至多一个当前版本（由存储层部分唯一索引保证）。
"""

import hashlib
import json
import time
import uuid

from storage import Store

ROLES = ("clinical_safety", "methodology", "ethics")
DECISIONS = ("release", "exclude", "safety_only")
SEGMENT_KINDS = ("stimulus", "behavior", "brain_signal")


class DomainError(Exception):
    """携带 HTTP 状态与稳定错误码的领域错误。"""

    def __init__(self, status, code, message):
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message


def _uid(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


def _now():
    return time.time()


def _canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _scope_hash(decisions):
    return hashlib.sha256(_canonical(decisions).encode("utf-8")).hexdigest()


def _overlap(a_start, a_end, b_start, b_end):
    return a_start <= b_end and a_end >= b_start


def _adjudicate(affected, interpretable):
    """双阶段结论矩阵；返回 (决定或 None, 是否分歧)。"""
    if not affected and interpretable:
        return "release", False
    if affected and not interpretable:
        return "exclude", False
    return None, True


class AdjudicationService:
    def __init__(self, store: Store):
        self.store = store

    # ------------------------------------------------------------------
    # 通用小工具
    # ------------------------------------------------------------------

    def _get(self, conn, table, key, label):
        row = conn.execute(
            f"SELECT * FROM {table} WHERE {label}=?", (key,)
        ).fetchone()
        if row is None:
            raise DomainError(404, "not_found", f"{table} 不存在: {key}")
        return row

    def _audit(self, conn, actor, action, entity_type, entity_id, payload):
        return self.store.append_audit(
            conn, actor, action, entity_type, entity_id, payload
        )

    @staticmethod
    def _same_content(existing_row, fields, payload):
        return all(existing_row[f] == payload.get(f) for f in fields)

    # ------------------------------------------------------------------
    # 登记：参与者 / 会话 / 片段 / 设备状态 / 医嘱
    # ------------------------------------------------------------------

    def register_participant(self, conn, actor, p):
        participant_id = p.get("participant_id") or _uid("pt")
        consent_scope = p.get("consent_scope")
        if not isinstance(consent_scope, dict) or not consent_scope:
            raise DomainError(422, "invalid_consent", "consent_scope 必须是非空对象")
        existing = conn.execute(
            "SELECT * FROM participants WHERE participant_id=?", (participant_id,)
        ).fetchone()
        if existing:
            if json.loads(existing["consent_scope"]) == consent_scope:
                return self._participant_view(existing), True
            raise DomainError(409, "conflict", "participant_id 已存在且内容不同")
        conn.execute(
            "INSERT INTO participants (participant_id, enrolled_at, consent_scope, meta)"
            " VALUES (?,?,?,?)",
            (
                participant_id,
                _now(),
                Store.dumps(consent_scope),
                Store.dumps(p.get("meta") or {}),
            ),
        )
        self._audit(
            conn, actor, "participant_registered", "participant", participant_id,
            {"consent_scope": consent_scope},
        )
        row = self._get(conn, "participants", participant_id, "participant_id")
        return self._participant_view(row), False

    def _participant_view(self, row):
        return {
            "participant_id": row["participant_id"],
            "enrolled_at": row["enrolled_at"],
            "consent_scope": json.loads(row["consent_scope"]),
            "withdrawn": bool(row["withdrawn"]),
            "withdrawn_at": row["withdrawn_at"],
            "meta": json.loads(row["meta"]),
        }

    def withdraw_participant(self, conn, actor, participant_id):
        row = self._get(conn, "participants", participant_id, "participant_id")
        if row["withdrawn"]:
            return self._participant_view(row), True
        conn.execute(
            "UPDATE participants SET withdrawn=1, withdrawn_at=? WHERE participant_id=?",
            (_now(), participant_id),
        )
        self._audit(
            conn, actor, "participant_withdrawn", "participant", participant_id,
            {"note": "停止新的研究使用；依法必须保存的安全记录保留"},
        )
        row = self._get(conn, "participants", participant_id, "participant_id")
        return self._participant_view(row), False

    def start_session(self, conn, actor, p):
        session_id = p.get("session_id") or _uid("ss")
        participant_id = p.get("participant_id")
        if not participant_id:
            raise DomainError(422, "invalid_session", "participant_id 必填")
        participant = self._get(conn, "participants", participant_id, "participant_id")
        if participant["withdrawn"]:
            raise DomainError(409, "participant_withdrawn", "参与者已撤回，不能开启新会话")
        started_at = p.get("started_at")
        if not isinstance(started_at, (int, float)):
            raise DomainError(422, "invalid_session", "started_at 必须是时间轴数值")
        existing = conn.execute(
            "SELECT * FROM sessions WHERE session_id=?", (session_id,)
        ).fetchone()
        if existing:
            if existing["participant_id"] == participant_id and existing["started_at"] == started_at:
                return self._session_view(existing), True
            raise DomainError(409, "conflict", "session_id 已存在且内容不同")
        conn.execute(
            "INSERT INTO sessions (session_id, participant_id, started_at, meta)"
            " VALUES (?,?,?,?)",
            (session_id, participant_id, started_at, Store.dumps(p.get("meta") or {})),
        )
        self._audit(conn, actor, "session_started", "session", session_id,
                    {"participant_id": participant_id, "started_at": started_at})
        return self._session_view(self._get(conn, "sessions", session_id, "session_id")), False

    def _session_view(self, row):
        return {
            "session_id": row["session_id"],
            "participant_id": row["participant_id"],
            "started_at": row["started_at"],
            "meta": json.loads(row["meta"]),
        }

    def register_segment(self, conn, actor, p):
        segment_id = p.get("segment_id") or _uid("seg")
        kind = p.get("kind")
        if kind not in SEGMENT_KINDS:
            raise DomainError(422, "invalid_segment", f"kind 必须是 {SEGMENT_KINDS}")
        t_start, t_end = p.get("t_start"), p.get("t_end")
        if not isinstance(t_start, (int, float)) or not isinstance(t_end, (int, float)):
            raise DomainError(422, "invalid_segment", "t_start/t_end 必须是时间轴数值")
        if t_end < t_start:
            raise DomainError(422, "invalid_segment", "t_end 不能早于 t_start")
        data_ref, checksum = p.get("data_ref"), p.get("checksum")
        if not data_ref or not checksum:
            raise DomainError(422, "invalid_segment", "data_ref 与 checksum 必填")
        session = self._get(conn, "sessions", p.get("session_id") or "", "session_id")
        existing = conn.execute(
            "SELECT * FROM segments WHERE segment_id=?", (segment_id,)
        ).fetchone()
        if existing:
            same = (
                existing["kind"] == kind and existing["t_start"] == t_start
                and existing["t_end"] == t_end and existing["checksum"] == checksum
            )
            if same:
                return self._segment_view(existing), True
            raise DomainError(409, "conflict", "segment_id 已存在且内容不同")
        conn.execute(
            """INSERT INTO segments
               (segment_id, session_id, participant_id, kind, t_start, t_end,
                collected_at, data_ref, checksum, meta)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (
                segment_id, session["session_id"], session["participant_id"], kind,
                t_start, t_end, _now(), data_ref, checksum,
                Store.dumps(p.get("meta") or {}),
            ),
        )
        self._audit(conn, actor, "segment_registered", "segment", segment_id,
                    {"session_id": session["session_id"], "kind": kind,
                     "t_start": t_start, "t_end": t_end, "checksum": checksum})
        return self._segment_view(self._get(conn, "segments", segment_id, "segment_id")), False

    def _segment_view(self, row):
        return {
            "segment_id": row["segment_id"],
            "session_id": row["session_id"],
            "participant_id": row["participant_id"],
            "kind": row["kind"],
            "t_start": row["t_start"],
            "t_end": row["t_end"],
            "data_ref": row["data_ref"],
            "checksum": row["checksum"],
            "meta": json.loads(row["meta"]),
        }

    def register_device_state(self, conn, actor, p):
        device_state_id = p.get("device_state_id") or _uid("dev")
        recorded_at = p.get("recorded_at")
        if not isinstance(recorded_at, (int, float)):
            raise DomainError(422, "invalid_device_state", "recorded_at 必须是时间轴数值")
        payload = p.get("payload")
        checksum = p.get("checksum")
        if not isinstance(payload, dict) or not checksum:
            raise DomainError(422, "invalid_device_state", "payload 与 checksum 必填")
        session = self._get(conn, "sessions", p.get("session_id") or "", "session_id")
        existing = conn.execute(
            "SELECT * FROM device_states WHERE device_state_id=?", (device_state_id,)
        ).fetchone()
        if existing:
            if existing["recorded_at"] == recorded_at and existing["checksum"] == checksum:
                return self._device_state_view(existing), True
            raise DomainError(409, "conflict", "device_state_id 已存在且内容不同")
        conn.execute(
            """INSERT INTO device_states
               (device_state_id, session_id, participant_id, recorded_at, payload, checksum)
               VALUES (?,?,?,?,?,?)""",
            (device_state_id, session["session_id"], session["participant_id"],
             recorded_at, Store.dumps(payload), checksum),
        )
        self._audit(conn, actor, "device_state_recorded", "device_state", device_state_id,
                    {"session_id": session["session_id"], "recorded_at": recorded_at})
        row = self._get(conn, "device_states", device_state_id, "device_state_id")
        return self._device_state_view(row), False

    def _device_state_view(self, row):
        return {
            "device_state_id": row["device_state_id"],
            "session_id": row["session_id"],
            "participant_id": row["participant_id"],
            "recorded_at": row["recorded_at"],
            "payload": json.loads(row["payload"]),
            "checksum": row["checksum"],
        }

    def register_medical_order(self, conn, actor, p):
        order_id = p.get("order_id") or _uid("ord")
        issued_at = p.get("issued_at")
        if not isinstance(issued_at, (int, float)):
            raise DomainError(422, "invalid_order", "issued_at 必须是时间轴数值")
        payload = p.get("payload")
        if not isinstance(payload, dict) or not payload:
            raise DomainError(422, "invalid_order", "payload 必须是非空对象")
        session = self._get(conn, "sessions", p.get("session_id") or "", "session_id")
        existing = conn.execute(
            "SELECT * FROM medical_orders WHERE order_id=?", (order_id,)
        ).fetchone()
        if existing:
            if existing["issued_at"] == issued_at and json.loads(existing["payload"]) == payload:
                return self._order_view(existing), True
            raise DomainError(409, "conflict", "order_id 已存在且内容不同")
        conn.execute(
            """INSERT INTO medical_orders
               (order_id, session_id, participant_id, issued_at, payload)
               VALUES (?,?,?,?,?)""",
            (order_id, session["session_id"], session["participant_id"],
             issued_at, Store.dumps(payload)),
        )
        self._audit(conn, actor, "medical_order_issued", "medical_order", order_id,
                    {"session_id": session["session_id"], "issued_at": issued_at})
        return self._order_view(self._get(conn, "medical_orders", order_id, "order_id")), False

    def _order_view(self, row):
        return {
            "order_id": row["order_id"],
            "session_id": row["session_id"],
            "participant_id": row["participant_id"],
            "issued_at": row["issued_at"],
            "payload": json.loads(row["payload"]),
        }

    # ------------------------------------------------------------------
    # 安全事件：统一时间轴封存
    # ------------------------------------------------------------------

    @staticmethod
    def _position(t_start, t_end, occurred_at):
        if t_end < occurred_at:
            return "pre"
        if t_start > occurred_at:
            return "post"
        return "at"

    def declare_safety_event(self, conn, actor, p):
        event_id = p.get("event_id") or _uid("ev")
        occurred_at = p.get("occurred_at")
        adverse_effect = p.get("adverse_effect")
        timeline = p.get("timeline")
        if not isinstance(occurred_at, (int, float)):
            raise DomainError(422, "invalid_event", "occurred_at 必须是统一时间轴数值")
        if not adverse_effect:
            raise DomainError(422, "invalid_event", "adverse_effect 必填")
        if not isinstance(timeline, dict) or not timeline.get("clock"):
            raise DomainError(422, "invalid_event", "timeline 必须说明统一时间轴口径 (clock)")
        session = self._get(conn, "sessions", p.get("session_id") or "", "session_id")
        existing = conn.execute(
            "SELECT * FROM safety_events WHERE event_id=?", (event_id,)
        ).fetchone()
        if existing:
            same = (
                existing["session_id"] == session["session_id"]
                and existing["occurred_at"] == occurred_at
                and existing["adverse_effect"] == adverse_effect
            )
            if same:
                return self.get_event(conn, event_id), True
            raise DomainError(409, "conflict", "event_id 已存在且内容不同")

        participant = self._get(
            conn, "participants", session["participant_id"], "participant_id"
        )
        sealed_at = _now()
        conn.execute(
            """INSERT INTO safety_events
               (event_id, session_id, participant_id, occurred_at, sealed_at,
                adverse_effect, timeline, consent_snapshot)
               VALUES (?,?,?,?,?,?,?,?)""",
            (event_id, session["session_id"], session["participant_id"], occurred_at,
             sealed_at, adverse_effect, Store.dumps(timeline), participant["consent_scope"]),
        )

        sealed = {"segment": 0, "device_state": 0, "medical_order": 0}
        for seg in conn.execute(
            "SELECT * FROM segments WHERE session_id=?", (session["session_id"],)
        ):
            self._seal(conn, event_id, "segment", seg["segment_id"],
                       self._position(seg["t_start"], seg["t_end"], occurred_at),
                       occurred_at if seg["t_start"] <= occurred_at <= seg["t_end"]
                       else (seg["t_end"] if seg["t_end"] < occurred_at else seg["t_start"]),
                       self._segment_view(seg), seg["checksum"])
            sealed["segment"] += 1
        for dev in conn.execute(
            "SELECT * FROM device_states WHERE session_id=?", (session["session_id"],)
        ):
            self._seal(conn, event_id, "device_state", dev["device_state_id"],
                       self._position(dev["recorded_at"], dev["recorded_at"], occurred_at),
                       dev["recorded_at"], self._device_state_view(dev), dev["checksum"])
            sealed["device_state"] += 1
        for order in conn.execute(
            "SELECT * FROM medical_orders WHERE session_id=?", (session["session_id"],)
        ):
            self._seal(conn, event_id, "medical_order", order["order_id"],
                       self._position(order["issued_at"], order["issued_at"], occurred_at),
                       order["issued_at"], self._order_view(order),
                       hashlib.sha256(order["payload"].encode("utf-8")).hexdigest())
            sealed["medical_order"] += 1

        window = self._initial_window(conn, event_id)
        conn.execute(
            """INSERT INTO review_rounds (event_id, round_no, triggered_by, proposed_window, stage)
               VALUES (?,?,?,?,?)""",
            (event_id, 1, "initial", Store.dumps(window), "await_clinical"),
        )
        self._audit(conn, actor, "safety_event_sealed", "safety_event", event_id,
                    {"session_id": session["session_id"], "occurred_at": occurred_at,
                     "sealed": sealed, "consent_snapshot": json.loads(participant["consent_scope"]),
                     "proposed_window": window})
        return self.get_event(conn, event_id), False

    def _seal(self, conn, event_id, category, ref_id, position, timeline_at, content, checksum):
        conn.execute(
            """INSERT INTO sealed_records
               (event_id, category, ref_id, position, timeline_at, content, checksum)
               VALUES (?,?,?,?,?,?,?)""",
            (event_id, category, ref_id, position, timeline_at,
             Store.dumps(content), checksum),
        )

    def _initial_window(self, conn, event_id):
        rows = conn.execute(
            """SELECT MIN(s.t_start) AS lo, MAX(s.t_end) AS hi
               FROM sealed_records sr JOIN segments s ON s.segment_id = sr.ref_id
               WHERE sr.event_id=? AND sr.category='segment'""",
            (event_id,),
        ).fetchone()
        occurred = conn.execute(
            "SELECT occurred_at FROM safety_events WHERE event_id=?", (event_id,)
        ).fetchone()["occurred_at"]
        lo = rows["lo"] if rows["lo"] is not None else occurred
        hi = rows["hi"] if rows["hi"] is not None else occurred
        return {"t_start": min(lo, occurred), "t_end": max(hi, occurred)}

    # ------------------------------------------------------------------
    # 裁决：临床安全 → 方法学 →（分歧）伦理 → 数据管理员应用
    # ------------------------------------------------------------------

    def _current_round(self, conn, event_id):
        return conn.execute(
            """SELECT * FROM review_rounds WHERE event_id=?
               ORDER BY round_no DESC LIMIT 1""",
            (event_id,),
        ).fetchone()

    def _round_segments(self, conn, event_id, window):
        """本轮审议的片段：已封存且与影响区间重叠。"""
        rows = conn.execute(
            """SELECT s.* FROM sealed_records sr
               JOIN segments s ON s.segment_id = sr.ref_id
               WHERE sr.event_id=? AND sr.category='segment'""",
            (event_id,),
        ).fetchall()
        return [
            r for r in rows
            if _overlap(r["t_start"], r["t_end"], window["t_start"], window["t_end"])
        ]

    def _review_payload_check(self, round_row, role, payload, segments):
        if not isinstance(payload, dict) or not isinstance(payload.get("segments"), dict):
            raise DomainError(422, "invalid_review", "payload.segments 必须是对象")
        if not payload["segments"]:
            raise DomainError(422, "invalid_review", "评审内容不能为空")
        expected = {s["segment_id"] for s in segments}
        got = set(payload["segments"].keys())
        # 伦理复核只需覆盖分歧片段，精确集合在提交时另行校验
        if role != "ethics" and got != expected:
            raise DomainError(
                422, "review_coverage",
                f"评审必须恰好覆盖本轮全部 {len(expected)} 个片段；"
                f"缺失 {sorted(expected - got)}，多余 {sorted(got - expected)}",
            )
        if role == "ethics" and not got <= expected:
            raise DomainError(422, "review_coverage",
                              f"伦理复核只能裁决本轮片段，多余 {sorted(got - expected)}")
        key = {"clinical_safety": "affected", "methodology": "interpretable"}.get(role)
        for seg_id, judgement in payload["segments"].items():
            if not isinstance(judgement, dict):
                raise DomainError(422, "invalid_review", f"片段 {seg_id} 的判断必须是对象")
            if key is not None and not isinstance(judgement.get(key), bool):
                raise DomainError(422, "invalid_review",
                                  f"片段 {seg_id} 缺少布尔判断 {key}")
            if role == "ethics" and judgement.get("decision") not in DECISIONS:
                raise DomainError(422, "invalid_review",
                                  f"片段 {seg_id} 的伦理决定必须是 {DECISIONS}")

    def _disputes(self, clinical, methodology, seg_ids):
        """返回 {seg_id: 是否分歧} 与一致决定。"""
        disputes, agreed = {}, {}
        for seg_id in seg_ids:
            affected = clinical["segments"][seg_id]["affected"]
            interpretable = methodology["segments"][seg_id]["interpretable"]
            decision, disputed = _adjudicate(affected, interpretable)
            if disputed:
                disputes[seg_id] = {"affected": affected, "interpretable": interpretable}
            else:
                agreed[seg_id] = decision
        return disputes, agreed

    def submit_review(self, conn, actor, event_id, p):
        role = p.get("role")
        reviewer = p.get("reviewer")
        signature = p.get("signature")
        payload = p.get("payload")
        if role not in ROLES:
            raise DomainError(422, "invalid_review", f"role 必须是 {ROLES}")
        if not reviewer or not isinstance(signature, str) or not signature:
            raise DomainError(422, "invalid_review", "reviewer 与 signature 必填（签署决定）")
        event = self._get(conn, "safety_events", event_id, "event_id")
        round_row = self._current_round(conn, event_id)
        if round_row["stage"] == "complete":
            raise DomainError(409, "round_complete",
                              "当前裁决轮已结束；如需复议请提交时钟校正开启新轮次")

        expected_stage = {
            "clinical_safety": "await_clinical",
            "methodology": "await_methodology",
            "ethics": "await_ethics",
        }[role]
        existing = conn.execute(
            "SELECT * FROM reviews WHERE event_id=? AND round_no=? AND role=?",
            (event_id, round_row["round_no"], role),
        ).fetchone()
        if existing:
            same = (
                existing["reviewer"] == reviewer
                and json.loads(existing["payload"]) == payload
                and existing["signature"] == signature
            )
            if same:
                return self._review_view(existing), True
            raise DomainError(409, "review_conflict",
                              "该角色本轮已提交不同内容的评审；分歧请走伦理复核")
        if round_row["stage"] != expected_stage:
            raise DomainError(
                409, "stage_order",
                f"当前阶段为 {round_row['stage']}，不能提交 {role} 评审；"
                "裁决顺序为 临床安全 → 方法学 →（分歧时）伦理",
            )

        window = json.loads(round_row["proposed_window"])
        segments = self._round_segments(conn, event_id, window)
        self._review_payload_check(round_row, role, payload, segments)

        if role == "ethics":
            clinical = json.loads(self._get(
                conn, "reviews", round_row["clinical_review_id"], "review_id")["payload"])
            methodology = json.loads(self._get(
                conn, "reviews", round_row["methodology_review_id"], "review_id")["payload"])
            seg_ids = [s["segment_id"] for s in segments]
            disputes, _ = self._disputes(clinical, methodology, seg_ids)
            if set(payload["segments"].keys()) != set(disputes.keys()):
                raise DomainError(422, "review_coverage",
                                  "伦理复核必须且只需裁决分歧片段: "
                                  f"{sorted(disputes.keys())}")

        review_id = _uid("rv")
        conn.execute(
            """INSERT INTO reviews (review_id, event_id, round_no, role, reviewer,
                                    signed_at, payload, signature)
               VALUES (?,?,?,?,?,?,?,?)""",
            (review_id, event_id, round_row["round_no"], role, reviewer,
             _now(), Store.dumps(payload), signature),
        )
        column = {
            "clinical_safety": "clinical_review_id",
            "methodology": "methodology_review_id",
            "ethics": "ethics_review_id",
        }[role]
        next_stage = round_row["stage"]
        if role == "clinical_safety":
            next_stage = "await_methodology"
        elif role == "methodology":
            clinical = json.loads(conn.execute(
                "SELECT payload FROM reviews WHERE review_id=?",
                (round_row["clinical_review_id"],)).fetchone()["payload"])
            seg_ids = [s["segment_id"] for s in segments]
            disputes, _ = self._disputes(clinical, payload, seg_ids)
            next_stage = "await_ethics" if disputes else "await_application"
        elif role == "ethics":
            next_stage = "await_application"
        conn.execute(
            f"UPDATE review_rounds SET {column}=?, stage=? WHERE id=?",
            (review_id, next_stage, round_row["id"]),
        )
        self._audit(conn, actor, "review_submitted", "review", review_id,
                    {"event_id": event_id, "round_no": round_row["round_no"],
                     "role": role, "reviewer": reviewer, "next_stage": next_stage})
        return self._review_view(
            self._get(conn, "reviews", review_id, "review_id")), False

    def _review_view(self, row):
        return {
            "review_id": row["review_id"],
            "event_id": row["event_id"],
            "round_no": row["round_no"],
            "role": row["role"],
            "reviewer": row["reviewer"],
            "signed_at": row["signed_at"],
            "payload": json.loads(row["payload"]),
            "signature": row["signature"],
        }

    def _compute_decisions(self, conn, event_id, round_row):
        """由已签署评审确定性地计算本轮决定；分歧取伦理裁决。"""
        window = json.loads(round_row["proposed_window"])
        segments = self._round_segments(conn, event_id, window)
        seg_ids = [s["segment_id"] for s in segments]
        clinical = json.loads(self._get(
            conn, "reviews", round_row["clinical_review_id"], "review_id")["payload"])
        methodology = json.loads(self._get(
            conn, "reviews", round_row["methodology_review_id"], "review_id")["payload"])
        disputes, agreed = self._disputes(clinical, methodology, seg_ids)

        decisions = {}
        for seg_id in seg_ids:
            basis = {
                "clinical_safety": clinical["segments"][seg_id],
                "methodology": methodology["segments"][seg_id],
            }
            if seg_id in agreed:
                decisions[seg_id] = {"decision": agreed[seg_id], "basis": basis}
            else:
                ethics = json.loads(self._get(
                    conn, "reviews", round_row["ethics_review_id"], "review_id")["payload"])
                basis["ethics"] = ethics["segments"][seg_id]
                decisions[seg_id] = {
                    "decision": ethics["segments"][seg_id]["decision"], "basis": basis,
                }
        agreement = "ethics_tiebreak" if disputes else "consensus"

        # 区间之外、此前已裁决的片段：沿用上一当前版本的决定
        predecessor = conn.execute(
            "SELECT * FROM decision_versions WHERE event_id=? AND superseded_at IS NULL",
            (event_id,),
        ).fetchone()
        if predecessor:
            carried = 0
            for seg_id, entry in json.loads(predecessor["decisions"]).items():
                if seg_id not in decisions:
                    decisions[seg_id] = {
                        "decision": entry["decision"],
                        "basis": {"carried_from": predecessor["version_id"]},
                    }
                    carried += 1
        return decisions, agreement

    def apply_release(self, conn, actor, event_id, p):
        applied_by = p.get("applied_by")
        if not applied_by:
            raise DomainError(422, "invalid_apply", "applied_by（数据管理员）必填")
        self._get(conn, "safety_events", event_id, "event_id")
        round_row = self._current_round(conn, event_id)
        if round_row["stage"] == "complete" and round_row["version_id"]:
            # 同内容重试：本轮决定已应用，直接返回既有版本
            version = self._get(conn, "decision_versions", round_row["version_id"], "version_id")
            return self._version_view(version), True
        if round_row["stage"] != "await_application":
            raise DomainError(409, "not_ready",
                              f"当前阶段为 {round_row['stage']}，两阶段结论"
                              "（分歧时含伦理复核）完成后数据管理员才能应用")

        decisions, agreement = self._compute_decisions(conn, event_id, round_row)
        scope_hash = _scope_hash(decisions)
        predecessor = conn.execute(
            "SELECT * FROM decision_versions WHERE event_id=? AND superseded_at IS NULL",
            (event_id,),
        ).fetchone()
        if predecessor and predecessor["scope_hash"] == scope_hash:
            # 决定内容未变：幂等返回当前版本，不产生取代
            conn.execute(
                "UPDATE review_rounds SET stage='complete', version_id=? WHERE id=?",
                (predecessor["version_id"], round_row["id"]),
            )
            return self._version_view(predecessor), True

        seq_row = conn.execute(
            "SELECT COALESCE(MAX(seq),0) AS mx FROM decision_versions WHERE event_id=?",
            (event_id,),
        ).fetchone()
        version_id = _uid("dv")
        now = _now()
        try:
            # 先在同事务内取代旧当前版本，再插入新版本，
            # 以满足“每事件至多一个当前版本”的部分唯一索引；
            # superseded_by 引用新行，需插入后回填
            if predecessor:
                conn.execute(
                    "UPDATE decision_versions SET superseded_at=?"
                    " WHERE version_id=?",
                    (now, predecessor["version_id"]),
                )
            conn.execute(
                """INSERT INTO decision_versions
                   (version_id, event_id, round_no, seq, predecessor_id, agreement,
                    decisions, scope_hash, applied_by, created_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (version_id, event_id, round_row["round_no"], seq_row["mx"] + 1,
                 predecessor["version_id"] if predecessor else None, agreement,
                 Store.dumps(decisions), scope_hash, applied_by, now),
            )
            if predecessor:
                conn.execute(
                    "UPDATE decision_versions SET superseded_by=? WHERE version_id=?",
                    (version_id, predecessor["version_id"]),
                )
        except Exception as exc:
            raise DomainError(409, "concurrent_version",
                              "并发裁决冲突：该事件已存在当前版本") from exc
        if predecessor:
            self._audit(conn, actor, "version_superseded", "decision_version",
                        predecessor["version_id"],
                        {"event_id": event_id, "superseded_by": version_id})
        conn.execute(
            "UPDATE review_rounds SET stage='complete', version_id=? WHERE id=?",
            (version_id, round_row["id"]),
        )
        if round_row["triggered_by"].startswith("clock_correction:"):
            correction_id = round_row["triggered_by"].split(":", 1)[1]
            conn.execute(
                "UPDATE clock_corrections SET resulting_version_id=? WHERE correction_id=?",
                (version_id, correction_id),
            )
        self._audit(conn, actor, "release_applied", "decision_version", version_id,
                    {"event_id": event_id, "round_no": round_row["round_no"],
                     "agreement": agreement, "applied_by": applied_by,
                     "scope_hash": scope_hash,
                     "decisions": {k: v["decision"] for k, v in decisions.items()}})
        return self._version_view(
            self._get(conn, "decision_versions", version_id, "version_id")), False

    def _version_view(self, row):
        return {
            "version_id": row["version_id"],
            "event_id": row["event_id"],
            "round_no": row["round_no"],
            "seq": row["seq"],
            "predecessor_id": row["predecessor_id"],
            "agreement": row["agreement"],
            "decisions": json.loads(row["decisions"]),
            "scope_hash": row["scope_hash"],
            "applied_by": row["applied_by"],
            "created_at": row["created_at"],
            "superseded_at": row["superseded_at"],
            "superseded_by": row["superseded_by"],
        }

    # ------------------------------------------------------------------
    # 迟到的时钟校正：只能提出影响区间并开启新裁决轮
    # ------------------------------------------------------------------

    def propose_clock_correction(self, conn, actor, event_id, p):
        delta = p.get("delta")
        source = p.get("source")
        basis = p.get("basis")
        impact_window = p.get("impact_window")
        if not isinstance(delta, (int, float)):
            raise DomainError(422, "invalid_correction", "delta 必须是统一时间轴偏移数值")
        if not source or not basis:
            raise DomainError(422, "invalid_correction", "source 与 basis 必填")
        if (not isinstance(impact_window, dict)
                or not isinstance(impact_window.get("t_start"), (int, float))
                or not isinstance(impact_window.get("t_end"), (int, float))
                or impact_window["t_end"] < impact_window["t_start"]):
            raise DomainError(422, "invalid_correction",
                              "impact_window 必须是 {t_start, t_end} 且 t_end>=t_start")
        self._get(conn, "safety_events", event_id, "event_id")
        correction_id = p.get("correction_id") or _uid("cc")
        existing = conn.execute(
            "SELECT * FROM clock_corrections WHERE correction_id=?", (correction_id,)
        ).fetchone()
        if existing:
            same = (existing["delta"] == delta and existing["source"] == source
                    and json.loads(existing["impact_window"]) == impact_window)
            if same:
                return self._correction_view(existing), True
            raise DomainError(409, "conflict", "correction_id 已存在且内容不同")
        conn.execute(
            """INSERT INTO clock_corrections
               (correction_id, event_id, proposed_at, delta, source, basis,
                impact_window, status)
               VALUES (?,?,?,?,?,?,?, 'proposed')""",
            (correction_id, event_id, _now(), delta, source, basis,
             Store.dumps(impact_window)),
        )
        self._audit(conn, actor, "clock_correction_proposed", "clock_correction",
                    correction_id,
                    {"event_id": event_id, "delta": delta, "impact_window": impact_window})
        row = self._get(conn, "clock_corrections", correction_id, "correction_id")
        return self._correction_view(row), False

    def decide_clock_correction(self, conn, actor, correction_id, p):
        accept = p.get("accept")
        decided_by = p.get("decided_by")
        if not isinstance(accept, bool) or not decided_by:
            raise DomainError(422, "invalid_decision", "accept(布尔) 与 decided_by 必填")
        correction = self._get(conn, "clock_corrections", correction_id, "correction_id")
        if correction["status"] != "proposed":
            existing = self._correction_view(correction)
            if ((correction["status"] == "accepted") == accept
                    and correction["decided_by"] == decided_by):
                return existing, True
            raise DomainError(409, "conflict", "该校正已被裁决且结论不同")
        event_id = correction["event_id"]
        round_row = self._current_round(conn, event_id)
        if accept and round_row["stage"] != "complete":
            raise DomainError(409, "adjudication_in_progress",
                              "当前裁决轮未结束，不能开启新轮次；校正决定请待本轮完成")

        now = _now()
        if accept:
            event = self._get(conn, "safety_events", event_id, "event_id")
            window = json.loads(correction["impact_window"])
            # 影响区间内尚未封存的片段立即进入默认隔离，等待新轮裁决
            newly_sealed = []
            for seg in conn.execute(
                "SELECT * FROM segments WHERE session_id=?", (event["session_id"],)
            ):
                if not _overlap(seg["t_start"], seg["t_end"],
                                window["t_start"], window["t_end"]):
                    continue
                already = conn.execute(
                    """SELECT 1 FROM sealed_records
                       WHERE event_id=? AND category='segment' AND ref_id=?""",
                    (event_id, seg["segment_id"]),
                ).fetchone()
                if already:
                    continue
                self._seal(conn, event_id, "segment", seg["segment_id"],
                           self._position(seg["t_start"], seg["t_end"],
                                          event["occurred_at"]),
                           seg["t_start"], self._segment_view(seg), seg["checksum"])
                newly_sealed.append(seg["segment_id"])
            round_no = event["next_round_no"]
            conn.execute(
                """INSERT INTO review_rounds (event_id, round_no, triggered_by,
                                              proposed_window, stage)
                   VALUES (?,?,?,?, 'await_clinical')""",
                (event_id, round_no, f"clock_correction:{correction_id}",
                 correction["impact_window"]),
            )
            conn.execute(
                "UPDATE safety_events SET next_round_no=? WHERE event_id=?",
                (round_no + 1, event_id),
            )
            conn.execute(
                """UPDATE clock_corrections SET status='accepted', decided_by=?,
                   decided_at=?, round_no=? WHERE correction_id=?""",
                (decided_by, now, round_no, correction_id),
            )
            self._audit(conn, actor, "clock_correction_accepted", "clock_correction",
                        correction_id,
                        {"event_id": event_id, "round_no": round_no,
                         "decided_by": decided_by, "newly_sealed": newly_sealed,
                         "note": "仅开启新裁决轮；已裁决版本保持有效直至被新版本取代"})
        else:
            conn.execute(
                """UPDATE clock_corrections SET status='rejected', decided_by=?,
                   decided_at=? WHERE correction_id=?""",
                (decided_by, now, correction_id),
            )
            self._audit(conn, actor, "clock_correction_rejected", "clock_correction",
                        correction_id,
                        {"event_id": event_id, "decided_by": decided_by})
        row = self._get(conn, "clock_corrections", correction_id, "correction_id")
        return self._correction_view(row), False

    def _correction_view(self, row):
        return {
            "correction_id": row["correction_id"],
            "event_id": row["event_id"],
            "round_no": row["round_no"],
            "proposed_at": row["proposed_at"],
            "delta": row["delta"],
            "source": row["source"],
            "basis": row["basis"],
            "impact_window": json.loads(row["impact_window"]),
            "status": row["status"],
            "decided_by": row["decided_by"],
            "decided_at": row["decided_at"],
            "resulting_version_id": row["resulting_version_id"],
        }

    # ------------------------------------------------------------------
    # 分析任务：只读默认隔离之外的数据
    # ------------------------------------------------------------------

    def _current_decision(self, conn, event_id, segment_id):
        row = conn.execute(
            "SELECT * FROM decision_versions WHERE event_id=? AND superseded_at IS NULL",
            (event_id,),
        ).fetchone()
        if row is None:
            return None, None
        decisions = json.loads(row["decisions"])
        entry = decisions.get(segment_id)
        return (entry["decision"] if entry else None), row["version_id"]

    def _segment_readability(self, conn, segment, kind):
        """返回 (是否可读, 版本血缘 {event_id: version_id}, 原因)。"""
        participant = self._get(
            conn, "participants", segment["participant_id"], "participant_id")
        if kind == "research" and participant["withdrawn"]:
            return False, {}, "participant_withdrawn"
        sealed_by = conn.execute(
            "SELECT event_id FROM sealed_records WHERE category='segment' AND ref_id=?",
            (segment["segment_id"],),
        ).fetchall()
        if not sealed_by:
            return True, {}, "never_sealed"
        lineage = {}
        saw_safety_only = False
        for row in sealed_by:
            decision, version_id = self._current_decision(
                conn, row["event_id"], segment["segment_id"])
            if decision is None:
                return False, {}, "quarantined_pending_adjudication"
            if decision == "exclude":
                return False, {}, "permanently_excluded"
            if decision == "safety_only":
                saw_safety_only = True
            lineage[row["event_id"]] = version_id
        if saw_safety_only and kind != "safety_review":
            return False, {}, "safety_review_only"
        return True, lineage, "released"

    def create_analysis_task(self, conn, actor, p):
        kind = p.get("kind")
        if kind not in ("research", "safety_review"):
            raise DomainError(422, "invalid_task", "kind 必须是 research 或 safety_review")
        creator = p.get("creator")
        if not creator:
            raise DomainError(422, "invalid_task", "creator 必填")
        scope = p.get("scope") or {}
        if not isinstance(scope, dict):
            raise DomainError(422, "invalid_task", "scope 必须是对象")
        endpoint_id = p.get("endpoint_id")
        if kind == "research" and not endpoint_id:
            raise DomainError(422, "invalid_task", "研究任务必须指明预注册终点 endpoint_id")

        clauses, args = [], []
        if scope.get("participant_ids"):
            clauses.append("participant_id IN (%s)"
                           % ",".join("?" * len(scope["participant_ids"])))
            args.extend(scope["participant_ids"])
        if scope.get("session_ids"):
            clauses.append("session_id IN (%s)"
                           % ",".join("?" * len(scope["session_ids"])))
            args.extend(scope["session_ids"])
        if scope.get("kinds"):
            clauses.append("kind IN (%s)" % ",".join("?" * len(scope["kinds"])))
            args.extend(scope["kinds"])
        if isinstance(scope.get("t_start"), (int, float)):
            clauses.append("t_end >= ?")
            args.append(scope["t_start"])
        if isinstance(scope.get("t_end"), (int, float)):
            clauses.append("t_start <= ?")
            args.append(scope["t_end"])
        sql = "SELECT * FROM segments" + (" WHERE " + " AND ".join(clauses) if clauses else "")
        candidates = conn.execute(sql, args).fetchall()

        explicit_withdrawn = []
        if kind == "research" and scope.get("participant_ids"):
            for pid in scope["participant_ids"]:
                row = self._get(conn, "participants", pid, "participant_id")
                if row["withdrawn"]:
                    explicit_withdrawn.append(pid)
        if explicit_withdrawn:
            raise DomainError(409, "participant_withdrawn",
                              "参与者已撤回，停止新的研究使用: "
                              f"{sorted(explicit_withdrawn)}")

        readable, excluded = [], {}
        withdrawn_seen = False
        for seg in candidates:
            ok, lineage, reason = self._segment_readability(conn, seg, kind)
            if ok:
                readable.append({
                    "segment_id": seg["segment_id"],
                    "kind": seg["kind"],
                    "t_start": seg["t_start"],
                    "t_end": seg["t_end"],
                    "checksum": seg["checksum"],
                    "data_ref": seg["data_ref"],
                    "versions": lineage,
                })
            else:
                excluded[seg["segment_id"]] = reason
                if reason == "participant_withdrawn":
                    withdrawn_seen = True

        task_id = p.get("task_id") or _uid("task")
        existing = conn.execute(
            "SELECT * FROM analysis_tasks WHERE task_id=?", (task_id,)).fetchone()
        if existing:
            same = (existing["kind"] == kind and existing["creator"] == creator
                    and json.loads(existing["scope"]) == scope
                    and existing["endpoint_id"] == endpoint_id)
            if same:
                return self._task_view(existing), True
            raise DomainError(409, "conflict", "task_id 已存在且内容不同")
        conn.execute(
            """INSERT INTO analysis_tasks
               (task_id, kind, endpoint_id, creator, created_at, scope, readable, withdrawn_seen)
               VALUES (?,?,?,?,?,?,?,?)""",
            (task_id, kind, endpoint_id, creator, _now(), Store.dumps(scope),
             Store.dumps({"readable": readable, "excluded": excluded}),
             1 if withdrawn_seen else 0),
        )
        self._audit(conn, actor, "analysis_task_created", "analysis_task", task_id,
                    {"kind": kind, "endpoint_id": endpoint_id, "creator": creator,
                     "readable_count": len(readable),
                     "excluded": excluded})
        return self._task_view(
            self._get(conn, "analysis_tasks", task_id, "task_id")), False

    def _task_view(self, row):
        return {
            "task_id": row["task_id"],
            "kind": row["kind"],
            "endpoint_id": row["endpoint_id"],
            "creator": row["creator"],
            "created_at": row["created_at"],
            "scope": json.loads(row["scope"]),
            "readset": json.loads(row["readable"]),
            "withdrawn_seen": bool(row["withdrawn_seen"]),
        }

    def record_analysis_result(self, conn, actor, p):
        task_id = p.get("task_id")
        endpoint_id = p.get("endpoint_id")
        values = p.get("values")
        segment_ids = p.get("segment_ids")
        producer = p.get("producer")
        if not producer:
            raise DomainError(422, "invalid_result", "producer 必填")
        if not isinstance(segment_ids, list) or not segment_ids:
            raise DomainError(422, "invalid_result", "segment_ids 必须是非空列表")
        if values is None or not endpoint_id:
            raise DomainError(422, "invalid_result", "endpoint_id 与 values 必填")
        task = self._get(conn, "analysis_tasks", task_id, "task_id")
        readset = json.loads(task["readable"])
        readable_map = {e["segment_id"]: e for e in readset["readable"]}

        inputs = []
        for seg_id in segment_ids:
            entry = readable_map.get(seg_id)
            if entry is None:
                reason = readset["excluded"].get(seg_id, "outside_task_scope")
                raise DomainError(
                    409, "segment_not_readable",
                    f"片段 {seg_id} 不在任务可读集内（{reason}）；"
                    "分析任务只能使用默认隔离之外的数据")
            participant = self._get(
                conn, "participants",
                self._get(conn, "segments", seg_id, "segment_id")["participant_id"],
                "participant_id")
            if task["kind"] == "research" and participant["withdrawn"]:
                raise DomainError(409, "participant_withdrawn",
                                  "参与者已撤回，停止新的研究使用；"
                                  "安全记录依法保留")
            inputs.append({"segment_id": seg_id, "checksum": entry["checksum"],
                           "versions": entry["versions"]})

        result_id = p.get("result_id") or _uid("res")
        existing = conn.execute(
            "SELECT * FROM analysis_results WHERE result_id=?", (result_id,)).fetchone()
        if existing:
            same = (existing["task_id"] == task_id
                    and existing["endpoint_id"] == endpoint_id
                    and json.loads(existing["segment_inputs"]) == inputs)
            if same:
                return self._result_view(existing), True
            raise DomainError(409, "conflict", "result_id 已存在且内容不同")
        conn.execute(
            """INSERT INTO analysis_results
               (result_id, task_id, endpoint_id, produced_at, values_json,
                segment_inputs, producer)
               VALUES (?,?,?,?,?,?,?)""",
            (result_id, task_id, endpoint_id, _now(), Store.dumps(values),
             Store.dumps(inputs), producer),
        )
        self._audit(conn, actor, "analysis_result_recorded", "analysis_result", result_id,
                    {"task_id": task_id, "endpoint_id": endpoint_id,
                     "segments": segment_ids})
        return self._result_view(
            self._get(conn, "analysis_results", result_id, "result_id")), False

    def _result_view(self, row):
        return {
            "result_id": row["result_id"],
            "task_id": row["task_id"],
            "endpoint_id": row["endpoint_id"],
            "produced_at": row["produced_at"],
            "values": json.loads(row["values_json"]),
            "segment_inputs": json.loads(row["segment_inputs"]),
            "producer": row["producer"],
        }

    # ------------------------------------------------------------------
    # 查询：事件详情 / 待裁决 / 血缘追溯 / 审计
    # ------------------------------------------------------------------

    def get_event(self, conn, event_id):
        event = self._get(conn, "safety_events", event_id, "event_id")
        sealed = [dict(r) for r in conn.execute(
            "SELECT category, ref_id, position, timeline_at, checksum FROM sealed_records"
            " WHERE event_id=? ORDER BY id", (event_id,))]
        rounds = []
        for r in conn.execute(
                "SELECT * FROM review_rounds WHERE event_id=? ORDER BY round_no",
                (event_id,)):
            rounds.append({
                "round_no": r["round_no"],
                "triggered_by": r["triggered_by"],
                "proposed_window": json.loads(r["proposed_window"]),
                "stage": r["stage"],
                "clinical_review_id": r["clinical_review_id"],
                "methodology_review_id": r["methodology_review_id"],
                "ethics_review_id": r["ethics_review_id"],
                "version_id": r["version_id"],
            })
        reviews = [self._review_view(r) for r in conn.execute(
            "SELECT * FROM reviews WHERE event_id=? ORDER BY signed_at", (event_id,))]
        versions = [self._version_view(r) for r in conn.execute(
            "SELECT * FROM decision_versions WHERE event_id=? ORDER BY seq", (event_id,))]
        corrections = [self._correction_view(r) for r in conn.execute(
            "SELECT * FROM clock_corrections WHERE event_id=? ORDER BY proposed_at",
            (event_id,))]
        return {
            "event_id": event["event_id"],
            "session_id": event["session_id"],
            "participant_id": event["participant_id"],
            "occurred_at": event["occurred_at"],
            "sealed_at": event["sealed_at"],
            "adverse_effect": event["adverse_effect"],
            "timeline": json.loads(event["timeline"]),
            "consent_snapshot": json.loads(event["consent_snapshot"]),
            "sealed_records": sealed,
            "rounds": rounds,
            "reviews": reviews,
            "versions": versions,
            "clock_corrections": corrections,
        }

    def list_pending(self, conn):
        """待裁决工作：当前轮未完成，或有待决定的时钟校正。重启后据此继续。"""
        pending = []
        for event in conn.execute("SELECT * FROM safety_events ORDER BY sealed_at"):
            round_row = self._current_round(conn, event["event_id"])
            open_corrections = conn.execute(
                "SELECT COUNT(*) AS n FROM clock_corrections"
                " WHERE event_id=? AND status='proposed'", (event["event_id"],),
            ).fetchone()["n"]
            if round_row["stage"] != "complete" or open_corrections:
                pending.append({
                    "event_id": event["event_id"],
                    "session_id": event["session_id"],
                    "participant_id": event["participant_id"],
                    "occurred_at": event["occurred_at"],
                    "round_no": round_row["round_no"],
                    "stage": round_row["stage"],
                    "open_clock_corrections": open_corrections,
                })
        return pending

    def trace_result(self, conn, result_id):
        """从分析结果追到中止事件、区间选择、签署决定与后来修订。"""
        result = self._get(conn, "analysis_results", result_id, "result_id")
        result_view = self._result_view(result)
        task = self._get(conn, "analysis_tasks", result["task_id"], "task_id")
        lineage = []
        event_ids = set()
        for item in result_view["segment_inputs"]:
            seg = self._get(conn, "segments", item["segment_id"], "segment_id")
            events = []
            for event_id, version_id in (item.get("versions") or {}).items():
                event_ids.add(event_id)
                version = self._get(conn, "decision_versions", version_id, "version_id")
                version_view = self._version_view(version)
                rounds = []
                for r in conn.execute(
                        "SELECT * FROM review_rounds WHERE event_id=? ORDER BY round_no",
                        (event_id,)):
                    rounds.append({
                        "round_no": r["round_no"],
                        "triggered_by": r["triggered_by"],
                        "proposed_window": json.loads(r["proposed_window"]),
                        "stage": r["stage"],
                    })
                reviews = [self._review_view(r) for r in conn.execute(
                    "SELECT * FROM reviews WHERE event_id=? ORDER BY signed_at",
                    (event_id,))]
                later = [self._version_view(r) for r in conn.execute(
                    """SELECT * FROM decision_versions
                       WHERE event_id=? AND seq>? ORDER BY seq""",
                    (event_id, version["seq"]))]
                corrections = [self._correction_view(r) for r in conn.execute(
                    "SELECT * FROM clock_corrections WHERE event_id=? ORDER BY proposed_at",
                    (event_id,))]
                event = self._get(conn, "safety_events", event_id, "event_id")
                events.append({
                    "event_id": event_id,
                    "occurred_at": event["occurred_at"],
                    "adverse_effect": event["adverse_effect"],
                    "timeline": json.loads(event["timeline"]),
                    "releasing_version": version_view,
                    "rounds": rounds,
                    "signed_reviews": reviews,
                    "later_revisions": later,
                    "clock_corrections": corrections,
                })
            lineage.append({
                "segment_id": item["segment_id"],
                "checksum": item["checksum"],
                "kind": seg["kind"],
                "t_start": seg["t_start"],
                "t_end": seg["t_end"],
                "quarantine_history": events,
            })
        audit = self.audit_for(conn, "analysis_result", result_id)
        return {
            "result": result_view,
            "task": self._task_view(task),
            "segment_lineage": lineage,
            "audit": audit,
        }

    def audit_for(self, conn, entity_type, entity_id):
        rows = conn.execute(
            """SELECT * FROM audit_log WHERE (entity_type=? AND entity_id=?)
               OR json_extract(payload, '$.event_id')=?
               OR json_extract(payload, '$.resulting_event_id')=?
               ORDER BY seq""",
            (entity_type, entity_id, entity_id, entity_id),
        ).fetchall()
        return [{
            "seq": r["seq"], "at": r["at"], "actor": r["actor"],
            "action": r["action"], "entity_type": r["entity_type"],
            "entity_id": r["entity_id"], "payload": json.loads(r["payload"]),
            "prev_hash": r["prev_hash"], "entry_hash": r["entry_hash"],
        } for r in rows]
