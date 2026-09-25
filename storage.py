"""中止封存与解封裁决的持久化层。

仅使用标准库 sqlite3。设计要点：

* 写操作一律 ``BEGIN IMMEDIATE`` 串行化，配合部分唯一索引保证
  “并发裁决只能产生一个当前版本”。
* 审计表为只增哈希链，任何领域动作都在同一事务内落一条审计。
* 封存时对片段、设备状态、医嘱、同意范围做不可变快照。
"""

import json
import sqlite3
import threading
import time

SCHEMA = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS participants (
    participant_id TEXT PRIMARY KEY,
    enrolled_at    REAL NOT NULL,
    consent_scope  TEXT NOT NULL,          -- 入组时同意范围 JSON
    withdrawn      INTEGER NOT NULL DEFAULT 0,
    withdrawn_at   REAL,
    meta           TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS sessions (
    session_id     TEXT PRIMARY KEY,
    participant_id TEXT NOT NULL REFERENCES participants(participant_id),
    started_at     REAL NOT NULL,
    meta           TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS segments (
    segment_id     TEXT PRIMARY KEY,
    session_id     TEXT NOT NULL REFERENCES sessions(session_id),
    participant_id TEXT NOT NULL REFERENCES participants(participant_id),
    kind           TEXT NOT NULL CHECK (kind IN ('stimulus','behavior','brain_signal')),
    t_start        REAL NOT NULL,
    t_end          REAL NOT NULL,
    collected_at   REAL NOT NULL,
    data_ref       TEXT NOT NULL,
    checksum       TEXT NOT NULL,
    meta           TEXT NOT NULL DEFAULT '{}',
    CHECK (t_end >= t_start)
);

CREATE TABLE IF NOT EXISTS device_states (
    device_state_id TEXT PRIMARY KEY,
    session_id      TEXT NOT NULL REFERENCES sessions(session_id),
    participant_id  TEXT NOT NULL REFERENCES participants(participant_id),
    recorded_at     REAL NOT NULL,
    payload         TEXT NOT NULL,
    checksum        TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS medical_orders (
    order_id       TEXT PRIMARY KEY,
    session_id     TEXT NOT NULL REFERENCES sessions(session_id),
    participant_id TEXT NOT NULL REFERENCES participants(participant_id),
    issued_at      REAL NOT NULL,
    payload        TEXT NOT NULL
);

-- 安全事件：统一时间轴上的中止点
CREATE TABLE IF NOT EXISTS safety_events (
    event_id         TEXT PRIMARY KEY,
    session_id       TEXT NOT NULL REFERENCES sessions(session_id),
    participant_id   TEXT NOT NULL REFERENCES participants(participant_id),
    occurred_at      REAL NOT NULL,        -- 统一时间轴上的不良事件时刻
    sealed_at        REAL NOT NULL,
    adverse_effect   TEXT NOT NULL,
    timeline         TEXT NOT NULL,        -- 时间轴口径 JSON
    consent_snapshot TEXT NOT NULL,        -- 封存当时的同意范围快照 JSON
    next_round_no    INTEGER NOT NULL DEFAULT 2 -- 首轮为 1，后续轮次自增
);

-- 封存快照：事件发生时的前后片段 / 设备状态 / 医嘱（不可变）
CREATE TABLE IF NOT EXISTS sealed_records (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id    TEXT NOT NULL REFERENCES safety_events(event_id),
    category    TEXT NOT NULL CHECK (category IN ('segment','device_state','medical_order')),
    ref_id      TEXT NOT NULL,
    position    TEXT NOT NULL CHECK (position IN ('pre','at','post')),
    timeline_at REAL NOT NULL,
    content     TEXT NOT NULL,            -- 冻结的完整内容 JSON
    checksum    TEXT NOT NULL,
    UNIQUE(event_id, category, ref_id)
);

-- 裁决轮：首轮为封存后双阶段裁决；时钟校正被接受后开启新一轮
CREATE TABLE IF NOT EXISTS review_rounds (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id            TEXT NOT NULL REFERENCES safety_events(event_id),
    round_no            INTEGER NOT NULL,
    triggered_by        TEXT NOT NULL,     -- 'initial' 或 'clock_correction:<id>'
    proposed_window     TEXT NOT NULL,     -- 本轮审议的影响区间 JSON
    stage               TEXT NOT NULL
        CHECK (stage IN ('await_clinical','await_methodology','await_ethics','await_application','complete')),
    clinical_review_id    TEXT,
    methodology_review_id TEXT,
    ethics_review_id      TEXT,
    version_id            TEXT,
    UNIQUE(event_id, round_no)
);

CREATE TABLE IF NOT EXISTS reviews (
    review_id   TEXT PRIMARY KEY,
    event_id    TEXT NOT NULL REFERENCES safety_events(event_id),
    round_no    INTEGER NOT NULL,
    role        TEXT NOT NULL CHECK (role IN ('clinical_safety','methodology','ethics')),
    reviewer    TEXT NOT NULL,
    signed_at   REAL NOT NULL,
    payload     TEXT NOT NULL,            -- 逐片段判断与理由
    signature   TEXT NOT NULL,
    UNIQUE(event_id, round_no, role)
);

CREATE TABLE IF NOT EXISTS decision_versions (
    version_id      TEXT PRIMARY KEY,
    event_id        TEXT NOT NULL REFERENCES safety_events(event_id),
    round_no        INTEGER NOT NULL,
    seq             INTEGER NOT NULL,      -- 同一事件内单调递增
    predecessor_id  TEXT REFERENCES decision_versions(version_id),
    agreement       TEXT NOT NULL CHECK (agreement IN ('consensus','ethics_tiebreak')),
    decisions       TEXT NOT NULL,         -- {segment_id: {decision, basis, ...}}
    scope_hash      TEXT NOT NULL,
    applied_by      TEXT NOT NULL,
    created_at      REAL NOT NULL,
    superseded_at   REAL,
    superseded_by   TEXT REFERENCES decision_versions(version_id),
    UNIQUE(event_id, seq)
);

-- 关键不变量：任一时刻每个事件至多一个未被取代的当前版本
CREATE UNIQUE INDEX IF NOT EXISTS ux_event_current_version
    ON decision_versions(event_id) WHERE superseded_at IS NULL;

CREATE TABLE IF NOT EXISTS clock_corrections (
    correction_id   TEXT PRIMARY KEY,
    event_id        TEXT NOT NULL REFERENCES safety_events(event_id),
    round_no        INTEGER,               -- 接受后开启的轮次
    proposed_at     REAL NOT NULL,
    delta           REAL NOT NULL,         -- 统一时间轴偏移量
    source          TEXT NOT NULL,
    basis           TEXT NOT NULL,
    impact_window   TEXT NOT NULL,         -- 仅为“影响区间”提案 JSON
    status          TEXT NOT NULL CHECK (status IN ('proposed','accepted','rejected')),
    decided_by      TEXT,
    decided_at      REAL,
    resulting_version_id TEXT
);

CREATE TABLE IF NOT EXISTS analysis_tasks (
    task_id         TEXT PRIMARY KEY,
    kind            TEXT NOT NULL CHECK (kind IN ('research','safety_review')),
    endpoint_id     TEXT,
    creator         TEXT NOT NULL,
    created_at      REAL NOT NULL,
    scope           TEXT NOT NULL,         -- 请求范围 JSON
    readable        TEXT NOT NULL,         -- 服务端裁定的可读片段快照 JSON
    withdrawn_seen  INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS analysis_results (
    result_id    TEXT PRIMARY KEY,
    task_id      TEXT NOT NULL REFERENCES analysis_tasks(task_id),
    endpoint_id  TEXT NOT NULL,
    produced_at  REAL NOT NULL,
    values_json  TEXT NOT NULL,
    segment_inputs TEXT NOT NULL,          -- 实际使用的片段及版本血缘 JSON
    producer     TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS idem_requests (
    idem_key    TEXT PRIMARY KEY,
    method      TEXT NOT NULL,
    path        TEXT NOT NULL,
    status_code INTEGER NOT NULL,
    body        TEXT NOT NULL,
    created_at  REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS audit_log (
    seq         INTEGER PRIMARY KEY AUTOINCREMENT,
    at          REAL NOT NULL,
    actor       TEXT NOT NULL,
    action      TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    entity_id   TEXT NOT NULL,
    payload     TEXT NOT NULL,
    prev_hash   TEXT NOT NULL,
    entry_hash  TEXT NOT NULL
);
"""


class Store:
    """每个请求持有一个连接；写事务由 ``transaction()`` 串行化。"""

    def __init__(self, path: str):
        self.path = path
        self._lock = threading.Lock()
        self._anchor = None
        if path == ":memory:":
            # 每连接独立的内存库无法跨连接共享；改用共享缓存 URI，
            # 并保留一个锚定连接防止库在连接间隙被回收。
            import uuid

            self.path = f"file:adjudication-{uuid.uuid4().hex}?mode=memory&cache=shared"
            self._anchor = self._connect()
        self._init()

    def _connect(self):
        conn = sqlite3.connect(
            self.path, timeout=10, isolation_level=None, uri=self.path.startswith("file:")
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=10000")
        return conn

    def _init(self):
        conn = self._connect()
        try:
            # executescript 自行管理事务，不要再包显式 BEGIN/COMMIT
            conn.executescript(SCHEMA)
        finally:
            conn.close()

    def connect(self):
        return self._connect()

    @staticmethod
    def dumps(value) -> str:
        return json.dumps(value, ensure_ascii=False, sort_keys=True)

    def write(self, fn):
        """在单个 IMMEDIATE 事务中执行 ``fn(conn)`` 并返回其结果。"""
        with self._lock:
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                result = fn(conn)
                conn.execute("COMMIT")
                return result
            except Exception:
                conn.execute("ROLLBACK")
                raise
            finally:
                conn.close()

    def read(self, fn):
        conn = self._connect()
        try:
            return fn(conn)
        finally:
            conn.close()

    # -- 只增审计哈希链 ---------------------------------------------------

    def append_audit(self, conn, actor, action, entity_type, entity_id, payload):
        at = time.time()
        row = conn.execute(
            "SELECT entry_hash FROM audit_log ORDER BY seq DESC LIMIT 1"
        ).fetchone()
        prev_hash = row["entry_hash"] if row else ""
        body = self.dumps(
            {
                "at": at,
                "actor": actor,
                "action": action,
                "entity_type": entity_type,
                "entity_id": entity_id,
                "payload": payload,
                "prev_hash": prev_hash,
            }
        )
        import hashlib

        entry_hash = hashlib.sha256(body.encode("utf-8")).hexdigest()
        cur = conn.execute(
            """INSERT INTO audit_log
               (at, actor, action, entity_type, entity_id, payload, prev_hash, entry_hash)
               VALUES (?,?,?,?,?,?,?,?)""",
            (
                at,
                actor,
                action,
                entity_type,
                entity_id,
                self.dumps(payload),
                prev_hash,
                entry_hash,
            ),
        )
        return cur.lastrowid, entry_hash

    def verify_chain(self, conn):
        """重算审计链，返回 (是否完整, 断裂序号或 None)。"""
        import hashlib

        prev_hash = ""
        for row in conn.execute("SELECT * FROM audit_log ORDER BY seq"):
            body = self.dumps(
                {
                    "at": row["at"],
                    "actor": row["actor"],
                    "action": row["action"],
                    "entity_type": row["entity_type"],
                    "entity_id": row["entity_id"],
                    "payload": json.loads(row["payload"]),
                    "prev_hash": row["prev_hash"],
                }
            )
            expected = hashlib.sha256(body.encode("utf-8")).hexdigest()
            if row["prev_hash"] != prev_hash or row["entry_hash"] != expected:
                return False, row["seq"]
            prev_hash = row["entry_hash"]
        return True, None
