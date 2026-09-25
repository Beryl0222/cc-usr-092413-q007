"""颅内决策实验编排的服务入口：健康检查 + 中止数据解封裁决 HTTP 接口。"""

import argparse
import json
import re
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from core import AdjudicationService, DomainError
from storage import Store

SERVICE_ID = "intracranial-decision-study"
SERVICE_NAME = "颅内决策实验编排"
DEFAULT_DB = "adjudication.db"


def health_payload():
    """返回稳定的服务身份信息。"""
    return {"status": "ok", "service": SERVICE_ID, "name": SERVICE_NAME}


def make_handler(service: AdjudicationService):
    """绑定领域服务，构造请求处理器（便于测试注入独立库）。"""

    class Handler(BaseHTTPRequestHandler):
        """健康检查与领域接口。"""

        protocol_version = "HTTP/1.1"

        # -- 基础 ------------------------------------------------------

        def _send(self, status, payload, replayed=False):
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            if replayed:
                self.send_header("Idempotent-Replay", "true")
            self.end_headers()
            self.wfile.write(body)

        def _error(self, status, code, message):
            self._send(status, {"error": {"code": code, "message": message}})

        def _body(self):
            length = int(self.headers.get("Content-Length") or 0)
            if length == 0:
                return {}
            raw = self.rfile.read(length)
            try:
                data = json.loads(raw.decode("utf-8"))
            except (ValueError, UnicodeDecodeError) as exc:
                raise DomainError(400, "bad_json", f"请求体不是合法 JSON: {exc}")
            if not isinstance(data, dict):
                raise DomainError(400, "bad_json", "请求体必须是 JSON 对象")
            return data

        def _actor(self, payload):
            return self.headers.get("X-Actor") or payload.get("actor") or "anonymous"

        def log_message(self, *_args):
            return

        # -- 路由 ------------------------------------------------------

        def do_GET(self):
            path = self.path.split("?", 1)[0]
            try:
                if path == "/health":
                    self._send(200, health_payload())
                    return
                if path == "/safety-events/pending":
                    self._send(200, {"pending": service.store.read(service.list_pending)})
                    return
                m = re.fullmatch(r"/safety-events/([^/]+)", path)
                if m:
                    self._send(200, service.store.read(
                        lambda c: service.get_event(c, m.group(1))))
                    return
                m = re.fullmatch(r"/analysis-results/([^/]+)/trace", path)
                if m:
                    self._send(200, service.store.read(
                        lambda c: service.trace_result(c, m.group(1))))
                    return
                m = re.fullmatch(r"/audit/([^/]+)/([^/]+)", path)
                if m:
                    self._send(200, {"audit": service.store.read(
                        lambda c: service.audit_for(c, m.group(1), m.group(2)))})
                    return
                if path == "/audit/verify":
                    ok, broken = service.store.read(service.store.verify_chain)
                    self._send(200, {"chain_intact": ok, "broken_seq": broken})
                    return
                self._error(404, "not_found", "未知路由")
            except DomainError as exc:
                self._error(exc.status, exc.code, exc.message)

        def do_POST(self):
            path = self.path.split("?", 1)[0]
            try:
                payload = self._body()
            except DomainError as exc:
                self._error(exc.status, exc.code, exc.message)
                return
            try:
                result = self._dispatch_post(path, payload)
            except DomainError as exc:
                self._error(exc.status, exc.code, exc.message)
                return
            if result is None:
                self._error(404, "not_found", "未知路由")
                return
            status, body, replayed = result
            self._send(status, body, replayed)

        def _dispatch_post(self, path, payload):
            """返回 (status, body, replayed)；未匹配返回 None。"""
            actor = self._actor(payload)
            idem_key = self.headers.get("Idempotency-Key")

            route = self._match(path)
            if route is None:
                return None
            action, resource_type = route

            def run(conn):
                if idem_key:
                    prior = conn.execute(
                        "SELECT * FROM idem_requests WHERE idem_key=?", (idem_key,)
                    ).fetchone()
                    if prior:
                        if prior["method"] != "POST" or prior["path"] != path:
                            raise DomainError(
                                409, "idempotency_conflict",
                                "同一 Idempotency-Key 不能用于不同请求")
                        body = json.loads(prior["body"])
                        if isinstance(body, dict):
                            body = dict(body, idempotent_replay=True)
                        return prior["status_code"], body, True
                status, body = action(conn, actor, payload)
                if idem_key:
                    conn.execute(
                        """INSERT INTO idem_requests
                           (idem_key, method, path, status_code, body, created_at)
                           VALUES (?,?,?,?,?,?)""",
                        (idem_key, "POST", path, status,
                         json.dumps(body, ensure_ascii=False),
                         time.time()),
                    )
                return status, body, False

            return service.store.write(run)

        def _match(self, path):
            """把路径映射为 (action, resource_type)。"""
            def wrap(fn, resource):
                return (lambda conn, actor, p: _created(fn(conn, actor, p))), resource

            def _created(outcome):
                body, replayed = outcome
                if replayed:
                    body = dict(body, idempotent_replay=True)
                    return 200, body
                return 201, body

            simple = {
                "/participants": (service.register_participant, "participant"),
                "/sessions": (service.start_session, "session"),
                "/segments": (service.register_segment, "segment"),
                "/device-states": (service.register_device_state, "device_state"),
                "/medical-orders": (service.register_medical_order, "medical_order"),
                "/safety-events": (service.declare_safety_event, "safety_event"),
                "/analysis-tasks": (service.create_analysis_task, "analysis_task"),
                "/analysis-results": (service.record_analysis_result, "analysis_result"),
            }
            if path in simple:
                fn, resource = simple[path]
                return wrap(fn, resource)

            m = re.fullmatch(r"/participants/([^/]+)/withdraw", path)
            if m:
                pid = m.group(1)
                return (lambda conn, actor, p: _created(
                    service.withdraw_participant(conn, actor, pid))), "participant"
            m = re.fullmatch(r"/safety-events/([^/]+)/reviews", path)
            if m:
                eid = m.group(1)
                return (lambda conn, actor, p: _created(
                    service.submit_review(conn, actor, eid, p))), "review"
            m = re.fullmatch(r"/safety-events/([^/]+)/apply", path)
            if m:
                eid = m.group(1)
                return (lambda conn, actor, p: _created(
                    service.apply_release(conn, actor, eid, p))), "decision_version"
            m = re.fullmatch(r"/safety-events/([^/]+)/clock-corrections", path)
            if m:
                eid = m.group(1)
                return (lambda conn, actor, p: _created(
                    service.propose_clock_correction(conn, actor, eid, p))), "clock_correction"
            m = re.fullmatch(r"/clock-corrections/([^/]+)/decision", path)
            if m:
                cid = m.group(1)
                return (lambda conn, actor, p: _created(
                    service.decide_clock_correction(conn, actor, cid, p))), "clock_correction"
            return None

    return Handler


def build_service(db_path: str) -> AdjudicationService:
    return AdjudicationService(Store(db_path))


def main():
    parser = argparse.ArgumentParser(description=SERVICE_NAME)
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--db", default=DEFAULT_DB, help="SQLite 数据库路径")
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    if args.check:
        import os
        import tempfile

        assert health_payload()["service"] == SERVICE_ID
        with tempfile.TemporaryDirectory() as tmp:
            svc = build_service(os.path.join(tmp, "check.db"))
            pending = svc.store.read(svc.list_pending)
            assert pending == []
            ok, _ = svc.store.read(svc.store.verify_chain)
            assert ok
        print("基础检查通过")
        return
    service = build_service(args.db)
    ThreadingHTTPServer(("0.0.0.0", args.port), make_handler(service)).serve_forever()


if __name__ == "__main__":
    main()
