"""颅内决策实验编排的运行入口与 HTTP 接口。

领域规则集中在 adjudication.Store；本模块只做路由、鉴权头解析与
JSON 编解码。除 /health 外，所有接口要求 X-Actor-Role 头为已知角色，
写操作另要求 X-Actor-Id 以签署决定。
"""

import argparse
import json
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from adjudication import KNOWN_ROLES, ApiError, Store

SERVICE_ID = "intracranial-decision-study"
SERVICE_NAME = "颅内决策实验编排"


def health_payload():
    """返回稳定的服务身份信息。"""
    return {"status": "ok", "service": SERVICE_ID, "name": SERVICE_NAME}


def _read_json(handler):
    length = int(handler.headers.get("Content-Length") or 0)
    if length == 0:
        return {}
    raw = handler.rfile.read(length)
    try:
        data = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise ApiError(400, "invalid_json", "请求体不是合法 JSON")
    if not isinstance(data, dict):
        raise ApiError(400, "invalid_json", "请求体必须是 JSON 对象")
    return data


def _actor(handler, require_identity):
    role = handler.headers.get("X-Actor-Role")
    if role not in KNOWN_ROLES:
        raise ApiError(403, "unknown_role",
                       "X-Actor-Role 必须是已知角色",
                       {"known_roles": sorted(KNOWN_ROLES)})
    actor_id = handler.headers.get("X-Actor-Id")
    if require_identity and not actor_id:
        raise ApiError(403, "missing_actor", "写操作必须提供 X-Actor-Id 以签署决定")
    return {"role": role, "actor_id": actor_id or "anonymous"}


# 路由表：(方法, 路径正则, 处理函数)。处理函数签名为
# (store, actor, payload, query, *路径参数) -> (状态码, 响应体)。
def _routes(store):
    return [
        ("POST", re.compile(r"^/participants$"),
         lambda a, p, q: store.register_participant(a, p)),
        ("GET", re.compile(r"^/participants/([^/]+)$"),
         lambda a, p, q, pid: store.get_participant(a, pid)),
        ("POST", re.compile(r"^/participants/([^/]+)/withdrawal$"),
         lambda a, p, q, pid: store.withdraw_participant(a, pid, p)),
        ("POST", re.compile(r"^/segments$"),
         lambda a, p, q: store.ingest_segment(a, p)),
        ("GET", re.compile(r"^/segments/([^/]+)$"),
         lambda a, p, q, sid: store.get_segment(a, sid)),
        ("POST", re.compile(r"^/suspension-events$"),
         lambda a, p, q: store.create_suspension_event(a, p)),
        ("GET", re.compile(r"^/suspension-events/([^/]+)$"),
         lambda a, p, q, eid: store.get_event(a, eid)),
        ("POST", re.compile(r"^/suspension-events/([^/]+)/safety-assessment$"),
         lambda a, p, q, eid: store.submit_safety_assessment(a, eid, p)),
        ("POST", re.compile(r"^/suspension-events/([^/]+)/methodology-assessment$"),
         lambda a, p, q, eid: store.submit_methodology_assessment(a, eid, p)),
        ("POST", re.compile(r"^/suspension-events/([^/]+)/dispositions$"),
         lambda a, p, q, eid: store.submit_disposition(a, eid, p)),
        ("GET", re.compile(r"^/adjudication-queue$"),
         lambda a, p, q: store.adjudication_queue(a)),
        ("POST", re.compile(r"^/clock-corrections$"),
         lambda a, p, q: store.propose_clock_correction(a, p)),
        ("POST", re.compile(r"^/analysis-tasks$"),
         lambda a, p, q: store.run_analysis(a, p)),
        ("GET", re.compile(r"^/analysis-results/([^/]+)$"),
         lambda a, p, q, rid: store.get_analysis_result(a, rid)),
        ("GET", re.compile(r"^/audit/analysis-results/([^/]+)$"),
         lambda a, p, q, rid: store.trace_analysis_result(a, rid)),
        ("GET", re.compile(r"^/audit/suspension-events/([^/]+)$"),
         lambda a, p, q, eid: store.trace_suspension_event(a, eid)),
        ("GET", re.compile(r"^/ethics-reviews$"),
         lambda a, p, q: store.list_ethics_reviews(
             a, status=(q.get("status", [None])[0]))),
        ("POST", re.compile(r"^/ethics-reviews/([^/]+)/resolve$"),
         lambda a, p, q, rid: store.resolve_ethics_review(a, rid, p)),
    ]


class Handler(BaseHTTPRequestHandler):
    """提供健康检查与临床中止数据解封裁决接口。"""

    store = None  # 默认内存态；make_handler 可注入持久化 Store。

    def _store(self):
        if self.__class__.store is None:
            self.__class__.store = Store()
        return self.__class__.store

    def do_GET(self):
        self._dispatch("GET")

    def do_POST(self):
        self._dispatch("POST")

    def _dispatch(self, method):
        parsed = urlparse(self.path)
        try:
            if method == "GET" and parsed.path == "/health":
                return self._send(200, health_payload())
            matched = None
            for route_method, pattern, handler in _routes(self._store()):
                if route_method != method:
                    continue
                match = pattern.match(parsed.path)
                if match:
                    matched = (handler, match.groups())
                    break
            if matched is None:
                raise ApiError(404, "not_found", "接口不存在")
            handler, groups = matched
            actor = _actor(self, require_identity=(method == "POST"))
            payload = _read_json(self) if method == "POST" else {}
            query = parse_qs(parsed.query)
            status, body = handler(actor, payload, query, *groups)
            return self._send(status, body)
        except ApiError as err:
            self._send(err.status, {"error": {
                "code": err.code, "message": err.message, "details": err.details}})
        except Exception:  # 不向调用方泄露内部细节
            self._send(500, {"error": {
                "code": "internal_error", "message": "服务内部错误", "details": {}}})

    def _send(self, status, body):
        raw = json.dumps(body, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def log_message(self, *_args):
        return


def make_handler(store):
    """构造绑定指定 Store 的处理器类，便于测试与多实例部署。"""

    class BoundHandler(Handler):
        pass

    BoundHandler.store = store
    return BoundHandler


def main():
    parser = argparse.ArgumentParser(description=SERVICE_NAME)
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--data-dir", default=None,
                        help="状态文件目录（默认 <目录>/state.json；缺省为内存态）")
    args = parser.parse_args()
    if args.check:
        assert health_payload()["service"] == SERVICE_ID
        print("基础检查通过")
        return
    store = Store(f"{args.data_dir}/state.json") if args.data_dir else Store()
    ThreadingHTTPServer(("0.0.0.0", args.port), make_handler(store)).serve_forever()


if __name__ == "__main__":
    main()
