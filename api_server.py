r"""
REST 查询接口：用标准库 http.server 给 MongoDB 里的爬虫数据套一层 HTTP 接口。

为什么不用 FastAPI / Flask？
    你现在的虚拟环境里没装它们，而这个文件零依赖，拿到就能跑，不需要联网装包。
    等以后想上生产，把 route() 里的逻辑搬到 FastAPI 里只需要换一层壳。

接口清单（默认 http://127.0.0.1:8000）：
    GET  /                                 接口说明
    GET  /api/health                       健康检查（能否连上 MongoDB）
    GET  /api/stats                        汇总统计：每个吧多少帖、总浏览量
    GET  /api/forums                       有哪些吧的数据
    GET  /api/threads?fname=中国人口&sort=view_num&order=desc&page=1&size=10
                                           分页查询帖子列表
    GET  /api/threads/{tid}                查单条帖子详情（含正文全文）
    GET  /api/top?fname=中国人口&by=views&limit=10
                                           浏览量/回复数排行榜
    GET  /api/search?q=房价&fname=中国人口  标题关键词搜索
    POST /api/crawl                        触发一次爬取，body 可传 {"fname":"中国人口","max_page":32}

启动：
    python api_server.py
    python api_server.py --port 8000

测试：
    curl "http://127.0.0.1:8000/api/top?fname=中国人口&by=views&limit=5"
"""

from __future__ import annotations

import argparse
import asyncio
import importlib
import json
import re
import sys
import threading
from datetime import date, datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse
from uuid import UUID

from bson import ObjectId
from pymongo import DESCENDING, MongoClient

# tieba_env 必须排在 mongo_store 前面：它负责把 .env 读进环境变量，
# 而 mongo_store 在 import 时就要读 MONGO_URI / MONGO_DB / MONGO_COLLECTION
from tieba_env import load_dotenv
from mongo_store import MONGO_COLLECTION, MONGO_DB, MONGO_URI

load_dotenv()

# 允许排序的字段白名单：不能让用户随便传字段名，否则容易被构造慢查询
SORTABLE = {
    "view_num": "view_num",
    "reply_num": "reply_num",
    "create_time": "create_time",
    "last_time": "last_time",
    "agree": "agree",
    "share_num": "share_num",
}

# 列表接口返回的字段（不含正文，避免响应过大）
LIST_FIELDS = {
    "_id": 0, "tid": 1, "fname": 1, "fid": 1, "title": 1,
    "author_id": 1, "user.user_name": 1, "user.nick_name_new": 1, "user.level": 1,
    "view_num": 1, "reply_num": 1, "share_num": 1, "agree": 1, "disagree": 1,
    "is_good": 1, "is_top": 1, "type": 1,
    "create_time": 1, "last_time": 1, "posted_at_text": 1, "crawled_at": 1,
    "first_crawled_at": 1,
}

# 同步客户端是线程安全的，内部自带连接池，多个请求线程共用一个实例即可
client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000, tz_aware=True)
collection = client[MONGO_DB][MONGO_COLLECTION]


def to_jsonable(obj):
    """把 MongoDB 返回的 BSON 类型转成 JSON 能表示的类型"""
    if isinstance(obj, ObjectId):
        return str(obj)
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, UUID):
        return str(obj)
    if isinstance(obj, bytes):
        return obj.decode("utf-8", "replace")
    if isinstance(obj, dict):
        return {key: to_jsonable(value) for key, value in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [to_jsonable(item) for item in obj]
    return obj


def clamp_int(raw: str | None, default: int, low: int, high: int) -> int:
    """把查询参数转成安全范围内的整数"""
    try:
        value = int(raw) if raw is not None else default
    except (TypeError, ValueError):
        return default
    return max(low, min(high, value))


# ------------------------------------------------------------------ 业务逻辑
def api_health() -> dict:
    info = client.server_info()
    return {"ok": True, "mongodb_version": info.get("version"), "db": MONGO_DB, "collection": MONGO_COLLECTION}


def api_stats() -> dict:
    pipeline = [
        {"$group": {
            "_id": "$fname",
            "threads": {"$sum": 1},
            "total_views": {"$sum": "$view_num"},
            "total_replies": {"$sum": "$reply_num"},
            "max_views": {"$max": "$view_num"},
        }},
        {"$sort": {"threads": -1}},
    ]
    by_forum = list(collection.aggregate(pipeline))
    return {"total_documents": collection.count_documents({}), "by_forum": to_jsonable(by_forum)}


def api_forums() -> dict:
    names = collection.distinct("fname")
    return {"count": len(names), "forums": sorted(name for name in names if name)}


def api_threads(params: dict) -> dict:
    fname = (params.get("fname") or [None])[0]
    sort_key = (params.get("sort") or ["view_num"])[0]
    order = (params.get("order") or ["desc"])[0].lower()
    keyword = (params.get("q") or [None])[0]
    page = clamp_int((params.get("page") or [None])[0], 1, 1, 100000)
    size = clamp_int((params.get("size") or [None])[0], 10, 1, 100)

    if sort_key not in SORTABLE:
        raise ValueError(f"sort 只支持 {sorted(SORTABLE)}，收到 {sort_key!r}")

    query: dict = {}
    if fname:
        query["fname"] = fname
    if keyword:
        query["title"] = {"$regex": re.escape(keyword), "$options": "i"}

    direction = DESCENDING if order != "asc" else 1
    total = collection.count_documents(query)
    cursor = (
        collection.find(query, LIST_FIELDS)
        .sort(SORTABLE[sort_key], direction)
        .skip((page - 1) * size)
        .limit(size)
    )
    return {
        "page": page, "size": size, "total": total,
        "pages": (total + size - 1) // size,
        "items": to_jsonable(list(cursor)),
    }


def api_thread_detail(tid: int) -> dict | None:
    doc = collection.find_one({"tid": tid})
    return to_jsonable(doc) if doc else None


def api_top(params: dict) -> dict:
    by = (params.get("by") or ["views"])[0]
    field = {"views": "view_num", "replies": "reply_num", "agree": "agree"}.get(by)
    if field is None:
        raise ValueError("by 只支持 views / replies / agree")
    limit = clamp_int((params.get("limit") or [None])[0], 10, 1, 100)
    fname = (params.get("fname") or [None])[0]

    query = {"fname": fname} if fname else {}
    cursor = collection.find(query, LIST_FIELDS).sort(field, DESCENDING).limit(limit)
    return {"by": field, "fname": fname, "items": to_jsonable(list(cursor))}


def api_search(params: dict) -> dict:
    keyword = (params.get("q") or [""])[0].strip()
    if not keyword:
        raise ValueError("缺少查询参数 q")
    fname = (params.get("fname") or [None])[0]
    limit = clamp_int((params.get("limit") or [None])[0], 20, 1, 100)

    query: dict = {"title": {"$regex": re.escape(keyword), "$options": "i"}}
    if fname:
        query["fname"] = fname
    cursor = collection.find(query, LIST_FIELDS).sort("view_num", DESCENDING).limit(limit)
    items = to_jsonable(list(cursor))
    return {"q": keyword, "fname": fname, "count": len(items), "items": items}


def api_crawl(body: dict) -> dict:
    """触发一次爬取。爬取比较慢，放到后台线程里跑，接口立刻返回 202"""
    fname = str(body.get("fname") or "中国人口")
    max_page = clamp_int(str(body.get("max_page", 32)), 32, 1, 500)
    worker_num = clamp_int(str(body.get("worker_num", 8)), 8, 1, 16)

    def _run() -> None:
        # 保证脚本目录在 sys.path 上（从别的工作目录启动服务时也能 import 到 crawler）
        script_dir = str(Path(__file__).resolve().parent)
        if script_dir not in sys.path:
            sys.path.insert(0, script_dir)
        crawl = importlib.import_module("crawler").crawl

        async def _job() -> None:
            try:
                stats = await crawl(fname, max_page=max_page, worker_num=worker_num)
                print(f"[crawl] {fname} 爬取完成：新增 {stats['inserted']} 条，库中共 {stats['after']} 条")
            except Exception as exc:
                # 后台线程里的异常不会被主线程感知，必须自己吞掉并打印，
                # 否则爬取失败时接口那边完全看不到任何线索
                print(f"[crawl] {fname} 爬取失败：{type(exc).__name__}: {exc}")

        asyncio.run(_job())

    thread = threading.Thread(target=_run, name=f"crawl-{fname}", daemon=True)
    thread.start()
    return {"accepted": True, "fname": fname, "max_page": max_page, "worker_num": worker_num}


ROUTES: list[tuple[str, str, str]] = [
    ("GET", r"^/$", "index"),
    ("GET", r"^/api/health$", "health"),
    ("GET", r"^/api/stats$", "stats"),
    ("GET", r"^/api/forums$", "forums"),
    ("GET", r"^/api/threads$", "threads"),
    ("GET", r"^/api/threads/(?P<tid>\d+)$", "thread_detail"),
    ("GET", r"^/api/top$", "top"),
    ("GET", r"^/api/search$", "search"),
    ("POST", r"^/api/crawl$", "crawl"),
]

INDEX_TEXT = {
    "name": "贴吧爬虫数据查询接口",
    "endpoints": [
        "GET  /api/health",
        "GET  /api/stats",
        "GET  /api/forums",
        "GET  /api/threads?fname=中国人口&sort=view_num&order=desc&page=1&size=10",
        "GET  /api/threads/{tid}",
        "GET  /api/top?fname=中国人口&by=views&limit=10",
        "GET  /api/search?q=关键词&fname=中国人口",
        "POST /api/crawl    body: {\"fname\":\"中国人口\",\"max_page\":32}",
    ],
    "note": "示例：http://127.0.0.1:8000/api/top?fname=中国人口&by=views&limit=5",
}


class Handler(BaseHTTPRequestHandler):
    server_version = "TiebaMongoAPI/1.0"
    protocol_version = "HTTP/1.1"

    # ---------------------------------------------------------- 响应工具
    def _send(self, payload: dict, status: int = 200) -> None:
        data = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(data)

    def _error(self, status: int, message: str) -> None:
        self._send({"ok": False, "error": message}, status)

    # -------------------------------------------------------------- 路由
    def do_GET(self) -> None:  # noqa: N802 (标准库要求的方法名)
        self._dispatch("GET")

    def do_POST(self) -> None:  # noqa: N802
        self._dispatch("POST")

    def do_OPTIONS(self) -> None:  # noqa: N802
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def _dispatch(self, method: str) -> None:
        parsed = urlparse(self.path)
        for route_method, pattern, name in ROUTES:
            if route_method != method:
                continue
            match = re.match(pattern, parsed.path)
            if not match:
                continue
            params = parse_qs(parsed.query)
            try:
                self._handle(name, match.groupdict(), params)
            except ValueError as exc:
                self._error(400, str(exc))
            except Exception as exc:  # 兜底，避免一个请求把连接线程搞崩
                self._error(500, f"{type(exc).__name__}: {exc}")
            return
        self._error(404, f"没有这个接口: {method} {parsed.path}")

    def _handle(self, name: str, path_params: dict, params: dict) -> None:
        if name == "index":
            self._send(INDEX_TEXT)
        elif name == "health":
            self._send(api_health())
        elif name == "stats":
            self._send(api_stats())
        elif name == "forums":
            self._send(api_forums())
        elif name == "threads":
            self._send(api_threads(params))
        elif name == "top":
            self._send(api_top(params))
        elif name == "search":
            self._send(api_search(params))
        elif name == "thread_detail":
            doc = api_thread_detail(int(path_params["tid"]))
            if doc is None:
                self._error(404, "没有找到这个 tid")
            else:
                self._send(doc)
        elif name == "crawl":
            length = clamp_int(self.headers.get("Content-Length"), 0, 0, 1 << 20)
            raw = self.rfile.read(length) if length else b"{}"
            try:
                body = json.loads(raw or b"{}")
            except json.JSONDecodeError:
                raise ValueError("请求体不是合法 JSON") from None
            self._send(api_crawl(body), status=202)

    def log_message(self, fmt: str, *args) -> None:
        print(f"[{datetime.now(tz=timezone.utc).astimezone():%H:%M:%S}] {self.address_string()} {fmt % args}")


def main() -> None:
    parser = argparse.ArgumentParser(description="贴吧爬虫数据查询接口")
    parser.add_argument("--host", default="127.0.0.1", help="监听地址，默认只允许本机访问")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()

    server = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"MongoDB  : {MONGO_URI} -> {MONGO_DB}.{MONGO_COLLECTION}")
    try:
        print(f"服务已启动: http://{args.host}:{args.port}/")
        print(f"试试看    : http://{args.host}:{args.port}/api/top?fname=中国人口&by=views&limit=5")
    except UnicodeEncodeError:  # 某些 Windows 控制台代码页会炸，忽略即可
        pass
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n正在关闭 ...")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
