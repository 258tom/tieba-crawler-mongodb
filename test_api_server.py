"""
接口层离线自测：用内存里的假集合替换 MongoDB，起真实的 HTTP 服务打请求，
验证路由、分页、排序白名单、参数校验和中文 JSON 编码。
（等 MongoDB 装好后，再用 api_server.py 打真库。）
"""
import io
import json
import sys
import threading
import urllib.error
import urllib.request
from datetime import datetime, timezone

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

import api_server
from http.server import ThreadingHTTPServer

FORUMS = ["中国人口", "弱智"]


def make_doc(tid: int, fname: str, views: int) -> dict:
    return {
        "tid": tid, "fname": fname, "fid": 1000, "title": f"{fname}的第{tid}个帖子：房价",
        "author_id": 111, "user": {"user_name": "某网友", "nick_name_new": "昵称", "level": 7},
        "view_num": views, "reply_num": views // 10, "share_num": 1, "agree": 5, "disagree": 0,
        "is_good": False, "is_top": False, "type": 1,
        "create_time": 1700000000 + tid, "last_time": 1700000000 + tid,
        "posted_at_text": "2023-11-15 06:13:20",
        "crawled_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
    }


DOCS = [make_doc(tid, FORUMS[tid % 2], (tid * 37) % 9999) for tid in range(1, 26)]


class FakeCursor:
    def __init__(self, docs):
        self._docs = list(docs)

    def sort(self, field, direction):
        self._docs.sort(key=lambda d: d.get(field) or 0, reverse=(direction == -1))
        return self

    def skip(self, n):
        self._docs = self._docs[n:]
        return self

    def limit(self, n):
        self._docs = self._docs[:n]
        return self

    def __iter__(self):
        return iter(self._docs)


class FakeCollection:
    def count_documents(self, query):
        return len(self._match(query))

    def _match(self, query):
        out = list(DOCS)
        for field, cond in query.items():
            if isinstance(cond, dict) and "$regex" in cond:
                import re as _re
                rx = _re.compile(cond["$regex"], _re.IGNORECASE)
                out = [d for d in out if rx.search(d.get(field) or "")]
            else:
                out = [d for d in out if d.get(field) == cond]
        return out

    def find(self, query=None, projection=None):
        import copy as _copy
        docs = _copy.deepcopy(self._match(query or {}))
        if projection:
            keep = [k for k, v in projection.items() if v and "." not in k]
            docs = [{k: d[k] for k in keep if k in d} for d in docs]
        return FakeCursor(docs)

    def find_one(self, query):
        found = self._match(query)
        return found[0] if found else None

    def distinct(self, field):
        return sorted({d.get(field) for d in DOCS if d.get(field)})

    def aggregate(self, pipeline):
        grouped = {}
        for doc in DOCS:
            row = grouped.setdefault(doc["fname"], {"_id": doc["fname"], "threads": 0, "total_views": 0,
                                                    "total_replies": 0, "max_views": 0})
            row["threads"] += 1
            row["total_views"] += doc["view_num"]
            row["total_replies"] += doc["reply_num"]
            row["max_views"] = max(row["max_views"], doc["view_num"])
        return sorted(grouped.values(), key=lambda r: -r["threads"])


api_server.collection = FakeCollection()

server = ThreadingHTTPServer(("127.0.0.1", 0), api_server.Handler)
port = server.server_address[1]
threading.Thread(target=server.serve_forever, daemon=True).start()
BASE = f"http://127.0.0.1:{port}"


def get(path):
    try:
        with urllib.request.urlopen(BASE + path, timeout=10) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode("utf-8"))


failures = []


def check(label, cond, extra=""):
    print(("PASS  " if cond else "FAIL  ") + label + (f"   {extra}" if extra else ""))
    if not cond:
        failures.append(label)


status, body = get("/")
check("GET / 接口说明", status == 200 and body["name"] == "贴吧爬虫数据查询接口")

status, body = get("/api/forums")
check("GET /api/forums", status == 200 and body["forums"] == FORUMS, str(body))

status, body = get("/api/threads?fname=%E4%B8%AD%E5%9B%BD%E4%BA%BA%E5%8F%A3&sort=view_num&order=desc&page=1&size=5")
views = [item["view_num"] for item in body.get("items", [])]
check("GET /api/threads 中文参数+降序分页", status == 200 and body["total"] == 12 and len(views) == 5 and views == sorted(views, reverse=True),
      f"total={body.get('total')} views={views}")

status, body = get("/api/threads?page=999&size=5")
check("GET /api/threads 越界页返回空列表", status == 200 and body["items"] == [] and body["total"] == 25)

status, body = get("/api/threads?sort=%3B%20drop%20db")
check("GET /api/threads 非法 sort 被拒绝", status == 400 and "sort 只支持" in body["error"], str(body))

status, body = get("/api/top?fname=%E4%B8%AD%E5%9B%BD%E4%BA%BA%E5%8F%A3&by=views&limit=3")
check("GET /api/top by=views", status == 200 and len(body["items"]) == 3, json.dumps(body["items"][0], ensure_ascii=False)[:90])

status, body = get("/api/top?by=%E4%B8%8D%E5%AD%98%E5%9C%A8")
check("GET /api/top 非法 by 被拒绝", status == 400)

status, body = get("/api/search?q=%E6%88%BF%E4%BB%B7&limit=4")
check("GET /api/search 中文关键词", status == 200 and body["count"] == 4)

status, body = get("/api/search")
check("GET /api/search 缺 q 被拒绝", status == 400)

status, body = get(f"/api/threads/{DOCS[3]['tid']}")
check("GET /api/threads/{tid} 详情", status == 200 and body["tid"] == DOCS[3]["tid"])

status, body = get("/api/threads/999999999")
check("GET /api/threads/{tid} 不存在返回 404", status == 404)

status, body = get("/api/nope")
check("未知路由返回 404", status == 404)

# 中文 JSON 必须是 UTF-8 而不是 \uXXXX 转义
with urllib.request.urlopen(BASE + "/api/forums", timeout=10) as resp:
    raw = resp.read().decode("utf-8")
check("响应为 UTF-8 中文（非 unicode 转义）", "中国人口" in raw and "\\u4e2d" not in raw, raw)

# POST /api/crawl 参数校验（不真的爬，只验证路由和 clamp）
import http.client
import importlib

# 关键：把真正的 crawl() 换成假的。
# 以前这个测试会真的在后台线程里发起爬取（只是碰巧本地没配 BDUSS 才没联网），
# 配上凭据后就会真打贴吧接口 —— 测试绝不能有网络副作用。
crawler_mod = importlib.import_module("crawler")
crawl_calls = []


async def fake_crawl(fname, max_page, worker_num, **kwargs):
    crawl_calls.append({"fname": fname, "max_page": max_page, "worker_num": worker_num})
    return {"seen": 0, "before": 0, "after": 0, "inserted": 0}


crawler_mod.crawl = fake_crawl

conn = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
conn.request("POST", "/api/crawl", body=json.dumps({"fname": "不存在的吧_selftest", "max_page": 9999}), headers={"Content-Type": "application/json"})
resp = conn.getresponse()
crawl_body = json.loads(resp.read().decode("utf-8"))
check("POST /api/crawl 参数 clamp 到上限", resp.status == 202 and crawl_body["max_page"] == 500, str(crawl_body))
conn.close()

# 等后台线程把假 crawl 调完，确认它真的被触发了、且拿到的是 clamp 之后的值
import time as _time
for _ in range(50):
    if crawl_calls:
        break
    _time.sleep(0.1)
check("POST /api/crawl 真的触发了爬取（且用的是 clamp 后的参数）",
      bool(crawl_calls) and crawl_calls[0]["max_page"] == 500, str(crawl_calls))

server.shutdown()
print()
if failures:
    print("失败项:", failures)
    sys.exit(1)
print("接口层全部通过")
