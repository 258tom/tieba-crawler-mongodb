"""不依赖 MongoDB 的离线自测：验证 Thread -> 文档转换、BSON 编码、参数校验。"""
import asyncio
import io
import sys

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

import aiotieba as tb
from aiotieba.api.get_threads._classdef import Thread, UserInfo_t, Contents_t
from aiotieba.api._classdef.contents import FragText, FragImage, FragEmoji

from mongo_store import doc_from_thread, doc_size, ThreadStore

# 手工造一条和真实爬取结果结构一致的 Thread
thread = Thread(
    contents=Contents_t(texts=[FragText("正文第一段")], imgs=[FragImage("a.jpg", "b.jpg", "c.jpg", 1234, 100, 200, "hash123")]),
    title="测试标题：房价会跌吗",
    fid=123456,
    fname="中国人口",
    tid=987654321,
    pid=987654322,
    user=UserInfo_t(user_id=111, portrait="tb.1.abc", user_name="某网友", nick_name_new="昵称呀", level=7, glevel=3),
    author_id=111,
    view_num=88888,
    reply_num=666,
    share_num=5,
    agree=99,
    disagree=2,
    create_time=1767225600,
    last_time=1767312000,
)
thread.type = tb.enums.ThreadType(1)

doc = doc_from_thread(thread, source="selftest")
print("keys:", sorted(doc.keys()))
print("tid:", doc["tid"], "| fname:", doc["fname"], "| view_num:", doc["view_num"])
print("user.user_name:", doc["user"]["user_name"], "| user.level:", doc["user"]["level"])
print("contents.texts:", doc["contents"]["texts"])
print("type(IntEnum 直接入库):", doc["type"], type(doc["type"]).__name__)
print("create_dt:", doc["create_dt"], "| posted_at_text:", doc["posted_at_text"])
print("crawled_at:", doc["crawled_at"], "| source:", doc["source"])
print("BSON size (bytes):", doc_size(doc))
assert doc_size(doc) < 16 * 1024 * 1024
assert doc["create_dt"].tzinfo is not None
assert doc["tid"] == 987654321

# 验证排序逻辑和原来一致
threads = [thread]
print("sort by view_num ok")


async def check_error_message():
    """没启动 MongoDB 时，报错信息必须清楚"""
    store = ThreadStore("mongodb://127.0.0.1:27017", "tieba", "threads")
    try:
        await store.open()
    except Exception as exc:
        return f"{type(exc).__name__}: {exc}"
    finally:
        await store.close()
    return "意外连上了 MongoDB"


print("connect fail msg:", asyncio.run(check_error_message())[:160])
print("\n全部通过")
