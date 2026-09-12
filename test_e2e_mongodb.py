"""
真实 MongoDB 端到端验证（不是假集合，是真的连 127.0.0.1:27017）。

验证内容：
1. 连接 + 建索引
2. 真实 aiotieba Thread 对象 -> 文档 -> 入库
3. upsert 幂等性：同一批数据写两次，条数不变（这是"爬多次不重复"的核心保证）
4. 字段类型在 MongoDB 里正确（IntEnum、Date、嵌套 user/contents）
5. 各类查询：Top / 最新 / 搜索 / 统计
"""
import asyncio
import io
import sys

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

import aiotieba as tb
from aiotieba.api.get_threads._classdef import Contents_t, Thread, UserInfo_t
from aiotieba.api._classdef.contents import FragImage, FragText

from mongo_store import MONGO_COLLECTION, MONGO_DB, ThreadStore

FNAME = "端到端验证吧"
failures = []


def check(label, cond, extra=""):
    print(("PASS  " if cond else "FAIL  ") + label + (f"   {extra}" if extra else ""))
    if not cond:
        failures.append(label)


def make_thread(tid: int, views: int, title: str) -> Thread:
    t = Thread(
        contents=Contents_t(texts=[FragText(f"{title} 的正文")],
                            imgs=[FragImage("s.jpg", "b.jpg", "o.jpg", 1024, 100, 200, "h" * 32)]),
        title=title, fid=88888, fname=FNAME, tid=tid, pid=tid + 1,
        user=UserInfo_t(user_id=999, portrait="tb.1.x", user_name="验证用户", nick_name_new="昵称", level=9, glevel=4),
        author_id=999, view_num=views, reply_num=views // 7, share_num=3, agree=views // 3, disagree=1,
        create_time=1767225600 + tid, last_time=1767312000 + tid,
    )
    t.type = tb.enums.ThreadType(1)
    return t


async def main():
    store = ThreadStore()
    await store.open()
    print(f"已连接 {MONGO_DB}.{MONGO_COLLECTION}\n")

    async with store:
        # --- 1. 建索引 ---
        names = await store.ensure_indexes()
        check("建索引", len(names) == 6, f"{names}")
        info = await store.collection.index_information()
        check("uniq_tid 是唯一索引", info["uniq_tid"].get("unique") is True)
        print("      索引:", ", ".join(info.keys()))

        # --- 2. 清掉本吧的旧测试数据，保证从干净状态开始 ---
        await store.collection.delete_many({"fname": FNAME})
        check("清空测试吧旧数据", await store.count(FNAME) == 0)

        # --- 3. 首次入库 ---
        batch = [make_thread(1000 + i, (i + 1) * 1111, f"验证帖{i}：房价与人口") for i in range(10)]
        r1 = await store.save_threads(batch, source="selftest/round1")
        check("首次入库 10 条", r1["inserted"] == 10 and r1["skipped"] == 0, str(r1))
        check("库中确实有 10 条", await store.count(FNAME) == 10)

        # --- 4. upsert 幂等性（最关键）---
        r2 = await store.save_threads(batch, source="selftest/round2")
        check("重复写同一批：新增 0 条", r2["inserted"] == 0, str(r2))
        check("重复写后仍是 10 条（没产生重复）", await store.count(FNAME) == 10,
              f"实际 {await store.count(FNAME)} 条")

        # --- 5. 更新语义：浏览量变化应该被刷新，首次爬取时间不变 ---
        before = await store.collection.find_one({"tid": 1000})
        batch[0].view_num = 999999
        await store.save_threads([batch[0]], source="selftest/round3")
        after = await store.collection.find_one({"tid": 1000})
        check("浏览量被更新", after["view_num"] == 999999, f"{before['view_num']} -> {after['view_num']}")
        check("first_crawled_at 没被覆盖",
              before["first_crawled_at"] == after["first_crawled_at"],
              f"{before['first_crawled_at']} == {after['first_crawled_at']}")

        # --- 6. 字段类型检查 ---
        doc = after
        check("type 以整数存储（IntEnum）", doc["type"] == 1 and isinstance(doc["type"], int), repr(doc["type"]))
        check("create_dt 是 Date 类型", hasattr(doc["create_dt"], "year"), type(doc["create_dt"]).__name__)
        check("user 是嵌套文档", isinstance(doc["user"], dict) and doc["user"]["user_name"] == "验证用户",
              str(doc["user"].get("user_name")))
        check("contents.imgs 嵌套列表存在",
              isinstance(doc["contents"]["imgs"], list) and len(doc["contents"]["imgs"]) == 1)
        check("has _id（MongoDB 自动生成）", "_id" in doc)

        # --- 7. 查询功能 ---
        top = await store.top_by_views(FNAME, limit=3)
        views = [d["view_num"] for d in top]
        check("top_by_views 降序正确", views == sorted(views, reverse=True) and len(top) == 3, str(views))
        check("Top1 是刚才改的 999999", top[0]["view_num"] == 999999, str(top[0]["view_num"]))

        latest = await store.latest(FNAME, limit=5)
        check("latest 返回 5 条", len(latest) == 5)

        # 标题搜索：必须限定在测试用的吧里再数。
        # 之前这里直接用 store.search_title("房价") 断言等于 10，结果库里有真实爬来的
        # "房价"相关帖子（比如中国人口吧的），总数变成 11 就把测试搞挂了 —— 这是测试
        # 写得不严谨（依赖了库里没有别的同关键词数据），不是代码问题。
        cursor = store.collection.find({"fname": FNAME, "title": {"$regex": "房价"}})
        found = [doc async for doc in cursor]
        check("标题搜索在本吧命中 10 条", len(found) == 10, f"实际 {len(found)}")
        check("搜索结果的标题都含关键词",
              all("房价" in (doc.get("title") or "") for doc in found))
        # 跨吧搜索应该至少包含这 10 条（真实数据里可能还有别的吧的同关键词帖子）
        cross = await store.search_title("房价", limit=200)
        check("跨吧搜索能搜到（不限定条数）", len(cross) >= 10, f"实际 {len(cross)}")

        stats = await store.stats()
        row = next((r for r in stats["by_forum"] if r["_id"] == FNAME), None)
        check("统计里能看到本吧", row is not None and row["threads"] == 10, str(row))
        check("全局总数 >= 10", stats["total_documents"] >= 10, str(stats["total_documents"]))

        # --- 8. 索引真的被用上了吗（explain）---
        explain = await store.collection.database.command(
            "explain",
            {"find": MONGO_COLLECTION, "filter": {"fname": FNAME}, "sort": {"view_num": -1}},
            verbosity="queryPlanner",
        )
        stage = explain["queryPlanner"]["winningPlan"]
        stage_name = stage.get("stage") or stage.get("queryPlan", {}).get("stage", "")
        check("排序查询走了索引（IXSCAN）而非全表扫描", "IXSCAN" in str(stage), str(stage_name))

        # --- 9. 自己清理测试数据（测试不该污染真实数据）---
        removed = await store.collection.delete_many({"fname": FNAME})
        check("清理掉本次测试数据", removed.deleted_count == 10, f"删除 {removed.deleted_count} 条")
        check("清理后本吧为空", await store.count(FNAME) == 0)

        print()
        if failures:
            print("失败项:", failures)
            sys.exit(1)
        print("真实 MongoDB 端到端验证：全部通过")


asyncio.run(main())
