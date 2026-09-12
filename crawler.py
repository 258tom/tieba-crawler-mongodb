r"""
贴吧多协程爬虫（入库版）：并发爬取指定贴吧的主题帖，直接 upsert 进 MongoDB。

设计要点
--------
1. **生产者-消费者 + 背压**：生产者往 `asyncio.Queue(maxsize=N)` 塞页码，
   N 个 worker 协程抢页码去爬。队列满时生产者被阻塞，天然限流。
2. **两层去重**：本次运行用 `seen_tids` 集合去重；数据库层用 `uniq_tid`
   唯一索引 + upsert 兜底。爬多少次都不会产生重复数据。
3. **边爬边入库**：worker 拿到一页立刻写库，不等全部爬完 ——
   内存占用小，中途挂掉也不丢已经爬到的数据。
4. **单页失败不影响整体**：某一页爬取失败只记 warning，继续处理下一页。

用法
----
    # 先准备凭据（只需一次）
    copy .env.example .env      # 然后编辑 .env 填上 TIEBA_BDUSS

    # 用 .env 里的默认吧名爬取
    python crawler.py

    # 指定吧名 / 页数 / 并发数（命令行参数优先于 .env）
    python crawler.py --fname 中国人口 --max-page 32 --workers 8

    # 作为库调用
    import asyncio
    from crawler import crawl
    asyncio.run(crawl("弱智", max_page=3))

    # 没有 BDUSS / 不想联网，只验证流程
    python crawler.py --offline
"""

from __future__ import annotations

import argparse
import asyncio
import os
import time

import aiotieba as tb
from pymongo.errors import ServerSelectionTimeoutError

# tieba_env 必须排在 mongo_store 前面 import：
# 它会把 .env 读进环境变量，而 mongo_store 在 import 时就要读取 MONGO_URI / MONGO_DB
from tieba_env import describe_config, require_bduss
from mongo_store import MONGO_URI, ThreadStore

# ---------------------------------------------------------------- 默认配置
# 优先级：命令行参数 > 环境变量 / .env > 这里的默认值
DEFAULT_FNAME = os.getenv("TIEBA_FNAME", "弱智")
DEFAULT_MAX_PAGE = int(os.getenv("TIEBA_MAX_PAGE", "32"))
DEFAULT_WORKER_NUM = int(os.getenv("TIEBA_WORKER_NUM", "8"))

# worker 等不到新任务的轮询间隔（秒）。也是"活干完了没有"的检查频率
POLL_INTERVAL = 1.0


async def crawl(
    fname: str = DEFAULT_FNAME,
    max_page: int = DEFAULT_MAX_PAGE,
    worker_num: int = DEFAULT_WORKER_NUM,
    *,
    bduss: str | None = None,
    offline: bool = False,
) -> dict[str, int]:
    """
    爬取贴吧 `fname` 的前 `max_page` 页主题帖，写入 MongoDB。

    Args:
        fname: 贴吧名
        max_page: 爬取页数上限
        worker_num: 并发消费者协程数量
        bduss: 贴吧凭据；传 None 则从环境变量 / .env 读取
        offline: 不联网、不用 BDUSS，用假数据跑通全流程（验证环境用）

    Returns:
        {"seen": 本次见到的帖子数, "before": 爬前总数, "after": 爬后总数, "inserted": 新增数}
    """
    start_time = time.perf_counter()
    logger = tb.get_logger()

    # 没传 BDUSS 就立刻检查配置：早失败，报错里带完整的获取步骤
    if not offline:
        bduss = bduss or require_bduss()

    logger.info("爬虫启动 | 贴吧=%s 页数=%d 并发=%d", fname, max_page, worker_num)

    # async with 打开连接，退出代码块时自动关闭
    async with ThreadStore() as store:
        # 建索引（第一次运行会创建，之后重复调用没有副作用）
        index_names = await store.ensure_indexes()
        logger.info("MongoDB 已连接 | 索引=%s", ",".join(index_names))

        # 记录爬取前的数量，最后用来算"本次新增了多少"
        before_count = await store.count(fname)

        # 本次运行已经处理过的 tid，避免同一条帖子被不同页重复入库
        seen_tids: set[int] = set()

        async with tb.Client(bduss or "") as client:
            # maxsize 就是背压的阈值：缓冲区满时生产者会被阻塞
            task_queue: asyncio.Queue[int] = asyncio.Queue(maxsize=worker_num)
            is_running = True

            async def producer() -> None:
                """生产者：把页码依次填进任务队列"""
                for pn in range(1, max_page + 1):
                    await task_queue.put(pn)
                # 页码发完后置为 False，worker 超时后据此退出。
                # nonlocal 必须写在函数体第一层，不能缩进进 for 循环，
                # 否则 SyntaxError: no binding for nonlocal 'is_running' found
                nonlocal is_running
                is_running = False

            async def fetch_page(pn: int) -> list:
                """取一页数据。离线模式下返回假数据，用来验证流程不依赖网络"""
                if offline:
                    await asyncio.sleep(0.05)          # 模拟网络延迟
                    return _make_offline_threads(fname, pn)
                return await client.get_threads(fname, pn)

            async def worker(i: int) -> None:
                """消费者：取页码 -> 爬这一页 -> 写进 MongoDB"""
                while True:
                    try:
                        # 超过 POLL_INTERVAL 秒没拿到页码就抛 TimeoutError
                        pn = await asyncio.wait_for(task_queue.get(), timeout=POLL_INTERVAL)
                    except TimeoutError:
                        # 拿不到任务时判断：活干完了就退出，否则继续等
                        if not is_running:
                            logger.debug("Worker#%d 退出", i)
                            return
                        continue

                    try:
                        threads = await fetch_page(pn)
                    except Exception as exc:
                        # 单页失败不要让整个爬虫挂掉，记一笔然后去处理下一页
                        logger.warning("Worker#%d 第 %d 页爬取失败: %r", i, pn, exc)
                        continue

                    # 本次运行内去重：只保留还没见过的 tid
                    fresh = [t for t in threads if getattr(t, "tid", 0) and t.tid not in seen_tids]
                    seen_tids.update(t.tid for t in fresh)

                    if fresh:
                        # await 入库：写库是异步的，不会阻塞其他协程
                        result = await store.save_threads(fresh, source=f"get_threads/pn={pn}")
                        logger.debug(
                            "Worker#%d pn=%d 本页%d条 新增%d 更新%d",
                            i, pn, len(fresh), result["inserted"], result["updated"],
                        )

            # gather 并发跑所有 worker + 生产者。
            # 注意 *workers：gather 不接受列表，要把列表展开成多个参数
            workers = [worker(i) for i in range(worker_num)]
            await asyncio.gather(*workers, producer())

        after_count = await store.count(fname)
        elapsed = time.perf_counter() - start_time
        logger.info(
            "爬取完成 | 用时 %.2f 秒 | 本次见到 %d 条 | 库中现有 %d 条（新增 %d）",
            elapsed, len(seen_tids), after_count, after_count - before_count,
        )

        # 从数据库读回来验证：这些数据是真的存在 MongoDB 里的
        logger.info("========== 浏览量 Top 10（从 MongoDB 读出）==========")
        for i, doc in enumerate(await store.top_by_views(fname, limit=10), 1):
            logger.info(
                "第%d名 浏览:%d 回复:%d tid:%d 标题:%s",
                i, doc["view_num"], doc["reply_num"], doc["tid"], doc["title"],
            )

        logger.info("========== 最新发布的 5 帖 ==========")
        for doc in await store.latest(fname, limit=5):
            logger.info("tid:%d %s %s", doc["tid"], doc.get("posted_at_text", ""), doc["title"])

    return {
        "seen": len(seen_tids),
        "before": before_count,
        "after": after_count,
        "inserted": after_count - before_count,
    }


def _make_offline_threads(fname: str, pn: int) -> list:
    """
    造一页假的 Thread 对象（离线模式用）。

    刻意复用真实的 aiotieba Thread 数据结构，所以它同时也在验证
    "Thread -> 文档 -> BSON" 这条转换链是否正常。
    """
    from aiotieba.api._classdef.contents import FragImage, FragText
    from aiotieba.api.get_threads._classdef import Contents_t, Thread, UserInfo_t

    threads = []
    for i in range(30):
        tid = 900_000_000 + pn * 1000 + i
        thread = Thread(
            contents=Contents_t(
                texts=[FragText(f"离线模式第 {pn} 页第 {i} 条正文")],
                imgs=[FragImage("small.jpg", "big.jpg", "origin.jpg", 1024, 100, 200, "h" * 32)],
            ),
            title=f"[离线] {fname} 第 {pn} 页第 {i} 帖：房价会跌吗",
            fid=88888,
            fname=fname,
            tid=tid,
            pid=tid + 1,
            user=UserInfo_t(
                user_id=100000 + i, portrait=f"tb.1.{i}", user_name=f"离线用户{i}",
                nick_name_new=f"昵称{i}", level=(i % 16) + 1, glevel=3,
            ),
            author_id=100000 + i,
            view_num=(i + 1) * (pn + 1) * 137,
            reply_num=(i + 1) * (pn + 1),
            share_num=i % 5,
            agree=(i + 1) * 3,
            disagree=i % 3,
            create_time=1767225600 + pn * 86400 + i * 600,
            last_time=1767312000 + pn * 86400 + i * 600,
        )
        thread.type = tb.enums.ThreadType(1)
        threads.append(thread)
    return threads


def make_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="crawler.py",
        description="贴吧多协程爬虫（结果直接入库 MongoDB）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--fname", default=DEFAULT_FNAME, help=f"贴吧名（默认 {DEFAULT_FNAME}）")
    parser.add_argument("--max-page", type=int, default=DEFAULT_MAX_PAGE,
                        dest="max_page", help=f"最多爬几页（默认 {DEFAULT_MAX_PAGE}）")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKER_NUM,
                        dest="worker_num", help=f"并发协程数（默认 {DEFAULT_WORKER_NUM}）")
    parser.add_argument("--offline", action="store_true",
                        help="离线模式：用假数据跑通全流程，不需要 BDUSS 也不联网")
    return parser


def main() -> None:
    args = make_parser().parse_args()
    if not args.offline:
        print(describe_config())
    else:
        print("离线模式：不联网，使用假数据（用于验证环境与入库链路）")
    try:
        stats = asyncio.run(
            crawl(args.fname, max_page=args.max_page, worker_num=args.worker_num, offline=args.offline)
        )
    except ServerSelectionTimeoutError as exc:
        # 最常见的情况：MongoDB 没启动。直接给出可操作的提示，而不是甩一堆 pymongo 堆栈
        print("\n连不上 MongoDB！")
        print(f"  地址：{MONGO_URI}")
        print(f"  错误：{type(exc).__name__}")
        print()
        print("  数据库没在跑。启动方式（三选一）：")
        print("    mongod --dbpath /your/data/path")
        print("    docker run -d -p 27017:27017 --name mongo mongo:8.0")
        print("    或用 MongoDB Atlas，把连接串填进 .env 的 MONGO_URI")
        print()
        print("  确认通了再重跑：")
        print("    python -c \"from pymongo import MongoClient; "
              "print(MongoClient('mongodb://127.0.0.1:27017', serverSelectionTimeoutMS=3000)"
              ".admin.command('ping'))\"")
        raise SystemExit(1) from None
    print(f"\n本次新增 {stats['inserted']} 条，库中共 {stats['after']} 条")


if __name__ == "__main__":
    main()
