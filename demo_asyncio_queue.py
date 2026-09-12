r"""
asyncio 第二课：生产者-消费者 + 队列背压（本项目爬虫的核心结构）。

这个文件包含三个版本，建议按顺序看：
    版本 1  broken_version()  —— 初学者最容易写错的样子，附逐条病因
    版本 2  queue_version()   —— 修好的生产者-消费者，本项目实际采用的结构
    版本 3  sentinel_version() —— 用哨兵值 + queue.join() 的另一种写法

运行（不需要 BDUSS、不联网，用假数据）：
    python demo_asyncio_queue.py
"""

from __future__ import annotations

import asyncio
import time

# 页码总数 / 并发数 / 队列缓冲区大小
TOTAL_PAGES = 20
WORKER_NUM = 4
QUEUE_MAXSIZE = 4
FAKE_LATENCY = 0.1      # 每页的模拟网络耗时（秒）
POLL_INTERVAL = 0.5     # worker 等任务的轮询间隔（秒）


async def fetch_page(pn: int) -> str:
    """模拟"爬第 pn 页"，返回一页的条数描述"""
    await asyncio.sleep(FAKE_LATENCY)
    return f"第{pn}页(30条)"


# ============================================================ 版本 1：反面教材
async def broken_version() -> None:
    """
    ⚠️ 这是初学者最容易写出的版本，它是**坏的**。四个病：

    病 1  `task_queue.put(i)` 漏了 await
          put 是协程，不 await 就只是创建一个永远不会被调度的协程对象，
          队列里一条数据都不会有（还会收获 RuntimeWarning: coroutine was never awaited）。
    病 2  `is_running = False` 缩进在 for 循环里
          第一次循环就把它置 False，等于告诉 worker"活干完了"，
          可此时队列里才有 1 个页码 —— 后面 30 页全被丢掉。
          正确位置是 for 循环**结束之后**。
    病 3  consumer 里 `except TimeoutError: pass` 之后没有 continue 判断
          拿不到任务时什么都不做继续死循环，而且**永远没有退出条件**，
          程序再也结束不了。
    病 4  没有把 consumer 启动起来
          只定义了 coroutine function 却没有 gather/await 调用它，
          最后加上 `asyncio.run()` 会直接退出，什么都不发生。

    这里用注释把"错误写法"标出来，真正执行的是版本 2。
    """
    print("\n【版本 1】反面教材（不执行，只解释）")
    print("  病1  task_queue.put(i) 漏 await      -> 队列永远是空的")
    print("  病2  is_running=False 写在循环里     -> 第一轮就通知 worker 收工，后 19 页被丢")
    print("  病3  except TimeoutError: pass       -> 没有退出条件，程序永不结束")
    print("  病4  没有 gather 启动 consumer       -> 协程压根没跑")
    print("  → 结论：跑这个文件不会有任何输出，也没有报错，最难查的就是这种")


# ==================================================== 版本 2：本项目采用的结构
async def queue_version() -> None:
    """
    修好的版本。三个关键设计：
      · 队列 maxsize 提供**背压**：缓冲区满时生产者阻塞，任务不会无限堆积
      · worker 用 `wait_for(queue.get(), timeout)` + `is_running` 标志位退出
      · `asyncio.gather(*workers, producer())` 一起并发跑
    """
    print("\n【版本 2】生产者-消费者（本项目采用）")
    queue: asyncio.Queue[int] = asyncio.Queue(maxsize=QUEUE_MAXSIZE)
    is_running = True
    results: list[str] = []
    peak_queue_size = 0

    async def producer() -> None:
        nonlocal is_running
        for pn in range(1, TOTAL_PAGES + 1):
            await queue.put(pn)          # 病 1 的修复：一定要 await
            print(f"  [生产者] 投递第 {pn} 页    队列长度={queue.qsize()}/{QUEUE_MAXSIZE}")
        # 病 2 的修复：循环结束后才置 False
        is_running = False
        print("  [生产者] 所有页码投递完毕，通知 worker 收工")

    async def worker(i: int) -> None:
        nonlocal peak_queue_size
        while True:
            try:
                pn = await asyncio.wait_for(queue.get(), timeout=POLL_INTERVAL)
            except TimeoutError:
                # 病 3 的修复：区分"暂时没任务"和"活干完了"
                if not is_running:
                    print(f"  [Worker#{i}] 队列空且生产者已结束 -> 退出")
                    return
                continue
            peak_queue_size = max(peak_queue_size, queue.qsize())
            result = await fetch_page(pn)
            results.append(result)
            print(f"  [Worker#{i}] 完成 {result}")

    start = time.perf_counter()
    # 病 4 的修复：用 gather 把所有协程真正跑起来
    # 注意 *workers 是解包，gather 不接受 list 作为单个参数
    await asyncio.gather(*[worker(i) for i in range(WORKER_NUM)], producer())
    elapsed = time.perf_counter() - start

    print(f"\n  完成 {len(results)} 页，耗时 {elapsed:.2f} 秒")
    print(f"  串行需要约 {TOTAL_PAGES * FAKE_LATENCY:.2f} 秒，"
          f"并发加速比约 {TOTAL_PAGES * FAKE_LATENCY / elapsed:.1f}x")
    print(f"  队列长度峰值 {peak_queue_size}/{QUEUE_MAXSIZE}"
          f"（没超过上限 = 背压生效，任务没有无限堆积）")


# ============================================== 版本 3：哨兵值 + queue.join()
async def sentinel_version() -> None:
    """
    另一种常见写法：不用 `is_running` 标志位，而是往队列里投 **哨兵值**（None），
    并且用 `queue.join()` 等所有任务真正处理完。

    区别：
      · 版本 2 的 worker 靠"超时 + 标志位"退出，空转最多 POLL_INTERVAL 秒
      · 版本 3 的 worker 拿到哨兵立刻退出，响应更快，而且一个 worker 一个哨兵
    两种都对。版本 2 更好懂，版本 3 更精确。
    """
    print("\n【版本 3】哨兵值 + queue.join()")
    queue: asyncio.Queue[int | None] = asyncio.Queue(maxsize=QUEUE_MAXSIZE)
    done: list[str] = []

    async def producer() -> None:
        for pn in range(1, TOTAL_PAGES + 1):
            await queue.put(pn)
        # 每个 worker 一个哨兵，投完就代表"没新活了"
        for _ in range(WORKER_NUM):
            await queue.put(None)

    async def worker(i: int) -> None:
        while True:
            pn = await queue.get()
            if pn is None:               # 收到哨兵 -> 收工
                queue.task_done()
                return
            try:
                done.append(await fetch_page(pn))
            finally:
                # 无论成功失败都要 task_done，否则 queue.join() 永远等不到
                queue.task_done()

    start = time.perf_counter()
    workers = [asyncio.create_task(worker(i)) for i in range(WORKER_NUM)]
    producer_task = asyncio.create_task(producer())
    await producer_task
    await queue.join()                   # 等到所有入队任务都被 task_done 处理完
    await asyncio.gather(*workers)       # 再收掉 worker 本身
    elapsed = time.perf_counter() - start
    print(f"  完成 {len(done)} 页，耗时 {elapsed:.2f} 秒（哨兵写法，无空转等待）")


async def main() -> None:
    broken_version()
    await queue_version()
    await sentinel_version()
    print("\n小结：爬虫骨架 = 生产者投页码 + 队列背压 + N 个 worker + gather 并发")


if __name__ == "__main__":
    asyncio.run(main())
