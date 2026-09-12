r"""
asyncio 入门第一课：`async def` / `await` / `asyncio.gather` 到底在干什么。

核心一句话
----------
**`await` 是"让出控制权"，不是"卡住程序"。**
一个协程在 `await` 网络 IO 时，事件循环会转身去跑别的协程 ——
所以 3 个请求并发跑，总耗时接近 1 个，而不是 3 个相加。

运行：
    python demo_asyncio_basic.py

没配 BDUSS 也能跑：默认用 `asyncio.sleep` 模拟网络延迟，纯本地。
配了 BDUSS 想打真接口，加 `--online`。
"""

from __future__ import annotations

import argparse
import asyncio
import time

import aiotieba as tb

from tieba_env import get_bduss

FORUMS = ["弱智", "中国人口", "营山二中"]

# 每个"请求"的模拟延迟（秒）
FAKE_LATENCY = 0.5


async def fake_get_threads(fname: str, pn: int = 1) -> str:
    """用 asyncio.sleep 模拟一次网络请求"""
    await asyncio.sleep(FAKE_LATENCY)
    return f"{fname} 第 {pn} 页的 30 条帖子"


async def serial() -> None:
    """串行版：一个 await 完再 await 下一个，总耗时 = 各任务之和"""
    print("\n【串行】一个接一个 await")
    start = time.perf_counter()
    for fname in FORUMS:
        result = await fake_get_threads(fname)
        print(f"  拿到 -> {result}")
    print(f"  总耗时 {time.perf_counter() - start:.2f} 秒"
          f"（= {FAKE_LATENCY} × {len(FORUMS)}，完全叠加）")


async def concurrent() -> None:
    """
    并发版：用 gather 把协程"同时"交给事件循环。

    注意传给 gather 的是**协程对象**（函数调用后还没 await 的东西），
    不是函数本身 —— 要写 `gather(fake_get_threads(f))`，不是 `gather(fake_get_threads, f)`。
    """
    print("\n【并发】用 asyncio.gather 一起跑")
    start = time.perf_counter()
    # 这一步只是构造协程对象，它们还没有开始执行
    coros = [fake_get_threads(fname) for fname in FORUMS]
    # gather 把它们注册到事件循环上并发执行，返回结果顺序和传入顺序一致
    results = await asyncio.gather(*coros)
    for result in results:
        print(f"  拿到 -> {result}")
    print(f"  总耗时 {time.perf_counter() - start:.2f} 秒（≈ {FAKE_LATENCY} 秒，几乎不叠加）")


async def online_demo() -> None:
    """真实请求：用 gather 同时拿账号信息和贴吧数据"""
    bduss = get_bduss()
    if not bduss:
        print("\n【真实请求】跳过：没有配置 TIEBA_BDUSS（复制 .env.example 为 .env 后填上）")
        return

    print("\n【真实请求】gather 同时请求两个接口")
    async with tb.Client(bduss) as client:
        user, threads = await asyncio.gather(
            client.get_self_info(),          # 拿自己的账号信息
            client.get_threads("弱智", 1),   # 拿弱智吧第一页
        )
        print(f"  当前账号：{user.user_name}")
        print(f"  弱智吧第一页拿到 {len(threads)} 条，前 3 条：")
        for thread in threads[:3]:
            print(f"    tid={thread.tid} 浏览={thread.view_num} 标题={thread.title}")


async def main(online: bool) -> None:
    await serial()
    await concurrent()
    if online:
        await online_demo()
    else:
        print("\n提示：加 --online 参数可真实请求贴吧接口（需要先在 .env 里配好 BDUSS）")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="asyncio 基础示例：串行 vs 并发")
    parser.add_argument("--online", action="store_true", help="真实请求贴吧接口（需要 BDUSS）")
    asyncio.run(main(parser.parse_args().online))
