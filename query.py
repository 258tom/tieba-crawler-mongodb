r"""
贴吧数据查询工具（替代 mongosh，功能比 mongosh 更贴合本项目）

两种用法：

【一】命令行直接查（适合写脚本、导出）
    .\.venv\Scripts\python.exe query.py top 10                 # 浏览量 Top10
    .\.venv\Scripts\python.exe query.py latest 10              # 最新发帖
    .\.venv\Scripts\python.exe query.py hot 10                 # 回复率高的热帖
    .\.venv\Scripts\python.exe query.py recent 7               # 最近 7 天的帖子
    .\.venv\Scripts\python.exe query.py search 房价            # 标题搜关键词
    .\.venv\Scripts\python.exe query.py author 某网友          # 搜某人的帖
    .\.venv\Scripts\python.exe query.py find "{\"view_num\":{\"$gt\":10000}}"   # 原生查询
    .\.venv\Scripts\python.exe query.py stats                  # 统计汇总
    .\.venv\Scripts\python.exe query.py daily 14               # 最近14天每天发帖量
    .\.venv\Scripts\python.exe query.py indexes                # 看索引
    .\.venv\Scripts\python.exe query.py show 8005359738        # 看单条完整内容

  常用附加参数：
    --fname 弱智          限定吧
    --limit 20            条数
    --min-views 10000     浏览量下限
    --export out.csv      导出（.csv / .json）
    --raw                 打印原始文档（学习 MongoDB 字段结构用）

【二】交互模式（推荐！像 mongosh 一样边敲边看，学查询语法最快）
    .\.venv\Scripts\python.exe query.py
    然后输入  help  看命令，例如：
        弱智                  # 切到"弱智"吧
        top 5                 # 看 Top5
        find {"view_num":{"$gt":100000}}
        daily 7
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import io
import json
import re
import sys
from datetime import datetime, timedelta, timezone

from pymongo import DESCENDING, AsyncMongoClient

# tieba_env 必须排在 mongo_store 前面：它负责把 .env 读进环境变量
from tieba_env import ENV_FILE, load_dotenv
from mongo_store import MONGO_COLLECTION, MONGO_DB, MONGO_URI, ThreadStore

load_dotenv()

# Windows 控制台里让中文正常显示
try:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
except Exception:
    pass

# 列表接口只取需要的字段，别把 contents 正文整段拉出来
BRIEF = {
    "_id": 0, "tid": 1, "fname": 1, "title": 1, "author_id": 1,
    "user.user_name": 1, "user.nick_name_new": 1, "user.level": 1,
    "view_num": 1, "reply_num": 1, "share_num": 1, "agree": 1,
    "is_good": 1, "is_top": 1, "type": 1,
    "create_time": 1, "posted_at_text": 1, "crawled_at": 1,
}

# IntEnum 打印出来是 "ThreadType.NORMAL" 这种，太吵；这里换成人能读的
_ENUM_TEXT = {
    0: "普通", 1: "普通", 2: "投票", 3: "求助", 6: "直播", 7: "吧务",
}
_ENUM_FIELDS = {"type"}


def human(value) -> str:
    """把各种类型转成好读的字符串"""
    text = str(value)
    # aiotieba 的 IntEnum 例如 "ThreadType.NORMAL" -> 取括号里的值或名字
    match = re.match(r"^(\w+)\((-?\d+)\)$", text)
    if match:
        number = int(match.group(2))
        if match.group(1) == "ThreadType":
            return f"{number}({_ENUM_TEXT.get(number, '?')})"
        return f"{number}"
    match = re.match(r"^\w+\.\w+$", text)
    if match:
        return text.split(".")[-1]
    if isinstance(value, datetime):
        return value.astimezone().strftime("%Y-%m-%d %H:%M")
    return text


def short(text: str, width: int) -> str:
    text = (text or "").replace("\n", " ")
    return text if len(text) <= width else text[: width - 1] + "…"


def print_rows(docs: list[dict], *, indent: str = "  ") -> None:
    if not docs:
        print(f"{indent}(没有匹配的数据)")
        return
    print(f"{indent}{'#':>3} {'浏览':>9} {'回复':>7} {'点赞':>6}  {'发帖时间':<16} {'作者':<12} 标题")
    print(f"{indent}{'-'*3} {'-'*9} {'-'*7} {'-'*6}  {'-'*16} {'-'*12} {'-'*34}")
    for i, doc in enumerate(docs, 1):
        author = doc.get("user", {}) or {}
        who = short(author.get("user_name") or author.get("nick_name_new") or "", 11)
        when = doc.get("posted_at_text") or ""
        if not when and doc.get("create_time"):
            when = datetime.fromtimestamp(doc["create_time"]).strftime("%Y-%m-%d %H:%M")
        good = "精" if doc.get("is_good") else ("顶" if doc.get("is_top") else " ")
        print(
            f"{indent}{i:>3} {doc.get('view_num', 0):>9,} {doc.get('reply_num', 0):>7,} "
            f"{doc.get('agree', 0):>6,}  {when:<16} {who:<12} {good}{short(doc.get('title', ''), 40)}"
        )


def print_raw(doc: dict) -> None:
    """以可读形式打印整条文档（学习嵌套结构用）"""
    print(json.dumps(doc, ensure_ascii=False, indent=2, default=str))


def remember(args, docs: list[dict]) -> None:
    """
    记下本次命令真正命中的文档，供 --raw 使用。

    坑：以前 --raw 是重新用 {"fname": args.fname} 查一次库取第一条，
    结果 `top 10 --fname 弱智 --raw` 打印出来的**不是**排行榜第一条，
    而是"弱智吧里随便一条"。--raw 应该打印你刚看到的结果，所以这里改成
    保存本次实际查出来的文档。
    """
    args.last_docs = docs


def build_query(fname: str | None, min_views: int | None, min_replies: int | None,
                days: int | None) -> dict:
    query: dict = {}
    if fname:
        query["fname"] = fname
    if min_views is not None:
        query["view_num"] = {"$gte": min_views}
    if min_replies is not None:
        query["reply_num"] = {"$gte": min_replies}
    # days 很大（比如 hot 默认的"不限时间"）就干脆不加时间条件。
    # 这一步是必须的：timedelta(days=36500) 会让 datetime 溢出报 OSError: Invalid argument
    if days is not None and days < 36500:
        since = int((datetime.now() - timedelta(days=days)).timestamp())
        query["create_time"] = {"$gte": since}
    return query


async def export_rows(store: ThreadStore, docs: list[dict], path: str) -> None:
    """导出到 csv 或 json"""
    rows = []
    for doc in docs:
        author = doc.get("user", {}) or {}
        rows.append({
            "tid": doc.get("tid"),
            "吧名": doc.get("fname"),
            "标题": doc.get("title"),
            "作者": author.get("user_name") or author.get("nick_name_new") or "",
            "浏览量": doc.get("view_num"),
            "回复数": doc.get("reply_num"),
            "点赞": doc.get("agree"),
            "发帖时间": doc.get("posted_at_text"),
        })
    if path.lower().endswith(".json"):
        with io.open(path, "w", encoding="utf-8") as fh:
            json.dump(rows, fh, ensure_ascii=False, indent=2, default=str)
    else:
        # utf-8-sig 让 Excel 打开中文 CSV 不乱码
        with io.open(path, "w", encoding="utf-8-sig", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()) if rows else ["tid"])
            writer.writeheader()
            writer.writerows(rows)
    print(f"\n  已导出 {len(rows)} 条到 {path}")


# ------------------------------------------------------------------ 各命令实现
async def cmd_top(store: ThreadStore, args) -> None:
    field = {"views": "view_num", "replies": "reply_num", "agree": "agree"}[args.by]
    query = build_query(args.fname, args.min_views, None, None)
    cursor = store.collection.find(query, BRIEF).sort(field, DESCENDING).limit(args.limit)
    docs = [doc async for doc in cursor]
    print(f"\n  【{field} 排行】{'全部吧' if not args.fname else args.fname}  Top{len(docs)}")
    print_rows(docs)
    remember(args, docs)
    if args.export:
        await export_rows(store, docs, args.export)


async def cmd_latest(store: ThreadStore, args) -> None:
    query = build_query(args.fname, args.min_views, None, None)
    cursor = store.collection.find(query, BRIEF).sort("create_time", DESCENDING).limit(args.limit)
    docs = [doc async for doc in cursor]
    print(f"\n  【最新发帖】{'全部吧' if not args.fname else args.fname}  {args.limit} 条")
    print_rows(docs)
    remember(args, docs)
    if args.export:
        await export_rows(store, docs, args.export)


async def cmd_hot(store: ThreadStore, args) -> None:
    """热帖：回复率（回复/浏览）高，且回复数不低。聚合管道示例"""
    match = build_query(args.fname, args.min_views, 50, args.days)
    # 注意：$project 是"白名单"，reply_rate 是下面 $addFields 新加的字段，
    # 必须显式写进 project 里，否则会被过滤掉（踩过这个坑：算出来了却显示不出来）
    project = dict(BRIEF)
    project["reply_rate"] = 1
    pipeline = [
        {"$match": match},
        # 加一个计算字段：回复率
        {"$addFields": {"reply_rate": {"$divide": ["$reply_num", {"$max": ["$view_num", 1]}]}}},
        {"$sort": {"reply_rate": -1, "reply_num": -1}},
        {"$limit": args.limit},
        {"$project": project},
    ]
    cursor = await store.collection.aggregate(pipeline)
    docs = [doc async for doc in cursor]
    print(f"\n  【热帖：回复率最高的 {len(docs)} 条】{'全部吧' if not args.fname else args.fname}")
    print_rows(docs)
    remember(args, docs)
    rates = [d for d in docs if d.get("reply_rate") is not None]
    if rates:
        print("\n  回复率（回复数 ÷ 浏览量，比值越高说明越多人愿意参与讨论）：")
        for doc in rates[:5]:
            print(f"    {doc['reply_rate']*100:7.2f}%  {short(doc.get('title',''), 40)}")
    if args.export:
        await export_rows(store, docs, args.export)


async def cmd_recent(store: ThreadStore, args) -> None:
    query = build_query(args.fname, args.min_views, None, args.days)
    cursor = store.collection.find(query, BRIEF).sort("create_time", DESCENDING).limit(args.limit)
    docs = [doc async for doc in cursor]
    print(f"\n  【最近 {args.days} 天的帖子】{'全部吧' if not args.fname else args.fname}")
    print_rows(docs)
    remember(args, docs)
    if args.export:
        await export_rows(store, docs, args.export)


async def cmd_search(store: ThreadStore, args) -> None:
    keyword = args.keyword
    query = build_query(args.fname, args.min_views, None, args.days)
    # $regex + $options: i 表示不区分大小写；re.escape 防止关键词里的特殊字符干扰正则
    query["title"] = {"$regex": re.escape(keyword), "$options": "i"}
    cursor = store.collection.find(query, BRIEF).sort("view_num", DESCENDING).limit(args.limit)
    docs = [doc async for doc in cursor]
    print(f"\n  【标题含“{keyword}”】{'全部吧' if not args.fname else args.fname}")
    print_rows(docs)
    remember(args, docs)
    if args.export:
        await export_rows(store, docs, args.export)


async def cmd_author(store: ThreadStore, args) -> None:
    """按作者查：注意 user 是嵌套文档，所以字段路径写 user.user_name"""
    name = args.keyword
    query = build_query(args.fname, None, None, None)
    query["$or"] = [
        {"user.user_name": name},
        {"user.nick_name_new": name},
    ]
    cursor = store.collection.find(query, BRIEF).sort("view_num", DESCENDING).limit(args.limit)
    docs = [doc async for doc in cursor]
    print(f"\n  【作者“{name}”的帖子】")
    print_rows(docs)


async def cmd_find(store: ThreadStore, args) -> None:
    """原生 MongoDB 查询，用来学语法 / 玩复杂条件"""
    try:
        query = json.loads(args.keyword)
    except json.JSONDecodeError as exc:
        print(f"\n  JSON 解析失败：{exc}")
        print("  注意 PowerShell 里要转义双引号，例如：")
        print('    query.py find "{\\"view_num\\":{\\"$gt\\":10000}}"')
        return
    cursor = store.collection.find(query, BRIEF).sort("view_num", DESCENDING).limit(args.limit)
    docs = [doc async for doc in cursor]
    total = await store.collection.count_documents(query)
    print(f"\n  【自定义查询】{args.keyword}")
    print(f"  条件命中 {total} 条，显示前 {len(docs)} 条")
    print_rows(docs)
    remember(args, docs)
    if args.export:
        await export_rows(store, docs, args.export)


async def cmd_show(store: ThreadStore, args) -> None:
    """看单条完整文档（含嵌套的 user / contents，学习数据结构用）"""
    tid = int(args.keyword)
    doc = await store.collection.find_one({"tid": tid})
    if not doc:
        print(f"\n  没有找到 tid={tid}")
        return
    print(f"\n  【tid={tid} 完整文档（这就是 MongoDB 里真实存的东西）】\n")
    print_raw(doc)


async def cmd_stats(store: ThreadStore, args) -> None:
    stats = await store.stats()
    print(f"\n  【总览】数据库 {MONGO_DB}   集合 {MONGO_COLLECTION}")
    print(f"  文档总数：{stats['total_documents']:,}\n")
    print(f"  {'吧名':<18}{'帖子数':>8}{'总浏览量':>13}{'总回复':>11}{'最高浏览':>11}")
    print(f"  {'-'*18}{'-'*8}{'-'*13}{'-'*11}{'-'*11}")
    for row in stats["by_forum"]:
        print(f"  {str(row['_id']):<18}{row['threads']:>8,}{row['total_views']:>13,}"
              f"{row['total_replies']:>11,}{row['max_views']:>11,}")


async def cmd_daily(store: ThreadStore, args) -> None:
    """按天统计发帖量。用聚合管道把时间戳换算成日期再分组——这正是可视化的数据准备"""
    since = int((datetime.now() - timedelta(days=args.days)).timestamp())
    match = {"create_time": {"$gte": since}}
    if args.fname:
        match["fname"] = args.fname
    pipeline = [
        {"$match": match},
        {
            "$group": {
                # create_dt 是我们额外存的 Date 类型字段，$dateToString 可以直接按天格式化
                "_id": {"$dateToString": {"format": "%Y-%m-%d", "date": "$create_dt"}},
                "posts": {"$sum": 1},
                "views": {"$sum": "$view_num"},
                "replies": {"$sum": "$reply_num"},
            }
        },
        {"$sort": {"_id": 1}},
    ]
    cursor = await store.collection.aggregate(pipeline)
    rows = [doc async for doc in cursor]
    print(f"\n  【最近 {args.days} 天每天发帖量】{'全部吧' if not args.fname else args.fname}")
    if not rows:
        print("  (没有数据)")
        return
    peak = max(r["posts"] for r in rows)
    print(f"  {'日期':<13}{'帖子':>6}{'浏览量':>12}{'回复':>9}   柱状图")
    for row in rows:
        bar = "█" * max(1, round(row["posts"] / peak * 28)) if row["posts"] else ""
        print(f"  {row['_id']:<13}{row['posts']:>6,}{row['views']:>12,}{row['replies']:>9,}   {bar}")
    print(f"\n  合计 {sum(r['posts'] for r in rows):,} 条帖子")


async def cmd_indexes(store: ThreadStore, args) -> None:
    info = await store.collection.index_information()
    print("\n  【索引】")
    for name, spec in info.items():
        print(f"  {name:<16} keys={spec.get('key')}  unique={spec.get('unique', False)}")
    print("\n  小知识：用 explain 可以看某个查询有没有走索引：")
    print('    query.py explain "{\\"view_num\\":{\\"$gt\\":10000}}"')

    # 顺便看下集合大小
    stats = await store.collection.database.command("collstats", MONGO_COLLECTION)
    print(f"\n  文档数 {stats.get('count'):,}  平均文档 {stats.get('avgObjSize', 0):,} 字节  "
          f"数据 {stats.get('size', 0)/1024/1024:.1f} MB  索引 {stats.get('totalIndexSize', 0)/1024/1024:.2f} MB")


async def cmd_explain(store: ThreadStore, args) -> None:
    """看查询计划：确认走没走索引（性能问题的第一排查手段）"""
    try:
        query = json.loads(args.keyword)
    except json.JSONDecodeError as exc:
        print(f"\n  JSON 解析失败：{exc}")
        return
    plan = await store.collection.database.command(
        "explain",
        {"find": MONGO_COLLECTION, "filter": query, "sort": {"view_num": -1}},
        verbosity="executionStats",
    )
    win = plan["queryPlanner"]["winningPlan"]
    stats = plan.get("executionStats", {})
    stage = win.get("stage") or win.get("queryPlan", {}).get("stage", "?")
    index = win.get("inputStage", {}).get("indexName") or win.get("indexName") or "(未用索引)"
    print(f"\n  【查询计划】{args.keyword}")
    print(f"  执行阶段   : {stage}")
    print(f"  使用索引   : {index}")
    print(f"  扫描文档数 : {stats.get('totalDocsExamined', '?')}")
    print(f"  返回文档数 : {stats.get('nReturned', '?')}")
    if "IXSCAN" in str(win):
        print("  → 走了索引，性能 OK")
    else:
        print("  → 没走索引（COLLSCAN = 全表扫描），数据量大时会很慢")


COMMANDS = {
    "top": cmd_top, "latest": cmd_latest, "hot": cmd_hot, "recent": cmd_recent,
    "search": cmd_search, "author": cmd_author, "find": cmd_find, "show": cmd_show,
    "stats": cmd_stats, "daily": cmd_daily, "indexes": cmd_indexes, "explain": cmd_explain,
}


def make_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="query.py",
        description="贴吧数据查询工具（不带参数运行则进入交互模式）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("command", nargs="?", default=None, help="命令，省略则进入交互模式")
    parser.add_argument("keyword", nargs="?", default="", help="关键词 / tid / JSON 表达式")
    parser.add_argument("--fname", default=None, help="限定贴吧名")
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument("--days", type=int, default=None,
                        help="recent/hot/daily 的天数（recent 默认7，daily 默认14，hot 默认不限天数）")
    parser.add_argument("--by", default="views", choices=["views", "replies", "agree"])
    parser.add_argument("--min-views", type=int, default=None, dest="min_views")
    parser.add_argument("--export", default=None, help="导出到 .csv 或 .json")
    parser.add_argument("--raw", action="store_true", help="打印原始文档（学习字段结构）")
    return parser


async def open_store():
    store = ThreadStore()
    try:
        await store.open()
    except Exception as exc:
        print("连不上 MongoDB！")
        print(f"  地址：{MONGO_URI}")
        print(f"  错误：{type(exc).__name__}: {exc}")
        print()
        print("  数据库没在跑。请先双击：")
        print(r"    D:\mongodb\启动MongoDB.cmd")
        print("  （那个窗口要一直开着；看到“MongoDB 已就绪”横幅就是成功了）")
        sys.exit(1)
    return store


async def run_once(args) -> None:
    if args.command not in COMMANDS:
        print(f"未知命令：{args.command}")
        print(f"可用命令：{', '.join(sorted(COMMANDS))}")
        return

    # 位置参数 keyword 的语义按命令区分。
    # argparse 只有一个位置参数槽，所以 `top 5` 会把 5 塞进 keyword，
    # 而 `search 房价` 塞的是关键词 —— 这里按命令把数字分派给 limit / days。
    raw = (args.keyword or "").strip()
    if args.command in ("top", "latest", "hot", "recent"):
        if raw.isdigit():
            if args.command == "recent":
                args.days = int(raw)      # recent 15 -> 最近 15 天
                args.limit = 30
            else:
                args.limit = int(raw)     # top 5     -> 取 5 条
            args.keyword = ""
        elif raw and args.command in ("top", "latest", "hot"):
            print(f"提示：{args.command} 的参数应该是条数（数字），收到 {raw!r}，已忽略")
            args.keyword = ""
    if args.command == "daily" and raw.isdigit():
        args.days = int(raw)
        args.keyword = ""

    # 每个命令的天数默认值不一样，这里补上（--days 没显式传就是 None）
    if args.days is None:
        args.days = {"recent": 7, "daily": 14, "hot": 36500}.get(args.command, 7)

    store = await open_store()
    async with store:
        await COMMANDS[args.command](store, args)
        # --raw：打印刚才那条命令实际命中的第一条完整文档（不是另外查一条）
        if args.raw:
            docs = getattr(args, "last_docs", None) or []
            if docs:
                print("\n  【上面第一条的原始文档】\n")
                print_raw(docs[0])
            else:
                print("\n  --raw：本次没有查到数据，没什么可打印的")


HELP_TEXT = """
  =============== 交互模式命令 ===============

  直接输入吧名切换当前吧，例如：
      弱智                      切到“弱智”吧
      中国人口                  切到“中国人口”吧
      all                      不限吧（查全部）

  查询命令（数字表示条数，默认 10）：
      stats                     总览统计
      top 10                    浏览量 Top10
      latest 10                 最新发帖
      hot 10                    回复率高的热帖
      recent 7                  最近 7 天的帖子
      search 房价               标题含“房价”
      author 某网友             某作者的帖子
      daily 14                  最近 14 天每天发帖量（柱状图）
      find {"view_num":{"$gt":100000}}     原生 MongoDB 查询
      show 8005359738           看某条帖子的完整文档
      indexes                   查看索引
      explain {"view_num":{"$gt":100000}}  查看查询计划

  其他：
      help / ?                  显示这个帮助
      exit / quit               退出

  提示：find 里可以用 MongoDB 的操作符
      $gt(大于) $gte(大于等于) $lt $lte $ne(不等于)
      $in(在列表内) $regex(正则) $and $or $exists(字段存在)
      例：find {"view_num":{"$gt":100000},"reply_num":{"$lt":50}}
"""


async def interactive(args) -> None:
    store = await open_store()
    async with store:
        print("\n  ===== 贴吧数据查询（交互模式）=====")
        print(f"  数据库 {MONGO_DB}.{MONGO_COLLECTION}   连接 {MONGO_URI}")

        forums = await store.collection.distinct("fname")
        forums = [f for f in forums if f]
        total = await store.collection.count_documents({})
        print(f"  共有 {total:,} 条数据，来自 {len(forums)} 个吧：{', '.join(forums) if forums else '(空)'}")
        print("\n  输入 help 看命令，输入 exit 退出\n")

        current = forums[0] if forums else None
        loop = asyncio.get_running_loop()

        while True:
            prompt = f"  [{current or 'all'}] > "
            try:
                line = await loop.run_in_executor(None, input, prompt)
            except (EOFError, KeyboardInterrupt):
                print()
                break
            # 去掉 BOM、零宽字符等不可见字符。
            # 踩过的坑：用管道喂命令时（echo help | python query.py），第一行前面会带 BOM，
            # 导致 "help" 变成 "\ufeffhelp"，看起来一模一样却不认识。
            line = line.strip().lstrip("\ufeff\u200b\ufeff").strip()
            if not line:
                continue

            parts = line.split(maxsplit=1)
            head = parts[0]
            rest = parts[1] if len(parts) > 1 else ""

            if head in ("exit", "quit", "q"):
                break
            if head in ("help", "?", "h"):
                print(HELP_TEXT)
                continue
            if head == "debug":
                continue

            # 切吧
            if head == "all":
                current = None
                print("  已切换到：不限吧")
                continue
            if head not in COMMANDS:
                # 不是命令就当成吧名
                if head in forums:
                    current = head
                    print(f"  已切换到：{current}")
                    continue
                lowered = head
                if lowered in [f.lower() for f in forums]:
                    current = next(f for f in forums if f.lower() == lowered)
                    print(f"  已切换到：{current}")
                    continue
                print(f"  不认识“{head}”，也不是已知的吧。输入 help 看命令，或输入 all。")
                continue

            # 执行命令
            local = argparse.Namespace(**vars(args))
            local.fname = current
            if head in ("search", "author", "show", "find", "explain"):
                local.keyword = rest.strip().strip('"')
            else:
                digits = re.findall(r"\d+", rest)
                if head == "daily":
                    local.days = int(digits[0]) if digits else 14
                elif head == "recent":
                    # recent 7 表示天数；不带参数默认 7 天
                    local.days = int(digits[0]) if digits else 7
                    local.limit = 30
                else:
                    if digits:
                        local.limit = int(digits[0])
                    if head == "hot":
                        local.days = 36500      # 热帖默认不限制时间
            if head in ("hot",) and local.days is None:
                local.days = 36500
            if head in ("recent",) and local.days is None:
                local.days = 7

            print()
            try:
                await COMMANDS[head](store, local)
            except Exception as exc:
                print(f"  执行出错：{type(exc).__name__}: {exc}")
            print()

        print("  再见！")


def main() -> None:
    args = make_parser().parse_args()
    if args.command is None:
        asyncio.run(interactive(args))
    else:
        asyncio.run(run_once(args))


if __name__ == "__main__":
    main()
