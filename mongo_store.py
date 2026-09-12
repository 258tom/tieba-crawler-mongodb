"""
MongoDB 存储层：负责把 aiotieba 爬到的主题帖写进 MongoDB，并提供查询方法。

设计要点（给刚接触 MongoDB 的你）：
1. MongoDB 里没有"表"，而是"集合(collection)"；没有"行"，而是"文档(document)"。
   文档就是 JSON 那样的字典，所以 Python 的 dict 可以几乎原样存进去。
2. 每条帖子用 tid（主题帖 id）当唯一标识，重复爬到同一条时用 upsert 覆盖更新，
   所以爬 10 次不会产生 10 条重复数据（浏览量这种会变化的字段也会一起刷新）。
3. 索引(index) 相当于 MySQL 的索引，建了索引以后按 fname / view_num 查询才不会全表扫描。
"""

from __future__ import annotations

import copy as dcopy
import dataclasses as dcs
import os
import re
from datetime import datetime, timezone
from typing import Any, ClassVar

from bson import BSON, ObjectId
from pymongo import ASCENDING, DESCENDING, AsyncMongoClient, UpdateOne

import aiotieba as tb

# 必须排在读取配置之前：它把项目根目录的 .env 读进环境变量
from tieba_env import load_dotenv

load_dotenv()

# ---------------------------------------------------------------- 连接配置
# 想改配置不用动代码，写进项目根目录的 .env 或者设成环境变量即可：
#   MONGO_URI=mongodb://127.0.0.1:27017
#   MONGO_DB=tieba
MONGO_URI = os.getenv("MONGO_URI", "mongodb://127.0.0.1:27017")
MONGO_DB = os.getenv("MONGO_DB", "tieba")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "threads")

# MongoDB 单条文档硬上限是 16MB，超了会直接报错，所以入库前先测一下
MAX_DOC_BYTES = 16 * 1024 * 1024


def doc_from_thread(thread: Any, *, source: str = "get_threads") -> dict[str, Any]:
    """
    把 aiotieba 的 Thread 对象转成可以存进 MongoDB 的字典。

    Thread 是 dataclass，用 dataclasses.asdict 就能递归转成 dict；
    里面的 Gender / ThreadType 都是 IntEnum，MongoDB 能直接存。

    额外补充了几个方便查询的字段：
        create_dt / last_dt : 把 10 位秒级时间戳换成真正的 Date 类型
        crawled_at          : 本次爬到这条数据的时间
        source              : 数据来源接口名
        posted_at_text      : 人类可读的时间，方便你直接看
    """
    doc = dcs.asdict(thread)

    # 清洗成 BSON 能编码的类型：去掉 nan/inf，把 yarl.URL 之类的第三方对象转成字符串。
    # 不做这一步，正文里带链接的帖子会让 **整页** 写入失败并让爬虫崩掉。
    doc, converted = make_bson_safe(doc)
    if converted:
        tb.get_logger().debug("字段类型已转换：%s", ", ".join(converted))

    create_time = doc.get("create_time") or 0
    last_time = doc.get("last_time") or 0
    if create_time:
        doc["create_dt"] = datetime.fromtimestamp(create_time, tz=timezone.utc)
        doc["posted_at_text"] = datetime.fromtimestamp(create_time).strftime("%Y-%m-%d %H:%M:%S")
    if last_time:
        doc["last_dt"] = datetime.fromtimestamp(last_time, tz=timezone.utc)

    doc["source"] = source
    doc["crawled_at"] = datetime.now(tz=timezone.utc)
    return doc


_BSON_SCALARS = (str, int, float, bool, bytes, ObjectId, datetime)


def _url_to_str(value: Any) -> str:
    """把 yarl.URL / 其它对象转成字符串"""
    try:
        return str(value)
    except Exception:
        return repr(value)


def _sanitize(obj: Any, unknown: list[tuple[str, str, Any]] | None = None, path: str = "doc") -> Any:
    """
    就地清洗成 BSON 能编码的类型。

    处理两类问题：
    1. NaN / Infinity —— MongoDB 存不了这种浮点值
    2. 第三方对象类型 —— 最典型的是 yarl.URL。

    关于第 2 点（这是实际踩到的坑）：
        贴吧帖子的正文里如果带**链接**，aiotieba 的 FragLink / FragTiebaPlus
        会把链接解析成 yarl.URL 对象。bson 不认识这个类型，编码整条文档时会抛：
            bson.errors.InvalidDocument: cannot encode object: URL('http://...'),
            of type: <class 'yarl.URL'>
        结果就是**整页写入失败、爬虫直接崩掉**。
        本项目实测：营山二中 849 条帖子里有 9 条带链接，全部会触发这个错误。
        "中国人口""弱智"两个吧没有这种帖子，所以一开始没暴露出来。

    对未知类型统一转成字符串 —— 信息不丢，而且以后不会再因为某个新类型崩掉。
    """
    if isinstance(obj, dict):
        for key in list(obj.keys()):
            value = obj[key]
            sub_path = f"{path}.{key}"
            if isinstance(value, float) and (value != value or value in (float("inf"), float("-inf"))):
                obj[key] = None
            elif isinstance(value, _BSON_SCALARS) or value is None:
                continue
            elif isinstance(value, (dict, list, tuple)):
                obj[key] = _sanitize(value, unknown, sub_path)
            else:
                if unknown is not None:
                    unknown.append((sub_path, type(value).__name__, str(value)[:80]))
                obj[key] = _url_to_str(value)
        return obj

    if isinstance(obj, (list, tuple)):
        items = list(obj)
        for i, value in enumerate(items):
            sub_path = f"{path}[{i}]"
            if isinstance(value, float) and (value != value or value in (float("inf"), float("-inf"))):
                items[i] = None
            elif isinstance(value, _BSON_SCALARS) or value is None:
                continue
            elif isinstance(value, (dict, list, tuple)):
                items[i] = _sanitize(value, unknown, sub_path)
            else:
                if unknown is not None:
                    unknown.append((sub_path, type(value).__name__, str(value)[:80]))
                items[i] = _url_to_str(value)
        return items

    # 不在容器里（文档根就是未知类型）—— 转字符串
    return _url_to_str(obj)


def make_bson_safe(doc: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    """
    把任意文档清洗成 BSON 能编码的形式，返回 (新文档, 被转成字符串的字段说明列表)。

    用在入库前的兜底：万一以后 aiotieba 引入了新的第三方类型，
    也不会再让整个爬虫崩掉，最多是那个字段变成字符串。
    """
    unknown: list[tuple[str, str, Any]] = []
    safe = _sanitize(dcopy.deepcopy(doc), unknown)
    notes = []
    seen = set()
    for sub_path, type_name, _ in unknown:
        # 同一类字段去重，比如 contents.links[3].raw_url 和 [7].raw_url 只报一次
        key = re.sub(r"\[\d+\]", "[]", sub_path)
        if key not in seen:
            seen.add(key)
            notes.append(f"{key} ({type_name})")
    return safe, notes


def doc_size(doc: dict[str, Any]) -> int:
    """文档的 BSON 字节大小"""
    return len(BSON.encode(doc))


# 查询时只取需要的字段，别把整个 contents 正文都拉出来（省内存、省网络）
_LIST_PROJECTION = {
    "_id": 1,
    "tid": 1,
    "fname": 1,
    "fid": 1,
    "title": 1,
    "author_id": 1,
    "user.user_name": 1,
    "user.nick_name_new": 1,
    "view_num": 1,
    "reply_num": 1,
    "share_num": 1,
    "agree": 1,
    "disagree": 1,
    "is_good": 1,
    "is_top": 1,
    "type": 1,
    "create_time": 1,
    "last_time": 1,
    "posted_at_text": 1,
    "crawled_at": 1,
}


class ThreadStore:
    """
    主题帖存储仓库。用法：

        async with ThreadStore() as store:
            await store.ensure_indexes()          # 首次运行建索引（可重复执行）
            await store.save_threads(threads)     # 批量入库（自动去重）
            rows = await store.top_by_views("中国人口", limit=10)
    """

    # 索引定义：(索引名, 字段列表, 是否唯一)
    _INDEXES: ClassVar[list[tuple[str, list[tuple[str, int]], bool]]] = [
        ("uniq_tid", [("tid", ASCENDING)], True),
        ("fname_create", [("fname", ASCENDING), ("create_time", DESCENDING)], False),
        ("fname_view", [("fname", ASCENDING), ("view_num", DESCENDING)], False),
        ("fname_reply", [("fname", ASCENDING), ("reply_num", DESCENDING)], False),
        ("create_time", [("create_time", DESCENDING)], False),
        ("title_text", [("title", "text")], False),
    ]

    def __init__(
        self,
        uri: str = MONGO_URI,
        db_name: str = MONGO_DB,
        collection_name: str = MONGO_COLLECTION,
    ) -> None:
        self.uri = uri
        self.db_name = db_name
        self.collection_name = collection_name
        self.client: AsyncMongoClient | None = None
        self.collection = None

    # ------------------------------------------------------------ 生命周期
    async def open(self) -> None:
        """
        建立连接并验证服务端确实可达。

        注意 serverSelectionTimeoutMS：默认是 30 秒，本地没启动 MongoDB 时
        会卡 30 秒才报错，这里调短一点，让错误来得快一些。
        """
        self.client = AsyncMongoClient(self.uri, serverSelectionTimeoutMS=5000, tz_aware=True)
        self.collection = self.client[self.db_name][self.collection_name]
        # ping 一下，确认真的连上了（否则要等到第一次写才报错）
        await self.client.admin.command("ping")

    async def close(self) -> None:
        if self.client is not None:
            await self.client.close()
            self.client = None
            self.collection = None

    async def __aenter__(self) -> ThreadStore:
        await self.open()
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.close()

    # -------------------------------------------------------------- 建索引
    async def ensure_indexes(self) -> list[str]:
        """创建索引，重复调用不会出错。返回建出来的索引名列表"""
        assert self.collection is not None, "请先 await store.open()"
        names = []
        for name, keys, unique in self._INDEXES:
            await self.collection.create_index(keys, name=name, unique=unique)
            names.append(name)
        return names

    # ---------------------------------------------------------------- 写入
    async def save_threads(
        self,
        threads: list[Any],
        *,
        source: str = "get_threads",
        batch_size: int = 500,
    ) -> dict[str, int]:
        """
        批量写入（按 tid 去重更新）。

        返回统计信息：新增几条、更新几条、跳过几条（文档过大、没 tid、或编码失败）。

        Why 用 update_one + upsert 而不是 insert_many：
            insert_many 遇到重复 tid 会报 DuplicateKeyError，
            而 upsert 是"有这个 tid 就更新、没有就插入"，天然幂等，
            而且 8 个协程并发爬同一页时也不会互相冲突。

        关于容错：
            单条帖子的编码失败**不能让整页写入失败**。以前正文里带链接的帖子会抛
            bson.errors.InvalidDocument（yarl.URL 存不了），导致整个爬虫崩掉。
            现在 doc_from_thread 会先做类型清洗，这里再加一层兜底：
            实在编码不了的，记一条 warning 日志然后跳过这一条，别的帖子照常入库。
        """
        assert self.collection is not None, "请先 await store.open()"

        now = datetime.now(tz=timezone.utc)
        ops: list[UpdateOne] = []
        stats = {"inserted": 0, "updated": 0, "skipped": 0, "total": len(threads)}

        for thread in threads:
            if not getattr(thread, "tid", 0):
                stats["skipped"] += 1
                continue

            try:
                doc = doc_from_thread(thread, source=source)
                size = doc_size(doc)
            except Exception as exc:
                # 兜底：清洗过后仍然编码不了（比如 aiotieba 以后引入了新的第三方类型）
                stats["skipped"] += 1
                tb.get_logger().warning(
                    "tid=%s 编码失败已跳过: %s: %s", getattr(thread, "tid", "?"),
                    type(exc).__name__, exc,
                )
                continue

            if size > MAX_DOC_BYTES:
                # 极长的楼中楼/大量图片才会遇到，跳过并让调用方知道
                tb.get_logger().warning(
                    "tid=%s 文档过大已跳过 (%.1f MB)", doc["tid"], size / 1024 / 1024
                )
                stats["skipped"] += 1
                continue

            tid = doc["tid"]
            doc.pop("_id", None)
            ops.append(
                UpdateOne(
                    {"tid": tid},
                    {
                        "$set": doc,
                        # 第一次插入时记录首次爬取时间，之后的更新不会覆盖它
                        "$setOnInsert": {"first_crawled_at": now},
                    },
                    upsert=True,
                )
            )

        if stats["skipped"]:
            tb.get_logger().warning(
                "本次共 %d/%d 条被跳过（见上面的 warning 原因）", stats["skipped"], stats["total"]
            )

        for start in range(0, len(ops), batch_size):
            chunk = ops[start : start + batch_size]
            result = await self.collection.bulk_write(chunk, ordered=False)
            stats["inserted"] += result.upserted_count
            stats["updated"] += result.modified_count
        return stats

    # ---------------------------------------------------------------- 查询
    async def top_by_views(self, fname: str | None = None, limit: int = 10) -> list[dict]:
        """按浏览量降序取前 N 条"""
        return await self._find_sorted("view_num", fname=fname, limit=limit)

    async def top_by_replies(self, fname: str | None = None, limit: int = 10) -> list[dict]:
        """按回复数降序取前 N 条"""
        return await self._find_sorted("reply_num", fname=fname, limit=limit)

    async def latest(self, fname: str | None = None, limit: int = 10) -> list[dict]:
        """按发帖时间降序取前 N 条"""
        return await self._find_sorted("create_time", fname=fname, limit=limit)

    async def _find_sorted(self, field: str, *, fname: str | None, limit: int) -> list[dict]:
        assert self.collection is not None, "请先 await store.open()"
        query = {"fname": fname} if fname else {}
        cursor = (
            self.collection.find(query, _LIST_PROJECTION)
            .sort(field, DESCENDING)
            .limit(limit)
        )
        return [doc async for doc in cursor]

    async def search_title(self, keyword: str, limit: int = 20) -> list[dict]:
        """按标题模糊搜索（用了正则，数据量大时较慢，建议配合 fname 缩小范围）"""
        assert self.collection is not None, "请先 await store.open()"
        import re

        query = {"title": {"$regex": re.escape(keyword), "$options": "i"}}
        cursor = self.collection.find(query, _LIST_PROJECTION).limit(limit)
        return [doc async for doc in cursor]

    async def count(self, fname: str | None = None) -> int:
        """统计文档总数"""
        assert self.collection is not None, "请先 await store.open()"
        return await self.collection.count_documents({"fname": fname} if fname else {})

    async def stats(self) -> dict[str, Any]:
        """汇总统计：每个吧有多少帖子、总浏览量等"""
        assert self.collection is not None, "请先 await store.open()"
        pipeline = [
            {
                "$group": {
                    "_id": "$fname",
                    "threads": {"$sum": 1},
                    "total_views": {"$sum": "$view_num"},
                    "total_replies": {"$sum": "$reply_num"},
                    "max_views": {"$max": "$view_num"},
                }
            },
            {"$sort": {"threads": -1}},
        ]
        # 注意：异步驱动的 aggregate() 返回的是协程，必须先 await 拿到游标再迭代
        # （find() 不一样，它直接返回游标，不需要 await —— 这两个 API 不一致，容易踩坑）
        cursor = await self.collection.aggregate(pipeline)
        rows = [doc async for doc in cursor]
        total = await self.collection.count_documents({})
        return {"total_documents": total, "by_forum": rows}
