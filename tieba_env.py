"""
配置加载：把项目根目录下的 `.env` 读进环境变量，并对外提供取配置的函数。

为什么要这个文件
----------------
贴吧的 **BDUSS 等同于账号凭据**，绝对不能写进代码里（尤其是要推到公开仓库的时候）。
本项目所有敏感配置都从环境变量 / `.env` 读取，代码里一个真实凭据都没有。

用法：
    1. 复制模板，填上自己的值
           copy .env.example .env
    2. 编辑 `.env`，至少填 `TIEBA_BDUSS=...`
    3. 正常跑 crawler.py / api_server.py 即可，无需改代码

优先级：**真实环境变量 > .env 文件 > 默认值**
也就是说 `$env:TIEBA_BDUSS = "xxx"` 会覆盖 `.env` 里的值，方便临时切换账号。

只依赖标准库，不需要 python-dotenv。
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Final

# 项目根目录 = 本文件所在目录
PROJECT_ROOT: Final[Path] = Path(__file__).resolve().parent
ENV_FILE: Final[Path] = PROJECT_ROOT / ".env"


def _parse_env_line(line: str) -> tuple[str, str] | None:
    """把 `.env` 里的一行解析成 (key, value)，解析不了就返回 None"""
    line = line.strip()
    # 跳过空行和注释
    if not line or line.startswith("#"):
        return None
    if line.startswith("export "):          # 兼容 shell 风格的写法
        line = line[len("export "):].lstrip()
    key, sep, value = line.partition("=")
    if not sep:
        return None
    key = key.strip()
    if not key:
        return None
    value = value.strip()
    # 去掉成对的引号（值里含 # 或空格时需要引号）
    if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
        value = value[1:-1]
    return key, value


def load_dotenv(path: Path = ENV_FILE, *, override: bool = False) -> list[str]:
    """
    读取 `.env` 并写入 `os.environ`。

    Args:
        path: `.env` 文件路径
        override: 为 True 时覆盖已存在的环境变量；默认 False（真实环境变量优先）

    Returns:
        实际生效的变量名列表（用于启动时打印，方便确认配置来源）
    """
    if not path.is_file():
        return []

    loaded: list[str] = []
    # utf-8-sig：Windows 上用记事本保存的 .env 常带 BOM，这个编码能自动吃掉 BOM
    for raw_line in path.read_text(encoding="utf-8-sig").splitlines():
        parsed = _parse_env_line(raw_line)
        if parsed is None:
            continue
        key, value = parsed
        if override or key not in os.environ:
            os.environ[key] = value
            loaded.append(key)
    return loaded


# 模块被 import 时立刻加载一次，这样后面的 os.getenv 才能拿到 .env 里的值
LOADED_KEYS: Final[list[str]] = load_dotenv()


# ---------------------------------------------------------------- BDUSS 相关
def get_bduss() -> str:
    """取 BDUSS，取不到返回空字符串"""
    return (os.getenv("TIEBA_BDUSS") or "").strip()


BDUSS_HELP = """
拿不到 TIEBA_BDUSS，爬虫没法登录贴吧。三步搞定：

  1. 复制一份配置文件（在项目根目录执行）
         copy .env.example .env

  2. 用记事本打开 .env，把 TIEBA_BDUSS 填上
         TIEBA_BDUSS=你的一长串BDUSS

  3. 重新运行

BDUSS 怎么拿（电脑浏览器）：
  · 打开 https://tieba.baidu.com 并确保**已登录**
  · 按 F12 → Application（应用程序）→ Cookies → https://tieba.baidu.com
  · 找到名字叫 BDUSS 的那一行，复制它的完整 Value（很长一串）
  · 只复制 Value，不要带 "BDUSS=" 这几个字

⚠️ BDUSS 等于你的账号凭据，泄露 = 别人可以用你的号发帖。
   `.env` 已经被 .gitignore 忽略，**不要**把真实 BDUSS 写进任何 .py 文件。
"""


def require_bduss() -> str:
    """取 BDUSS，取不到就抛一个带完整操作指引的异常"""
    bduss = get_bduss()
    if not bduss:
        raise RuntimeError(BDUSS_HELP)
    return bduss


def describe_config() -> str:
    """一行话说明当前配置从哪来，给启动日志用（不会打印任何密钥内容）"""
    source = f"{ENV_FILE.name} + 环境变量" if ENV_FILE.is_file() else "环境变量（未找到 .env）"
    bduss = get_bduss()
    bduss_state = f"已配置（{len(bduss)} 字符）" if bduss else "未配置"
    return f"配置来源：{source} | TIEBA_BDUSS：{bduss_state}"
