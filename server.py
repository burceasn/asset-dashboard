#!/usr/bin/env python3
"""配套脚本：让 index.html “全部持仓”的卡片编辑写回 data.json。

    python server.py        # 打开 http://127.0.0.1:8000

只做两件事：原样提供本目录的静态文件；把页面保存的修改校验后写回 data.json
（原文件备份为 data.json.bak，并刷新 dashboard.updatedAt）。只接受持仓的总敞口、
持仓数量、成本价、现价、保证金、多空方向这六个字段，其余字段不开放修改；
校验口径与 update_data.py 一致，写坏的数据会被拒绝。静态托管（如 GitHub Pages）
没有该接口，页面会提示并提供“导出 data.json”。仅依赖标准库。
"""

from __future__ import annotations

import json
import math
import os
import re
import tempfile
import threading
from datetime import datetime, timedelta, timezone
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit

DATA = Path(__file__).resolve().with_name("data.json")
SHANGHAI = timezone(timedelta(hours=8))
LOGO_RE = re.compile(r"asset/[\w.-]+\.svg")
COLOR_RE = re.compile(r"#(?:[0-9a-f]{3}|[0-9a-f]{4}|[0-9a-f]{6}|[0-9a-f]{8})", re.IGNORECASE)
SYMBOL_RE = re.compile(r"[A-Za-z0-9]+(?:-[A-Za-z0-9]+)*")
NONNEGATIVE = {"cost", "price", "margin", "balance", "annualRate"}  # 数量与敞口可为负表示空头，其余金额不允许为负。
HOLDING_FIELDS = (
    {"direction"},                              # 必填文本
    set(),                                      # 可留空文本
    {"quantity", "cost", "price", "exposure", "margin"},  # 仅开放这六个字段的修改
)
LIQUIDITY_FIELDS = {
    "cash": ({"symbol", "name"}, {"logo"}, {"balance"}),
    "margin": ({"symbol", "name"}, {"logo"}, {"balance", "annualRate"}),
}


class DataError(ValueError):
    """可回传给页面展示的输入错误。"""


def number(value, label: str, key: str = "") -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise DataError(f"{label} 必须是数字")
    result = float(value)
    if not math.isfinite(result):
        raise DataError(f"{label} 不是有效数值")
    if key in NONNEGATIVE and result < 0:
        raise DataError(f"{label} 不能为负数")
    return result


def text(value, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DataError(f"{label} 不能为空")
    return value.strip()


def validate_document(data: dict) -> None:
    """与 update_data.py 的 validate_document 保持同一口径。"""
    if (not isinstance(data, dict) or not isinstance(data.get("dashboard"), dict)
            or not isinstance(data.get("sentiment"), dict) or not isinstance(data.get("liquidity"), dict)
            or not isinstance(data.get("holdings"), list)):
        raise DataError("data.json 结构不完整")
    for key in ("usStocks", "crypto"):
        if not isinstance(data["sentiment"].get(key), dict):
            raise DataError(f"缺少 sentiment.{key}")
    for part, (_, _, number_fields) in LIQUIDITY_FIELDS.items():
        record = data["liquidity"].get(part)
        if not isinstance(record, dict) or not isinstance(record.get("symbol"), str) or not record["symbol"]:
            raise DataError(f"liquidity.{part} 数据无效")
        for key in number_fields:
            number(record.get(key), f"{part}.{key}", key)
    seen = set()
    for asset in data["holdings"]:
        if not isinstance(asset, dict) or not isinstance(asset.get("symbol"), str) or not asset["symbol"]:
            raise DataError("holdings 中存在无效持仓")
        symbol = asset["symbol"]
        if symbol.lower() in seen:
            raise DataError(f"持仓代码重复：{symbol}")
        seen.add(symbol.lower())
        for key in HOLDING_FIELDS[2]:
            number(asset.get(key), f"{symbol}.{key}", key)
        if asset.get("direction") not in ("long", "short"):
            raise DataError(f"{symbol}.direction 必须是 long 或 short")
        logo, color = asset.get("logo", ""), asset.get("color", "")
        if not isinstance(logo, str) or (logo and not LOGO_RE.fullmatch(logo)):
            raise DataError(f"{symbol}.logo 必须形如 asset/XXX.svg")
        if not isinstance(color, str) or (color and not COLOR_RE.fullmatch(color)):
            raise DataError(f"{symbol}.color 必须是 #RGB 形式的色值")
        if asset["exposure"] != 0 and asset["quantity"] == 0:
            raise DataError(f"{symbol} 非零敞口缺少持仓数量")
        if asset["exposure"] != 0 and not SYMBOL_RE.fullmatch(symbol.upper()):
            raise DataError(f"{symbol} 非零敞口的代码只允许字母、数字和连字符")


def apply_patch(record: dict, patch: dict, fields, label: str) -> None:
    required, optional, numbers = fields
    unknown = set(patch) - required - optional - numbers
    if unknown:
        raise DataError(f"{label} 不支持修改：{'、'.join(sorted(unknown))}")
    for key in required & set(patch):
        record[key] = text(patch[key], f"{label}.{key}")
    for key in optional & set(patch):
        if not isinstance(patch[key], str):
            raise DataError(f"{label}.{key} 必须是文本")
        record[key] = patch[key].strip()
    for key in numbers & set(patch):
        record[key] = number(patch[key], f"{label}.{key}", key)


def apply_update(data: dict, payload: dict) -> None:
    target = payload.get("target") if isinstance(payload, dict) else None
    patch = payload.get("patch") if isinstance(payload, dict) else None
    if not isinstance(target, dict) or not isinstance(patch, dict) or not patch:
        raise DataError("缺少需要保存的修改")
    if target.get("type") != "holding":
        raise DataError("target.type 必须是 holding")
    record = next((a for a in data["holdings"] if isinstance(a, dict)
                   and a.get("symbol") == target.get("symbol")), None)
    if record is None:
        raise DataError(f"未找到持仓 {target.get('symbol')}")
    apply_patch(record, patch, HOLDING_FIELDS, str(target.get("symbol")))
    validate_document(data)
    data["dashboard"]["updatedAt"] = datetime.now(SHANGHAI).isoformat(timespec="seconds")


def atomic_write(path: Path, content: bytes) -> None:
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="wb", dir=path.parent, prefix=f".{path.name}.",
                                         suffix=".tmp", delete=False) as handle:
            temporary = Path(handle.name)
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


class Handler(SimpleHTTPRequestHandler):
    lock = threading.Lock()

    def send_json(self, payload: dict, status: int = 200) -> None:
        body = json.dumps(payload, ensure_ascii=False, allow_nan=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        if urlsplit(self.path).path != "/api/update":
            self.send_json({"ok": False, "error": "未知接口"}, status=404)
            return
        try:
            length = int(self.headers.get("Content-Length") or 0)
            if not 0 < length <= 65536:
                raise DataError("请求体大小无效")
            payload = json.loads(self.rfile.read(length).decode("utf-8"))
            with self.lock:
                original = DATA.read_bytes()
                data = json.loads(original.decode("utf-8-sig"))
                apply_update(data, payload)
                content = (json.dumps(data, ensure_ascii=False, indent=2, allow_nan=False) + "\n").encode("utf-8")
                atomic_write(DATA.with_suffix(DATA.suffix + ".bak"), original)
                atomic_write(DATA, content)
        except DataError as exc:
            self.send_json({"ok": False, "error": str(exc)}, status=400)
            return
        except (OSError, ValueError, UnicodeError) as exc:
            self.send_json({"ok": False, "error": f"写入失败：{exc}"}, status=400)
            return
        self.send_json({"ok": True, "data": data})

    def log_message(self, format, *args):  # noqa: A002 - 基类接口
        print(f"{format % args}")


if __name__ == "__main__":
    validate_document(json.loads(DATA.read_bytes().decode("utf-8-sig")))
    print("Atlas 本地编辑：http://127.0.0.1:8000 （Ctrl+C 退出）")
    ThreadingHTTPServer(("127.0.0.1", 8000), Handler).serve_forever()
