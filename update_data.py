#!/usr/bin/env python3
"""更新 Atlas data.json；Python 3.10+，先执行 pip install -r requirements.txt。

    python update_data.py
    python update_data.py --dry-run
    python update_data.py --strict --history-days 30

接口参考 feer_index.md。日线按 UTC+8 划分，仅接受 confirm=1。
默认独立更新各数据源，失败项保留原值；--strict 在任何失败时不写入。
退出码：0 全部成功，1 文件/配置错误，2 部分或全部接口失败。
BTC、ETH、BNB 使用 SYMBOL-USDT，其余资产使用 SYMBOL-USDT-SWAP，与分组无关。
仅获取敞口不为 0 的资产价格；零敞口资产保持原样，不发起价格请求。
资产仅更新 price 和 exposure，敞口按数量乘新价格计算并保留空头方向。
敞口四舍五入至两位小数，其他资产字段保持不变，不新增价格附加字段。
"""

from __future__ import annotations

import argparse
import copy
import json
import logging
import math
import os
import re
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path

import requests

LOG = logging.getLogger("atlas.update")
SHANGHAI = timezone(timedelta(hours=8))
CNN_URL = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"
CNN_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://www.cnn.com/",
}
FNG_URL = "https://api.alternative.me/fng/"
OKX_URL = "https://www.okx.com/api/v5/market/candles"
SPOT_SYMBOLS = frozenset({"BTC", "ETH", "BNB"})
DEFAULT_FILE = Path(__file__).resolve().with_name("data.json")


class DataError(ValueError):
    """可向用户报告的输入或上游数据错误。"""


def number(value, label: str, *, minimum=None, maximum=None) -> float:
    if isinstance(value, bool) or value is None:
        raise DataError(f"{label} 不是有效数值")
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise DataError(f"{label} 不是有效数值") from exc
    if not math.isfinite(result) or (minimum is not None and result < minimum) or (
        maximum is not None and result > maximum
    ):
        raise DataError(f"{label} 超出有效范围")
    return result


def timestamp(value, *, milliseconds=False) -> datetime:
    try:
        if isinstance(value, str) and not re.fullmatch(r"\d+(?:\.\d+)?", value):
            result = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if result.tzinfo is None:
                raise DataError("时间必须包含时区")
        else:
            seconds = number(value, "时间戳", minimum=1)
            result = datetime.fromtimestamp(seconds / (1000 if milliseconds else 1), timezone.utc)
        if result > datetime.now(timezone.utc) + timedelta(minutes=5):
            raise DataError("上游时间戳位于未来")
        return result.astimezone(timezone.utc)
    except (ValueError, OverflowError, OSError) as exc:
        raise DataError(f"无效时间戳: {value}") from exc


def rating(score: float) -> str:
    # 与 index.html 的分段保持一致。
    return next(label for ceiling, label in [(24, "极端恐惧"), (44, "恐惧"),
                (55, "中性"), (74, "贪婪"), (100, "极端贪婪")] if score <= ceiling)


class HttpClient:
    def __init__(self, timeout=15.0, retries=2):
        self.timeout = timeout
        self.retries = retries

    def get(self, url: str, params=None, headers=None) -> dict:
        request_headers = {"User-Agent": "Mozilla/5.0",
                           "Accept": "application/json", **(headers or {})}
        for attempt in range(self.retries + 1):
            delay = min(2 ** attempt, 8)
            try:
                with requests.get(url, params=params, headers=request_headers,
                                  timeout=self.timeout) as response:
                    response.raise_for_status()
                    payload = response.json()
                if not isinstance(payload, dict):
                    raise DataError("接口返回的 JSON 必须为对象")
                # OKX 业务限流也可能使用 HTTP 200。
                if str(payload.get("code")) in {"50011", "50040"}:
                    raise requests.exceptions.ConnectionError("接口限流")
                return payload
            except requests.exceptions.HTTPError as exc:
                status = exc.response.status_code
                if status not in {429, 500, 502, 503, 504} or attempt == self.retries:
                    raise DataError(f"HTTP {status}") from exc
                retry_after = exc.response.headers.get("Retry-After", "")
                if retry_after.isdigit():
                    delay = min(int(retry_after), 30)
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError, OSError) as exc:
                if attempt == self.retries:
                    raise DataError(f"连接失败: {exc}") from exc
            except (ValueError, UnicodeError) as exc:
                raise DataError(f"响应格式错误: {exc}") from exc
            except requests.exceptions.RequestException as exc:
                raise DataError(f"请求失败: {exc}") from exc
            time.sleep(delay)
        raise DataError("请求失败")


def sentiment_result(points, source: str, days: int) -> dict:
    if not points:
        raise DataError("情绪历史为空")
    # 每个 UTC 日期保留最新观测值，历史从旧到新；不生成模拟值。
    daily = {}
    for observed, score in sorted(points):
        score = number(score, "情绪分数", minimum=0, maximum=100)
        daily[observed.date()] = (
            observed,
            int(Decimal(str(score)).quantize(Decimal("1"), rounding=ROUND_HALF_UP)),
        )
    records = list(daily.values())[-days:]
    observed, score = records[-1]
    if datetime.now(timezone.utc) - observed > timedelta(days=7):
        raise DataError("情绪数据超过 7 天未更新")
    return {"score": score, "status": rating(score), "source": source,
            "history": [value for _, value in records],
            "historyDates": [date.date().isoformat() for date, _ in records],
            "observedAt": observed.isoformat()}


def fetch_cnn(client: HttpClient, days: int) -> dict:
    payload = client.get(CNN_URL, headers=CNN_HEADERS)
    current = payload.get("fear_and_greed")
    history = payload.get("fear_and_greed_historical", {}).get("data")
    if not isinstance(current, dict) or not isinstance(history, list) or not history:
        raise DataError("CNN 缺少当前指数或 fear_and_greed_historical.data")
    points = []
    for row in history:
        if not isinstance(row, dict):
            raise DataError("CNN 历史记录格式错误")
        points.append((timestamp(row.get("x"), milliseconds=True), row.get("y")))
    latest = timestamp(current.get("timestamp"))
    # 当前快照为最终观测；避免相同日期的历史值覆盖当前分数。
    points = [point for point in points if point[0].date() < latest.date()]
    points.append((latest, current.get("score")))
    return sentiment_result(points, "CNN Fear & Greed", days)


def fetch_crypto(client: HttpClient, days: int) -> dict:
    payload = client.get(FNG_URL, {"limit": days, "format": "json"})
    if payload.get("metadata", {}).get("error"):
        raise DataError("Alternative.me 返回业务错误")
    rows = payload.get("data")
    if not isinstance(rows, list) or not rows or not all(isinstance(row, dict) for row in rows):
        raise DataError("Alternative.me 历史记录格式错误")
    points = [(timestamp(row.get("timestamp")), row.get("value")) for row in rows]
    return sentiment_result(points, "Alternative.me Fear & Greed", days)


def instrument_for(asset: dict) -> str:
    symbol = asset["symbol"].upper()
    suffix = "-USDT" if symbol in SPOT_SYMBOLS else "-USDT-SWAP"
    instrument = symbol + suffix
    if not re.fullmatch(r"[A-Z0-9]+(?:-[A-Z0-9]+)*" + re.escape(suffix), instrument):
        raise DataError(f"{asset['symbol']} 无法生成有效产品 ID")
    return instrument


def fetch_close(client: HttpClient, instrument: str) -> float:
    payload = client.get(OKX_URL, {"instId": instrument, "bar": "1D", "limit": 10})
    if str(payload.get("code")) != "0":
        raise DataError(f"OKX {payload.get('code')}: {str(payload.get('msg', '请求失败'))[:180]}")
    rows = payload.get("data")
    if not isinstance(rows, list):
        raise DataError("日线数据格式错误")
    closed = []
    for row in rows:
        if not isinstance(row, list) or len(row) != 9 or str(row[8]) not in {"0", "1"}:
            raise DataError("日线字段格式错误")
        if str(row[8]) == "1":
            opened = timestamp(row[0], milliseconds=True)
            close = number(row[4], "日线收盘价", minimum=0)
            if close <= 0:
                raise DataError("日线收盘价必须大于零")
            closed.append((opened, close))
    if not closed:
        raise DataError("没有已完结日线")
    opened, close = max(closed)
    closed_at = opened + timedelta(days=1)
    now = datetime.now(timezone.utc)
    if closed_at > now or now - closed_at > timedelta(days=7):
        raise DataError("已完结日线时间无效或超过 7 天未更新")
    return close


def validate_document(data: dict) -> None:
    if not isinstance(data, dict) or not all(isinstance(data.get(k), dict)
                                            for k in ("dashboard", "sentiment", "holdings")):
        raise DataError("data.json 缺少 dashboard、sentiment 或 holdings 对象")
    for key in ("usStocks", "crypto"):
        if not isinstance(data["sentiment"].get(key), dict):
            raise DataError(f"缺少 sentiment.{key}")
    for group, assets in data["holdings"].items():
        if not isinstance(assets, list):
            raise DataError(f"持仓分组 {group} 必须为数组")
        for asset in assets:
            if not isinstance(asset, dict) or not isinstance(asset.get("symbol"), str) or not asset["symbol"]:
                raise DataError(f"{group} 中存在无效持仓")
            for key in ("price", "quantity", "cost", "exposure", "margin"):
                if not isinstance(asset.get(key), (int, float)):
                    raise DataError(f"{asset['symbol']}.{key} 必须为 JSON 数值")
                number(asset[key], f"{asset['symbol']}.{key}")
            if asset["exposure"] != 0 and asset["quantity"] == 0:
                raise DataError(f"{asset['symbol']} 非零敞口缺少持仓数量")
            if asset["exposure"] != 0:
                instrument_for(asset)


def updated_exposure(asset: dict, price: float) -> float:
    if asset["exposure"] == 0:
        return 0.0
    sign = -1 if asset["exposure"] < 0 or asset["quantity"] < 0 else 1
    try:
        value = abs(Decimal(str(asset["quantity"]))) * Decimal(str(price)) * sign
        return number(value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP), "更新后敞口")
    except InvalidOperation as exc:
        raise DataError("更新后敞口超出有效范围") from exc


def update_document(data: dict, client: HttpClient, days: int = 30) -> tuple[dict, dict]:
    validate_document(data)
    updated = copy.deepcopy(data)
    jobs = {"sentiment.usStocks": (fetch_cnn, days), "sentiment.crypto": (fetch_crypto, days)}
    targets = {}
    skipped = []
    skipped_zero_exposure = []
    for group, assets in updated["holdings"].items():
        for asset in assets:
            if asset["exposure"] == 0:
                skipped.append(asset["symbol"])
                skipped_zero_exposure.append(asset["symbol"])
                LOG.info("跳过零敞口资产 %s", asset["symbol"])
                continue
            instrument = instrument_for(asset)
            key = f"price.{instrument}"
            jobs[key] = (fetch_close, instrument)
            targets.setdefault(key, []).append(asset)
    report = {"status": "success", "succeeded": [], "failed": {}, "skipped": skipped,
              "skippedZeroExposure": skipped_zero_exposure}
    with ThreadPoolExecutor(max_workers=4) as pool:
        pending = {pool.submit(fn, client, arg): key for key, (fn, arg) in jobs.items()}
        for future in as_completed(pending):
            key = pending[future]
            try:
                result = future.result()
                if key.startswith("sentiment."):
                    target = updated["sentiment"][key.split(".")[1]]
                    if target.get("observedAt") and timestamp(result["observedAt"]) < timestamp(target["observedAt"]):
                        raise DataError("拒绝用较旧指数覆盖已有数据")
                    target.update(result)
                else:
                    if key == "price.XIAOMI-USDT-SWAP":
                        result = number(
                            (Decimal(str(result)) * Decimal("7.84")).quantize(
                                Decimal("0.01"), rounding=ROUND_HALF_UP
                            ),
                            "小米换算后价格",
                        )
                    changes = [(asset, updated_exposure(asset, result)) for asset in targets[key]]
                    for asset, exposure in changes:
                        asset["price"] = result
                        asset["exposure"] = exposure
                report["succeeded"].append(key)
                LOG.info("已获取 %s", key)
            except (DataError, TypeError, KeyError, AttributeError) as exc:
                report["failed"][key] = str(exc)
                LOG.warning("保留原值 %s: %s", key, exc)
    report["succeeded"].sort()
    report["failed"] = dict(sorted(report["failed"].items()))
    if report["failed"]:
        report["status"] = "partial" if report["succeeded"] else "failed"
    if report["succeeded"]:
        updated["dashboard"]["updatedAt"] = datetime.now(SHANGHAI).isoformat(timespec="seconds")
        updated["dashboard"]["dataUpdate"] = report
    validate_document(updated)
    return updated, report


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


def save_document(path: Path, original: bytes, updated: dict) -> None:
    content = (json.dumps(updated, ensure_ascii=False, indent=2, allow_nan=False) + "\n").encode("utf-8")
    if path.read_bytes() != original:
        raise DataError("获取期间 data.json 已被修改，本次取消写入，请重试")
    atomic_write(path.with_suffix(path.suffix + ".bak"), original)
    atomic_write(path, content)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--data", type=Path, default=DEFAULT_FILE, help="目标 JSON，默认脚本同目录 data.json")
    parser.add_argument("--history-days", type=int, default=30, help="情绪历史观测数，1–365")
    parser.add_argument("--timeout", type=float, default=15, help="单次请求超时秒数")
    parser.add_argument("--retries", type=int, default=2, help="临时错误重试次数，0–5")
    parser.add_argument("--dry-run", action="store_true", help="实际获取和校验，但不写文件")
    parser.add_argument("--strict", action="store_true", help="任一接口失败则不写文件")
    args = parser.parse_args(argv)
    if not 1 <= args.history_days <= 365 or not 0 <= args.retries <= 5 or not 0 < args.timeout <= 120:
        parser.error("history-days、retries 或 timeout 超出有效范围")
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        path = args.data.resolve()
        original = path.read_bytes()
        data = json.loads(original.decode("utf-8-sig"))
        updated, report = update_document(data, HttpClient(args.timeout, args.retries), args.history_days)
        should_write = bool(report["succeeded"]) and not args.dry_run and not (args.strict and report["failed"])
        if should_write:
            save_document(path, original, updated)
        LOG.info("%s：成功 %d，失败 %d，跳过 %d；%s", report["status"],
                 len(report["succeeded"]), len(report["failed"]), len(report["skipped"]),
                 f"已写入 {path}，原文件备份为 {path.name}.bak" if should_write else "未写入文件")
        return 2 if report["failed"] else 0
    except (OSError, ValueError, UnicodeError) as exc:
        LOG.error("更新终止: %s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
