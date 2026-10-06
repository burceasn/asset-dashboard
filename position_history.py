#!/usr/bin/env python3
"""仅使用 CSV 保存合约交易历史，不读取或生成独立状态文件。

python position_history.py             # 从 CSV 最新日期零点查询到现在，更新末尾
python position_history.py --dry-run   # 查询和校验，不写入

独立脚本，只依赖 requests、python-dotenv 和标准库；读取同目录 .env。
读取 CSV 最新的北京时间日期，从当天零点重新查询两家交易所的成交。
保留该日期以前的 CSV 行，重建该日期及之后的记录，避免重复追加当天旧交易。
本次查询结果按北京时间日期分组，各日内使用现有合并策略：同交易对、同操作，
相邻成交间隔不超过一分钟或价格相同即合并。历史日期不再重算或跨日归并。
价格按数量加权，数量、名义金额、交易盈亏相加，最后四舍五入到两位。
CSV 按时间排序，时间格式 YYYY/MM/DD/HH/MM/SS；不含交易所或成交 ID 列。
盈亏不含手续费、资金费；仓位金额为名义金额而不是保证金。
两家接口全部成功后原子写入 CSV；写入期间的临时文件自动清除。
"""
from __future__ import annotations

import argparse
import base64
import calendar
import csv
import hashlib
import hmac
import io
import logging
import os
import tempfile
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP, localcontext
from pathlib import Path
from urllib.parse import urlencode

import requests
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent
DEFAULT_FILE = ROOT / "trade_history.csv"
LOCAL = timezone(timedelta(hours=8))
DAY = 86400000
LOG = logging.getLogger("assets.position_history")
FIELDS = ["交易对", "操作", "成交时间", "成交价格", "成交数量", "仓位（USDT）", "已实现盈亏（USDT）"]
NUMBERS = ("price", "qty", "amount", "pnl")


class HistoryError(ValueError):
    pass


def decimal(value):
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise HistoryError("接口或文件包含无效数值") from None
    if not result.is_finite():
        raise HistoryError("接口或文件包含非有限数值")
    return result


def display(value):
    value = decimal(value).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    if value == 0:
        return "0"
    return format(value, "f").rstrip("0").rstrip(".")


def timestamp(value):
    try:
        if isinstance(value, (int, float)) or str(value).isdigit():
            return int(value)
        text = str(value)
        dt = (datetime.strptime(text, "%Y/%m/%d/%H/%M/%S") if "/" in text
              else datetime.fromisoformat(text.replace("Z", "+00:00")))
        return int((dt if dt.tzinfo else dt.replace(tzinfo=LOCAL)).timestamp() * 1000)
    except (ValueError, OverflowError):
        raise HistoryError("时间必须为 ISO 8601 或北京时间 YYYY/MM/DD/HH/MM/SS") from None


def local_time(value):
    return datetime.fromtimestamp(int(value) / 1000, LOCAL).strftime("%Y/%m/%d/%H/%M/%S")


def pair(symbol):
    text = symbol.upper()
    if "/" in text:
        return text
    if "-" in text:
        base, quote, *expiry = text.split("-")
        suffix = "" if not expiry or expiry[0] == "SWAP" else " " + expiry[0]
        return f"{base}/{quote}{suffix}"
    text, _, expiry = text.partition("_")
    for quote in ("USDT", "USDC", "BUSD", "USD"):
        if text.endswith(quote):
            suffix = "" if expiry in ("", "PERP") else " " + expiry
            return text[:-len(quote)] + "/" + quote + suffix
    raise HistoryError(f"无法识别交易对：{symbol}")


def checked_rows(payload, key=None):
    value = payload.get(key) if key and isinstance(payload, dict) else payload
    if not isinstance(value, list) or not all(isinstance(r, dict) for r in value):
        raise HistoryError("接口返回的列表格式错误")
    return value


class HttpClient:
    def __init__(self, timeout=15, retries=2):
        self.timeout, self.retries = timeout, retries
        self.session = requests.Session()

    def get(self, url, *, params=None, headers=None):
        for attempt in range(self.retries + 1):
            try:
                response = self.session.get(url, params=params, headers=headers, timeout=self.timeout)
                if response.status_code == 429 or response.status_code >= 500:
                    if attempt < self.retries:
                        delay = response.headers.get("Retry-After", "")
                        time.sleep(min(int(delay), 30) if delay.isdigit() else 2 ** attempt)
                        continue
                payload = response.json()
            except requests.RequestException:
                if attempt < self.retries:
                    time.sleep(2 ** attempt)
                    continue
                raise HistoryError("网络连接失败或超时") from None
            except ValueError:
                raise HistoryError("接口响应不是 JSON") from None
            return response.status_code, payload
        raise HistoryError("请求重试后仍失败")

    def close(self):
        self.session.close()


class BinanceClient(HttpClient):
    base = "https://fapi.binance.com"

    def __init__(self, timeout=15, retries=2):
        super().__init__(timeout, retries)
        self.key = os.getenv("BINANCE_API_KEY", "")
        self.secret = os.getenv("BINANCE_API_SECRET", "")
        if not self.key or not self.secret:
            raise HistoryError("请配置 BINANCE_API_KEY 和 BINANCE_API_SECRET")
        self.offset = None

    def sync_time(self):
        began = int(time.time() * 1000)
        status, payload = super().get(self.base + "/fapi/v1/time")
        if status != 200 or "serverTime" not in payload:
            raise HistoryError("币安服务器时间获取失败")
        self.offset = int(payload["serverTime"]) - (began + int(time.time() * 1000)) // 2

    def request(self, path, params=None, signed=True):
        for attempt in range(2):
            query, headers = dict(params or {}), {}
            if signed:
                if self.offset is None:
                    self.sync_time()
                query.update(timestamp=int(time.time() * 1000) + self.offset, recvWindow=10000)
                query["signature"] = hmac.new(self.secret.encode(), urlencode(query).encode(), hashlib.sha256).hexdigest()
                headers["X-MBX-APIKEY"] = self.key
            time.sleep(0.1)
            status, payload = super().get(self.base + path, params=query, headers=headers)
            code = payload.get("code") if isinstance(payload, dict) else None
            if code == -1021 and attempt == 0:
                self.sync_time()
                continue
            if status != 200 or (isinstance(code, int) and code < 0):
                raise HistoryError(f"币安请求失败：{path}，HTTP {status}，错误码 {code}")
            return payload
        raise HistoryError("币安请求重试后仍失败")


class OKXClient(HttpClient):
    def __init__(self, timeout=15, retries=2):
        super().__init__(timeout, retries)
        self.key = os.getenv("OKX_API_KEY", "")
        self.secret = os.getenv("OKX_API_SECRET") or os.getenv("OKX_SECRET_KEY", "")
        self.passphrase = os.getenv("OKX_PASSPHRASE") or os.getenv("OKX_API_PASSPHRASE", "")
        if not all((self.key, self.secret, self.passphrase)):
            raise HistoryError("请配置 OKX_API_KEY、OKX_API_SECRET 和 OKX_PASSPHRASE")
        self.base = os.getenv("OKX_BASE_URL", "https://openapi.okx.com").rstrip("/")
        self.demo = os.getenv("OKX_DEMO", "0") == "1"
        self.offset = 0

    def request(self, path, params=None, signed=True):
        query = urlencode({k: v for k, v in (params or {}).items() if v is not None})
        path += "?" + query if query else ""
        for attempt in range(self.retries + 1):
            headers = {}
            if signed:
                stamp = datetime.fromtimestamp(time.time() + self.offset / 1000, timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
                sign = base64.b64encode(hmac.new(self.secret.encode(), (stamp + "GET" + path).encode(), hashlib.sha256).digest()).decode()
                headers = {"OK-ACCESS-KEY": self.key, "OK-ACCESS-SIGN": sign,
                           "OK-ACCESS-TIMESTAMP": stamp, "OK-ACCESS-PASSPHRASE": self.passphrase}
                if self.demo:
                    headers["x-simulated-trading"] = "1"
            time.sleep(0.22)
            status, payload = super().get(self.base + path, headers=headers)
            code = str(payload.get("code")) if isinstance(payload, dict) else ""
            if signed and code == "50102" and attempt < self.retries:
                began = int(time.time() * 1000)
                server = self.request("/api/v5/public/time", signed=False)
                self.offset = int(server[0]["ts"]) - (began + int(time.time() * 1000)) // 2
                continue
            if code in ("50011", "50040") and attempt < self.retries:
                time.sleep(2 ** attempt)
                continue
            if status != 200 or code != "0":
                raise HistoryError(f"OKX 请求失败：HTTP {status}，错误码 {code}")
            return checked_rows(payload, "data")
        raise HistoryError("OKX 请求重试后仍失败")


def windows(start, end):
    while start < end:
        stop = min(start + 7 * DAY - 1, end)
        yield start, stop - 1
        start = stop


def bounded_query(client, path, start, end, params=None):
    """满页二分时间窗口，避免遗漏同毫秒的成交。"""
    def fetch(left, right):
        batch = checked_rows(client.request(path, {**(params or {}), "startTime": left, "endTime": right, "limit": 1000}))
        if len(batch) < 1000:
            return batch
        if left == right:
            raise HistoryError("单毫秒记录达到接口上限，无法确认完整性")
        middle = (left + right) // 2
        return fetch(left, middle) + fetch(middle + 1, right)
    result = []
    for left, right in windows(start, end):
        result.extend(fetch(left, right))
    return result


def discover_symbols(client, start, end, events):
    # 全市场订单发现零手续费交易对；收入流水覆盖强平/旧挂单的新成交。
    symbols = {r["symbol"] for r in bounded_query(client, "/fapi/v1/allOrders", start, end)}
    for left, right in windows(start, end):
        seen = set()
        for page in range(1, 10001):
            batch = checked_rows(client.request("/fapi/v1/income", {
                "startTime": left, "endTime": right, "page": page, "limit": 1000,
            }))
            keys = {(r["incomeType"], str(r["tranId"])) for r in batch}
            symbols.update(r["symbol"] for r in batch if r.get("symbol"))
            if len(batch) < 1000:
                break
            if not keys - seen:
                raise HistoryError("币安流水分页没有前进")
            seen.update(keys)
        else:
            raise HistoryError("币安流水分页超过上限")
    positions = checked_rows(client.request("/fapi/v3/positionRisk"))
    symbols.update(r["symbol"] for r in positions)
    symbols.update(e["symbol"] for e in events if e.get("source") == "binance")
    discovered = set(symbols)
    # 已有 CSV 不含交易所，启动基线的普通 U 本位交易对也可作为候选。
    symbols.update(e["pair"].replace("/", "") for e in events
                   if e.get("source") == "baseline" and e["pair"].endswith(("/USDT", "/USDC")))
    # 过滤仅在 OKX 上存在的基线交易对，但保留实际发现的币安历史/退市代码。
    actual = {r["symbol"] for r in client.request("/fapi/v1/exchangeInfo", signed=False)["symbols"]}
    return sorted(discovered | (symbols & actual))


class Converter:
    def __init__(self, http):
        self.http, self.cache = http, {}

    def rate(self, asset, ts):
        if asset == "USDT":
            return Decimal(1)
        minute = ts // 60000 * 60000
        key = (asset, minute)
        if key not in self.cache:
            status, data = self.http.get("https://api.binance.com/api/v3/klines", params={
                "symbol": asset + "USDT", "interval": "1m", "startTime": minute,
                "endTime": minute + 59999, "limit": 1,
            })
            if status != 200 or not isinstance(data, list) or not data or int(data[0][0]) != minute:
                raise HistoryError(f"缺少 {asset}/USDT 成交时的历史汇率")
            self.cache[key] = decimal(data[0][1])
        return self.cache[key]


def event(source, uid, symbol, op, ts, price, qty, amount, pnl):
    if decimal(price) <= 0 or decimal(qty) <= 0 or decimal(amount) < 0:
        raise HistoryError("成交价格、数量或金额无效")
    return {"id": f"{source}:{uid}", "source": source, "symbol": symbol,
            "pair": pair(symbol), "op": op, "time": int(ts),
            **{k: str(decimal(v)) for k, v in zip(NUMBERS, (price, qty, amount, pnl))}}


def fetch_binance(client, start, end, events, converter):
    symbols = discover_symbols(client, start, end, events)
    LOG.info("币安查询 %d 个候选合约", len(symbols))
    result = []
    operations = {("BUY", "LONG"): "开多", ("SELL", "LONG"): "平多",
                  ("SELL", "SHORT"): "开空", ("BUY", "SHORT"): "平空"}
    for symbol in symbols:
        for r in bounded_query(client, "/fapi/v1/userTrades", start, end, {"symbol": symbol}):
            ts = int(r["time"])
            if not start <= ts < end:
                continue
            quote = pair(symbol).split("/")[1].split()[0]
            op = operations.get((r["side"], r["positionSide"]), {"BUY": "买入", "SELL": "卖出"}[r["side"]])
            price, qty = decimal(r["price"]), decimal(r["qty"])
            amount = decimal(r.get("quoteQty") or price * qty)
            result.append(event("binance", f"{symbol}:{r['id']}", symbol, op, ts, price, qty,
                                amount * converter.rate(quote, ts),
                                decimal(r["realizedPnl"]) * converter.rate(r.get("marginAsset") or quote, ts)))
    return result


def fetch_okx(client, start, end, converter):
    found, specs = {}, {}
    operations = {"1": "买入", "2": "卖出", "3": "开多", "4": "开空", "5": "平多", "6": "平空",
                  "100": "强平多", "101": "强平空", "102": "强平买入", "103": "强平卖出",
                  "104": "强平多", "105": "强平空", "106": "强平买入", "107": "强平卖出",
                  "112": "交割平多", "113": "交割平空", "125": "ADL平多", "126": "ADL平空",
                  "127": "ADL买入", "128": "ADL卖出"}
    for code, op in zip(("204", "205", "206", "207", "208", "209", "270", "271", "272", "273", "274", "275", "324", "325", "326", "327", "328", "329"),
                        ("买入", "卖出", "开多", "开空", "平多", "平空") * 3):
        operations[code] = op
    for kind in ("SWAP", "FUTURES"):
        cursor = None
        for _ in range(10000):
            batch = client.request("/api/v5/trade/fills-history", {
                "instType": kind, "begin": start, "end": end - 1, "after": cursor, "limit": 100,
            })
            if not batch:
                break
            for r in batch:
                ts = int(r.get("fillTime") or r["ts"])
                if not start <= ts < end:
                    continue
                symbol = r["instId"]
                op = operations.get(str(r["subType"]))
                if op is None:
                    raise HistoryError(f"未知 OKX 成交类型 {r['subType']}")
                if symbol not in specs:
                    data = client.request("/api/v5/public/instruments", {"instType": kind, "instId": symbol}, signed=False)
                    if not data:
                        raise HistoryError(f"无法取得 {symbol} 合约面值")
                    specs[symbol] = data[0]
                spec = specs[symbol]
                base, quote = symbol.split("-")[:2]
                price, size = decimal(r["fillPx"]), decimal(r["fillSz"])
                face = decimal(spec["ctVal"]) * decimal(spec.get("ctMult") or "1")
                if spec["ctValCcy"] == base:
                    qty, amount = size * face, size * face * price
                elif spec["ctValCcy"] == quote:
                    qty, amount = size * face / price, size * face
                else:
                    raise HistoryError(f"无法换算 {symbol} 的合约面值")
                pnl_ccy = spec.get("settleCcy") or quote
                item = event("okx", r["billId"], symbol, op, ts, price, qty,
                             amount * converter.rate(quote, ts), decimal(r["fillPnl"]) * converter.rate(pnl_ccy, ts))
                found[item["id"]] = item
            following = str(min(int(r["billId"]) for r in batch))
            if cursor and int(following) >= int(cursor):
                raise HistoryError("OKX 分页没有前进")
            cursor = following
        else:
            raise HistoryError("OKX 分页超过上限")
    return list(found.values())


def merge_events(events, seconds=60, same_price=True):
    """同交易对/操作：相邻成交间隔 <= 一分钟，或原始价格相同，传递合并。

    同价不限制时间；价格比较使用未舍入 Decimal。只在最终输出时舍入。
    """
    by_pair = {}
    for item in events:
        by_pair.setdefault((item["pair"], item["op"]), []).append(item)
    merged = []
    with localcontext() as context:
        context.prec = 50
        for (symbol, operation), batch in by_pair.items():
            batch.sort(key=lambda r: (r["time"], r["id"]))
            parents = list(range(len(batch)))

            def root(i):
                while parents[i] != i:
                    parents[i] = parents[parents[i]]
                    i = parents[i]
                return i

            def union(a, b):
                a, b = root(a), root(b)
                if a != b:
                    parents[max(a, b)] = min(a, b)

            prices = {}
            for i, item in enumerate(batch):
                price = decimal(item["price"])
                if i and item["time"] - batch[i - 1]["time"] <= seconds * 1000:
                    union(i - 1, i)
                if same_price and price in prices:
                    union(prices[price], i)
                prices[price] = i
            groups = {}
            for i, item in enumerate(batch):
                group = groups.setdefault(root(i), {"pair": symbol, "op": operation,
                    "time": item["time"], "qty": Decimal(0), "weighted": Decimal(0),
                    "amount": Decimal(0), "pnl": Decimal(0)})
                price, qty, amount, pnl = (decimal(item[k]) for k in NUMBERS)
                if qty <= 0:
                    raise HistoryError("原始成交数量必须大于零")
                group["time"] = min(group["time"], item["time"])
                group["qty"] += qty
                group["weighted"] += price * qty
                group["amount"] += amount
                group["pnl"] += pnl
            merged.extend(groups.values())
        return [dict(zip(FIELDS, [g["pair"], g["op"], local_time(g["time"]),
                     display(g["weighted"] / g["qty"]), display(g["qty"]),
                     display(g["amount"]), display(g["pnl"])]))
                for g in sorted(merged, key=lambda r: (r["time"], r["pair"], r["op"]))]


def csv_content(records):
    stream = io.StringIO(newline="")
    writer = csv.DictWriter(stream, fieldnames=FIELDS)
    writer.writeheader()
    writer.writerows(records)
    return stream.getvalue().encode("utf-8-sig")


def read_csv(content):
    reader = csv.DictReader(io.StringIO(content.decode("utf-8-sig")))
    if reader.fieldnames != FIELDS:
        raise HistoryError("CSV 字段与预期不一致")
    return list(reader)


def atomic_write(path, content):
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="wb", dir=path.parent, prefix=f".{path.name}.", suffix=".tmp", delete=False) as f:
            temporary = Path(f.name)
            f.write(content)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temporary, path)
    finally:
        if temporary:
            temporary.unlink(missing_ok=True)


def latest_date(records):
    if not records:
        raise HistoryError("CSV 没有历史记录，无法确定增量查询起点")
    last = max(timestamp(r["成交时间"]) for r in records)
    day = datetime.fromtimestamp(last / 1000, LOCAL).replace(hour=0, minute=0, second=0, microsecond=0)
    return int(day.timestamp() * 1000)


def refresh_tail(existing, incoming, start, end, seconds=60):
    """最后一天是重叠查询窗口，用新结果替换，不能直接重复追加。"""
    unique = {}
    for item in incoming:
        if not start <= item["time"] < end:
            continue
        if item["id"] in unique and unique[item["id"]] != item:
            raise HistoryError("接口返回相同成交 ID 的不同内容")
        unique[item["id"]] = item
    old_tail = [r for r in existing if timestamp(r["成交时间"]) >= start]
    if old_tail and not unique:
        raise HistoryError("查询未返回 CSV 最后一天已有的成交，已停止写入以保留原数据")
    preserved = [r.copy() for r in existing if timestamp(r["成交时间"]) < start]
    days = {}
    for item in unique.values():
        day = datetime.fromtimestamp(item["time"] / 1000, LOCAL).date()
        days.setdefault(day, []).append(item)
    rebuilt = [row for day in sorted(days) for row in merge_events(days[day], seconds)]
    # 不跨日期合并，确保下一次从 CSV 最新日期回查时不会重复计算前一天的数量。
    rows = preserved + rebuilt
    for row in rows:
        row["成交时间"] = local_time(timestamp(row["成交时间"]))
    rows.sort(key=lambda r: (timestamp(r["成交时间"]), r["交易对"], r["操作"]))
    return rows, len(unique)


def save_csv(path, original, content):
    if path.read_bytes() != original:
        raise HistoryError("查询期间 CSV 被其他程序修改，本次取消写入，请重试")
    if content != original:
        atomic_write(path, content)


def history_floor(now):
    dt = datetime.fromtimestamp(now / 1000, timezone.utc)
    year, month = dt.year, dt.month - 3
    if month <= 0:
        year, month = year - 1, month + 12
    return int(dt.replace(year=year, month=month, day=min(dt.day, calendar.monthrange(year, month)[1])).timestamp() * 1000) + 3600000


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--csv", type=Path, default=DEFAULT_FILE)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--merge-seconds", type=int, default=60)
    parser.add_argument("--timeout", type=float, default=15)
    parser.add_argument("--retries", type=int, default=2)
    args = parser.parse_args(argv)
    if not (0 <= args.merge_seconds <= 300 and 0 < args.timeout <= 120 and 0 <= args.retries <= 5):
        parser.error("合并间隔或请求设置超出有效范围")
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    load_dotenv(ROOT / ".env")
    csv_path = args.csv.resolve()
    clients = []
    try:
        with localcontext() as context:
            context.prec = 50
            original = csv_path.read_bytes()
            existing = read_csv(original)
            start = latest_date(existing)
            end = int(time.time() * 1000)
            if start >= end or any(timestamp(r["成交时间"]) >= end for r in existing):
                raise HistoryError("CSV 包含未来时间，无法确定查询范围")
            if start < history_floor(end):
                raise HistoryError("CSV 最新日期超出接口三个月保留期，请先补齐历史数据")
            LOG.info("从 CSV 最新日期查询：北京时间 %s 至 %s", local_time(start), local_time(end))
            binance = BinanceClient(args.timeout, args.retries)
            clients.append(binance)
            okx = OKXClient(args.timeout, args.retries)
            clients.append(okx)
            http = HttpClient(args.timeout, args.retries)
            clients.append(http)
            converter = Converter(http)
            # 旧 CSV 只用于发现候选合约，不从两位小数数据推算原始成交。
            hints = [{"source": "baseline", "pair": pair(r["交易对"])} for r in existing]
            incoming = fetch_binance(binance, start, end, hints, converter)
            incoming.extend(fetch_okx(okx, start, end, converter))
            records, fetched = refresh_tail(existing, incoming, start, end, args.merge_seconds)
            content = csv_content(records)
            if not args.dry_run:
                save_csv(csv_path, original, content)
            total = sum((decimal(r["已实现盈亏（USDT）"]) for r in records), Decimal(0))
            LOG.info("区间内查询 %d 笔成交，CSV 共 %d 行；按 CSV 汇总已实现盈亏 %s USDT；%s",
                     fetched, len(records), display(total), "未写入文件" if args.dry_run else f"已写入 {csv_path}")
        return 0
    except (HistoryError, OSError, ValueError, KeyError, TypeError) as exc:
        LOG.error("同步终止：%s", str(exc) if isinstance(exc, HistoryError) else type(exc).__name__)
        return 1
    finally:
        for client in clients:
            client.close()


if __name__ == "__main__":
    raise SystemExit(main())
