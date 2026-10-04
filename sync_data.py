#!/usr/bin/env python3
"""合并币安与 OKX，按 symbol 和 direction 更新 data.json 已有持仓。

python sync_data.py              # 获取并写入
python sync_data.py --dry-run    # 获取并校验，不写入

读取同目录 .env 或环境变量中的币安及 OKX 配置，仅发起 GET 请求。
现货与理财跨平台合并，过滤 USDT/USDC；除 BTC/ETH/BNB 外不足 0.1 不展示。
合约按币种和方向汇总敞口和数量加权成本；HK1810 映射到 XIAOMI。
现货不更新成本，也不参与合约成本加权；敞口沿用现有估值逻辑。
股票代码B / x股票代码现货按自身行情估值，并入对应股票多头，成本仅取合约。
映射不区分大小写，优先匹配完整代码；仅映射至已有持仓，现货合计后过滤小额。
数量保留八位小数，敞口和合约成本保留两位小数。
不新增持仓、字段或账户分组；其他字段和记录顺序保持不变。
没有匹配仓位的记录保留原值；BTC/ETH/BNB 无仓位时数量和敞口归零。
敞口使用舍入前数量估值为 USDT，并按原页面约定保留正的绝对值。
"""

from __future__ import annotations

import argparse
import base64
import copy
import hashlib
import hmac
import logging
import os
import time
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from urllib.parse import urlencode

import requests
import simplejson as json
from dotenv import load_dotenv

from update_data import atomic_write

ROOT = Path(__file__).resolve().parent
SPOT = "https://api.binance.com"
FUTURES = "https://fapi.binance.com"
ZERO = Decimal("0")
CENT = Decimal("0.01")
LOG = logging.getLogger("assets.sync")
SPOT_EXCEPTIONS = {"BTC", "ETH", "BNB"}
IGNORED_SPOT = {"USDT", "USDC"}


class SyncError(ValueError):
    pass


def decimal(value):
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise SyncError("接口或文件包含无效数值") from exc
    if not result.is_finite():
        raise SyncError("接口或文件包含非有限数值")
    return result


def rounded(value):
    result = decimal(value).quantize(CENT, rounding=ROUND_HALF_UP)
    return abs(result) if result == 0 else result


def rows(payload, key=None):
    value = payload[key] if key else payload
    if not isinstance(value, list) or not all(isinstance(r, dict) for r in value):
        raise SyncError("接口返回的列表格式错误")
    return value


class BinanceClient:
    def __init__(self, api_key, api_secret, timeout=15):
        self.secret = api_secret.encode()
        self.session = requests.Session()
        self.session.headers["X-MBX-APIKEY"] = api_key
        self.offsets = {}
        self.timeout = timeout

    def sync_time(self, base):
        start = int(time.time() * 1000)
        path = "/api/v3/time" if base == SPOT else "/fapi/v1/time"
        payload = self.get(base, path)
        self.offsets[base] = int(payload["serverTime"]) - (start + int(time.time() * 1000)) // 2

    def get(self, base, path, params=None, *, signed=False):
        for attempt in range(3):
            query = dict(params or {})
            if signed:
                if base not in self.offsets:
                    self.sync_time(base)
                query.update(timestamp=int(time.time() * 1000) + self.offsets[base], recvWindow=5000)
                query["signature"] = hmac.new(self.secret, urlencode(query).encode(), hashlib.sha256).hexdigest()
            try:
                response = self.session.get(base + path, params=query, timeout=self.timeout)
            except requests.RequestException:
                # requests 的异常可能包含签名 URL，禁止直接输出。
                if attempt < 2:
                    time.sleep(attempt + 1)
                    continue
                raise SyncError(f"{path} 网络连接失败或超时") from None
            try:
                payload = response.json()
            except ValueError:
                raise SyncError(f"{path} 响应不是 JSON（HTTP {response.status_code}）") from None
            code = payload.get("code") if isinstance(payload, dict) else None
            if signed and code == -1021 and attempt < 2:
                self.sync_time(base)
                continue
            if response.status_code >= 500 and attempt < 2:
                time.sleep(attempt + 1)
                continue
            if response.status_code != 200 or (isinstance(code, int) and code < 0):
                raise SyncError(f"{path} 请求失败：HTTP {response.status_code}，币安错误码 {code}")
            return payload
        raise SyncError(f"{path} 重试后仍失败")

    def earn_positions(self, kind):
        path = f"/sapi/v1/simple-earn/{kind}/position"
        result = []
        for page in range(1, 10001):
            payload = self.get(SPOT, path, {"current": page, "size": 100}, signed=True)
            batch = rows(payload, "rows")
            result.extend(batch)
            total = int(payload["total"])
            if len(result) >= total:
                return result
            if not batch:
                raise SyncError(f"{kind} 理财分页提前结束")
        raise SyncError(f"{kind} 理财分页超过上限")


class OKXClient:
    """OKX 只读接口；配置在 load_dotenv 后读取，不依赖外部查询脚本。"""

    def __init__(self):
        self.key = os.getenv("OKX_API_KEY", "")
        self.secret = os.getenv("OKX_API_SECRET") or os.getenv("OKX_SECRET_KEY", "")
        self.passphrase = os.getenv("OKX_PASSPHRASE") or os.getenv("OKX_API_PASSPHRASE", "")
        missing = [name for name, value in (
            ("OKX_API_KEY", self.key), ("OKX_API_SECRET", self.secret),
            ("OKX_PASSPHRASE", self.passphrase),
        ) if not value]
        if missing:
            raise SyncError("请在 .env 或环境变量中补充：" + ", ".join(missing))
        self.base = os.getenv("OKX_BASE_URL", "https://openapi.okx.com").rstrip("/")
        self.demo = os.getenv("OKX_DEMO", "0") == "1"
        self.session = requests.Session()
        self.offset = 0

    def get(self, path, headers=None):
        try:
            response = self.session.get(self.base + path, headers=headers, timeout=15)
        except requests.RequestException:
            raise SyncError("OKX 网络连接失败或超时") from None
        try:
            payload = response.json()
        except ValueError:
            raise SyncError(f"OKX 返回非 JSON 响应 (HTTP {response.status_code})") from None
        if not isinstance(payload, dict):
            raise SyncError("OKX 响应格式异常")
        return response, payload

    @staticmethod
    def checked_rows(response, payload):
        if not response.ok or str(payload.get("code")) != "0":
            raise SyncError(f"OKX HTTP {response.status_code}, code={payload.get('code')}")
        return rows(payload, "data")

    def sync_server_time(self):
        started = time.time() * 1000
        response, payload = self.get("/api/v5/public/time")
        finished = time.time() * 1000
        result = self.checked_rows(response, payload)
        if not result or not result[0].get("ts"):
            raise SyncError("OKX 未返回服务器时间")
        self.offset = int(int(result[0]["ts"]) - (started + finished) / 2)

    def signed_request(self, path, params=None):
        query = urlencode({k: v for k, v in (params or {}).items() if v is not None})
        request_path = path + (f"?{query}" if query else "")
        for attempt in range(2):
            now = datetime.fromtimestamp(time.time() + self.offset / 1000, timezone.utc)
            timestamp = now.isoformat(timespec="milliseconds").replace("+00:00", "Z")
            message = timestamp + "GET" + request_path
            digest = hmac.new(self.secret.encode(), message.encode(), hashlib.sha256).digest()
            headers = {
                "OK-ACCESS-KEY": self.key,
                "OK-ACCESS-SIGN": base64.b64encode(digest).decode(),
                "OK-ACCESS-TIMESTAMP": timestamp,
                "OK-ACCESS-PASSPHRASE": self.passphrase,
            }
            if self.demo:
                headers["x-simulated-trading"] = "1"
            response, payload = self.get(request_path, headers)
            if str(payload.get("code")) == "50102" and attempt == 0:
                self.sync_server_time()
                continue
            return self.checked_rows(response, payload)

    def get_futures_positions(self):
        return [row for row in self.signed_request("/api/v5/account/positions")
                if row.get("instType") in ("SWAP", "FUTURES") and decimal(row.get("pos") or "0") != 0]

    def get_combined_spot_positions(self):
        trading = self.signed_request("/api/v5/account/balance")
        if not trading:
            raise SyncError("OKX 未返回交易账户余额")
        funding = self.signed_request("/api/v5/asset/balances")
        savings = self.signed_request("/api/v5/finance/savings/balance")
        combined = defaultdict(lambda: ZERO)
        # cashBal 不含合约浮盈；不重复累加冻结额或已出借额。
        for balances, field in ((rows(trading[0], "details"), "cashBal"),
                                (funding, "bal"), (savings, "amt")):
            for row in balances:
                combined[row["ccy"]] += decimal(row.get(field) or "0")
        return {ccy: amount for ccy, amount in combined.items() if amount != 0}


def fetch_snapshot(client):
    spot = client.get(SPOT, "/api/v3/account", signed=True)
    combined = defaultdict(lambda: ZERO)
    for row in rows(spot, "balances"):
        asset = row["asset"]
        if asset.startswith("LD"):
            continue
        free, locked = decimal(row["free"]), decimal(row["locked"])
        if free < 0 or locked < 0:
            raise SyncError("现货余额为负，无法按现货账户处理")
        combined[asset] += free + locked
    for kind, field in (("flexible", "totalAmount"), ("locked", "amount")):
        for row in client.earn_positions(kind):
            asset = row["asset"]
            if not asset.startswith("LD"):
                amount = decimal(row[field])
                if amount < 0:
                    raise SyncError("理财余额为负")
                combined[asset] += amount
    positions = rows(client.get(FUTURES, "/fapi/v3/positionRisk", signed=True))
    tickers = rows(client.get(SPOT, "/api/v3/ticker/price"))
    prices = {row["symbol"]: decimal(row["price"]) for row in tickers}

    def price(asset):
        if asset == "USDT":
            return Decimal("1")
        direct = prices.get(asset + "USDT", ZERO)
        if direct > 0:
            return direct
        inverse = prices.get("USDT" + asset, ZERO)
        if inverse > 0:
            return 1 / inverse
        for bridge in ("BTC", "ETH", "BNB"):
            pair, rate = prices.get(asset + bridge, ZERO), prices.get(bridge + "USDT", ZERO)
            if pair > 0 and rate > 0:
                return pair * rate
        raise SyncError(f"{asset} 缺少可用的 USDT 估值价格，取消写入")

    return combined, positions, price


SYMBOL_ALIASES = {"HK1810": "XIAOMI"}


def spot_symbol(asset, targets):
    """仅对现货解析股票代币；完整代码优先，避免误拆 BRKB、BNB 等。"""
    symbol = asset.upper()
    symbol = SYMBOL_ALIASES.get(symbol, symbol)
    if any((symbol, side) in targets for side in ("long", "short")):
        return symbol
    candidates = []
    if symbol.endswith("B"):
        candidates.append(symbol[:-1])
    if symbol.startswith("X"):
        candidates.append(symbol[1:])
    for candidate in candidates:
        candidate = SYMBOL_ALIASES.get(candidate, candidate)
        if candidate not in SPOT_EXCEPTIONS | IGNORED_SPOT and (candidate, "long") in targets:
            return candidate
    return symbol


def fetch_okx_snapshot(client):
    """获取 OKX 合约及交易、资金、活期余币宝三个账户余额。"""
    try:
        client.sync_server_time()
        combined = client.get_combined_spot_positions()
        positions = client.get_futures_positions()
        instruments = {}
        for kind in sorted({row["instType"] for row in positions}):
            for row in client.signed_request("/api/v5/public/instruments", {"instType": kind}):
                instruments[row["instId"]] = row
        tickers = client.signed_request("/api/v5/market/tickers", {"instType": "SPOT"})
        prices = {row["instId"]: decimal(row["last"]) for row in tickers if row.get("last")}
    except requests.RequestException:
        raise SyncError("OKX 网络连接失败或超时") from None
    except RuntimeError:
        raise SyncError("OKX 查询失败，请检查 API 配置、权限及服务状态") from None

    def price(asset):
        if asset in {"USD", "USDT"}:
            return Decimal("1")
        value = prices.get(asset + "-USDT", ZERO)
        if value <= 0:
            raise SyncError(f"OKX {asset} 缺少可用的 USDT 估值价格")
        return value

    normalized = []
    for row in positions:
        instrument = instruments[row["instId"]]
        symbol = row["instId"].split("-")[0]
        pos = decimal(row["pos"])
        side = row["posSide"]
        if side == "net":
            side = "short" if pos < 0 else "long"
        if side not in {"long", "short"}:
            raise SyncError(f"OKX {row['instId']} 持仓方向无效")
        face = abs(pos) * decimal(instrument["ctVal"]) * decimal(instrument.get("ctMult") or "1")
        entry, mark = decimal(row["avgPx"]), decimal(row["markPx"])
        if face <= 0 or entry <= 0 or mark <= 0:
            raise SyncError(f"OKX {row['instId']} 合约面值或价格无效")
        if instrument["ctValCcy"] == symbol:
            quantity = cost_quantity = face
            quote = row["instId"].split("-")[1]
        elif instrument["ctValCcy"] in {"USD", "USDT", "USDC"}:
            # 币本位：当前标的敞口与开仓时标的数量不同。
            quantity, cost_quantity = face / mark, face / entry
            quote = instrument["ctValCcy"]
        else:
            raise SyncError(f"OKX {row['instId']} 合约面值币种无法识别")
        normalized.append({"symbol": symbol, "direction": side, "quantity": quantity,
                           "exposure": quantity * mark * price(quote),
                           "cost_quantity": cost_quantity, "cost": entry * price(quote)})
    return combined, normalized, price


def update_document(data, snapshot, okx_snapshot=None):
    updated = copy.deepcopy(data)
    if not isinstance(updated.get("holdings"), list):
        raise SyncError("data.json 缺少 holdings 数组")
    binance_spot, positions, price_for = snapshot
    combined = defaultdict(lambda: ZERO, binance_spot)
    okx_positions = []
    okx_price = None
    if okx_snapshot is not None:
        okx_spot, okx_positions, okx_price = okx_snapshot
        for asset, amount in okx_spot.items():
            combined[asset] += decimal(amount)
    targets = {}
    for item in updated["holdings"]:
        key = (item["symbol"].upper(), item["direction"])
        if key[1] not in {"long", "short"}:
            raise SyncError(f"{key[0]} 的 direction 无效")
        if key in targets:
            raise SyncError(f"重复持仓 {key}，请先合并已有记录")
        targets[key] = item
    totals = defaultdict(lambda: [ZERO, ZERO])
    costs = defaultdict(lambda: [ZERO, ZERO])
    spot_keys = set()
    spot_quantities = defaultdict(lambda: ZERO)
    for asset, amount in combined.items():
        if asset.upper() not in IGNORED_SPOT:
            spot_quantities[spot_symbol(asset, targets)] += amount
    for asset, amount in combined.items():
        symbol = spot_symbol(asset, targets)
        key = (symbol, "long")
        spot_keys.add(key)
        if asset.upper() in IGNORED_SPOT or amount <= 0 or (symbol not in SPOT_EXCEPTIONS and spot_quantities[symbol] < Decimal("0.1")):
            continue
        if key in targets:
            try:
                rate = price_for(asset)
            except SyncError:
                if okx_price is None:
                    raise
                rate = okx_price(asset)
            totals[key][0] += amount
            totals[key][1] += amount * rate
    for row in positions:
        amount = decimal(row["positionAmt"])
        if amount == 0:
            continue
        contract = row["symbol"].split("_")[0]
        quote = next((q for q in ("USDT", "USDC") if contract.endswith(q)), None)
        if quote is None:
            raise SyncError(f"无法识别合约计价币种：{row['symbol']}")
        symbol = contract[:-len(quote)]
        key = (SYMBOL_ALIASES.get(symbol, symbol), "short" if amount < 0 else "long")
        if key in targets:
            rate = price_for(quote)
            totals[key][0] += abs(amount)
            totals[key][1] += abs(decimal(row["notional"])) * rate
            costs[key][0] += abs(amount)
            costs[key][1] += abs(amount) * decimal(row["entryPrice"]) * rate
    for row in okx_positions:
        key = (SYMBOL_ALIASES.get(row["symbol"], row["symbol"]), row["direction"])
        if key in targets:
            totals[key][0] += row["quantity"]
            totals[key][1] += row["exposure"]
            costs[key][0] += row["cost_quantity"]
            costs[key][1] += row["cost_quantity"] * row["cost"]
    count = 0
    for key, item in targets.items():
        if key not in totals and key not in spot_keys and key[0] not in SPOT_EXCEPTIONS | IGNORED_SPOT:
            continue
        quantity, exposure = totals[key]
        item["quantity"] = quantity.quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP)
        item["exposure"] = rounded(exposure)
        cost_quantity, entry_value = costs[key]
        if cost_quantity > 0:
            item["cost"] = rounded(entry_value / cost_quantity)
        count += 1
    LOG.info("匹配并更新 %d 条已有持仓；合约成本按数量加权，现货成本保留原值", count)
    return updated


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--data", type=Path, default=ROOT / "data.json")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    client = None
    okx_client = None
    try:
        load_dotenv(ROOT / ".env")
        key, secret = os.getenv("BINANCE_API_KEY"), os.getenv("BINANCE_API_SECRET")
        if not key or not secret:
            raise SyncError("请在 .env 中设置 BINANCE_API_KEY 和 BINANCE_API_SECRET")
        path = args.data.resolve()
        original = path.read_bytes()
        data = json.loads(original.decode("utf-8-sig"), use_decimal=True)
        client = BinanceClient(key, secret)
        okx_client = OKXClient()
        updated = update_document(data, fetch_snapshot(client), fetch_okx_snapshot(okx_client))
        content = (json.dumps(updated, ensure_ascii=False, indent=2, use_decimal=True, allow_nan=False) + "\n").encode("utf-8")
        if args.dry_run:
            LOG.info("币安与 OKX 数据获取成功，预览模式未写入文件")
        else:
            if path.read_bytes() != original:
                raise SyncError("获取期间 data.json 已被修改，取消写入，请重试")
            atomic_write(path, content)
            LOG.info("同步成功：%s", path.name)
        return 0
    except (OSError, ValueError, KeyError, TypeError, InvalidOperation, RuntimeError) as exc:
        LOG.error("同步失败，未更新文件：%s", exc)
        return 1
    finally:
        if client is not None:
            client.session.close()
        if okx_client is not None:
            okx_client.session.close()


if __name__ == "__main__":
    raise SystemExit(main())
