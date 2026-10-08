"""持仓和历史成交共用的代码、计价口径。"""
from decimal import Decimal

# OKX XIAOMI 美元报价 -> HK1810 港股价格口径；可手动调整。
XIAOMI_OKX_RATE = Decimal("7.85")

HK_SYMBOL_ALIASES = {"XIAOMI": "HK1810", "TENCENT": "HK0700", "LENOVO": "HK0992"}


def canonical_symbol(symbol):
    symbol = symbol.upper()
    return HK_SYMBOL_ALIASES.get(symbol, symbol)


def okx_price_factor(symbol):
    return XIAOMI_OKX_RATE if symbol.upper() == "XIAOMI" else Decimal(1)
