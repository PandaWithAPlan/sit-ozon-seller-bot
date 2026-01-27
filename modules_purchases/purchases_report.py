# modules_purchases/purchases_report.py
from __future__ import annotations
from modules_purchases.purchases_report_data import load_excel  # «У Seller» (Excel)

import os
import math
import datetime as dt
from typing import Dict, List, Tuple, Any

from dotenv import load_dotenv

# ── .env / базовые пути ──────────────────────────────────────────────────────
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
load_dotenv(os.path.join(ROOT_DIR, ".env"))

"""
Изменения:
1) Порядок юнитов теперь строго по WATCH_SKU (порядок в .env сохраняется).
2) Имена берём из ALIAS (через get_alias_for_sku).
3) В отчёт попадают только те SKU, что перечислены в WATCH_SKU
    (если WATCH_SKU непустой). Прочие юниты отфильтрованы.
4) Переменная WATCH_OFFERS в этом модуле не используется.
    Она нужна, когда отчёты работают в режиме по офферам (PRODUCTS_MODE=OFFER),
    чтобы фильтровать/упорядочивать по offer_id. Здесь PRODUCTS_MODE=SKU,
    поэтому сортировка/фильтрация выполняется только по WATCH_SKU.
"""

# ── источники данных ─────────────────────────────────────────────────────────
# ⬇️ ВАЖНО: используем парсер из purchases_report_data (а не из purchases_data)

# ── алиасы/форматирование ───────────────────────────────────────────────────
try:
    from modules_sales.sales_facts_store import get_alias_for_sku, _fmt_units  # type: ignore
except Exception:

    def get_alias_for_sku(sku: int) -> str | None:  # type: ignore
        return str(sku)

    def _fmt_units(v: float) -> str:  # type: ignore
        return f"{int(round(v or 0))} шт"


ALERT_PLAN_HORIZON_DAYS = int(os.getenv("ALERT_PLAN_HORIZON_DAYS", "30"))


def _now_stamp() -> str:
    return dt.datetime.now().strftime("%d.%m.%Y %H:%M")


def _alias_pad(alias: str, width: int = 26) -> str:
    alias = alias or ""
    return alias if len(alias) >= width else alias + " " * (width - len(alias))


# ── WATCH_SKU: порядок и фильтр допускаемых SKU ──────────────────────────────


def _watch_sku_order_list() -> List[int]:
    """
    Возвращает список SKU из WATCH_SKU в исходном порядке (без дублей).
    Поддерживает записи вида «123» или «123:alias».
    """
    raw = (os.getenv("WATCH_SKU", "") or "").replace("\n", ",").replace(" ", ",")
    out: List[int] = []
    seen: set[int] = set()
    for tok in raw.split(","):
        tok = tok.strip()
        if not tok:
            continue
        left = tok.split(":", 1)[0].strip()
        try:
            s = int(left)
        except Exception:
            continue
        if s not in seen:
            seen.add(s)
            out.append(s)
    return out


_WATCH_POS = {sku: i for i, sku in enumerate(_watch_sku_order_list())}
_WATCH_SET = set(_WATCH_POS.keys())


def _order_key_for_sku(sku: int) -> Tuple[int, str]:
    """
    Сортировка:
        1) позиция в WATCH_SKU (меньше — раньше),
        2) при равенстве — по alias (a..z).
    """
    pos = _WATCH_POS.get(int(sku), 10**9)
    alias = (get_alias_for_sku(sku) or str(sku)).strip()
    return (pos, alias.lower())


# ════════════════════════════════════════════════════════════════════════════
#   🚚 ВЫКУПЫ — СТАТУС ЗАКАЗОВ (КОМПАКТНО, ПО СТРОКАМ)
# ════════════════════════════════════════════════════════════════════════════


def inprogress_text() -> str:
    head = "🚚 ВЫКУПЫ — СТАТУС ЗАКАЗОВ\n" f"Обновлено {_now_stamp()}\n" "\n"

    # ожидается: { sku: {"Выкупаются": x, "Доставляются": y, "Обрабатываются": z}, ... }
    excel = load_excel(force=True) or {}

    # ⛔️ Фильтруем только разрешённые SKU из WATCH_SKU (если он задан)
    if _WATCH_SET:
        skus = [int(s) for s in excel.keys() if int(s) in _WATCH_SET]
    else:
        skus = [int(s) for s in excel.keys()]

    # 🔢 Порядок — строго как в WATCH_SKU (затем по alias)
    skus_sorted = sorted(skus, key=_order_key_for_sku)

    lines: List[str] = []
    for sku in skus_sorted:
        rec = excel.get(sku) or {}
        buy = float(rec.get("Выкупаются", 0) or 0)
        deliv = float(rec.get("Доставляются", 0) or 0)
        proc = float(rec.get("Обрабатываются", 0) or 0)
        total = buy + deliv + proc
        if total <= 0:
            continue

        alias = _alias_pad(get_alias_for_sku(sku) or str(sku))
        # каждая метрика на своей строке + Σ, затем пустая строка
        lines.append(f"{alias}")
        lines.append(f"👤 Выкупается {_fmt_units(buy)}")
        lines.append(f"🚚 Доставляется {_fmt_units(deliv)}")
        lines.append(f"🛠 Обрабатывается {_fmt_units(proc)}")
        lines.append(f"Σ {_fmt_units(total)}")
        lines.append("")

    if not lines:
        lines = [
            "ℹ️ Нет данных по статусам. Добавьте строки в «Товары.xlsx» или проверьте WATCH_SKU."
        ]

    footer = "Источник: «Товары.xlsx».\n"
    return "\n".join([head] + lines + [footer])


# ════════════════════════════════════════════════════════════════════════════
#   УТИЛИТЫ АНАЛИТИКИ ПО ВЫКУПАМ (без рекомендаций закупок)
# ════════════════════════════════════════════════════════════════════════════

# До какого «капу» добиваем запасы (во сколько раз план на горизонт)
SHORTAGE_CAP_MULTIPLE = float(os.getenv("SHORTAGE_CAP_MULTIPLE", "1.00"))
# Округление «рекомендации к докупке» вверх до шага
SHORTAGE_ROUND_STEP = int(os.getenv("SHORTAGE_ROUND_STEP", "1"))
if SHORTAGE_ROUND_STEP < 1:
    SHORTAGE_ROUND_STEP = 1


def _ceil_to_step(x: float, step: int) -> int:
    if x <= 0:
        return 0
    return int(math.ceil(x / step) * step)


def merge_data(
    excel_rows: Dict[int, Dict[str, int]],
    stocks_rows: Dict[int, Dict[str, float]],
) -> Dict[int, Dict[str, float]]:
    out: Dict[int, Dict[str, float]] = {}
    all_skus = set(excel_rows.keys()) | set(stocks_rows.keys())

    for sku in all_skus:
        ex = excel_rows.get(sku, {})
        st = stocks_rows.get(sku, {}) or {}

        buying = float(ex.get("Выкупаются", 0) or 0)
        delivering = float(ex.get("Доставляются", 0) or 0)
        processing = float(ex.get("Обрабатываются", 0) or 0)

        available = float(st.get("available_for_sale", 0.0) or 0.0)
        checking = float(st.get("checking", 0.0) or 0.0)
        in_transit = float(st.get("in_transit", 0.0) or 0.0)
        valid = float(st.get("valid_stock_count", 0.0) or 0.0)
        returns = float(st.get("return_from_customer_stock_count", 0.0) or 0.0)
        reserved = float(st.get("reserved", 0.0) or 0.0)

        cand_total = buying + delivering + processing
        plat_total = available + checking + in_transit + valid + returns + reserved

        out[sku] = {
            "buying": buying,
            "delivering": delivering,
            "processing": processing,
            "available_for_sale": available,
            "checking": checking,
            "in_transit": in_transit,
            "valid_stock_count": valid,
            "return_from_customer_stock_count": returns,
            "reserved": reserved,
            "candidates_total": cand_total,
            "platform_total": plat_total,
        }

    return out


def shortage_to_cap(total_units: float, plan_units: float) -> int:
    try:
        plan = float(plan_units or 0.0)
        total = float(total_units or 0.0)
    except Exception:
        return 0
    if plan <= 0:
        return 0
    target = SHORTAGE_CAP_MULTIPLE * plan
    need = max(0.0, target - total)
    return _ceil_to_step(need, SHORTAGE_ROUND_STEP)


__all__ = [
    "inprogress_text",
    "merge_data",
    "shortage_to_cap",
]
