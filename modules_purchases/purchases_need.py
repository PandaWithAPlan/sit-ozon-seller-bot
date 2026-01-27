# modules_purchases/purchases_need.py
from __future__ import annotations
from .purchases_need_data import (  # type: ignore
    fetch_seller_rows,
    fetch_ozon_platform_totals,
    fetch_plan_units,
    get_forecast_method_title,
    # распределение
    load_dist_prefs,
    get_needs_by_warehouse_for_sku,
    get_needs_by_warehouse_total,
    get_city_config,
    list_warehouses_for_dist,
    BUY_COEF,
)

import os
import re
import datetime as dt
from typing import Dict, List, Tuple, Any, Optional

from dotenv import load_dotenv

"""
Рекомендации «Закупки»:
• жёсткий порядок по цвету/весу/позиции в WATCH_SKU,
• алиасы из ALIAS,
• берём только SKU из WATCH_SKU (если задан),
• корректный суффикс распределения по городам:
    – 1 город: "(<CITY1> — N)",
    – 2 города: "(<CITY1> — M / <CITY2> — H)",
    – распределение считаем по складам с ΣD/день > 0,
    – перечень складов для UI берём из list_warehouses_for_dist().
"""

# ── .env ─────────────────────────────────────────────────────────────────────
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
load_dotenv(os.path.join(ROOT_DIR, ".env"))

# ── источники данных ─────────────────────────────────────────────────────────

# ── цели продаж (эффективный план: цели > план) ─────────────────────────────
try:
    from modules_sales.sales_goal import effective_plan30_by_sku as _effective_plan  # type: ignore
except Exception:

    def _effective_plan(plan: Dict[int, float], horizon_days: int = 30) -> Dict[int, float]:
        return dict(plan)


# ── алиасы/форматирование ───────────────────────────────────────────────────
try:
    from modules_sales.sales_facts_store import get_alias_for_sku, _fmt_units  # type: ignore
except Exception:

    def get_alias_for_sku(sku: int) -> Optional[str]:
        return str(sku)

    def _fmt_units(v: float) -> str:
        return f"{int(round(v or 0))} шт"


# Позитивный спрос по складам (ΣD/день > 0) — из блока «Потребность по складам»
try:
    from modules_shipments.shipments_demand import get_positive_demand_wids  # type: ignore
except Exception:
    get_positive_demand_wids = None  # type: ignore


ALERT_PLAN_HORIZON_DAYS = int(os.getenv("ALERT_PLAN_HORIZON_DAYS", "30"))


def _now_stamp() -> str:
    return dt.datetime.now().strftime("%d.%m.%Y %H:%M")


def _alias_pad(alias: str, width: int = 26) -> str:
    alias = alias or ""
    return alias if len(alias) >= width else alias + " " * (width - len(alias))


# ── WATCH_SKU: порядок и набор SKU ──────────────────────────────────────────
def _watch_sku_order_list() -> List[int]:
    raw = (os.getenv("WATCH_SKU", "") or "").replace("\n", ",").replace(" ", ",")
    res: List[int] = []
    seen: set[int] = set()
    for tok in raw.split(","):
        t = tok.strip()
        if not t:
            continue
        left = t.split(":", 1)[0].strip()
        try:
            s = int(left)
        except Exception:
            continue
        if s not in seen:
            seen.add(s)
            res.append(s)
    return res


def _watch_pos_map() -> Dict[int, int]:
    return {s: i for i, s in enumerate(_watch_sku_order_list())}


def _allowed_sku_set() -> set[int]:
    return set(_watch_sku_order_list())


WATCH_POS = _watch_pos_map()
WATCH_SET = _allowed_sku_set()


# ── светофорные пороги ──────────────────────────────────────────────────────
def _get_float_env(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return default


BUY_RED_FACTOR = _get_float_env("BUY_RED_FACTOR", 4.0)
BUY_YELLOW_FACTOR = _get_float_env("BUY_YELLOW_FACTOR", 4.25)
BUY_MAX_FACTOR = _get_float_env("BUY_MAX_FACTOR", 5.0)
PROF_YELLOW_FACTOR = _get_float_env("PROF_YELLOW_FACTOR", 5.25)
PROF_RED_FACTOR = _get_float_env("PROF_RED_FACTOR", 5.5)

_EPS = 1e-6


def _status_light_by_plan_coverage(coverage_plan: float) -> str:
    c = float(coverage_plan or 0.0)
    if c >= PROF_RED_FACTOR:
        return "🟥"
    if c >= PROF_YELLOW_FACTOR:
        return "🟨"
    if c > BUY_MAX_FACTOR + _EPS:
        return "🟩"
    if abs(c - BUY_MAX_FACTOR) <= _EPS:
        return "✅"
    if c >= BUY_YELLOW_FACTOR:
        return "🟢"
    if c >= BUY_RED_FACTOR:
        return "🟠"
    return "🔴"


# ── флаг/утилита для светофорной индикации ──────────────────────────────────
def _env_bool(val: str | None, default: bool = True) -> bool:
    """
    Универсальный парсер булевых флагов из .env.
    Поддерживаем значения: 1/0, true/false, yes/no, on/off, да/нет.
    """
    if val is None or val == "":
        return default
    s = str(val).strip().lower()
    if s in {"1", "true", "yes", "y", "on", "да", "истина"}:
        return True
    if s in {"0", "false", "no", "n", "off", "нет"}:
        return False
    return default


# PURCHASES_NEED_LIGHTS_ENABLED=1 (по умолчанию) — показывать светофор и легенду
# PURCHASES_NEED_LIGHTS_ENABLED=0           — убрать 🟥🟨🟩🟢🟠🔴✅ и блок «Обозначения: …»
PURCHASES_NEED_LIGHTS_ENABLED: bool = _env_bool(
    os.getenv("PURCHASES_NEED_LIGHTS_ENABLED"),
    default=True,
)


def _strip_need_lights(text: str) -> str:
    """
    Убираем светофорную индикацию и легенду из блока «ЗАКУПКИ — РЕКОМЕНДАЦИИ».
    Используется как в UI (/buyouts), так и в автоотчётах (/notice).
    """
    marker = "Обозначения:"
    idx = text.find(marker)
    if idx != -1:
        text = text[:idx].rstrip()
    for sign in ("🟥", "🟨", "🟩", "🟢", "🟠", "🔴", "✅"):
        text = text.replace(sign, "")
    while "  " in text:
        text = text.replace("  ", " ")
    return text


def _normalize_need_layout(text: str) -> str:
    """
    Нормализуем вёрстку блока рекомендаций по закупкам, чтобы формат
    совпадал между разделом «Необходимо закупить» и уведомлениями /notice.

    Цель:
        phone_metal_stand_black
        👤 У Seller 1504 шт • 📦 У Ozon 678 шт
        📊 Необходимо 3150 шт
        🛒 Закупить 968 шт
    """
    markers = (
        "👤 У Seller",
        "📊 Необходимо",
        "🛒 Закупить",
    )

    for marker in markers:
        pattern = rf"(?<!\n){re.escape(marker)}"
        replacement = f"\n{marker}"
        text = re.sub(pattern, replacement, text)

    # Сжимаем лишние пустые строки
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text


# ───────── распределение по городам ─────────
def _compute_group_shares_for_sku(
    sku: int,
    moscow_wids: set[int],
    khab_wids: set[int],
) -> tuple[float, float]:
    """
    Доли распределения между группами складов CITY1/CITY2 для SKU.
    Учитываем только склады с ΣD/день > 0 за период из /warehouse.
    """
    moscow = set(moscow_wids or [])
    khab = set(khab_wids or [])

    try:
        if callable(get_positive_demand_wids):  # type: ignore
            pos_list = get_positive_demand_wids()
            if pos_list is not None:
                pos = {int(w) for w in (pos_list or [])}
                moscow = {w for w in moscow if w in pos}
                khab = {w for w in khab if w in pos}
    except Exception:
        pass

    if not moscow or not khab:
        return (0.0, 0.0)

    try:
        per_sku = get_needs_by_warehouse_for_sku(int(sku)) or {}
    except Exception:
        per_sku = {}

    if per_sku:
        wm = sum(float(per_sku.get(w, 0.0)) for w in moscow)
        wh = sum(float(per_sku.get(w, 0.0)) for w in khab)
    else:
        total = get_needs_by_warehouse_total() or {}
        wm = sum(float(total.get(w, 0.0)) for w in moscow)
        wh = sum(float(total.get(w, 0.0)) for w in khab)

    W = wm + wh
    if W <= 0:
        return (0.0, 0.0)
    return (wm / W, wh / W)


def _split_by_shares(qty: int, m_share: float, h_share: float) -> tuple[int, int]:
    if qty <= 0 or (m_share <= 0 and h_share <= 0):
        return (0, 0)
    m_raw = qty * float(m_share)
    h_raw = qty * float(h_share)
    m = int(m_raw)
    h = int(h_raw)
    rem = qty - (m + h)
    parts = sorted([(m_raw - m, "M"), (h_raw - h, "H")], reverse=True, key=lambda t: t[0])
    i = 0
    while rem > 0 and i < len(parts):
        if parts[i][1] == "M":
            m += 1
        else:
            h += 1
        rem -= 1
        i += 1
    return (m, h)


def _format_dist_suffix(sku: int, qty: int) -> str:
    """
    Текст распределения по городам для строки рекомендации.
    — 1 город: "(CITY1 — qty)"
    — 2 города: "(CITY1 — m / CITY2 — h)" по долям ΣD.
    — Если нет валидных настроек/долей — пустая строка.
    """
    try:
        cfg = get_city_config()
    except Exception:
        cfg = {"count": 2, "city1": "Москва", "city2": "Хабаровск"}

    if qty <= 0:
        return ""

    if int(cfg.get("count", 2)) == 1:
        return f" ({cfg.get('city1', 'Город')} — {int(qty)})"

    try:
        prefs = load_dist_prefs()
        moscow = {int(w) for w in (prefs.get("moscow_wids") or prefs.get("city1_wids") or [])}
        khab = {int(w) for w in (prefs.get("khabarovsk_wids") or prefs.get("city2_wids") or [])}
    except Exception:
        return ""

    # Разрешённые склады только из «ΣD>0» списка для UI
    try:
        allowed_pairs = list_warehouses_for_dist()
        allowed_wids = {int(w) for w, _ in (allowed_pairs or [])}
    except Exception:
        allowed_wids = set()

    if allowed_wids:
        moscow = {w for w in moscow if w in allowed_wids}
        khab = {w for w in khab if w in allowed_wids}

    if not moscow or not khab:
        return ""

    m_share, h_share = _compute_group_shares_for_sku(int(sku), moscow, khab)
    if (m_share + h_share) <= 0:
        return ""

    m, h = _split_by_shares(int(qty), m_share, h_share)
    if (m + h) <= 0:
        return ""

    c1 = cfg.get("city1", "Город1")
    c2 = cfg.get("city2", "Город2")
    return f" ({c1} — {m} / {c2} — {h})"


# ───────── сортировки по цвету ─────────
def _rank_deficit_color(light: str) -> int:
    return {"🔴": 0, "🟠": 1}.get(light, 9)


def _rank_surplus_color(light: str) -> int:
    return {"🟥": 0, "🟨": 1}.get(light, 9)


def _rank_enough_color(light: str) -> int:
    return {"🟢": 0, "✅": 1, "🟩": 2}.get(light, 9)


# ───────── безопасный вывод под parse_mode=HTML ─────────
def _html_escape(text: str) -> str:
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


# ── goal‑flags ───────────────────────────────────────────────────────────────
def _is_trueish(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return float(v) != 0.0
    s = str(v).strip().lower()
    return s in {"1", "true", "yes", "on", "goal", "targets"}


def _goal_mode_from_kwargs(kwargs: Dict[str, Any]) -> bool:
    if _is_trueish(kwargs.get("use_goal")):
        return True
    if _is_trueish(kwargs.get("use_sales_goal")):
        return True
    if _is_trueish(kwargs.get("goal")):
        return True
    if _is_trueish(kwargs.get("use_targets")):
        return True
    if _is_trueish(kwargs.get("use_sales_targets")):
        return True
    if str(kwargs.get("target", "")).strip().lower() == "goal":
        return True
    if str(kwargs.get("mode", "")).strip().lower() == "goal":
        return True
    return False


# ════════════════════════════════════════════════════════════════════════════
#  📊  ЗАКУПКИ — РЕКОМЕНДАЦИИ
# ════════════════════════════════════════════════════════════════════════════
def need_to_purchase_text(horizon_days: int = ALERT_PLAN_HORIZON_DAYS, **goal_kwargs: Any) -> str:
    goal_mode = _goal_mode_from_kwargs(goal_kwargs)

    method_title = get_forecast_method_title()
    if goal_mode:
        method_title = f"{method_title} + цели"

    head = (
        "📊 ЗАКУПКИ — РЕКОМЕНДАЦИИ\n"
        f"План продаж — {method_title} • Коэф. выкупа ×{BUY_COEF:g} • Обновлено {_now_stamp()}\n"
        "\n"
    )

    excel = fetch_seller_rows(force=True)  # У Seller
    plan = fetch_plan_units(horizon_days)  # План на горизонт (шт)
    ozon = fetch_ozon_platform_totals(force=True)  # У Ozon — сумма 6 метрик

    # goal‑mode: подменяем план на «эффективный» (цели > план)
    if goal_mode:
        try:
            plan = _effective_plan(plan, horizon_days=horizon_days)
        except TypeError:
            try:
                plan = _effective_plan(plan)  # type: ignore[misc]
            except Exception:
                pass
        except Exception:
            pass

    # Собираем множество SKU, присутствующих в источниках
    all_src_skus = set(int(s) for s in (excel.keys() | plan.keys() | ozon.keys()))
    # Строгая фильтрация: если WATCH_SKU задан — берём только его
    if WATCH_SET:
        all_src_skus &= WATCH_SET

    def _env_idx(s: int) -> int:
        return WATCH_POS.get(int(s), 10**9)

    all_skus: List[int] = sorted(all_src_skus, key=_env_idx)

    enough_items: List[Tuple[int, int, int, str]] = []
    deficit_items: List[Tuple[int, int, int, str]] = []
    surplus_items: List[Tuple[int, int, int, str]] = []

    for sku in all_skus:
        rec = excel.get(sku) or {}
        seller_total = (
            float(rec.get("Выкупаются", 0))
            + float(rec.get("Доставляются", 0))
            + float(rec.get("Обрабатываются", 0))
        )
        ozon_total = float(ozon.get(sku, 0.0))
        plan_units = float(plan.get(sku, 0.0))

        plan_q = int(round(plan_units))
        if plan_q <= 0:
            continue

        need = plan_q * BUY_COEF
        covered = seller_total + ozon_total

        coverage_plan = covered / plan_q if plan_q > 0 else 0.0
        light = _status_light_by_plan_coverage(coverage_plan)
        delta = int(round(need - covered))
        weight = abs(delta)

        tail = (
            f"👤 У Seller {_fmt_units(seller_total)}"
            f" • 🛒 У Ozon {_fmt_units(ozon_total)}"
            f" • 📊 Необходимо {_fmt_units(need)}"
        )
        alias = (get_alias_for_sku(sku) or str(sku)).strip()
        alias_pad = _alias_pad(alias)

        if delta > 0:
            dist_suffix = _format_dist_suffix(sku, delta)
            action = f"🛒 Закупить {_fmt_units(delta)}{dist_suffix}"
        elif delta < 0:
            action = f"🏷 Распродать {_fmt_units(-delta)}"
        else:
            action = "♻️ Поддерживать"

        line = f"{light} {alias_pad}{tail} → {action}"
        env_idx = _env_idx(sku)

        if light in {"✅", "🟢", "🟩"}:
            rank = _rank_enough_color(light)
            enough_items.append((rank, -weight, env_idx, line))
        else:
            if delta > 0:
                rank = _rank_deficit_color(light)
                deficit_items.append((rank, -weight, env_idx, line))
            elif delta < 0:
                rank = _rank_surplus_color(light)
                surplus_items.append((rank, -weight, env_idx, line))
            else:
                rank = _rank_enough_color(light)
                enough_items.append((rank, -weight, env_idx, line))

    enough_items.sort(key=lambda t: (t[0], t[1], t[2]))
    deficit_items.sort(key=lambda t: (t[0], t[1], t[2]))
    surplus_items.sort(key=lambda t: (t[0], t[1], t[2]))

    def _flatten(items: List[Tuple[int, int, int, str]]) -> List[str]:
        out: List[str] = []
        for _k1, _k2, _k3, ln in items:
            out += [ln, ""]
        return out

    enough_lines = _flatten(enough_items)
    deficit_lines = _flatten(deficit_items)
    surplus_lines = _flatten(surplus_items)

    def _section(title: str, items: List[str]) -> List[str]:
        """
        Формат пустого статуса — общий для блока рекомендаций и /notice:
        • Нет позиций в статусе «Название статуса».
        """
        # Внутри кавычек используем именно название статуса без эмодзи.
        status_name = title.strip()
        for prefix in ("✅ ", "🔻 ", "🔺 "):
            if status_name.startswith(prefix):
                status_name = status_name[len(prefix) :]
                break

        if not items:
            return [
                title,
                "",
                f"• Нет позиций в статусе «{status_name}».",
                "",
            ]
        return [title, ""] + items

    blocks: List[str] = []
    # ✅ перед словом «Достаточно» в заголовке
    blocks += _section("✅ Достаточно", enough_lines)
    blocks += _section("🔻 Дефицит", deficit_lines)
    blocks += _section("🔺 Профицит", surplus_lines)

    footer = (
        "Обозначения:\n"
        "✅ — норма (ровно ×5)\n"
        "🟥🟨🟩 — профицит (рекомендация: распродать);\n"
        "🔴🟠🟢 — дефицит (рекомендация: приобрести)\n"
    )

    body = "\n".join([head] + blocks + [footer])

    # ⬇️ Централизованный учёт флага PURCHASES_NEED_LIGHTS_ENABLED:
    #    теперь и /buyouts, и /notice получают текст уже с/без светофора.
    if not PURCHASES_NEED_LIGHTS_ENABLED:
        body = _strip_need_lights(body)

    # ⬇️ Приведение формата к виду раздела «Необходимо закупить»
    #    (одинаковая вёрстка для меню и уведомлений /notice).
    body = _normalize_need_layout(body)

    # ⬇️ Заменяем иконку для Ozon, как в UI:
    #    было «🛒 У Ozon», стало «📦 У Ozon».
    body = body.replace("🛒 У Ozon", "📦 У Ozon")

    return _html_escape(body)


__all__ = ["need_to_purchase_text"]
