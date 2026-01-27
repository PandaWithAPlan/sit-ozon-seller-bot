from __future__ import annotations

import os
import json
import math
import datetime as dt
from typing import Dict, Tuple, List, Optional

from dotenv import load_dotenv

# ── env / paths ──────────────────────────────────────────────────────────────
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
load_dotenv(os.path.join(ROOT_DIR, ".env"))

CACHE_DIR = os.path.join(ROOT_DIR, "data", "cache", "sales")
os.makedirs(CACHE_DIR, exist_ok=True)
GOAL_FILE = os.path.join(CACHE_DIR, "sales_goal.json")

# ── импорты данных/форматирования (без внешних зависимостей на UI) ──────────
try:
    from modules_sales.sales_facts_store import (
        get_alias_for_sku,
        _fmt_money,
        _fmt_units,
        _watch_skus_order_list as _order_list,  # порядок из WATCH_SKU
        _allowed_sku_set as _allowed_set,  # допустимые SKU из WATCH_SKU
    )  # type: ignore
except Exception:

    def get_alias_for_sku(sku: int) -> str | None:
        return str(sku)

    def _fmt_money(v: float) -> str:
        try:
            return f"{int(round(float(v) or 0)):,}".replace(",", " ") + " ₽"
        except Exception:
            return "0 ₽"

    def _fmt_units(v: float) -> str:
        try:
            return f"{int(round(float(v) or 0))} шт"
        except Exception:
            return "0 шт"

    # Фолбэк: собрать порядок и множество из WATCH_SKU на случай отсутствия импорта
    def _order_list() -> List[int]:
        raw = (os.getenv("WATCH_SKU", "") or "").replace("\n", ",")
        out: List[int] = []
        seen: set[int] = set()
        for token in raw.split(","):
            token = token.strip()
            if not token:
                continue
            left = token.split(":", 1)[0].strip()
            if not left.isdigit():
                continue
            v = int(left)
            if v not in seen:
                seen.add(v)
                out.append(v)
        return out

    def _allowed_set() -> set[int]:
        st: set[int] = set()
        for s in (os.getenv("WATCH_SKU", "") or "").split(","):
            s = s.strip()
            if ":" in s:
                s = s.split(":", 1)[0].strip()
            if s.isdigit():
                st.add(int(s))
        return st


# Источники фактов/прогноза/среднего чека
try:
    from modules_sales.sales_forecast import (  # type: ignore
        _fetch_series_from_api as _fetch_series,
        _avg_price_from_api as _avg_price,
        _forecast_next as _forecast_next,
        get_forecast_method_title,
    )
except Exception:

    def _fetch_series(_days_back: int) -> Dict[int, List[Tuple[dt.date, float, float]]]:
        return {}

    def _avg_price(_days_back: int = 30) -> Dict[int, float]:
        return {}

    def _forecast_next(_series, _h: int) -> Tuple[float, float]:
        return (0.0, 0.0)

    def get_forecast_method_title() -> str:
        return "метод прогноза не задан"


# ── светофорные пороги ───────────────────────────────────────────────────────
def _f(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return default


def _b(name: str, default: bool) -> bool:
    """
    Чтение булевого флага из .env.
    Поддерживаем значения: 1/true/yes/on и 0/false/no/off (регистр не важен).
    """
    raw = os.getenv(name)
    if raw is None:
        return default
    raw = raw.strip().lower()
    return raw in {"1", "true", "yes", "y", "on"}


GOAL_RED_FACTOR_HIGH = _f("GOAL_RED_FACTOR_HIGH", 1.20)  # 🟥 сильный профицит (перевыполнение)
GOAL_YELLOW_FACTOR_HIGH = _f("GOAL_YELLOW_FACTOR_HIGH", 1.10)  # 🟨 умеренный профицит
GOAL_GREEN_TOL = _f("GOAL_GREEN_TOL", 0.02)  # ✅ норма |r−1| ≤ tol
GOAL_YELLOW_FACTOR_LOW = _f("GOAL_YELLOW_FACTOR_LOW", 0.95)  # 🟢 почти норма (дефицит)
GOAL_RED_FACTOR_LOW = _f("GOAL_RED_FACTOR_LOW", 0.90)  # 🔴 сильный дефицит

# Включение/выключение светофорной индикации в отчёте по целям
GOAL_LIGHTS_ENABLED = _b("GOAL_LIGHTS_ENABLED", True)


# ── утилиты хранения ─────────────────────────────────────────────────────────
def _read_json(path: str) -> dict:
    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f) or {}
    except Exception:
        pass
    return {}


def _write_json_atomic(path: str, payload: dict) -> None:
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        os.replace(tmp, path)
    except Exception:
        pass


# ── публичное API хранения целей ─────────────────────────────────────────────
def get_goal_per_day_by_sku() -> Dict[int, float]:
    js = _read_json(GOAL_FILE) or {}
    per_day = js.get("per_day") or {}
    out: Dict[int, float] = {}
    for k, v in (per_day.items() if isinstance(per_day, dict) else []):
        try:
            out[int(k)] = float(v or 0.0)
        except Exception:
            continue
    return out


def set_goal_per_day(sku: int, value: float) -> None:
    try:
        s = int(sku)
    except Exception:
        return
    val = float(value or 0.0)
    if val < 0:
        val = 0.0
    js = _read_json(GOAL_FILE) or {}
    if not isinstance(js, dict):
        js = {}
    js["version"] = 1
    js["updated_at"] = dt.datetime.now().isoformat(timespec="seconds")
    pd = js.get("per_day") or {}
    pd[str(s)] = float(val)
    js["per_day"] = pd
    _write_json_atomic(GOAL_FILE, js)


def reset_goal_per_day(sku: int) -> None:
    set_goal_per_day(sku, 0.0)


def get_goal30_by_sku(horizon_days: int = 30) -> Dict[int, float]:
    H = max(1, int(horizon_days))
    k = float(H)
    per_day = get_goal_per_day_by_sku()
    return {int(s): float(v) * k for s, v in per_day.items() if float(v) > 0.0}


def effective_plan30_by_sku(
    forecast_plan30: Dict[int, float], horizon_days: int = 30
) -> Dict[int, float]:
    """
    Возвращает «лучший» PlanH: если для SKU задана цель — используем её (Goal×H),
    иначе — берём из прогноза forecast_plan30.
    """
    H = max(1, int(horizon_days))
    goalH = get_goal30_by_sku(H)  # только >0
    out: Dict[int, float] = {}
    keys = set(forecast_plan30.keys()) | set(goalH.keys())
    for k in keys:
        try:
            s = int(k)
        except Exception:
            continue
        gv = float(goalH.get(s, 0.0))
        pv = float(forecast_plan30.get(s, 0.0))
        out[s] = gv if gv > 0 else pv
    return out


# ── форматирование для горизонтов ────────────────────────────────────────────
def _fmt_units_h(value: float, horizon: int) -> str:
    if int(horizon) == 1:
        try:
            return f"{float(value):.1f} шт/д"
        except Exception:
            return "0.0 шт/д"
    return _fmt_units(value)


def _fmt_money_h(value: float, horizon: int) -> str:
    return _fmt_money(value)


# ── вспомогательный вывод алиаса (не скрывать числовые SKU) ──────────────────
def _display_alias(sku: int) -> str:
    """
    Всегда выводим читаемую метку: ALIAS (если есть) или "SKU <id>".
    """
    try:
        alias_raw = get_alias_for_sku(sku)
        alias = str(alias_raw).strip() if alias_raw is not None else ""
    except Exception:
        alias = ""
    if not alias:
        return f"SKU {sku}"
    if alias.isdigit():
        try:
            if int(alias) == int(sku):
                return f"SKU {sku}"
        except Exception:
            pass
        return f"SKU {alias}"
    return alias


# ── расчёт отчёта ────────────────────────────────────────────────────────────
def _sum_last_n_days(seq: List[Tuple[dt.date, float, float]], n: int) -> Tuple[float, float]:
    if not seq:
        return (0.0, 0.0)
    days = max(1, int(n))
    tail = seq[-days:]
    u = sum(float(t[1] or 0.0) for t in tail)
    r = sum(float(t[2] or 0.0) for t in tail)
    return (u, r)


# === Светофор и группа от соотношения ФАКТ/ЦЕЛЬ-или-ПЛАН =====================
def _status_light_goal(r: Optional[float]) -> str:
    """
    Возвращает эмодзи-светофор по отношению r = FACT / TARGET,
    где TARGET — Цель (если задана) или План (если цели нет).
    """
    if r is None:
        return "✅"  # нет базы для сравнения — считаем «без отклонения»
    try:
        x = float(r)
    except Exception:
        return "✅"

    # Норма
    if abs(x - 1.0) <= GOAL_GREEN_TOL:
        return "✅"

    # Профицит (перевыполнение фактом)
    if x >= GOAL_RED_FACTOR_HIGH:
        return "🟥"
    if x >= GOAL_YELLOW_FACTOR_HIGH:
        return "🟨"
    if x > 1.0:
        return "🟩"

    # Дефицит (недовыполнение фактом)
    if x >= GOAL_YELLOW_FACTOR_LOW:
        return "🟢"
    if x >= GOAL_RED_FACTOR_LOW:
        return "🟠"
    return "🔴"


def _group_from_ratio(r: Optional[float]) -> str:
    """
    Группа из r = FACT / TARGET:
        r < 1  → DEFICIT (ускорить),
        r > 1  → SURPLUS (снизить),
        |r−1| ≤ tol → ENOUGH.
    """
    if r is None:
        return "ENOUGH"
    try:
        x = float(r)
    except Exception:
        return "ENOUGH"
    if abs(x - 1.0) <= GOAL_GREEN_TOL:
        return "ENOUGH"
    return "DEFICIT" if x < 1.0 else "SURPLUS"


def _color_rank_for_group(group: str, light: str) -> int:
    """
    Приоритет цвета внутри группы.
        ENOUGH:  🟢 (0) → 🟩 (1) → ✅ (2)
        DEFICIT: 🔴 (0) → 🟠 (1) → 🟢 (2)
        SURPLUS: 🟥 (0) → 🟨 (1) → 🟩 (2)
    """
    if group == "ENOUGH":
        return {"🟢": 0, "🟩": 1, "✅": 2}.get(light, 99)
    if group == "DEFICIT":
        return {"🔴": 0, "🟠": 1, "🟢": 2}.get(light, 99)
    if group == "SURPLUS":
        return {"🟥": 0, "🟨": 1, "🟩": 2}.get(light, 99)
    return 99


def sales_goal_report_text(horizon_days: int = 30, metric: str = "units") -> str:
    """
    Формирует отчёт «📊 ЦЕЛЬ ПРОДАЖ — РЕКОМЕНДАЦИИ» для горизонта H∈{1,30}
    и метрики metric∈{"units","revenue"}.

    ❗️Порядок элементов: (1) по цвету, (2) по «весу» рекомендации (убывание),
    (3) при равном весе — по порядку из WATCH_SKU.
    В отчёт попадают только SKU из WATCH_SKU.
    """
    H = 30 if int(horizon_days) != 1 else 1
    metric = (metric or "units").strip().lower()
    if metric not in {"units", "revenue"}:
        metric = "units"

    order_list = _order_list()
    pos_by_watch: Dict[int, int] = {sku: i for i, sku in enumerate(order_list)}
    allowed = _allowed_set()

    series = _fetch_series(max(60, 180)) or {}
    avg_price = _avg_price(30) or {}
    method_title = get_forecast_method_title()
    now_str = dt.datetime.now().strftime("%d.%m.%Y %H:%M")

    goals_day = get_goal_per_day_by_sku()

    # Обрабатываем только наблюдаемые SKU
    sku_set = set(series.keys()) | set(goals_day.keys())
    if allowed:
        sku_set = {s for s in sku_set if s in allowed}

    # собираем записи с метаданными, чтобы потом отсортировать
    entries: Dict[str, List[dict]] = {"DEFICIT": [], "ENOUGH": [], "SURPLUS": []}

    total_norm_count = 0
    total_goaled = 0
    deficit30 = 0.0
    surplus30 = 0.0

    for sku in sku_set:
        alias = _display_alias(sku)

        seq = series.get(sku) or {}
        seq = series.get(sku) or []
        fact30_u, fact30_r = _sum_last_n_days(seq, 30)
        plan30_u, plan30_r = _forecast_next(seq, 30)  # план по юнитам и выручке
        plan1_u = float(plan30_u) / 30.0
        fact1_u = float(fact30_u) / 30.0
        fact1_r = float(fact30_r) / 30.0
        ap = float(avg_price.get(sku, 0.0))

        goal_day_u = float(goals_day.get(sku, 0.0))
        goalH_u = goal_day_u * float(H)

        # План/Факт/Цель в выбранной метрике на H (для отображения)
        if metric == "revenue":
            if ap > 0:
                planH_v = (plan1_u * H) * ap
                plan30_v = plan30_u * ap
                goalH_v = (goal_day_u * H) * ap
            else:
                # нет среднего чека → считаем всё в «доходе по ряду» (fallback)
                planH_v = (plan30_r / 30.0) * H
                plan30_v = plan30_r
                goalH_v = 0.0  # цели в ₽ посчитать не можем
            factH_v = fact1_r if H == 1 else fact30_r

            # Цель на H: если цель задана и есть ap → цель, иначе → план
            targetH_v = goalH_v if (goal_day_u > 0 and ap > 0) else planH_v

        else:
            # metric == "units"
            planH_v = plan1_u * H
            plan30_v = float(plan30_u)
            goalH_v = goalH_u
            factH_v = fact1_u if H == 1 else fact30_u

            # Цель на H: если цель задана → цель, иначе → план
            targetH_v = goalH_v if goal_day_u > 0 else planH_v

        # ── Рекомендация: если цель задана, считаем от ФАКТА до ЦЕЛИ,
        #    если цель не задана, считаем от ФАКТА до ПЛАНА (в 30‑дневном горизонте)
        if metric == "revenue":
            if ap > 0:
                fact30_v = float(fact30_r)  # ₽
                plan30_v_revenue = plan30_u * ap  # ₽
                target30_v = (goal_day_u * 30.0 * ap) if goal_day_u > 0 else plan30_v_revenue
            else:
                fact30_v = float(fact30_r)  # ₽ (fallback)
                plan30_v_revenue = float(plan30_r)  # ₽
                target30_v = plan30_v_revenue

            delta30 = float(target30_v - fact30_v)
            per_day = delta30 / 30.0

            if H == 1:
                action = (
                    f"⏫ Ускорить на {_fmt_money(abs(per_day))} в день"
                    if per_day > 0
                    else (
                        f"⏬ Снизить на {_fmt_money(abs(per_day))} в день"
                        if per_day < 0
                        else "🔄 Поддерживать"
                    )
                )
            else:
                action = (
                    f"⏫ Ускорить на {_fmt_money(abs(delta30))} за 30 дней"
                    if delta30 > 0
                    else (
                        f"⏬ Снизить на {_fmt_money(abs(delta30))} за 30 дней"
                        if delta30 < 0
                        else "🔄 Поддерживать"
                    )
                )

        else:
            # metric == "units"
            fact30_v = float(fact30_u)
            plan30_v_units = float(plan30_v)
            target30_v = (goal_day_u * 30.0) if goal_day_u > 0 else plan30_v_units

            delta30 = float(target30_v - fact30_v)
            per_day = delta30 / 30.0

            if H == 1:
                if per_day > 0:
                    action = f"⏫ Ускорить на {per_day:.1f} шт в день"
                elif per_day < 0:
                    action = f"⏬ Снизить на {abs(per_day):.1f} шт в день"
                else:
                    action = "🔄 Поддерживать"
            else:
                d30_int = int(round(abs(delta30)))
                if delta30 > 0:
                    action = f"⏫ Ускорить на {d30_int} шт за 30 дней"
                elif delta30 < 0:
                    action = f"⏬ Снизить на {d30_int} шт за 30 дней"
                else:
                    action = "🔄 Поддерживать"

        # === ВАРИАНТ A: группа и светофор от FACT/TARGET на горизонте H ======
        r_htarget: Optional[float] = None
        try:
            r_htarget = float(factH_v) / float(targetH_v) if float(targetH_v) > 0 else None
        except Exception:
            r_htarget = None

        light = _status_light_goal(r_htarget)  # эмодзи соответствует действию
        group = _group_from_ratio(r_htarget)  # SURPLUS→снизить, DEFICIT→ускорить

        # Вес для сортировки и агрегаты дефицита/профицита (по цели, если есть)
        weight = abs(delta30)
        if goal_day_u > 0:
            total_goaled += 1
            # «в норме» считаем по Факт/Цель на горизонте H (если есть цель в деньгах/шт)
            r_for_norm: Optional[float] = None
            try:
                if metric == "revenue":
                    if ap > 0:
                        goalH_only = goal_day_u * H * ap
                        r_for_norm = float(factH_v) / float(goalH_only) if goalH_only > 0 else None
                else:
                    goalH_only = goalH_u
                    r_for_norm = float(factH_v) / float(goalH_only) if goalH_only > 0 else None
            except Exception:
                r_for_norm = None

            if _status_light_goal(r_for_norm) in {"✅", "🟢", "🟩"}:
                total_norm_count += 1

            if delta30 > 0:
                deficit30 += delta30
            elif delta30 < 0:
                surplus30 += abs(delta30)

        # Формируем строки: факт/план/цель — в одной строке, рекомендация — на новой
        if metric == "revenue":
            if H == 1:
                fact_str = f"💰 Факт день: {_fmt_money(factH_v)}"
            else:
                fact_str = f"💰 Факт: {_fmt_money(factH_v)}"
            plan_str = f"🧮 План: {_fmt_money(planH_v)}"
            goal_str = f"🎯 Цель: {_fmt_money(goalH_v)}"
        else:
            if H == 1:
                fact_str = f"📦 Факт день: {_fmt_units_h(factH_v, H)}"
            else:
                fact_str = f"📦 Факт: {_fmt_units_h(factH_v, H)}"
            plan_str = f"🧮 План: {_fmt_units_h(planH_v, H)}"
            goal_str = f"🎯 Цель: {_fmt_units_h(goalH_v, H)}"

        fact_goal_line = f"{fact_str} • {plan_str} • {goal_str}"
        action_line = action

        color_rank = _color_rank_for_group(group, light)

        # Шапка строки: либо с эмодзи светофора, либо без него (если отключено)
        if GOAL_LIGHTS_ENABLED and light:
            header_line = f"{light} {alias}"
        else:
            header_line = f"{alias}"

        entry = {
            "sku": int(sku),
            "pos": pos_by_watch.get(int(sku), 10**9),  # ← позиция из WATCH_SKU (tie‑breaker)
            "light": light,
            "group": group,
            "alias": alias,
            "alias_sort": alias.lower().strip(),
            "weight": float(weight or 0.0),  # для сортировки по убыванию
            "color_rank": color_rank,
            "lines": [
                header_line,
                fact_goal_line,
                action_line,
                "",
            ],
        }
        entries[group].append(entry)

    # Заголовок
    head = [
        "📊 ЦЕЛЬ ПРОДАЖ — РЕКОМЕНДАЦИИ",
        f"Метод плана: {method_title} (30 дн) • Горизонт: {H} дн • ⏱ Обновлено: {now_str}",
        "",
    ]

    # ── Порядок блоков: дефицит → профицит → норма ────────────────────────────
    groups_order = ["DEFICIT", "SURPLUS", "ENOUGH"]
    group_title = {
        "DEFICIT": "🔻 Дефицит",
        "SURPLUS": "🔺 Профицит",
        "ENOUGH": "✅ Достаточно",
    }

    # Сортировка внутри группы: цвет → «вес» (desc) → порядок WATCH_SKU
    def _sorted_lines(group_key: str) -> List[str]:
        arr = entries.get(group_key, []) or []
        arr.sort(key=lambda e: (e["color_rank"], -e["weight"], e["pos"]))
        out: List[str] = []
        for e in arr:
            out.extend(e["lines"])
        return out

    def _status_name_for_group(group_key: str) -> str:
        if group_key == "DEFICIT":
            return "Дефицит"
        if group_key == "SURPLUS":
            return "Профицит"
        if group_key == "ENOUGH":
            return "Достаточно"
        return "статус"

    body: List[str] = []
    for g in groups_order:
        body += [group_title[g], ""]
        block = _sorted_lines(g)
        if block:
            body += block
        else:
            # Единое форматирование, когда в статусе нет данных.
            status_name = _status_name_for_group(g)
            body += [f"• Нет позиций в статусе «{status_name}».", ""]

    # Итоги
    norm_pct = int(
        round(100.0 * (float(total_norm_count) / float(total_goaled)) if total_goaled > 0 else 0.0)
    )
    if metric == "revenue":
        tail = [
            "📊 ИТОГО: «в норме» — "
            f"{norm_pct}%, дефицит — {_fmt_money(deficit30)} за 30 дней, "
            f"профицит — {_fmt_money(surplus30)} за 30 дней"
        ]
    else:
        tail = [
            "📊 ИТОГО: «в норме» — "
            f"{norm_pct}%, дефицит — {int(round(deficit30))} шт за 30 дней, "
            f"профицит — {int(round(surplus30))} шт за 30 дней"
        ]

    # ── Подвал с обозначениями ────────────────────────────────────────────────
    legend: List[str] = []
    if GOAL_LIGHTS_ENABLED:
        legend = [
            "",
            "Обозначения:",
            "✅ — норма (равно цели внутри допуска)",
            "🟥🟨🟩 — профицит (рекомендация: распродать);",
            "🔴🟠🟢 — дефицит (рекомендация: приобрести)",
        ]

    return "\n".join(head + body + [""] + tail + legend)


__all__ = [
    "get_goal_per_day_by_sku",
    "set_goal_per_day",
    "reset_goal_per_day",
    "get_goal30_by_sku",
    "effective_plan30_by_sku",
    "sales_goal_report_text",
]
