# scheduler.py
from modules_sales.sales_forecast import forecast_text
from modules_sales.sales_facts_store import (
    facts_text,
    get_alias_for_sku,
)
import asyncio
import json
import logging
import os
import re
import sys
import time
from collections import OrderedDict
from datetime import datetime, date, time as dtime, timedelta
from typing import List, Dict, Set, Tuple, Optional, Callable

from aiogram import Bot
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from dotenv import dotenv_values
from pytz import timezone
from re import findall

# ─────────────────────────────────────────────────────────────────────────────
# Базовые пути (важно объявить ДО чтения .env)
# ─────────────────────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
CACHE_COMMON_DIR = os.path.join(DATA_DIR, "cache", "common")
os.makedirs(CACHE_COMMON_DIR, exist_ok=True)

# Добавляем потенциальные каталоги модулей в sys.path (если они не являются пакетами)
MODULES_SHIP_DIR = os.path.join(BASE_DIR, "modules_shipments")
MODULES_SALES_DIR = os.path.join(BASE_DIR, "modules_sales")
MODULES_LOG_DIR = os.path.join(BASE_DIR, "modules_logistics")
MODULES_MKT_DIR = os.path.join(BASE_DIR, "modules_marketing")
MODULES_TRAFFIC_DIR = os.path.join(BASE_DIR, "modules_traffic")
for _p in (
    MODULES_SHIP_DIR,
    MODULES_SALES_DIR,
    MODULES_LOG_DIR,
    MODULES_MKT_DIR,
    MODULES_TRAFFIC_DIR,
):
    try:
        if os.path.isdir(_p) and _p not in sys.path:
            sys.path.insert(0, _p)
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# ВСПОМОГАТЕЛЬНО: безопасный поиск функций по нескольким путям/имён
# ─────────────────────────────────────────────────────────────────────────────
def _resolve_text_function(
    module_names: List[str], function_names: List[str]
) -> Optional[Callable[..., str]]:
    """
    Пытаемся импортировать первую найденную функцию из перечня модулей/имён.
    Возвращаем саму функцию или None.
    """
    for mod_name in module_names:
        try:
            mod = __import__(mod_name, fromlist=function_names)
        except Exception:
            continue
        for fn_name in function_names:
            try:
                fn = getattr(mod, fn_name, None)
                if callable(fn):
                    return fn  # type: ignore
            except Exception:
                continue
    return None


def _try_warmup_module(mod_name: str, fn_names: List[str]) -> bool:
    """
    Best‑effort вызов функций тёплого старта кэшей в разных реализациях.
    Возвращает True, если удалось вызвать что‑то без исключения.
    """
    try:
        mod = __import__(mod_name, fromlist=fn_names)
    except Exception:
        return False
    ok = False
    for fn_name in fn_names:
        try:
            fn = getattr(mod, fn_name, None)
            if callable(fn):
                fn()  # type: ignore
                ok = True
        except TypeError:
            try:
                getattr(mod, fn_name)()  # type: ignore
                ok = True
            except Exception:
                continue
        except Exception:
            continue
    return ok


def _shipments_best_effort_warmup() -> bool:
    """
    Пробуем прогреть все возможные кэши данных отгрузок/потребности/логистики,
    чтобы отчёты не отдавали «модуль не подключён/нет данных».
    """
    warm_calls = [
        # demand / shipments
        (
            "modules_shipments.shipments_demand_data",
            [
                "warmup",
                "warmup_cache",
                "ensure_loaded",
                "load",
                "load_cache",
                "preload",
                "init",
                "init_cache",
                "build_cache",
            ],
        ),
        (
            "shipments_demand_data",
            [
                "warmup",
                "warmup_cache",
                "ensure_loaded",
                "load",
                "load_cache",
                "preload",
                "init",
                "init_cache",
                "build_cache",
            ],
        ),
        (
            "modules_shipments.shipments_need_data",
            ["warmup", "ensure_loaded", "load", "preload", "init", "build", "build_cache"],
        ),
        (
            "shipments_need_data",
            ["warmup", "ensure_loaded", "load", "preload", "init", "build", "build_cache"],
        ),
        # leadtime
        (
            "modules_shipments.shipments_leadtime_stats_data",
            ["warmup", "ensure_loaded", "load", "preload", "init", "build", "build_cache"],
        ),
        (
            "shipments_leadtime_stats_data",
            ["warmup", "ensure_loaded", "load", "preload", "init", "build", "build_cache"],
        ),
        (
            "modules_shipments.shipments_leadtime_data",
            ["warmup", "ensure_loaded", "load", "preload", "init", "build", "build_cache"],
        ),
        (
            "shipments_leadtime_data",
            ["warmup", "ensure_loaded", "load", "preload", "init", "build", "build_cache"],
        ),
    ]
    any_ok = False
    for mod_name, names in warm_calls:
        if _try_warmup_module(mod_name, names):
            any_ok = True
    return any_ok


# ─────────────────────────────────────────────────────────────────────────────
# Импорты согласно структуре
# ─────────────────────────────────────────────────────────────────────────────
# Продажи: факты/алиасы и прогноз
# Цели продаж (новые уведомления «Необходимо заработать/продать»)
try:
    from modules_sales.sales_goal import sales_goal_report_text  # type: ignore
except Exception:

    def sales_goal_report_text(*args, **kwargs) -> str:  # type: ignore
        return "📊 ЦЕЛЬ ПРОДАЖ — РЕКОМЕНДАЦИИ\nДанные недоступны."


# Приватный загрузчик фактов по SKU (если есть)
try:
    from modules_sales.sales_facts_store import _fetch_series as _facts_fetch_series  # type: ignore
except Exception:
    _facts_fetch_series = None  # type: ignore

# ВНИМАНИЕ: НЕ импортируем здесь напрямую need_to_purchase_text из легаси‑модуля,
# чтобы не потерять поддержку goal‑аргументов. Резолвим динамически ниже.

# Отгрузки — гибко определяем реальную функцию
_NEED_SHIP_FN: Optional[Callable[..., str]] = _resolve_text_function(
    module_names=[
        "modules_shipments.shipments_need",
        "shipments_need",
        "modules_shipments.shipments_reports",  # совместимость
        "shipments_reports",  # совместимость (корень)
    ],
    function_names=["need_to_ship_text", "shipments_text", "need_text"],
)

# >>> Использовать ли «цели продаж» в операционных отчётах (выкупы/отгрузки) в автоуведомлениях
#     По умолчанию включено, чтобы поведение совпадало с меню.
CFG = dotenv_values(os.path.join(BASE_DIR, ".env"))


def _flag(name: str, default: str = "1") -> bool:
    return str(CFG.get(name, default)).strip().lower() in ("1", "true", "yes", "on")


OPERATIONS_USE_SALES_GOAL: bool = _flag("OPERATIONS_USE_SALES_GOAL", "1")  # <<< новый флаг


# ─────────────────────────────────────────────────────────────────────────────
# Вызовы «Необходимо отгрузить» с учётом sales goal (если модуль поддерживает)
# ─────────────────────────────────────────────────────────────────────────────
def _call_need_to_ship_text(use_goal: bool = False) -> Optional[str]:
    """
    1) Если есть прямая текстовая функция → пробуем вызвать её с параметрами «goal mode».
       Поддерживаем разные имена аргументов: use_goal/use_sales_goal/goal, target='goal', mode='goal'.
    2) Фолбэк: modules_shipments.shipments_need.{compute_need, format_need_text}
       — также пробуем «goal mode», затем дефолт.
    """
    goal_kw_candidates: List[Dict[str, object]] = [
        {"use_goal": True},
        {"use_sales_goal": True},
        {"goal": True},
        {"target": "goal"},
        {"mode": "goal"},
        {"use_targets": True},
        {"use_sales_targets": True},
    ]

    # Прямой путь
    if _NEED_SHIP_FN:
        # Сначала с goal‑параметрами (если нужно), потом — без
        param_sets: List[Dict[str, object]] = []
        if use_goal:
            for gkw in goal_kw_candidates:
                param_sets.append({**gkw, "view": "sku"})
                param_sets.append({**gkw})
        param_sets.append({"view": "sku"})
        param_sets.append({})

        for kwargs in param_sets:
            try:
                t = _NEED_SHIP_FN(**kwargs)  # type: ignore
                if isinstance(t, str) and t.strip():
                    return t
            except TypeError:
                continue
            except Exception:
                # при любой иной ошибке пробуем следующий вариант
                continue

    # Фолбэк: compute_need + format_need_text
    for mod_name in (
        "modules_shipments.shipments_need",
        "shipments_need",
        "modules_shipments.shipments_reports",
        "shipments_reports",
    ):
        try:
            mod = __import__(mod_name, fromlist=["compute_need", "format_need_text"])
        except Exception:
            continue
        try:
            compute_need = getattr(mod, "compute_need", None)
            format_need_text = getattr(mod, "format_need_text", None)
        except Exception:
            compute_need = None
            format_need_text = None

        if callable(compute_need) and callable(format_need_text):
            # Набор попыток: goal‑режимы, затем обычный
            param_sets = []
            if use_goal:
                for gkw in goal_kw_candidates:
                    param_sets.append({**gkw, "scope": "sku"})
                    param_sets.append({**gkw})
            param_sets.append({"scope": "sku"})
            param_sets.append({})

            for kwargs in param_sets:
                try:
                    payload = compute_need(**kwargs)  # type: ignore
                    txt = format_need_text(payload)  # type: ignore
                    if isinstance(txt, str) and txt.strip():
                        return txt
                except TypeError:
                    continue
                except Exception:
                    continue

    return None


# ─────────────────────────────────────────────────────────────────────────────
# «Потребность по SKU» (best‑effort, разные имена функций)
# ─────────────────────────────────────────────────────────────────────────────
_DEMAND_TEXT_FN: Optional[Callable[..., str]] = _resolve_text_function(
    module_names=[
        "modules_shipments.shipments_demand",
        "shipments_demand",
        "modules_shipments.shipments_reports",  # на всякий случай
        "shipments_reports",
    ],
    function_names=[
        "demand_text",
        "demand_by_sku_text",
        "need_by_sku_text",
        "warehouse_demand_text",
        "demand_report_text",
        "demand_report",
        "report_text",
        "text",
    ],
)


def _call_demand_text() -> Optional[str]:
    if not _DEMAND_TEXT_FN:
        return None
    for kwargs in ({}, {"view": "sku"}):
        try:
            t = _DEMAND_TEXT_FN(**kwargs)  # type: ignore
            if t and isinstance(t, str) and t.strip():
                return t
        except TypeError:
            continue
        except Exception:
            break
    return None


# ─────────────────────────────────────────────────────────────────────────────
# «Сроки доставки / Lead time» (best‑effort, разные пакеты)
# ─────────────────────────────────────────────────────────────────────────────
_DELIVERY_TEXT_FN: Optional[Callable[..., str]] = _resolve_text_function(
    module_names=[
        # приоритет — stats‑вариант
        "modules_shipments.shipments_leadtime_stats",
        "shipments_leadtime_stats",
        # альтернативные варианты
        "modules_shipments.shipments_leadtime_stats_data",
        "shipments_leadtime_stats_data",
        "modules_shipments.shipments_leadtime",
        "shipments_leadtime",
        "modules_logistics.delivery_stats",
        "modules_shipments.delivery_kpi",
    ],
    function_names=[
        "leadtime_stats_text",
        "delivery_stats_text",
        "lead_stats_text",
        "stats_text",
        "report_text",
        "leadtime_text",
        "leadtime_report_text",
    ],
)

# ── эвристика: есть ли строки по SKU (буллеты 🔹) в тексте лидинга
_SKU_LINE_RE = re.compile(r"^\s*🔹\s", re.M)


def _leadtime_has_sku_rows(text: Optional[str]) -> bool:
    return bool(text and _SKU_LINE_RE.search(text))


def _call_delivery_stats_text() -> Optional[str]:
    """
    Пытаемся получить именно «Статистика сроков доставки по SKU»:
    пробуем разные имена функций и сигнатуры (view/group_by/by/scope = 'sku'),
    а также разные имена параметра «период». Если текст без строк по SKU —
    пытаемся следующую комбинацию аргументов.
    """
    if not _DELIVERY_TEXT_FN:
        return None

    # Сначала явно SKU‑режимы, затем прочее
    kwargs_list = [
        {"view": "sku"},
        {"scope": "sku"},
        {"group_by": "sku"},
        {"by": "sku"},
        {"view": "sku", "days": 360},
        {"view": "sku", "period_days": 360},
        {"view": "sku", "lookback_days": 360},
        {"days": 360},
        {"period_days": 360},
        {"lookback_days": 360},
        {},  # в самом конце — дефолт
    ]

    first_txt: Optional[str] = None
    for kwargs in kwargs_list:
        try:
            t = _DELIVERY_TEXT_FN(**kwargs)  # type: ignore
            if not (t and isinstance(t, str) and t.strip()):
                continue
            # Если это кластерный отчёт — продолжаем искать SKU‑вариант
            if "Кластеры:" in t and not re.search(r"\bSKU\b|\bАртикул\b", t, re.I):
                if first_txt is None:
                    first_txt = t
                continue
            # Если тело без строк по SKU — помечаем как кандидат и продолжаем
            if not _leadtime_has_sku_rows(t):
                if first_txt is None:
                    first_txt = t
                continue
            return t
        except TypeError:
            continue
        except Exception:
            break
    return first_txt


# Метрики «Конверсия/CTR»
_TRAFFIC_TEXT_FN: Optional[Callable[..., str]] = _resolve_text_function(
    module_names=["modules_sales.sales_traffic", "sales_traffic"], function_names=["traffic_text"]
)
_CONV_TEXT_FN: Optional[Callable[..., str]] = _resolve_text_function(
    module_names=["modules_marketing.metrics_reports", "modules_traffic.metrics_reports"],
    function_names=["conversion_text"],
)
_CTR_TEXT_FN: Optional[Callable[..., str]] = _resolve_text_function(
    module_names=["modules_marketing.metrics_reports", "modules_traffic.metrics_reports"],
    function_names=["ctr_text"],
)

# Автопрогрев demand‑кэша (если доступен)
try:
    from handlers.handlers_shipments_demand import REGISTER_WARMUP_JOB  # type: ignore
except Exception:
    REGISTER_WARMUP_JOB = None  # type: ignore

# ─────────────────────────────────────────────────────────────────────────────
log = logging.getLogger("scheduler")

# === Конфиг (.env читаем относительно файла)
# (CFG уже инициализирован выше)

# === Кому слать (поддерживаем отрицательные id супергрупп/каналов)


def _parse_chat_ids(raw: str) -> List[int]:
    tokens = findall(r"-?\d+", raw or "")
    out: List[int] = []
    for t in tokens:
        try:
            v = int(t)
            if v != 0 and v not in out:
                out.append(v)
        except Exception:
            continue
    return out


CHAT_IDS: List[int] = _parse_chat_ids(CFG.get("CHAT_IDS", ""))

_VALID_CIDS: Set[int] = set()
_INVALID_CIDS: Set[int] = set()

# === Таймзона
TZ_NAME = CFG.get("TZ", "Europe/Moscow")
if TZ_NAME:
    os.environ["TZ"] = TZ_NAME
    try:
        time.tzset()
    except Exception:
        pass


# === Парсер HH:MM
def _parse_hhmm(s: str, default: Tuple[int, int]) -> dtime:
    try:
        hh, mm = s.split(":")
        return dtime(hour=int(hh), minute=int(mm))
    except Exception:
        return dtime(*default)


# === Дайджесты (краткий/полный) и пауза между сообщениями
WEEKDAY_DIGEST_T = _parse_hhmm(CFG.get("DAILY_NOTICES_WEEKDAY_AT", "08:45"), (8, 45))  # будни, утро
WEEKEND_DIGEST_T = _parse_hhmm(
    CFG.get("DAILY_NOTICES_WEEKEND_AT", "10:00"), (10, 0)
)  # выходные, утро
WEEKDAY_DIGEST_PM_T = _parse_hhmm(
    CFG.get("DAILY_NOTICES_WEEKDAY_PM_AT", "17:45"), (17, 45)
)  # будни, вечер
WEEKEND_DIGEST_PM_T = _parse_hhmm(
    CFG.get("DAILY_NOTICES_WEEKEND_PM_AT", "17:45"), (17, 45)
)  # выходные, вечер
FULL_DIGEST_WEEKDAY_T = _parse_hhmm(
    CFG.get("FULL_DIGEST_WEEKDAY_AT", "10:00"), (10, 0)
)  # полный, будни

SPREAD_SEC = max(0, int(CFG.get("NOTIFY_SPREAD_SEC", "8")))

# === Excel путь (для напоминания — встраиваем в ПОЛНЫЙ дайджест)
XLSX_NAME = CFG.get("PURCHASES_XLSX_NAME", "Товары.xlsx")
XLSX_PATH = os.path.join(DATA_DIR, XLSX_NAME)

# === «Предпочитаем локальный чат»
NOTICES_PREFER_LOCAL = CFG.get("NOTICES_PREFER_LOCAL", "1").strip().lower() in ("1", "true", "yes")
NOTICE_TARGET_FILE = os.path.join(CACHE_COMMON_DIR, "notice_target_chat.json")


# ===== helpers: управление получателями ======================================
def _read_notice_chat() -> Optional[int]:
    try:
        if os.path.exists(NOTICE_TARGET_FILE):
            with open(NOTICE_TARGET_FILE, "r", encoding="utf-8") as f:
                payload = json.load(f) or {}
                cid = int(payload.get("chat_id") or 0)
                return cid if cid != 0 else None
    except Exception:
        pass
    return None


def register_notice_chat(chat_id: int) -> None:
    try:
        with open(NOTICE_TARGET_FILE, "w", encoding="utf-8") as f:
            json.dump(
                {"chat_id": int(chat_id), "saved_at": datetime.now().isoformat()},
                f,
                ensure_ascii=False,
                indent=2,
            )
    except Exception:
        pass


_LOCAL_OVERRIDE_CHAT_ID: Optional[int] = None


async def _ensure_chat(bot: Bot, cid: int) -> bool:
    if cid in _VALID_CIDS:
        return True
    if cid in _INVALID_CIDS:
        return False
    try:
        await bot.get_chat(cid)
        _VALID_CIDS.add(cid)
        return True
    except Exception as e:
        _INVALID_CIDS.add(cid)
        log.warning(f"drop recipient {cid}: {e}")
        return False


def _recipients(override_chat_id: Optional[int] = None) -> List[int]:
    if override_chat_id is not None:
        return [override_chat_id]
    if NOTICES_PREFER_LOCAL:
        saved = _read_notice_chat()
        if saved:
            return [saved]
    return list(CHAT_IDS)


async def _send_text(bot: Bot, text: str, *, to_chat_id: Optional[int] = None):
    if not text:
        return
    recipients = _recipients(to_chat_id if to_chat_id is not None else _LOCAL_OVERRIDE_CHAT_ID)
    if not recipients:
        return
    for cid in recipients:
        if not await _ensure_chat(bot, cid):
            continue
        try:
            await bot.send_message(cid, text)
        except TelegramForbiddenError as e:
            log.warning(f"drop recipient {cid}: {e}")
            _INVALID_CIDS.add(cid)
        except TelegramBadRequest as e:
            if "chat not found" in str(e).lower():
                log.warning(f"drop recipient {cid}: chat not found")
                _INVALID_CIDS.add(cid)
            else:
                log.warning(f"send →{cid} failed: {e}")
        except Exception as e:
            log.warning(f"send →{cid} failed: {e}")


def _fmt_dt(dt_: datetime) -> str:
    return dt_.strftime("%d.%m.%Y %H:%M")


def _days_ago(dt_: datetime) -> int:
    return max((datetime.now() - dt_).days, 0)


# ── Проверки «пустого» отчёта (расширены под эмодзи/без буллетов) ────────────
_BULLET_RE = re.compile(
    r"^\s*(?:[•\-—\*·▪–►▶●○◆◇■□◼◻◾◽]|[🟥🟧🟨🟩🟦🟪🟫🔴🟠🟡🟢🔵🟣🟤✅🔹🔺🔻])\s|^\s{3,}\S", re.M
)
_SINGLE_DASH_LINE_RE = re.compile(r"^[\s—\-]+$", re.M)


def _has_bullets(text: str) -> bool:
    return bool(_BULLET_RE.search(text or ""))


def _looks_placeholder(text: str) -> bool:
    t = text or ""
    t_low = t.lower().strip()
    if not t_low:
        return True
    if ("раздел рекомендаций будет обновлён" in t_low) or (
        "раздел рекомендаций будет обновлен" in t_low
    ):
        return True
    nonempty = [ln.strip() for ln in t.splitlines() if ln.strip()]
    return bool(nonempty) and all(_SINGLE_DASH_LINE_RE.fullmatch(ln) for ln in nonempty)


def _has_content_signals(text: str) -> bool:
    t = text or ""
    if len(t.strip()) < 8:
        return False
    if re.search(r"^\s*\d+[\.\)]\s+", t, re.M):
        return True
    if re.search(
        r"(шт|₽|руб|→|Необходимо|Профицит|Дефицит|Отгрузки|План|Факт|склад|остатк|Рекомендации)",
        t,
        re.I,
    ):
        return True
    if sum(1 for ln in t.splitlines() if ln.strip()) >= 3 and len(t) > 100:
        return True
    return False


def _is_effectively_empty(text: Optional[str]) -> bool:
    if not text or not text.strip():
        return True
    if _looks_placeholder(text):
        return True
    if _has_bullets(text):
        return False
    return not _has_content_signals(text)


# ── Дополнительно: более мягкая проверка для отгрузок/потребности ────────────
def _is_relaxed_nonempty(text: Optional[str]) -> bool:
    if not text:
        return False
    if _looks_placeholder(text):
        return False
    nonempty = [ln for ln in (text or "").splitlines() if ln.strip()]
    return len(nonempty) >= 2 or len((text or "").strip()) >= 60


# ── Нормализация заголовков блоков рекомендаций (Достаточно/Дефицит/Профицит) ─
def _normalize_reco_headers(text: Optional[str]) -> str:
    """
    Приводим заголовки статусов к единому виду:
    «✅ Достаточно», «🔻 Дефицит», «🔺 Профицит».

    Дополнительно следим, чтобы между заголовком статуса и строкой
    «• Нет позиций в статусе …» всегда была одна пустая строка‑разделитель.
    Работает только по строкам, где заголовок стоит один.
    """
    if not text:
        return text or ""

    t = text

    # Достаточно
    t = re.sub(
        r"(?m)^(?P<indent>\s*)Достаточно\s*$",
        r"\g<indent>✅ Достаточно",
        t,
    )
    # Дефицит
    t = re.sub(
        r"(?m)^(?P<indent>\s*)Дефицит\s*$",
        r"\g<indent>🔻 Дефицит",
        t,
    )
    # Профицит
    t = re.sub(
        r"(?m)^(?P<indent>\s*)Профицит\s*$",
        r"\g<indent>🔺 Профицит",
        t,
    )

    # Гарантируем пустую строку между заголовком статуса и строкой
    # «• Нет позиций в статусе …», если заголовок идёт сразу перед ней.
    lines = t.split("\n")
    out_lines: List[str] = []
    for i, line in enumerate(lines):
        out_lines.append(line)
        stripped = line.strip()
        if stripped in {"✅ Достаточно", "🔻 Дефицит", "🔺 Профицит"}:
            if i + 1 < len(lines):
                next_line = lines[i + 1]
                # уже есть пустая строка — ничего не делаем
                if next_line.strip() == "":
                    continue
                # если сразу после заголовка идёт «нет позиций» — вставляем разделитель
                if next_line.lstrip().startswith("• Нет позиций в статусе"):
                    out_lines.append("")
    return "\n".join(out_lines)


# ── Удаление «(vs 30д ...)» из текста CTR/CR ─────────────────────────────────
_VS_ANY_RE = re.compile(r"\s*\(\s*vs\s*30[^)]*\)\s*", re.IGNORECASE)


def _strip_vs_suffix(s: Optional[str]) -> Optional[str]:
    if not s:
        return s
    return _VS_ANY_RE.sub("", s)


async def _send_notice(bot: Bot, text: str, reorder_by_env: bool = True):
    if not text:
        return
    await _send_text(bot, _reorder_text_by_env_sku_order(text) if reorder_by_env else text)


# ── SKU‑сортировка по порядку .env ───────────────────────────────────────────
def _sku_env_order() -> Dict[int, int]:
    order: Dict[int, int] = {}
    seq = 0
    for k, v in CFG.items():
        for source in (k, v):
            if not source:
                continue
            for tok in findall(r"\b\d{5,}\b", str(source)):
                try:
                    sku = int(tok)
                    if sku not in order:
                        order[sku] = seq
                        seq += 1
                except Exception:
                    continue
    return order


_ENV_SKU_ORDER = _sku_env_order()


def _reorder_text_by_env_sku_order(text: str) -> str:
    if not text or not _ENV_SKU_ORDER:
        return text

    lines = text.splitlines()
    bullets: List[Tuple[int, int, str]] = []
    nonbullets: List[Tuple[int, str]] = []

    def _is_bullet(s: str) -> bool:
        s = s.lstrip()
        return bool(
            re.match(r"^(?:[•\-—\*·▪–►▶●○◆◇■□◼◻◾◽]|[🟥🟧🟨🟩🟦🟪🟫🔴🟠🟡🟢🔵🟣🟤✅🔹🔺🔻])\s", s)
        )

    for i, line in enumerate(lines):
        if _is_bullet(line):
            idx: Optional[int] = None
            for tok in findall(r"\b\d{5,}\b", line):
                sku = int(tok)
                if sku in _ENV_SKU_ORDER:
                    pos = _ENV_SKU_ORDER[sku]
                    idx = pos if idx is None else min(idx, pos)
            if idx is not None:
                bullets.append((idx, i, line))
                continue
        nonbullets.append((i, line))

    if not bullets:
        return text

    bullets_sorted = [ln for _, _, ln in sorted(bullets, key=lambda t: (t[0], t[1]))]
    first_bullet_idx = min(i for _, i, _ in bullets)
    header = [ln for i, ln in nonbullets if i < first_bullet_idx]
    tail = [ln for i, ln in nonbullets if i >= first_bullet_idx]
    return "\n".join(header + bullets_sorted + tail)


# ===== УВЕДОМЛЕНИЯ ===========================================================


# 1–3. Факты за вчера (юниты/выручка/средний чек)
async def _notice_fact_units_yday(bot: Bot):
    txt = facts_text(period_days=1, metric="units")
    if _is_effectively_empty(txt):
        await _send_text(bot, "📄 Факт продаж — ЮНИТЫ\nЗа вчера нет данных по юнитам.")
    else:
        await _send_notice(bot, txt)


async def _notice_fact_revenue_yday(bot: Bot):
    try:
        txt = facts_text(period_days=1, metric="revenue")
        if _is_effectively_empty(txt):
            await _send_text(bot, "📄 Факт продаж — ВЫРУЧКА\nЗа вчера нет данных по выручке.")
        else:
            await _send_notice(bot, txt)
    except Exception:
        await _send_text(
            bot, "📄 Факт продаж — ВЫРУЧКА\nМетрика недоступна (модуль не поддерживает)."
        )


async def _notice_fact_avgcheck_yday(bot: Bot):
    try:
        txt = facts_text(period_days=1, metric="avg_price")
        if _is_effectively_empty(txt):
            await _send_text(
                bot,
                "📄 Факт продаж — СРЕДНИЙ ЧЕК\n" "За вчера нет данных для расчёта среднего чека.",
            )
        else:
            await _send_notice(bot, txt, reorder_by_env=False)
    except Exception:
        await _send_text(
            bot, "📄 Факт продаж — СРЕДНИЙ ЧЕК\nМетрика недоступна в текущем модуле фактов."
        )


# 4–5. Конверсия и Кликабельность (без «vs 30д»)
def _traffic_metric_text(metric: str) -> Optional[str]:
    if _TRAFFIC_TEXT_FN:
        for kwargs in (dict(period_days=1, metric=metric), dict(metric=metric), dict()):
            try:
                txt = _TRAFFIC_TEXT_FN(**kwargs)  # type: ignore
                if txt and txt.strip():
                    txt = _strip_vs_suffix(txt)
                    if metric == "ctr":
                        txt = re.sub(r"\bCTR\b", "Кликабельность", txt, flags=re.IGNORECASE)
                    return txt
            except TypeError:
                continue
            except Exception:
                break
    if metric == "cvr" and _CONV_TEXT_FN:
        for kwargs in (
            dict(period_days=1),
            dict(),
        ):
            try:
                txt = _CONV_TEXT_FN(**kwargs)  # type: ignore
                if txt and txt.strip():
                    return _strip_vs_suffix(txt)
            except TypeError:
                continue
            except Exception:
                break
    if metric == "ctr" and _CTR_TEXT_FN:
        for kwargs in (
            dict(period_days=1),
            dict(),
        ):
            try:
                txt = _CTR_TEXT_FN(**kwargs)  # type: ignore
                if txt and txt.strip():
                    txt = _strip_vs_suffix(txt)
                    txt = re.sub(r"\bCTR\b", "Кликабельность", txt, flags=re.IGNORECASE)
                    return txt
            except TypeError:
                continue
            except Exception:
                break
    return None


async def _notice_conversion_yday(bot: Bot):
    txt = _traffic_metric_text("cvr")
    if not txt:
        await _send_text(
            bot,
            "📈 Конверсия — недоступно.\nПодключите modules_sales.sales_traffic.traffic_text().",
        )
    elif _is_effectively_empty(txt):
        await _send_text(bot, "📈 Конверсия — за вчера данных нет.")
    else:
        await _send_notice(bot, txt, reorder_by_env=False)


async def _notice_ctr_yday(bot: Bot):
    txt = _traffic_metric_text("ctr")
    if not txt:
        await _send_text(
            bot,
            "🖱 Кликабельность — недоступно.\nПодключите modules_sales.sales_traffic.traffic_text().",
        )
    elif _is_effectively_empty(txt):
        await _send_text(bot, "🖱 Кликабельность — за вчера данных нет.")
    else:
        await _send_notice(bot, txt, reorder_by_env=False)


# 6–8. План на 30 дней (юниты/выручка/ср. чек)
async def _notice_plan_units_30d(bot: Bot):
    try:
        txt = forecast_text(period_days=30)
        if _is_effectively_empty(txt):
            await _send_text(bot, "📈 План 30д — ЮНИТЫ\nНет данных для расчёта.")
        else:
            await _send_notice(bot, txt)
    except Exception as e:
        await _send_text(bot, f"📈 План 30д — ЮНИТЫ\nОшибка: {e}")


async def _notice_plan_revenue_30d(bot: Bot):
    try:
        txt = forecast_text(period_days=30, metric="revenue")  # type: ignore
        if _is_effectively_empty(txt):
            await _send_text(bot, "📈 План 30д — ВЫРУЧКА\nНет данных для расчёта.")
        else:
            await _send_notice(bot, txt)
    except TypeError:
        await _send_text(bot, "📈 План 30д — ВЫРУЧКА\nМетрика не поддерживается модулем прогноза.")
    except Exception as e:
        await _send_text(bot, f"📈 План 30д — ВЫРУЧКА\nОшибка: {e}")


async def _notice_plan_avgcheck_30d(bot: Bot):
    try:
        txt = forecast_text(period_days=30, metric="avg_price")  # type: ignore
        if _is_effectively_empty(txt):
            await _send_text(
                bot, "📄 План продаж — СРЕДНИЙ ЧЕК\nМетрика недоступна или нет данных для расчёта."
            )
        else:
            await _send_notice(bot, txt, reorder_by_env=False)
    except TypeError:
        await _send_text(
            bot, "📄 План продаж — СРЕДНИЙ ЧЕК\nМетрика не поддерживается модулем прогноза."
        )
    except Exception as e:
        await _send_text(bot, f"📄 План продаж — СРЕДНИЙ ЧЕК\nОшибка: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# 9. Необходимо закупить — теперь учитываем «цели продаж» (если поддерживается)
# ─────────────────────────────────────────────────────────────────────────────

# РЕЗОЛВЕР функции выкупов (меньше рисков, чем жёсткий импорт из легаси‑модуля)
_NEED_PURCHASE_FN: Optional[Callable[..., str]] = _resolve_text_function(
    module_names=[
        # приоритет: пакетный прокси (внутри — purchases_need)
        "modules_purchases",
        "modules_purchases.purchases_need",  # прямой вызов новой реализации
        "modules_purchases.purchases_reports",  # легаси-агрегатор (если остался)
    ],
    function_names=["need_to_purchase_text"],
)


def _call_need_to_purchase_text(use_goal: bool = False) -> Optional[str]:
    """
    Пытаемся вызвать need_to_purchase_text с goal‑аргументами.
    Если текущая реализация не принимает такие аргументы — пробуем другой набор или дефолт.
    """
    # Если по какой-то причине резолвер не нашёлся — пробуем прямые импорты.
    if _NEED_PURCHASE_FN is None:
        try:
            from modules_purchases import need_to_purchase_text as _fallback_pkg  # type: ignore

            return _fallback_pkg(use_goal=True) if use_goal else _fallback_pkg()
        except Exception:
            try:
                from modules_purchases.purchases_need import need_to_purchase_text as _fallback_new  # type: ignore

                return _fallback_new(use_goal=True) if use_goal else _fallback_new()
            except Exception:
                try:
                    from modules_purchases.purchases_reports import need_to_purchase_text as _fallback_legacy  # type: ignore

                    return _fallback_legacy()
                except Exception:
                    return None

    goal_kw_candidates: List[Dict[str, object]] = [
        {"use_goal": True},
        {"use_sales_goal": True},
        {"goal": True},
        {"target": "goal"},
        {"mode": "goal"},
        {"use_targets": True},
        {"use_sales_targets": True},
    ]
    # Сначала — попытки с goal, затем — без
    param_sets: List[Dict[str, object]] = []
    if use_goal:
        param_sets.extend(goal_kw_candidates)
    param_sets.append({})

    for kwargs in param_sets:
        try:
            txt = _NEED_PURCHASE_FN(**kwargs)  # type: ignore
            if isinstance(txt, str) and txt.strip():
                return txt
        except TypeError:
            # текущая реализация не принимает такие kwargs — пробуем следующий набор
            continue
        except Exception:
            continue
    return None


async def _notice_need_to_purchase(bot: Bot):
    txt = _call_need_to_purchase_text(use_goal=OPERATIONS_USE_SALES_GOAL) or ""
    txt = _normalize_reco_headers(txt)
    if _is_effectively_empty(txt):
        if not os.path.exists(XLSX_PATH):
            await _send_text(
                bot,
                "🏷️ Необходимо закупить\nФайл <b>Товары.xlsx</b> не найден. "
                "Загрузите его через <code>/data</code>, чтобы обновить данные по выкупам.",
            )
        else:
            # Приведено к единому формату «пустых» статусов
            now = datetime.now().strftime("%d.%m.%Y %H:%M")
            empty = "\n".join(
                [
                    "📊 ЗАКУПКИ — РЕКОМЕНДАЦИИ",
                    f"⏱ Обновлено: {now}",
                    "",
                    "✅ Достаточно",
                    "",
                    "• Нет позиций в статусе «Достаточно».",
                    "",
                    "🔻 Дефицит",
                    "",
                    "• Нет позиций в статусе «Дефицит».",
                    "",
                    "🔺 Профицит",
                    "",
                    "• Нет позиций в статусе «Профицит».",
                ]
            )
            await _send_notice(bot, empty, reorder_by_env=False)
    else:
        # Для выкупов сохраняем исходное reorder_by_env=False (как было)
        await _send_notice(bot, txt, reorder_by_env=False)


# ─────────────────────────────────────────────────────────────────────────────
# 10. Необходимо отгрузить — с учётом «цели продаж» (если поддерживается)
# ─────────────────────────────────────────────────────────────────────────────
async def _notice_need_to_ship(bot: Bot):
    _shipments_best_effort_warmup()
    txt = _call_need_to_ship_text(use_goal=OPERATIONS_USE_SALES_GOAL) or ""
    txt = _normalize_reco_headers(txt)
    if _is_effectively_empty(txt) and not _is_relaxed_nonempty(txt):
        _shipments_best_effort_warmup()
        txt = _call_need_to_ship_text(use_goal=OPERATIONS_USE_SALES_GOAL) or ""
        txt = _normalize_reco_headers(txt)
    if _is_effectively_empty(txt) and not _is_relaxed_nonempty(txt):
        # Единый формат «пустых» статусов для блока рекомендаций отгрузок
        now = datetime.now().strftime("%d.%m.%Y %H:%M")
        empty = "\n".join(
            [
                "📊 ОТГРУЗКИ — РЕКОМЕНДАЦИИ",
                f"⏱ Обновлено: {now}",
                "",
                "✅ Достаточно",
                "",
                "• Нет позиций в статусе «Достаточно».",
                "",
                "🔻 Дефицит",
                "",
                "• Нет позиций в статусе «Дефицит».",
                "",
                "🔺 Профицит",
                "",
                "• Нет позиций в статусе «Профицит».",
            ]
        )
        await _send_notice(bot, empty)
    else:
        await _send_notice(bot, txt)


# 11. Потребность по SKU
async def _notice_demand_by_sku(bot: Bot):
    _shipments_best_effort_warmup()
    txt = _call_demand_text() or ""
    if _is_effectively_empty(txt) and not _is_relaxed_nonempty(txt):
        _shipments_best_effort_warmup()
        txt = _call_demand_text() or ""
    if _is_effectively_empty(txt) and not _is_relaxed_nonempty(txt):
        await _send_text(bot, "🏬 Потребность по SKU — модуль не подключён или данных нет.")
    else:
        await _send_notice(bot, txt)


# 12. Сроки доставки
async def _notice_delivery_stats(bot: Bot):
    _shipments_best_effort_warmup()
    txt = _call_delivery_stats_text()

    # Если текста нет — ещё одна попытка после прогрева
    if txt is None:
        _shipments_best_effort_warmup()
        txt = _call_delivery_stats_text()

    # Если текст есть, но НЕТ строк по SKU — пробуем «автовосстановление» и ещё раз
    if txt and not _leadtime_has_sku_rows(txt):
        try:
            from modules_shipments.shipments_leadtime_stats import (  # type: ignore
                rebuild_events_from_states,
                invalidate_stats_cache,
                leadtime_stats_text,
            )

            try:
                rebuild_events_from_states()
            except Exception:
                pass
            try:
                invalidate_stats_cache()
            except Exception:
                pass
            txt2 = leadtime_stats_text(view="sku")
            if txt2 and _leadtime_has_sku_rows(txt2):
                txt = txt2
        except Exception:
            pass

    if txt is None:
        await _send_text(bot, "⏱ Сроки доставки — модуль не подключён или данных нет.")
    else:
        # Сохраняем порядок, сформированный отчётом (не переупорядочиваем по .env)
        await _send_notice(bot, txt, reorder_by_env=False)


# 13. «Цель продаж»: Необходимо заработать / продать (30д)
async def _notice_goal_revenue_30d(bot: Bot):
    txt = sales_goal_report_text(horizon_days=30, metric="revenue")
    await _send_notice(bot, txt, reorder_by_env=False)


async def _notice_goal_units_30d(bot: Bot):
    txt = sales_goal_report_text(horizon_days=30, metric="units")
    await _send_notice(bot, txt, reorder_by_env=False)


# ===== ЕЖЕНЕДЕЛЬНОЕ напоминание — только вручную/в составе ПОЛНОГО дайджеста ===
async def _job_weekly_seller_reminder(bot: Bot):
    if not os.path.exists(XLSX_PATH):
        await _send_text(
            bot,
            "🗒 Напоминание (выкупы): файл <b>Товары.xlsx</b> не найден.\n"
            "Загрузите его через <code>/data</code>, чтобы обновить данные модуля «Выкупы».",
        )
        return
    mtime = datetime.fromtimestamp(os.path.getmtime(XLSX_PATH))
    days = _days_ago(mtime)
    if days >= 7:
        await _send_text(
            bot,
            "🗒 Напоминание (выкупы): файл <b>Товары.xlsx</b> давно не обновлялся.\n"
            f"Последнее обновление: {_fmt_dt(mtime)} (прошло {days} дн.)\n"
            "Загрузите свежий через <code>/data</code>.",
        )
    else:
        await _send_text(
            bot,
            "🗒 Напоминание (выкупы): проверьте актуальность <b>Товары.xlsx</b>.\n"
            f"Последнее обновление: {_fmt_dt(mtime)}.",
        )


# ===== /notice: экспорт доступных уведомлений ================================
NOTICE_REGISTRY: "OrderedDict[str, Callable[[Bot], asyncio.Future]]" = OrderedDict(
    [
        # Новые «цели продаж»
        ("goal_revenue_30d", _notice_goal_revenue_30d),
        ("goal_units_30d", _notice_goal_units_30d),
        # План/факт/трафик
        ("plan_units_30d", _notice_plan_units_30d),
        ("fact_units_yday", _notice_fact_units_yday),
        ("plan_revenue_30d", _notice_plan_revenue_30d),
        ("fact_revenue_yday", _notice_fact_revenue_yday),
        ("plan_avgcheck_30d", _notice_plan_avgcheck_30d),
        ("fact_avgcheck_yday", _notice_fact_avgcheck_yday),
        ("ctr_yday", _notice_ctr_yday),
        ("conversion_yday", _notice_conversion_yday),
        # Операционка
        ("need_to_purchase", _notice_need_to_purchase),
        ("need_to_ship", _notice_need_to_ship),
        ("demand_by_sku", _notice_demand_by_sku),
        ("delivery_stats", _notice_delivery_stats),
    ]
)


# ===== Вспомогательные раннеры ===============================================
async def run_notice(bot: Bot, name: str, chat_id: Optional[int] = None) -> bool:
    fn = NOTICE_REGISTRY.get(name)
    if not fn:
        return False
    global _LOCAL_OVERRIDE_CHAT_ID
    prev = _LOCAL_OVERRIDE_CHAT_ID
    _LOCAL_OVERRIDE_CHAT_ID = chat_id
    try:
        await fn(bot)
    finally:
        _LOCAL_OVERRIDE_CHAT_ID = prev
    return True


async def send_seller_reminder(bot: Bot, chat_id: Optional[int] = None) -> None:
    global _LOCAL_OVERRIDE_CHAT_ID
    prev = _LOCAL_OVERRIDE_CHAT_ID
    _LOCAL_OVERRIDE_CHAT_ID = chat_id
    try:
        await _job_weekly_seller_reminder(bot)
    finally:
        _LOCAL_OVERRIDE_CHAT_ID = prev


# ===== Наборы для дайджестов =================================================
# Сокращённый: цели + выкупы + ОТГРУЗКИ
_SHORT_DIGEST_CODES: List[str] = [
    "goal_revenue_30d",
    "goal_units_30d",
    "need_to_purchase",
    "need_to_ship",
]


def _flatten_notice_order() -> List[str]:
    # в качестве базового порядка используем NOTICE_REGISTRY
    return list(NOTICE_REGISTRY.keys())


def _codes_for_full_digest() -> List[str]:
    # Полный — «всё, кроме» коротких позиций (включая исключение «Необходимо отгрузить»),
    # напоминание об Excel добавим отдельным вызовом в конце.
    exclude = set(_SHORT_DIGEST_CODES)
    ordered = _flatten_notice_order()
    seen: Set[str] = set()
    out: List[str] = []
    for code in ordered:
        if code in exclude:
            continue
        if code not in NOTICE_REGISTRY:
            continue
        if code in seen:
            continue
        seen.add(code)
        out.append(code)
    return out


async def _job_morning_digest_short(bot: Bot, reason: str = ""):
    _shipments_best_effort_warmup()
    for code in _SHORT_DIGEST_CODES:
        try:
            await run_notice(bot, code)
        except Exception as e:
            log.warning(f"[digest:short] {code} failed: {e}")
        if SPREAD_SEC > 0:
            await asyncio.sleep(SPREAD_SEC)


async def _job_morning_digest_full(bot: Bot, reason: str = ""):
    _shipments_best_effort_warmup()
    for code in _codes_for_full_digest():
        try:
            await run_notice(bot, code)
        except Exception as e:
            log.warning(f"[digest:full] {code} failed: {e}")
        if SPREAD_SEC > 0:
            await asyncio.sleep(SPREAD_SEC)
    # В конце — напоминание об Excel (выкупы)
    try:
        if SPREAD_SEC > 0:
            await asyncio.sleep(SPREAD_SEC)
        await _job_weekly_seller_reminder(bot)
    except Exception as e:
        log.warning(f"[digest:full] seller reminder failed: {e}")


# Публичные вызовы дайджестов (для кнопок)
async def send_digest_short(bot: Bot, chat_id: Optional[int] = None) -> None:
    global _LOCAL_OVERRIDE_CHAT_ID
    prev = _LOCAL_OVERRIDE_CHAT_ID
    _LOCAL_OVERRIDE_CHAT_ID = chat_id
    try:
        await _job_morning_digest_short(bot, reason="manual")
    finally:
        _LOCAL_OVERRIDE_CHAT_ID = prev


async def send_digest_full(bot: Bot, chat_id: Optional[int] = None) -> None:
    global _LOCAL_OVERRIDE_CHAT_ID
    prev = _LOCAL_OVERRIDE_CHAT_ID
    _LOCAL_OVERRIDE_CHAT_ID = chat_id
    try:
        await _job_morning_digest_full(bot, reason="manual")
    finally:
        _LOCAL_OVERRIDE_CHAT_ID = prev


# ===== запуск ================================================================
async def scheduler_start(bot: Bot) -> AsyncIOScheduler:
    tz = timezone(TZ_NAME)
    sched = AsyncIOScheduler(timezone=tz)

    try:
        if REGISTER_WARMUP_JOB is not None:
            REGISTER_WARMUP_JOB(sched)
        else:
            log.info("[demand:warmup] handler not available — skipped registration")
    except Exception as e:
        log.warning(f"[demand:warmup] registration failed: {e}")

    # Сокращённый дайджест — утро
    sched.add_job(
        _job_morning_digest_short,
        CronTrigger(
            day_of_week="mon-fri",
            hour=WEEKDAY_DIGEST_T.hour,
            minute=WEEKDAY_DIGEST_T.minute,
            timezone=tz,
        ),
        args=(bot, "weekday-short-am"),
        id="digest_weekday_short_am",
        replace_existing=True,
        coalesce=True,
        max_instances=1,
        misfire_grace_time=300,
    )
    sched.add_job(
        _job_morning_digest_short,
        CronTrigger(
            day_of_week="sat,sun",
            hour=WEEKEND_DIGEST_T.hour,
            minute=WEEKEND_DIGEST_T.minute,
            timezone=tz,
        ),
        args=(bot, "weekend-short-am"),
        id="digest_weekend_short_am",
        replace_existing=True,
        coalesce=True,
        max_instances=1,
        misfire_grace_time=300,
    )

    # Сокращённый дайджест — вечер
    sched.add_job(
        _job_morning_digest_short,
        CronTrigger(
            day_of_week="mon-fri",
            hour=WEEKDAY_DIGEST_PM_T.hour,
            minute=WEEKDAY_DIGEST_PM_T.minute,
            timezone=tz,
        ),
        args=(bot, "weekday-short-pm"),
        id="digest_weekday_short_pm",
        replace_existing=True,
        coalesce=True,
        max_instances=1,
        misfire_grace_time=300,
    )
    sched.add_job(
        _job_morning_digest_short,
        CronTrigger(
            day_of_week="sat,sun",
            hour=WEEKEND_DIGEST_PM_T.hour,
            minute=WEEKEND_DIGEST_PM_T.minute,
            timezone=tz,
        ),
        args=(bot, "weekend-short-pm"),
        id="digest_weekend_short_pm",
        replace_existing=True,
        coalesce=True,
        max_instances=1,
        misfire_grace_time=300,
    )

    # Полный дайджест — будни
    sched.add_job(
        _job_morning_digest_full,
        CronTrigger(
            day_of_week="mon-fri",
            hour=FULL_DIGEST_WEEKDAY_T.hour,
            minute=FULL_DIGEST_WEEKDAY_T.minute,
            timezone=tz,
        ),
        args=(bot, "weekday-full"),
        id="digest_weekday_full",
        replace_existing=True,
        coalesce=True,
        max_instances=1,
        misfire_grace_time=300,
    )

    # ВНИМАНИЕ: по требованию «Иные автоуведомления убирай»
    #  * НЕ планируем еженедельные напоминания отдельно
    #  * НЕ планируем legacy-ежедневные уведомления
    #  * НЕ планируем событийные проверки

    sched.start()
    return sched
