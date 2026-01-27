# modules_common/paths.py
from __future__ import annotations

import os
from typing import Optional

from dotenv import load_dotenv

# ── База ─────────────────────────────────────────────────────────────────────
# Файл расположен в seller-bot/modules_common/paths.py
# Поднимаемся к корню репозитория (seller-bot/)
BASE_DIR = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))

# Грузим .env из корня (повторный вызов load_dotenv безопасен)
load_dotenv(os.path.join(BASE_DIR, ".env"))

# ── Директории данных ────────────────────────────────────────────────────────
# Разрешаем переопределение DATA_DIR из окружения (по умолчанию BASE/data)
DATA_DIR = os.getenv("DATA_DIR", os.path.join(BASE_DIR, "data"))

CACHE_DIR = os.path.join(DATA_DIR, "cache")
CACHE_PUR = os.path.join(CACHE_DIR, "purchases")
CACHE_SHIP = os.path.join(CACHE_DIR, "shipments")
CACHE_SALES = os.path.join(CACHE_DIR, "sales")
CACHE_COMMON = os.path.join(CACHE_DIR, "common")

LOGS_DIR = os.path.join(DATA_DIR, "logs")
TMP_DIR = os.path.join(DATA_DIR, "tmp")

# ── Вспомогательные утилиты ─────────────────────────────────────────────────


def _is_writable_dir(path: str) -> bool:
    """
    Проверяем, можно ли писать в каталог: пытаемся создать и удалить временный файл.
    """
    try:
        os.makedirs(path, exist_ok=True)
        test_path = os.path.join(path, ".wtest")
        with open(test_path, "w", encoding="utf-8") as f:
            f.write("ok")
        os.remove(test_path)
        return True
    except Exception:
        return False


def resolve_reports_dir() -> str:
    """
    Выбираем каталог для отчётов по приоритетам:
        1) REPORTS_DIR из .env (если задан и доступен на запись)
        2) <DATA_DIR>/reports
        3) /tmp/seller-bot-reports
        4) <CWD>/data/reports
        5) Текущая директория "."
    Возвращает первый доступный для записи путь (создаёт при необходимости).
    """
    candidates = []

    env_dir = os.getenv("REPORTS_DIR")
    if env_dir:
        candidates.append(env_dir)

    candidates.append(os.path.join(DATA_DIR, "reports"))
    candidates.append("/tmp/seller-bot-reports")
    candidates.append(os.path.join(os.getcwd(), "data", "reports"))

    for d in candidates:
        if _is_writable_dir(d):
            return d

    # Последний шанс — текущая директория
    return "."


# ── Каталог отчётов (единая «истина») ───────────────────────────────────────
REPORTS_DIR = resolve_reports_dir()

# ── Файлы по ТЗ 5.0/5.1 ─────────────────────────────────────────────────────
# 1) Кэш управленческого «📊 Отчёта по продажам»
SALES_REPORT_CACHE = os.path.join(CACHE_SALES, "sales_report_cache.json")

# 2) Имя XLSX «📊 Отчёт по отгрузкам» (можно переопределить через .env)
#    В .env переменная SHIPMENTS_REPORT_XLSX должна содержать ИМЯ файла (не путь).
#    Полный путь собираем через REPORTS_DIR.
SHIPMENTS_REPORT_XLSX_NAME = os.getenv("SHIPMENTS_REPORT_XLSX", "shipments_report.xlsx")
SHIPMENTS_REPORT_XLSX = os.path.join(REPORTS_DIR, os.path.basename(SHIPMENTS_REPORT_XLSX_NAME))

# 3) Имя файла закупок (из .env, по умолчанию «Товары.xlsx»)
PURCHASES_XLSX_NAME = os.getenv("PURCHASES_XLSX_NAME", "Товары.xlsx")


# ── Хелперы путей ────────────────────────────────────────────────────────────
def purchases_xlsx_path(name: Optional[str] = None) -> str:
    """
    Полный путь к файлу Excel с закупками (Товары.xlsx).
    :param name: опционально переопределить имя файла.
    """
    fname = (name or PURCHASES_XLSX_NAME).strip() or "Товары.xlsx"
    return os.path.join(DATA_DIR, fname)


def sales_report_cache_path() -> str:
    """Полный путь к кэшу sales_report_cache.json."""
    return SALES_REPORT_CACHE


def shipments_report_xlsx_path(name: Optional[str] = None) -> str:
    """
    Полный путь к XLSX отчёта по отгрузкам.

    Если name не передан, берём имя из переменной окружения SHIPMENTS_REPORT_XLSX
    (или дефолт 'shipments_report.xlsx') и собираем путь на основе актуального
    REPORTS_DIR, определённого resolve_reports_dir().
    """
    base_name = (
        name or os.getenv("SHIPMENTS_REPORT_XLSX") or os.path.basename(SHIPMENTS_REPORT_XLSX)
    ).strip()
    base_name = os.path.basename(base_name) or "shipments_report.xlsx"
    return os.path.join(REPORTS_DIR, base_name)


def ensure_dirs() -> None:
    """
    Создать (если нет) все директории данных/кэшей/логов/отчётов.
    """
    for p in [
        DATA_DIR,
        CACHE_DIR,
        CACHE_PUR,
        CACHE_SHIP,
        CACHE_SALES,
        CACHE_COMMON,
        REPORTS_DIR,
        LOGS_DIR,
        TMP_DIR,
    ]:
        os.makedirs(p, exist_ok=True)


# ── Удобные алиасы для обратной совместимости ───────────────────────────────
DATA = DATA_DIR
CACHE = CACHE_DIR
REPORTS = REPORTS_DIR
TMP = TMP_DIR
LOGS = LOGS_DIR

__all__ = [
    "BASE_DIR",
    "DATA_DIR",
    "CACHE_DIR",
    "CACHE_PUR",
    "CACHE_SHIP",
    "CACHE_SALES",
    "CACHE_COMMON",
    "REPORTS_DIR",
    "LOGS_DIR",
    "TMP_DIR",
    "SALES_REPORT_CACHE",
    "SHIPMENTS_REPORT_XLSX_NAME",
    "SHIPMENTS_REPORT_XLSX",
    "PURCHASES_XLSX_NAME",
    "purchases_xlsx_path",
    "sales_report_cache_path",
    "shipments_report_xlsx_path",
    "resolve_reports_dir",
    "ensure_dirs",
    "DATA",
    "CACHE",
    "REPORTS",
    "TMP",
    "LOGS",
]
