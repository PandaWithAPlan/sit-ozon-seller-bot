from __future__ import annotations

import os
import html as _html
import asyncio
from typing import Dict, List, Tuple, Set

from aiogram import Router
from aiogram.filters import Command
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.enums import ParseMode

import aiohttp

# ── где искать .env: сначала корень проекта, затем рядом с модулем ───────────
MOD_DIR = os.path.abspath(os.path.dirname(__file__))
ROOT_DIR = os.path.abspath(os.path.join(MOD_DIR, ".."))
ENV_PATH = os.path.join(ROOT_DIR, ".env")
if not os.path.exists(ENV_PATH):
    alt = os.path.join(MOD_DIR, ".env")
    if os.path.exists(alt):
        ENV_PATH = alt

router = Router(name="units")

TG_MAX = 4096
_PAGE_MAX = 3600  # безопасный размер одной страницы сообщения

# ─────────────────────────────────────────────────────────────────────────────
# Чтение .env (только для WATCH_OFFERS), без записи/изменений
# ─────────────────────────────────────────────────────────────────────────────


def _read_env_file(path: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if not os.path.exists(path):
        return out
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                s = line.rstrip("\n")
                if not s or s.lstrip().startswith("#") or "=" not in s:
                    continue
                k, v = s.split("=", 1)
                out[k.strip()] = v.strip()
    except Exception:
        # тихо возвращаем пустоту — команда /units не должна падать
        pass
    return out


def _watch_offers_from_env() -> Set[str]:
    env = _read_env_file(ENV_PATH)
    raw = env.get("WATCH_OFFERS", "") or os.getenv("WATCH_OFFERS", "") or ""
    # убираем пустые элементы и пробелы
    vals = {s.strip() for s in raw.split(",") if s.strip()}
    # экранируем HTML-символы, чтобы не ломать ParseMode.HTML
    return {_html.escape(v) for v in vals}


# ─────────────────────────────────────────────────────────────────────────────
# Источники офферов (необязательные; если нет доступа, покажем только из env)
# ─────────────────────────────────────────────────────────────────────────────


def _all_offers() -> List[str]:
    """
    Возвращает объединённый список офферов:
    • из WATCH_OFFERS;
    • по возможности — из OZON API /v3/product/list;
    • по возможности — из stocks(view='sku').
    Если доступов/зависимостей нет — вернём только то, что в WATCH_OFFERS.
    """
    offers: Set[str] = set(_watch_offers_from_env())

    # 1) OZON API (опционально)
    client_id = os.getenv("OZON_CLIENT_ID", "")
    api_key = os.getenv("OZON_API_KEY", "")
    if client_id and api_key:
        try:
            url = "https://api-seller.ozon.ru/v3/product/list"
            last_id = ""
            async with aiohttp.ClientSession() as session:
                while True:
                    body = {"filter": {"visibility": "ALL"}, "last_id": last_id, "limit": 1000}
                    async with session.post(
                        url,
                        headers={
                            "Client-Id": client_id,
                            "Api-Key": api_key,
                            "Content-Type": "application/json",
                        },
                        json=body,
                    ) as r:
                        r.raise_for_status()
                        js = await r.json() or {}
                        items = (js.get("result") or {}).get("items") or []
                        for it in items:
                            off = str(it.get("offer_id") or "").strip()
                            if off:
                                offers.add(_html.escape(off))
                        last_id = str((js.get("result") or {}).get("last_id") or "")
                        if not items or not last_id:
                            break
        except Exception:
            # молча игнорируем — это вспомогательный источник
            pass

    # 2) stocks(view='sku') (опционально)
    try:
        from modules_shipments.shipments_data import fetch_stocks_view  # type: ignore

        for r in await fetch_stocks_view(view="sku", force=True) or []:
            name = (
                r.get("offer_id")
                or r.get("offer")
                or r.get("name")
                or r.get("sku_name")
                or r.get("product_name")
            )
            name = str(name).strip() if name is not None else ""
            if name:
                offers.add(_html.escape(name))
    except Exception:
        pass

    out = sorted(offers, key=str.lower)
    return out


# ─────────────────────────────────────────────────────────────────────────────
# Представление
# ─────────────────────────────────────────────────────────────────────────────


def _home_kb() -> InlineKeyboardMarkup:
    """Клавиатура с кнопкой «Домой»."""
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="🏠 Домой", callback_data="nav:home")]]
    )


def _title_block(total_found: int, total_selected: int) -> List[str]:
    env_hint = _html.escape(ENV_PATH) if ENV_PATH else "не найден"
    return [
        "🧩 <b>Юниты (WATCH_OFFERS)</b>",
        f"Всего обнаружено: <b>{total_found}</b> • В WATCH_OFFERS: <b>{total_selected}</b>",
        f"Файл .env: <code>{env_hint}</code>",
        "",
        "ℹ️ Как изменить список:",
        "• отредактируйте переменную <code>WATCH_OFFERS</code> в файле <code>.env</code>;",
        "• перезапустите бота, чтобы изменения применились.",
        "",
    ]


def _format_sections(offers: List[str], selected: Set[str]) -> List[str]:
    lines: List[str] = []
    sel_sorted = sorted(selected, key=str.lower)
    other_sorted = sorted(set(offers) - selected, key=str.lower)

    # Секция 1 — в WATCH_OFFERS
    lines.append("✅ <b>Уже в WATCH_OFFERS</b>")
    if sel_sorted:
        for off in sel_sorted:
            lines.append(f"☑ {off}")
    else:
        lines.append("— нет позиций —")
    lines.append("")

    # Секция 2 — доступные, но не отмеченные
    lines.append("▫️ <b>Доступны (не отмечены)</b>")
    if other_sorted:
        for off in other_sorted:
            lines.append(f"▫ {off}")
    else:
        lines.append("— нет позиций —")
    lines.append("")

    return lines


def _paginate_text(text: str, max_len: int = _PAGE_MAX) -> List[str]:
    """Нарезаем длинный текст на страницы, чтобы не превысить лимит Telegram."""
    if len(text) <= max_len:
        return [text]
    pages: List[str] = []
    cur: List[str] = []
    cur_len = 0
    for ln in text.splitlines():
        add = len(ln) + 1
        if cur and (cur_len + add) > max_len:
            pages.append("\n".join(cur))
            cur, cur_len = [ln], add
        else:
            cur.append(ln)
            cur_len += add
    if cur:
        pages.append("\n".join(cur))
    return pages


# ─────────────────────────────────────────────────────────────────────────────
# Хендлер (ТОЛЬКО ПОКАЗАТЬ)
# ─────────────────────────────────────────────────────────────────────────────


@router.message(Command("units"))
async def cmd_units(message: Message):
    selected = _watch_offers_from_env()
    offers = await _all_offers()

    head = _title_block(total_found=len(set(offers) | set(selected)), total_selected=len(selected))
    body = _format_sections(offers, selected)

    full_text = "\n".join(head + body)
    pages = _paginate_text(full_text)

    # выводим несколькими сообщениями при необходимости; кнопку «Домой» даём в последнем
    for i, page in enumerate(pages):
        kb = _home_kb() if i == (len(pages) - 1) else None
        await message.answer(page, parse_mode=ParseMode.HTML, reply_markup=kb)
