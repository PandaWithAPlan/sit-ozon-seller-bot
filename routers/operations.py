from __future__ import annotations
import logging
from aiogram import Router, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import Command

from modules_operations.services import fetch_prices
from modules_operations.views import prices_report_text
from modules_common.ui import home_kb

prices_router = Router(name="operations")
log = logging.getLogger("seller-bot.prices_router")

def operations_menu() -> InlineKeyboardMarkup:
    kb = [
        [InlineKeyboardButton(text="🔄 Обновить", callback_data="ops:prices")],
        [InlineKeyboardButton(text="🏠 Домой", callback_data="nav:home")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=kb)

@prices_router.message(Command("prices"))
async def on_prices_cmd_correct(message: Message):
    msg = await message.answer("⏳ Загружаю текущие цены...")
    try:
        items = await fetch_prices()
        text = prices_report_text(items)
        await msg.edit_text(text, reply_markup=operations_menu())
    except Exception as e:
        log.error(f"Prices error: {e}")
        await msg.edit_text("❌ Ошибка при загрузке цен")

@prices_router.callback_query(F.data == "menu:prices")
async def on_prices_menu(cb: CallbackQuery):
    await cb.answer()
    msg = await cb.message.answer("⏳ Загружаю текущие цены...")
    try:
        items = await fetch_prices()
        text = prices_report_text(items)
        await msg.edit_text(text, reply_markup=operations_menu())
    except Exception as e:
        log.error(f"Prices menu error: {e}")
        await msg.edit_text("❌ Ошибка при загрузке цен")

@prices_router.callback_query(F.data == "ops:prices")
async def on_prices_refresh(cb: CallbackQuery):
    await cb.answer("Обновляю...")
    try:
        items = await fetch_prices()
        text = prices_report_text(items)
        await cb.message.edit_text(text, reply_markup=operations_menu())
    except Exception as e:
        log.error(f"Prices refresh error: {e}")
