from __future__ import annotations
import logging
from aiogram import Router, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import Command

from modules_marketing.services import fetch_campaigns
from modules_marketing.views import marketing_report_text

marketing_router = Router(name="marketing")
log = logging.getLogger("seller-bot.marketing_router")

def marketing_menu() -> InlineKeyboardMarkup:
    kb = [
        [InlineKeyboardButton(text="🔄 Обновить", callback_data="mkt:refresh")],
        [InlineKeyboardButton(text="🏠 Домой", callback_data="nav:home")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=kb)

@marketing_router.message(Command("marketing"))
async def on_marketing_cmd(message: Message):
    msg = await message.answer("⏳ Загружаю кампании...")
    try:
        data = await fetch_campaigns()
        text = marketing_report_text(data)
        await msg.edit_text(text, reply_markup=marketing_menu())
    except Exception as e:
        log.error(f"Marketing error: {e}")
        await msg.edit_text("❌ Ошибка при загрузке кампаний")

@marketing_router.callback_query(F.data == "menu:marketing")
async def on_marketing_menu(cb: CallbackQuery):
    await cb.answer()
    msg = await cb.message.answer("⏳ Загружаю кампании...")
    try:
        data = await fetch_campaigns()
        text = marketing_report_text(data)
        await msg.edit_text(text, reply_markup=marketing_menu())
    except Exception as e:
        log.error(f"Marketing menu error: {e}")
        await msg.edit_text("❌ Ошибка при загрузке кампаний")

@marketing_router.callback_query(F.data == "mkt:refresh")
async def on_marketing_refresh(cb: CallbackQuery):
    await cb.answer("Обновляю...")
    try:
        data = await fetch_campaigns()
        text = marketing_report_text(data)
        await cb.message.edit_text(text, reply_markup=marketing_menu())
    except Exception as e:
        log.error(f"Marketing refresh error: {e}")
