from __future__ import annotations
import logging
from aiogram import Router, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import Command
import datetime as dt

from modules_finance.services import fetch_transactions
from modules_finance.views import finance_report_text
from modules_common.ui import home_kb

finance_router = Router(name="finance")
log = logging.getLogger("seller-bot.finance_router")

def finance_menu() -> InlineKeyboardMarkup:
    kb = [
        [InlineKeyboardButton(text="📅 За сегодня", callback_data="fin:today")],
        [InlineKeyboardButton(text="🗓 За месяц", callback_data="fin:month")],
        [InlineKeyboardButton(text="🏠 Домой", callback_data="nav:home")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=kb)

@finance_router.message(Command("finance"))
async def on_finance_cmd(message: Message):
    await message.answer("💰 <b>Раздел Финансы (Beta)</b>\nВыберите период отчета:", reply_markup=finance_menu())

@finance_router.callback_query(F.data == "menu:finance")
async def on_finance_menu(cb: CallbackQuery):
    await cb.answer()
    await cb.message.answer("💰 <b>Раздел Финансы (Beta)</b>\nВыберите период отчета:", reply_markup=finance_menu())

@finance_router.callback_query(F.data.startswith("fin:"))
async def on_finance_cb(cb: CallbackQuery):
    action = cb.data.split(":")[1]
    
    await cb.answer("Загружаю данные...")
    
    now = dt.datetime.now()
    if action == "today":
        d_from = now.replace(hour=0, minute=0, second=0, microsecond=0)
        d_to = now
        period_name = "Сегодня"
    elif action == "month":
        d_from = now - dt.timedelta(days=30)
        d_to = now
        period_name = "30 дней"
    else:
        return

    try:
        txs = await fetch_transactions(d_from, d_to)
        text = finance_report_text(txs, period_name)
        await cb.message.edit_text(text, reply_markup=finance_menu())
    except Exception as e:
        log.error(f"Finance error: {e}")
        await cb.message.answer("❌ Ошибка при получении данных")
