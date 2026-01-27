"""
Роутер для команд /start, /help, /method
"""

from modules_common.ui import (
    welcome_text as _welcome_text,
    build_main_menu_kb as _build_main_menu_kb,
    build_method_kb as _build_method_kb,
    home_kb as _home_kb,
)
from scheduler import register_notice_chat
import logging

from aiogram import Router
from aiogram.filters import Command
from aiogram.types import Message
from aiogram.fsm.context import FSMContext
from aiogram.enums import ParseMode

# Логирование
log = logging.getLogger("seller-bot.start_router")

# Импорты из config_package

# Создаем роутер
start_router = Router(name="start")


# ==================== /start ====================
@start_router.message(Command("start"))
async def on_start(message: Message):
    """
    Обработчик команды /start.
    Запоминает чат для уведомлений и показывает главное меню.
    """
    msg = message
    try:
        register_notice_chat(msg.chat.id)
        log.debug(f"Registered chat {msg.chat.id} for notices on /start")
    except Exception as e:
        log.error(f"Failed to register chat {msg.chat.id}: {e}", exc_info=True)

    await message.answer(_welcome_text(), reply_markup=_build_main_menu_kb())


# ==================== /help ====================
@start_router.message(Command("help"))
async def on_help(message: Message):
    """
    Обработчик команды /help.
    Показывает справку по возможностям бота.
    """
    try:
        register_notice_chat(message.chat.id)
        log.debug(f"Registered chat {message.chat.id} for notices on /help")
    except Exception as e:
        log.error(f"Failed to register chat {message.chat.id}: {e}", exc_info=True)

    text = (
        "ℹ️ <b>Методики и возможности бота</b>\n\n"
        "📈 <b>План продаж</b>\n"
        "Бот строит прогноз спроса по каждому SKU несколькими способами:\n"
        "• <b>Скользящее среднее</b> (MA7/14/30/60/90/180/360) — усредняем продажи за выбранные дни.\n"
        "• <b>Экспоненциальное сглаживание</b> (ES, параметр α из .env) — свежие продажи влияют сильнее.\n"
        "<i>Пример:</i> если за последние 30 дней продали 300 шт, прогноз MA30 ≈ 10 шт/день.\n"
        "Выбрать метод можно командой <code>/method</code> — бот покажет список и текущую настройку.\n\n"
        "🏷️ <b>Рекомендации по выкупам</b>\n"
        "Необходимый объём зависит от плана на горизонт и коэффициента выкупа.\n"
        "Файл заявок Seller — <b>«Товары.xlsx»</b> — загрузите командой <code>/data</code>.\n\n"
        "🚚 <b>Рекомендации по отгрузкам</b>\n"
        "Цель — поддерживать комфортный запас по сети с учётом лагов L/S, планов и остатков.\n\n"
        "🏬 <b>Потребность складов</b> управляется в <code>/warehouse</code>.\n\n"
        "🔔 <b>Куда приходят уведомления</b>\n"
        "Бот присылает уведомления в этот чат. Чат автоматически запоминается после команд <code>/start</code> и <code>/notice</code>."
    )
    await message.answer(text, reply_markup=_home_kb(), parse_mode=ParseMode.HTML)


# ==================== /method ====================
@start_router.message(Command("method"))
async def on_method(message: Message):
    """
    Обработчик команды /method.
    Показывает меню выбора метода прогноза.
    """
    from modules_sales.sales_forecast import get_forecast_method_title

    try:
        register_notice_chat(message.chat.id)
        log.debug(f"Registered chat {message.chat.id} for notices on /method")
    except Exception as e:
        log.error(f"Failed to register chat {message.chat.id}: {e}", exc_info=True)

    current = get_forecast_method_title()
    text = (
        "⚙️ <b>Метод расчёта прогноза продаж</b>\n"
        f"Текущий: <b>{current}</b>\n\n"
        "Выберите новый метод:"
    )
    await message.answer(text, reply_markup=_build_method_kb(), parse_mode=ParseMode.HTML)


# ==================== /data ====================
@start_router.message(Command("data"))
async def on_data(message: Message, state: FSMContext):
    """
    Обработчик команды /data.
    Запускает сценарий загрузки файла.
    """
    from handlers.handlers_purchases import BuyoutsUpload
    from menu import back_home_menu
    from config_package import settings

    try:
        register_notice_chat(message.chat.id)
    except Exception:
        pass

    await state.set_state(BuyoutsUpload.waiting_file)
    xlsx_name = settings.purchases_xlsx_name
    hint = (
        "🗂 <b>Загрузка файла «Товары.xlsx»</b>\n\n"
        "Пришлите файл Excel <i>как документ</i> (формат .xlsx).\n"
        f"Будет сохранён как <code>{xlsx_name}</code> в папку <code>data/</code>.\n\n"
        "Поддерживаемые столбцы: SKU/Артикул, Статус, Кол-во. Город (Москва/Хабаровск) можно не указывать — "
        "если указан, значения суммируются по SKU."
    )
    await message.answer(hint, reply_markup=back_home_menu(), parse_mode=ParseMode.HTML)
