"""
Роутер для обработки callback queries уведомлений и навигации.
"""

import logging

from aiogram import Router, F
from aiogram.types import CallbackQuery
from aiogram.exceptions import TelegramBadRequest

# Логирование
log = logging.getLogger("seller-bot.notifications_router")

# Создаем роутер
notifications_router = Router(name="notifications")


# ==================== Главная кнопка «Домой» ====================
@notifications_router.callback_query(F.data == "nav:home")
async def on_nav_home(cb: CallbackQuery):
    """Обработчик кнопки «Домой»."""
    from modules_common.ui import welcome_text, build_main_menu_kb

    try:
        await cb.answer()
    except Exception:
        pass

    try:
        await cb.message.edit_text(welcome_text(), reply_markup=build_main_menu_kb())
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e):
            log.warning(f"Failed to navigate to home: {e}")
    except Exception as e:
        log.error(f"Unexpected error navigating to home: {e}", exc_info=True)


# ==================== Кнопка «Назад» ====================
@notifications_router.callback_query(F.data == "nav:back")
async def on_back(cb: CallbackQuery):
    """Универсальный «Назад» - возвращает в главное меню."""
    from modules_common.ui import welcome_text, build_main_menu_kb

    try:
        await cb.answer()
    except Exception:
        pass

    try:
        await cb.message.edit_text(welcome_text(), reply_markup=build_main_menu_kb())
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e):
            log.warning(f"Failed to navigate back: {e}")
    except Exception as e:
        log.error(f"Unexpected error navigating back: {e}", exc_info=True)


# ==================== Отправка всех уведомлений ====================
@notifications_router.callback_query(F.data == "notice:send:all")
async def on_notice_send_all(cb: CallbackQuery):
    """Отправляет полный утренний дайджест."""
    from scheduler import send_digest_full, register_notice_chat

    try:
        await cb.answer()
    except Exception:
        pass

    chat_id = cb.message.chat.id if cb.message else None
    if chat_id:
        try:
            register_notice_chat(chat_id)
        except Exception as e:
            log.error(f"Failed to register chat {chat_id}: {e}", exc_info=True)

    try:
        if cb.message:
            await cb.message.answer("📬 Показываю полный утренний дайджест здесь…")
    except TelegramBadRequest as e:
        log.warning(f"Failed to send digest message: {e}")
    except Exception as e:
        log.error(f"Unexpected error sending digest message: {e}", exc_info=True)

    if chat_id:
        await send_digest_full(cb.bot, chat_id=chat_id)


# ==================== Сокращенный дайджест ====================
@notifications_router.callback_query(F.data == "notice:send:short")
async def on_notice_send_short(cb: CallbackQuery):
    """Отправляет сокращенный дайджест."""
    from scheduler import send_digest_short, register_notice_chat

    try:
        await cb.answer()
    except Exception:
        pass

    chat_id = cb.message.chat.id if cb.message else None
    if chat_id:
        try:
            register_notice_chat(chat_id)
        except Exception as e:
            log.error(f"Failed to register chat {chat_id}: {e}", exc_info=True)

    try:
        if cb.message:
            await cb.message.answer("🗞️ Показываю сокращённый дайджест здесь…")
    except TelegramBadRequest as e:
        log.warning(f"Failed to send short digest message: {e}")
    except Exception as e:
        log.error(f"Unexpected error sending short digest message: {e}", exc_info=True)

    if chat_id:
        await send_digest_short(cb.bot, chat_id=chat_id)


# ==================== Отправка конкретного уведомления ====================
@notifications_router.callback_query(F.data.startswith("notice:send:"))
async def on_notice_send_one(cb: CallbackQuery):
    """Отправляет конкретное уведомление по коду."""
    from scheduler import run_notice, send_seller_reminder, NOTICE_REGISTRY, register_notice_chat
    from modules_common.ui import _label_for_notice

    try:
        await cb.answer()
    except Exception:
        pass

    chat_id = cb.message.chat.id if cb.message else None
    if chat_id:
        try:
            register_notice_chat(chat_id)
        except Exception as e:
            log.error(f"Failed to register chat {chat_id}: {e}", exc_info=True)

    code = cb.data.split(":")[-1]

    # Обработчики для :all и :short уже есть выше
    if code in ("all", "short"):
        return

    if code == "seller_reminder":
        await send_seller_reminder(cb.bot, chat_id=chat_id)
        try:
            if cb.message:
                await cb.message.answer("✅ Отправлено: Напоминание об Excel (выкупы)")
        except Exception as e:
            log.error(f"Failed to send confirmation for seller_reminder: {e}", exc_info=True)
        return

    if code not in NOTICE_REGISTRY:
        try:
            if cb.message:
                await cb.message.answer("❌ Неизвестный код уведомления")
        except Exception as e:
            log.error(f"Failed to send unknown code message: {e}", exc_info=True)
        return

    await run_notice(cb.bot, code, chat_id=chat_id)
    try:
        if cb.message:
            await cb.message.answer(f"✅ Отправлено: {_label_for_notice(code)}")
    except Exception as e:
        log.error(f"Failed to send confirmation for notice {code}: {e}", exc_info=True)
