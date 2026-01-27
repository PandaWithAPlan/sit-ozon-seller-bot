import asyncio
import logging
import sys

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.types import BotCommand

# Конфиг и Логирование
from config_package.logging_config import setup_logging
from config_package import settings

# Роутеры и Шедулер
from scheduler import scheduler_start
from handlers import router as handlers_router
# Явные импорты, так как __init__.py может отсутствовать или быть неполным
from routers.start import start_router
from routers.notifications import notifications_router
from routers.warehouse import warehouse_router
from routers.fallback import fallback_router

log = logging.getLogger("seller-bot")

async def setup_bot_commands(bot: Bot) -> None:
    """Настройка команд бота."""
    await bot.set_my_commands(
        [
            BotCommand(command="start", description="Главное меню"),
            BotCommand(command="units", description="Посмотреть перечень отслеживаемых юнитов"),
            BotCommand(command="method", description="Выбрать метод расчета прогноза продаж"),
            BotCommand(command="warehouse", description="Выбрать метод расчета потребности"),
            BotCommand(command="data", description="Загрузить «Товары.xlsx»"),
            BotCommand(command="notice", description="Показать уведомления"),
            BotCommand(command="help", description="Справка по возможностям"),
        ]
    )

async def main():
    # 1. Настройка логирования
    setup_logging()
    
    # 2. Валидация конфига
    try:
        settings.validate_on_startup()
    except Exception as e:
        log.critical(f"Configuration error: {e}")
        return

    # 3. Инициализация
    token = settings.effective_token
    bot = Bot(token=token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher()

    # 4. Подключение роутеров
    # Порядок важен для перехвата сообщений
    dp.include_router(start_router)
    dp.include_router(warehouse_router)
    dp.include_router(notifications_router)
    dp.include_router(handlers_router)
    
    # Fallback должен быть последним
    dp.include_router(fallback_router)

    # 5. Запуск
    await setup_bot_commands(bot)
    await scheduler_start(bot)

    log.info("Bot is starting polling...")
    try:
        await dp.start_polling(bot)
    except Exception as e:
        log.error(f"Polling error: {e}")
    finally:
        await bot.session.close()


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        log.info("Bot stopped.")
