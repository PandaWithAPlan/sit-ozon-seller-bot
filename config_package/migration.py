"""
Миграция на новую конфигурацию.

Этот файл показывает как использовать новую систему конфигурации (Pydantic Settings)
в существующем коде без разрыва функциональности.
"""

import logging
from pathlib import Path

from config_package import settings, get_settings, load_env_with_fallback, ensure_directories
from config_package.constants import ForecastMethod, DemandMethod

log = logging.getLogger(__name__)


def migrate_bot_py_usage():
    """
    Пример миграции bot.py на новую конфигурацию.

    Было:
        TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN") or os.getenv("BOT_TOKEN")
        OZON_CLIENT_ID = os.getenv("OZON_CLIENT_ID")

    Стало:
        from config_package import settings
        token = settings.effective_token
        client_id = settings.ozon_client_id
    """
    # Получаем настройки
    token = settings.effective_token
    client_id = settings.ozon_client_id
    api_key = settings.ozon_api_key
    company_id = settings.ozon_company_id

    log.info(f"Токен: {token[:10]}...")
    log.info(f"Client ID: {client_id}")
    log.info(f"Company ID: {company_id or 'не задан'}")

    # Парсинг списков
    watch_sku = settings.parsed_watch_sku
    chat_ids = settings.parsed_chat_ids

    log.info(f"Отслеживаем {len(watch_sku)} SKU")
    log.info(f"Чаты для уведомлений: {chat_ids}")

    # Пути к директориям
    data_dir = settings.data_dir
    cache_dir = settings.cache_dir

    log.info(f"Data: {data_dir}")
    log.info(f"Cache: {cache_dir}")


def migrate_scheduler_py_usage():
    """
    Пример миграции scheduler.py на новую конфигурацию.
    """
    # Было:
    # CFG = dotenv_values(os.path.join(BASE_DIR, ".env"))
    # CHAT_IDS = _parse_chat_ids(CFG.get("CHAT_IDS", ""))

    # Стало:
    chat_ids = settings.parsed_chat_ids

    # Время дайджестов
    weekday_digest_t = settings.daily_notices_weekday_at
    weekend_digest_t = settings.daily_notices_weekend_at

    # Пауза между сообщениями
    spread_sec = settings.notify_spread_sec

    log.info(f"Чаты: {chat_ids}")
    log.info(f"Время будни: {weekday_digest_t}")
    log.info(f"Время выходные: {weekend_digest_t}")
    log.info(f"Пауза: {spread_sec} сек")


def migrate_modules_sales_forecast_usage():
    """
    Пример миграции modules_sales/sales_forecast.py на новую конфигурацию.
    """
    # Было:
    # OZON_CLIENT_ID = os.getenv("OZON_CLIENT_ID", "")
    # OZON_API_KEY = os.getenv("OZON_API_KEY", "")
    # ES_ALPHA = float(os.getenv("ES_ALPHA", "0.3"))
    # WATCH_SKU = [s.strip() for s in (os.getenv("WATCH_SKU", "") or "").split(",") if s.strip()]

    # Стало:
    ozon_client_id = settings.ozon_client_id
    ozon_api_key = settings.ozon_api_key
    ozon_company_id = settings.ozon_company_id
    es_alpha = settings.es_alpha
    watch_sku = settings.parsed_watch_sku

    # API URL
    ozon_api_url = settings.ozon_api_url

    # API настройки (retry, pause)
    api_max_retries = settings.api_max_retries
    api_base_pause = settings.api_base_pause
    api_max_pause = settings.api_max_pause
    api_jitter = settings.api_jitter

    log.info(f"Client ID: {ozon_client_id}")
    log.info(f"API URL: {ozon_api_url}")
    log.info(f"ES Alpha: {es_alpha}")
    log.info(f"SKU: {watch_sku}")
    log.info(f"API retries: {api_max_retries}")


def migrate_modules_purchases_need_usage():
    """
    Пример миграции modules_purchases/purchases_need.py на новую конфигурацию.
    """
    # Было:
    # ALERT_PLAN_HORIZON_DAYS = int(os.getenv("ALERT_PLAN_HORIZON_DAYS", "30"))
    # BUY_COEF = float(os.getenv("BUY_COEF", "5.0"))
    # BUY_RED_FACTOR = float(os.getenv("BUY_RED_FACTOR", "4.0"))
    # BUY_YELLOW_FACTOR = float(os.getenv("BUY_YELLOW_FACTOR", "4.25"))

    # Стало:
    alert_plan_horizon_days = settings.alert_plan_horizon_days
    buy_coef = settings.buy_coef

    # Пороги светофора
    buy_red_factor = settings.buy_red_factor
    buy_yellow_factor = settings.buy_yellow_factor
    buy_max_factor = settings.buy_max_factor
    prof_yellow_factor = settings.prof_yellow_factor
    prof_red_factor = settings.prof_red_factor

    # Флаги
    lights_enabled = settings.purchases_need_lights_enabled

    log.info(f"Горизонт: {alert_plan_horizon_days} дн")
    log.info(f"Коэффициент выкупа: {buy_coef}")
    log.info(f"Пороги: R={buy_red_factor:.2f}, Y={buy_yellow_factor:.2f}, MX={buy_max_factor:.2f}")
    log.info(f"Светофор: {'включён' if lights_enabled else 'выключен'}")


def migrate_modules_shipments_need_usage():
    """
    Пример миграции modules_shipments/shipments_need.py на новую конфигурацию.
    """
    # Было:
    # SHIP_ROUND_STEP = int(os.getenv("SHIP_ROUND_STEP", "2"))
    # SHIP_SAFETY_COEF = float(os.getenv("SHIP_SAFETY_COEF", "2.0"))
    # SHIP_RED_FACTOR_SHIP = float(os.getenv("SHIP_RED_FACTOR_SHIP", "1.5"))
    # SHIP_YELLOW_FACTOR_SHIP = float(os.getenv("SHIP_YELLOW_FACTOR_SHIP", "1.75"))
    # SHIP_GREEN_FACTOR_SHIP = float(os.getenv("SHIP_GREEN_FACTOR_SHIP", "2.0"))

    # Стало:
    ship_round_step = settings.ship_round_step
    ship_safety_coef = settings.ship_safety_coef

    # Пороги светофора
    ship_red_factor = settings.ship_red_factor
    ship_yellow_factor = settings.ship_yellow_factor
    ship_green_factor = settings.ship_green_factor
    ship_max_factor = settings.ship_max_factor

    prof_ship_green_factor = settings.prof_ship_green_factor
    prof_ship_yellow_factor = settings.prof_ship_yellow_factor
    prof_ship_red_factor = settings.prof_ship_red_factor

    # Флаги
    lights_enabled = settings.shipments_need_lights_enabled
    demand_eps_strict = settings.demand_eps_strict
    demand_clear_on_period_change = settings.demand_clear_on_period_change

    log.info(f"Шаг округления: {ship_round_step}")
    log.info(f"Коэффициент безопасности: {ship_safety_coef}")
    log.info(f"Пороги: R={
            ship_red_factor:.2f}, Y={
            ship_yellow_factor:.2f}, G={
                ship_green_factor:.2f}")
    log.info(f"Светофор: {'включён' if lights_enabled else 'выключен'}")


def migrate_modules_sales_goal_usage():
    """
    Пример миграции modules_sales/sales_goal.py на новую конфигурацию.
    """
    # Было:
    # GOAL_RED_FACTOR_HIGH = float(os.getenv("GOAL_RED_FACTOR_HIGH", "1.20"))
    # GOAL_YELLOW_FACTOR_HIGH = float(os.getenv("GOAL_YELLOW_FACTOR_HIGH", "1.10"))
    # GOAL_GREEN_TOL = float(os.getenv("GOAL_GREEN_TOL", "0.02"))
    # GOAL_YELLOW_FACTOR_LOW = float(os.getenv("GOAL_YELLOW_FACTOR_LOW", "0.95"))
    # GOAL_RED_FACTOR_LOW = float(os.getenv("GOAL_RED_FACTOR_LOW", "0.90"))

    # Стало:
    goal_red_factor_high = settings.goal_red_factor_high
    goal_yellow_factor_high = settings.goal_yellow_factor_high
    goal_green_tol = settings.goal_green_tol
    goal_yellow_factor_low = settings.goal_yellow_factor_low
    goal_red_factor_low = settings.goal_red_factor_low

    # Флаги
    goal_lights_enabled = settings.goal_lights_enabled

    log.info(f"Пороги целей: R_h={
            goal_red_factor_high:.2f}, Y_h={
            goal_yellow_factor_high:.2f}, G={
                goal_green_tol:.2f}, Y_l={
                    goal_yellow_factor_low:.2f}, R_l={
                        goal_red_factor_low:.2f}")
    log.info(f"Светофор: {'включён' if goal_lights_enabled else 'выключен'}")


def demonstrate_enum_usage():
    """
    Пример использования Enum вместо строковых констант.
    """
    # Было:
    # method = "ma30"
    # if method == "ma30":
    #     ...

    # Стало:
    forecast_method = settings.get_forecast_method()  # ForecastMethod.MA30

    if forecast_method == ForecastMethod.MA30:
        log.info("Используем среднюю за 30 дней")
        title = forecast_method.title  # "Средняя за 30 дней"

    demand_method = settings.get_demand_method()  # DemandMethod.AVERAGE

    if demand_method == DemandMethod.AVERAGE:
        log.info("Используем среднесуточный спрос")
        title = demand_method.title  # "Среднесуточный спрос"


def startup_validation():
    """
    Пример валидации при старте бота.
    """
    try:
        # Проверяем критичные настройки
        is_valid, errors = settings.validate_on_startup()

        if not is_valid:
            log.error("Ошибки конфигурации:")
            for error in errors:
                log.error(f"  • {error}")
            raise ValueError("Неверная конфигурация")

        # Загружаем .env с нужной кодировкой
        env_path = settings.base_dir / ".env"
        encoding = load_env_with_fallback(env_path)
        log.info(f".env загружен (кодировка: {encoding})")

        # Создаём необходимые директории
        ensure_directories(settings)
        log.info("Директории проверены/созданы")

        log.info("✓ Валидация конфигурации пройдена успешно")

    except Exception as e:
        log.error(f"Ошибка при старте: {e}")
        raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    print("=== Демонстрация миграции на новую конфигурацию ===\n")

    print("1. Bot.py")
    migrate_bot_py_usage()
    print()

    print("2. Scheduler.py")
    migrate_scheduler_py_usage()
    print()

    print("3. Modules Sales / Forecast")
    migrate_modules_sales_forecast_usage()
    print()

    print("4. Modules Purchases / Need")
    migrate_modules_purchases_need_usage()
    print()

    print("5. Modules Shipments / Need")
    migrate_modules_shipments_need_usage()
    print()

    print("6. Modules Sales / Goal")
    migrate_modules_sales_goal_usage()
    print()

    print("7. Enum Usage")
    demonstrate_enum_usage()
    print()

    print("8. Startup Validation")
    startup_validation()
    print()

    print("=== Миграция завершена ===")
