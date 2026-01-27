"""
Проверка новой конфигурации.

Проверяет, что новая система конфигурации работает корректно.
"""

import sys
import os

# Добавляем родительскую директорию в путь
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

try:
    from config_package import settings, get_settings
    from config_package.constants import ForecastMethod, DemandMethod

    print("=== Проверка конфигурации ===")

    # Проверка 1: Базовые настройки
    print(f"\n1. Базовые настройки:")
    print(f"  • Токен: {settings.effective_token[:10]}...")
    print(f"  • Client ID: {settings.ozon_client_id}")
    print(f"  • API URL: {settings.ozon_api_url}")
    print(f"  • Timezone: {settings.timezone}")

    # Проверка 2: SKU и Offers
    print(f"\n2. Отслеживаемые товары:")
    print(f"  • Products Mode: {settings.products_mode}")
    print(f"  • SKU: {len(settings.parsed_watch_sku)} шт")
    print(f"  • Offers: {len(settings.parsed_watch_offers)} шт")
    if settings.parsed_watch_sku:
        print(f"  • Первые 3 SKU: {settings.parsed_watch_sku[:3]}")

    # Проверка 3: Chat IDs
    print(f"\n3. Получатели уведомлений:")
    print(f"  • Chat IDs: {len(settings.parsed_chat_ids)} шт")
    if settings.parsed_chat_ids:
        print(f"  • Получатели: {settings.parsed_chat_ids}")

    # Проверка 4: Прогноз продаж
    print(f"\n4. Прогноз продаж:")
    forecast_method = settings.get_forecast_method()
    print(f"  • Метод: {forecast_method.value} ({forecast_method.title})")
    print(f"  • ES Alpha: {settings.es_alpha}")
    print(f"  • Горизонт: {settings.get_plan_period_days()} дн")

    # Проверка 5: Покупки
    print(f"\n5. Покупки:")
    print(f"  • Коэффициент выкупа: {settings.buy_coef}")
    print(f"  • Пороги светофора:")
    print(f"    - Red: {settings.buy_red_factor}")
    print(f"    - Yellow: {settings.buy_yellow_factor}")
    print(f"    - Max: {settings.buy_max_factor}")

    # Проверка 6: Отгрузки
    print(f"\n6. Отгрузки:")
    print(f"  • Коэффициент безопасности: {settings.ship_safety_coef}")
    print(f"  • Шаг округления: {settings.ship_round_step}")
    print(f"  • Пороги светофора:")
    print(f"    - Red: {settings.ship_red_factor}")
    print(f"    - Yellow: {settings.ship_yellow_factor}")
    print(f"    - Green: {settings.ship_green_factor}")
    print(f"    - Max: {settings.ship_max_factor}")

    # Проверка 7: Пути
    print(f"\n7. Пути:")
    print(f"  • Data: {settings.data_dir}")
    print(f"  • Cache: {settings.cache_dir}")
    print(f"  • Sales Cache: {settings.sales_cache_dir}")
    print(f"  • Shipments Cache: {settings.shipments_cache_dir}")

    # Проверка 8: Города
    print(f"\n8. Городская конфигурация:")
    city_cfg = settings.city_config
    print(f"  • Городов: {city_cfg['count']}")
    print(f"  • Город 1: {city_cfg['city1']}")
    print(f"  • Город 2: {city_cfg.get('city2', 'не задан')}")

    # Проверка 9: Enum
    print(f"\n9. Enum константы:")
    print(f"  • ForecastMethod: {list(ForecastMethod.__members__)}")
    print(f"  • DemandMethod: {list(DemandMethod.__members__)}")

    # Проверка 10: Валидация
    print(f"\n10. Валидация при старте:")
    try:
        settings.validate_on_startup()
        print("  ✓ Валидация пройдена успешно")
    except ValueError as e:
        print(f"  ✗ Ошибки валидации:")
        for error in str(e).split("\n"):
            print(f"    • {error}")

    print("\n=== Проверка завершена ===")

except ImportError as e:
    print(f"✗ Ошибка импорта: {e}")
    print("\nУбедитесь, что:")
    print("  • Вы находитесь в корневой директории проекта")
    print("  • Пакет pydantic установлен (pip install pydantic pydantic-settings)")
    print("  • Родительская директория добавлена в sys.path")
    sys.exit(1)
