# Руководство для разработчиков

## Установка зависимостей

```bash
pip install -r requirements.txt
```

Для разработки также установите:
```bash
pip install -r requirements.txt
pip install pre-commit
```

## Настройка pre-commit hooks

```bash
pre-commit install
```

## Конфигурация

### .env файл
Скопируйте `.env.example` в `.env` и заполните обязательные параметры:

```env
TELEGRAM_TOKEN=your_bot_token_here
OZON_CLIENT_ID=your_client_id_here
OZON_API_KEY=your_api_key_here
WATCH_SKU=123456,789012,345678
CHAT_IDS=123456789,-1001234567890
```

### Запуск

```bash
# Запуск бота
python -m ozon-seller.main

# Или (для отладки)
python ozon-seller/main.py
```

## Использование новой конфигурации

```python
from config_package import settings, get_settings
from config_package.constants import ForecastMethod, DemandMethod

# Получение настроек
```

## Код-стайл

### Black (форматирование)
```bash
black ozon-seller/
```

### Ruff (линтинг)
```bash
ruff check ozon-seller/
ruff check --fix ozon-seller/
```

### MyPy (типизация)
```bash
mypy ozon-seller/
```

Все три проверки одновременно:
```bash
black ozon-seller/ && ruff check ozon-seller/ && mypy ozon-seller/
```

## Архитектура модулей

### config/
- `settings.py` - Pydantic Settings с валидацией
- `constants.py` - Enum для методов и статусов
- `env_helpers.py` - Helper функции для .env

### modules_sales/
- `sales_facts_store.py` - Хранение и загрузка фактов продаж
- `sales_forecast.py` - Прогнозирование (MA/ES)
- `sales_goal.py` - Цели продаж
- `sales_report.py` - Отчёты по продажам
- `sales_buyout.py` - Выкупы (в разделе Продажи)

### modules_purchases/
- `purchases_need.py` - Рекомендации по закупкам
- `purchases_report_data.py` - Данные для отчётов
- `purchases_need_data.py` - Данные о потребностях

### modules_shipments/
- `shipments_need.py` - Рекомендации по отгрузкам
- `shipments_demand.py` - Потребность по складам
- `shipments_leadtime*.py` - Сроки доставки
- `shipments_report_data.py` - Данные для отчётов

### modules_common/
- `paths.py` - Пути к директориям
- `calendar.py` - Календарь
- `units.py` - Управление юнитами

### handlers/
- `handlers_sales.py` - Продажи
- `handlers_purchases.py` - Выкупы
- `handlers_shipments_*.py` - Отгрузки

## Типизация

### TypedDict
Используйте `types.py` для типизации структур данных:

```python
from types import SalesSeriesRecord, ForecastResult

def process_series(series: List[SalesSeriesRecord]) -> ForecastResult:
    return {
        "sku": record.sku,
        "units_forecast": record.units,
        "revenue_forecast": record.revenue,
    }
```

### Type hints
Добавляйте type hints во все функции:

```python
from typing import Dict, List, Optional
from datetime import date

def calculate_metrics(
    sku: int,
    period_days: int,
    horizon: int
) -> Tuple[float, float]:
    """
    Вычисляет метрики для SKU.
    
    Args:
        sku: Идентификатор товара
        period_days: Период для расчёта (дни)
        horizon: Горизонт прогнозирования (дни)
    
    Returns:
        Кортеж (units_forecast, revenue_forecast)
    """
    units = 0.0
    revenue = 0.0
    # ...
    return units, revenue
```

## Миграция старого кода

### Было (os.getenv)
```python
import os

TOKEN = os.getenv("TELEGRAM_TOKEN")
WATCH_SKU = [s.strip() for s in (os.getenv("WATCH_SKU", "") or "").split(",")]
```

### Стало (config)
```python
from config_package import settings

token = settings.effective_token
sku_list = settings.parsed_watch_sku
```

## Тестирование

```bash
# Запуск всех тестов
pytest

# С покрытием
pytest --cov=ozon-seller --cov-report=html

# Определённый тест
pytest tests/test_sales_facts_store.py -v
```

## Обработка ошибок

### Логирование
```python
import logging

log = logging.getLogger(__name__)

try:
    result = risky_operation()
except ValueError as e:
    log.error(f"Ошибка валидации: {e}")
    # Graceful degradation
    result = fallback_value()
except Exception as e:
    log.exception(f"Неожиданная ошибка: {e}")
    raise
```

### Graceful degradation
```python
from config_package import settings

try:
    from modules_sales.sales_facts_store import get_alias_for_sku
except ImportError:
    # Фолбэк для отсутствующего модуля
    def get_alias_for_sku(sku: int) -> str | None:
        return str(sku)
```

## CI/CD

Проект использует GitHub Actions для автоматизации:
- Lint (Black, Ruff, MyPy)
- Test (pytest + coverage)
- Coverage upload (Codecov)

Конфигурация: `.github/workflows/ci.yml`

## Структура данных

### Факты продаж
```python
{
    sku: int,
    date: date,
    units: float,
    revenue: float,
}
```

### Прогноз
```python
{
    sku: int,
    units_forecast: float,
    revenue_forecast: float,
    method: str,  # "ma30", "es", etc.
    period_days: int,
}
```

### Рекомендации закупок
```python
{
    sku: int,
    alias: str,
    seller_total: float,
    ozon_total: float,
    plan_30d: float,
    need_qty: float,
    status: str,  # "DEFICIT" | "ENOUGH" | "SURPLUS"
    action: str,  # "BUY" | "SELL" | "MAINTAIN"
}
```

### Рекомендации отгрузок
```python
{
    sku: int,
    alias: str,
    title: str,  # товар или склад
    dest: str,  # "sku" | "cluster" | "warehouse"
    plan30: float,
    stock: float,
    qty: int,
    status: str,  # "DEFICIT" | "ENOUGH" | "SURPLUS"
    action: str,  # "🚚 Отгрузить" | "🔄 Поддерживать" | "🏷 Распродать"
}
```

## API Integration

### Ozon Analytics API
Эндпоинт: `https://api-seller.ozon.ru/v1/analytics/data`

Параметры:
- `date_from` / `date_to`: Период (YYYY-MM-DD)
- `metrics`: Метрики (ordered_units, revenue, cvr, ctr)
- `dimension`: Разрезы (sku, day, warehouse, etc.)
- `filters`: Фильтры по sku/offer_id

Пример запроса:
```python
payload = {
    "date_from": "2024-01-01",
    "date_to": "2024-01-31",
    "metrics": ["ordered_units", "revenue"],
    "dimension": ["day", "sku"],
    "filters": [
        {"key": "sku", "value": "123456,789012", "operator": "IN"}
    ],
    "limit": 1000,
    "offset": 0,
}
```

## Telegram Bot

### Команды
- `/start` - Главное меню
- `/help` - Справка
- `/units` - Список юнитов
- `/method` - Метод прогноза
- `/warehouse` - Метод потребности
- `/data` - Загрузить Excel
- `/notice` - Уведомления

### Клавиатуры
- Inline клавиатуры для навигации
- Callback handlers с FSM для многошаговых сценариев
- Deduplication для быстрых повторных кликов

## Планировщик (Scheduler)

### Типы уведомлений
- **Дайджесты**: Полный и сокращённый утренние
- **Отдельные**: Факт/план, конверсия, CTR, закупки, отгрузки
- **Цели продаж**: Напоминания о загрузке Excel

### Время отправки
Настроится в `.env`:
- `DAILY_NOTICES_WEEKDAY_AT` - будни
- `DAILY_NOTICES_WEEKEND_AT` - выходные
- `NOTIFY_SPREAD_SEC` - пауза между сообщениями

## Советы

1. **Используйте Enum вместо строк**
   - `ForecastMethod.MA30` вместо `"ma30"`
   - Типобезопасность + автодополнение

2. **Избегайте `os.getenv` напрямую**
   - Используйте `settings` из `config`
   - Валидация при старте бота

3. **Добавляйте docstrings**
   - Формат: Args, Returns, Raises
   - Автогенерация документации

4. **Graceful degradation**
   - Фолбэки для отсутствующих модулей
   - Кэширование для снижения нагрузки

5. **Изоляция модулей**
   - Каждый модуль должен работать независимо
   - Минимум зависимостей между модулями

## Полезные ресурсы

- [Pydantic documentation](https://docs.pydantic.dev/)
- [Aiogram 3.x docs](https://docs.aiogram.dev/)
- [Python typing cheatsheet](https://mypy.readthedocs.io/en/stable/cheat_sheet_py3.html)
- [Black style guide](https://black.readthedocs.io/en/stable/the_black_code_style/)
- [Ruff rules](https://docs.astral.sh/ruff/rules/)
