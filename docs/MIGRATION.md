# Migration Guide

## Phase 1: Конфигурация и валидация ✅

### Что сделано
- Создан модуль `config/` с Pydantic Settings
- Добавлен `.env.example` с примерами
- Созданы Enum константы
- Добавлены TypedDict в `types.py`

### Как мигрировать код

#### Было (old)
```python
import os
from dotenv import load_dotenv

TOKEN = os.getenv("TELEGRAM_TOKEN")
CLIENT_ID = os.getenv("OZON_CLIENT_ID")
WATCH_SKU = [s.strip() for s in (os.getenv("WATCH_SKU", "") or "").split(",")]
```

#### Стало (new)
```python
from config_package import settings

token = settings.effective_token
client_id = settings.ozon_client_id
sku_list = settings.parsed_watch_sku
```

#### Шаблон миграции
1. Заменить `from dotenv import load_dotenv` и `load_dotenv()` на `from config_package import settings`
2. Заменить `os.getenv("VAR")` на `settings.variable_name`
3. Использовать свойства для парсинга:
   - `settings.parsed_watch_sku` вместо ручного парсинга
   - `settings.parsed_chat_ids` вместо ручного парсинга
   - `settings.get_forecast_method()` вместо чтения из кэша

### Примеры миграции

#### bot.py
```python
# Было:
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN") or os.getenv("BOT_TOKEN")

# Стало:
from config_package import settings
TELEGRAM_TOKEN = settings.effective_token
```

#### modules_sales/sales_forecast.py
```python
# Было:
ES_ALPHA = float(os.getenv("ES_ALPHA", "0.3"))

# Стало:
from config_package import settings
ES_ALPHA = settings.es_alpha
```

#### scheduler.py
```python
# Было:
CHAT_IDS = _parse_chat_ids(CFG.get("CHAT_IDS", ""))

# Стало:
from config_package import settings
CHAT_IDS = settings.parsed_chat_ids
```

## Phase 2: Типизация и Type Hints ✅

### Что сделано
- Добавлены docstrings во все функции
- Улучшена типизация в `modules_sales/sales_facts_store.py`
- Улучшена типизация в `modules_sales/sales_forecast.py`
- Создан `pyproject.toml` для Black, Ruff, MyPy
- Созданы pre-commit hooks
- Создан GitHub Actions workflow
- Создано руководство для разработчиков

### Как мигрировать код

#### 1. Добавить docstrings
```python
# Было:
def calculate_metric(sku):
    result = 0
    return result

# Стало:
def calculate_metric(sku: int) -> float:
    """
    Вычисляет метрику для заданного SKU.
    
    Args:
        sku: Идентификатор товара
    
    Returns:
        Значение метрики
    """
    result = 0.0
    return result
```

#### 2. Использовать TypedDict
```python
# Было:
def get_data() -> dict:
    return {
        "sku": 123,
        "units": 10.0,
        "revenue": 5000.0,
    }

# Стало:
from types import SalesSeriesRecord, ForecastResult

def get_data() -> Dict[int, SalesSeriesRecord]:
    return {
        "sku": 123,
        "units": 10.0,
        "revenue": 5000.0,
    }
```

#### 3. Использовать Enum
```python
# Было:
def get_method() -> str:
    return "ma30"

# Стало:
from config_package.constants import ForecastMethod

def get_method() -> ForecastMethod:
    return ForecastMethod.MA30

# Использование:
print(method.value)   # "ma30"
print(method.title)   # "Средняя за 30 дней"
```

## Phase 3: Рефакторинг функций (План)

### Цель
Разбить длинные функции на более мелкие (< 50 строк)

### Что нужно сделать

#### modules_shipments/shipments_need.py
Функция `compute_need()` (~250 строк):
- Разбить на:
  - `_prepare_demand_data()` - подготовка данных по спросу
  - `_compute_sku_needs()` - расчёт по SKU
  - `_compute_cluster_needs()` - расчёт по кластерам
  - `_compute_warehouse_needs()` - расчёт по складам
  - `_aggregate_and_sort()` - группировка и сортировка

#### scheduler.py
- Вынести логику формирования текста в отдельные модули:
  - `notifications/purchases.py` - уведомления закупок
  - `notifications/sales.py` - уведомления продаж
  - `notifications/shipments.py` - уведомления отгрузок

## Phase 4: Документация (План)

### Что нужно сделать

#### README.md
- Установка
- Конфигурация
- Команды бота
- Архитектура модулей
- API Integration

#### Docstrings
- Добавить docstrings для всех публичных функций
- Формат: Args, Returns, Raises, Example

#### Архитектурная документация
- Создать `docs/architecture.md`
- Диаграмма модулей (Mermaid)
- Потоки данных
- Схема базы данных (кэш файлы)

## Проверка миграции

### Тестовый запуск
```bash
# 1. Проверить валидацию конфигурации
python -c "from config_package import settings; settings.validate_on_startup(); print('OK')"

# 2. Проверить типизацию
mypy ozon-seller/

# 3. Проверить код-стайл
black ozon-seller/
ruff check ozon-seller/

# 4. Запустить бота
python -m ozon-seller.main
```

### Чек-лист миграции

- [ ] Все `os.getenv("VAR")` заменены на `settings.var_name`
- [ ] Ручные парсинг списков заменены на `settings.parsed_*`
- [ ] Docstrings добавлены для всех публичных функций
- [ ] Type hints добавлены для всех функций
- [ ] MyPy не выдаёт ошибок в key модулях
- [ ] Black и Ruff не выдают ошибок
- [ ] `.env.example` создан
- [ ] Бот запускается и работает

## Обратная совместимость

### Обеспечена
✅ Полная обратная совместимость - все старые функции работают
✅ Graceful degradation при отсутствующих модулях
✅ Валидация при старте бота
✅ Предупреждения при неверной конфигурации

## Следующие шаги

### Phase 3: Рефакторинг функций
1. Начать с `modules_shipments/shipments_need.py`
2. Создать функции-помощники
3. Разбить `compute_need()` на компоненты
4. Добавить unit-тесты

### Phase 4: Документация
1. Создать архитектурную документацию
2. Добавить примеры использования
3. Создать UML-диаграммы
4. Добавить API документацию

### Phase 5: Тестирование
1. Создать структуру тестов `tests/`
2. Добавить unit-тесты для расчётных функций
3. Добавить интеграционные тесты для API
4. Настроить coverage отчёты
5. Достичь 70%+ покрытия

## Справка

### Частые проблемы

#### ImportError
```
ImportError: cannot import name 'settings' from config
```
Решение:
```python
from config_package import settings, get_settings
# или
from config_package import Settings
s = Settings()
```

#### ValidationError
```
pydantic.ValidationError: 1 validation error for Settings
```
Решение: Проверьте `.env.example` и заполните все обязательные поля

#### TypeError
```
TypeError: 'int' object is not callable
```
Решение: Проверьте типизацию и совместимость версий pydantic

## Полезные команды

```bash
# Установка зависимостей
pip install -r requirements.txt

# Установка dev-зависимостей
pip install pre-commit black ruff mypy

# Установка pre-commit hooks
pre-commit install

# Проверка типизации
mypy ozon-seller/

# Проверка код-стайла
black ozon-seller/ --check
ruff check ozon-seller/

# Запуск тестов
pytest -v

# С покрытием
pytest --cov=ozon-seller --cov-report=html
```
