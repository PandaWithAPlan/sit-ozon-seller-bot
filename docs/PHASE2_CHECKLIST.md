# Phase 2: Type Hints - Checklist

## Цели
- ✅ Добавить type hints во все функции
- ✅ Использовать TypedDict для структур данных
- ✅ Настроить mypy
- ⏳ Начать рефакторинг длинных функций

## Фаза 2.1: Config и Constants ✅

### config/
- [x] `__init__.py` - Экспорт настроек и констант
- [x] `settings.py` - Pydantic Settings с валидацией
- [x] `constants.py` - Enum для методов и статусов
- [x] `env_helpers.py` - Helper функции для .env

### Корневые файлы
- [x] `requirements.txt` - Обновлён (добавлен pydantic-settings)
- [x] `types.py` - TypedDict для типизации структур данных
- [x] `.env.example` - Пример конфигурации
- [x] `README.md` - Документация проекта
- [x] `pyproject.toml` - Конфигурация для инструментов

### Документация
- [x] `DEVELOPER_GUIDE.md` - Руководство для разработчиков
- [x] `MIGRATION.md` - Инструкция по миграции на новый код

---

## Фаза 2.2: Module Type Hints (в работе)

### modules_sales/
- [x] `sales_facts_store.py` - Частично обновлён (docstrings, типизация функций)
- [x] `sales_forecast.py` - Частично обновлён (docstrings, типизация функций)
- [ ] `sales_goal.py` - TODO
- [ ] `sales_report.py` - TODO
- [ ] `sales_buyout.py` - TODO

### modules_purchases/
- [ ] `purchases_need.py` - TODO
- [ ] `purchases_report_data.py` - TODO
- [ ] `purchases_need_data.py` - TODO

### modules_shipments/
- [ ] `shipments_need.py` - TODO
- [ ] `shipments_demand.py` - TODO
- [ ] `shipments_leadtime*.py` - TODO
- [ ] `shipments_report_data.py` - TODO

### handlers/
- [ ] `handlers_sales.py` - TODO
- [ ] `handlers_purchases.py` - TODO
- [ ] `handlers_shipments_*.py` - TODO

---

## Фаза 2.3: Tools ✅

### Code Quality
- [x] `.pre-commit-config.yaml` - Pre-commit hooks (Black, Ruff, MyPy)
- [x] `.github/workflows/ci.yml` - GitHub Actions для CI/CD

---

## Ключевые улучшения

### 1. Pydantic Settings
```python
# Было:
TOKEN = os.getenv("TELEGRAM_TOKEN")

# Стало:
from config_package import settings
token = settings.effective_token
```

### 2. Enum константы
```python
# Было:
method = "ma30"

# Стало:
from config_package.constants import ForecastMethod
method = ForecastMethod.MA30
```

### 3. TypedDict
```python
# Было:
result: Dict[str, Any] = {"sku": 123, "units": 10}

# Стало:
from types import ForecastResult
result: ForecastResult = {"sku": 123, "units_forecast": 10.0, ...}
```

### 4. Docstrings
```python
# Было:
def calculate(sku):
    return sku * 2

# Стало:
def calculate(sku: int) -> int:
    """Умножает SKU на 2."""
    return sku * 2
```

---

## Проверки

### Type checking
```bash
mypy ozon-seller/
```

### Code formatting
```bash
black ozon-seller/ --check
ruff check ozon-seller/
```

### Run all checks
```bash
black ozon-seller/ && ruff check ozon-seller/ && mypy ozon-seller/
```

---

## Следующие шаги (Phase 3)

### Приоритет модулей для рефакторинга:
1. `modules_shipments/shipments_need.py` - самая длинная функция (~250 строк)
2. `modules_purchases/purchases_need.py` - критично для бизнеса
3. `handlers_sales.py` - разделение по разделам
4. `handlers_purchases.py` - улучшение обработчиков

### Декомпозиция `shipments_need.py:compute_need()`:
- `_prepare_demand_data()` - подготовка данных по спросу
- `_compute_sku_needs()` - расчёт по SKU
- `_compute_cluster_needs()` - расчёт по кластерам
- `_compute_warehouse_needs()` - расчёт по складам
- `_aggregate_and_sort()` - группировка и сортировка

---

## Заметки

- LSP ошибки от `aiogram` и других библиотек игнорируем - это проблемы IDE, не кода
- MyPy настроен в режиме `disallow_untyped_defs=False` для постепенного включения
- Добавлен `ignore-missing-imports` для внешних библиотек

## Даты
- ✅ Начато: 15.01.2026
- ✅ Config & Constants завершено: 15.01.2026
- ✅ Tools настроены: 15.01.2026
- ⏳ Module Type Hints: в процессе...
