"""
Конфигурация бота Ozon Seller.

Модуль содержит:
- Settings: Pydantic-модель для валидации .env
- Constants: Константы приложения (Enum)
- JSON utils: Утилиты для работы с JSON файлами
- SKU utils: Утилиты для парсинга SKU
"""

from .settings import settings, Settings, get_settings, reload_settings
from .constants import (
    ForecastMethod,
    DemandMethod,
    TrafficMetric,
    StockMetric,
    ShipmentStatus,
    PurchaseStatus,
    NoticeCode,
    ForecastMethodLiteral,
    DemandMethodLiteral,
    TrafficMetricLiteral,
)
from .json_utils import (
    safe_read_json,
    safe_write_json,
    ensure_dir_exists,
)
from .sku_utils import (
    parse_sku_string,
    parse_sku_list,
    validate_sku,
    filter_valid_skus,
    format_sku_with_alias,
    parse_sku_with_aliases,
    deduplicate_skus,
    batch_skus,
)

__all__ = [
    "settings",
    "Settings",
    "get_settings",
    "reload_settings",
    "ForecastMethod",
    "DemandMethod",
    "TrafficMetric",
    "StockMetric",
    "ShipmentStatus",
    "PurchaseStatus",
    "NoticeCode",
    "ForecastMethodLiteral",
    "DemandMethodLiteral",
    "TrafficMetricLiteral",
    "safe_read_json",
    "safe_write_json",
    "ensure_dir_exists",
    "parse_sku_string",
    "parse_sku_list",
    "validate_sku",
    "filter_valid_skus",
    "format_sku_with_alias",
    "parse_sku_with_aliases",
    "deduplicate_skus",
    "batch_skus",
]
