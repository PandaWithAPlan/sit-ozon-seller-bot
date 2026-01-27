"""
Константы приложения.

Содержит Enum для безопасного использования строковых значений.
"""

from enum import Enum
from typing import Literal


class ForecastMethod(str, Enum):
    """Методы прогноза продаж."""

    MA7 = "ma7"
    MA14 = "ma14"
    MA30 = "ma30"
    MA60 = "ma60"
    MA90 = "ma90"
    MA180 = "ma180"
    MA360 = "ma360"
    ES = "es"

    @property
    def title(self) -> str:
        """Человеческое название метода."""
        titles = {
            "ma7": "Средняя за 7 дней",
            "ma14": "Средняя за 14 дней",
            "ma30": "Средняя за 30 дней",
            "ma60": "Средняя за 60 дней",
            "ma90": "Средняя за 90 дней",
            "ma180": "Средняя за 180 дней",
            "ma360": "Средняя за 360 дней",
            "es": "Экспоненциальное сглаживание",
        }
        return titles.get(self.value, self.value)


class DemandMethod(str, Enum):
    """Методы расчёта потребности по складам."""

    AVERAGE = "average"
    DYNAMICS = "dynamics"
    HYBRID = "hybrid"
    PLAN_DISTRIBUTION = "plan_distribution"

    @property
    def title(self) -> str:
        """Человеческое название метода."""
        titles = {
            "average": "Среднесуточный спрос",
            "dynamics": "Динамика заказов",
            "hybrid": "Адаптивный гибрид",
            "plan_distribution": "Распределение плана",
        }
        return titles.get(self.value, self.value)


class TrafficMetric(str, Enum):
    """Метрики трафика."""

    UNITS = "ordered_units"
    REVENUE = "revenue"
    AVG_PRICE = "avg_price"
    CVR = "cvr"
    CTR = "ctr"


class ShipmentStatus(str, Enum):
    """Статусы для рекомендаций по отгрузкам."""

    DEFICIT = "DEFICIT"
    ENOUGH = "ENOUGH"
    SURPLUS = "SURPLUS"


class PurchaseStatus(str, Enum):
    """Статусы для рекомендаций по закупкам."""

    DEFICIT = "DEFICIT"
    ENOUGH = "ENOUGH"
    SURPLUS = "SURPLUS"


class StockMetric(str, Enum):
    """Метрики остатков на складе."""

    AVAILABLE_FOR_SALE = "available_for_sale"
    CHECKING = "checking"
    IN_TRANSIT = "in_transit"
    RESERVED = "reserved"
    RETURN_FROM_CUSTOMER = "return_from_customer_stock_count"
    VALID_STOCK_COUNT = "valid_stock_count"


class NoticeCode(str, Enum):
    """Коды уведомлений."""

    # Цели продаж
    GOAL_REVENUE_30D = "goal_revenue_30d"
    GOAL_UNITS_30D = "goal_units_30d"

    # План/факт
    PLAN_UNITS_30D = "plan_units_30d"
    PLAN_REVENUE_30D = "plan_revenue_30d"
    PLAN_AVGCHECK_30D = "plan_avgcheck_30d"

    FACT_UNITS_YDAY = "fact_units_yday"
    FACT_REVENUE_YDAY = "fact_revenue_yday"
    FACT_AVGCHECK_YDAY = "fact_avgcheck_yday"

    # Трафик
    CTR_YDAY = "ctr_yday"
    CONVERSION_YDAY = "conversion_yday"

    # Операционка
    NEED_TO_PURCHASE = "need_to_purchase"
    NEED_TO_SHIP = "need_to_ship"
    DEMAND_BY_SKU = "demand_by_sku"
    DELIVERY_STATS = "delivery_stats"
    SELLER_REMINDER = "seller_reminder"


# Literals для Type Hints
ForecastMethodLiteral = Literal["ma7", "ma14", "ma30", "ma60", "ma90", "ma180", "ma360", "es"]
DemandMethodLiteral = Literal["average", "dynamics", "hybrid", "plan_distribution"]
TrafficMetricLiteral = Literal["ordered_units", "revenue", "avg_price", "cvr", "ctr"]
