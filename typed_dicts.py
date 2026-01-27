"""
TypedDict для структур данных приложения.

Используется для типизации данных в расчётных функциях и API запросах.
"""

from typing import TypedDict, Optional
from datetime import date
from config_package.constants import (
    ForecastMethodLiteral,
    DemandMethodLiteral,
    TrafficMetricLiteral,
)

# ===== Sales =====


class SalesSeriesRecord(TypedDict):
    """Одна запись временного ряда продаж."""

    date: date
    units: float
    revenue: float


class ForecastResult(TypedDict):
    """Результат прогноза продаж."""

    sku: int
    units_forecast: float
    revenue_forecast: float
    method: ForecastMethodLiteral
    period_days: int
    horizon: int


class SalesFactsPayload(TypedDict):
    """Пейлоад с фактическими данными продаж."""

    metric: TrafficMetricLiteral
    period_days: int
    sku_data: dict[int, SalesSeriesRecord]
    total_units: float
    total_revenue: float


# ===== Goals =====


class GoalEntry(TypedDict):
    """Запись цели по SKU."""

    sku: int
    goal_per_day: float
    goal_30d: float


class GoalComparisonResult(TypedDict):
    """Результат сравнения факта с целью."""

    sku: int
    alias: str
    fact_30d: float
    plan_30d: float
    goal_30d: float
    target_30d: float
    ratio: float
    delta: float
    status: str
    light: str


# ===== Purchases =====


class SellerStockRecord(TypedDict):
    """Запись остатка у продавца."""

    sku: int
    buying: float
    delivering: float
    processing: float
    total: float


class OzonStockRecord(TypedDict):
    """Запись остатка на складах Ozon (сумма 6 метрик)."""

    sku: int
    total: float
    available_for_sale: float
    checking: float
    in_transit: float
    reserved: float
    return_from_customer: float
    valid_stock: float


class PurchaseRecommendation(TypedDict):
    """Рекомендация по закупкам."""

    sku: int
    alias: str
    seller_total: float
    ozon_total: float
    plan_30d: float
    need_qty: float
    covered_qty: float
    coverage_plan: float
    action: str
    delta: float
    status: str
    light: str


class PurchaseReportPayload(TypedDict):
    """Пейлоад отчёта по закупкам."""

    horizon_days: int
    method: str
    buy_coef: float
    items: list[PurchaseRecommendation]
    total_deficit: float
    total_surplus: float


# ===== Shipments =====


class Stock6Metrics(TypedDict):
    """6 метрик остатка на складе Ozon."""

    available_for_sale: float
    checking: float
    in_transit: float
    reserved: float
    return_from_customer_stock_count: float
    valid_stock_count: float

    @property
    def total(self) -> float:
        """Сумма всех 6 метрик."""
        return (
            self.get("available_for_sale", 0.0)
            + self.get("checking", 0.0)
            + self.get("in_transit", 0.0)
            + self.get("reserved", 0.0)
            + self.get("return_from_customer_stock_count", 0.0)
            + self.get("valid_stock_count", 0.0)
        )


class ShipmentRecommendation(TypedDict):
    """Рекомендация по отгрузкам."""

    sku: int
    alias: str
    title: str
    dest: str
    dest_name: Optional[str]
    dest_id: Optional[int]

    d: float
    l: int
    s: int

    plan30: float
    coef: float
    base_need: float
    upper_need: float

    stock: float
    r: float

    block: str
    sub: str
    action: str
    qty: int

    qty_wh: int
    qty_closed: int
    qty_l: int
    qty_s: int

    closed_wh: int
    closed_mark: int


class ShipmentReportPayload(TypedDict):
    """Пейлоад отчёта по отгрузкам."""

    updated_at: str
    scope: str
    lines: list[ShipmentRecommendation]
    groups: dict[str, list[ShipmentRecommendation]]
    plan_method_title: str
    wh_method: str
    wh_period_days: int
    dispatch_days: int
    goal_mode: int


# ===== Demand =====


class WarehouseDemand(TypedDict):
    """Спрос по складу."""

    warehouse_id: int
    warehouse_name: str
    cluster_id: int
    cluster_name: str
    d: float
    period_days: int
    l: int


class DemandReportPayload(TypedDict):
    """Пейлоад отчёта по потребности."""

    method: DemandMethodLiteral
    period_days: int
    warehouse_demands: list[WarehouseDemand]
    sku_demands: dict[int, list[WarehouseDemand]]
    total_demand: float


# ===== Lead Time =====


class LeadTimeStats(TypedDict):
    """Статистика сроков доставки."""

    sku: int
    alias: str
    avg_lead_days: float
    min_lead_days: float
    max_lead_days: float
    orders_count: int
    warehouse_id: Optional[int]
    warehouse_name: Optional[str]


class LeadTimeReportPayload(TypedDict):
    """Пейлоад отчёта по срокам доставки."""

    view: str
    stats: list[LeadTimeStats]


# ===== API =====


class OzonAPIRequest(TypedDict):
    """Запрос к Ozon API."""

    date_from: str
    date_to: str
    metrics: list[str]
    dimension: list[str]
    filters: list[dict]
    limit: int
    offset: int


class OzonAPIResponse(TypedDict):
    """Ответ от Ozon API."""

    result: dict
    data: list


# ===== Notices =====


class NoticePayload(TypedDict):
    """Пейлоад уведомления."""

    code: str
    text: str
    timestamp: str
    chat_ids: list[int]


class DigestPayload(TypedDict):
    """Пейлоад дайджеста."""

    notice_codes: list[str]
    short: bool
    timestamp: str
    chat_ids: list[int]


# ===== General =====


class APIError(TypedDict):
    """Ошибка API."""

    message: str
    status_code: int
    endpoint: str
    retry_after: Optional[int]


class CacheEntry(TypedDict):
    """Запись в кэше."""

    key: str
    data: dict
    timestamp: str
    ttl: int
