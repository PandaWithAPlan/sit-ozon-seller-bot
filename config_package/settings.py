"""
Настройки приложения (Pydantic Settings).

Содержит валидацию всех параметров из .env файла.
"""

import os
from pathlib import Path
from typing import List, Optional

from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field, field_validator, model_validator

from .constants import (
    ForecastMethod,
    DemandMethod,
    NoticeCode,
    ForecastMethodLiteral,
    DemandMethodLiteral,
)


class Settings(BaseSettings):
    """Настройки приложения с валидацией."""

    model_config = SettingsConfigDict(
        env_file=os.path.join(os.path.dirname(__file__), "..", ".env"),
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # ===== Telegram =====
    telegram_token: str = Field(alias="TELEGRAM_TOKEN", description="Токен Telegram бота")
    bot_token: Optional[str] = Field(default=None, alias="BOT_TOKEN")
    chat_ids: str = Field(
        default="", alias="CHAT_IDS", description="ID чатов для уведомлений (через запятую)"
    )

    # ===== Ozon API =====
    ozon_client_id: str = Field(alias="OZON_CLIENT_ID", description="Client ID Ozon API")
    ozon_api_key: str = Field(alias="OZON_API_KEY", description="API Key Ozon API")
    ozon_company_id: str = Field(
        default="", alias="OZON_COMPANY_ID", description="Company ID Ozon API"
    )
    ozon_api_url: str = Field(
        default="https://api-seller.ozon.ru/v1/analytics/data",
        alias="OZON_API_URL",
        description="URL Ozon API",
    )

    # ===== API настройки =====
    api_max_retries: int = Field(default=6, alias="SALES_API_MAX_RETRIES", ge=1, le=20)
    api_base_pause: float = Field(default=0.6, alias="SALES_API_BASE_PAUSE", gt=0)
    api_max_pause: float = Field(default=5.0, alias="SALES_API_MAX_PAUSE", gt=0)
    api_jitter: float = Field(default=0.35, alias="SALES_API_JITTER", ge=0, le=1)

    # ===== Фильтрация SKU =====
    watch_sku: str = Field(default="",                               alias="WATCH_SKU",                               description="Список SKU для отслеживания")
    watch_offers: str = Field(
        default="", alias="WATCH_OFFERS", description="Список offer_id для отслеживания"
    )
    products_mode: str = Field(default="SKU", alias="PRODUCTS_MODE")

    # ===== Прогноз продаж =====
    es_alpha: float = Field(default=0.3, alias="ES_ALPHA", ge=0, le=1)
    alert_plan_horizon_days: int = Field(default=30, alias="ALERT_PLAN_HORIZON_DAYS", ge=1, le=365)

    # ===== Покупки (выкупы) =====
    purchases_xlsx_name: str = Field(default="Товары.xlsx", alias="PURCHASES_XLSX_NAME")
    purchases_template_name: str = Field(
        default="Товары_шаблон.xlsx", alias="PURCHASES_TEMPLATE_NAME"
    )
    buy_coef: float = Field(default=5.0, alias="BUY_COEF", gt=0)

    # Пороги светофора покупок
    buy_red_factor: float = Field(default=4.0, alias="BUY_RED_FACTOR", gt=0)
    buy_yellow_factor: float = Field(default=4.25, alias="BUY_YELLOW_FACTOR", gt=0)
    buy_max_factor: float = Field(default=5.0, alias="BUY_MAX_FACTOR", gt=0)
    prof_yellow_factor: float = Field(default=5.25, alias="PROF_YELLOW_FACTOR", gt=0)
    prof_red_factor: float = Field(default=5.5, alias="PROF_RED_FACTOR", gt=0)

    # Флаги
    purchases_need_lights_enabled: bool = Field(default=True,                                                    alias="PURCHASES_NEED_LIGHTS_ENABLED")

    # ===== Отгрузки =====
    ship_round_step: int = Field(default=2, alias="SHIP_ROUND_STEP", ge=1, le=100)
    ship_safety_coef: float = Field(default=2.0, alias="SHIP_SAFETY_COEF", gt=0)

    # Пороги светофора отгрузок
    ship_red_factor: float = Field(default=1.5, alias="SHIP_RED_FACTOR_SHIP", gt=0)
    ship_yellow_factor: float = Field(default=1.75, alias="SHIP_YELLOW_FACTOR_SHIP", gt=0)
    ship_green_factor: float = Field(default=2.0, alias="SHIP_GREEN_FACTOR_SHIP", gt=0)
    ship_max_factor: float = Field(default=2.0, alias="SHIP_MAX_FACTOR_SHIP", gt=0)

    prof_ship_green_factor: float = Field(default=2.0, alias="PROF_SHIP_GREEN_FACTOR_SHIP", gt=0)
    prof_ship_yellow_factor: float = Field(default=2.25,                                               alias="PROF_SHIP_YELLOW_FACTOR_SHIP",                                               gt=0)
    prof_ship_red_factor: float = Field(default=2.5, alias="PROF_SHIP_RED_FACTOR_SHIP", gt=0)

    # Флаги
    shipments_need_lights_enabled: bool = Field(default=True,                                                    alias="SHIPMENTS_NEED_LIGHTS_ENABLED")
    demand_clear_on_period_change: bool = Field(
        default=False, alias="DEMAND_CLEAR_ON_PERIOD_CHANGE"
    )
    demand_eps_strict: float = Field(default=0.05, alias="DEMAND_EPS_STRICT", ge=0)

    # ===== Цели продаж =====
    goal_red_factor_high: float = Field(default=1.20, alias="GOAL_RED_FACTOR_HIGH", gt=0)
    goal_yellow_factor_high: float = Field(default=1.10, alias="GOAL_YELLOW_FACTOR_HIGH", gt=0)
    goal_green_tol: float = Field(default=0.02, alias="GOAL_GREEN_TOL", ge=0, le=1)
    goal_yellow_factor_low: float = Field(default=0.95, alias="GOAL_YELLOW_FACTOR_LOW", gt=0)
    goal_red_factor_low: float = Field(default=0.90, alias="GOAL_RED_FACTOR_LOW", gt=0)

    # Флаги
    goal_lights_enabled: bool = Field(default=True, alias="GOAL_LIGHTS_ENABLED")

    # ===== Планировщик (Scheduler) =====
    daily_notices_weekday_at: str = Field(default="08:45", alias="DAILY_NOTICES_WEEKDAY_AT")
    daily_notices_weekend_at: str = Field(default="10:00", alias="DAILY_NOTICES_WEEKEND_AT")
    daily_notices_weekday_pm_at: str = Field(default="17:45", alias="DAILY_NOTICES_WEEKDAY_PM_AT")
    daily_notices_weekend_pm_at: str = Field(default="17:45", alias="DAILY_NOTICES_WEEKEND_PM_AT")
    full_digest_weekday_at: str = Field(default="10:00", alias="FULL_DIGEST_WEEKDAY_AT")
    notify_spread_sec: int = Field(default=8, alias="NOTIFY_SPREAD_SEC", ge=0)

    # Получатели уведомлений
    notices_prefer_local: bool = Field(default=True, alias="NOTICES_PREFER_LOCAL")

    # Названия дайджестов
    notice_digest_short_title: str = Field(
        default="🗞️ Сокращенный дайджест", alias="NOTICE_DIGEST_SHORT_TITLE"
    )
    notice_digest_title: str = Field(
        default="📬 Весь утренний дайджест", alias="NOTICE_DIGEST_TITLE"
    )

    # Флаг использования целей продаж в отчётах
    operations_use_sales_goal: bool = Field(default=True, alias="OPERATIONS_USE_SALES_GOAL")

    # ===== UI настройки =====
    cb_dedup_window_ms: int = Field(default=800, alias="CB_DEDUP_WINDOW_MS", ge=0, le=5000)

    # ===== Дата/Время =====
    timezone: str = Field(default="Europe/Moscow", alias="TZ")

    # ===== Пути =====
    base_dir: Path = Field(default_factory=lambda: Path(__file__).parent.parent)

    # ===== Городские настройки =====
    city1: str = Field(default="Москва", alias="CITY1")
    city2: str = Field(default="Хабаровск", alias="CITY2")
    city_count: int = Field(default=2, alias="CITY_COUNT", ge=1, le=2)

    # ===== Валидаторы =====

    @field_validator("telegram_token", mode="before")
    @classmethod
    def validate_token(cls, v: Optional[str]) -> str:
        """Валидация токена: приоритет TELEGRAM_TOKEN над BOT_TOKEN."""
        if not v or not v.strip():
            raise ValueError("TELEGRAM_TOKEN обязателен")
        return v.strip()

    @model_validator(mode="before")
    @classmethod
    def merge_bot_token(cls, data: dict) -> dict:
        """Объединяет TELEGRAM_TOKEN и BOT_TOKEN (старая совместимость)."""
        telegram_token = data.get("TELEGRAM_TOKEN") or data.get("telegram_token")
        bot_token = data.get("BOT_TOKEN") or data.get("bot_token")

        if bot_token and not telegram_token:
            data["TELEGRAM_TOKEN"] = bot_token
            data["telegram_token"] = bot_token
        return data

    @field_validator("products_mode", mode="before")
    @classmethod
    def validate_products_mode(cls, v: str) -> str:
        """Валидация режима продуктов (SKU или OFFER)."""
        v_upper = v.upper().strip()
        if v_upper not in ("SKU", "OFFER"):
            raise ValueError('PRODUCTS_MODE должен быть "SKU" или "OFFER"')
        return v_upper

    @field_validator("city_count", mode="before")
    @classmethod
    def validate_city_count(cls, v: int, info) -> int:
        """Автоматически определяет количество городов."""
        city1 = info.data.get("city1", "").strip()
        city2 = info.data.get("city2", "").strip()

        # Если city2 не задан или пустой - один город
        if not city2:
            return 1
        return v

    @field_validator("chat_ids", mode="before")
    @classmethod
    def validate_chat_ids(cls, v: str) -> str:
        """Валидация списка chat_ids."""
        if not v:
            return ""
        # Проверка формата: список чисел через запятую
        tokens = v.replace("\n", ",").replace(" ", ",").split(",")
        for token in tokens:
            token = token.strip()
            if token and not token.lstrip("-").isdigit():
                raise ValueError(
                    f"CHAT_IDS должен содержать только числа через запятую. Неверный: {token}"
                )
        return v

    @field_validator("timezone", mode="before")
    @classmethod
    def validate_timezone(cls, v: str) -> str:
        """Валидация часового пояса."""
        if not v:
            return "Europe/Moscow"
        # Простая проверка на формат IANA
        if "/" not in v:
            raise ValueError(
                f"TZ должен быть в формате IANA (например, Europe/Moscow). Получено: {v}"
            )
        return v.strip()

    # ===== Свойства =====

    @property
    def effective_token(self) -> str:
        """Эффективный токен бота (backward compatibility)."""
        return self.telegram_token or self.bot_token or ""

    @property
    def parsed_chat_ids(self) -> List[int]:
        """Парсит список chat_ids в список int."""
        if not self.chat_ids:
            return []

        result = []
        seen = set()
        for token in self.chat_ids.replace("\n", ",").split(","):
            token = token.strip()
            if not token:
                continue
            try:
                chat_id = int(token)
                if chat_id != 0 and chat_id not in seen:
                    result.append(chat_id)
                    seen.add(chat_id)
            except ValueError:
                continue
        return result

    @property
    def parsed_watch_sku(self) -> List[int]:
        """Парсит список SKU в список int."""
        if not self.watch_sku:
            return []

        result = []
        seen = set()
        for token in self.watch_sku.replace("\n", ",").split(","):
            token = token.strip()
            if not token:
                continue
            # Поддержка формата "sku:alias"
            sku_part = token.split(":")[0].strip()
            try:
                sku = int(sku_part)
                if sku not in seen:
                    result.append(sku)
                    seen.add(sku)
            except ValueError:
                continue
        return result

    @property
    def parsed_watch_offers(self) -> List[str]:
        """Парсит список offers в список строк."""
        if not self.watch_offers:
            return []

        return [t.strip() for t in self.watch_offers.replace("\n", ",").split(",") if t.strip()]

    @property
    def data_dir(self) -> Path:
        """Директория с данными."""
        return self.base_dir / "data"

    @property
    def cache_dir(self) -> Path:
        """Директория с кэшем."""
        return self.base_dir / "data" / "cache"

    @property
    def sales_cache_dir(self) -> Path:
        """Директория с кэшем продаж."""
        return self.cache_dir / "sales"

    @property
    def shipments_cache_dir(self) -> Path:
        """Директория с кэшем отгрузок."""
        return self.base_dir / "data" / "cache" / "shipments"

    # ===== Городская конфигурация =====

    @property
    def city_config(self) -> dict:
        """Возвращает конфигурацию городов."""
        return {
            "city1": self.city1,
            "city2": self.city2,
            "count": self.city_count,
        }

    def get_forecast_method(self) -> ForecastMethod:
        """Возвращает метод прогноза по умолчанию."""
        return ForecastMethod.MA30

    def get_demand_method(self) -> DemandMethod:
        """Возвращает метод расчёта потребности по умолчанию."""
        return DemandMethod.AVERAGE

    def get_demand_period(self) -> int:
        """Возвращает период расчёта потребности по умолчанию."""
        return self.alert_plan_horizon_days

    def validate_on_startup(self) -> None:
        """
        Проверяет критичные настройки при старте.
        Выбрасывает ValueError при ошибках.
        """
        errors = []

        if not self.effective_token:
            errors.append("TELEGRAM_TOKEN или BOT_TOKEN обязателен")

        if not self.ozon_client_id:
            errors.append("OZON_CLIENT_ID обязателен")

        if not self.ozon_api_key:
            errors.append("OZON_API_KEY обязателен")

        if errors:
            raise ValueError(f"Ошибки конфигурации:\n" + "\n".join(f"  • {e}" for e in errors))


# ===== Глобальный инстанс =====
_settings_instance: Optional[Settings] = None


def get_settings() -> Settings:
    """Возвращает глобальный инстанс настроек."""
    global _settings_instance
    if _settings_instance is None:
        _settings_instance = Settings()
    return _settings_instance


def reload_settings() -> Settings:
    """Перезагружает настройки из .env файла."""
    global _settings_instance
    _settings_instance = Settings()
    return _settings_instance


# ===== Быстрый доступ =====
settings = get_settings()
