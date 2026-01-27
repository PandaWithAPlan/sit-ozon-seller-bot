"""
Модульные тесты для modules_sales/sales_forecast.py
"""

import pytest
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from datetime import datetime, date, timedelta
from decimal import Decimal

# Настройка для импорта
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


@pytest.fixture
def mock_logger():
    """Мок для логгера."""
    with patch("modules_sales.sales_forecast.log") as mock_log:
        yield mock_log


@pytest.fixture
def mock_settings():
    """Мок для настроек."""
    with patch("modules_sales.sales_forecast.settings") as mock_settings:
        mock_settings.ozon_client_id = "test_client"
        mock_settings.ozon_api_key = "test_key"
        mock_settings.ozon_api_url = "https://test.api/v1/analytics/data"
        mock_settings.api_max_retries = 3
        mock_settings.api_base_pause = 0.1
        mock_settings.api_max_pause = 1.0
        mock_settings.api_jitter = 0.1
        mock_settings.products_mode = "SKU"
        mock_settings.parsed_watch_sku = [123456, 789012]
        mock_settings.parsed_watch_offers = []
        mock_settings.es_alpha = 0.3
        mock_settings.timezone = "Europe/Moscow"
        yield mock_settings


@pytest.mark.unit
class TestHeaders:
    """Тесты функции _headers."""

    def test_headers_content_type(self):
        """Проверяет наличие заголовка Content-Type."""
        from modules_sales.sales_forecast import _headers

        headers = _headers()

        assert "Content-Type" in headers
        assert headers["Content-Type"] == "application/json"
        assert "Client-Id" in headers
        assert "Api-Key" in headers
        assert "User-Agent" in headers

    def test_headers_user_agent(self):
        """Проверяет заголовок User-Agent."""
        from modules_sales.sales_forecast import _headers

        headers = _headers()

        assert "seller-bot/forecast" in headers["User-Agent"]


@pytest.mark.unit
class TestForecastMethods:
    """Тесты функций прогнозирования."""

    def test_list_forecast_methods(self):
        """Проверяет список методов прогноза."""
        from modules_sales.sales_forecast import list_forecast_methods

        methods = list_forecast_methods()

        assert len(methods) == 8
        method_codes = [code for code, _ in methods]
        assert "ma7" in method_codes
        assert "ma14" in method_codes
        assert "ma30" in method_codes
        assert "ma60" in method_codes
        assert "ma90" in method_codes
        "ma180" in method_codes
        assert "ma360" in method_codes
        assert "es" in method_codes

    def test_get_forecast_method(self):
        """Проверяет получение метода по умолчанию."""
        from modules_sales.sales_forecast import get_forecast_method, _DEFAULT_METHOD

        method = get_forecast_method()

        assert method == _DEFAULT_METHOD

    def test_set_forecast_method(self, tmp_path):
        """Проверяет сохранение метода."""
        from modules_sales.sales_forecast import set_forecast_method, PREFS_FILE
        from modules_sales.sales_forecast import get_forecast_method

        # Создаем временный файл настроек
        import json

        os.makedirs(os.path.dirname(PREFS_FILE), exist_ok=True)

        # Тестируем
        result = set_forecast_method("ma60")

        assert result is not None
        assert "60" in result

        # Проверяем чтение
        new_method = get_forecast_method()
        assert new_method == "ma60"

        # Очистка
        if os.path.exists(PREFS_FILE):
            os.remove(PREFS_FILE)


@pytest.mark.unit
class TestMAForecast:
    """Тесты методов скользящего среднего."""

    def test_ma_positive_values(self):
        """Тест MA с положительными значениями."""
        from modules_sales.sales_forecast import _ma

        values = [1.0, 2.0, 3.0, 4.0, 5.0]
        result = _ma(values, window=3)

        assert abs(result - 3.0) < 0.01

    def test_ma_window_size_1(self):
        """Тест MA с окном 1."""
        from modules_sales.sales_forecast import _ma

        values = [1.0, 2.0, 3.0]
        result = _ma(values, window=1)

        assert result == 3.0

    def test_ma_window_size_5(self):
        """Тест MA с окном 5."""
        from modules_sales.sales_forecast import _ma

        values = [1.0, 2.0, 3.0, 4.0, 5.0]
        result = _ma(values, window=5)

        assert abs(result - 3.0) < 0.01

    def test_ma_empty_list(self):
        """Тест MA с пустым списком."""
        from modules_sales.sales_forecast import _ma

        values = []
        result = _ma(values, window=5)

        assert result == 0.0

    def test_ma_window_larger_than_list(self):
        """Тест MA когда окно больше списка."""
        from modules_sales.sales_forecast import _ma

        values = [1.0, 2.0]
        result = _ma(values, window=10)

        assert abs(result - 1.5) < 0.01

    def test_ma_single_value(self):
        """Тест MA с одним значением."""
        from modules_sales.forecast import _ma

        values = [5.0]
        result = _ma(values, window=1)

        assert result == 5.0


@pytest.mark.unit
class TestESForecast:
    """Тесты методов экспоненциального сглаживания."""

    def test_es_start_value(self):
        """Тест ES когда первое значение считается как стартовое."""
        from modules_sales_forecast import _es

        values = [5.0, 6.0, 7.0, 8.0, 9.0]
        alpha = 0.5
        result = _es(values, alpha=alpha)

        # Ручит все значения, последнее будет доминировать
        assert 7.5 < result < 9.0

    def test_es_alpha_0(self):
        """Тест ES с alpha=0 (просто последнее значение)."""
        from modules_sales.forecast import _es

        values = [5.0, 6.0, 7.0]
        alpha = 0.0
        result = _es(values, alpha=alpha)

        assert result == 7.0

    def test_es_alpha_1(self):
        """Тест ES с alpha=1 (просто первое значение)."""
        from modules_sales.sales_forecast import _es

        values = [5.0, 6.0, 7.0]
        alpha = 1.0
        result = _es(values, alpha=alpha)

        assert result == 5.0

    def test_es_alpha_03(self):
        """Тест ES с alpha=0.3."""
        from modules_sales.forecast import _es

        values = [5.0, 6.0, 7.0]
        alpha = 0.3
        result = _es(values, alpha=alpha)

        # Промежуточное между 5 и 7
        assert 5.0 < result < 7.0

    def test_es_empty_list(self):
        """Тест ES с пустым списком."""
        from modules_sales.forecast import _es

        values = []
        result = _es(values, alpha=0.5)

        assert result == 0.0


@pytest.mark.unit
class TestWindowResolution:
    """Тесты функции _resolve_window_and_model."""

    def test_resolve_ma7(self):
        """Тест разрешения для MA7."""
        from modules.sales.forecast import _resolve_window_and_model

        mode, window = _resolve_window_and_model()

        assert mode == "ma"
        assert window == 7

    def test_resolve_ma30(self):
        """Тест разрешения для MA30."""
        from modules_sales.forecast import _resolve_window_and_model

        # Изменим метод
        from modules_sales.forecast import PREFS_FILE
        import json

        os.makedirs(os.path.dirname(PREFS_FILE), exist_ok=True)

        with open(PREFS_FILE, "w") as f:
            json.dump({"method": "ma30"}, f)

        from modules_sales.forecast import get_forecast_method

        mode, window = _resolve_window_and_model()

        assert mode == "ma"
        assert window == 30

        # Очистка
        os.remove(PREFS_FILE)


@pytest.mark.unit
class TestMetricNormalization:
    """Тесты функции _normalize_metric."""

    def test_normalize_metric_units(self):
        """Тест нормализации для юнитов."""
        from modules.sales.forecast import _normalize_metric

        result = _normalize_metric("units")
        assert result == "units"

        result = _normalize_metric("UNIT")
        assert result == "units"

        result = _normalize_metric("ЮНИТЫ")
        assert result == "units"

    def test_normalize_metric_revenue(self):
        """Тест нормализации для выручки."""
        from modules.sales.forecast import _normalize_metric

        result = _normalize_metric("revenue")
        assert result == "revenue"

        result = _normalize_metric("REV")
        assert result == "revenue"

        result = _normalize_metric("money")
        assert result == "revenue"

    def test_normalize_metric_avgprice(self):
        """Тест нормализации для среднего чека."""
        from modules_sales.forecast import _normalize_metric

        result = _normalize_metric("avgprice")
        assert result == "avgprice"

        result = _normalize_metric("avg_check")
        assert result == "avgprice"

        result = normalize_metric("средний чек")
        assert result == "avgprice"

    def test_normalize_metric_default(self):
        """Тест дефолтного значения."""
        from modules.sales.forecast import _normalize_metric

        result = _normalize_metric("unknown")
        assert result == "units"


@pytest.mark.unit
class TestDateFunctions:
    """Тесты функций работы с датами."""

    def test_today_local(self):
        """Проверяет получение сегодняшней даты."""
        from modules_sales.forecast import _today_local

        today = _today_local()

        assert isinstance(today, date)
        assert (datetime.now().date() - today).days < 1

    def test_yesterday_local(self):
        """Проверяет получение вчерашней даты."""
        from modules_sales.forecast import _yesterday_local

        yesterday = _yesterday_local()
        today = _today_local()

        assert (today - yesterday).days == 1

    def test_is_date_like_valid_date(self):
        """Проверяет распознавание валидной даты."""
        from modules.sales.forecast import _is_date_like

        assert _is_date_like("2024-01-15") is True
        assert _is_date_like("15.01.2024") is True
        assert _is_date_like("15-01-2024") is True
        assert _is_date_like("2024/01/15") is True
        assert _is_date_like("invalid") is False
        assert _is_date_like("") is False
        assert _is_date_like("2024-13-32") is False  # Неверный месяц
        assert _is_date_like("2024-02-30") is True  # Високосный год


@pytest.mark.unit
class TestFilterBuilding:
    """Тесты функций построения фильтров."""

    @patch("modules_sales.sales_forecast.WATCH_SKU", [123456, 789012])
    def test_build_filters_sku_mode(self):
        """Тест построения фильтров для SKU."""
        from modules.sales.forecast import _build_filters

        filters = _build_filters()

        assert len(filters) == 1
        assert filters[0]["key"] == "sku"
        assert filters[0]["operator"] == "IN"
        assert "123456" in filters[0]["value"]
        assert "789012" in filters[0]["value"]

    @patch("modules_sales.forecast.WATCH_OFFERS", ["offer1", "offer2"])
    def test_build_filters_offer_mode(self):
        """Тест построения фильтров для OFFER."""
        from modules.sales.forecast import _build_filters, PRODUCTS_MODE

        with patch("modules.sales.forecast.PRODUCTS_MODE", "OFFER"):
            filters = _build_filters()

            assert len(filters) == 1
            assert filters[0]["key"] == "offer_id"
            assert "offer1" in filters[0]["value"]
            assert "offer2" in filters[0]["value"]

    def test_build_filters_empty(self):
        """Тест когда нет SKU/OFFER."""
        from modules.sales.forecast import _build_filters, WATCH_SKU, WATCH_OFFERS, PRODUCTS_MODE

        with patch.multiple(
            "modules.sales.forecast", [patch.object(WATCH_SKU, "__iter__", return_value=iter([]))]
        ):
            with patch.object(WATCH_OFFERS, "__iter__", return_value=iter([])):
                with patch.object(PRODUCTS_MODE, "WATCH_SKU", ""):
                    filters = _build_filters()

                    assert len(filters) == 0


@pytest.mark.unit
class TestCacheFunctions:
    """Тесты функций кэширования."""

    @patch("modules_sales.forecast._HTTP_CACHE", {})
    def test_cache_key_stable(self):
        """Тест стабильности ключа кэша."""
        from modules.sales.forecast import _cache_key

        payload1 = {"filters": [{"key": "sku", "value": "123,456"}]}
        payload2 = {"filters": [{"key": "sku", "value": "123,456"}]}

        key1 = _cache_key(payload1)
        key2 = _cache_key(payload2)

        assert key1 == key2

    @patch("modules_sales.forecast._HTTP_CACHE", {})
    def test_cache_key_different(self):
        """Тест различия ключей кэша для разных payload."""
        from modules.sales.forecast import _cache_key

        payload1 = {"filters": [{"key": "sku", "value": "123,456"}]}
        payload2 = {"filters": [{"key": "sku", "value": "456,789"}]}

        key1 = _cache_key(payload1)
        key2 = _cache_key(payload2)

        assert key1 != key2

    @patch("modules.sales.forecast._HTTP_CACHE", {})
    @patch("modules.sales.sales.forecast._HTTP_CACHE_MAX", 5)
    def test_put_and_get_from_cache(self):
        """Тест записи и чтения из кэша."""
        from modules.sales.forecast import _put_to_cache, _get_from_cache

        key = "test_key"
        value = {"result": {"data": [{"sku": 123, "units": 10}]}}

        # Запись
        _put_to_cache(key, value)

        # Чтение
        retrieved = _get_from_cache(key)

        assert retrieved == value

        # Повторная запись
        _put_to_cache(key, {"result": {"data": [{"sku": 456, "units": 20}]}})
        retrieved2 = _get_from_cache(key)

        assert retrieved2 == {"result": {"data": [{"sku": 456, "units": 20}]}}

    @patch("modules.sales.forecast._HTTP_CACHE", {})
    def test_get_from_cache_miss(self):
        """Тест чтения несуществующего ключа."""
        from modules.sales.forecast import _get_from_cache

        result = _get_from_cache("nonexistent_key")

        assert result is None

    @patch("modules.sales.forecast._HTTP_CACHE", {})
    @patch("modules.sales.forecast._HTTP_CACHE_MAX", 3)
    def test_cache_max_size_limit(self):
        """Тест ограничения размера кэша."""
        from modules.sales.forecast import _put_to_cache

        # Заполняем кэш до лимита
        _put_to_cache("key1", {"data": []})
        _put_to_cache("key2", {"data": []})
        _put_to_cache("key3", {"data": []})

        # Четвый должна быть удален самый старый
        _put_to_cache("key4", {"data": []})

        from modules.sales.forecast import _HTTP_CACHE

        # Должно быть только 3 записи
        assert len(_HTTP_CACHE) == 3


@pytest.mark.unit
class TestRetryMechanism:
    """Тесты механизма повторных попыток."""

    @pytest.mark.asyncio
    async def test_sleep_with_backoff_with_retry_after(self):
        """Тест функции _sleep_with_backoff с заголовком Retry-After."""
        from modules_sales.forecast import (
            _sleep_with_backoff,
            SALES_API_BASE_PAUSE,
            SALES_API_MAX_PAUSE,
        )

        # Тест с заголовком
        start = pytest.importorskipif(time).time()
        _sleep_with_backoff(1, "60")
        elapsed = pytest.importorskipif(time).time() - start

        # Должно подождать 60 секунд
        assert 59 < elapsed < 61

    @pytest.mark.asyncio
    async def test_sleep_with_backoff_exponential(self, mock_logger):
        """Тест экспоненциальной задержки."""
        from modules.sales.forecast import _sleep_with_backoff, SALES_API_BASE_PAUSE

        import time

        # Попытка 2 - задержка должна быть в 2 раза больше базовой
        start = time.time()
        _sleep_with_backoff(2, None)
        elapsed = time.time() - start

        # Базовая пауза * 2^(2-1) = базовая * 2
        expected = SALES_API_BASE_PAUSE * 2
        assert abs(elapsed - expected) < 0.1

    @pytest.mark.asyncio
    async def test_sleep_with_backoff_max_limit(self, mock_logger):
        """Тест ограничения максимальной задержки."""
        from modules.sales_forecast import (
            _sleep_with_backoff,
            SALES_API_BASE_PAUSE,
            SALES_API_MAX_PAUSE,
        )

        import time

        # Попытка 10 с маленькой базовой паузой
        start = time.time()
        _sleep_with_backoff(10, None)
        elapsed = time.time() - start

        # Не должно превышать максимум
        assert elapsed <= SALES_API_MAX_PAUSE + 0.1


@pytest.mark.integration
@pytest.mark.slow
class TestPostAnalytics:
    """Интеграционные тесты функции _post_analytics."""

    @pytest.mark.asyncio
    async def test_post_analytics_success(self, mock_settings):
        """Тест успешного API запроса."""
        from modules.sales.forecast import _post_analytics
        from unittest.mock import patch, MagicMock

        payload = {
            "date_from": "2024-01-01",
            "date_to": "2024-01-31",
            "metrics": ["ordered_units"],
            "dimensions": ["day", "sku"],
            "filters": [],
        }

        with patch("modules.sales.forecast.requests.post") as mock_post:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {"result": {"data": []}}
            mock_post.return_value = mock_response

            result = _post_analytics(payload)

            assert result == {"result": {"data": []}}
            mock_post.assert_called_once()

    @pytest.mark.asyncio
    async def test_post_analytics_rate_limit(self, mock_settings):
        """Тест обработки HTTP 429."""
        from modules.sales.forecast import _post_analytics
        from unittest.mock import patch, MagicMock

        payload = {"test": "data"}

        with patch("modules.sales.forecast.requests.post") as mock_post:
            mock_response = MagicMock()
            mock_response.status_code = 429
            mock_response.headers = {"Retry-After": "5"}
            mock_post.return_value = mock_response

            with patch("modules.sales.forecast._sleep_with_backoff") as mock_sleep:
                result = _post_analytics(payload)

                # Должна быть вызвана повторная попытка
                assert mock_sleep.call_count >= 1

    @pytest.mark.asyncio
    async def test_post_analytics_timeout(self, mock_settings):
        """Тест обработки таймаута."""
        from modules.sales.forecast import _post_analytics
        from unittest.mock import patch

        payload = {"test": "data"}

        from requests import Timeout

        with patch("modules.sales.forecast.requests.post") as mock_post:
            mock_post.side_effect = Timeout("Request timeout")

            with patch("modules.sales.forecast._sleep_with_backoff"):
                result = _post_analytics(payload)

                assert result == {"result": {"data": []}}

    @pytest.mark.asyncio
    async def test_post_analytics_auth_error(self, mock_settings):
        """Тест обработки ошибки аутентификации."""
        from modules.sales.forecast import _post_analytics
        from unittest.mock import patch

        payload = {"test": "data"}

        from requests import HTTPError

        mock_response = MagicMock()
        mock_response.status_code = 401
        mock_response.text = "Unauthorized"

        error = HTTPError(response=mock_response)

        with patch("modules.sales.forecast.requests.post") as mock_post:
            mock_post.side_effect = error

            with patch("modules.sales.forecast._sleep_with_backoff"):
                result = _post_analytics(payload)

                # При 401 не должно быть повторных попыток
                assert result == {"result": {"data": []}}

    @pytest.mark.asyncio
    async def post_analytics_all_attempts_fail(self, mock_settings):
        """Тест когда все попытки неудачны."""
        from modules.sales.forecast import _post_analytics
        from unittest.mock import patch

        payload = {"test": "data"}

        from requests import RequestException

        with patch("modules.sales.forecast.requests.post") as mock_post:
            mock_post.side_effect = RequestException("Connection error")

            with patch("modules.sales.forecast._sleep_with_backoff"):
                result = _post_analytics(payload)

                # Должен вернуть fallback
                assert result == {"result": {"data": []}}
