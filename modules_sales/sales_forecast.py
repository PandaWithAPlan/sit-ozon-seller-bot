from __future__ import annotations
import datetime as dt
from typing import List, Tuple, Dict, Any

# Реэкспорт сервисов и представлений
from modules_sales.services import (
    list_forecast_methods,
    get_forecast_method,
    set_forecast_method,
    get_forecast_method_title,
    fetch_series_from_api as _fetch_series_from_api, # алиас для внутренней совместимости
    ES_ALPHA,
)
from modules_sales.views import forecast_text

# Для обратной совместимости, если кто-то импортирует эти приватные функции
async def _fetch_series(days_back: int = 60) -> Dict[int, List[Tuple[dt.date, float, float]]]:
    return await _fetch_series_from_api(days_back)
