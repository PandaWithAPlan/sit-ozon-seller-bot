from __future__ import annotations
import logging
import asyncio
from typing import List, Dict, Any

import aiohttp
from config_package import settings

log = logging.getLogger("seller-bot.operations")

OZON_API_URL_PRICES = "https://api-seller.ozon.ru/v4/product/info/prices"

def _headers() -> Dict[str, str]:
    return {
        "Client-Id": settings.ozon_client_id,
        "Api-Key": settings.ozon_api_key,
        "Content-Type": "application/json",
    }

async def fetch_prices(skus: List[int] = None) -> List[dict]:
    """
    Получает информацию о ценах.
    Если skus не предан, берет из WATCH_SKU.
    """
    targets = skus or settings.parsed_watch_sku
    # Лимит 1000, если больше - надо батчить. Пока предполагаем < 1000
    if not targets:
        return []

    # API принимает filter.product_id (список строк)
    payload = {
        "filter": {
            "product_id": [str(x) for x in targets],
            "visibility": "ALL"
        },
        "limit": 1000
    }
    
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(OZON_API_URL_PRICES, headers=_headers(), json=payload) as r:
                r.raise_for_status()
                data = await r.json()
                return data.get("result", {}).get("items", [])
        except Exception as e:
            log.error(f"Error fetching prices: {e}")
            return []
