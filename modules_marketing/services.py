from __future__ import annotations
import logging
import aiohttp
from typing import List, Dict, Any
from config_package import settings

log = logging.getLogger("seller-bot.marketing")

# Используем /v1/promotion/list
OZON_API_URL_PROMO = "https://api-seller.ozon.ru/v1/promotion/list"

def _headers() -> Dict[str, str]:
    return {
        "Client-Id": settings.ozon_client_id,
        "Api-Key": settings.ozon_api_key,
        "Content-Type": "application/json",
    }

async def fetch_campaigns() -> List[dict]:
    """Получает список рекламных кампаний."""
    # Метод v1/promotion/list может быть устаревшим или ограниченным.
    # Если он не работает, можно попробовать v1/performance/campaigns (но там часто нужны доп права).
    # Пока пробуем этот как самый совместимый для Seller-token.
    payload = {
        "page": 0,
        "page_size": 100
    }
    
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(OZON_API_URL_PROMO, headers=_headers(), json=payload) as r:
                if r.status == 404: # Метод удален
                     log.warning("Promotion/list 404. API might have changed.")
                     return []
                     
                r.raise_for_status()
                data = await r.json()
                return data.get("result", {}).get("list", [])
        except Exception as e:
            log.error(f"Error fetching campaigns: {e}")
            return []
