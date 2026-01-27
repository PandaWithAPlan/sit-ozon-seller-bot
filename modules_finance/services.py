from __future__ import annotations
import logging
import datetime as dt
from typing import Dict, List, Any, Optional

import aiohttp
from config_package import settings

log = logging.getLogger("seller-bot.finance")

# Константы
OZON_API_URL_FINANCE = "https://api-seller.ozon.ru/v3/finance/transaction/list"

# Хелперы
def _headers() -> Dict[str, str]:
    return {
        "Client-Id": settings.ozon_client_id,
        "Api-Key": settings.ozon_api_key,
        "Content-Type": "application/json",
    }

async def fetch_transactions(
    date_from: dt.datetime, 
    date_to: dt.datetime, 
    transaction_type: str = "all"
) -> List[dict]:
    """
    Получает список транзакций за период.
    transaction_type: 'all', 'orders', 'returns', 'services' и т.д. (фильтрация на стороне клиента или API)
    """
    payload = {
        "filter": {
            "date": {
                "from": date_from.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                "to": date_to.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            },
            "transaction_type": transaction_type.upper() if transaction_type != "all" else "ALL"
        },
        "page": 1,
        "page_size": 100 
    }
    
    # API Ozon требует transaction_type в специфичном формате ENUM, 
    # для простоты пока оставим "ALL" или конкретные типы если знаем.
    # В v3 методе типы: ORDERS, RETURNS, SERVICES, OTHERS...
    if transaction_type == "all":
        payload["filter"]["transaction_type"] = "ALL"

    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(OZON_API_URL_FINANCE, headers=_headers(), json=payload) as r:
                r.raise_for_status()
                data = await r.json()
                return data.get("result", {}).get("operations", [])
        except Exception as e:
            log.error(f"Error fetching finance: {e}")
            return []

def calc_summary(transactions: List[dict]) -> Dict[str, float]:
    """Считает итоги по транзакциям."""
    summary = {
        "income": 0.0,   # Начисления
        "expense": 0.0,  # Списания (комиссии, логистика)
        "total": 0.0     # Итого
    }
    
    for tx in transactions:
        amount = float(tx.get("amount", 0.0) or 0.0)
        # Ozon отдает amount со знаком. + это начисление нам, - списание.
        if amount > 0:
            summary["income"] += amount
        else:
            summary["expense"] += amount
        summary["total"] += amount
            
    return summary
