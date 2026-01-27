from typing import List
from modules_sales.sales_facts_store import get_alias_for_sku, _fmt_money

def prices_report_text(items: List[dict]) -> str:
    if not items:
        return "🏷 <b>Операции — Цены</b>\n\nНет данных для отображения."

    lines = []
    
    # Сортируем: сначала те что в WATCH_SKU по порядку (если получится), иначе просто по имени
    # Здесь просто по порядку ответа API
    
    for item in items:
        p_id = item.get("product_id") or 0
        try: sku = int(p_id) 
        except: sku = 0
            
        alias = get_alias_for_sku(sku) or str(sku)
        price_info = item.get("price", {})
        
        price = float(price_info.get("price", 0) or 0)
        old_price = float(price_info.get("old_price", 0) or 0)
        marketing_price = float(price_info.get("marketing_price", 0) or 0) # Цена с учетом акций Ozon
        
        # Индикаторы
        icon = "🔹"
        price_str = f"{_fmt_money(price)}"
        
        if marketing_price > 0 and marketing_price < price:
             price_str += f" (Ozon: {_fmt_money(marketing_price)})"
             
        lines.append(f"{icon} <b>{alias}</b>: {price_str}")
        
    return (
        f"🏷 <b>Текущие цены (Action)</b>\n"
        f"Товаров: {len(items)}\n\n" + 
        "\n".join(lines) + 
        "\n\n<i>Изменение цен пока недоступно в этой версии.</i>"
    )
