from typing import List
from modules_sales.sales_facts_store import _fmt_money

def marketing_report_text(campaigns: List[dict]) -> str:
    if not campaigns:
        return "📢 <b>Маркетинг</b>\n\nАктивных кампаний не найдено (или метод недоступен)."

    active_count = 0
    lines = []
    
    # Сортируем: сначала активные
    # У кампании есть state / status
    # Пример поля: 'state': 'CAMPAIGN_STATE_RUNNING'
    
    sorted_cmps = sorted(campaigns, key=lambda x: x.get("state", ""), reverse=True)
    
    for c in sorted_cmps:
        c_id = c.get("id")
        title = c.get("title") or f"Кампания {c_id}"
        state = c.get("state", "UNKNOWN")
        budget = c.get("daily_budget")
        
        status_icon = "⚪️"
        if "RUNNING" in state:
            status_icon = "🟢"
            active_count += 1
        elif "PAUSED" in state:
            status_icon = "⏸"
        elif "FINISHED" in state or "ARCHIVED" in state:
            status_icon = "⚫️"
            
        budget_str = ""
        if budget:
            budget_str = f" | 💰 {budget}р/день"
            
        lines.append(f"{status_icon} <b>{title}</b>{budget_str}\n<small>{state}</small>")
        
    return (
        f"📢 <b>Рекламные кампании</b>\n"
        f"Всего: {len(campaigns)} | Активных: {active_count}\n\n" + 
        "\n".join(lines)
    )
