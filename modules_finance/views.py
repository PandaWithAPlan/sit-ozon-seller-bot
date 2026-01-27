from typing import List, Dict
from .services import calc_summary
from modules_sales.sales_facts_store import _fmt_money # Переиспользуем форматтер

def finance_report_text(transactions: List[dict], period_name: str) -> str:
    if not transactions:
        return f"💰 <b>Финансы — {period_name}</b>\n\nТранзакций не найдено."

    summary = calc_summary(transactions)
    
    # Последние 5 транзакций
    last_txs = sorted(transactions, key=lambda x: x.get("operation_date", ""), reverse=True)[:5]
    
    tx_lines = []
    for tx in last_txs:
        date_str = tx.get("operation_date", "")[:10]
        t_type = tx.get("type_name") or tx.get("operation_type_name") or "Операция"
        amt = float(tx.get("amount", 0.0))
        msk = "🟢" if amt >= 0 else "🔴"
        tx_lines.append(f"{msk} {date_str}: {_fmt_money(amt)}\n<small>{t_type}</small>")
        
    income = summary["income"]
    expense = summary["expense"]
    total = summary["total"]
    
    txt = (
        f"💰 <b>Финансы — {period_name}</b>\n"
        f"Всего операций: {len(transactions)}\n\n"
        f"📥 Начисления: {_fmt_money(income)}\n"
        f"📤 Удержания: {_fmt_money(expense)}\n"
        f"<b>💰 ИТОГО: {_fmt_money(total)}</b>\n\n"
        f"📋 <b>Последние операции:</b>\n" + 
        "\n".join(tx_lines)
    )
    return txt
