from typing import List, Dict, Tuple
import datetime as dt
from config_package import settings
from modules_sales import services
from modules_sales.sales_facts_store import get_alias_for_sku, _fmt_money, _fmt_units

async def forecast_text(period_days: int, metric: str = "units") -> str:
    """
    Генерирует текстовый отчет прогноза продаж.
    """
    # Нормализация метрики
    m = (metric or "units").strip().lower()
    if m in {"revenue", "rev", "money", "gmv"}: metric_norm = "revenue"
    elif m in {"avgprice", "avg_price"}: metric_norm = "avgprice"
    else: metric_norm = "units"

    head_metric = {"units": "ЮНИТЫ", "revenue": "ВЫРУЧКА", "avgprice": "СРЕДНИЙ ЧЕК"}[metric_norm]
    method_title = services.get_forecast_method_title()
    
    now_str = dt.datetime.now().strftime('%d.%m.%Y %H:%M')
    head = (
        f"📄 План продаж — {head_metric}\n"
        f"🧮 Метод: {method_title} • ⏱ Обновлено: {now_str}\n"
    )

    horizon = max(1, int(period_days))
    start = dt.date.today()
    end = start if horizon == 1 else (start + dt.timedelta(days=horizon - 1))
    
    if horizon == 1:
        period_line = f"📅 На сегодня: {start.strftime('%d.%m.%Y')}"
    else:
        period_line = f"📅 На {horizon} дней: {start.strftime('%d.%m.%Y')}–{end.strftime('%d.%m.%Y')}"

    # Получение данных через сервис
    daily = await services.fetch_series_from_api(max(60, 2 * 90))
    
    avg_price = {}
    if metric_norm in {"revenue", "avgprice"}:
        avg_price = await services.fetch_avg_price(30)

    lines = [head, period_line, ""]
    
    # Порядок и список определяются settings
    order = settings.parsed_watch_sku
    
    tot_val = 0.0
    sum_ap = 0.0
    cnt_ap = 0

    for sku in order:
        alias = get_alias_for_sku(sku) # Используем хелпер из стора (или utils)
        if not alias: alias = str(sku)
        
        # Если данных нет, считаем прогноз 0
        seq = daily.get(sku) or []
        u_sum, r_sum = services.calculate_forecast(seq, horizon)
        
        val_str = ""
        val = 0.0

        if metric_norm == "avgprice":
            ap = avg_price.get(sku, 0.0)
            val = ap
            val_str = _fmt_money(val)
            sum_ap += val
            cnt_ap += 1
        elif metric_norm == "revenue":
            ap = avg_price.get(sku, 0.0)
            # Если есть средний чек, считаем revenue = units_forecast * avg_price
            # (так было в оригинальной логике, чтобы прогноз денег зависел от units)
            if ap > 0:
                val = u_sum * ap
            else:
                val = r_sum
            val_str = _fmt_money(val)
            tot_val += val
        else: # units
            val = u_sum
            val_str = _fmt_units(val)
            tot_val += val
            
        lines.append(f"🔹 {alias}: {val_str}")

    lines.append("")
    if metric_norm == "avgprice":
        avg = (sum_ap / cnt_ap) if cnt_ap > 0 else 0.0
        lines.append(f"📊 СРЕДНЕЕ — {_fmt_money(avg)}")
    elif metric_norm == "revenue":
        lines.append(f"📊 ИТОГО — {_fmt_money(tot_val)}")
    else:
        lines.append(f"📊 ИТОГО — {_fmt_units(tot_val)}")

    return "\n".join(lines)
