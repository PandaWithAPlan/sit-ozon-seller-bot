from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from config_package import settings
from modules_sales import services as sales_services
from modules_common.cache_manager import WarehouseCache
from scheduler import NOTICE_REGISTRY
import os
import re

# ── Texts ────────────────────────────────────────────────────────────────────
def welcome_text() -> str:
    return (
        "👋 Привет! Я помогу Вам спланировать продажи, выкупы и отгрузки.\n\n"
        "📈 В разделе <b>«Продажи»</b> — помогают понять динамику: что продаётся лучше, где растёт конверсия и насколько выполнен план.\n"
        "🏷️ В разделе <b>«Выкупы»</b> — показывают, какие товары нужно докупить и где сейчас ваши заказы в пути или обработке.\n"
        "🚚 В разделе <b>«Отгрузки»</b> — подсказывают, какие позиции пора отгрузить на склад, чтобы не потерять продажи и держать запас под контролем.\n\n"
        "Выберите раздел:"
    )

# ── Keyboards ────────────────────────────────────────────────────────────────

def build_main_menu_kb() -> InlineKeyboardMarkup:
    try:
        from menu import main_menu
        return main_menu()
    except ImportError:
        pass
        
    rows = [
        [InlineKeyboardButton(text="📈 Продажи", callback_data="sales")],
        [InlineKeyboardButton(text="🏷️ Выкупы", callback_data="buyouts")],
        [InlineKeyboardButton(text="🚚 Отгрузки", callback_data="shipments")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)

def home_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="🏠 Домой", callback_data="nav:home")]]
    )

# ── Sales / Method ───────────────────────────────────────────────────────────

def build_method_kb() -> InlineKeyboardMarkup:
    current_code = sales_services.get_forecast_method()
    methods = dict(sales_services.list_forecast_methods())

    rows = []
    # Hardcoded layout based on typical options
    keys = ["ma7", "ma14", "ma30", "ma60", "ma90", "ma180", "ma360", "es"]
    chunked = [keys[i:i+2] for i in range(0, len(keys), 2)]
    
    for chunk in chunked:
        row = []
        for code in chunk:
            title = methods.get(code, code)
            mark = "✓ " if code == current_code else ""
            row.append(InlineKeyboardButton(text=f"{mark}{title}", callback_data=f"plan:method:set:{code}"))
        rows.append(row)
        
    rows.append([InlineKeyboardButton(text="🏠 Домой", callback_data="nav:home")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

# ── Warehouse ────────────────────────────────────────────────────────────────

WH_METHOD_TITLES = {
    "average": "Среднесуточный спрос",
    "dynamics": "Динамика заказов",
    "hybrid": "Адаптивный гибрид",
}
WH_PERIODS = (30, 60, 90, 180, 360)

def get_wh_prefs():
    data = WarehouseCache.get_prefs_manager().get_data()
    m = (data.get("method") or "average").strip().lower()
    p = int(data.get("period") or 90)
    return m, p

def build_warehouse_kb(method: str, period: int) -> InlineKeyboardMarkup:
    # rows logic similar to main.py
    m_rows = []
    for code in ["average", "dynamics", "hybrid"]:
        title = WH_METHOD_TITLES.get(code, code)
        mark = "✓ " if code == method else ""
        m_rows.append([InlineKeyboardButton(text=f"{mark}{title}", callback_data=f"wh:method:set:{code}")])

    p_rows = []
    p_list = [30, 60, 90]
    row1 = []
    for val in p_list:
         mark = "✓ " if val == period else ""
         row1.append(InlineKeyboardButton(text=f"{mark}{val} дн.", callback_data=f"wh:period:set:{val}"))
    p_rows.append(row1)
    
    p_list2 = [180, 360]
    row2 = []
    for val in p_list2:
         mark = "✓ " if val == period else ""
         row2.append(InlineKeyboardButton(text=f"{mark}{val} дн.", callback_data=f"wh:period:set:{val}"))
    p_rows.append(row2)

    tail = [[InlineKeyboardButton(text="🏠 Домой", callback_data="nav:home")]]
    return InlineKeyboardMarkup(inline_keyboard=m_rows + p_rows + tail)

# ── Notices ──────────────────────────────────────────────────────────────────

NOTICE_TITLES = {
    "goal_revenue_30d": "Необходимо заработать",
    "goal_units_30d": "Необходимо продать",
    "fact_units_yday": "Факт вчера (юниты)",
    "fact_revenue_yday": "Факт вчера (выручка)",
    "fact_avgcheck_yday": "Факт вчера (ср. чек)",
    "conversion_yday": "Конверсия вчера",
    "ctr_yday": "Кликабельность вчера",
    "plan_units_30d": "План 30д (юниты)",
    "plan_revenue_30d": "План 30д (выручка)",
    "plan_avgcheck_30d": "План 30д (ср. чек)",
    "need_to_purchase": "Необходимо закупить",
    "need_to_ship": "Необходимо отгрузить",
    "demand_by_sku": "Потребность по SKU",
    "delivery_stats": "Сроки доставки по SKU",
    "seller_reminder": "Напоминание об Excel (выкупы)",
}
NOTICE_ORDER = [
    ["goal_revenue_30d", "goal_units_30d"],
    ["need_to_purchase", "need_to_ship"],
    ["plan_units_30d", "fact_units_yday"],
    ["plan_revenue_30d", "fact_revenue_yday"],
    ["plan_avgcheck_30d", "fact_avgcheck_yday"],
    ["ctr_yday", "conversion_yday"],
    ["demand_by_sku", "delivery_stats"],
]

def _label_for_notice(code: str) -> str:
    # Simply lookup for now
    return NOTICE_TITLES.get(code, code)

def build_notice_kb() -> InlineKeyboardMarkup:
    rows = []
    # Digests
    rows.append([InlineKeyboardButton(text=settings.notice_digest_short_title, callback_data="notice:send:short")])
    rows.append([InlineKeyboardButton(text=settings.notice_digest_title, callback_data="notice:send:all")])

    for pair in NOTICE_ORDER:
        btns = []
        for code in pair:
            if code in NOTICE_REGISTRY:
                btns.append(InlineKeyboardButton(text=_label_for_notice(code), callback_data=f"notice:send:{code}"))
        if btns: rows.append(btns)

    rows.append([InlineKeyboardButton(text="Напоминание об Excel", callback_data="notice:send:seller_reminder")])
    rows.append([InlineKeyboardButton(text="ℹ️ Список кодов", callback_data="notice:list")])
    rows.append([InlineKeyboardButton(text="🏠 Домой", callback_data="nav:home")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def codes_list_text() -> str:
    lines = []
    seen = set()
    for row in NOTICE_ORDER:
        for code in row:
            if code in NOTICE_REGISTRY:
                lines.append(f"<code>{code}</code> — {_label_for_notice(code)}")
                seen.add(code)
    lines.append(f"<code>seller_reminder</code> — Напоминание об Excel")
    return "🔔 <b>Доступные уведомления</b>\n" + "\n".join(f"• {l}" for l in lines)
