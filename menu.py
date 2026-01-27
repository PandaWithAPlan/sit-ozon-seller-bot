# menu.py
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

# ========== Общие ==========


def back_home_menu() -> InlineKeyboardMarkup:
    """Кнопки «Назад» и «Домой»."""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="nav:back")],
            [InlineKeyboardButton(text="🏠 Домой", callback_data="nav:home")],
        ]
    )


# ========== Главное меню ==========
def main_menu() -> InlineKeyboardMarkup:
    """Главное меню: Продажи / Выкупы / Отгрузки + Новые разделы."""
    kb = [
        [InlineKeyboardButton(text="📈 Продажи", callback_data="sales")],
        [InlineKeyboardButton(text="🏷️ Закупки", callback_data="buyouts")],
        [InlineKeyboardButton(text="🚚 Отгрузки", callback_data="shipments")],
        # Новые разделы
        [
             InlineKeyboardButton(text="💰 Финансы", callback_data="menu:finance"),
             InlineKeyboardButton(text="🏷 Цены", callback_data="menu:prices")
        ],
        [
             InlineKeyboardButton(text="📢 Маркетинг", callback_data="menu:marketing")
        ]
    ]
    return InlineKeyboardMarkup(inline_keyboard=kb)


# ========== Продажи ==========
def sales_menu() -> InlineKeyboardMarkup:
    """Меню раздела Продажи (порядок: Цель → План → Факт → Отчёты)."""
    kb = [
        [InlineKeyboardButton(text="🎯 Цель продаж", callback_data="sales:goal")],
        [InlineKeyboardButton(text="📈 План продаж", callback_data="sales:plan")],
        [InlineKeyboardButton(text="📊 Факт продаж", callback_data="sales:facts")],
        [InlineKeyboardButton(text="🧾 Отчёт по продажам", callback_data="sales:report")],
        # ↓ Новый пункт для подраздела «Отчёт по выкупам»
        [InlineKeyboardButton(text="🧾 Отчёт по выкупам", callback_data="sales:buyout")],
        [InlineKeyboardButton(text="🏠 Домой", callback_data="nav:home")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=kb)


# ——— Цель продаж
def sales_goal_menu() -> InlineKeyboardMarkup:
    """Корень раздела «Цель продаж»: выбор метрики."""
    rows = [
        [InlineKeyboardButton(text="📦 По юнитам", callback_data="sales:goal:report:units")],
        [InlineKeyboardButton(text="💰 По выручке", callback_data="sales:goal:report:revenue")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="sales")],
        [InlineKeyboardButton(text="🏠 Домой", callback_data="nav:home")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def sales_goal_report_menu(horizon: int, metric: str) -> InlineKeyboardMarkup:
    """
    Меню под отчётом «ЦЕЛЬ ПРОДАЖ — РЕКОМЕНДАЦИИ»:
        • переключение горизонта (За день / За 30 дней)
        • ✏️ Изменить цель
        • Назад/Домой
    """
    h = 1 if int(horizon) == 1 else 30
    # отмечаем активный горизонт галочкой
    btn_day = InlineKeyboardButton(
        text=("✓ За день" if h == 1 else "За день"), callback_data="sales:goal:horizon:1"
    )
    btn_30 = InlineKeyboardButton(
        text=("✓ За 30 дней" if h == 30 else "За 30 дней"), callback_data="sales:goal:horizon:30"
    )

    rows = [
        [btn_day, btn_30],
        [InlineKeyboardButton(text="✏️ Изменить цель", callback_data="sales:goal:edit:list:0")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="sales:goal")],
        [InlineKeyboardButton(text="🏠 Домой", callback_data="nav:home")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


# ========== Факт продаж ==========
def facts_metric_menu() -> InlineKeyboardMarkup:
    """Выбор метрики для факта продаж."""
    metrics = [
        ("📦 Юниты", "units"),
        ("💰 Выручка", "revenue"),
        ("🧾 Средний чек", "avgprice"),
        ("🖱️ Кликабельность", "ctr"),
        ("✅ Конверсия", "cvr"),
    ]
    kb = [
        [InlineKeyboardButton(text=title, callback_data=f"facts:metric:{code}")]
        for title, code in metrics
    ]
    kb.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="sales")])
    kb.append([InlineKeyboardButton(text="🏠 Домой", callback_data="nav:home")])
    return InlineKeyboardMarkup(inline_keyboard=kb)


def facts_period_menu(metric: str) -> InlineKeyboardMarkup:
    """Выбор периода для факта продаж."""
    periods = [
        ("📅 Сегодня", 0),
        ("📆 Вчера", 1),
        ("📊 7 дней", 7),
        ("📈 14 дней", 14),
        ("🗓️ 30 дней", 30),
    ]
    kb = [
        [InlineKeyboardButton(text=title, callback_data=f"facts:period:{days}:{metric}")]
        for title, days in periods
    ]
    kb.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="facts:back_to_metrics")])
    kb.append([InlineKeyboardButton(text="🏠 Домой", callback_data="nav:home")])
    return InlineKeyboardMarkup(inline_keyboard=kb)


# ========== План продаж ==========
def plan_metric_menu() -> InlineKeyboardMarkup:
    """Выбор метрики для планов продаж."""
    metrics = [
        ("📦 Юниты", "units"),
        ("💰 Выручка", "revenue"),
        ("🧾 Средний чек", "avgprice"),
    ]
    kb = [
        [InlineKeyboardButton(text=title, callback_data=f"plan:metric:{code}")]
        for title, code in metrics
    ]
    kb.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="sales")])
    kb.append([InlineKeyboardButton(text="🏠 Домой", callback_data="nav:home")])
    return InlineKeyboardMarkup(inline_keyboard=kb)


def plan_period_menu(metric: str) -> InlineKeyboardMarkup:
    """Выбор периода для планов продаж."""
    periods = [
        ("📅 Сегодня", 0),
        ("🚀 Завтра", 1),
        ("📊 7 дней", 7),
        ("📈 14 дней", 14),
        ("🗓️ 30 дней", 30),
    ]
    kb = [
        [InlineKeyboardButton(text=title, callback_data=f"plan:period:{days}:{metric}")]
        for title, days in periods
    ]
    kb.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="plan:back_to_metrics")])
    kb.append([InlineKeyboardButton(text="🏠 Домой", callback_data="nav:home")])
    return InlineKeyboardMarkup(inline_keyboard=kb)


# ========== Выкупы ==========
def buyouts_menu() -> InlineKeyboardMarkup:
    """Меню раздела Выкупы."""
    kb = [
        [InlineKeyboardButton(text="🛒 Необходимо закупить", callback_data="buyouts:need")],
        [InlineKeyboardButton(text="📦 Статус закупок", callback_data="buyouts:inprogress")],
        [InlineKeyboardButton(text="🗂️ Загрузить товары.xlsx", callback_data="buyouts:upload")],
        [InlineKeyboardButton(text="🏠 Домой", callback_data="nav:home")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=kb)


def buyouts_need_menu() -> InlineKeyboardMarkup:
    """
    Подменю для экрана «🛒 Необходимо закупить».
    Здесь размещаем кнопку «⚙️ Распределение».
    """
    kb = [
        [InlineKeyboardButton(text="⚙️ Распределение", callback_data="buyouts:dist")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="nav:back")],
        [InlineKeyboardButton(text="🏠 Домой", callback_data="nav:home")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=kb)


# ========== Отгрузки ==========
def shipments_menu() -> InlineKeyboardMarkup:
    """Меню раздела Отгрузки."""
    kb = [
        [InlineKeyboardButton(text="🚚 Необходимо отгрузить", callback_data="shipments:need")],
        [InlineKeyboardButton(text="📍 Статус отгрузок", callback_data="shipments:onsale")],
        [InlineKeyboardButton(text="🧮 Потребность по складам", callback_data="demand:start")],
        [InlineKeyboardButton(text="⏰ Сроки доставки", callback_data="lead:start")],
        [InlineKeyboardButton(text="🏠 Домой", callback_data="nav:home")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=kb)


# ========== Отгрузки — режимы представления ==========
def shipments_view_menu(
    has_cluster: bool = True, has_warehouse: bool = True
) -> InlineKeyboardMarkup:
    """Кнопки представления данных: по SKU, кластерам, складам."""
    rows: list[list[InlineKeyboardButton]] = []
    rows.append([InlineKeyboardButton(text="🔢 По SKU", callback_data="shipments:view:sku")])
    if has_cluster:
        rows.append(
            [InlineKeyboardButton(text="🏢 По кластерам", callback_data="shipments:view:cluster")]
        )
    if has_warehouse:
        rows.append(
            [InlineKeyboardButton(text="🏭 По складам", callback_data="shipments:view:warehouse")]
        )
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="nav:back")])
    rows.append([InlineKeyboardButton(text="🏠 Домой", callback_data="nav:home")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


# ========== Отчёт по отгрузкам ==========
def shipments_report_menu(
    has_cluster: bool = True, has_warehouse: bool = True
) -> InlineKeyboardMarkup:
    """Подменю внутри «Необходимо отгрузить»."""
    rows: list[list[InlineKeyboardButton]] = []
    rows.append([InlineKeyboardButton(text="🔢 По SKU", callback_data="shipments:view:sku")])
    if has_cluster:
        rows.append(
            [InlineKeyboardButton(text="🏢 По кластерам", callback_data="shipments:view:cluster")]
        )
    if has_warehouse:
        rows.append(
            [InlineKeyboardButton(text="🏭 По складам", callback_data="shipments:view:warehouse")]
        )
    rows.append(
        [InlineKeyboardButton(text="📥 Экспорт в Excel", callback_data="shipments:report:export")]
    )
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="shipments:need")])
    rows.append([InlineKeyboardButton(text="🏠 Домой", callback_data="nav:home")])
    return InlineKeyboardMarkup(inline_keyboard=rows)
