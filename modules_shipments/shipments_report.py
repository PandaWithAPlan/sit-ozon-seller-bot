# modules_sales/sales_report.py
from __future__ import annotations

import os
import time
import datetime as dt
from typing import List, Tuple, Dict, Any, Optional, Iterable
import html
import requests
from dotenv import load_dotenv

# ─── ENV ──────────────────────────────────────────────────────────────────────
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
load_dotenv(os.path.join(BASE_DIR, ".env"))

OZON_CLIENT_ID = os.getenv("OZON_CLIENT_ID", "")
OZON_API_KEY = os.getenv("OZON_API_KEY", "")
OZON_COMPANY_ID = os.getenv("OZON_COMPANY_ID")  # может быть пустым
WATCH_SKU_RAW = os.getenv("WATCH_SKU", "")

# ⚠️ Примечание по WATCH_OFFERS:
# Если PRODUCTS_MODE=OFFER, то в модулях, которые обращаются к analytics (факт/план/трафик),
# фильтрация идёт по offer_id из WATCH_OFFERS. Данный модуль формирует отчёт ПО ОДНОМУ SKU,
# поэтому WATCH_OFFERS здесь не используется; контроль «чужих» юнитов реализован через WATCH_SKU.

# Название для «неопределённого» кластера (раньше было «Не указан»)
UNKNOWN_CLUSTER = os.getenv("UNKNOWN_CLUSTER_TITLE", "Без кластера")
# Максимальная ширина колонки «Кластер» в отчёте (для мобильных можно уменьшить)
MAX_CLUSTER_NAME_W = int(os.getenv("SALES_REPORT_MAX_CLUSTER_NAME_W", "26"))

API_ANALYTICS = "https://api-seller.ozon.ru/v1/analytics/data"
API_CLUSTERS = "https://api-seller.ozon.ru/v1/cluster/list"
API_FBO_LIST = "https://api-seller.ozon.ru/v2/posting/fbo/list"  # v2
API_FBS_LIST = "https://api-seller.ozon.ru/v3/posting/fbs/list"  # v3
API_RETURNS = "https://api-seller.ozon.ru/v1/returns/list"  # возвр FBO/FBS

# ─── HTTP ─────────────────────────────────────────────────────────────────────
_SESSION = requests.Session()
_SESSION.headers.update({"User-Agent": "seller-bot/units-report/1.9"})
_MIN_INTERVAL_SEC = 0.5
_last_call_ts: float = 0.0


def _headers() -> Dict[str, str]:
    return {
        "Client-Id": OZON_CLIENT_ID,
        "Api-Key": OZON_API_KEY,
        "Content-Type": "application/json",
    }


def _throttle():
    global _last_call_ts
    now = time.time()
    if now - _last_call_ts < _MIN_INTERVAL_SEC:
        time.sleep(_MIN_INTERVAL_SEC - (now - _last_call_ts))
    _last_call_ts = time.time()


def _post(url: str, body: Dict[str, Any], timeout: int = 30) -> Dict[str, Any]:
    _throttle()
    r = _SESSION.post(url, headers=_headers(), json=body, timeout=timeout)
    r.raise_for_status()
    return r.json()


# ─── Алиасы SKU ───────────────────────────────────────────────────────────────
try:
    from .sales_facts_store import get_alias_for_sku  # type: ignore
except Exception:

    def get_alias_for_sku(sku: int) -> str:
        return str(sku)


# ─── Список/набор наблюдаемых SKU ────────────────────────────────────────────


def _parse_watch_skus(txt: str) -> List[int]:
    if not txt:
        return []
    parts = [p.strip() for p in txt.replace("\n", ",").replace(" ", ",").split(",")]
    out: List[int] = []
    for p in parts:
        if not p:
            continue
        try:
            out.append(int(p.split(":")[0]))
        except Exception:
            pass
    seen = set()
    uniq: List[int] = []
    for s in out:
        if s not in seen:
            uniq.append(s)
            seen.add(s)
    return uniq


def _allowed_sku_set() -> set[int]:
    """Множество разрешённых SKU (строгий фильтр: только из WATCH_SKU)."""
    return set(_parse_watch_skus(WATCH_SKU_RAW))


def list_skus(limit: int = 200) -> List[Tuple[int, str]]:
    """
    Возвращает список (sku, alias) ДЛЯ МЕНЮ в порядке WATCH_SKU.
    Имя берётся из ALIAS (через get_alias_for_sku), при отсутствии — сам sku.
    Иные юниты в меню не попадают (если WATCH_SKU задан).
    """
    skus: List[int] = _parse_watch_skus(WATCH_SKU_RAW)
    if not skus:
        # Фолбэк: когда WATCH_SKU пуст — разрешить все, но это поведение «по умолчанию».
        try:
            from .sales_facts_store import all_skus  # type: ignore

            seen = set()
            for s in list(all_skus()):
                try:
                    si = int(s)
                except Exception:
                    continue
                if si not in seen:
                    seen.add(si)
                    skus.append(si)
        except Exception:
            pass

    pairs: List[Tuple[int, str]] = []
    for sku in skus:
        try:
            alias = get_alias_for_sku(int(sku)) or str(sku)
        except Exception:
            alias = str(sku)
        pairs.append((int(sku), alias))

    # ⚙️ ВАЖНО: НЕ сортируем по имени — сохраняем порядок WATCH_SKU.
    return pairs[:limit]


# ─── Утилиты ──────────────────────────────────────────────────────────────────


def _parse_date(s: str) -> dt.date:
    if "-" in s:
        return dt.date.fromisoformat(s)
    d, m, y = s.split(".")
    return dt.date(int(y), int(m), int(d))


def _analytics_period(start: str, end: str) -> tuple[str, str]:
    """
    Для /v1/analytics/data используем конец ПЕРИОДА ВКЛЮЧИТЕЛЬНО (как в UI).
    Если ответ окажется пустым, позже сделаем ретрай со сдвигом +1 день.
    """
    ds = _parse_date(start)
    de = _parse_date(end)
    if de < ds:
        ds, de = de, ds
    return ds.isoformat(), de.isoformat()


def _to_z(d: dt.date, end_of_day: bool = False) -> str:
    return f"{d.isoformat()}T{'23:59:59.999' if end_of_day else '00:00:00.000'}Z"


def _fmt_int(n: int) -> str:
    return f"{n:,}".replace(",", " ")


def _pad(s: str, width: int) -> str:
    return s[: width - 1] + "…" if len(s) > width else s.ljust(width)


def _pre_block(text: str) -> str:
    return f"<pre>{html.escape(text, quote=False)}</pre>"


def _pre_kv(pairs: List[Tuple[str, str]]) -> str:
    """Вертикальная «таблица»: ключи слева, значения справа, выравнивание по колону ключа."""
    if not pairs:
        return _pre_block("")
    key_w = max(len(k) for k, _ in pairs)
    lines = [f"{k.ljust(key_w)}  {v}" for k, v in pairs]
    return _pre_block("\n".join(lines))


def _as_int_maybe(x: Any) -> Optional[int]:
    try:
        return int(str(x))
    except Exception:
        return None


# Сокращение длинных имён кластеров для компактности


def _shorten_cluster(name: str) -> str:
    s = str(name or "")
    repl = {
        "Санкт-Петербург": "СПб",
        "Дальние регионы": "ДР",
        "Северо-Запад": "СЗФО",
        "Центральный": "Центр",
        "Дальневосточный": "ДВ",
        ",": " ",
        "  ": " ",
        " и ": "+",
        " ,": " ",
        ", ": " ",
    }
    for k, v in repl.items():
        s = s.replace(k, v)
    while "  " in s:
        s = s.replace("  ", " ")
    s = s.strip()
    if not s:
        s = UNKNOWN_CLUSTER
    return s


# ─── Кластеры ────────────────────────────────────────────────────────────────
_clusters_cache: Optional[Dict[int, str]] = None  # warehouse_id -> cluster_name


def _load_clusters() -> Dict[int, str]:
    global _clusters_cache
    if _clusters_cache is not None:
        return _clusters_cache

    mapping: Dict[int, str] = {}
    # РФ + СНГ, чтобы видеть «Казахстан» и «Беларусь»
    for cluster_type in ("CLUSTER_TYPE_OZON", "CLUSTER_TYPE_CIS"):
        try:
            js = _post(API_CLUSTERS, {"cluster_type": cluster_type})
        except Exception:
            continue
        for cl in js.get("clusters", []) or []:
            name = cl.get("name") or UNKNOWN_CLUSTER
            for lc in cl.get("logistic_clusters", []) or []:
                for wh in lc.get("warehouses", []) or []:
                    wid = wh.get("warehouse_id")
                    if isinstance(wid, int):
                        mapping[wid] = name
    _clusters_cache = mapping
    return mapping


# ─── Постинги FBO/FBS ────────────────────────────────────────────────────────


def _iter_postings(
    url: str, since_iso: str, to_iso: str, limit: int = 500
) -> Iterable[Dict[str, Any]]:
    offset = 0
    while True:
        body = {
            "dir": "ASC",
            "filter": {"since": since_iso, "to": to_iso, "status": ""},
            "limit": limit,
            "offset": offset,
            "with": {"analytics_data": True, "financial_data": True},
        }
        js = _post(url, body)
        if url.endswith("/v2/posting/fbo/list"):
            items = js.get("result") or []
            has_next = len(items) == limit
        else:
            res = js.get("result") or {}
            items = res.get("postings") or []
            has_next = bool(res.get("has_next"))
        for it in items:
            yield it
        if not has_next:
            break
        offset += limit


def _cluster_from_posting(p: Dict[str, Any], wh2cluster: Dict[int, str]) -> str:
    """
    Пытаемся извлечь warehouse_id из разных мест постинга
    и сопоставить его с названием кластера.
    """
    # 1) analytics_data.warehouse_id (самый надёжный)
    a = p.get("analytics_data") or {}
    wid = _as_int_maybe(a.get("warehouse_id"))
    if isinstance(wid, int) and wid in wh2cluster:
        return wh2cluster[wid]

    # 2) shipment_warehouse_id (встречается в FBS)
    wid = _as_int_maybe(p.get("shipment_warehouse_id"))
    if isinstance(wid, int) and wid in wh2cluster:
        return wh2cluster[wid]

    # 3) delivery_method.warehouse_id (иногда бывает в v3)
    dm = p.get("delivery_method") or {}
    wid = _as_int_maybe(dm.get("warehouse_id"))
    if isinstance(wid, int) and wid in wh2cluster:
        return wh2cluster[wid]

    # 4) fallback: корневой warehouse_id (редко)
    wid = _as_int_maybe(p.get("warehouse_id"))
    if isinstance(wid, int) and wid in wh2cluster:
        return wh2cluster[wid]

    return UNKNOWN_CLUSTER


# ─── Возвраты FBO/FBS ────────────────────────────────────────────────────────


def _iter_returns(time_from: str, time_to: str, limit: int = 500) -> Iterable[Dict[str, Any]]:
    """
    Пагинация по last_id. Фильтр по logistic_return_date (включительно по времени).
    """
    last_id: Optional[int] = None
    while True:
        body = {
            "filter": {
                "logistic_return_date": {
                    "time_from": time_from,
                    "time_to": time_to,
                }
            },
            "limit": limit,
        }
        if last_id:
            body["last_id"] = last_id
        js = _post(API_RETURNS, body)
        items = js.get("returns") or []
        for it in items:
            yield it
        if not js.get("has_next"):
            break
        try:
            last_id = int(items[-1].get("id"))
        except Exception:
            break


def _cluster_from_return(
    r: Dict[str, Any],
    wh2cluster: Dict[int, str],
    posting2cluster: Dict[str, str],
) -> str:
    """
    Пытаемся определить кластер возврата:
      1) place.id
      2) target_place.id
      3) по posting_number через ранее собранный posting2cluster
    """
    # 1) place.id
    place = r.get("place") or {}
    wid = _as_int_maybe(place.get("id"))
    if isinstance(wid, int) and wid in wh2cluster:
        return wh2cluster[wid]

    # 2) target_place.id
    tplace = r.get("target_place") or {}
    wid = _as_int_maybe(tplace.get("id"))
    if isinstance(wid, int) and wid in wh2cluster:
        return wh2cluster[wid]

    # 3) posting_number → кластер из постингов
    pn = r.get("posting_number")
    if pn and pn in posting2cluster:
        return posting2cluster[pn]

    return UNKNOWN_CLUSTER


# ─── Сбор по кластерам ───────────────────────────────────────────────────────


def _collect_clusters_for_sku(sku: int, start: str, end: str) -> Dict[str, Dict[str, int]]:
    """
    Считаем заказанные юниты ТАК ЖЕ, как в аналитике:
    - ordered   — все заказанные юниты (включая отменённые/возвраты);
    - cancelled — отдельно количество отменённых;
    - returned  — отдельно количество возвращённых (из /v1/returns/list);
    Тогда ИТОГ = ordered - cancelled - returned.
    """
    ds = _parse_date(start)
    de = _parse_date(end)
    since = _to_z(ds)
    to_eod = _to_z(de, end_of_day=True)

    wh2cluster = _load_clusters()
    out: Dict[str, Dict[str, int]] = {}

    def bump(cl: str, k: str, v: int):
        row = out.setdefault(cl, {"ordered": 0, "cancelled": 0, "returned": 0})
        row[k] += int(v)

    # будем копить соответствие posting_number -> cluster для последующего матчинга возвратов
    posting2cluster: Dict[str, str] = {}

    # 1) Постинги: считаем ordered и cancelled
    for url in (API_FBO_LIST, API_FBS_LIST):
        try:
            for p in _iter_postings(url, since, to_eod):
                st = (p.get("status") or "").lower()
                cluster = _cluster_from_posting(p, wh2cluster)

                # сохранить posting_number -> cluster (если удалось определить)
                pn = p.get("posting_number")
                if cluster != UNKNOWN_CLUSTER and isinstance(pn, str) and pn:
                    posting2cluster[pn] = cluster

                for pr in p.get("products", []) or []:
                    try:
                        p_sku = int(pr.get("sku") or 0)
                    except Exception:
                        continue
                    if p_sku != int(sku):
                        continue
                    q = int(pr.get("quantity") or 0)

                    # «Заказано» всегда растёт
                    bump(cluster, "ordered", q)

                    # Отмены — дополнительно
                    if st == "cancelled":
                        bump(cluster, "cancelled", q)
        except requests.HTTPError as e:
            if getattr(e.response, "status_code", None) in (403, 404):
                continue
            raise
        except Exception:
            continue

    # 2) Возвраты: из /v1/returns/list (по logistic_return_date)
    try:
        for r in _iter_returns(since, to_eod):
            pr = r.get("product") or {}
            try:
                r_sku = int(pr.get("sku") or 0)
            except Exception:
                continue
            if r_sku != int(sku):
                continue
            qty = int(pr.get("quantity") or 0)

            cluster = _cluster_from_return(r, wh2cluster, posting2cluster)
            bump(cluster, "returned", qty)
    except Exception:
        # Возвраты недоступны — не падаем, просто остаются 0
        pass

    return out


# ─── Аналитика: точные totals по SKU ─────────────────────────────────────────


def _analytics_sum_rows(rows: list, sku: int) -> Dict[str, int]:
    total_ord = total_cnc = total_ret = 0
    sku_str = str(sku)
    for r in rows:
        dims = r.get("dimensions") or []
        if not any(str(d.get("id", "")) == sku_str for d in dims):
            continue
        m = r.get("metrics") or []
        if isinstance(m, list):
            if len(m) >= 1:
                total_ord += int(m[0] or 0)
            if len(m) >= 2:
                total_cnc += int(m[1] or 0)
            if len(m) >= 3:
                total_ret += int(m[2] or 0)
        elif isinstance(m, dict):
            total_ord += int(m.get("ordered_units", 0) or 0)
            total_cnc += int(m.get("cancellations", 0) or 0)
            total_ret += int(m.get("returns", 0) or 0)
    return {"ordered_units": total_ord, "cancellations": total_cnc, "returns": total_ret}


def _analytics_query_rows(sku: int, date_from: str, date_to: str) -> list:
    """dimension=['day','sku'] + пагинация, тянем только нужный SKU (через фильтр и пост-фильтр)."""
    rows: list = []
    offset = 0
    limit = 1000
    while True:
        body = {
            "date_from": date_from,
            "date_to": date_to,
            "metrics": ["ordered_units", "cancellations", "returns"],
            "dimension": ["day", "sku"],
            "filters": [{"key": "sku", "value": str(sku)}],
            "limit": limit,
            "offset": offset,
        }
        if OZON_COMPANY_ID and OZON_COMPANY_ID.isdigit():
            body["company_id"] = int(OZON_COMPANY_ID)
        js = _post(API_ANALYTICS, body)
        chunk = (
            (js.get("result", {}) or {}).get("data")
            or (js.get("result", {}) or {}).get("rows")
            or []
        )
        rows.extend(chunk)
        if len(chunk) < limit:
            break
        offset += limit
    return rows


def _fetch_analytics_totals(sku: int, start: str, end: str) -> Dict[str, int]:
    # сначала — «как в UI» (конец периода включительно)
    df, dt_incl = _analytics_period(start, end)
    rows = _analytics_query_rows(sku, df, dt_incl)
    totals = _analytics_sum_rows(rows, sku)
    if any(v != 0 for v in totals.values()):
        return totals

    # если вдруг пусто — ретрай по спецификации (полуинтервал [from, to) )
    try:
        dt_semi = (_parse_date(end) + dt.timedelta(days=1)).isoformat()
        rows2 = _analytics_query_rows(sku, df, dt_semi)
        return _analytics_sum_rows(rows2, sku)
    except Exception:
        return {"ordered_units": 0, "cancellations": 0, "returns": 0}


# ─── Табличный рендер (компактный, числа столбцами) ─────────────────────────


def _pre_table(headers: List[str], rows: List[List[str]], max_name_w: int) -> str:
    """
    Рисуем моноширинную таблицу:
      • первая колонка — «Кластер» (обрезаем/паддим до max_name_w);
      • остальные — числовые, прижаты вправо и выровнены по столбцам;
      • между колонками один пробел.
    """
    if not rows:
        return _pre_block("—")

    # ширины числовых колонок
    num_cols = list(zip(*[r[1:] for r in rows])) if rows else []
    num_w = [max(len(h), max(len(v) for v in col)) for h, col in zip(headers[1:], num_cols)]

    # заголовок
    name_h = _pad(headers[0], max_name_w)
    head_cells = [name_h] + [h.rjust(w) for h, w in zip(headers[1:], num_w)]
    lines = [" ".join(head_cells)]

    # разделитель
    sep = "-" * max_name_w + " " + " ".join("-" * w for w in num_w)
    lines.append(sep)

    # строки
    for r in rows:
        name = _pad(r[0], max_name_w)
        nums = [v.rjust(w) for v, w in zip(r[1:], num_w)]
        lines.append(" ".join([name] + nums))

    return _pre_block("\n".join(lines))


# ─── Отчёт (табличный формат) ────────────────────────────────────────────────


def sales_report_text(sku: int, start: str, end: str) -> str:
    # 🔒 Жёсткая проверка: отчёт только для SKU из WATCH_SKU (если WATCH_SKU задан)
    allowed = _allowed_sku_set()
    if allowed and int(sku) not in allowed:
        alias = get_alias_for_sku(sku)
        now = dt.datetime.now().strftime("%d.%m.%Y %H:%M")
        return (
            f"⛔️ SKU {sku} — <b>{html.escape(alias)}</b> не наблюдается (не входит в WATCH_SKU).\n"
            f"Обновлено {now}\n"
            "Добавьте SKU в WATCH_SKU в .env и повторите попытку."
        )

    alias = get_alias_for_sku(sku)
    now = dt.datetime.now().strftime("%d.%m.%Y %H:%M")
    head = f"📄 Юнит-отчёт по SKU {sku} — <b>{html.escape(alias)}</b>\n" f"📅 {
            _parse_date(start).strftime('%d.%m.%Y')}–{
            _parse_date(end).strftime('%d.%m.%Y')} • Обновлено {now}\n"

    clusters = _collect_clusters_for_sku(sku, start, end)

    # Подготовка табличных данных
    total_o = total_c = total_r = 0

    def _sort_key(kv: Tuple[str, Dict[str, int]]):
        cl, agg = kv
        return (1 if cl == UNKNOWN_CLUSTER else 0, -int(agg["ordered"]), str(cl))

    rows: List[List[str]] = []
    unknown_present = False
    max_name_len = 0

    for cl, agg in sorted(clusters.items(), key=_sort_key):
        if cl == UNKNOWN_CLUSTER:
            unknown_present = True
        o, c, r = int(agg["ordered"]), int(agg["cancelled"]), int(agg["returned"])
        net = o - c - r
        total_o += o
        total_c += c
        total_r += r

        name = _shorten_cluster(cl)
        max_name_len = max(max_name_len, len(name))
        rows.append([name, _fmt_int(o), _fmt_int(c), _fmt_int(r), _fmt_int(net)])

    # Если данных нет — показываем заглушку
    if not rows:
        table_block = _pre_block("— данных по кластерам нет —")
    else:
        headers = ["Кластер", "Заказ", "Отм", "Возвр", "Итог"]
        name_w = min(max(max_name_len, len(headers[0])), MAX_CLUSTER_NAME_W)
        table_block = _pre_table(headers, rows, name_w)

    # ИТОГО
    net_total = total_o - total_c - total_r
    total_block = "<b>📊 ИТОГО</b>\n" + _pre_kv(
        [
            ("Заказано", _fmt_int(total_o)),
            ("Отменено", _fmt_int(total_c)),
            ("Возвращено", _fmt_int(total_r)),
            ("Итог", _fmt_int(net_total)),
        ]
    )

    # — Сверка с аналитикой OZON — (вертикальный KV‑блок)
    a = _fetch_analytics_totals(sku, start, end)
    d_ord = a["ordered_units"] - total_o
    d_cnc = a["cancellations"] - total_c
    d_ret = a["returns"] - total_r

    check_pairs = [
        ("Заказано (OZON)", _fmt_int(a["ordered_units"])),
        ("Отменено (OZON)", _fmt_int(a["cancellations"])),
        ("Возвращено (OZON)", _fmt_int(a["returns"])),
    ]
    if any([d_ord, d_cnc, d_ret]):
        diffs = []
        if d_ord:
            diffs.append(f"заказы {d_ord:+}")
        if d_cnc:
            diffs.append(f"отмены {d_cnc:+}")
        if d_ret:
            diffs.append(f"возвраты {d_ret:+}")
        check_pairs.append(("Расхождение", "; ".join(diffs)))
    else:
        check_pairs.append(("Сверка", "✅ Совпадает с листингами FBO/FBS"))

    check_block = "<b>🧮 Сверка с аналитикой OZON</b>\n" + _pre_kv(check_pairs)

    legend = "📦 — заказано • ❌ — отменено • ↩️ — возвращено • ⚖️ — итог"
    note = (
        "\nℹ️ «Без кластера» — не удалось однозначно определить логистический кластер "
        "(нет связи warehouse_id → кластер)."
    )

    parts = [head, table_block, "", total_block, "", check_block, legend]
    if unknown_present:
        parts.append(note)
    return "\n".join(parts)
