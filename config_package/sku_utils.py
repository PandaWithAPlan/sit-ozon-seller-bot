"""
Утилиты для работы со SKU (Stock Keeping Unit).
Централизованные функции для парсинга и валидации SKU.
"""

import logging
from typing import List, Set, Tuple

log = logging.getLogger("seller-bot.sku_utils")


def parse_sku_string(sku_string: str) -> List[int]:
    """
    Универсальный парсер SKU из строки.
    Поддерживает различные форматы:
    - "123456" - один SKU
    - "123456:Название" - SKU с наименованием
    - "123456,789012" - несколько SKU через запятую
    - "123456:Телефон, 789012:Чехол" - несколько SKU с названиями

    Args:
        sku_string: Строка с SKU (может содержать названия через двоеточие)

    Returns:
        Список уникальных SKU в формате int
    """
    result = []
    seen: Set[int] = set()

    # Разбиваем по запятым и переводам строки
    for token in sku_string.replace("\n", ",").split(","):
        token = token.strip()
        if not token:
            continue

        # SKU - это часть до первого двоеточия
        sku_part = token.split(":")[0].strip()

        try:
            sku = int(sku_part)
            if sku not in seen:
                result.append(sku)
                seen.add(sku)
                log.debug(f"Parsed SKU: {sku}")
            else:
                log.debug(f"Duplicate SKU ignored: {sku}")
        except ValueError as e:
            log.warning(f"Invalid SKU token '{sku_part}': {e}")
        except Exception as e:
            log.error(f"Unexpected error parsing SKU '{sku_part}': {e}", exc_info=True)

    log.info(f"Parsed {len(result)} unique SKUs from string")
    return result


def parse_sku_list(sku_list: List[str]) -> List[int]:
    """
    Парсит список строк с SKU.

    Args:
        sku_list: Список строк с SKU

    Returns:
        Список уникальных SKU в формате int
    """
    result = []
    seen: Set[int] = set()

    for sku_string in sku_list:
        skus = parse_sku_string(sku_string)
        for sku in skus:
            if sku not in seen:
                result.append(sku)
                seen.add(sku)

    log.info(f"Parsed {len(result)} unique SKUs from list of {len(sku_list)} items")
    return result


def validate_sku(sku: int) -> bool:
    """
    Валидирует SKU.

    Args:
        sku: SKU для валидации

    Returns:
        True если SKU валиден
    """
    if sku <= 0:
        log.warning(f"Invalid SKU (must be positive): {sku}")
        return False

    if sku > 999999999:  # Максимально 9 цифр
        log.warning(f"Invalid SKU (too large): {sku}")
        return False

    return True


def filter_valid_skus(sku_list: List[int]) -> List[int]:
    """
    Фильтрует список SKU, оставляя только валидные.

    Args:
        sku_list: Список SKU

    Returns:
        Список валидных SKU
    """
    result = []
    for sku in sku_list:
        if validate_sku(sku):
            result.append(sku)
        else:
            log.warning(f"Filtered out invalid SKU: {sku}")

    log.info(f"Filtered {len(result)}/{len(sku_list)} valid SKUs")
    return result


def format_sku_with_alias(sku: int, alias: str = "") -> str:
    """
    Форматирует SKU с наименованием.

    Args:
        sku: SKU
        alias: Наименование (опционально)

    Returns:
        Отформатированная строка
    """
    if alias:
        return f"{sku}:{alias}"
    return str(sku)


def parse_sku_with_aliases(sku_string: str) -> List[Tuple[int, str]]:
    """
    Парсит SKU с наименованиями.

    Args:
        sku_string: Строка с SKU (формат "123456:Название")

    Returns:
        Список кортежей (sku, alias)
    """
    result = []
    seen: Set[int] = set()

    for token in sku_string.replace("\n", ",").split(","):
        token = token.strip()
        if not token:
            continue

        parts = token.split(":", 1)
        sku_part = parts[0].strip()
        alias = parts[1].strip() if len(parts) > 1 else ""

        try:
            sku = int(sku_part)
            if sku not in seen:
                result.append((sku, alias))
                seen.add(sku)
                log.debug(f"Parsed SKU with alias: {sku}:{alias}")
            else:
                log.debug(f"Duplicate SKU ignored: {sku}")
        except ValueError as e:
            log.warning(f"Invalid SKU token '{sku_part}': {e}")
        except Exception as e:
            log.error(f"Unexpected error parsing SKU '{sku_part}': {e}", exc_info=True)

    log.info(f"Parsed {len(result)} unique SKUs with aliases")
    return result


def deduplicate_skus(sku_list: List[int]) -> List[int]:
    """
    Убирает дубликаты из списка SKU.

    Args:
        sku_list: Список SKU

    Returns:
        Список уникальных SKU
    """
    seen: Set[int] = set()
    unique = []

    for sku in sku_list:
        if sku not in seen:
            unique.append(sku)
            seen.add(sku)

    if len(unique) != len(sku_list):
        log.info(f"Removed {len(sku_list) - len(unique)} duplicate SKUs")

    return unique


def batch_skus(skus: List[int], batch_size: int = 100) -> List[List[int]]:
    """
    Разбивает список SKU на батчи.

    Args:
        skus: Список SKU
        batch_size: Размер батча

    Returns:
        Список батчей
    """
    batches = []
    for i in range(0, len(skus), batch_size):
        batch = skus[i : i + batch_size]
        batches.append(batch)

    log.info(f"Split {len(skus)} SKUs into {len(batches)} batches of size {batch_size}")
    return batches
