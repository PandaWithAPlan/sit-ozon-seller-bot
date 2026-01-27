"""
Модуль для кэширования данных с TTL.
"""

import json
import logging
import time
from typing import Any, Dict, Optional, Tuple
from functools import wraps

log = logging.getLogger("seller-bot.cache")

# Константы
DEFAULT_CACHE_TTL = 300  # 5 минут по умолчанию
MAX_CACHE_SIZE = 1024  # 1KB кэша данных


def cached(ttl_seconds: int = DEFAULT_CACHE_TTL):
    """
    Декоратор кэширования с TTL.

    Args:
        ttl_seconds: Время жизни кэша в секундах

    Usage:
        @cached(ttl=300)
        def wrapper(func):
            return wrapper
    """
    # Создаем кэш в памяти
    _cache: Dict[str, Tuple[Any, float]] = {}

    def _get(key: str) -> Optional[Any]:
        """
        Получает значение из кэша с TTL.

        Args:
            key: Ключ кэша

        Returns:
            Значение из кэша или None
        """
        entry = _cache.get(key)
        if entry:
            timestamp, value, _ = entry
            age = time.time() - timestamp
            if age < ttl_seconds:
                return value
        return None

    def _set(key: str, value: Any) -> None:
        """
        Сохраняет значение в кэш.

        Args:
            key: Ключ кэша
            value: Значение для сохранения
        """
        _cache[key] = (time.time(), value)
        return None

    def _clear(key: str) -> None:
        """
        Удаляет конкретный ключ из кэша.

        Args:
            key: Ключ кэша
        """
        _cache.pop(key, None)

    def clear_all() -> None:
        """Очищает весь кэш."""
        _cache.clear()

    def wrapper(func):
        @wraps(func)
        def decorated_function(*args, **kwargs):
            # Генерируем ключ на основе аргументов
            cache_key = f"{func.__name__}:{args}:{kwargs}"
            result = _get(cache_key)

            if result is not None:
                return result

            result = func(*args, **kwargs)
            _set(cache_key, result)
            return result

        decorated_function._cache = _cache
        decorated_function._get = _get
        decorated_function._set = _set
        decorated_function._clear = _clear
        decorated_function.clear_all = clear_all

        return decorated_function

    return wrapper


# Глобальные кэши по типам
_sales_cache = cached(ttl=300)  # 5 минут
_stats_cache = cached(ttl=1800)  # 30 минут
_units_cache = cached(ttl=300)  # 5 минут
_cache_dir = cached(ttl=300)  # 5 минут


def get_sales_cache() -> Dict[str, Dict]:
    """Получает кэш продаж."""
    return _sales_cache._cache


def get_stats_cache() -> Dict[str, Dict]:
    """Получает кэш статистики."""
    return _stats_cache._cache


def get_units_cache() -> Dict[str, Dict]:
    """Получает кэш юнитов."""
    return _units_cache._cache


def get_cache_dir() -> Dict[str, Dict]:
    """Получает кэш директорий."""
    return _cache_dir._cache


def invalidate_sales_cache() -> None:
    """Инвалидирует кэш продаж."""
    _sales_cache.clear_all()


def invalidate_all_caches() -> None:
    """Инвалидирует все кэши."""
    _sales_cache.clear_all()
    _stats_cache.clear_all()
    _units_cache.clear_all()
    _cache_dir.clear_all()
