"""
Пакет роутеров для организованного разделения ответственности.
"""

from .start import start_router
from .notifications import notifications_router
from .warehouse import warehouse_router
from .fallback import fallback_router

__all__ = [
    "start_router",
    "notifications_router",
    "warehouse_router",
    "fallback_router",
]
