import os
from typing import Optional, Any, Dict
from config_package.json_utils import safe_read_json, safe_write_json
from config_package import settings

class JsonCacheManager:
    """
    Менеджер для работы с JSON-кэшем.
    Инкапсулирует операции чтения и записи через config_package.json_utils.
    """

    def __init__(self, file_path: str):
        """
        Args:
            file_path: Полный путь к файлу кэша
        """
        self.file_path = file_path

    def get_data(self) -> Dict[str, Any]:
        """Чтение данных из кэша."""
        return safe_read_json(self.file_path)

    def set_data(self, data: Dict[str, Any]) -> bool:
        """Запись данных в кэш."""
        return safe_write_json(self.file_path, data)

    def update_key(self, key: str, value: Any) -> bool:
        """Обновление или добавление одного ключа."""
        data = self.get_data()
        data[key] = value
        return self.set_data(data)

    def get_key(self, key: str, default: Any = None) -> Any:
        """Получение значения по ключу."""
        data = self.get_data()
        return data.get(key, default)


class SalesCache:
    """Обертка для специфичных путей кэша продаж."""
    
    @staticmethod
    def get_forecast_prefs_manager() -> JsonCacheManager:
        path = os.path.join(settings.sales_cache_dir, "forecast_method.json")
        return JsonCacheManager(path)

    @staticmethod
    def get_facts_cache_manager() -> JsonCacheManager:
        path = os.path.join(settings.sales_cache_dir, "facts_cache.json")
        return JsonCacheManager(path)


class WarehouseCache:
    """Обертка для настроек склада."""
    
    @staticmethod
    def get_prefs_manager() -> JsonCacheManager:
        # Используем shipments_cache_dir (нужно добавить его в settings, если нет, или вычислить)
        path = os.path.join(settings.shipments_cache_dir, "common", "warehouse_prefs.json")
        return JsonCacheManager(path)
