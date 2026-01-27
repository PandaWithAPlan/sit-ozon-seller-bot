# Simple config test without typing import
"""
Тест конфигурации без проблем с импортом typing на Windows.
"""

import os
import sys

# Добавляем родительскую директорию в путь
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))


def test_import_config():
    """Тест импорта конфигурации."""
    try:
        from config import settings, get_settings, ForecastMethod

        print("✓ Config import successful")
        return True
    except ImportError as e:
        print(f"✗ Config import failed: {e}")
        return False


def test_settings_instance():
    """Тест создания инстанса настроек."""
    try:
        from config import settings, get_settings

        s = settings
        print("✓ Settings instance created")

        # Проверяем базовые свойства
        print(f"  • Has ozon_client_id: {hasattr(s, 'ozon_client_id')}")
        print(f"  • Has ozon_api_key: {hasattr(s, 'ozon_api_key')}")
        print(f"  • Has effective_token: {hasattr(s, 'effective_token')}")
        print(f"  • Has parsed_watch_sku: {hasattr(s, 'parsed_watch_sku')}")

        return True
    except Exception as e:
        print(f"✗ Settings instance failed: {e}")
        return False


def test_config_validation():
    """Тест валидации настроек."""
    try:
        from config import settings

        settings.validate_on_startup()
        print("✓ Config validation passed")
        return True
    except ValueError as e:
        print(f"✗ Validation failed: {e}")
        return False
    except Exception as e:
        print(f"✗ Validation error: {e}")
        return False


def test_constants():
    """Тест констант."""
    try:
        from config.constants import ForecastMethod, DemandMethod

        print("✓ Constants import successful")

        # Проверяем Enum значения
        assert ForecastMethod.MA7.value == "ma7"
        assert ForecastMethod.MA30.value == "ma30"
        assert ForecastMethod.ES.value == "es"

        # Проверяем titles
        assert ForecastMethod.MA7.title == "Средняя за 7 дней"
        assert ForecastMethod.MA30.title == "Средняя за 30 дней"

        print("✓ Constants validated")
        return True
    except Exception as e:
        print(f"✗ Constants test failed: {e}")
        return False


def test_env_file():
    """Тест наличия .env файла."""
    env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env")
    if os.path.exists(env_path):
        print(f"✓ .env exists: {env_path}")
        return True
    else:
        print(f"✗ .env not found: {env_path}")
        return False


def main():
    """Запуск всех тестов."""
    print("=== Testing Config ===\n")

    tests = [
        ("Import config", test_import_config),
        ("Settings instance", test_settings_instance),
        ("Config validation", test_config_validation),
        ("Constants", test_constants),
        (".env file", test_env_file),
    ]

    results = []
    for name, test in tests:
        print(f"\n{name}...")
        result = test()
        results.append((name, result))

    print(f"\n=== Results ===")
    passed = sum(1 for _, r in results)
    total = len(results)
    print(f"Passed: {passed}/{total}")

    for name, result in results:
        status = "✓ PASS" if result else "✗ FAIL"
        print(f"{status}: {name}")

    return passed == total


if __name__ == "__main__":
    sys.exit(0 if main() else 1)
