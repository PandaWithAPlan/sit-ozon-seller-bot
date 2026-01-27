# -*- coding: utf-8 -*-
"""
Simple test without Unicode characters for Windows compatibility.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def test_import_config():
    """Test config import."""
    try:
        from config_package import settings, get_settings, ForecastMethod

        print("OK: Config import successful")
        return True
    except ImportError as e:
        print(f"FAIL: Config import failed - {e}")
        return False


def test_settings_instance():
    """Test settings instance."""
    try:
        from config_package import settings, get_settings

        s = settings
        print("OK: Settings instance created")

        # Check properties
        assert hasattr(s, "ozon_client_id"), "Missing ozon_client_id"
        assert hasattr(s, "ozon_api_key"), "Missing ozon_api_key"
        assert hasattr(s, "effective_token"), "Missing effective_token"
        assert hasattr(s, "parsed_watch_sku"), "Missing parsed_watch_sku"
        assert hasattr(s, "parsed_chat_ids"), "Missing parsed_chat_ids"

        return True
    except Exception as e:
        print(f"FAIL: Settings instance failed - {e}")
        return False


def test_settings_validation():
    """Test settings validation."""
    try:
        from config_package import settings

        settings.validate_on_startup()
        print("OK: Config validation passed")
        return True
    except ValueError as e:
        print(f"FAIL: Validation failed - {e}")
        return False
    except Exception as e:
        print(f"FAIL: Validation error - {e}")
        return False


def test_constants():
    """Test constants."""
    try:
        from config_package.constants import ForecastMethod, DemandMethod

        # Check Enum values
        assert ForecastMethod.MA7.value == "ma7", "MA7 value is wrong"
        assert ForecastMethod.MA30.value == "ma30", "MA30 value is wrong"
        assert ForecastMethod.ES.value == "es", "ES value is wrong"

        # Check titles
        title_check = "Средняя за 7 дней" in ForecastMethod.MA7.title
        assert title_check, "MA7 title is wrong"

        title_check = "Средняя за 30 дней" in ForecastMethod.MA30.title
        assert title_check, "MA30 title is wrong"

        title_check = "Экспоненциальное сглаживание" in ForecastMethod.ES.title
        assert title_check, "ES title is wrong"

        print("OK: Constants validated")
        return True
    except Exception as e:
        print(f"FAIL: Constants test failed - {e}")
        return False


def test_env_file():
    """Test .env file."""
    env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env")
    if os.path.exists(env_path):
        print(f"OK: .env exists - {env_path}")
        return True
    else:
        print(f"FAIL: .env not found - {env_path}")
        return False


def main():
    """Run all tests."""
    print("=== Testing Config ===\n")

    tests = [
        ("Import config", test_import_config),
        ("Settings instance", test_settings_instance),
        ("Config validation", test_settings_validation),
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
        status = "OK" if result else "FAIL"
        print(f"{status}: {name}")

    return passed == total


if __name__ == "__main__":
    sys.exit(0 if main() else 1)
