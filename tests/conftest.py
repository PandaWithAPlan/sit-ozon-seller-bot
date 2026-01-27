"""
Конфигурационный файл pytest с фикстурами и настройками."""
import os
import sys

# Set environment variables BEFORE importing application modules
os.environ["TELEGRAM_TOKEN"] = "test_token_123"
os.environ["OZON_CLIENT_ID"] = "test_client_id"
os.environ["OZON_API_KEY"] = "test_api_key"
os.environ["OZON_COMPANY_ID"] = "12345"
# Set .env file path to non-existent to avoid loading real env
os.environ["DOTENV_FILE"] = "/nonexistent/.env"

import pytest
from unittest.mock import MagicMock, patch

# Добавляем родительскую директорию в путь
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


@pytest.fixture
def mock_logger():
    """Глобальный мок для логгера во всех тестах."""
    with patch("logging.getLogger") as mock_getLogger:
        mock_log = MagicMock()
        mock_getLogger.return_value = mock_log
        yield mock_log

@pytest.fixture
def mock_env_file(tmp_path):
    """Создает тестовый .env файл."""
    env_file = tmp_path / ".env"
    env_file.write_text(
        "TELEGRAM_TOKEN=test_token\nOZON_CLIENT_ID=test_client\nOZON_API_KEY=test_key", encoding="utf-8")
    return env_file

def pytest_configure(config):
    """Настройка pytest маркеров."""
    config.addinivalue_line("markers", "slow: mark test as slow")
    config.addinivalue_line("markers", "integration: mark test as integration")
    config.addinivalue_line("markers", "unit: mark test as unit")
