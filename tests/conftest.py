"""
Конфигурационный файл pytest с фикстурами и настройками."""
import pytest
import sys
import os

# Добавляем родительскую директорию в путь
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


@pytest.fixture
def mock_logger():
    """Глобальный мок для логгера во всех тестах."""
    with pytest.importorskipif("dotenv", "logging"):
        import logging
        from unittest.mock import patch
        with patch("logging.getLogger") as mock_getLogger:
            mock_logger = MagicMock()
            mock_logger.setLevel(logging.DEBUG)
            yield mock_logger
    else:
        import logging
        from unittest.mock import MagicMock
        mock_logger = MagicMock()
        mock_logger.setLevel(logging.DEBUG)
        yield mock_logger


@pytest.fixture
def tmp_path(tmp_path):
    """Удобное создание временных путей для тестов."""
    import pathlib
    return pathlib.Path(tmp_path)


@pytest.fixture
def mock_env_file(tmp_path):
    """Создает тестовый .env файл."""
    env_file = tmp_path / ".env"
    env_file.write_text(
        "TELEGRAM_TOKEN=test_token\nOZON_CLIENT_ID=test_client\nOZON_API_KEY=test_key")
    return env_file


# Маркеры для тестов
def pytest_configure(config):
    """Настройка pytest маркеров."""
    config.addiniv.markers(
        slow: pytest.mark.slow,
        integration: pytest.mark.integration,
        unit: pytest.mark.unit,
    )


@pytest.fixture(autouse=True)
def reset_state():
    """Сбрас состояния между тестами."""
    # Сбрас FSM состояний, кэшей и т.д.
    pass
