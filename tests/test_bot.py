"""
Модульные тесты для bot.py
"""

import pytest
from unittest.mock import Mock, AsyncMock, MagicMock, patch
from datetime import datetime
import os

# Настройка для импорта
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


@pytest.fixture
def mock_logger():
    """Мок для логгера."""
    with patch("bot.log") as mock_log:
        yield mock_log


@pytest.fixture
def mock_env_file(tmp_path):
    """Создает тестовый .env файл."""
    env_path = tmp_path / ".env"
    env_path.write_text(
        "TELEGRAM_TOKEN=test_token\nOZON_CLIENT_ID=test_client\nOZON_API_KEY=test_key"
    )
    return env_path


@pytest.mark.unit
class TestLoadEnvWithFallback:
    """Тесты функции _load_env_with_fallback."""

    @pytest.mark.asyncio
    async def test_load_env_utf8(self, tmp_path):
        """Проверяет загрузку UTF-8 файла."""
        env_file = tmp_path / "test.env"
        env_file.write_text("TEST=привет", encoding="utf-8")

        from bot import _load_env_with_fallback

        result = _load_env_with_fallback(str(env_file))

        assert result == "utf-8"

    @pytest.mark.asyncio
    async def test_load_env_cp1251(self, tmp_path):
        """Проверяет загрузку CP1251 файла."""
        env_file = tmp_path / "test.env"
        env_file.write_text("TEST=привет", encoding="cp1251")

        from bot import _load_env_with_fallback

        result = _load_env_with_fallback(str(env_file))

        assert result == "cp1251"

    @pytest.mark.asyncio
    async def test_load_env_nonexistent(self, tmp_path):
        """Проверяет обработку несуществующего файла."""
        env_file = tmp_path / "nonexistent.env"

        from bot import _load_env_with_fallback

        result = _load_env_with_fallback(str(env_file))

        # Должен вернуть "auto" и не падать
        assert result == "auto"


@pytest.mark.unit
class TestJsonFunctions:
    """Тесты функций _read_json и _write_json."""

    @pytest.mark.asyncio
    async def test_read_json_valid(self, tmp_path, mock_logger):
        """Проверяет чтение валидного JSON."""
        json_file = tmp_path / "test.json"
        json_file.write_text('{"key": "value"}', encoding="utf-8")

        from bot import _read_json

        result = _read_json(str(json_file))

        assert result == {"key": "value"}

    @pytest.mark.asyncio
    async def test_read_json_invalid(self, tmp_path, mock_logger):
        """Проверяет обработку невалидного JSON."""
        json_file = tmp_path / "test.json"
        json_file.write_text('{"invalid": json}', encoding="utf-8")

        from bot import _read_json

        result = _read_json(str(json_file))

        # Должен вернуть пустой словарь
        assert result == {}

    @pytest.mark.asyncio
    async def test_read_json_nonexistent(self, tmp_path, mock_logger):
        """Проверяет чтение несуществующего файла."""
        json_file = tmp_path / "nonexistent.json"

        from bot import _read_json

        result = _read_json(str(json_file))

        assert result == {}

    @pytest.mark.asyncio
    async def test_write_json(self, tmp_path, mock_logger):
        """Проверяет запись JSON файла."""
        json_file = tmp_path / "test.json"

        from bot import _write_json

        result = _write_json(str(json_file), {"key": "value"})

        assert result is True
        assert json_file.exists()

        # Проверяем содержимое
        content = json_file.read_text(encoding="utf-8")
        assert '"key"' in content
        assert '"value"' in content

    @pytest.asyncio
    async def test_write_json_permission_error(self, tmp_path, mock_logger):
        """Проверяет обработку ошибки прав доступа."""
        # Создаем директорию с тем же именем
        dir_path = tmp_path / "test.json"
        dir_path.mkdir()

        from bot import _write_json

        result = _write_json(str(dir_path), {"key": "value"})

        # Должен вернуть False
        assert result is False


@pytest.mark.unit
class TestWarehousePreferences:
    """Тесты функций управления настройками складов."""

    @pytest.mark.asyncio
    async def test_load_wh_global_defaults(self, tmp_path):
        """Проверяет значения по умолчанию."""
        from bot import _load_wh_global

        result = _load_wh_global()

        assert result["method"] == "average"
        assert result["period"] == 90

    @pytest.mark.asyncio
    async def test_load_wh_global_invalid_method(self, tmp_path):
        """Проверяет валидацию некорректного метода."""
        json_file = tmp_path / "warehouse_prefs.json"
        json_file.write_text('{"method": "invalid", "period": 90}', encoding="utf-8")

        from bot import _load_wh_global

        result = _load_wh_global()

        # Должен использовать метод по умолчанию
        assert result["method"] == "average"

    @pytest.mark.asyncio
    async def test_load_wh_global_invalid_period(self, tmp_path):
        """Проверяет валидацию некорректного периода."""
        json_file = tmp_path / "warehouse_prefs.json"
        json_file.write_text('{"method": "average", "period": 999}', encoding="utf-8")

        from bot import _load_wh_global

        result = _load_wh_global()

        # Должен использовать период по умолчанию
        assert result["period"] == 90

    @pytest.mark.asyncio
    async def test_save_wh_global(self, tmp_path):
        """Проверяет сохранение настроек."""
        json_file = tmp_path / "warehouse_prefs.json"

        from bot import _save_wh_global

        result = _save_wh_global("hybrid", 180)

        assert result is True
        assert json_file.exists()

        # Проверяем содержимое
        content = json_file.read_text(encoding="utf-8")
        assert '"hybrid"' in content
        assert "180" in content


@pytest.mark.unit
class TestKeyboardBuilders:
    """Тесты функций построения клавиатур."""

    @pytest.mark.asyncio
    async def test_build_main_menu_kb(self):
        """Проверяет создание главного меню."""
        from bot import _build_main_menu_kb

        kb = _build_main_kb()

        assert kb is not None
        assert hasattr(kb, "inline_keyboard")
        assert len(kb.inline_keyboard) == 3

    @pytest.mark.asyncio
    async def test_build_method_kb(self):
        """Проверяет создание меню методов прогноза."""
        from bot import _build_method_kb

        kb = _build_method_kb()

        assert kb is not None
        assert len(kb.inline_keyboard) == 5  # 4 ряда + 1 кнопка дома

    @pytest.mark.asyncio
    async def test_build_warehouse_kb(self):
        """Проверяет создание меню настроек складов."""
        from bot import _build_warehouse_kb

        kb = _build_warehouse_kb("average", 90)

        assert kb is not None
        assert len(kb.inline_keyboard) >= 5

    @pytest.mark.asyncio
    async def test_build_notice_kb(self):
        """Проверяет создание меню уведомлений."""
        from bot import _build_notice_kb

        kb = _build_notice_kb()

        assert kb is not None
        assert len(kb.inline_keyboard) >= 3


@pytest.mark.unit
class TestNoticeTitles:
    """Тесты функций работы с названиями уведомлений."""

    @pytest.mark.asyncio
    async def test_normalize_notice_title(self):
        """Проверяет нормализацию заголовков."""
        from bot import _normalize_notice_title, _CTR_WORD_RE, _CR_WORD_RE

        # Проверяем замену CTR
        result = _normalize_notice_title("CTR yesterday")
        assert "Кликабельность" in result
        assert "CTR" not in result

        # Проверяем замену CR/CVR
        result = _normalize_notice_title("CR today")
        assert "Конверсия" in result
        assert "CR" not in result
        assert "CVR" not in result

        # Проверяем удаление (vs 30)
        result = _normalize_notice_title("Test (vs 30) text")
        assert "vs 30" not in result.lower()

    @pytest.mark.asyncio
    async def test_label_for(self):
        """Проверяет получение меток для кодов."""
        from bot import _label_for, NOTICE_TITLES

        # Существующий код
        result = _label_for("plan_units_30d")
        assert "План" in result

        # Несуществующий код
        result = _label_for("nonexistent_code")
        assert "nonexistent_code" in result


@pytest.mark.unit
class TestHelperFunctions:
    """Тесты вспомогательных функций."""

    @pytest.mark.asyncio
    async def test_fmt_alpha(self):
        """Проверяет форматирование параметра alpha."""
        from bot import _fmt_alpha

        assert _fmt_alpha(0.3000) == "0.3"
        assert _fmt_alpha(0.5) == "0.5"
        assert _fmt_alpha(1.0) == "1"
        assert _fmt_alpha(0.0005) == "0.0005"
        assert _fmt_alpha(0.0) == "0"


@pytest.mark.asyncio
async def test_ack_handler_callback_query():
    """Тест функции _ack с разными типами исключений."""
    from aiogram.types import CallbackQuery
    from bot import _ack

    # Мок callback query
    mock_cb = AsyncMock(spec=CallbackQuery)

    # Тест без исключения - должен выполниться без ошибок
    await _ack(mock_cb)
    mock_cb.answer.assert_called_once()

    # Тест с QueryExpired - должен игнорироваться
    mock_cb.answer.side_effect = Exception("Query is too old")
    await _ack(mock_cb)
    # Не должно быть исключения

    # Тест с другим исключением - должно игнорироваться
    mock_cb.answer.side_effect = Exception("Other error")
    await _ack(mock_cb)
    # Не должно быть исключения


@pytest.mark.asyncio
async def test_safe_edit_msg_success():
    """Тест функции _safe_edit_msg при успехе."""
    from aiogram.types import CallbackQuery, Message
    from bot import _safe_edit_msg

    # Моки
    mock_cb = AsyncMock(spec=CallbackQuery)
    mock_message = AsyncMock(spec=Message)
    mock_cb.message = mock_message

    mock_message.edit_text = AsyncMock(return_value=None)

    # Тест
    await _safe_edit(mock_cb, "Test text", parse_mode="HTML")

    mock_message.edit_text.assert_called_once_with("Test text", parse_mode="HTML")


@pytest.mark.asyncio
async def test_safe_edit_msg_not_modified():
    """Тест функции _safe_edit_msg с ошибкой 'message is not modified'."""
    from aiogram.types import CallbackQuery, Message, TelegramBadRequest
    from bot import _safe_edit_msg

    # Моки
    mock_cb = AsyncMock(spec=CallbackQuery)
    mock_message = AsyncMock(spec=Message)
    mock_cb.message = mock_message

    from aiogram.exceptions import TelegramBadRequest

    error = TelegramBadRequest("message is not modified: new content is the same")
    mock_message.edit_text = AsyncMock(side_effect=error)

    # Тест
    await _safe_edit(mock_cb, "Test text")

    # Не должно вызывать исключение
    mock_message.edit_text.assert_called_once()


@pytest.mark.asyncio
async def test_safe_edit_msg_other_error():
    """Тест функции _safe_edit_msg с другой ошибкой."""
    from aiogram.types import CallbackQuery, Message
    from bot import _safe_edit_msg

    # Моки
    mock_cb = AsyncMock(spec=CallbackQuery)
    mock_message = AsyncMock(spec=Message)
    mock_cb.message = mock_message

    from aiogram.exceptions import TelegramBadRequest

    error = TelegramBadRequest("Bad request: chat not found")
    mock_message.edit_text = AsyncMock(side_effect=error)

    # Тест - должно вызывать исключение
    with pytest.raises(TelegramBadRequest):
        await _safe_edit(mock_cb, "Test text")
