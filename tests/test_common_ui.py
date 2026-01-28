import pytest
from modules_common import ui
from aiogram.types import InlineKeyboardMarkup

@pytest.mark.asyncio
async def test_build_main_menu_kb():
    kb = ui.build_main_menu_kb()
    assert isinstance(kb, InlineKeyboardMarkup)

    # Check for new sections
    texts = [btn.text for row in kb.inline_keyboard for btn in row]
    assert any("Финансы" in t for t in texts)
    assert any("Цены" in t for t in texts)
    assert any("Маркетинг" in t for t in texts)

@pytest.mark.asyncio
async def test_build_method_kb():
    kb = ui.build_method_kb()
    assert isinstance(kb, InlineKeyboardMarkup)
    texts = [btn.text for row in kb.inline_keyboard for btn in row]
    # Check for some method names (localized)
    assert any("30 дней" in t for t in texts)
    assert any("Домой" in t for t in texts)
