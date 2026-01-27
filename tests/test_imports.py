import pytest
import sys
import os

# Ensure repo root is in path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

def test_bot_import():
    """Checks if bot.py can be imported (no syntax errors or missing deps)."""
    try:
        import bot
    except ImportError as e:
        pytest.fail(f"Failed to import bot: {e}")
    except Exception as e:
        pytest.fail(f"Failed to load bot module: {e}")

def test_routers_import():
    """Checks if all routers can be imported."""
    try:
        from routers import finance
        from routers import marketing
        from routers import operations
    except ImportError as e:
        pytest.fail(f"Failed to import routers: {e}")
