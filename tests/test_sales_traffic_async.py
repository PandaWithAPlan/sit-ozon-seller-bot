import pytest
from unittest.mock import patch, AsyncMock
from modules_sales import sales_traffic

@pytest.mark.asyncio
async def test_read_cache_async():
    with patch("modules_sales.sales_traffic._read_cache_sync") as mock_sync:
        mock_sync.return_value = {"rows": {}}
        result = await sales_traffic._read_cache()
        assert result == {"rows": {}}
        mock_sync.assert_called_once()

@pytest.mark.asyncio
async def test_write_cache_async():
    with patch("modules_sales.sales_traffic._write_cache_sync") as mock_sync:
        await sales_traffic._write_cache({"data": 123})
        mock_sync.assert_called_once_with({"data": 123})

@pytest.mark.asyncio
async def test_fetch_traffic_session_reuse():
    # We want to verify that _fetch_traffic creates a session and passes it to _try_fetch
    # We mock _try_fetch to verify it receives the session
    with patch("modules_sales.sales_traffic._try_fetch", new_callable=AsyncMock) as mock_try_fetch:
        mock_try_fetch.return_value = {}

        mock_session = AsyncMock()
        mock_session.__aenter__.return_value = mock_session
        mock_session.__aexit__.return_value = None

        # Mock ClientSession context manager to return our mock session
        with patch("aiohttp.ClientSession", return_value=mock_session):
            await sales_traffic._fetch_traffic("2023-01-01", "2023-01-02")

            # Verify _try_fetch was called at least once
            assert mock_try_fetch.call_count >= 1

            # Verify the session argument was passed
            # _try_fetch(payload, tag, session=session)
            call_args = mock_try_fetch.call_args
            kwargs = call_args.kwargs
            assert "session" in kwargs
            assert kwargs["session"] == mock_session

@pytest.mark.asyncio
async def test_try_fetch_uses_session():
    # Setup
    payload = {"foo": "bar"}
    tag = "test"

    # Mock _do_fetch because _try_fetch calls it
    with patch("modules_sales.sales_traffic._do_fetch", new_callable=AsyncMock) as mock_do_fetch:
        mock_do_fetch.return_value = {"result": "ok"}

        # Scenario 1: session provided
        mock_session = AsyncMock()
        await sales_traffic._try_fetch(payload, tag, session=mock_session)
        mock_do_fetch.assert_called_with(mock_session, payload, tag)

        mock_do_fetch.reset_mock()

        # Scenario 2: session not provided
        # We need to mock ClientSession context manager
        mock_temp_session = AsyncMock()
        mock_temp_session.__aenter__.return_value = mock_temp_session
        mock_temp_session.__aexit__.return_value = None

        with patch("aiohttp.ClientSession", return_value=mock_temp_session):
            await sales_traffic._try_fetch(payload, tag)
            mock_do_fetch.assert_called_with(mock_temp_session, payload, tag)
