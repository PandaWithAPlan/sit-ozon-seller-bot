import datetime as dt
from unittest.mock import patch, MagicMock
import pytest
from modules_sales import sales_traffic

# Mock data
MOCK_RESPONSE_DATA = {
    "result": {
        "data": [
            # Standard case: direct date field
            {
                "sku": "111",
                "date": "2023-10-27",
                "metrics": [100, 10, 50, 5]
            },
            # Case: date in dimensions (valid)
            {
                "dimensions": [
                    {"id": "222"}, # sku
                    {"id": "2023-10-26"} # date
                ],
                "metrics": [200, 20, 100, 10]
            },
            # Case: date in dimensions (invalid date string should be ignored)
            {
                "dimensions": [
                    {"id": "333"},
                    {"id": "not-a-date"}
                ],
                "metrics": [0, 0, 0, 0]
            },
            # Case: invalid direct date field (should be ignored)
            {
                "sku": "444",
                "date": "invalid-date",
                "metrics": [10, 1, 5, 0]
            }
        ]
    }
}

@pytest.mark.asyncio
@patch("modules_sales.sales_traffic._fetch_traffic")
@patch("modules_sales.sales_traffic._allowed_set")
@patch("modules_sales.sales_traffic._read_cache") # ensure cache is ignored
async def test_collect_traffic_matrix_parsing(mock_read_cache, mock_allowed_set, mock_fetch_traffic):
    # Setup mocks
    async def async_return(*args, **kwargs):
        return MOCK_RESPONSE_DATA

    mock_fetch_traffic.side_effect = async_return
    mock_allowed_set.return_value = {111, 222, 333, 444} # Allow all SKUs in test data
    mock_read_cache.return_value = {}

    # Call the function under test
    # Period days doesn't matter much for parsing logic, just needs to cover the dates in mock data
    # Mock data dates are 2023-10-27 and 2023-10-26.
    # The function calculates range relative to today.
    # To ensure we capture these dates, we need to mock "today" or allow a very long range.
    # Easier to mock datetime.date inside the module, but that's hard.
    # Alternatively, construct dynamic dates in mock data relative to today.

    today = dt.date.today()
    d1 = (today - dt.timedelta(days=1)).strftime("%Y-%m-%d")
    d2 = (today - dt.timedelta(days=2)).strftime("%Y-%m-%d")

    # Update mock data with dynamic dates
    MOCK_RESPONSE_DATA["result"]["data"][0]["date"] = d1
    MOCK_RESPONSE_DATA["result"]["data"][1]["dimensions"][1]["id"] = d2

    # mock_fetch_traffic side_effect is set

    matrix = await sales_traffic._collect_traffic_matrix(days=30)

    # Verify results

    # Check SKU 111 (Direct date)
    assert 111 in matrix
    d1_date = dt.datetime.strptime(d1, "%Y-%m-%d").date()
    assert d1_date in matrix[111]
    assert matrix[111][d1_date] == (100.0, 10.0, 50.0, 5.0)

    # Check SKU 222 (Dimension date)
    # Note: Logic in sales_traffic.py extracts SKU from dimensions if not present.
    # In my mock data for SKU 222, I put sku in dimensions[0].id.
    # The code tries: sku = ... dimensions[0].id
    assert 222 in matrix
    d2_date = dt.datetime.strptime(d2, "%Y-%m-%d").date()
    assert d2_date in matrix[222]
    assert matrix[222][d2_date] == (200.0, 20.0, 100.0, 10.0)

    # Check SKU 333 (Invalid date in dimension) -> Should not be in matrix
    assert 333 not in matrix

    # Check SKU 444 (Invalid direct date) -> Should not be in matrix
    assert 444 not in matrix

if __name__ == "__main__":
    # Manually run the test function if executed directly (for quick check)
    pytest.main([__file__])
