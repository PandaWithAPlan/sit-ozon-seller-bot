import re
from modules_sales.sales_facts_store import _now_stamp

def test_now_stamp_format():
    """Test that _now_stamp returns a string with the correct format and no Cyrillic M."""
    timestamp = _now_stamp()

    # Check format: DD.MM.YYYY HH:MM
    # Regex: ^\d{2}\.\d{2}\.\d{4} \d{2}:\d{2}$
    assert re.match(r"^\d{2}\.\d{2}\.\d{4} \d{2}:\d{2}$", timestamp), f"Timestamp '{timestamp}' does not match format DD.MM.YYYY HH:MM"

    # Check for Cyrillic 'М' (U+041C)
    assert 'М' not in timestamp, "Timestamp contains Cyrillic 'М'"

    # Check for literal %M (which might happen if strftime doesn't substitute correctly)
    assert '%M' not in timestamp, "Timestamp contains literal '%M'"
