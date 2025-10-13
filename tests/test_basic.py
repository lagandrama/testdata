from health_sync import UNIFIED_HEADER
from health_sync.models import UnifiedRow


def test_unified_header_order_and_length():
    assert UNIFIED_HEADER[0] == "date"
    assert UNIFIED_HEADER[1] == "source"
    assert len(UNIFIED_HEADER) == 22


def test_unified_row_length_matches_header():
    row = UnifiedRow(date="2025-10-07", source="test").as_row()
    assert len(row) == len(UNIFIED_HEADER)
