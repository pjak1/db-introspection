from datetime import date, datetime
from decimal import Decimal
from uuid import UUID

from src.adapters.normalization import normalize_row


def test_normalization_serializes_common_types():
    row = {
        "amount": Decimal("12.34"),
        "day": date(2025, 1, 2),
        "created_at": datetime(2025, 1, 2, 3, 4, 5),
        "id": UUID("00000000-0000-0000-0000-000000000001"),
    }
    normalized = normalize_row(row)
    assert normalized["amount"] == "12.34"
    assert normalized["day"] == "2025-01-02"
    assert normalized["created_at"].startswith("2025-01-02T03:04:05")
    assert normalized["id"] == "00000000-0000-0000-0000-000000000001"
