"""Tests standard tap features using the built-in SDK tests library."""

import datetime
from datetime import timedelta

from singer_sdk.testing import get_standard_tap_tests

from tap_amazon_seller.streams import ListTransactionsStream
from tap_amazon_seller.tap import TapAmazonSeller

SAMPLE_CONFIG = {
    "lwa_client_id": "test-client-id",
    "client_secret": "test-client-secret",
    "refresh_token": "test-refresh-token",
    "sandbox": True,
    "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d"),
}


# Run standard built-in tap tests from the SDK:
def test_standard_tap_tests():
    """Run standard tap tests from the SDK (skip live connection test)."""
    tests = get_standard_tap_tests(TapAmazonSeller, config=SAMPLE_CONFIG)
    for test in tests[:2]:
        test()


def test_list_transactions_stream_meta():
    assert ListTransactionsStream.primary_keys == ["transactionId"]
    assert ListTransactionsStream.replication_key == "postedDate"
    assert ListTransactionsStream.WINDOW_DAYS == 30
    assert ListTransactionsStream.WINDOW_DAYS <= 180
    props = ListTransactionsStream.schema["properties"]
    assert "transactionId" in props
    assert "postedDate" in props
    start = datetime.datetime(2024, 1, 1, 0, 0, 0)
    end = datetime.datetime(2024, 3, 1, 0, 0, 0)
    windows = list(ListTransactionsStream.iter_posted_windows(start, end, 30))
    assert len(windows) == 2
    assert windows[0] == (start, start + timedelta(days=30))
    assert windows[1][0] == windows[0][1]
    assert windows[-1][1] == end


def test_list_transactions_clamp_to_retention_inside_window():
    now = datetime.datetime(2026, 8, 17, 12, 17, 2)
    start = datetime.datetime(2025, 1, 1, 0, 0, 0)
    assert ListTransactionsStream.clamp_to_retention(start, now) == start


def test_list_transactions_clamp_to_retention_old_start():
    now = datetime.datetime(2026, 8, 17, 12, 17, 2)
    start = datetime.datetime(2000, 1, 1, 0, 0, 1)
    clamped = ListTransactionsStream.clamp_to_retention(start, now)
    boundary = now - timedelta(days=730)
    expected = boundary + timedelta(hours=ListTransactionsStream.RETENTION_BUFFER_HOURS)
    assert clamped == expected
    assert clamped > boundary


def test_list_transactions_clamp_to_retention_exact_boundary():
    now = datetime.datetime(2026, 8, 17, 12, 17, 2)
    boundary = now - timedelta(days=730)
    clamped = ListTransactionsStream.clamp_to_retention(boundary, now)
    assert clamped == boundary + timedelta(hours=ListTransactionsStream.RETENTION_BUFFER_HOURS)
