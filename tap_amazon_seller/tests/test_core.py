"""Tests standard tap features using the built-in SDK tests library."""

import datetime
from datetime import timedelta

from singer_sdk.testing import get_standard_tap_tests

from tap_amazon_seller.streams import ListTransactionsStream
from tap_amazon_seller.tap import TapAmazonSeller

SAMPLE_CONFIG = {
    "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
    # TODO: Initialize minimal tap config
}


# Run standard built-in tap tests from the SDK:
def test_standard_tap_tests():
    """Run standard tap tests from the SDK."""
    tests = get_standard_tap_tests(TapAmazonSeller, config=SAMPLE_CONFIG)
    for test in tests:
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
