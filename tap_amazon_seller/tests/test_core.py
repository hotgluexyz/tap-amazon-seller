"""Tests standard tap features using the built-in SDK tests library."""

import datetime
from datetime import timedelta

from singer_sdk.testing import get_standard_tap_tests

from tap_amazon_seller.streams import (
    ListOrderFinancialEventsStream,
    OrderFinancialEvents,
)
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


def test_list_order_financial_events_pk_and_windows():
    assert ListOrderFinancialEventsStream.WINDOW_DAYS == 30
    assert ListOrderFinancialEventsStream.RETENTION_DAYS == 730
    assert ListOrderFinancialEventsStream.primary_keys is None
    assert ListOrderFinancialEventsStream.replication_key == "LastUpdateDate"
    ofe = OrderFinancialEvents.schema["properties"]
    props = ListOrderFinancialEventsStream.schema["properties"]
    assert set(ofe) <= set(props)
    assert set(props) - set(ofe) == {"marketplace_id"}
    # hotglue singer validate: only one non-null type per field
    shipment = props["ShipmentEventList"]["type"]
    non_null = [t for t in (shipment if isinstance(shipment, list) else [shipment]) if t != "null"]
    assert non_null == ["array"]

    start = datetime.datetime(2024, 1, 1, 0, 0, 0)
    end = datetime.datetime(2024, 3, 1, 0, 0, 0)
    windows = list(
        ListOrderFinancialEventsStream.iter_posted_windows(start, end, 30)
    )
    assert len(windows) == 2
    assert windows[0] == (start, start + timedelta(days=30))
    assert windows[1][0] == windows[0][1]
    assert windows[-1][1] == end
    for after, before in windows:
        assert (before - after) <= timedelta(days=30)


def test_clamp_to_retention():
    end = datetime.datetime(2026, 7, 24, 12, 0, 0)
    earliest = end - timedelta(days=730)
    assert (
        ListOrderFinancialEventsStream.clamp_to_retention(
            datetime.datetime(2000, 1, 1), end
        )
        == earliest
    )
    inside = end - timedelta(days=100)
    assert ListOrderFinancialEventsStream.clamp_to_retention(inside, end) == inside


def test_group_financial_events_by_order():
    fe = {
        "ShipmentEventList": [
            {
                "AmazonOrderId": "111-1",
                "PostedDate": "2026-07-01T00:01:01Z",
                "ShipmentItemList": [{"SellerSKU": "A"}],
            },
            {
                "AmazonOrderId": "111-2",
                "PostedDate": "2026-07-01T00:02:00Z",
            },
        ],
        "RefundEventList": [
            {
                "AmazonOrderId": "111-1",
                "PostedDate": "2026-07-02T00:00:00Z",
            }
        ],
        "AdjustmentEventList": [{"AdjustmentType": "no-order-id"}],
    }
    records = {
        r["AmazonOrderId"]: r
        for r in ListOrderFinancialEventsStream.group_financial_events_by_order(fe)
    }
    assert set(records) == {"111-1", "111-2"}
    assert len(records["111-1"]["ShipmentEventList"]) == 1
    assert len(records["111-1"]["RefundEventList"]) == 1
    assert records["111-1"]["LastUpdateDate"] == "2026-07-02T00:00:00Z"
    assert records["111-2"]["LastUpdateDate"] == "2026-07-01T00:02:00Z"
    assert "RefundEventList" not in records["111-2"]
    assert "AdjustmentEventList" not in records["111-1"]


def test_max_posted_date():
    fe = {
        "ShipmentEventList": [
            {"AmazonOrderId": "1", "PostedDate": "2026-07-01T00:01:01Z"},
            {"AmazonOrderId": "2", "PostedDate": "2026-07-01T00:02:00Z"},
        ],
        "RefundEventList": [
            {"AmazonOrderId": "1", "PostedDate": "2026-07-02T00:00:00Z"},
        ],
        "AdjustmentEventList": [],
    }
    assert (
        ListOrderFinancialEventsStream.max_posted_date(fe) == "2026-07-02T00:00:00Z"
    )
    assert (
        ListOrderFinancialEventsStream.max_posted_date({"ShipmentEventList": []})
        is None
    )


def test_filter_order_financial_event_fields():
    fe = {
        "ShipmentEventList": [{"AmazonOrderId": "1"}],
        "PerformanceBondRefundEventList": [{"something": 1}],
        "TDSReimbursementEventList": [],
    }
    assert ListOrderFinancialEventsStream.filter_order_financial_event_fields(
        fe
    ) == {
        "ShipmentEventList": [{"AmazonOrderId": "1"}],
    }
