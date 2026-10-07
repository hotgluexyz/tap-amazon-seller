"""Unit tests for the orders streams and marketplace discovery."""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from tap_amazon_seller.orders_transform import (
    ORDER_V2_CONTEXT_KEY,
    SEARCH_INCLUDED_DATA_BASE,
    OrderPayload,
)
from tap_amazon_seller.streams import MarketplacesStream, OrdersStream
from tap_amazon_seller.tap import TapAmazonSeller

SAMPLE_CONFIG = {
    "lwa_client_id": "fake-client-id",
    "client_secret": "fake-client-secret",
    "refresh_token": "fake-refresh-token",
    "start_date": "2026-10-01T00:00:00Z",
    "end_date": "2026-10-02T00:00:00Z",
}


def make_stream(stream_class, **config):
    tap = TapAmazonSeller(config={**SAMPLE_CONFIG, **config}, validate_config=False)
    return stream_class(tap=tap)


def participation(marketplace_id, country_code, participating=True):
    return {
        "marketplace": {
            "id": marketplace_id,
            "countryCode": country_code,
            "name": f"Amazon {country_code}",
        },
        "participation": {"isParticipating": participating},
    }


def stub_participations(stream, entries):
    sellers = MagicMock()
    sellers.get_marketplace_participation.return_value = SimpleNamespace(payload=entries)
    stream.get_sp_sellers = lambda: sellers


@pytest.fixture
def marketplaces_stream():
    return make_stream(MarketplacesStream)


@pytest.fixture
def orders_stream():
    return make_stream(OrdersStream)


def test_valid_marketplaces_skip_inactive_and_unknown(marketplaces_stream):
    stub_participations(
        marketplaces_stream,
        [
            participation("ATVPDKIKX0DER", "US"),
            participation("A2EUQ1WTGCTBG2", "CA", participating=False),
            participation("UNKNOWN-ID", "ZZ"),
        ],
    )
    assert marketplaces_stream.get_valid_marketplaces() == [
        {"id": "US", "name": "Amazon US"}
    ]


@pytest.mark.parametrize("configured", [["CA"], "CA", " CA , MX"])
def test_valid_marketplaces_filtered_by_config(configured):
    stream = make_stream(MarketplacesStream, marketplaces=configured)
    stub_participations(
        stream,
        [participation("ATVPDKIKX0DER", "US"), participation("A2EUQ1WTGCTBG2", "CA")],
    )
    assert [mp["id"] for mp in stream.get_valid_marketplaces()] == ["CA"]


def test_orders_yield_v0_record_with_child_context(orders_stream):
    order_v2 = {"orderId": "111-1", "createdTime": "2026-10-01T10:00:00Z"}
    orders_stream.load_order_page = MagicMock(return_value=iter([[order_v2]]))

    ((record, child_context),) = orders_stream.get_records({"marketplace_id": "US"})

    assert record["AmazonOrderId"] == "111-1"
    assert child_context == {
        "AmazonOrderId": "111-1",
        "marketplace_id": "US",
        ORDER_V2_CONTEXT_KEY: order_v2,
    }


def test_orders_search_window_and_included_data(orders_stream):
    orders_stream.load_order_page = MagicMock(return_value=iter([]))
    context = {"marketplace_id": "US"}
    orders_stream._write_starting_replication_value(context)

    list(orders_stream.get_records(context))

    kwargs = orders_stream.load_order_page.call_args.kwargs
    assert kwargs["mp"] == "US"
    assert kwargs["lastUpdatedAfter"] == "2026-10-01T00:00:00Z"
    assert kwargs["lastUpdatedBefore"] == "2026-10-02T00:00:00Z"
    assert kwargs["includedData"] == sorted(SEARCH_INCLUDED_DATA_BASE)


def test_orders_sandbox_ignores_marketplace_context():
    stream = make_stream(OrdersStream, sandbox=True)
    stream.load_order_page = MagicMock(return_value=iter([]))

    list(stream.get_records({"marketplace_id": "US"}))

    kwargs = stream.load_order_page.call_args.kwargs
    assert kwargs["sandbox"] is True
    assert kwargs["mp"] == "JP"
    assert "createdAfter" in kwargs


def test_child_context_repr_hides_order_contents():
    order = OrderPayload({"orderId": "111-1", "buyer": {"buyerEmail": "buyer@example.com"}})
    context = {"AmazonOrderId": "111-1", ORDER_V2_CONTEXT_KEY: order}

    assert "buyer@example.com" not in str(context)
    assert "111-1" in str(context)
