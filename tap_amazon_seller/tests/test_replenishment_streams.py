"""Unit tests for replenishment stream helpers."""

from datetime import datetime, timedelta
from unittest.mock import MagicMock

import pytest

from tap_amazon_seller.replenishment_streams import (
    FORECAST_REQUEST_WINDOW_DAYS,
    ReplenishmentDailyMetricsStream,
    ReplenishmentOfferForecastStream,
    ReplenishmentStreamBase,
)
from tap_amazon_seller.tap import TapAmazonSeller

SAMPLE_CONFIG = {
    "lwa_client_id": "fake-client-id",
    "client_secret": "fake-client-secret",
    "refresh_token": "fake-refresh-token",
    "start_date": "2024-01-01T00:00:00Z",
}


@pytest.fixture
def tap():
    """Minimal tap instance for replenishment stream tests."""
    return TapAmazonSeller(config=SAMPLE_CONFIG, validate_config=False)


@pytest.fixture
def stream(tap):
    """Replenishment daily stream wired to the test tap."""
    return ReplenishmentDailyMetricsStream(tap=tap)


def test_parse_report_end_date(stream):
    assert stream.parse_report_end_date({"endDate": "2026-05-31T00:00:00Z"}) == "2026-05-31"
    assert stream.parse_report_end_date({}) is None


def test_parse_interval_bounds():
    start, end = ReplenishmentStreamBase.parse_interval_bounds(
        {"startDate": "2026-01-01T00:00:00Z", "endDate": "2026-06-01T00:00:00Z"}
    )
    assert start == "2026-01-01T00:00:00Z"
    assert end == "2026-06-01T00:00:00Z"
    assert ReplenishmentStreamBase.parse_interval_bounds(None) == (None, None)


def test_clamp_start_to_lookback(stream):
    old_start = datetime.now() - timedelta(days=900)
    clamped = stream.clamp_start_to_lookback(old_start)
    assert clamped >= datetime.now() - timedelta(days=stream.lookback_days + 1)


def test_resolve_max_sync_end_respects_config_end_date():
    config = dict(SAMPLE_CONFIG, end_date="2020-01-01T00:00:00Z")
    tap = TapAmazonSeller(config=config, validate_config=False)
    stream = ReplenishmentDailyMetricsStream(tap=tap)
    max_end = stream.resolve_max_sync_end()
    assert max_end == datetime(2020, 1, 1)


def test_forecast_request_interval_length():
    interval = ReplenishmentStreamBase.forecast_request_interval()
    start = datetime.strptime(interval["startDate"], "%Y-%m-%dT00:00:00Z")
    end = datetime.strptime(interval["endDate"], "%Y-%m-%dT00:00:00Z")
    assert (end - start).days == FORECAST_REQUEST_WINDOW_DAYS - 1


def test_build_forecast_filters(tap):
    forecast_stream = ReplenishmentOfferForecastStream(tap=tap)
    forecast_stream.get_marketplace_api_id = MagicMock(return_value="TEST-MARKETPLACE-ID")

    filters = forecast_stream.build_forecast_filters("US")

    assert filters["timePeriodType"] == "FORECAST"
    assert filters["programTypes"] == ["SUBSCRIBE_AND_SAVE"]
    assert filters["marketplaceId"] == "TEST-MARKETPLACE-ID"
    assert "aggregationFrequency" not in filters
    assert filters["timeInterval"]["startDate"].endswith("T00:00:00Z")
