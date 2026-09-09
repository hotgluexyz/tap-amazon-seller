"""Subscribe & Save replenishment streams for Amazon Seller Central.

SP-API Replenishment API (2022-11-07). Sellers support both PERFORMANCE and
FORECAST on listOfferMetrics; getSellingPartnerMetrics uses PERFORMANCE only here.

Partner-level getSellingPartnerMetrics returns mixed metric bundles. We split
daily KPIs (incremental) from rolling windows (full sync).

Offer-level listOfferMetrics is split into historical daily PERFORMANCE metrics
(incremental) and forward-looking FORECAST projections (full sync snapshot).

API reference:
- https://developer-docs.amazon.com/sp-api/reference/getsellingpartnermetrics
- https://developer-docs.amazon.com/sp-api/reference/listoffermetrics
"""

from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Optional, Tuple

import backoff
from dateutil.parser import parse
from singer_sdk import typing as th
from sp_api.base import Marketplaces
from sp_api.base.exceptions import SellingApiBadRequestException, SellingApiForbiddenException

from tap_amazon_seller.client import AmazonSellerStream
from tap_amazon_seller.exceptions import InvalidReportParameter, PermissionError, report_giveup
from tap_amazon_seller.streams import MarketplacesStream
from tap_amazon_seller.utils import InvalidResponse

PROGRAM_TYPES = ["SUBSCRIBE_AND_SAVE"]
PERFORMANCE = "PERFORMANCE"
FORECAST = "FORECAST"
AGGREGATION_DAY = "DAY"
MAX_DAY_CHUNK = 31
OFFER_PAGE_SIZE = 500
MAX_OFFER_OFFSET = 9000
FORECAST_REQUEST_WINDOW_DAYS = 7

DAILY_PERFORMANCE_METRICS = [
    "SHIPPED_SUBSCRIPTION_UNITS",
    "TOTAL_SUBSCRIPTIONS_REVENUE",
    "ACTIVE_SUBSCRIPTIONS",
    "NOT_DELIVERED_DUE_TO_OOS",
    "LOST_REVENUE_DUE_TO_OOS",
    "REVENUE_PENETRATION",
    "COUPONS_REVENUE_PENETRATION",
    "SHARE_OF_COUPON_SUBSCRIPTIONS",
]

ROLLING_PERFORMANCE_METRICS = [
    "SUBSCRIBER_NON_SUBSCRIBER_AVERAGE_REVENUE",
    "SUBSCRIBER_NON_SUBSCRIBER_AVERAGE_REORDERS",
    "SUBSCRIBER_RETENTION",
    "SUBSCRIBER_LIFETIME_VALUE_BY_CUSTOMER_SEGMENT",
    "SIGNUP_CONVERSION_BY_SELLER_FUNDING",
    "REVENUE_BY_DELIVERIES",
    "REVENUE_PENETRATION_BY_SELLER_FUNDING",
]

_DAILY_METRIC_SCHEMA = [
    th.Property("shippedSubscriptionUnits", th.NumberType),
    th.Property("totalSubscriptionsRevenue", th.NumberType),
    th.Property("activeSubscriptions", th.NumberType),
    th.Property("notDeliveredDueToOOS", th.NumberType),
    th.Property("lostRevenueDueToOOS", th.NumberType),
    th.Property("revenuePenetration", th.NumberType),
    th.Property("couponsRevenuePenetration", th.NumberType),
    th.Property("shareOfCouponSubscriptions", th.NumberType),
    th.Property("currencyCode", th.StringType),
    th.Property("timeInterval", th.CustomType({"type": ["object", "string"]})),
]

_ROLLING_METRIC_SCHEMA = [
    th.Property("subscriberAverageRevenue", th.NumberType),
    th.Property("nonSubscriberAverageRevenue", th.NumberType),
    th.Property("subscriberAverageReorders", th.NumberType),
    th.Property("nonSubscriberAverageReorders", th.NumberType),
    th.Property("subscriberRetentionFor30Days", th.NumberType),
    th.Property("subscriberRetentionFor90Days", th.NumberType),
    th.Property("growingSubscriberLifeTimeValueFromOTP", th.NumberType),
    th.Property("growingSubscriberLifeTimeValueFromSNS", th.NumberType),
    th.Property("establishedSubscriberLifeTimeValueFromOTP", th.NumberType),
    th.Property("establishedSubscriberLifeTimeValueFromSNS", th.NumberType),
    th.Property("lostSubscriberLifeTimeValueFromOTP", th.NumberType),
    th.Property("lostSubscriberLifeTimeValueFromSNS", th.NumberType),
    th.Property("nonSubscriberLifeTimeValueFromOTP", th.NumberType),
    th.Property("signupConversionFor0PercentSellerFunding", th.NumberType),
    th.Property("signupConversionFor5PlusPercentSellerFunding", th.NumberType),
    th.Property("signupConversionFor5PercentSellerFunding", th.NumberType),
    th.Property("signupConversionFor10PercentSellerFunding", th.NumberType),
    th.Property("revenueFromSubscriptionsWithMultipleDeliveries", th.NumberType),
    th.Property("revenueFromActiveSubscriptionsWithSingleDelivery", th.NumberType),
    th.Property("revenueFromCancelledSubscriptionsAfterSingleDelivery", th.NumberType),
    th.Property("revenuePenetrationFor0PercentSellerFunding", th.NumberType),
    th.Property("revenuePenetrationFor5PlusPercentSellerFunding", th.NumberType),
    th.Property("revenuePenetrationFor5PercentSellerFunding", th.NumberType),
    th.Property("currencyCode", th.StringType),
    th.Property("timeInterval", th.CustomType({"type": ["object", "string"]})),
]

_OFFER_PERFORMANCE_SCHEMA = [
    th.Property("asin", th.StringType),
    th.Property("sku", th.StringType),
    th.Property("brandName", th.StringType),
    th.Property("productGroup", th.StringType),
    th.Property("fulfillmentChannelType", th.StringType),
    th.Property("shippedSubscriptionUnits", th.NumberType),
    th.Property("totalSubscriptionsRevenue", th.NumberType),
    th.Property("activeSubscriptions", th.NumberType),
    th.Property("notDeliveredDueToOOS", th.NumberType),
    th.Property("lostRevenueDueToOOS", th.NumberType),
    th.Property("revenuePenetration", th.NumberType),
    th.Property("couponsRevenuePenetration", th.NumberType),
    th.Property("shareOfCouponSubscriptions", th.NumberType),
    th.Property("currencyCode", th.StringType),
    th.Property("timeInterval", th.CustomType({"type": ["object", "string"]})),
]

_OFFER_FORECAST_SCHEMA = [
    th.Property("asin", th.StringType),
    th.Property("sku", th.StringType),
    th.Property("fulfillmentChannelType", th.StringType),
    th.Property("next30DayShippedSubscriptionUnits", th.NumberType),
    th.Property("next30DayTotalSubscriptionsRevenue", th.NumberType),
    th.Property("next60DayShippedSubscriptionUnits", th.NumberType),
    th.Property("next60DayTotalSubscriptionsRevenue", th.NumberType),
    th.Property("next90DayShippedSubscriptionUnits", th.NumberType),
    th.Property("next90DayTotalSubscriptionsRevenue", th.NumberType),
    th.Property("currencyCode", th.StringType),
    th.Property("timeInterval", th.CustomType({"type": ["object", "string"]})),
    th.Property("interval_start_date", th.StringType),
    th.Property("interval_end_date", th.StringType),
    th.Property("synced_at", th.DateTimeType),
]


class ReplenishmentStreamBase(AmazonSellerStream):
    """Shared helpers for Replenishment API streams."""

    parent_stream_type = MarketplacesStream
    lookback_days = 730
    correct_end_date_minus_days = 2

    def get_marketplace_api_id(self, marketplace_code: str) -> str:
        """Return SP-API marketplace id for a marketplace code (e.g. US)."""
        return Marketplaces[marketplace_code].marketplace_id

    def format_day(self, day: datetime) -> str:
        """Format a datetime as an SP-API day boundary timestamp."""
        return day.strftime("%Y-%m-%dT00:00:00Z")

    def day_interval(self, day: datetime) -> Dict[str, str]:
        """Build a single-day timeInterval for Replenishment API requests."""
        formatted = self.format_day(day)
        return {"startDate": formatted, "endDate": formatted}

    @staticmethod
    def forecast_request_interval(window_days: int = FORECAST_REQUEST_WINDOW_DAYS) -> Dict[str, str]:
        """Build a fixed future window for FORECAST listOfferMetrics requests."""
        start = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
        end = start + timedelta(days=window_days - 1)
        return {"startDate": start.strftime("%Y-%m-%dT00:00:00Z"), "endDate": end.strftime("%Y-%m-%dT00:00:00Z")}

    def parse_report_end_date(self, time_interval: Optional[dict]) -> Optional[str]:
        """Parse report_end_date from a Replenishment timeInterval."""
        if not time_interval or not time_interval.get("endDate"):
            return None
        return parse(time_interval["endDate"]).strftime("%Y-%m-%dT00:00:00Z")

    @staticmethod
    def parse_interval_bounds(time_interval: Optional[dict]) -> Tuple[Optional[str], Optional[str]]:
        """Return start/end strings from a Replenishment timeInterval."""
        if not time_interval:
            return None, None
        return time_interval.get("startDate"), time_interval.get("endDate")

    def resolve_sync_start(self, context: Optional[dict]) -> datetime:
        """Resolve the incremental sync start datetime (naive local)."""
        start_date = self.get_starting_timestamp(context)
        if start_date:
            return start_date.replace(tzinfo=None)
        if self.config.get("start_date"):
            return parse(self.config.get("start_date")).replace(tzinfo=None)
        return datetime.now() - timedelta(days=self.lookback_days)

    def resolve_max_sync_end(self) -> datetime:
        """Return the latest day to sync, applying lag and optional config end_date."""
        current_date = datetime.now()
        max_end = current_date - timedelta(days=self.correct_end_date_minus_days)
        if self.config.get("end_date"):
            config_end = parse(self.config.get("end_date")).replace(tzinfo=None)
            if config_end < max_end:
                max_end = config_end
        return max_end.replace(hour=0, minute=0, second=0, microsecond=0)

    def clamp_start_to_lookback(self, start_date: datetime) -> datetime:
        """Clamp start_date to the API trailing window."""
        minimum_start = datetime.now() - timedelta(days=self.lookback_days)
        if start_date < minimum_start:
            return minimum_start
        return start_date

    def translate_replenishment_error(self, exc: Exception) -> None:
        """Map SP-API errors to tap exceptions for giveup and child-stream handling."""
        if isinstance(exc, SellingApiForbiddenException):
            raise PermissionError(exc.error or exc.message or str(exc)) from exc
        if isinstance(exc, SellingApiBadRequestException):
            raise InvalidReportParameter(exc.error or exc.message or str(exc)) from exc
        raise InvalidResponse(exc) from exc

    @backoff.on_exception(
        backoff.expo,
        Exception,
        max_tries=10,
        factor=3,
        giveup=report_giveup,
    )
    def get_selling_partner_metrics(
        self,
        marketplace_code: str,
        time_interval: Dict[str, str],
        metrics: List[str],
    ) -> dict:
        """Call getSellingPartnerMetrics for a marketplace."""
        client = self.get_sp_replenishment(marketplace_code)
        try:
            response = client.get_selling_partner_metrics(
                marketplaceId=self.get_marketplace_api_id(marketplace_code),
                timePeriodType=PERFORMANCE,
                programTypes=PROGRAM_TYPES,
                timeInterval=time_interval,
                aggregationFrequency=AGGREGATION_DAY,
                metrics=metrics,
            )
        except Exception as exc:
            self.translate_replenishment_error(exc)
        return response.payload


class OfferMetricsStreamBase(ReplenishmentStreamBase):
    """Shared listOfferMetrics pagination for offer-level replenishment streams."""

    @backoff.on_exception(
        backoff.expo,
        Exception,
        max_tries=10,
        factor=3,
        giveup=report_giveup,
    )
    def list_offer_metrics_page(
        self,
        marketplace_code: str,
        filters: Dict[str, object],
        offset: int,
    ) -> dict:
        """Fetch one page of listOfferMetrics."""
        client = self.get_sp_replenishment(marketplace_code)
        try:
            response = client.list_offer_metrics(
                pagination={"limit": OFFER_PAGE_SIZE, "offset": offset},
                filters=filters,
            )
        except Exception as exc:
            self.translate_replenishment_error(exc)
        return response.payload

    def iter_all_offers(
        self,
        marketplace_code: str,
        filters: Dict[str, object],
    ) -> Iterable[dict]:
        """Yield all offer records for a listOfferMetrics query."""
        time_interval = filters.get("timeInterval") or {}
        interval_start = time_interval.get("startDate", "")
        offset = 0
        while True:
            payload = self.list_offer_metrics_page(marketplace_code, filters, offset)
            offers = payload.get("offers", [])
            if not offers:
                break
            for offer in offers:
                yield offer
            if len(offers) < OFFER_PAGE_SIZE:
                break
            next_offset = offset + len(offers)
            if next_offset > MAX_OFFER_OFFSET:
                if len(offers) == OFFER_PAGE_SIZE:
                    self.logger.warning(
                        "listOfferMetrics offset limit (%s) reached for %s on %s. "
                        "Remaining offers were not fetched.",
                        MAX_OFFER_OFFSET,
                        marketplace_code,
                        interval_start,
                    )
                break
            offset = next_offset


class ReplenishmentDailyMetricsStream(ReplenishmentStreamBase):
    """Daily Subscribe & Save performance metrics for the selling partner."""

    name = "replenishment_daily_metrics"
    primary_keys = ["marketplace_id", "report_end_date"]
    replication_key = "report_end_date"

    schema = th.PropertiesList(
        th.Property("marketplace_id", th.StringType),
        th.Property("report_end_date", th.DateTimeType),
        *_DAILY_METRIC_SCHEMA,
    ).to_dict()

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        marketplace_code = context.get("marketplace_id")
        start_date = self.clamp_start_to_lookback(self.resolve_sync_start(context))
        max_end = self.resolve_max_sync_end()
        start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)

        while start_date <= max_end:
            chunk_end = min(start_date + timedelta(days=MAX_DAY_CHUNK - 1), max_end)
            time_interval = {
                "startDate": self.format_day(start_date),
                "endDate": self.format_day(chunk_end),
            }
            self.logger.info(
                "Fetching replenishment daily metrics for %s: %s to %s",
                marketplace_code,
                time_interval["startDate"],
                time_interval["endDate"],
            )
            payload = self.get_selling_partner_metrics(
                marketplace_code,
                time_interval,
                DAILY_PERFORMANCE_METRICS,
            )
            for bundle in payload.get("metrics", []):
                record = dict(bundle)
                record["marketplace_id"] = marketplace_code
                report_end_date = self.parse_report_end_date(record.get("timeInterval"))
                if not report_end_date:
                    continue
                record["report_end_date"] = report_end_date
                yield record
            start_date = chunk_end + timedelta(days=1)


class ReplenishmentRollingMetricsStream(ReplenishmentStreamBase):
    """Rolling-window Subscribe & Save metrics (full sync, no replication state)."""

    name = "replenishment_rolling_metrics"
    primary_keys = ["marketplace_id", "interval_start_date", "interval_end_date"]
    replication_key = None

    schema = th.PropertiesList(
        th.Property("marketplace_id", th.StringType),
        th.Property("interval_start_date", th.StringType),
        th.Property("interval_end_date", th.StringType),
        *_ROLLING_METRIC_SCHEMA,
    ).to_dict()

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        marketplace_code = context.get("marketplace_id")
        anchor_day = self.resolve_max_sync_end()
        time_interval = self.day_interval(anchor_day)
        self.logger.info(
            "Fetching replenishment rolling metrics for %s (anchor %s)",
            marketplace_code,
            time_interval["startDate"],
        )
        payload = self.get_selling_partner_metrics(
            marketplace_code,
            time_interval,
            ROLLING_PERFORMANCE_METRICS,
        )
        for bundle in payload.get("metrics", []):
            start, end = self.parse_interval_bounds(bundle.get("timeInterval"))
            if not start or not end or start == end:
                continue
            record = dict(bundle)
            record["marketplace_id"] = marketplace_code
            record["interval_start_date"] = start
            record["interval_end_date"] = end
            yield record


class ReplenishmentOfferMetricsStream(OfferMetricsStreamBase):
    """Per-ASIN daily Subscribe & Save offer metrics (PERFORMANCE)."""

    name = "replenishment_offer_metrics"
    primary_keys = [
        "marketplace_id",
        "asin",
        "sku",
        "fulfillmentChannelType",
        "report_end_date",
    ]
    replication_key = "report_end_date"

    schema = th.PropertiesList(
        th.Property("marketplace_id", th.StringType),
        th.Property("report_end_date", th.DateTimeType),
        *_OFFER_PERFORMANCE_SCHEMA,
    ).to_dict()

    def build_performance_filters(
        self, marketplace_code: str, day: datetime
    ) -> Dict[str, object]:
        """Build listOfferMetrics filters for one PERFORMANCE day."""
        return {
            "timePeriodType": PERFORMANCE,
            "programTypes": PROGRAM_TYPES,
            "marketplaceId": self.get_marketplace_api_id(marketplace_code),
            "timeInterval": self.day_interval(day),
            "aggregationFrequency": AGGREGATION_DAY,
        }

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        marketplace_code = context.get("marketplace_id")
        start_date = self.clamp_start_to_lookback(self.resolve_sync_start(context))
        max_end = self.resolve_max_sync_end()
        start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)

        while start_date <= max_end:
            self.logger.info(
                "Fetching replenishment offer metrics for %s: %s",
                marketplace_code,
                self.format_day(start_date),
            )
            filters = self.build_performance_filters(marketplace_code, start_date)
            for offer in self.iter_all_offers(marketplace_code, filters):
                record = dict(offer)
                record["marketplace_id"] = marketplace_code
                report_end_date = self.parse_report_end_date(record.get("timeInterval"))
                if not report_end_date:
                    continue
                record["report_end_date"] = report_end_date
                yield record
            start_date += timedelta(days=1)


class ReplenishmentOfferForecastStream(OfferMetricsStreamBase):
    """Per-ASIN forward-looking Subscribe & Save offer projections (FORECAST)."""

    name = "replenishment_offer_forecast"
    primary_keys = ["marketplace_id", "asin", "sku", "fulfillmentChannelType"]
    replication_key = None

    schema = th.PropertiesList(
        th.Property("marketplace_id", th.StringType),
        *_OFFER_FORECAST_SCHEMA,
    ).to_dict()

    def build_forecast_filters(self, marketplace_code: str) -> Dict[str, object]:
        """Build listOfferMetrics filters for a FORECAST snapshot request."""
        return {
            "timePeriodType": FORECAST,
            "programTypes": PROGRAM_TYPES,
            "marketplaceId": self.get_marketplace_api_id(marketplace_code),
            "timeInterval": self.forecast_request_interval(),
        }

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        marketplace_code = context.get("marketplace_id")
        filters = self.build_forecast_filters(marketplace_code)
        synced_at = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        self.logger.info(
            "Fetching replenishment offer forecast for %s (synced_at %s)",
            marketplace_code,
            synced_at,
        )
        for offer in self.iter_all_offers(marketplace_code, filters):
            record = dict(offer)
            record["marketplace_id"] = marketplace_code
            start, end = self.parse_interval_bounds(record.get("timeInterval"))
            record["interval_start_date"] = start
            record["interval_end_date"] = end
            record["synced_at"] = synced_at
            yield record
