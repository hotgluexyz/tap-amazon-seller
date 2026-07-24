"""Stream type classes for tap-amazon-seller."""
from datetime import datetime, timedelta
from typing import Iterable, Optional

import backoff
from singer_sdk import typing as th
from sp_api.util import load_all_pages

from tap_amazon_seller.client import AmazonSellerStream
from tap_amazon_seller.utils import InvalidResponse, RetriableError, timeout
from sp_api.base.exceptions import SellingApiServerException, SellingApiRequestThrottledException, SellingApiForbiddenException, SellingApiBadRequestException
from sp_api.auth.exceptions import AuthorizationError
from dateutil.relativedelta import relativedelta
from sp_api.base import Marketplaces
from dateutil.parser import parse
import time


class MarketplacesStream(AmazonSellerStream):
    """Define custom stream."""

    name = "marketplaces"
    primary_keys = ["id"]
    replication_key = None
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("name", th.StringType),
    ).to_dict()

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "marketplace_id": record["id"],
        }

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        if self.config.get("marketplaces"):
            marketplaces = self.config.get("marketplaces")
        else:
            marketplaces = [
                "US",
                "CA",
                "MX",
                "BR",
                "ES",
                "GB",
                "FR",
                "NL",
                "DE",
                "IT",
                "SE",
                "PL",
                "EG",
                "TR",
                "SA",
                "AE",
                "IN",
                "SG",
                "AU",
                "JP",
            ]
        # orders = self.get_sp_orders()
        sandbox = self.config.get("sandbox", False)
        for mp in marketplaces:    
            marketplace = self.validate_marketplace(mp)
            if marketplace is None:
                self.logger.info(f"Giving up on marketplace {mp}. Moving on to next marketplace")
                continue
            if sandbox is True and marketplace:
                # Since all sandbox orders are same and we found a valid marketplace. Break the loop.
                yield {"id": mp}
                break
            yield marketplace

    @backoff.on_exception(
        backoff.constant,
        RetriableError,
        max_tries=10,
        interval=30, # we will always backoff 30 sec
        jitter=None
    )
    @timeout(15)
    def validate_marketplace(self, mp):
        try:
            # Fetch minimum number of orders and verify credentials are working
            today_date = datetime.today().strftime("%Y-%m-%d")
            orders = self.get_sp_orders(mp)
            sandbox = self.config.get("sandbox", False)
            if sandbox is True:
                allorders = orders.get_orders(CreatedAfter="TEST_CASE_200")
                return {"id": mp}
            else:
                allorders = orders.get_orders(CreatedAfter=today_date)
                self.logger.info(f"Marketplace {mp} with id {Marketplaces[mp]} is valid for this account.")
            return {"id": mp}
        except SellingApiForbiddenException as e:
            self.logger.info(f"Marketplace {mp} not part of current SP account")
        except AuthorizationError as e:
            self.logger.error(f"Error getting records for marketplace {mp}: {e}")
            raise e
        except Exception as e:
            if self.backoff_retries >= 9:
                self.backoff_retries = 0
                return
            self.backoff_retries += 1
            raise RetriableError(e)


class OrdersStream(AmazonSellerStream):
    """Define custom stream."""

    name = "orders"
    primary_keys = ["AmazonOrderId"]
    replication_key = "LastUpdateDate"
    records_jsonpath = "$.Orders[*]"
    parent_stream_type = MarketplacesStream
    marketplace_id = "{marketplace_id}"

    schema = th.PropertiesList(
        th.Property("AmazonOrderId", th.StringType),
        th.Property("SellerOrderId", th.StringType),
        th.Property("PurchaseDate", th.DateTimeType),
        th.Property("LastUpdateDate", th.DateTimeType),
        th.Property("OrderStatus", th.StringType),
        th.Property("FulfillmentChannel", th.StringType),
        th.Property("SalesChannel", th.StringType),
        th.Property("ShipServiceLevel", th.StringType),
        th.Property("OrderChannel", th.StringType),
        th.Property(
            "OrderTotal",
            th.ObjectType(
                th.Property("CurrencyCode", th.StringType),
                th.Property("Amount", th.StringType),
            ),
        ),
        th.Property("NumberOfItemsShipped", th.NumberType),
        th.Property("NumberOfItemsUnshipped", th.NumberType),
        th.Property("PaymentMethod", th.StringType),
        th.Property(
            "PaymentMethodDetails", th.CustomType({"type": ["array", "string"]})
        ),
        th.Property(
            "PaymentExecutionDetail", th.CustomType({"type": ["array", "string"]})
        ),
        th.Property(
            "BuyerTaxInformation", th.CustomType({"type": ["object", "string"]})
        ),
        th.Property(
            "MarketplaceTaxInfo", th.CustomType({"type": ["object", "string"]})
        ),
        th.Property("ShippingAddress", th.CustomType({"type": ["object", "string"]})),
        th.Property("BuyerInfo", th.CustomType({"type": ["object", "string"]})),
        th.Property("IsReplacementOrder", th.BooleanType),
        th.Property("ReplacedOrderId", th.StringType),
        th.Property("MarketplaceId", th.StringType),
        th.Property("SellerDisplayName", th.StringType),
        th.Property("EasyShipShipmentStatus", th.StringType),
        th.Property("CbaDisplayableShippingLabel", th.StringType),
        th.Property("ShipmentServiceLevelCategory", th.StringType),
        th.Property("BuyerInvoicePreference", th.StringType),
        th.Property("OrderType", th.StringType),
        th.Property("EarliestShipDate", th.DateTimeType),
        th.Property("LatestShipDate", th.DateTimeType),
        th.Property("EarliestDeliveryDate", th.DateTimeType),
        th.Property("PromiseResponseDueDate", th.DateTimeType),
        th.Property("LatestDeliveryDate", th.DateTimeType),
        th.Property("IsBusinessOrder", th.BooleanType),
        th.Property("IsEstimatedShipDateSet", th.BooleanType),
        th.Property("IsPrime", th.BooleanType),
        th.Property("IsGlobalExpressEnabled", th.BooleanType),
        th.Property("HasRegulatedItems", th.BooleanType),
        th.Property("IsPremiumOrder", th.BooleanType),
        th.Property("IsSoldByAB", th.BooleanType),
        th.Property("IsIBA", th.BooleanType),
        th.Property(
            "DefaultShipFromLocationAddress",
            th.ObjectType(
                th.Property("Name", th.StringType),
                th.Property("AddressLine1", th.StringType),
                th.Property("City", th.StringType),
                th.Property("StateOrRegion", th.StringType),
                th.Property("PostalCode", th.StringType),
                th.Property("CountryCode", th.StringType),
                th.Property("Phone", th.StringType),
                th.Property("AddressType", th.StringType),
            ),
        ),
        th.Property(
            "FulfillmentInstruction",
            th.ObjectType(
                th.Property("FulfillmentSupplySourceId", th.StringType),
                th.Property("IsISPU", th.BooleanType),
            ),
        ),
        th.Property(
            "AutomatedShippingSettings",
            th.ObjectType(th.Property("HasAutomatedShippingSettings", th.BooleanType)),
        ),
        th.Property("MarketplaceName", th.StringType)
    ).to_dict()


    @load_all_pages()
    @backoff.on_exception(
        backoff.constant,
        (Exception, RetriableError),
        interval=60,
        jitter=None
    )
    def load_all_orders(self, mp, **kwargs):
        """
        a generator function to return all pages, obtained by NextToken
        """
        try:
            orders = self.get_sp_orders(mp)
            orders_obj = orders.get_orders(**kwargs)
            # reset the counter once it works
            self.backoff_retries = 0
            return orders_obj
        except SellingApiRequestThrottledException as e:
            if self.backoff_retries >= 14:
                self.logger.warning(
                    f"Giving up on stream {self.name} after {self.backoff_retries} attempts. Ending gracefully."
                )
                self.backoff_retries = 0
                return type(
                    "Page", (), {"payload": {"Orders": []}, "next_token": None}
                )()
            else:
                self.backoff_retries += 1
                self.logger.info("Caught SellingApiRequestThrottledException, retrying...")
                raise RetriableError("Request was throttled, retrying...") from e
        
        except SellingApiBadRequestException as e:
            if self.backoff_retries >= 3:
                self.logger.warning(
                    f"Giving up on stream {self.name} after {self.backoff_retries} attempts. Ending gracefully."
                )
                self.backoff_retries = 0
                return type(
                    "Page", (), {"payload": {"Orders": []}, "next_token": None}
                )()
            else:
                self.backoff_retries += 1
                self.logger.info(f"Kwargs in latest request: {kwargs}")
                raise e

        except Exception as e:
            self.logger.error("Caught an exception in load_all_orders: %s", e)
            raise InvalidResponse(e)

    def load_order_page(self, mp, **kwargs):
        """
        a generator function to return all pages, obtained by NextToken
        """

        for page in self.load_all_orders(mp, **kwargs):
            orders = []
            for order in page.payload.get("Orders"):
                orders.append(order)

            yield orders

    @backoff.on_exception(
        backoff.expo,
        (Exception),
        max_tries=15,
        factor=3,
    )
    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        try:
            # Get start_date
            start_date = self.get_starting_timestamp(context) or datetime(2000, 1, 1)
            start_date = start_date.strftime("%Y-%m-%dT%H:%M:%S")

            sandbox = self.config.get("sandbox", False)
            if sandbox is True:
                rows = self.load_order_page(
                    mp=context.get("marketplace_id"), CreatedAfter="TEST_CASE_200"
                )
            else:
                rows = self.load_order_page(
                    mp=context.get("marketplace_id"), LastUpdatedAfter=start_date
                )
            for row in rows:
                for item in row:
                    yield item
        except Exception as e:
            raise InvalidResponse(e)

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        mp = context.get("marketplace_id")
        return {"AmazonOrderId": record["AmazonOrderId"], "marketplace_id": mp, "LastUpdateDate": record["LastUpdateDate"]}


class OrderItemsStream(AmazonSellerStream):
    """Define custom stream."""

    name = "orderitems"
    primary_keys = ["OrderItemId"]
    replication_key = "LastUpdateDate"
    order_id = "{AmazonOrderId}"
    parent_stream_type = OrdersStream
    schema_writed = False
    new_state = {}

    schema = th.PropertiesList(
        th.Property("AmazonOrderId", th.StringType),
        th.Property(
            "OrderItems",
            th.ArrayType(
                th.ObjectType(
                    th.Property("ASIN", th.StringType),
                    th.Property("OrderItemId", th.StringType),
                    th.Property("SellerSKU", th.StringType),
                    th.Property("Title", th.StringType),
                    th.Property("QuantityOrdered", th.NumberType),
                    th.Property("QuantityShipped", th.NumberType),
                    th.Property(
                        "ProductInfo", th.CustomType({"type": ["object", "string"]})
                    ),
                    th.Property(
                        "PointsGranted", th.CustomType({"type": ["object", "string"]})
                    ),
                    th.Property(
                        "ItemPrice", th.CustomType({"type": ["object", "string"]})
                    ),
                    th.Property(
                        "ShippingPrice", th.CustomType({"type": ["object", "string"]})
                    ),
                    th.Property(
                        "ShippingDiscount",
                        th.CustomType({"type": ["object", "string"]}),
                    ),
                    th.Property(
                        "ShippingDiscountTax",
                        th.CustomType({"type": ["object", "string"]}),
                    ),
                    th.Property(
                        "PromotionDiscount",
                        th.CustomType({"type": ["object", "string"]}),
                    ),
                    th.Property(
                        "PromotionDiscountTax",
                        th.CustomType({"type": ["object", "string"]}),
                    ),
                    th.Property(
                        "ItemTax", th.CustomType({"type": ["object", "string"]})
                    ),
                    th.Property(
                        "ShippingTax", th.CustomType({"type": ["object", "string"]})
                    ),
                    th.Property(
                        "PromotionIds", th.CustomType({"type": ["array", "string"]})
                    ),
                    th.Property(
                        "CODFee", th.CustomType({"type": ["object", "string"]})
                    ),
                    th.Property(
                        "CODFeeDiscount", th.CustomType({"type": ["object", "string"]})
                    ),
                    th.Property(
                        "TaxCollection", th.CustomType({"type": ["object", "string"]})
                    ),
                    th.Property(
                        "BuyerInfo", th.CustomType({"type": ["object", "string"]})
                    ),
                    th.Property(
                        "BuyerRequestedCancel",
                        th.CustomType({"type": ["object", "string"]}),
                    ),
                    th.Property("IsGift", th.StringType),
                    th.Property("ConditionId", th.StringType),
                    th.Property("ConditionNote", th.StringType),
                    th.Property("ConditionSubtypeId", th.StringType),
                    th.Property("ScheduledDeliveryStartDate", th.StringType),
                    th.Property("ScheduledDeliveryEndDate", th.StringType),
                    th.Property("PriceDesignation", th.StringType),
                    th.Property("IsTransparency", th.BooleanType),
                    th.Property("SerialNumberRequired", th.BooleanType),
                    th.Property("IossNumber", th.StringType),
                    th.Property("DeemedResellerCategory", th.StringType),
                    th.Property("StoreChainStoreId", th.StringType),
                    th.Property(
                        "BuyerRequestedCancel",
                        th.CustomType({"type": ["object", "string"]}),
                    ),
                )
            ),
        ),
        th.Property("LastUpdateDate", th.DateTimeType),
        th.Property("MarketplaceName", th.StringType)
    ).to_dict()

    @backoff.on_exception(
        backoff.expo,
        (Exception),
        max_tries=15,
        factor=3,
    )
    @timeout(15)
    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        try:
            order_id = context.get("AmazonOrderId", [])

            orders = self.get_sp_orders(context.get("marketplace_id"))
            # keep state for each marketplace partition
            self.build_child_stream_state(context)

            sandbox = self.config.get("sandbox", False)
            if sandbox is False:
                items = orders.get_order_items(order_id=order_id).payload
            else:
                items = orders.get_order_items("'TEST_CASE_200'").payload
            return [items]
        except Exception as e:
            raise InvalidResponse(e)


class OrderBuyerInfo(AmazonSellerStream):
    """Define custom stream."""

    name = "orderbuyerinfo"
    primary_keys = ["AmazonOrderId"]
    replication_key = "LastUpdateDate"
    order_id = "{AmazonOrderId}"
    parent_stream_type = OrdersStream
    # Optionally, you may also use `schema_filepath` in place of `schema`:
    # schema_filepath = SCHEMAS_DIR / "users.json"
    schema = th.PropertiesList(
        th.Property("AmazonOrderId", th.StringType),
        th.Property("BuyerEmail", th.StringType),
        th.Property("BuyerName", th.StringType),
        th.Property("BuyerCounty", th.StringType),
        th.Property("BuyerTaxInfo", th.CustomType({"type": ["object", "string"]})),
        th.Property("PurchaseOrderNumber", th.StringType),
        th.Property("LastUpdateDate", th.DateTimeType),
    ).to_dict()

    @backoff.on_exception(
        backoff.expo,
        (Exception),
        max_tries=15,
        factor=3,
    )
    @timeout(15)
    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        try:
            order_id = context.get("AmazonOrderId", [])

            orders = self.get_sp_orders(context.get("marketplace_id"))
            items = orders.get_order_buyer_info(order_id=order_id).payload
            return [items]
        except Exception as e:
            raise InvalidResponse(e)


class OrderAddress(AmazonSellerStream):
    """Define custom stream."""

    name = "orderaddress"
    primary_keys = ["AmazonOrderId"]
    replication_key = "LastUpdateDate"
    order_id = "{AmazonOrderId}"
    parent_stream_type = OrdersStream
    # Optionally, you may also use `schema_filepath` in place of `schema`:
    # schema_filepath = SCHEMAS_DIR / "users.json"
    schema = th.PropertiesList(
        th.Property("AmazonOrderId", th.StringType),
        th.Property(
            "ShippingAddress",
            th.ObjectType(
                th.Property("Name", th.StringType),
                th.Property("AddressLine1", th.StringType),
                th.Property("AddressLine2", th.StringType),
                th.Property("AddressLine3", th.StringType),
                th.Property("City", th.StringType),
                th.Property("County", th.StringType),
                th.Property("District", th.StringType),
                th.Property("StateOrRegion", th.StringType),
                th.Property("Municipality", th.StringType),
                th.Property("PostalCode", th.StringType),
                th.Property("CountryCode", th.StringType),
                th.Property("Phone", th.StringType),
                th.Property("AddressType", th.StringType),
            ),
        ),
        th.Property("LastUpdateDate", th.DateTimeType)
    ).to_dict()

    @backoff.on_exception(
        backoff.expo,
        (Exception),
        max_tries=15,
        factor=3,
    )
    @timeout(15)
    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        try:
            order_id = context.get("AmazonOrderId", [])

            orders = self.get_sp_orders(context.get("marketplace_id"))
            items = orders.get_order_address(order_id=order_id).payload
            return [items]
        except Exception as e:
            raise InvalidResponse(e)


class OrderFinancialEvents(AmazonSellerStream):
    """Define custom stream."""

    name = "orderfinancialevents"
    primary_keys = ["AmazonOrderId"]
    replication_key = "LastUpdateDate"
    order_id = "{AmazonOrderId}"
    parent_stream_type = OrdersStream
    new_state = {}

    # Optionally, you may also use `schema_filepath` in place of `schema`:
    # schema_filepath = SCHEMAS_DIR / "users.json"
    schema = th.PropertiesList(
        th.Property("AmazonOrderId", th.StringType),
        th.Property("ShipmentEventList", th.CustomType({"type": ["array", "string"]})),
        th.Property("RefundEventList", th.CustomType({"type": ["array", "string"]})),
        th.Property(
            "GuaranteeClaimEventList", th.CustomType({"type": ["array", "string"]})
        ),
        th.Property(
            "ChargebackEventList", th.CustomType({"type": ["array", "string"]})
        ),
        th.Property(
            "PayWithAmazonEventList", th.CustomType({"type": ["array", "string"]})
        ),
        th.Property(
            "ServiceProviderCreditEventList",
            th.CustomType({"type": ["array", "string"]}),
        ),
        th.Property(
            "RetrochargeEventList", th.CustomType({"type": ["array", "string"]})
        ),
        th.Property(
            "RentalTransactionEventList", th.CustomType({"type": ["array", "string"]})
        ),
        th.Property(
            "ProductAdsPaymentEventList", th.CustomType({"type": ["array", "string"]})
        ),
        th.Property(
            "ServiceFeeEventList", th.CustomType({"type": ["array", "string"]})
        ),
        th.Property(
            "SellerDealPaymentEventList", th.CustomType({"type": ["array", "string"]})
        ),
        th.Property(
            "DebtRecoveryEventList", th.CustomType({"type": ["array", "string"]})
        ),
        th.Property(
            "LoanServicingEventList", th.CustomType({"type": ["array", "string"]})
        ),
        th.Property(
            "AdjustmentEventList", th.CustomType({"type": ["array", "string"]})
        ),
        th.Property(
            "SAFETReimbursementEventList", th.CustomType({"type": ["array", "string"]})
        ),
        th.Property(
            "SellerReviewEnrollmentPaymentEventList",
            th.CustomType({"type": ["array", "string"]}),
        ),
        th.Property(
            "FBALiquidationEventList", th.CustomType({"type": ["array", "string"]})
        ),
        th.Property(
            "CouponPaymentEventList", th.CustomType({"type": ["array", "string"]})
        ),
        th.Property(
            "ImagingServicesFeeEventList", th.CustomType({"type": ["array", "string"]})
        ),
        th.Property(
            "NetworkComminglingTransactionEventList",
            th.CustomType({"type": ["array", "string"]}),
        ),
        th.Property(
            "AffordabilityExpenseEventList",
            th.CustomType({"type": ["array", "string"]}),
        ),
        th.Property(
            "AffordabilityExpenseReversalEventList",
            th.CustomType({"type": ["array", "string"]}),
        ),
        th.Property(
            "TrialShipmentEventList", th.CustomType({"type": ["array", "string"]})
        ),
        th.Property(
            "ShipmentSettleEventList", th.CustomType({"type": ["array", "string"]})
        ),
        th.Property(
            "TaxWithholdingEventList", th.CustomType({"type": ["array", "string"]})
        ),
        th.Property(
            "RemovalShipmentEventList", th.CustomType({"type": ["array", "string"]})
        ),
        th.Property(
            "RemovalShipmentAdjustmentEventList",
            th.CustomType({"type": ["array", "string"]}),
        ),
        th.Property("LastUpdateDate", th.DateTimeType),
        th.Property("MarketplaceName", th.StringType)
    ).to_dict()

    # Finances: 0.5 rps / burst 30. factor=2 → 2s, 4s, 8s...; no jitter so we never retry under the refill interval.
    @backoff.on_exception(
        backoff.expo,
        (Exception),
        max_tries=15,
        factor=2,
        jitter=None,
    )
    @timeout(15)
    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        try:
            order_id = context.get("AmazonOrderId", [])

            finance = self.get_sp_finance(context.get("marketplace_id"))

            sandbox = self.config.get("sandbox", False)
            if sandbox is False:
                # keep state for each marketplace partition
                self.build_child_stream_state(context)

                items = finance.get_financial_events_for_order(order_id).payload
                items["AmazonOrderId"] = order_id
            else:
                items = finance.get_financial_events_for_order("TEST_CASE_200").payload
            return [items["FinancialEvents"]]
        except Exception as e:
            raise InvalidResponse(e)


class ListOrderFinancialEventsStream(AmazonSellerStream):
    """Bulk financial events via listFinancialEvents (date windows).

    Emits one record per AmazonOrderId per API page (group within the page only).
    RequestPK is Amazon's NextToken when present, else a synthetic per-request id.
    """

    name = "list_order_financial_events"
    primary_keys = ["AmazonOrderId", "RequestPK"]
    replication_key = "LastUpdateDate"
    parent_stream_type = MarketplacesStream
    marketplace_id = "{marketplace_id}"
    # Amazon: window >180 days returns empty; retention >730 days returns InvalidInput.
    WINDOW_DAYS = 30
    # "errors": [
    #     {
    #         "code": "InvalidInput",
    #         "message": "Start date: Tue Jun 25 05:00:00 UTC 2024 is not valid, given the retention period: 730",
    #         "details": ""
    #     }
    # ]
    RETENTION_DAYS = 730
    # Copied from orderfinancialevents; EventLists are array-only (hotglue singer validate
    # rejects multiple non-null types like ["array","string"]).
    schema = th.PropertiesList(
        th.Property("AmazonOrderId", th.StringType),
        th.Property("ShipmentEventList", th.ArrayType(th.ObjectType())),
        th.Property("RefundEventList", th.ArrayType(th.ObjectType())),
        th.Property("GuaranteeClaimEventList", th.ArrayType(th.ObjectType())),
        th.Property("ChargebackEventList", th.ArrayType(th.ObjectType())),
        th.Property("PayWithAmazonEventList", th.ArrayType(th.ObjectType())),
        th.Property("ServiceProviderCreditEventList", th.ArrayType(th.ObjectType())),
        th.Property("RetrochargeEventList", th.ArrayType(th.ObjectType())),
        th.Property("RentalTransactionEventList", th.ArrayType(th.ObjectType())),
        th.Property("ProductAdsPaymentEventList", th.ArrayType(th.ObjectType())),
        th.Property("ServiceFeeEventList", th.ArrayType(th.ObjectType())),
        th.Property("SellerDealPaymentEventList", th.ArrayType(th.ObjectType())),
        th.Property("DebtRecoveryEventList", th.ArrayType(th.ObjectType())),
        th.Property("LoanServicingEventList", th.ArrayType(th.ObjectType())),
        th.Property("AdjustmentEventList", th.ArrayType(th.ObjectType())),
        th.Property("SAFETReimbursementEventList", th.ArrayType(th.ObjectType())),
        th.Property("SellerReviewEnrollmentPaymentEventList", th.ArrayType(th.ObjectType())),
        th.Property("FBALiquidationEventList", th.ArrayType(th.ObjectType())),
        th.Property("CouponPaymentEventList", th.ArrayType(th.ObjectType())),
        th.Property("ImagingServicesFeeEventList", th.ArrayType(th.ObjectType())),
        th.Property("NetworkComminglingTransactionEventList", th.ArrayType(th.ObjectType())),
        th.Property("AffordabilityExpenseEventList", th.ArrayType(th.ObjectType())),
        th.Property("AffordabilityExpenseReversalEventList", th.ArrayType(th.ObjectType())),
        th.Property("TrialShipmentEventList", th.ArrayType(th.ObjectType())),
        th.Property("ShipmentSettleEventList", th.ArrayType(th.ObjectType())),
        th.Property("TaxWithholdingEventList", th.ArrayType(th.ObjectType())),
        th.Property("RemovalShipmentEventList", th.ArrayType(th.ObjectType())),
        th.Property("RemovalShipmentAdjustmentEventList", th.ArrayType(th.ObjectType())),
        th.Property("LastUpdateDate", th.DateTimeType),
        th.Property("MarketplaceName", th.StringType),
        th.Property("marketplace_id", th.StringType),
        th.Property("RequestPK", th.StringType),
    ).to_dict()

    @staticmethod
    def filter_order_financial_event_fields(financial_events: dict) -> dict:
        """Keep only fields declared on this stream (ignore extra Amazon lists)."""
        allowed = ListOrderFinancialEventsStream.schema["properties"]
        return {k: v for k, v in financial_events.items() if k in allowed}

    @staticmethod
    def group_financial_events_by_order(financial_events: dict) -> list:
        """Group a FinancialEvents page into one record per AmazonOrderId.

        Events without AmazonOrderId are skipped (account-level / non-order).
        """
        by_order = {}
        for list_name, events in financial_events.items():
            for event in events:
                order_id = event.get("AmazonOrderId")
                if not order_id:
                    continue
                if order_id not in by_order:
                    by_order[order_id] = {"AmazonOrderId": order_id}
                by_order[order_id].setdefault(list_name, []).append(event)
                posted = event.get("PostedDate")
                current = by_order[order_id].get("LastUpdateDate")
                if posted and (not current or posted > current):
                    by_order[order_id]["LastUpdateDate"] = posted
        return list(by_order.values())

    @staticmethod
    def max_posted_date(financial_events: dict) -> Optional[str]:
        """Return the latest PostedDate across all event lists, if any."""
        latest = None
        for events in financial_events.values():
            for event in events:
                posted = event.get("PostedDate")
                if posted and (latest is None or posted > latest):
                    latest = posted
        return latest

    @staticmethod
    def clamp_to_retention(
        start: datetime, end: datetime, retention_days: int = RETENTION_DAYS
    ) -> datetime:
        """Raise PostedAfter to the earliest date Amazon still retains."""
        earliest = end - timedelta(days=retention_days)
        return start if start >= earliest else earliest

    @staticmethod
    def request_pk(
        page_token: str, posted_after: str, posted_before: str
    ) -> str:
        """Amazon NextToken when present; else synthetic id for the first page."""
        if page_token:
            return page_token
        return (
            f"{posted_after}|{posted_before}|"
            f"{datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')}"
        )

    @staticmethod
    def iter_posted_windows(start: datetime, end: datetime, window_days: int = WINDOW_DAYS):
        """Yield (PostedAfter, PostedBefore) windows of at most window_days.

        PostedBefore is exclusive; next window starts at the previous PostedBefore.
        """
        while start < end:
            window_end = min(start + timedelta(days=window_days), end)
            yield start, window_end
            start = window_end

    @backoff.on_exception(
        backoff.expo,
        (Exception,),
        max_tries=15,
        factor=2,
        jitter=None,
    )
    def fetch_financial_events(self, mp, **kwargs):
        try:
            return self.get_sp_finance(mp).list_financial_events(**kwargs)
        except Exception as e:
            raise InvalidResponse(e)

    def _advance_bookmark(self, context: Optional[dict], when: datetime) -> None:
        state = self.get_context_state(context)
        state["replication_key"] = self.replication_key
        state["replication_key_value"] = when.strftime("%Y-%m-%dT%H:%M:%SZ")

    def _increment_stream_state(self, latest_record, *, context=None):
        """Skip per-record bookmark updates.

        Cursor is the PostedAfter/PostedBefore window, advanced only in
        ``_advance_bookmark`` after each window finishes. Per-record
        ``LastUpdateDate`` is unsafe here: ServiceFeeEventList (and similar)
        often have AmazonOrderId but no PostedDate, so SDK increment would
        KeyError; even dated rows would race the window cursor mid-page.

        STATE messages still emit; resume is at the last completed window.
        """
        return

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        mp = context.get("marketplace_id")
        # PostedBefore must be >2 minutes before request time.
        end = datetime.utcnow() - timedelta(minutes=3)
        start = self.get_starting_timestamp(context) or datetime(2000, 1, 1)
        if getattr(start, "tzinfo", None) is not None:
            start = start.replace(tzinfo=None)
        clamped = self.clamp_to_retention(start, end, self.RETENTION_DAYS)
        if clamped != start:
            self.logger.warning(
                "PostedAfter %s is outside Amazon finances retention (%s days); "
                "clamping to %s",
                start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                self.RETENTION_DAYS,
                clamped.strftime("%Y-%m-%dT%H:%M:%SZ"),
            )
            start = clamped

        for window_start, window_end in self.iter_posted_windows(
            start, end, self.WINDOW_DAYS
        ):
            posted_after = window_start.strftime("%Y-%m-%dT%H:%M:%SZ")
            posted_before = window_end.strftime("%Y-%m-%dT%H:%M:%SZ")
            page_token = ""
            page_num = 0
            while True:
                page_num += 1
                kwargs = {
                    "PostedAfter": posted_after,
                    "PostedBefore": posted_before,
                    "MaxResultsPerPage": 100,
                }
                if page_token:
                    kwargs["NextToken"] = page_token
                self.logger.info(
                    "list_order_financial_events request: "
                    "PostedAfter=%s PostedBefore=%s page=%s has_next_token=%s",
                    posted_after,
                    posted_before,
                    page_num,
                    bool(page_token),
                )
                page = self.fetch_financial_events(mp, **kwargs)
                financial_events = self.filter_order_financial_event_fields(
                    page.payload["FinancialEvents"]
                )
                req_pk = self.request_pk(page_token, posted_after, posted_before)
                for record in self.group_financial_events_by_order(financial_events):
                    record["RequestPK"] = req_pk
                    yield record

                if not page.next_token:
                    break
                page_token = page.next_token
                # Stay near Finances ~0.5 rps; also limits NextToken TTL risk.
                time.sleep(2)

            # PostedBefore is exclusive; bookmark window_end to avoid gaps/dupes.
            self._advance_bookmark(context, window_end)


class ReportsStream(AmazonSellerStream):
    """Define custom stream."""

    name = "reports"
    primary_keys = ["reportId"]
    replication_key = None
    report_id = None
    document_id = None
    schema = th.PropertiesList(
        th.Property("marketplaceIds", th.CustomType({"type": ["array", "string"]})),
        th.Property("reportId", th.StringType),
        th.Property("Date", th.DateTimeType),
        th.Property("FNSKU", th.StringType),
        th.Property("ASIN", th.StringType),
        th.Property("MSKU", th.StringType),
        th.Property("Title", th.StringType),
        th.Property("Event Type", th.StringType),
        th.Property("Reference ID", th.StringType),
        th.Property("Quantity", th.StringType),
        th.Property("Fulfillment Center", th.StringType),
        th.Property("Disposition", th.StringType),
        th.Property("Reason", th.StringType),
        th.Property("Country", th.StringType),
        # th.Property("reportType", th.StringType),
        # th.Property("dataStartTime", th.DateTimeType),
        # th.Property("dataEndTime", th.DateTimeType),
        # th.Property("dataEndreportScheduleIdime", th.StringType),
        # th.Property("createdTime", th.DateTimeType),
        # th.Property("processingStatus", th.StringType),
        # th.Property("processingStartTime", th.DateTimeType),
        # th.Property("processingEndTime", th.DateTimeType),
        # th.Property("reportDocumentId", th.StringType),
    ).to_dict()

    @backoff.on_exception(
        backoff.expo,
        (Exception),
        max_tries=15,
        factor=3,
    )
    @timeout(15)
    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        try:
            start_date = self.get_starting_timestamp(context) or datetime(2005, 1, 1)
            end_date = None
            if self.config.get("start_date"):
                start_date = parse(self.config.get("start_date"))
            if self.config.get("end_date"):
                end_date = parse(self.config.get("end_date"))
            start_date = start_date.strftime("%Y-%m-%dT00:00:00")
            report_types = self.config.get("report_types")
            processing_status = self.config.get("processing_status")
            marketplace_id = None
            if context is not None:
                marketplace_id = context.get("marketplace_id")

            report = self.get_sp_reports()
            if start_date and end_date is not None:
                end_date = end_date.strftime("%Y-%m-%dT23:59:59")
                items = report.get_reports(
                    reportTypes=report_types,
                    processingStatuses=processing_status,
                    dataStartTime=start_date,
                    dataEndTime=end_date,
                ).payload
            else:
                items = report.get_reports(
                    reportTypes=report_types,
                    processingStatuses=processing_status,
                    dataStartTime=start_date,
                ).payload

            if not items["reports"]:
                reports = self.create_report(start_date, report, end_date)
                for row in reports:
                    yield row

            # If reports are form loop through, download documents and populate the data.txt
            for row in items["reports"]:
                reports = self.check_report(row["reportId"], report)
                for report_row in reports:
                    yield report_row

        except Exception as e:
            raise InvalidResponse(e)


class WarehouseInventory(AmazonSellerStream):
    """Define custom stream."""

    next_token = None
    name = "warehouse_inventory"
    primary_keys = ["asin", "fnSku", "sellerSku"]
    replication_key = None
    parent_stream_type = MarketplacesStream
    marketplace_id = "{marketplace_id}"
    schema = th.PropertiesList(
        th.Property("marketplace_id", th.StringType),
        th.Property("granularityType", th.StringType),
        th.Property("granularityId", th.StringType),
        th.Property("asin", th.StringType),
        th.Property("fnSku", th.StringType),
        th.Property("sellerSku", th.StringType),
        th.Property("condition", th.StringType),
        th.Property("lastUpdatedTime", th.DateTimeType),
        th.Property("productName", th.StringType),
        th.Property("totalQuantity", th.NumberType),
        th.Property("inventoryDetails", th.CustomType({"type": ["object", "string"]})),
    ).to_dict()

    @load_all_pages()
    @backoff.on_exception(
        backoff.expo,
        (Exception),
        max_tries=15,
        factor=3,
    )
    def load_all_items(self, mp, **kwargs):
        """
        a generator function to return all pages, obtained by NextToken
        """
        try:
            wi = self.get_warehouse_object(mp)
            kwargs.update({"details": True})
            del kwargs["startDateTime"]
            if self.next_token is not None:
                kwargs.update({"nextToken": self.next_token})

            list = wi.get_inventory_summary_marketplace(**kwargs)
            return list
        except Exception as e:
            raise InvalidResponse(e)

    def load_item_page(self, mp, **kwargs):
        """
        a generator function to return all pages, obtained by NextToken
        """

        for page in self.load_all_items(mp, **kwargs):
            self.next_token = page.next_token
            yield page.payload

    @backoff.on_exception(
        backoff.expo,
        (Exception),
        max_tries=15,
        factor=3,
    )
    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        try:
            six_months_ago = datetime.today() - relativedelta(months=18)
            start_date = self.get_starting_timestamp(context) or six_months_ago
            start_date = start_date.strftime("%Y-%m-%dT%H:%M:%S")
            rows = self.load_item_page(
                mp=context.get("marketplace_id"), startDateTime=start_date
            )

            for row in rows:
                return_row = {"marketplace_id": context.get("marketplace_id")}
                if "granularity" in row:
                    return_row.update(row["granularity"])
                    if "inventorySummaries" in row:
                        if len(row["inventorySummaries"]) > 0:
                            for summary in row["inventorySummaries"]:
                                return_row.update(summary)
                                yield return_row
                    else:
                        return_row.update({"lastUpdatedTime": ""})
                else:
                    yield return_row
        except Exception as e:
            raise InvalidResponse(e)


class ProductsIventoryStream(AmazonSellerStream):
    """Define custom stream."""

    name = "products_inventory"
    primary_keys = ["listing-id"]
    replication_key = None
    report_id = None
    document_id = None
    parent_stream_type = MarketplacesStream
    schema = th.PropertiesList(
        th.Property("marketplaceIds", th.CustomType({"type": ["array", "string"]})),
        th.Property("item-name", th.StringType),
        th.Property("marketplace_id", th.StringType),
        th.Property("item-description", th.StringType),
        th.Property("listing-id", th.StringType),
        th.Property("seller-sku", th.StringType),
        th.Property("price", th.StringType),
        th.Property("quantity", th.StringType),
        th.Property("open-date", th.StringType),
        th.Property("image-url", th.StringType),
        th.Property("item-is-marketplace", th.StringType),
        th.Property("product-id-type", th.StringType),
        th.Property("zshop-shipping-fee", th.StringType),
        th.Property("item-note", th.StringType),
        th.Property("item-condition", th.StringType),
        th.Property("zshop-category1", th.StringType),
        th.Property("zshop-browse-path", th.StringType),
        th.Property("asin1", th.StringType),
        th.Property("asin2", th.StringType),
        th.Property("asin3", th.StringType),
        th.Property("will-ship-internationally", th.StringType),
        th.Property("zshop-boldface", th.StringType),
        th.Property("product-id", th.StringType),
        th.Property("bid-for-featured-placement", th.StringType),
        th.Property("add-delete", th.StringType),
        th.Property("pending-quantity", th.StringType),
        th.Property("fulfilment-channel", th.StringType),
        th.Property("merchant-shipping-group", th.StringType),
        th.Property("status", th.StringType),
        th.Property("Minimum order quantity", th.StringType),
        th.Property("Sell remainder", th.StringType),
        th.Property("product-id", th.StringType),
        th.Property("marketplace_id", th.StringType),
    ).to_dict()

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        if "asin1" in record:
            return {
                "ASIN": record["asin1"],
                "marketplace_id": context.get("marketplace_id"),
            }
        elif "product-id" in record:
            return {
                "ASIN": record["product-id"],
                "marketplace_id": context.get("marketplace_id"),
            }
        else:
            return []

    @timeout(15)
    @backoff.on_exception(
        backoff.expo,
        (Exception),
        max_tries=15,
        factor=3,
    )
    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        try:
            start_date = self.get_starting_timestamp(context) or datetime(2005, 1, 1)
            end_date = None
            if self.config.get("start_date"):
                start_date = parse(self.config.get("start_date"))
            if self.config.get("end_date"):
                end_date = parse(self.config.get("end_date"))

            start_date = start_date.strftime("%Y-%m-%dT00:00:00")
            report_types = ["GET_MERCHANT_LISTINGS_ALL_DATA"]
            processing_status = self.config.get("processing_status")
            marketplace_id = None
            if context is not None:
                marketplace_id = context.get("marketplace_id")

            report = self.get_sp_reports(marketplace_id=marketplace_id)
            if start_date and end_date is not None:
                end_date = end_date.strftime("%Y-%m-%dT23:59:59")
                items = report.get_reports(
                    reportTypes=report_types,
                    processingStatuses=processing_status,
                    dataStartTime=start_date,
                    dataEndTime=end_date,
                ).payload
            else:
                items = report.get_reports(
                    reportTypes=report_types,
                    processingStatuses=processing_status,
                    dataStartTime=start_date,
                ).payload

            if not items["reports"]:
                reports = self.create_report(
                    start_date, report, end_date, "GET_MERCHANT_LISTINGS_ALL_DATA"
                )
                for row in reports:
                    yield row

            # If reports are form loop through, download documents and populate the data.txt
            for row in items["reports"]:
                reports = self.check_report(row["reportId"], report)
                for report_row in reports:
                    if context is not None:
                        report_row.update(
                            {marketplace_id: context.get("marketplace_id")}
                        )
                    yield report_row

        except Exception as e:
            raise InvalidResponse(e)


class ProductDetails(AmazonSellerStream):
    """Define custom stream."""

    name = "product_details"
    primary_keys = ["ASIN"]
    replication_key = None
    asin = "{ASIN}"
    parent_stream_type = ProductsIventoryStream
    # Optionally, you may also use `schema_filepath` in place of `schema`:
    # schema_filepath = SCHEMAS_DIR / "users.json"
    schema = th.PropertiesList(
        th.Property("asin", th.StringType),
        th.Property("attributes", th.CustomType({"type": ["object", "string"]})),
        th.Property("identifiers", th.CustomType({"type": ["array", "string"]})),
        th.Property("productTypes", th.CustomType({"type": ["array", "string"]})),
        th.Property("ranks", th.CustomType({"type": ["array", "string"]})),
        th.Property("salesRanks", th.CustomType({"type": ["array", "string"]})),
        th.Property("summaries", th.CustomType({"type": ["array", "string"]})),
        th.Property("marketplace_id", th.StringType),
    ).to_dict()

    @backoff.on_exception(
        backoff.expo,
        (Exception),
        max_tries=15,
        factor=3,
    )
    # @timeout(15)
    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        try:
            includedData = ["attributes,summaries,identifiers,productTypes,salesRanks"]
            # if context is not None:
            asin = context.get("ASIN")
            catalog = self.get_sp_catalog(marketplace_id=context.get("marketplace_id"))
            if context.get("marketplace_id") == "JP":
                items = catalog.get_catalog_item(JAN=asin).payload
            elif context.get("marketplace_id") in ["FR"]:
                items = catalog.get_catalog_item(EAN=asin).payload
            else:
                items = catalog.get_catalog_item(
                    asin=asin, includedData=includedData
                ).payload
            if "Items" in items:
                if len(items["Items"]) > 0:
                    items = items["Items"][0]

            items.update({"marketplace_id": context.get("marketplace_id")})
            return [items]
        except Exception as e:
            self.logger.warn(e)
            return []


class VendorFulfilmentPurchaseOrdersStream(AmazonSellerStream):
    """Define custom stream."""

    name = "vendor_fulfilment_purchase_orders"
    primary_keys = ["purchaseOrderNumber"]
    # TODO loook for relevant replication key in the live data
    replication_key = None
    parent_stream_type = MarketplacesStream
    marketplace_id = "{marketplace_id}"

    schema = th.PropertiesList(
        th.Property("purchaseOrderNumber", th.StringType),
        # Optional, not always populated
        th.Property(
            "orderDetails",
            th.ObjectType(
                th.Property("customerOrderNumber", th.StringType),
                th.Property("orderDate", th.DateTimeType),
                th.Property("orderStatus", th.StringType),
                th.Property(
                    "shipmentDetails", th.CustomType({"type": ["object", "string"]})
                ),
                th.Property("taxTotal", th.CustomType({"type": ["object", "string"]})),
                th.Property(
                    "sellingParty", th.CustomType({"type": ["object", "string"]})
                ),
                th.Property(
                    "shipToParty", th.CustomType({"type": ["object", "string"]})
                ),
                th.Property(
                    "billToParty", th.CustomType({"type": ["object", "string"]})
                ),
                th.Property("items", th.CustomType({"type": ["array", "string"]})),
            ),
        ),
    ).to_dict()

    @load_all_pages()
    @backoff.on_exception(
        backoff.expo,
        (Exception),
        max_tries=15,
        factor=3,
    )
    def load_all_orders(self, mp, **kwargs):
        """
        a generator function to return all pages, obtained by NextToken
        """
        try:
            orders = self.get_sp_vendor_fulfilment(mp)
            orders_obj = orders.get_orders(**kwargs)
            return orders_obj
        except Exception as e:
            raise InvalidResponse(e)

    def load_order_page(self, mp, **kwargs):
        """
        a generator function to return all pages, obtained by NextToken
        """

        for page in self.load_all_orders(mp, **kwargs):
            orders = []
            for order in page.payload.get("Orders"):
                orders.append(order)

            yield orders

    @backoff.on_exception(
        backoff.expo,
        (Exception),
        max_tries=15,
        factor=3,
    )
    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        try:
            # Get start_date
            start_date = self.get_starting_timestamp(context) or datetime(2000, 1, 1)
            start_date = start_date.strftime("%Y-%m-%dT%H:%M:%S")
            if self.config.get("end_date"):
                end_date = parse(self.config.get("end_date"))
            else:
                # End date required by the endpoint
                end_date = datetime.today().strftime("%Y-%m-%dT%H:%M:%S.%fZ")

            sandbox = self.config.get("sandbox", False)
            if sandbox is True:
                rows = self.load_order_page(
                    mp=context.get("marketplace_id"), CreatedAfter="TEST_CASE_200"
                )
            else:
                rows = self.load_order_page(
                    mp=context.get("marketplace_id"),
                    createdBefore=end_date,
                    createdAfter=start_date,
                )
            for row in rows:
                for item in row:
                    yield item
        except Exception as e:
            raise InvalidResponse(e)


class VendorFulfilmentCustomerInvoicesStream(AmazonSellerStream):
    """Define custom stream."""

    name = "vendor_fulfilment_customer_invoices"
    primary_keys = ["purchaseOrderNumber"]
    # TODO loook for relevant key in live data
    replication_key = None
    parent_stream_type = MarketplacesStream
    marketplace_id = "{marketplace_id}"

    schema = th.PropertiesList(
        th.Property("purchaseOrderNumber", th.StringType),
        th.Property("content", th.StringType),
        th.Property("sellingParty", th.CustomType({"type": ["object", "string"]})),
        th.Property("shipFromParty", th.CustomType({"type": ["object", "string"]})),
        th.Property("labelFormat", th.CustomType({"type": ["object", "string"]})),
        th.Property("labelData", th.CustomType({"type": ["array", "string"]})),
    ).to_dict()

    @timeout(15)
    @load_all_pages()
    @backoff.on_exception(
        backoff.expo,
        (Exception),
        max_tries=15,
        factor=3,
    )
    def load_all_orders(self, mp, **kwargs):
        """
        a generator function to return all pages, obtained by NextToken
        """
        try:
            vendor_shipping = self.get_sp_vendor_fulfilment_shipping(mp)
            invoices_obj = vendor_shipping.get_orders(**kwargs)
            return invoices_obj
        except Exception as e:
            raise InvalidResponse(e)

    def load_order_page(self, mp, **kwargs):
        """
        a generator function to return all pages, obtained by NextToken
        """

        for page in self.load_all_orders(mp, **kwargs):
            orders = []
            for order in page.payload.get("shippingLabels"):
                orders.append(order)

            yield orders

    @backoff.on_exception(
        backoff.expo,
        (Exception),
        max_tries=15,
        factor=3,
    )
    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        try:
            # Get start_date
            start_date = self.get_starting_timestamp(context) or datetime(2000, 1, 1)
            start_date = start_date.strftime("%Y-%m-%dT%H:%M:%S")
            if self.config.get("end_date"):
                end_date = parse(self.config.get("end_date"))
            else:
                # End date required by the endpoint
                end_date = datetime.today().strftime("%Y-%m-%dT%H:%M:%S.%fZ")

            sandbox = self.config.get("sandbox", False)
            if sandbox is True:
                rows = self.load_order_page(
                    mp=context.get("marketplace_id"), CreatedAfter="TEST_CASE_200"
                )
            else:
                rows = self.load_order_page(
                    mp=context.get("marketplace_id"),
                    createdBefore=end_date,
                    createdAfter=start_date,
                )
            for row in rows:
                for item in row:
                    yield item
        except Exception as e:
            raise InvalidResponse(e)


class VendorPurchaseOrdersStream(AmazonSellerStream):
    """Define custom stream."""

    name = "vendor_purchase_orders"
    primary_keys = ["purchaseOrderNumber"]
    # TODO loook for relevant replication key in the live data
    replication_key = None
    parent_stream_type = MarketplacesStream
    marketplace_id = "{marketplace_id}"

    schema = th.PropertiesList(
        th.Property("purchaseOrderNumber", th.StringType),
        th.Property("purchaseOrderState", th.StringType),
        # Optional, not always populated
        th.Property("orderDetails", th.CustomType({"type": ["object", "string"]})),
        th.Property("deliveryWindow", th.StringType),
        th.Property("items", th.CustomType({"type": ["array", "string"]})),
    ).to_dict()

    @timeout(15)
    @load_all_pages()
    @backoff.on_exception(
        backoff.expo,
        (Exception),
        max_tries=15,
        factor=3,
    )
    def load_all_orders(self, mp, **kwargs):
        """
        a generator function to return all pages, obtained by NextToken
        """
        try:
            orders = self.get_sp_vendor(mp)
            orders_obj = orders.get_purchase_orders(**kwargs)
            return orders_obj
        except Exception as e:
            raise InvalidResponse(e)

    def load_order_page(self, mp, **kwargs):
        """
        a generator function to return all pages, obtained by NextToken
        """

        for page in self.load_all_orders(mp, **kwargs):
            orders = []
            for order in page.payload.get("Orders"):
                orders.append(order)

            yield orders

    @backoff.on_exception(
        backoff.expo,
        (Exception),
        max_tries=15,
        factor=3,
    )
    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        try:
            # Get start_date
            start_date = self.get_starting_timestamp(context) or datetime.today()
            start_date = start_date.strftime("%Y-%m-%dT%H:%M:%S")
            if self.config.get("end_date"):
                end_date = parse(self.config.get("end_date"))
            else:
                # End date required by the endpoint
                end_date = datetime.today().strftime("%Y-%m-%dT%H:%M:%S.%fZ")

            sandbox = self.config.get("sandbox", False)
            if sandbox is True:
                rows = self.load_order_page(
                    mp=context.get("marketplace_id"), CreatedAfter="TEST_CASE_200"
                )
            else:
                rows = self.load_order_page(
                    mp=context.get("marketplace_id"),
                    createdAfter=start_date,
                    limit=100,
                    SortOrder="DESC",
                )
            for row in rows:
                for item in row:
                    yield item
        except Exception as e:
            raise InvalidResponse(e)


class AFNInventoryCountryStream(AmazonSellerStream):
    """Define custom stream."""

    name = "afn_inventory_country"
    primary_keys = None
    replication_key = None
    report_id = None
    document_id = None
    schema = th.PropertiesList(
        th.Property("seller-sku", th.StringType),
        th.Property("fulfillment-channel-sku", th.StringType),
        th.Property("asin", th.StringType),
        th.Property("condition-type", th.StringType),
        th.Property("country", th.StringType),
        th.Property("quantity-for-local-fulfillment", th.StringType),
    ).to_dict()

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        if "asin1" in record:
            return {
                "ASIN": record["asin1"],
                "marketplace_id": context.get("marketplace_id"),
            }
        elif "product-id" in record:
            return {
                "ASIN": record["product-id"],
                "marketplace_id": context.get("marketplace_id"),
            }
        else:
            return []

    @backoff.on_exception(
        backoff.expo,
        (Exception),
        max_tries=15,
        factor=3,
    )
    # @timeout(15)
    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        try:
            # Leaving it here for EU marketplaces reference.
            eu_marketplaces = [
                "ES",
                "UK",
                "BE",
                "GB",
                "FR",
                "NL",
                "DE",
                "IT",
                "SE",
                "ZA",
                "PL",
                "EG",
                "TR",
                "SA",
                "AE",
                "IN",
            ]
            start_date = self.get_starting_timestamp(context) or datetime(2005, 1, 1)
            end_date = None
            if self.config.get("start_date"):
                start_date = parse(self.config.get("start_date"))

            start_date = start_date.strftime("%Y-%m-%dT00:00:00")
            report_types = ["GET_AFN_INVENTORY_DATA_BY_COUNTRY"]
            processing_status = self.config.get("processing_status")
            # Get list of valid marketplaces
            marketplaces = self.get_valid_marketplaces()
            common_marketplaces = list(set(marketplaces).intersection(eu_marketplaces))
            marketplace_id = None
            if len(common_marketplaces) > 0:
                marketplace_id = common_marketplaces[0]

            if marketplace_id in eu_marketplaces:
                report = self.get_sp_reports(marketplace_id=marketplace_id)

                items = report.get_reports(
                    reportTypes=report_types,
                    processingStatuses=processing_status,
                    dataStartTime=start_date,
                ).payload

                if not items["reports"]:
                    reports = self.create_report(
                        start_date,
                        report,
                        end_date,
                        "GET_AFN_INVENTORY_DATA_BY_COUNTRY",
                    )
                    for row in reports:
                        yield row

                # If reports are form loop through, download documents and populate the data.txt
                for row in items["reports"]:
                    reports = self.check_report(row["reportId"], report)
                    for report_row in reports:
                        if context is not None:
                            report_row.update(
                                {marketplace_id: context.get("marketplace_id")}
                            )
                        yield report_row

        except Exception as e:
            raise InvalidResponse(e)


class SalesTrafficReportStream(AmazonSellerStream):
    """Define custom stream."""

    name = "sales_traffic_report"
    primary_keys = None
    replication_key = "report_end_date"
    report_id = None
    document_id = None
    schema = th.PropertiesList(
        th.Property("reportId", th.StringType),
        th.Property(
            "reportSpecification", th.CustomType({"type": ["object", "string"]})
        ),
        th.Property(
            "salesAndTrafficByDate", th.CustomType({"type": ["array", "string"]})
        ),
        th.Property(
            "salesAndTrafficByAsin", th.CustomType({"type": ["array", "string"]})
        ),
        th.Property("report_end_date", th.DateTimeType),
    ).to_dict()

    @backoff.on_exception(
        backoff.expo,
        (Exception),
        max_tries=15,
        factor=5,
    )
    # @timeout(15)
    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        try:
            start_date = self.get_starting_timestamp(context)
            if start_date:
                # Remove timezone info from replication date so we can compare it with other dates.
                start_date = start_date.replace(tzinfo=None)
            end_date = None
            if self.config.get("start_date") and not start_date:
                start_date = parse(self.config.get("start_date"))
            # We can only do look back of maximum two years in this report type
            days_look_back = 730
            current_date = datetime.now()
            minimum_start_date = current_date - timedelta(days=days_look_back)
            if start_date < minimum_start_date:
                # Reset start date to days limit if it is greater than days_look_back days
                start_date = current_date - timedelta(days=days_look_back)

            end_date = start_date + timedelta(days=14)
            report_type = "GET_SALES_AND_TRAFFIC_REPORT"
            report_types = [report_type]
            processing_status = self.config.get("processing_status")
            # Get list of valid marketplaces

            marketplace_id = None
            if context is not None:
                marketplace_id = context.get("marketplace_id")

            report = self.get_sp_reports(marketplace_id=marketplace_id)
            while start_date <= current_date:
                start_date_f = start_date.strftime("%Y-%m-%dT00:00:00")
                end_date_f = end_date.strftime("%Y-%m-%dT23:59:59")
                items = self.get_reports_list(
                    report, report_types, processing_status, start_date_f, end_date_f
                )

                if not items["reports"]:
                    reports = self.create_report(
                        start_date_f,
                        report,
                        end_date_f,
                        report_type,
                        # reportOptions={"reportPeriod": "DAY","sellingProgram": "RETAIL","distributorView": "MANUFACTURING"},
                        report_format_type="json",
                    )
                    for row in reports:
                        row.update({"report_end_date": end_date.isoformat()})
                        yield row

                # If reports are form loop through, download documents and populate the data.txt
                for row in items["reports"]:
                    reports = self.check_report(row["reportId"], report, "json")
                    for report_row in reports:
                        report_row.update({"report_end_date": end_date.isoformat()})
                        yield report_row
                # Move to the next time period
                start_date = end_date + timedelta(days=1)
                end_date += timedelta(days=14)
                do_something = ""

        except Exception as e:
            raise InvalidResponse(e)


class FBAInventoryLedgerDetailedReportStream(AmazonSellerStream):
    """Define custom stream."""

    name = "fba_inventory_ledger_detailed"
    primary_keys = None
    replication_key = "report_end_date"
    report_id = None
    document_id = None
    correct_end_date_minus_days = 1
    schema = th.PropertiesList(
        th.Property("Date", th.DateTimeType),
        th.Property("FNSKU", th.StringType),
        th.Property("ASIN", th.StringType),
        th.Property("MSKU", th.StringType),
        th.Property("Title", th.StringType),
        th.Property("Event Type", th.StringType),
        th.Property("Reference ID", th.StringType),
        th.Property("Quantity", th.StringType),
        th.Property("Fulfillment Center", th.StringType),
        th.Property("Disposition", th.StringType),
        th.Property("Reason", th.StringType),
        th.Property("Country", th.StringType),
        th.Property("Reconciled Quantity", th.StringType),
        th.Property("Unreconciled Quantity", th.StringType),
        th.Property("Date and Time", th.DateTimeType),
        th.Property("report_end_date", th.DateTimeType),
    ).to_dict()

    def correct_end_date(self, end_date, start_date, current_date):
        if end_date >= current_date:
            # If end_date is greater than today then fetch report for yesterday.
            end_date = current_date - timedelta(days=self.correct_end_date_minus_days)

        if end_date <= start_date:
            end_date = start_date
        return end_date

    @backoff.on_exception(
        backoff.expo,
        (Exception),
        max_tries=15,
        factor=5,
    )
    # @timeout(15)
    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        try:
            start_date = self.get_starting_timestamp(context)
            if start_date:
                # Remove timezone info from replication date so we can compare it with other dates.
                start_date = start_date.replace(tzinfo=None)
            end_date = None
            if self.config.get("start_date") and not start_date:
                start_date = parse(self.config.get("start_date"))
            # We can only do look back of maximum two years in this report type
            months_lookback = 18
            current_date = datetime.now()
            minimum_start_date = current_date - relativedelta(months=months_lookback)
            if start_date < minimum_start_date:
                # Reset start date to days limit if it is greater than days_look_back days
                start_date = current_date - relativedelta(months=months_lookback)

            end_date = current_date
            end_date = self.correct_end_date(end_date, start_date, current_date)
            report_type = "GET_LEDGER_DETAIL_VIEW_DATA"
            report_types = [report_type]
            processing_status = self.config.get("processing_status")
            # Get list of valid marketplaces

            marketplace_id = None
            if context is not None:
                marketplace_id = context.get("marketplace_id")

            report = self.get_sp_reports(marketplace_id=marketplace_id)
            start_date_f = start_date.strftime("%Y-%m-%dT00:00:00")
            end_date_f = end_date.strftime("%Y-%m-%dT23:59:59")

            reports = self.create_report(
                start_date_f,
                report,
                end_date_f,
                report_type,
                reportOptions={"eventType": "Adjustments"},
                encoding="utf-8",
                fix_encoding=True,
            )
            for row in reports:
                row.update({"report_end_date": end_date.isoformat()})
                if "Date" in row:
                    date_object = datetime.strptime(row["Date"], "%m/%d/%Y")
                    row["Date"] = date_object.date().isoformat()
                yield row

        except Exception as e:
            raise InvalidResponse(e)


class FBAInventoryLedgerSummaryReportStream(AmazonSellerStream):
    """Define custom stream."""

    name = "fba_inventory_ledger_summary"
    primary_keys = None
    replication_key = "report_end_date"
    report_id = None
    document_id = None
    correct_end_date_minus_days = 1
    schema = th.PropertiesList(
        th.Property("Date", th.DateTimeType),
        th.Property("FNSKU", th.StringType),
        th.Property("ASIN", th.StringType),
        th.Property("MSKU", th.StringType),
        th.Property("Title", th.StringType),
        th.Property("Disposition", th.StringType),
        th.Property("Starting Warehouse Balance", th.StringType),
        th.Property("In Transit Between Warehouses", th.StringType),
        th.Property("Receipts", th.StringType),
        th.Property("Customer Shipments", th.StringType),
        th.Property("Customer Returns", th.StringType),
        th.Property("Vendor Returns", th.StringType),
        th.Property("Warehouse Transfer In/Out", th.StringType),
        th.Property("Found", th.StringType),
        th.Property("Lost", th.StringType),
        th.Property("Damaged", th.StringType),
        th.Property("Disposed", th.StringType),
        th.Property("Other Events", th.StringType),
        th.Property("Ending Warehouse Balance", th.StringType),
        th.Property("Unknown Events", th.StringType),
        th.Property("Location", th.StringType),
        th.Property("Store", th.StringType),
        th.Property("reportId", th.StringType),
        th.Property("MarketplaceName", th.StringType),
        th.Property("report_end_date", th.DateTimeType),
    ).to_dict()

    def correct_end_date(self, end_date, start_date, current_date):
        if end_date >= current_date:
            end_date = current_date - timedelta(days=self.correct_end_date_minus_days)

        if end_date <= start_date:
            end_date = start_date
        return end_date

    @backoff.on_exception(
        backoff.expo,
        (Exception),
        max_tries=15,
        factor=5,
    )
    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        try:
            start_date = self.get_starting_timestamp(context)
            if start_date:
                start_date = start_date.replace(tzinfo=None)
            end_date = None
            if self.config.get("start_date") and not start_date:
                start_date = parse(self.config.get("start_date"))
            months_lookback = 18
            current_date = datetime.now()
            minimum_start_date = current_date - relativedelta(months=months_lookback)
            if start_date < minimum_start_date:
                start_date = current_date - relativedelta(months=months_lookback)

            end_date = current_date
            end_date = self.correct_end_date(end_date, start_date, current_date)
            report_type = "GET_LEDGER_SUMMARY_VIEW_DATA"
            marketplace_id = None
            if context is not None:
                marketplace_id = context.get("marketplace_id")

            report = self.get_sp_reports(marketplace_id=marketplace_id)
            start_date_f = start_date.strftime("%Y-%m-%dT00:00:00")
            end_date_f = end_date.strftime("%Y-%m-%dT23:59:59")

            reports = self.create_report(
                start_date_f,
                report,
                end_date_f,
                report_type,
                reportOptions={
                    "aggregateByLocation": "FC",
                    "aggregatedByTimePeriod": "DAILY",
                },
                encoding="utf-8",
                fix_encoding=True,
            )
            for row in reports:
                row.update({"report_end_date": end_date.isoformat()})
                if "Date" in row:
                    date_object = datetime.strptime(row["Date"], "%m/%d/%Y")
                    row["Date"] = date_object.date().isoformat()
                yield row

        except Exception as e:
            raise InvalidResponse(e)


class FBACustomerShipmentSalesReportStream(AmazonSellerStream):
    """Define custom stream."""

    name = "fba_customer_shipment_sales"
    primary_keys = None
    replication_key = "shipment-date"
    report_id = None
    document_id = None
    correct_end_date_minus_days = 2 #EU has upto 24 hour delay in updates
    schema = th.PropertiesList(
        th.Property("shipment-date", th.DateTimeType),
        th.Property("sku", th.StringType),
        th.Property("fnsku", th.StringType),
        th.Property("asin", th.StringType),
        th.Property("fulfillment-center-id", th.StringType),
        th.Property("quantity", th.StringType),
        th.Property("amazon-order-id", th.StringType),
        th.Property("currency", th.StringType),
        th.Property("item-price-per-unit", th.StringType),
        th.Property("shipping-price", th.StringType),
        th.Property("gift-wrap-price", th.StringType),
        th.Property("ship-city", th.StringType),
        th.Property("ship-state", th.StringType),
        th.Property("ship-postal-code", th.StringType),
    ).to_dict()

    def correct_end_date(self, end_date, start_date, current_date):
        if end_date > current_date:
            # If end_date is greater than today then fetch report for yesterday.
            end_date = current_date - timedelta(days=self.correct_end_date_minus_days)

        if end_date <= start_date:
            end_date = start_date
        return end_date

    @backoff.on_exception(
        backoff.expo,
        (Exception),
        max_tries=15,
        factor=5,
    )
    # @timeout(15)
    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        try:
            start_date = self.get_starting_timestamp(context)
            if start_date:
                # Remove timezone info from replication date so we can compare it with other dates.
                start_date = start_date.replace(tzinfo=None)
            end_date = None
            if self.config.get("start_date") and not start_date:
                start_date = parse(self.config.get("start_date"))
            # We can only do look back of maximum two years in this report type
            days_look_back = 545  # Few days less than 18 months
            current_date = datetime.now()
            minimum_start_date = current_date - timedelta(days=days_look_back)
            if start_date < minimum_start_date:
                # Reset start date to days limit if it is greater than days_look_back days
                start_date = current_date - timedelta(days=days_look_back)

            end_date = start_date + timedelta(days=30)
            end_date = self.correct_end_date(end_date, start_date, current_date)
            report_type = "GET_FBA_FULFILLMENT_CUSTOMER_SHIPMENT_SALES_DATA"
            report_types = [report_type]
            processing_status = self.config.get("processing_status")
            # Get list of valid marketplaces

            marketplace_id = None
            if context is not None:
                marketplace_id = context.get("marketplace_id")

            report = self.get_sp_reports(marketplace_id=marketplace_id)
            while start_date <= current_date:
                start_date_f = start_date.strftime("%Y-%m-%dT00:00:00")
                end_date_f = end_date.strftime("%Y-%m-%dT23:59:59")
                items = self.get_reports_list(
                    report, report_types, processing_status, start_date_f, end_date_f
                )

                if not items["reports"]:
                    reports = self.create_report(
                        start_date_f,
                        report,
                        end_date_f,
                        report_type,
                    )
                    if not reports:
                        return None
                    for row in reports:
                        row.update({"report_end_date": end_date.isoformat()})
                        yield row

                # If reports are form loop through, download documents and populate the data.txt
                for row in items["reports"]:
                    reports = self.check_report(row["reportId"], report, "json")
                    for report_row in reports:
                        report_row.update({"report_end_date": end_date.isoformat()})
                        yield report_row
                # Move to the next time period
                start_date = end_date + timedelta(days=1)
                end_date += timedelta(days=30)
                end_date = self.correct_end_date(end_date, start_date, current_date)
                # According to Amazon, spamming bad is, wait for it, good you should - Yoda's lesson of the day!
                time.sleep(60)

        except Exception as e:
            raise InvalidResponse(e)
