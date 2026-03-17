"""Stream type classes for tap-amazon-seller."""
import time
import urllib.parse
from datetime import datetime, timedelta
from typing import Iterable, Optional

import backoff
from dateutil.parser import parse
from dateutil.relativedelta import relativedelta
from singer_sdk import typing as th
from sp_api.base.exceptions import SellingApiBadRequestException, SellingApiNotFoundException
from sp_api.util import load_all_pages

from tap_amazon_seller.client import AmazonSellerStream
from tap_amazon_seller.orders_transform import (
    SANDBOX_V2,
    transform_order_address_v2_to_v0,
    transform_order_buyer_info_v2_to_v0,
    transform_order_items_v2_to_v0,
    transform_order_v2_to_v0,
)
from tap_amazon_seller.utils import timeout


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

    @backoff.on_exception(
        backoff.expo,
        Exception,
        max_tries=10,
        factor=3,
    )
    @timeout(15)
    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        sandbox = self.config.get("sandbox", False)
        for mp in self.get_valid_marketplaces():
            yield mp
            if sandbox:
                break


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
    ).to_dict()

    @load_all_pages(next_token_param="paginationToken")
    @backoff.on_exception(
        backoff.expo,
        (Exception),
        max_tries=10,
        factor=3,
    )
    @timeout(15)
    def _load_all_orders_paginated(self, mp, **kwargs):
        orders = self.get_sp_orders_v2(mp)
        try:
            orders_obj = orders.search_orders(**kwargs)
            self.backoff_retries = 0
        except SellingApiBadRequestException as e:
            if self.backoff_retries >= 3:
                self.logger.warning(
                    f"Giving up on stream {self.name} after {self.backoff_retries} attempts. Ending gracefully."
                )
                self.backoff_retries = 0
                return type(
                    "Page", (), {"payload": {"orders": []}, "next_token": None}
                )()
            else:
                self.backoff_retries += 1
                self.logger.info(f"Kwargs in latest request: {kwargs}")
                raise e
        return orders_obj

    @backoff.on_exception(
        backoff.expo,
        (Exception),
        max_tries=10,
        factor=3,
    )
    @timeout(15)
    def _load_orders_single_page(self, mp, **kwargs):
        orders = self.get_sp_orders_v2(mp)
        return orders.search_orders(**kwargs)

    def load_order_page(self, mp, sandbox=False, **kwargs):
        if sandbox:
            page = self._load_orders_single_page(mp, **kwargs)
            orders = [
                transform_order_v2_to_v0(o)
                for o in page.payload.get("orders", [])
            ]
            yield orders
        else:
            for page in self._load_all_orders_paginated(mp, **kwargs):
                orders = [
                    transform_order_v2_to_v0(o)
                    for o in page.payload.get("orders", [])
                ]
                yield orders

    @backoff.on_exception(
        backoff.expo,
        (Exception),
        max_tries=10,
        factor=3,
    )
    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        start_date = self.get_starting_timestamp(context) or datetime(2000, 1, 1)
        start_date = start_date.strftime("%Y-%m-%dT%H:%M:%SZ")
        end_date = None
        if self.config.get("end_date"):
            end_date = parse(self.config.get("end_date"))
            end_date = end_date.strftime("%Y-%m-%dT%H:%M:%SZ")

        sandbox = self.config.get("sandbox", False)
        if sandbox is True:
            rows = self.load_order_page(
                mp=SANDBOX_V2.marketplace,
                sandbox=True,
                createdAfter=SANDBOX_V2.created_after,
                marketplaceIds=[SANDBOX_V2.marketplace_id],
                includedData=SANDBOX_V2.included_data,
            )
        else:
            included = ["FULFILLMENT", "PROCEEDS"]
            if start_date and end_date:
                rows = self.load_order_page(
                    mp=context.get("marketplace_id"),
                    lastUpdatedAfter=start_date,
                    lastUpdatedBefore=end_date,
                    includedData=included,
                )
            else:
                rows = self.load_order_page(
                    mp=context.get("marketplace_id"),
                    lastUpdatedAfter=start_date,
                    includedData=included,
                )
        for row in rows:
            for item in row:
                yield item

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        mp = context.get("marketplace_id")
        return {"AmazonOrderId": record["AmazonOrderId"], "marketplace_id": mp}


class OrderItemsStream(AmazonSellerStream):
    """Define custom stream."""

    name = "orderitems"
    primary_keys = ["AmazonOrderId"]
    replication_key = None
    order_id = "{AmazonOrderId}"
    parent_stream_type = OrdersStream
    schema_writed = False

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
    ).to_dict()

    @backoff.on_exception(
        backoff.expo,
        (Exception),
        max_tries=10,
        factor=3,
    )
    @timeout(15)
    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        order_id = context.get("AmazonOrderId", [])

        self.state_partitioning_keys = self.partitions[len(self.partitions) - 1]
        self.logger.info(f"Requesting orderitems for order with AmazonOrderId {order_id}")
        sandbox = self.config.get("sandbox", False)
        mp = SANDBOX_V2.marketplace if sandbox else context.get("marketplace_id")
        order = self.get_order_v2(
            order_id,
            mp,
            included_data=["FULFILLMENT", "PROCEEDS", "EXPENSE", "PROMOTION", "CANCELLATION"],
        )
        return [transform_order_items_v2_to_v0(order)]


class OrderBuyerInfo(AmazonSellerStream):
    """Define custom stream."""

    name = "orderbuyerinfo"
    primary_keys = ["AmazonOrderId"]
    replication_key = None
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
    ).to_dict()

    @backoff.on_exception(
        backoff.expo,
        (Exception),
        max_tries=10,
        factor=3,
    )
    @timeout(15)
    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        order_id = context.get("AmazonOrderId", [])

        self.logger.info(f"Requesting orderbuyerinfo for order with AmazonOrderId {order_id}")
        sandbox = self.config.get("sandbox", False)
        mp = SANDBOX_V2.marketplace if sandbox else context.get("marketplace_id")
        order = self.get_order_v2(order_id, mp, included_data=["BUYER"])
        return [transform_order_buyer_info_v2_to_v0(order)]


class OrderAddress(AmazonSellerStream):
    """Define custom stream."""

    name = "orderaddress"
    primary_keys = ["AmazonOrderId"]
    replication_key = None
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
    ).to_dict()

    @backoff.on_exception(
        backoff.expo,
        (Exception),
        max_tries=10,
        factor=3,
    )
    @timeout(15)
    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        order_id = context.get("AmazonOrderId", [])

        self.logger.info(f"Requesting orderaddress for order with AmazonOrderId {order_id}")
        sandbox = self.config.get("sandbox", False)
        mp = SANDBOX_V2.marketplace if sandbox else context.get("marketplace_id")
        order = self.get_order_v2(order_id, mp, included_data=["RECIPIENT", "BUYER"])
        return [transform_order_address_v2_to_v0(order)]


class OrderFinancialEvents(AmazonSellerStream):
    """Define custom stream."""

    name = "orderfinancialevents"
    primary_keys = ["AmazonOrderId"]
    replication_key = None
    order_id = "{AmazonOrderId}"
    parent_stream_type = OrdersStream
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
    ).to_dict()

    @backoff.on_exception(
        backoff.expo,
        (Exception),
        max_tries=10,
        factor=3,
    )
    @timeout(15)
    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        order_id = context.get("AmazonOrderId", [])

        finance = self.get_sp_finance(context.get("marketplace_id"))

        self.logger.info(f"Requesting orderfinancialevents for order with AmazonOrderId {order_id}")
        sandbox = self.config.get("sandbox", False)
        if sandbox is False:
            # self.state_partitioning_keys = self.partitions
            self.state_partitioning_keys = self.partitions[len(self.partitions) - 1]
            items = finance.get_financial_events_for_order(order_id).payload
            items["AmazonOrderId"] = order_id
        else:
            items = finance.get_financial_events_for_order("TEST_CASE_200").payload
        return [items["FinancialEvents"]]


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
        max_tries=10,
        factor=3,
    )
    @timeout(15)
    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
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
            self.logger.info(f"Creating new report. StartDate:{start_date}, EndDate: {end_date}, ReportName:{self.name}")
            reports = self.create_report(start_date, report, end_date)
            for row in reports:
                yield row

        # If reports are form loop through, download documents and populate the data.txt
        for row in items["reports"]:
            reports = self.check_report(row["reportId"], report)
            for report_row in reports:
                yield report_row


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
        max_tries=10,
        factor=3,
    )
    @timeout(15)
    def load_all_items(self, mp, **kwargs):
        """
        a generator function to return all pages, obtained by NextToken
        """
        wi = self.get_warehouse_object(mp)
        kwargs.update({"details": True})
        del kwargs["startDateTime"]
        if self.next_token is not None:
            kwargs.update({"nextToken": self.next_token})

        try:
            list = wi.get_inventory_summary_marketplace(**kwargs)
            self.backoff_retries = 0
        except SellingApiBadRequestException as e:
            if self.backoff_retries >= 3:
                self.logger.warning(
                    f"Giving up on stream {self.name} after {self.backoff_retries} attempts. Ending gracefully."
                )
                self.backoff_retries = 0
                return type(
                    "Page", (), {"payload": {}, "next_token": None}
                )()
            else:
                self.backoff_retries += 1
                # decode token
                self.logger.info(f"Kwargs in latest request: {kwargs}")
                if kwargs.get("NextToken"):
                    kwargs["NextToken"] = urllib.parse.quote(kwargs["NextToken"])
                if kwargs.get("nextToken"):
                    kwargs["nextToken"] = urllib.parse.quote(kwargs["nextToken"])
                raise e
        return list

    def load_item_page(self, mp, **kwargs):
        """
        a generator function to return all pages, obtained by NextToken
        """

        for page in self.load_all_items(mp, **kwargs):
            self.next_token = page.next_token
            yield page.payload
            
    

    @backoff.on_exception(backoff.expo, (Exception), max_tries=10, factor=3)
    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
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


class ProductsIventoryStream(AmazonSellerStream):
    """Define custom stream."""

    name = "products_inventory"
    primary_keys = ["listing_id"]
    replication_key = None
    report_id = None
    document_id = None
    parent_stream_type = MarketplacesStream
    schema = th.PropertiesList(
        th.Property("marketplaceIds", th.CustomType({"type": ["array", "string"]})),
        th.Property("item_name", th.StringType),
        th.Property("marketplace_id", th.StringType),
        th.Property("item_description", th.StringType),
        th.Property("listing_id", th.StringType),
        th.Property("seller_sku", th.StringType),
        th.Property("price", th.StringType),
        th.Property("quantity", th.StringType),
        th.Property("open_date", th.StringType),
        th.Property("image_url", th.StringType),
        th.Property("item_is_marketplace", th.StringType),
        th.Property("product_id_type", th.StringType),
        th.Property("zshop_shipping_fee", th.StringType),
        th.Property("item_note", th.StringType),
        th.Property("item_condition", th.StringType),
        th.Property("zshop_category1", th.StringType),
        th.Property("zshop_browse_path", th.StringType),
        th.Property("asin1", th.StringType),
        th.Property("asin2", th.StringType),
        th.Property("asin3", th.StringType),
        th.Property("will_ship_internationally", th.StringType),
        th.Property("zshop_boldface", th.StringType),
        th.Property("product_id", th.StringType),
        th.Property("bid_for_featured_placement", th.StringType),
        th.Property("add_delete", th.StringType),
        th.Property("pending_quantity", th.StringType),
        th.Property("fulfilment_channel", th.StringType),
        th.Property("merchant_shipping_group", th.StringType),
        th.Property("status", th.StringType),
        th.Property("Minimum order quantity", th.StringType),
        th.Property("Sell remainder", th.StringType),
        th.Property("product_id", th.StringType),
        th.Property("marketplace_id", th.StringType),
    ).to_dict()

    def post_process(self, row, context = None):
        """As needed, append or transform raw data to match expected structure.

        Args:
            row: An individual record from the stream.
            context: The stream context.

        Returns:
            The updated record dictionary, or ``None`` to skip the record.
        """
        if row is not None:
            processed_row = {}
            for k in row:
                new_k = k
                if "-" in k:
                    new_k = k.replace("-", "_")
                processed_row[new_k] = row[k]
            return processed_row

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        if "asin1" in record or (record.get("product_id_type") and record.get("product_id_type") in ["1", "ASIN"]):
            return {
                "ASIN": record.get("asin1", record.get("product_id")),
                "marketplace_id": context.get("marketplace_id"),
            }
        elif "product_id" in record and record.get("product_id_type") not in ["1", "ASIN"]:
            return {
                "product-id": record["product_id"],
                "marketplace_id": context.get("marketplace_id"),
                "product-id-type": self.get_product_type(record.get("product_id_type")),
            }
        else:
            return []
        
    def get_product_type(self, product_type: dict) -> str:
        types = {
            "1": "ASIN",
            "2": "SellerSKU",
            "3": "UPC",
            "4": "EAN",
            "5": "ISBN",
            "6": "JAN",
        }
        return types.get(product_type, product_type)

    @backoff.on_exception(
        backoff.expo,
        (Exception),
        max_tries=10,
        factor=3,
    )
    @timeout(15)
    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
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
            self.logger.info(f"Creating new report. StartDate:{start_date}, EndDate: {end_date}, ReportName:{self.name}")
            reports = self.create_report(
                start_date, report, end_date, "GET_MERCHANT_LISTINGS_ALL_DATA"
            )
            for row in reports:
                transformed_record = self.post_process(row)
                if transformed_record is None:
                    continue
                yield transformed_record

        # If reports are form loop through, download documents and populate the data.txt
        for row in items["reports"]:
            reports = self.check_report(row["reportId"], report)
            for report_row in reports:
                if context is not None:
                    report_row.update(
                        {marketplace_id: context.get("marketplace_id")}
                    )
                transformed_record = self.post_process(report_row)
                if transformed_record is None:
                    continue
                yield transformed_record


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
        th.Property("ASIN", th.StringType),
        th.Property("Identifiers", th.CustomType({"type": ["object", "string"]})),
        th.Property("AttributeSets", th.CustomType({"type": ["array", "string"]})),
        th.Property("Relationships", th.CustomType({"type": ["array", "string"]})),
        th.Property("SalesRankings", th.CustomType({"type": ["array", "string"]})),
        th.Property("marketplace_id", th.StringType),
    ).to_dict()

    @backoff.on_exception(
        backoff.expo,
        (Exception),
        max_tries=10,
        factor=3,
    )
    @timeout(15)
    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        # if context is not None:
        asin = context.get("ASIN")
        catalog = self.get_sp_catalog(context.get("marketplace_id"))
        self.logger.info(f"Requesting product_details with asin {asin}")
        if context.get("marketplace_id") == "JP":
            items = catalog.list_items(JAN=asin).payload
        elif context.get("marketplace_id") in ["FR"]:
            items = catalog.list_items(EAN=asin).payload
        else:
            items = catalog.get_item(asin=asin).payload
        if "Items" in items:
            if len(items["Items"]) > 0:
                items = items["Items"][0]
        items.update({"ASIN": asin})
        items.update({"marketplace_id": context.get("marketplace_id")})
        return [items]


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
        max_tries=10,
        factor=3,
    )
    @timeout(15)
    def load_all_orders(self, mp, **kwargs):
        """
        a generator function to return all pages, obtained by NextToken
        """
        orders = self.get_sp_vendor_fulfilment(mp)
        orders_obj = orders.get_orders(**kwargs)
        return orders_obj

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
        max_tries=10,
        factor=3,
    )
    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
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

    @load_all_pages()
    @backoff.on_exception(
        backoff.expo,
        (Exception),
        max_tries=10,
        factor=3,
    )
    @timeout(15)
    def load_all_orders(self, mp, **kwargs):
        """
        a generator function to return all pages, obtained by NextToken
        """
        vendor_shipping = self.get_sp_vendor_fulfilment_shipping(mp)
        invoices_obj = vendor_shipping.get_orders(**kwargs)
        return invoices_obj

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
        max_tries=10,
        factor=3,
    )
    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
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

    @load_all_pages()
    @backoff.on_exception(
        backoff.expo,
        (Exception),
        max_tries=10,
        factor=3,
    )
    @timeout(15)
    def load_all_orders(self, mp, **kwargs):
        """
        a generator function to return all pages, obtained by NextToken
        """
        orders = self.get_sp_vendor(mp)
        orders_obj = orders.get_purchase_orders(**kwargs)
        return orders_obj

    def load_order_page(self, mp, **kwargs):
        """
        a generator function to return all pages, obtained by NextToken
        """
        self.logger.info(f"Requesting vendor_purchase_orders with kwargs: {kwargs} in marketplace: {mp}")
        for page in self.load_all_orders(mp, **kwargs):
            orders = []
            for order in page.payload.get("Orders"):
                orders.append(order)

            yield orders

    @backoff.on_exception(
        backoff.expo,
        (Exception),
        max_tries=10,
        factor=3,
    )
    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
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
        max_tries=10,
        factor=3,
    )
    # @timeout(15)
    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
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
        marketplaces = [mp["id"] for mp in self.get_valid_marketplaces()]
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
                self.logger.info(f"Creating new report. StartDate:{start_date}, EndDate: {end_date}, ReportName:{self.name}")
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


class SalesTrafficReportStream(AmazonSellerStream):
    """Define custom stream."""

    name = "sales_traffic_report"
    primary_keys = ["reportId"]
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
        max_tries=10,
        factor=5,
    )
    # @timeout(15)
    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
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
                self.logger.info(f"Creating new report. StartDate:{start_date_f}, EndDate: {end_date_f}, ReportName:{self.name}")
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


class FBAInventoryLedgerDetailedReportStream(AmazonSellerStream):
    """Define custom stream."""

    name = "fba_inventory_ledger_detailed"
    primary_keys = None
    replication_key = "Date"
    report_id = None
    document_id = None
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

    @backoff.on_exception(
        backoff.expo,
        (Exception),
        max_tries=10,
        factor=5,
    )
    # @timeout(15)
    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
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
        items = self.get_reports_list(
            report, report_types, processing_status, start_date_f, end_date_f
        )

        if not items["reports"]:
            report_options = {"eventType": "Adjustments"}
            self.logger.info(f"Creating new report. StartDate:{start_date_f}, EndDate: {end_date_f}, ReportName:{self.name}, ReportOptions: {report_options}")
            reports = self.create_report(
                start_date_f,
                report,
                end_date_f,
                report_type,
                reportOptions=report_options,
            )
            for row in reports:
                row.update({"report_end_date": end_date.isoformat()})
                if "Date" in row:
                    date_object = datetime.strptime(row["Date"], "%m/%d/%Y")
                    row["Date"] = date_object.date().isoformat()
                yield row

        # If reports are form loop through, download documents and populate the data.txt
        for row in items["reports"]:
            reports = self.check_report(row["reportId"], report, "json")
            for report_row in reports:
                if "Date" in report_row:
                    date_object = datetime.strptime(report_row["Date"], "%m/%d/%Y")
                    report_row["Date"] = date_object.date().isoformat()
                
                report_row.update({"report_end_date": end_date.isoformat()})
                yield report_row


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
        max_tries=10,
        factor=5,
    )
    # @timeout(15)
    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
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
                self.logger.info(f"Creating new report. StartDate:{start_date_f}, EndDate: {end_date_f}, ReportName:{self.name}")
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

class ProductDetailsV2Stream(AmazonSellerStream):
    """Define custom stream."""

    name = "product_catalog_details"
    primary_keys = ["ASIN"]
    replication_key = None
    asin = "{ASIN}"
    parent_stream_type = ProductsIventoryStream
    # Optionally, you may also use `schema_filepath` in place of `schema`:
    # schema_filepath = SCHEMAS_DIR / "users.json"
    schema = th.PropertiesList(
        th.Property("ASIN", th.StringType),
        th.Property("identifiers", th.CustomType({"type": ["array", "string"]})),
        th.Property("attributes", th.CustomType({"type": ["object", "string"]})),
       
        th.Property("marketplace_id", th.StringType),
    ).to_dict()

    @backoff.on_exception(
        backoff.expo,
        (Exception),
        max_tries=10,
        factor=3,
    )
    @timeout(15)
    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        try:
            # if context is not None:
            asin = context.get("ASIN")
            product_id = context.get("product-id")
            catalog = self.get_sp_catalog_item(context.get("marketplace_id"))
            # Requesting relationships along with this data results in an error. 
            # requesting summaries nullifies other requests and only summaries are part of the response
            product_include_data = ['attributes','identifiers']
            if self.config.get("products_include_data"):
                product_include_data = self.config.get("products_include_data")
                if isinstance(product_include_data, str):
                    product_include_data = product_include_data.split(",")
                
            
            if asin:
                self.logger.info(f"Making request to product_catalog_details with asin {asin}, includedData {product_include_data}")
                items = catalog.get_catalog_item(asin=asin,includedData=product_include_data).payload
            elif product_id:
                self.logger.info(f"Making request to product_catalog_details with product_id {product_id}, product_id_type {context.get('product-id-type')}, includedData {product_include_data}")
                items = catalog.get_catalog_item(identifiers=product_id, identifiersType=context.get("product-id-type"),includedData=product_include_data, version="2022-04-01").payload

            if "Items" in items:
                if len(items["Items"]) > 0:
                    items = items["Items"][0]
            items.update({"ASIN": asin})
            items.update({"marketplace_id": context.get("marketplace_id")})
            return [items]
            # else:
            #     return []
        except SellingApiNotFoundException as e:
            self.logger.warn(e)
            return []

class AWDInventoryStream(AmazonSellerStream):
    """AWD (Amazon Warehousing and Distribution) Inventory stream."""
    # Note: AWD is only available for the US marketplace
    name = "awd_inventory"
    primary_keys = ["sku"]
    replication_key = None

    schema = th.PropertiesList(
        th.Property("sku", th.StringType),
        th.Property("totalInboundQuantity", th.IntegerType),
        th.Property("totalOnhandQuantity", th.IntegerType),
        th.Property("inventoryDetails", 
            th.ObjectType(
                th.Property("replenishmentQuantity", th.IntegerType),
                th.Property("availableDistributableQuantity", th.IntegerType),
                th.Property("reservedDistributableQuantity", th.IntegerType),
            )
        ),
    ).to_dict()

    @backoff.on_exception(
        backoff.expo,
        (Exception),
        max_tries=10,
        factor=3,
    )
    @timeout(15)
    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        """Get AWD inventory records.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            AWD inventory records.
        """
        awd = self.get_sp_awd()
    

        response = awd.list_inventory(details="SHOW")
        inventory_items = response.payload.get("inventory")

        # Handle pagination if there are more results
        while True:
            for item in inventory_items:
                yield item

            next_token = response.next_token
            if not next_token:
                break

            response = awd.list_inventory(details="SHOW", nextToken=next_token)
            inventory_items = response.payload.get("inventory", [])
            
class AccountStream(AmazonSellerStream):
    """
    Stream to fetch seller account information using:
    GET /accounts/v1/account
    Ref: https://developer-docs.amazon.com/sp-api/reference/getaccount
    """

    name = "account"
    primary_keys = []  # No unique ID in response; adjust if needed
    replication_key = None

    schema = th.PropertiesList(
        th.Property("businessType", th.StringType),
        th.Property("sellingPlan", th.StringType),
        th.Property("business", th.ObjectType(
            th.Property("name", th.StringType),
            th.Property("nonLatinName", th.StringType),
            th.Property("companyRegistrationNumber", th.StringType),
            th.Property("companyTaxIdentificationNumber", th.StringType),
            th.Property("registeredBusinessAddress", th.ObjectType(
                th.Property("addressLine1", th.StringType),
                th.Property("addressLine2", th.StringType),
                th.Property("city", th.StringType),
                th.Property("stateOrProvinceCode", th.StringType),
                th.Property("postalCode", th.StringType),
                th.Property("countryCode", th.StringType),
            ))
        )),
        th.Property("primaryContact", th.ObjectType(
            th.Property("name", th.StringType),
            th.Property("nonLatinName", th.StringType),
            th.Property("address", th.ObjectType(
                th.Property("addressLine1", th.StringType),
                th.Property("addressLine2", th.StringType),
                th.Property("city", th.StringType),
                th.Property("stateOrProvinceCode", th.StringType),
                th.Property("postalCode", th.StringType),
                th.Property("countryCode", th.StringType),
            ))
        )),
        th.Property("marketplaceParticipationList", th.ArrayType(th.ObjectType(
            th.Property("storeName", th.StringType),
            th.Property("marketplace", th.ObjectType(
                th.Property("id", th.StringType),
                th.Property("name", th.StringType),
                th.Property("countryCode", th.StringType),
                th.Property("domainName", th.StringType),
            )),
            th.Property("participation", th.ObjectType(
                th.Property("isParticipating", th.BooleanType),
                th.Property("hasSuspendedListings", th.BooleanType),
            ))
        )))
    ).to_dict()

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        """Yield the account payload as a single record."""
        response = self.get_account_info()
        yield response 

    def get_account_info(self) -> dict:
        """Call /accounts/v1/account endpoint using authenticated client."""
        client = self.get_sp_sellers()  
        response = client.get_account()
        return response.payload 
