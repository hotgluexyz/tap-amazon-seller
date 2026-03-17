"""Amazon-Seller tap class."""
import json
import os
import sys
from typing import List

os.environ["ENV_DISABLE_DONATION_MSG"] = "1"


def _apply_sandbox_env_from_argv() -> None:
    """Set AWS_ENV=SANDBOX before sp_api is imported if the config file requests it.

    sp_api reads AWS_ENV at import time, so this must run before the streams
    import below. Errors reading the file are intentionally ignored here —
    they will surface as proper exceptions when the SDK validates the config.
    """
    for i, arg in enumerate(sys.argv):
        if arg != "--config" or i + 1 >= len(sys.argv):
            continue
        try:
            with open(sys.argv[i + 1]) as f:
                config = json.load(f)
        except (OSError, json.JSONDecodeError):
            return
        if config.get("sandbox"):
            os.environ["AWS_ENV"] = "SANDBOX"
        return


_apply_sandbox_env_from_argv()

from singer_sdk import Stream, Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_amazon_seller.streams import (
    AmazonSellerStream,
    MarketplacesStream,
    OrderAddress,
    OrderBuyerInfo,
    OrderFinancialEvents,
    OrderItemsStream,
    OrdersStream,
    ReportsStream,
    WarehouseInventory,
    ProductsIventoryStream,
    ProductDetails,
    VendorPurchaseOrdersStream,
    VendorFulfilmentPurchaseOrdersStream,
    VendorFulfilmentCustomerInvoicesStream,
    AFNInventoryCountryStream,
    SalesTrafficReportStream,
    FBAInventoryLedgerDetailedReportStream,
    FBACustomerShipmentSalesReportStream,
    ProductDetailsV2Stream,
    AWDInventoryStream,
    AccountStream
)

STREAM_TYPES = [
    OrdersStream,
    OrderItemsStream,
    MarketplacesStream,
    OrderBuyerInfo,
    OrderAddress,
    OrderFinancialEvents,
    ReportsStream,
    WarehouseInventory,
    ProductsIventoryStream,
    ProductDetails,
    VendorPurchaseOrdersStream,
    VendorFulfilmentPurchaseOrdersStream,
    VendorFulfilmentCustomerInvoicesStream,
    AFNInventoryCountryStream,
    SalesTrafficReportStream,
    FBAInventoryLedgerDetailedReportStream,
    FBACustomerShipmentSalesReportStream,
    ProductDetailsV2Stream,
    AWDInventoryStream,
    AccountStream
]


class TapAmazonSeller(Tap):
    """Amazon-Seller tap class."""

    name = "tap-amazon-seller"

    # TODO: Update this section with the actual config values you expect:
    config_jsonschema = th.PropertiesList(
        th.Property("lwa_client_id", th.StringType, required=True),
        th.Property("client_secret", th.StringType, required=True),
        th.Property("aws_access_key", th.StringType, required=False),
        th.Property("aws_secret_key", th.StringType, required=False),
        th.Property("role_arn", th.StringType, required=False),
        th.Property("refresh_token", th.StringType, required=True),
        th.Property("sandbox", th.BooleanType, default=False),
        th.Property(
            "report_types",
            th.CustomType({"type": ["array", "string"]}),
            default=["GET_LEDGER_DETAIL_VIEW_DATA", "GET_MERCHANT_LISTINGS_ALL_DATA"],
        ),
        th.Property(
            "processing_status",
            th.CustomType({"type": ["array", "string"]}),
            default=["IN_QUEUE", "IN_PROGRESS"],
        ),
        th.Property(
            "marketplaces",
            th.CustomType({"type": ["array", "string"]}),
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]


if __name__ == "__main__":
    TapAmazonSeller.cli()
