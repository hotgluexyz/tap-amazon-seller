"""Transforms orders from the v2026-01-01 API format to the legacy v0 schema."""
import re
from types import SimpleNamespace

FULFILLMENT_STATUS_V2_TO_V0 = {
    "UNSHIPPED": "Unshipped",
    "PARTIALLY_SHIPPED": "PartiallyShipped",
    "SHIPPED": "Shipped",
    "CANCELLED": "Canceled",
    "PENDING": "Pending",
    "UNFULFILLABLE": "Unfulfillable",
    "PENDING_AVAILABILITY": "PendingAvailability",
    "INVOICE_UNCONFIRMED": "InvoiceUnconfirmed",
}

FULFILLMENT_CHANNEL_V2_TO_V0 = {
    "MERCHANT": "MFN",
    "AMAZON": "AFN",
}

SANDBOX_V2 = SimpleNamespace(
    marketplace="JP",
    marketplace_id="A1VC38T7YXB528",
    created_after="2024-12-25T00:00:00Z",
    included_data=[
        "BUYER", "RECIPIENT", "PROCEEDS", "EXPENSE",
        "PROMOTION", "CANCELLATION", "FULFILLMENT", "PACKAGES",
    ],
)

# Child stream context key: raw v2026-01-01 order from parent search_orders (not emitted to Singer).
ORDER_V2_CONTEXT_KEY = "order_v2"

# includedData the orders stream needs for its own row: fulfillment and proceeds,
# buyer and recipient for BuyerInfo/ShippingAddress, packages for the ship-from address.
SEARCH_INCLUDED_DATA_BASE = ["BUYER", "FULFILLMENT", "PACKAGES", "PROCEEDS", "RECIPIENT"]

# Additional search_orders includedData tokens when a child stream is selected in the catalog.
SEARCH_INCLUDED_DATA_BY_CHILD_STREAM = {
    "orderitems": ["EXPENSE", "PROMOTION", "CANCELLATION"],
}

# v0 ShippingAddress on orders only exposes the unrestricted address fields.
ORDER_SHIPPING_ADDRESS_FIELDS = ("City", "StateOrRegion", "PostalCode", "CountryCode")

# The only value v0 reported in an item's AmazonPrograms.Programs.
V0_AMAZON_PROGRAMS = ("SUBSCRIBE_AND_SAVE",)

# Enum words whose v0 spelling is not simply capitalized.
V0_ENUM_WORD_OVERRIDES = {"OEM": "OEM"}

FRACTIONAL_SECONDS = re.compile(r"\.\d+(?=Z|[+-]\d{2}:?\d{2}$)")


def build_search_included_data(selected_child_stream_names):
    """Build the includedData list for search_orders from selected order child streams."""
    included = set(SEARCH_INCLUDED_DATA_BASE)
    for name in selected_child_stream_names:
        for token in SEARCH_INCLUDED_DATA_BY_CHILD_STREAM.get(name, ()):
            included.add(token)
    return sorted(included)


class OrderPayload(dict):
    """Raw v2026-01-01 order whose repr hides the contents.

    The Singer SDK logs the child context when a child stream starts, and the
    order carries buyer and recipient data that must stay out of the logs.
    """

    def __repr__(self):
        """Show only the order id."""
        return f"<order {self.get('orderId')}>"


def order_v2_from_child_context(context):
    """Return the v2026-01-01 order dict passed from OrdersStream via child context."""
    order = context.get(ORDER_V2_CONTEXT_KEY) if context else None
    if order is None:
        raise RuntimeError(
            f"Child stream context is missing {ORDER_V2_CONTEXT_KEY}; "
            "order child streams must run as children of the orders stream."
        )
    return order


def _without_nones(values):
    """Return a copy of the dict without the keys whose value is None."""
    return {key: value for key, value in values.items() if value is not None}


def _money(value):
    """Convert a v2026-01-01 money object to the v0 {CurrencyCode, Amount} shape."""
    return {"CurrencyCode": value.get("currencyCode"), "Amount": value.get("amount")}


def _seller_order_id(order):
    """Return the seller's own order id from the order aliases, if any."""
    for alias in order.get("orderAliases", []):
        if alias.get("aliasType") == "SELLER_ORDER_ID":
            return alias.get("aliasId")
    return None


def _replaced_order_id(order):
    """Return the original order id when this order replaces or exchanges another."""
    for assoc in order.get("associatedOrders") or []:
        if assoc.get("associationType") in (
            "REPLACEMENT_ORIGINAL_ID",
            "EXCHANGE_ORIGINAL_ID",
        ):
            return assoc.get("orderId")
    return None


def _enum_to_words(raw, joiner):
    """Convert a v2 enum such as VERY_GOOD to the v0 spelling (Very Good, VeryGood)."""
    if not raw:
        return None
    return joiner.join(
        V0_ENUM_WORD_OVERRIDES.get(word, word.capitalize()) for word in raw.split("_")
    )


def _service_level_category(fulfillment):
    """Convert e.g. NEXT_DAY to NextDay."""
    return _enum_to_words(fulfillment.get("fulfillmentServiceLevel"), "")


def _to_seconds(timestamp):
    """Drop fractional seconds, since v0 timestamps had none and the ETL parses them strictly."""
    return FRACTIONAL_SECONDS.sub("", timestamp) if timestamp else timestamp


def _address_v2_to_v0(address):
    """Convert a v2026-01-01 address to the v0 Address shape."""
    return _without_nones(
        {
            "Name": address.get("name"),
            "AddressLine1": address.get("addressLine1"),
            "AddressLine2": address.get("addressLine2"),
            "AddressLine3": address.get("addressLine3"),
            "City": address.get("city"),
            "County": address.get("county"),
            "District": address.get("district"),
            "StateOrRegion": address.get("stateOrRegion"),
            "Municipality": address.get("municipality"),
            "PostalCode": address.get("postalCode"),
            "CountryCode": address.get("countryCode"),
            "Phone": address.get("phone"),
            "AddressType": address.get("addressType"),
        }
    )


def _buyer_info(buyer):
    """Convert a v2026-01-01 buyer to the v0 BuyerInfo shape."""
    return _without_nones(
        {
            "BuyerEmail": buyer.get("buyerEmail"),
            "BuyerName": buyer.get("buyerName"),
            "PurchaseOrderNumber": buyer.get("buyerPurchaseOrderNumber"),
        }
    )


def _delivery_address(order):
    """Return the recipient's delivery address in the v0 shape, empty without one."""
    recipient = order.get("recipient") or {}
    return _address_v2_to_v0(recipient.get("deliveryAddress") or {})


def _order_shipping_address(order):
    """Return the part of the delivery address that v0 exposed on the order row."""
    address = _delivery_address(order)
    return {
        key: address[key] for key in ORDER_SHIPPING_ADDRESS_FIELDS if key in address
    }


def _ship_from_address(order):
    """Return the first package's ship-from address in the v0 shape, if any."""
    packages = order.get("packages") or []
    address = packages[0].get("shipFromAddress") if packages else None
    return _address_v2_to_v0(address) if address else None


def _item_quantity_total(order, field):
    """Sum a fulfillment quantity over the order items, or None without items."""
    items = order.get("orderItems")
    if not items:
        return None
    return sum(item.get("fulfillment", {}).get(field, 0) for item in items)


def transform_order_v2_to_v0(order: dict) -> dict:
    """Transform an order from the v2026-01-01 format to the v0 schema.

    v0 fields with no v2026-01-01 equivalent are omitted.
    """
    fulfillment = order.get("fulfillment", {})
    sales_channel = order.get("salesChannel", {})
    grand_total = order.get("proceeds", {}).get("grandTotal")
    ship_by = fulfillment.get("shipByWindow", {})
    deliver_by = fulfillment.get("deliverByWindow", {})
    programs = order.get("programs") or []
    status = fulfillment.get("fulfillmentStatus")
    fulfilled_by = fulfillment.get("fulfilledBy")
    replaced_order_id = _replaced_order_id(order)

    return _without_nones(
        {
            "AmazonOrderId": order.get("orderId"),
            "SellerOrderId": _seller_order_id(order),
            "PurchaseDate": _to_seconds(order.get("createdTime")),
            "LastUpdateDate": _to_seconds(order.get("lastUpdatedTime")),
            "OrderStatus": FULFILLMENT_STATUS_V2_TO_V0.get(status, status),
            "FulfillmentChannel": FULFILLMENT_CHANNEL_V2_TO_V0.get(
                fulfilled_by, fulfilled_by
            ),
            "SalesChannel": sales_channel.get("marketplaceName"),
            "OrderTotal": _money(grand_total) if grand_total else None,
            "NumberOfItemsShipped": _item_quantity_total(order, "quantityFulfilled"),
            "NumberOfItemsUnshipped": _item_quantity_total(order, "quantityUnfulfilled"),
            "BuyerInfo": _buyer_info(order.get("buyer") or {}),
            "ShippingAddress": _order_shipping_address(order) or None,
            "DefaultShipFromLocationAddress": _ship_from_address(order),
            "IsReplacementOrder": replaced_order_id is not None,
            "ReplacedOrderId": replaced_order_id,
            "MarketplaceId": sales_channel.get("marketplaceId"),
            "ShipmentServiceLevelCategory": _service_level_category(fulfillment),
            "OrderType": "Preorder" if "PREORDER" in programs else "StandardOrder",
            "EarliestShipDate": _to_seconds(ship_by.get("earliestDateTime")),
            "LatestShipDate": _to_seconds(ship_by.get("latestDateTime")),
            "EarliestDeliveryDate": _to_seconds(deliver_by.get("earliestDateTime")),
            "LatestDeliveryDate": _to_seconds(deliver_by.get("latestDateTime")),
            "IsBusinessOrder": "AMAZON_BUSINESS" in programs,
            "IsPrime": "PRIME" in programs,
            "IsPremiumOrder": "PREMIUM" in programs,
        }
    )


def _extract_breakdown(breakdowns, type_, subtype=None, default_currency=None):
    """Return the v0 money dict for a breakdown type and optional subtype.

    When it is not found, returns a zero amount in default_currency. v2 omits
    zero-value breakdowns, so absence is the same as 0.00.
    """
    for breakdown in breakdowns:
        if breakdown.get("type") != type_:
            continue
        if subtype is None:
            if breakdown.get("subtotal"):
                return _money(breakdown["subtotal"])
        else:
            for detail in breakdown.get("detailedBreakdowns", []):
                if detail.get("subtype") == subtype:
                    return _money(detail.get("value", {}))
    if default_currency:
        return {"CurrencyCode": default_currency, "Amount": "0.00"}
    return None


def _points_granted(expense):
    """Convert the expense points granted to the v0 PointsGranted shape."""
    points = (expense.get("pointsCost") or {}).get("pointsGranted")
    if not points:
        return None
    return {
        "PointsNumber": points.get("pointsNumber"),
        "PointsMonetaryValue": _money(points.get("pointsMonetaryValue") or {}),
    }


def _amazon_programs(programs):
    """Return the v0 AmazonPrograms object for the programs v0 reported, if any."""
    v0_programs = [program for program in programs if program in V0_AMAZON_PROGRAMS]
    return {"Programs": v0_programs} if v0_programs else None


def _buyer_requested_cancel(cancellation):
    """Convert an item cancellation to the v0 BuyerRequestedCancel shape."""
    if not cancellation:
        return None
    return {
        "IsBuyerRequestedCancel": cancellation.get("requester") == "BUYER",
        "BuyerCancelReason": cancellation.get("cancelReason"),
    }


def _item_buyer_info(product, packing):
    """Build the v0 item BuyerInfo from customization and gift options."""
    customization = product.get("customization")
    return _without_nones(
        {
            "BuyerCustomizedInfo": (
                {"CustomizedURL": customization.get("customizedUrl")}
                if customization
                else None
            ),
            "GiftMessageText": packing.get("giftMessage") or None,
            "GiftWrapLevel": packing.get("giftWrapLevel") or None,
        }
    )


def _transform_order_item_v2_to_v0(item: dict) -> dict:
    """Transform one v2026-01-01 order item into a v0 order item."""
    product = item.get("product", {})
    condition = product.get("condition", {})
    fulfillment = item.get("fulfillment", {})
    packing = fulfillment.get("packing", {})
    shipping = fulfillment.get("shipping", {})
    scheduled = shipping.get("scheduledDeliveryWindow", {})
    proceeds = item.get("proceeds", {})
    breakdowns = proceeds.get("breakdowns", [])
    currency = (proceeds.get("proceedsTotal") or {}).get("currencyCode")
    programs = item.get("programs") or []
    promotion_ids = [
        breakdown["promotionId"]
        for breakdown in item.get("promotion", {}).get("breakdowns") or []
        if breakdown.get("promotionId")
    ]

    def breakdown(type_, subtype=None):
        """Money for this item's breakdown, zero when v2 omitted it."""
        return _extract_breakdown(
            breakdowns, type_, subtype, default_currency=currency
        )

    return _without_nones(
        {
            "ASIN": product.get("asin"),
            "SellerSKU": product.get("sellerSku"),
            "OrderItemId": item.get("orderItemId"),
            "Title": product.get("title"),
            "QuantityOrdered": item.get("quantityOrdered"),
            "QuantityShipped": fulfillment.get("quantityFulfilled"),
            "ConditionId": _enum_to_words(condition.get("conditionType"), " "),
            "ConditionSubtypeId": _enum_to_words(condition.get("conditionSubtype"), " "),
            "ConditionNote": condition.get("conditionNote"),
            "PriceDesignation": product.get("price", {}).get("priceDesignation"),
            "IsGift": str(bool(packing.get("giftOption"))).lower(),
            "ScheduledDeliveryStartDate": scheduled.get("earliestDateTime"),
            "ScheduledDeliveryEndDate": scheduled.get("latestDateTime"),
            "IossNumber": shipping.get("internationalShipping", {}).get("iossNumber"),
            "ItemPrice": breakdown("ITEM"),
            "ShippingPrice": breakdown("SHIPPING"),
            "ItemTax": breakdown("TAX", "ITEM"),
            "ShippingTax": breakdown("TAX", "SHIPPING"),
            "PromotionDiscount": breakdown("DISCOUNT", "ITEM"),
            "PromotionDiscountTax": breakdown("TAX", "DISCOUNT"),
            "ShippingDiscount": breakdown("DISCOUNT", "SHIPPING"),
            "CODFee": breakdown("COD_FEE"),
            "CODFeeDiscount": breakdown("DISCOUNT", "COD_FEE"),
            "PromotionIds": promotion_ids or None,
            "AmazonPrograms": _amazon_programs(programs),
            "PointsGranted": _points_granted(item.get("expense", {})),
            "IsTransparency": "TRANSPARENCY" in programs,
            "BuyerInfo": _item_buyer_info(product, packing) or None,
            "BuyerRequestedCancel": _buyer_requested_cancel(item.get("cancellation")),
        }
    )


def transform_order_items_v2_to_v0(order: dict) -> dict:
    """Transform a v2026-01-01 order into the v0 orderitems payload shape."""
    return {
        "AmazonOrderId": order.get("orderId"),
        "OrderItems": [
            _transform_order_item_v2_to_v0(item)
            for item in order.get("orderItems", [])
        ],
    }


def transform_order_buyer_info_v2_to_v0(order: dict) -> dict:
    """Transform a v2026-01-01 order into the v0 orderbuyerinfo payload shape."""
    return {
        "AmazonOrderId": order.get("orderId"),
        **_buyer_info(order.get("buyer") or {}),
    }


def transform_order_address_v2_to_v0(order: dict) -> dict:
    """Transform a v2026-01-01 order into the v0 orderaddress payload shape."""
    shipping_address = _delivery_address(order)
    result = {"AmazonOrderId": order.get("orderId")}
    if shipping_address:
        result["ShippingAddress"] = shipping_address
    return result
