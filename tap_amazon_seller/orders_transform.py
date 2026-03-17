"""Transforms orders from the v2026-01-01 API format to the legacy v0 schema."""
from types import SimpleNamespace

_FULFILLMENT_SERVICE_LEVEL_V2_TO_V0 = {
    parts: "".join(w.capitalize() for w in parts.split("_"))
    for parts in [
        "EXPEDITED", "STANDARD", "FREE_ECONOMY", "NEXT_DAY",
        "SAME_DAY", "SECOND_DAY", "SCHEDULED",
    ]
}

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


def transform_order_v2_to_v0(order: dict) -> dict:
    """Transform an order from the v2026-01-01 format to the v0 schema."""
    fulfillment = order.get("fulfillment", {})
    sales_channel = order.get("salesChannel", {})
    proceeds = order.get("proceeds", {})
    grand_total = proceeds.get("grandTotal", {})
    ship_by = fulfillment.get("shipByWindow", {})
    deliver_by = fulfillment.get("deliverByWindow", {})

    v0_status = FULFILLMENT_STATUS_V2_TO_V0.get(
        fulfillment.get("fulfillmentStatus", ""),
        fulfillment.get("fulfillmentStatus"),
    )
    v0_channel = FULFILLMENT_CHANNEL_V2_TO_V0.get(
        fulfillment.get("fulfilledBy", ""),
        fulfillment.get("fulfilledBy"),
    )

    seller_order_id = None
    for alias in order.get("orderAliases", []):
        if alias.get("aliasType") == "SELLER_ORDER_ID":
            seller_order_id = alias.get("aliasId")
            break

    programs = order.get("programs") or []

    is_replacement = False
    replaced_order_id = None
    for assoc in order.get("associatedOrders") or []:
        if assoc.get("associationType") in (
            "REPLACEMENT_ORIGINAL_ID", "EXCHANGE_ORIGINAL_ID"
        ):
            is_replacement = True
            replaced_order_id = assoc.get("orderId")
            break

    order_type = "StandardOrder"
    if "PREORDER" in programs:
        order_type = "Preorder"

    raw_service_level = fulfillment.get("fulfillmentServiceLevel")
    service_level = _FULFILLMENT_SERVICE_LEVEL_V2_TO_V0.get(
        raw_service_level,
        "".join(w.capitalize() for w in raw_service_level.split("_")) if raw_service_level else None,
    )

    order_total = None
    if grand_total:
        order_total = {
            "CurrencyCode": grand_total.get("currencyCode"),
            "Amount": grand_total.get("amount"),
        }

    v0_order = {
        "AmazonOrderId": order.get("orderId"),
        "SellerOrderId": seller_order_id,
        "PurchaseDate": order.get("createdTime"),
        "LastUpdateDate": order.get("lastUpdatedTime"),
        "OrderStatus": v0_status,
        "FulfillmentChannel": v0_channel,
        "SalesChannel": sales_channel.get("marketplaceName"),
        "ShipServiceLevel": None,
        "OrderChannel": None,
        "OrderTotal": order_total,
        "NumberOfItemsShipped": None,
        "NumberOfItemsUnshipped": None,
        "PaymentMethod": None,
        "PaymentMethodDetails": None,
        "PaymentExecutionDetail": None,
        "BuyerTaxInformation": None,
        "MarketplaceTaxInfo": None,
        "ShippingAddress": None,
        "BuyerInfo": None,
        "IsReplacementOrder": is_replacement,
        "ReplacedOrderId": replaced_order_id,
        "MarketplaceId": sales_channel.get("marketplaceId"),
        "SellerDisplayName": None,
        "EasyShipShipmentStatus": None,
        "CbaDisplayableShippingLabel": None,
        "ShipmentServiceLevelCategory": service_level,
        "BuyerInvoicePreference": None,
        "OrderType": order_type,
        "EarliestShipDate": ship_by.get("earliestDateTime"),
        "LatestShipDate": ship_by.get("latestDateTime"),
        "EarliestDeliveryDate": deliver_by.get("earliestDateTime"),
        "PromiseResponseDueDate": None,
        "LatestDeliveryDate": deliver_by.get("latestDateTime"),
        "IsBusinessOrder": "AMAZON_BUSINESS" in programs,
        "IsEstimatedShipDateSet": None,
        "IsPrime": "PRIME" in programs,
        "IsGlobalExpressEnabled": None,
        "HasRegulatedItems": None,
        "IsPremiumOrder": "PREMIUM" in programs,
        "IsSoldByAB": None,
        "IsIBA": None,
        "IsISPU": "IN_STORE_PICK_UP" in programs,
        "DefaultShipFromLocationAddress": None,
        "FulfillmentInstruction": None,
        "AutomatedShippingSettings": None,
    }

    return {k: v for k, v in v0_order.items() if v is not None}


def _extract_breakdown(breakdowns, type_, subtype=None, default_currency=None):
    """Return the monetary dict {CurrencyCode, Amount} for a given breakdown type/subtype.

    If not found and default_currency is provided, returns a zero-amount entry —
    v2 omits zero-value breakdowns, so absence is equivalent to 0.00.
    """
    for bd in breakdowns:
        if bd.get("type") != type_:
            continue
        if subtype is None:
            subtotal = bd.get("subtotal")
            if subtotal:
                return {"CurrencyCode": subtotal.get("currencyCode"), "Amount": subtotal.get("amount")}
        else:
            for detail in bd.get("detailedBreakdowns", []):
                if detail.get("subtype") == subtype:
                    val = detail.get("value", {})
                    return {"CurrencyCode": val.get("currencyCode"), "Amount": val.get("amount")}
    if default_currency:
        return {"CurrencyCode": default_currency, "Amount": "0.00"}
    return None


def _transform_order_item_v2_to_v0(item: dict) -> dict:
    product = item.get("product", {})
    condition = product.get("condition", {})
    fulfillment = item.get("fulfillment", {})
    packing = fulfillment.get("packing", {})
    shipping = fulfillment.get("shipping", {})
    scheduled = shipping.get("scheduledDeliveryWindow", {})
    intl = shipping.get("internationalShipping", {})
    proceeds = item.get("proceeds", {})
    breakdowns = proceeds.get("breakdowns", [])
    currency = (proceeds.get("proceedsTotal") or {}).get("currencyCode")
    promotion = item.get("promotion", {})
    cancellation = item.get("cancellation", {})
    programs = item.get("programs") or []

    promotion_ids = [
        bd.get("promotionId")
        for bd in (promotion.get("breakdowns") or [])
        if bd.get("promotionId")
    ] or None

    buyer_requested_cancel = None
    if cancellation:
        buyer_requested_cancel = {
            "IsBuyerRequestedCancel": cancellation.get("requester") == "BUYER",
            "BuyerCancelReason": cancellation.get("cancelReason"),
        }

    buyer_info = {}
    customization = product.get("customization", {})
    if customization:
        buyer_info["BuyerCustomizedInfo"] = {"CustomizedURL": customization.get("customizedUrl")}
    gift_message = packing.get("giftMessage")
    gift_wrap_level = packing.get("giftWrapLevel")
    if gift_message:
        buyer_info["GiftMessageText"] = gift_message
    if gift_wrap_level:
        buyer_info["GiftWrapLevel"] = gift_wrap_level

    gift_option = packing.get("giftOption")
    is_gift = str(bool(gift_option)).lower() if gift_option is not None else "false"

    item_programs = [p for p in programs if p not in ("TRANSPARENCY",)]
    amazon_programs = {"Programs": item_programs} if item_programs else None

    v0_item = {
        "ASIN": product.get("asin"),
        "SellerSKU": product.get("sellerSku"),
        "OrderItemId": item.get("orderItemId"),
        "Title": product.get("title"),
        "QuantityOrdered": item.get("quantityOrdered"),
        "QuantityShipped": fulfillment.get("quantityFulfilled"),
        "ConditionId": condition.get("conditionType"),
        "ConditionSubtypeId": condition.get("conditionSubtype"),
        "ConditionNote": condition.get("conditionNote"),
        "PriceDesignation": product.get("price", {}).get("priceDesignation"),
        "IsGift": is_gift,
        "ScheduledDeliveryStartDate": scheduled.get("earliestDateTime"),
        "ScheduledDeliveryEndDate": scheduled.get("latestDateTime"),
        "IossNumber": intl.get("iossNumber"),
        "ItemPrice": _extract_breakdown(breakdowns, "ITEM"),
        "ShippingPrice": _extract_breakdown(breakdowns, "SHIPPING"),
        "ItemTax": _extract_breakdown(breakdowns, "TAX", "ITEM"),
        "ShippingTax": _extract_breakdown(breakdowns, "TAX", "SHIPPING"),
        "PromotionDiscount": _extract_breakdown(breakdowns, "DISCOUNT", "ITEM"),
        "PromotionDiscountTax": _extract_breakdown(breakdowns, "TAX", "DISCOUNT"),
        "ShippingDiscount": _extract_breakdown(breakdowns, "DISCOUNT", "SHIPPING"),
        "ShippingDiscountTax": _extract_breakdown(breakdowns, "TAX", "DISCOUNT"),
        "CODFee": _extract_breakdown(breakdowns, "COD_FEE"),
        "CODFeeDiscount": _extract_breakdown(breakdowns, "DISCOUNT", "COD_FEE"),
        "PromotionIds": promotion_ids,
        "IsTransparency": "TRANSPARENCY" in programs,
        "AmazonPrograms": amazon_programs,
        "BuyerInfo": buyer_info if buyer_info else {},
        "BuyerRequestedCancel": buyer_requested_cancel,
    }

    return {k: v for k, v in v0_item.items() if v is not None}


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
    buyer = order.get("buyer", {})
    result = {
        "AmazonOrderId": order.get("orderId"),
        "BuyerEmail": buyer.get("buyerEmail"),
        "BuyerName": buyer.get("buyerName"),
        "PurchaseOrderNumber": buyer.get("buyerPurchaseOrderNumber"),
    }
    return {k: v for k, v in result.items() if v is not None}


def transform_order_address_v2_to_v0(order: dict) -> dict:
    """Transform a v2026-01-01 order into the v0 orderaddress payload shape."""
    recipient = order.get("recipient", {})
    addr = recipient.get("deliveryAddress", {})

    shipping_address = {
        "Name": addr.get("name"),
        "AddressLine1": addr.get("addressLine1"),
        "AddressLine2": addr.get("addressLine2"),
        "AddressLine3": addr.get("addressLine3"),
        "City": addr.get("city"),
        "County": addr.get("county"),
        "District": addr.get("district"),
        "StateOrRegion": addr.get("stateOrRegion"),
        "Municipality": addr.get("municipality"),
        "PostalCode": addr.get("postalCode"),
        "CountryCode": addr.get("countryCode"),
        "Phone": addr.get("phone"),
        "AddressType": addr.get("addressType"),
    }
    shipping_address = {k: v for k, v in shipping_address.items() if v is not None}

    result = {"AmazonOrderId": order.get("orderId")}
    if shipping_address:
        result["ShippingAddress"] = shipping_address
    return result
