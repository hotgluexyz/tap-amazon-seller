"""Unit tests for the Orders API v2026-01-01 to v0 transforms."""

import pytest

from tap_amazon_seller.orders_transform import (
    ORDER_V2_CONTEXT_KEY,
    build_search_included_data,
    order_v2_from_child_context,
    transform_order_address_v2_to_v0,
    transform_order_buyer_info_v2_to_v0,
    transform_order_items_v2_to_v0,
    transform_order_v2_to_v0,
)


def money(amount, currency="USD"):
    return {"amount": amount, "currencyCode": currency}


@pytest.fixture
def order_v2():
    """Synthetic v2026-01-01 order covering every mapped field."""
    return {
        "orderId": "111-0000000-0000001",
        "orderAliases": [
            {"aliasType": "OTHER", "aliasId": "other"},
            {"aliasType": "SELLER_ORDER_ID", "aliasId": "seller-1"},
        ],
        "createdTime": "2026-10-01T10:00:00.123Z",
        "lastUpdatedTime": "2026-10-02T10:00:00.456Z",
        "programs": ["PRIME", "PREORDER"],
        "associatedOrders": [
            {"associationType": "REPLACEMENT_ORIGINAL_ID", "orderId": "111-orig"}
        ],
        "salesChannel": {"marketplaceId": "ATVPDKIKX0DER", "marketplaceName": "Amazon.com"},
        "proceeds": {"grandTotal": money("20.97")},
        "fulfillment": {
            "fulfillmentStatus": "PARTIALLY_SHIPPED",
            "fulfilledBy": "MERCHANT",
            "fulfillmentServiceLevel": "NEXT_DAY",
            "shipByWindow": {
                "earliestDateTime": "2026-10-03T00:00:00Z",
                "latestDateTime": "2026-10-04T00:00:00Z",
            },
            "deliverByWindow": {
                "earliestDateTime": "2026-10-05T00:00:00Z",
                "latestDateTime": "2026-10-06T00:00:00Z",
            },
        },
        "buyer": {
            "buyerEmail": "buyer@example.com",
            "buyerName": "Jane Buyer",
            "buyerPurchaseOrderNumber": "PO-1",
        },
        "recipient": {
            "deliveryAddress": {
                "name": "Jane Buyer",
                "addressLine1": "1 Main St",
                "city": "Springfield",
                "stateOrRegion": "CA",
                "postalCode": "12345",
                "countryCode": "US",
            }
        },
        "payment": {
            "paymentExecutions": [{"paymentMethod": "Standard"}, {"paymentMethod": "COD"}]
        },
        "packages": [
            {
                "shipFromAddress": {
                    "name": "Warehouse",
                    "addressLine1": "9 Dock Rd",
                    "city": "Wilmot",
                    "stateOrRegion": "WI",
                    "postalCode": "53192",
                    "countryCode": "US",
                }
            }
        ],
        "orderItems": [
            {
                "orderItemId": "item-1",
                "quantityOrdered": 2,
                "programs": ["TRANSPARENCY", "SUBSCRIBE_AND_SAVE"],
                "product": {
                    "asin": "B000TEST",
                    "sellerSku": "SKU-1",
                    "title": "Widget",
                    "condition": {"conditionType": "USED", "conditionSubtype": "VERY_GOOD"},
                    "price": {"priceDesignation": "BUSINESS_PRICE"},
                    "customization": {"customizedUrl": "https://example.com/c"},
                },
                "fulfillment": {
                    "quantityFulfilled": 1,
                    "quantityUnfulfilled": 1,
                    "packing": {"giftOption": True, "giftMessage": "hi", "giftWrapLevel": "L1"},
                    "shipping": {
                        "scheduledDeliveryWindow": {
                            "earliestDateTime": "2026-10-05T00:00:00Z",
                            "latestDateTime": "2026-10-06T00:00:00Z",
                        },
                        "internationalShipping": {"iossNumber": "IM123"},
                    },
                },
                "proceeds": {
                    "proceedsTotal": money("20.97"),
                    "breakdowns": [
                        {"type": "ITEM", "subtotal": money("15.00")},
                        {"type": "SHIPPING", "subtotal": money("4.00")},
                        {
                            "type": "TAX",
                            "detailedBreakdowns": [
                                {"subtype": "ITEM", "value": money("1.50")},
                                {"subtype": "SHIPPING", "value": money("0.47")},
                            ],
                        },
                        {
                            "type": "DISCOUNT",
                            "detailedBreakdowns": [
                                {"subtype": "ITEM", "value": money("-1.00")},
                            ],
                        },
                    ],
                },
                "expense": {
                    "pointsCost": {
                        "pointsGranted": {
                            "pointsNumber": 10,
                            "pointsMonetaryValue": money("0.10"),
                        }
                    }
                },
                "promotion": {"breakdowns": [{"promotionId": "PROMO-1"}, {}]},
                "cancellation": {"requester": "BUYER", "cancelReason": "changed mind"},
            }
        ],
    }


def test_order_header_is_mapped_to_v0(order_v2):
    assert transform_order_v2_to_v0(order_v2) == {
        "AmazonOrderId": "111-0000000-0000001",
        "SellerOrderId": "seller-1",
        "PurchaseDate": "2026-10-01T10:00:00Z",
        "LastUpdateDate": "2026-10-02T10:00:00Z",
        "OrderStatus": "PartiallyShipped",
        "FulfillmentChannel": "MFN",
        "SalesChannel": "Amazon.com",
        "OrderTotal": {"CurrencyCode": "USD", "Amount": "20.97"},
        "NumberOfItemsShipped": 1,
        "NumberOfItemsUnshipped": 1,
        "PaymentMethod": "Standard",
        "PaymentMethodDetails": ["Standard", "COD"],
        "BuyerInfo": {
            "BuyerEmail": "buyer@example.com",
            "BuyerName": "Jane Buyer",
            "PurchaseOrderNumber": "PO-1",
        },
        "ShippingAddress": {
            "City": "Springfield",
            "StateOrRegion": "CA",
            "PostalCode": "12345",
            "CountryCode": "US",
        },
        "DefaultShipFromLocationAddress": {
            "Name": "Warehouse",
            "AddressLine1": "9 Dock Rd",
            "City": "Wilmot",
            "StateOrRegion": "WI",
            "PostalCode": "53192",
            "CountryCode": "US",
        },
        "IsReplacementOrder": True,
        "ReplacedOrderId": "111-orig",
        "MarketplaceId": "ATVPDKIKX0DER",
        "ShipmentServiceLevelCategory": "NextDay",
        "OrderType": "Preorder",
        "EarliestShipDate": "2026-10-03T00:00:00Z",
        "LatestShipDate": "2026-10-04T00:00:00Z",
        "EarliestDeliveryDate": "2026-10-05T00:00:00Z",
        "LatestDeliveryDate": "2026-10-06T00:00:00Z",
        "IsBusinessOrder": False,
        "IsPrime": True,
        "IsPremiumOrder": False,
    }


def test_minimal_order_defaults(order_v2):
    result = transform_order_v2_to_v0({"orderId": "1"})
    assert result == {
        "AmazonOrderId": "1",
        "BuyerInfo": {},
        "IsReplacementOrder": False,
        "OrderType": "StandardOrder",
        "IsBusinessOrder": False,
        "IsPrime": False,
        "IsPremiumOrder": False,
    }


def test_order_header_timestamps_drop_fractional_seconds(order_v2):
    result = transform_order_v2_to_v0(order_v2)
    assert result["PurchaseDate"] == "2026-10-01T10:00:00Z"
    assert result["LastUpdateDate"] == "2026-10-02T10:00:00Z"


def test_order_header_without_buyer_still_has_empty_buyer_info():
    assert transform_order_v2_to_v0({"orderId": "1", "buyer": {"buyerCompanyName": "Acme"}})[
        "BuyerInfo"
    ] == {}


def test_order_shipping_address_only_exposes_unrestricted_fields(order_v2):
    order_v2["recipient"]["deliveryAddress"]["phone"] = "555"
    result = transform_order_v2_to_v0(order_v2)
    assert set(result["ShippingAddress"]) == {
        "City",
        "StateOrRegion",
        "PostalCode",
        "CountryCode",
    }


def test_unknown_status_and_channel_pass_through():
    order = {"orderId": "1", "fulfillment": {"fulfillmentStatus": "NEW", "fulfilledBy": "X"}}
    result = transform_order_v2_to_v0(order)
    assert result["OrderStatus"] == "NEW"
    assert result["FulfillmentChannel"] == "X"


def test_order_items_are_mapped_to_v0(order_v2):
    result = transform_order_items_v2_to_v0(order_v2)
    assert result["AmazonOrderId"] == "111-0000000-0000001"
    (item,) = result["OrderItems"]
    assert item == {
        "ASIN": "B000TEST",
        "SellerSKU": "SKU-1",
        "OrderItemId": "item-1",
        "Title": "Widget",
        "QuantityOrdered": 2,
        "QuantityShipped": 1,
        "ConditionId": "Used",
        "ConditionSubtypeId": "Very Good",
        "PriceDesignation": "BUSINESS_PRICE",
        "IsGift": "true",
        "ScheduledDeliveryStartDate": "2026-10-05T00:00:00Z",
        "ScheduledDeliveryEndDate": "2026-10-06T00:00:00Z",
        "IossNumber": "IM123",
        "ItemPrice": {"CurrencyCode": "USD", "Amount": "15.00"},
        "ShippingPrice": {"CurrencyCode": "USD", "Amount": "4.00"},
        "ItemTax": {"CurrencyCode": "USD", "Amount": "1.50"},
        "ShippingTax": {"CurrencyCode": "USD", "Amount": "0.47"},
        "PromotionDiscount": {"CurrencyCode": "USD", "Amount": "-1.00"},
        "PromotionDiscountTax": {"CurrencyCode": "USD", "Amount": "0.00"},
        "ShippingDiscount": {"CurrencyCode": "USD", "Amount": "0.00"},
        "CODFee": {"CurrencyCode": "USD", "Amount": "0.00"},
        "CODFeeDiscount": {"CurrencyCode": "USD", "Amount": "0.00"},
        "PromotionIds": ["PROMO-1"],
        "AmazonPrograms": {"Programs": ["SUBSCRIBE_AND_SAVE"]},
        "PointsGranted": {
            "PointsNumber": 10,
            "PointsMonetaryValue": {"CurrencyCode": "USD", "Amount": "0.10"},
        },
        "IsTransparency": True,
        "BuyerInfo": {
            "BuyerCustomizedInfo": {"CustomizedURL": "https://example.com/c"},
            "GiftMessageText": "hi",
            "GiftWrapLevel": "L1",
        },
        "BuyerRequestedCancel": {
            "IsBuyerRequestedCancel": True,
            "BuyerCancelReason": "changed mind",
        },
    }


def test_order_item_without_optional_data():
    result = transform_order_items_v2_to_v0(
        {"orderId": "1", "orderItems": [{"orderItemId": "i", "product": {"asin": "A"}}]}
    )
    (item,) = result["OrderItems"]
    assert item == {
        "ASIN": "A",
        "OrderItemId": "i",
        "IsGift": "false",
        "IsTransparency": False,
    }


def test_buyer_info_is_mapped_to_v0(order_v2):
    assert transform_order_buyer_info_v2_to_v0(order_v2) == {
        "AmazonOrderId": "111-0000000-0000001",
        "BuyerEmail": "buyer@example.com",
        "BuyerName": "Jane Buyer",
        "PurchaseOrderNumber": "PO-1",
    }


def test_buyer_info_drops_missing_fields():
    assert transform_order_buyer_info_v2_to_v0({"orderId": "1"}) == {"AmazonOrderId": "1"}


def test_address_is_mapped_to_v0(order_v2):
    assert transform_order_address_v2_to_v0(order_v2) == {
        "AmazonOrderId": "111-0000000-0000001",
        "ShippingAddress": {
            "Name": "Jane Buyer",
            "AddressLine1": "1 Main St",
            "City": "Springfield",
            "StateOrRegion": "CA",
            "PostalCode": "12345",
            "CountryCode": "US",
        },
    }


@pytest.mark.parametrize(
    "order",
    [
        {"orderId": "1"},
        {"orderId": "1", "recipient": None},
        {"orderId": "1", "recipient": {"deliveryAddress": None}},
    ],
)
def test_address_omitted_without_recipient(order):
    assert transform_order_address_v2_to_v0(order) == {"AmazonOrderId": "1"}
    assert "ShippingAddress" not in transform_order_v2_to_v0(order)


@pytest.mark.parametrize(
    "selected, expected",
    [
        ([], ["BUYER", "FULFILLMENT", "PACKAGES", "PAYMENT", "PROCEEDS", "RECIPIENT"]),
        (
            ["orderitems", "orderbuyerinfo", "orderfinancialevents"],
            [
                "BUYER",
                "CANCELLATION",
                "EXPENSE",
                "FULFILLMENT",
                "PACKAGES",
                "PAYMENT",
                "PROCEEDS",
                "PROMOTION",
                "RECIPIENT",
            ],
        ),
    ],
)
def test_build_search_included_data(selected, expected):
    assert build_search_included_data(selected) == expected


def test_order_from_child_context_returns_order(order_v2):
    assert order_v2_from_child_context({ORDER_V2_CONTEXT_KEY: order_v2}) is order_v2


@pytest.mark.parametrize("context", [None, {}, {ORDER_V2_CONTEXT_KEY: None}])
def test_order_from_child_context_requires_order(context):
    with pytest.raises(RuntimeError):
        order_v2_from_child_context(context)


def test_condition_subtype_keeps_v0_acronym_spelling():
    result = transform_order_items_v2_to_v0(
        {
            "orderId": "1",
            "orderItems": [{"product": {"condition": {"conditionSubtype": "OEM"}}}],
        }
    )
    assert result["OrderItems"][0]["ConditionSubtypeId"] == "OEM"
