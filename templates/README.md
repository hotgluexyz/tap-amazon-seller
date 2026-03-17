# tap-amazon-seller Configuration

This document describes the configuration options for **tap-amazon-seller**, a Singer tap that extracts data from Amazon Seller Central.

---

## Authentication

#### `lwa_client_id` (string, required)
The Login with Amazon (LWA) application client ID from your Amazon SP-API application.
- **Example**: `"amzn1.application-oa2-client.xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"`

#### `client_secret` (string, required)
The LWA client secret for your Amazon SP-API application.
- **Example**: `"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"`

#### `refresh_token` (string, required)
The LWA refresh token used to obtain access tokens for SP-API calls. Generated via the Amazon SP-API authorization flow.
- **Example**: `"Atzr|xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"`

#### `aws_access_key` (string, optional)
AWS IAM access key ID. Required when using IAM user credentials for SP-API (alternative to role-based auth).
- **Example**: `"AKIAXXXXXXXXXXXXXXXX"`

#### `aws_secret_key` (string, optional)
AWS IAM secret access key. Required when using IAM user credentials for SP-API.
- **Example**: `"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"`

#### `role_arn` (string, optional)
ARN of the IAM role to assume for SP-API. Used when authenticating via IAM role instead of access key/secret.
- **Example**: `"arn:aws:iam::123456789012:role/your-sp-api-role"`

---

## Marketplace Configuration

#### `marketplace` (string, optional)
Default marketplace ID when a single marketplace is used. Used when `marketplaces` is not set.
- **Default**: `"US"`
- **Example**: `"US"`, `"CA"`, `"GB"`, `"DE"`, `"JP"`

#### `marketplaces` (array of strings, optional)
List of marketplace IDs to sync. If provided, the tap syncs data for each marketplace in the list. If omitted, the tap uses `marketplace` (default `"US"`) or all supported marketplaces depending on stream logic.
- **Example**: `["US", "CA", "GB", "DE"]`

---

## Environment

#### `sandbox` (boolean, optional)
When true, uses the Amazon SP-API sandbox environment instead of production. **Required** when using credentials from the [Solution Provider Portal](https://solutionproviderportal.amazon.com/) (sandbox app); otherwise you will get "Access to requested resource is denied" / invalid token.
- **Default**: `false`
- **Example**: `true` (for sandbox), `false` (for production)

---

## Date Filtering

#### `start_date` (string, optional)
Start date/time for report and stream filtering. ISO 8601 format recommended.
- **Example**: `"2024-01-01T00:00:00Z"` or `"2024-01-01"`

#### `end_date` (string, optional)
End date/time for report and stream filtering. ISO 8601 format recommended.
- **Example**: `"2024-12-31T23:59:59Z"` or `"2024-12-31"`

---

## Reports Configuration

#### `report_types` (array of strings, optional)
Report types to request from the Reports API.
- **Default**: `["GET_LEDGER_DETAIL_VIEW_DATA", "GET_MERCHANT_LISTINGS_ALL_DATA"]`
- **Example**: `["GET_LEDGER_DETAIL_VIEW_DATA", "GET_MERCHANT_LISTINGS_ALL_DATA", "GET_FBA_FULFILLMENT_CUSTOMER_SHIPMENT_REPLACEMENT_DATA"]`

#### `processing_status` (array of strings, optional)
Report processing statuses to include when listing or filtering reports.
- **Default**: `["IN_QUEUE", "IN_PROGRESS"]`
- **Example**: `["IN_QUEUE", "IN_PROGRESS", "DONE"]`

---

## Product Catalog

#### `products_include_data` (array of strings or comma-separated string, optional)
Data to include when fetching product catalog details (e.g. for Product Details streams). Can be an array or a comma-separated string.
- **Example (array)**: `["attributes", "identifiers", "summaries"]`
- **Example (string)**: `"attributes,identifiers,summaries"`

---

## Minimal config.json (required options only)

```json
{
  "lwa_client_id": "amzn1.application-oa2-client.xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "client_secret": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "refresh_token": "Atzr|xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
}
```

---

## Complete config.json example

```json
{
  "lwa_client_id": "amzn1.application-oa2-client.xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "client_secret": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "refresh_token": "Atzr|xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "aws_access_key": "AKIAXXXXXXXXXXXXXXXX",
  "aws_secret_key": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "role_arn": "arn:aws:iam::123456789012:role/your-sp-api-role",
  "marketplace": "US",
  "marketplaces": ["US", "CA", "GB", "DE"],
  "sandbox": false,
  "start_date": "2024-01-01T00:00:00Z",
  "end_date": "2024-12-31T23:59:59Z",
  "report_types": [
    "GET_LEDGER_DETAIL_VIEW_DATA",
    "GET_MERCHANT_LISTINGS_ALL_DATA"
  ],
  "processing_status": ["IN_QUEUE", "IN_PROGRESS", "DONE"],
  "products_include_data": ["attributes", "identifiers", "summaries"]
}
```
