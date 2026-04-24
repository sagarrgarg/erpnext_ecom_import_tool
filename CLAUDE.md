# Ecom Import Tool

E-commerce MTR (Merchant Tax Report) import tool for ERPNext. Parses CSV exports from Amazon (B2B/B2C), Flipkart, Jio Mart, and CRED, then creates Sales Invoices, Credit Notes, and Debit Notes automatically.

## Architecture

```
ecom_import_tool/
  override.py              → Custom classes for SI/DN/PR/PI (name override via before_insert)
  hooks.py                 → doctype_js, override_doctype_class, fixtures
  ecom_import_tool/
    doctype/
      ecommerce_bill_import/   → Main import engine (3700+ lines) — CSV parse, group, create docs
      ecommerce_mapping/       → Platform config (Amazon/Flipkart/CRED/Jiomart)
      ecommerce_item_mapping/  → SKU → ERPNext Item mapping (child table)
      ecommerce_warehouse_mapping/ → External warehouse_id → ERPNext Warehouse
      ecommerce_gstin_mapping/ → Seller GSTIN mapping
      ecommerce_platform/      → Platform master (Amazon, Flipkart, CRED, Jiomart)
      amazon_mtr_b2c/          → Child table for B2C MTR rows
      ecommerce_mtr_b2b/       → Child table for B2B MTR rows
      flipkart_items/          → Child table for Flipkart transaction rows
      flipkart_transaction_items/ → Child table for Flipkart items
      jio_mart/                → Child table for Jio Mart rows
      cred/                    → Child table for CRED rows
      cred_items/              → Child table for CRED line items
      amazon_stock_transfer/   → Amazon FC stock transfer
    custom/                    → Custom field JSON exports (SI, SI Item, PI, DN, PR)
    fixtures/                  → Fixture JSON (custom_field, ecommerce_platform, customer_group)
    public/js/                 → Client scripts for SI, DN, PI, PR (e-commerce field visibility)
```

## Core Flow

1. User creates Ecommerce Bill Import, selects Ecommerce Mapping (platform)
2. Uploads CSV attachment for the platform
3. Clicks "Start Import" → `create_invoice()` dispatches to platform-specific handler
4. Handler: parse CSV → group by invoice_number → separate shipments vs refunds → create SI/CN/DN
5. Refunds sub-grouped by credit_note_no (Amazon B2C) — one return per credit note
6. Zero-qty refunds → Debit Note (`is_debit_note=1, qty=0, rate=abs(amount)`)

## Key Patterns

- **CSV parsing**: `pd.read_csv(dtype=str, keep_default_na=False)` to preserve IDs
- **Cell cleanup**: `clean_csv_cell()` strips quotes, backticks, normalizes null-ish strings
- **Date parsing**: `parse_export_datetime()` with day-first (DD-MM-YYYY) preference
- **Rate calc**: `taxable_value / qty` (not total/qty)
- **Doc naming**: `__newname` sets doc name directly to ecommerce invoice ID (no custom fields)
- **Idempotency**: Check existing doc by `name` + `docstatus` filter
- **GSTIN**: `ecommerce_gstin_mapping` required — maps seller GSTIN per platform

## Doctypes Modified (via hooks)

- Sales Invoice — `custom_ecommerce_invoice_id` field, JS for ecom visibility
- Delivery Note — `custom_inv_no` field
- Purchase Receipt — `custom_inv_no` field
- Purchase Invoice — `custom_inv_no` field

## Bench Commands

```bash
# After any .py/.json/.js change
cd /home/ubuntu/frappe-bench-new && bench --site erpnextkgopl.local migrate

# After JS/CSS changes
cd /home/ubuntu/frappe-bench-new && bench build --app ecom_import_tool

# Full reset
cd /home/ubuntu/frappe-bench-new && bench --site erpnextkgopl.local clear-cache && bench --site erpnextkgopl.local migrate
```

## Code Style

- Python: tabs for indentation, 110 char lines
- No type hints in existing code — match the style
- `frappe.throw()` for validation errors, `frappe.log_error()` for non-fatal
- Use `flt()` for numeric conversions, `getdate()` for dates

## graphify-out

After significant code changes, run graphify to update the knowledge graph. Read `graphify-out/obsidian/` vault pages BEFORE reading raw source code for architecture context.

## Testing

```bash
cd /home/ubuntu/frappe-bench-new && bench --site erpnextkgopl.local run-tests --app ecom_import_tool
```
