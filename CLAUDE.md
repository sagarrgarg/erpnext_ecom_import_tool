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

## Ecommerce Mapping — `mode_of_payment` (added 2026-05-10)

- `mode_of_payment` (Link → Mode of Payment, required, unique per mapping). Drives `is_pos=1` on every SI / CN created from this mapping (Amazon B2B + B2C + Flipkart sales/returns + CRED sales/refunds).
- The MoP must have a Default Account configured for at least one company (validated at Ecommerce Mapping save).
- Stock Transfer (Amazon SI/DN + PI/PR pair) uses the shared helpers but passes `mode_of_payment=None` — inter-company internal flow, no real payment movement, no POS row.
- JioMart will adopt the same field in a follow-up port; helpers in `ecom_import_tool/utils/amazon_si.py` already accept platform-agnostic params.

## CRED — Refund handling (added 2026-05-10)

- `cred_attach` (CSV) — sales export (`sales_all_*.csv`). Required.
- `cred_refund_attach` (XLSX) — CRED Mail Report. Optional. The `Refund` sheet is parsed into a new `cred_refund` child table (one CRED Refund row per refund line item).
- Sales rows with `Order Status` Cancelled are skipped (no SI is created).
- For each refund row, the parser joins `cred_order_item_id` ↔ CSV's `Suborder No` (with leading backtick stripped) to resolve the EE Invoice No.
- Credit notes are created ONLY when the parent SI (`ee_invoice_no`) is already submitted in ERPNext. If the parent SI is not yet in DB, the refund is skipped silently and will be picked up on a subsequent refund-only import once the sales report for that EE Invoice No has been imported.
- CN naming: `<EE_INV>RT` (no dash). Idempotent: re-runs skip existing CNs.
- Penalty sheet from CRED Mail Report is NOT imported automatically (different beast — would be a Journal Entry, not a Sales Invoice).

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

## State names (updated 2026-11-14)

- Every platform resolves states through `state_code_dict` + `normalize_state_key()` in
  `ecommerce_bill_import.py`. Add new names to `_STATE_ALIASES` there, never per handler.
- Covered: Keralam (Kerala's new official name), the pre-2020 UTs Dadra and Nagar Haveli / Daman and Diu
  (both → 26), old names (Orissa, Uttaranchal, Pondicherry), spellings, 2-letter and ISO codes (KL, IN-KL),
  GST codes ("32") and POS labels ("32-Kerala"). `&` reads as "and"; trailing dots/commas are ignored.

## Flipkart — IGST on same-state sales (added 2026-11-14)

- Flipkart's report sometimes charges IGST on a sale or return whose seller GSTIN state equals the place
  of supply (e.g. Haryana → Haryana); India Compliance refuses it ("Cannot charge IGST for intra-state
  supplies"). `split_intra_state_igst()` splits that IGST equally into CGST + SGST (same total tax).
  Only when the states match; every other row keeps Flipkart's taxes exactly. Agreed with the owner
  2026-11-14 (option a); the invoice Flipkart issued to the buyer still shows IGST.

## Stock safety (added 2026-11-15) — `utils/stock_guard.py`

Built after KGO's October Flipkart imports left 2,833 `update_stock=1` invoices with no Stock Ledger Entry
(no COGS / Stock In Hand GL), and an SD-18 batch sharing one timestamp queued 301 deadlocking reposts.
- **Every import submit goes through `submit_checked(doc)`** (also inside `_amazon_save_and_submit`). It submits
  under a savepoint; if submit raises, or any stock-item row of an SI/PI (`update_stock=1`) / DN / PR has no
  active SLE, the doc is rolled back to draft and the import log shows "Submit failed: no stock ledger: …".
  Never call `doc.submit()` directly in a handler.
- **One posting time per document**: `stamp_posting(doc, platform_dt)` keeps the platform date and moves the
  time on by whole seconds until free (Flipkart's 00:00:00 → 00:00:00, 00:00:01, … in file order). Times of
  already-submitted vouchers that day count as taken; the date never changes. Active only inside a run
  (`stock_guard.start_run` / `end_run` in `invoice_creation`).
- **Stock before sales**: `EcommerceBillImport._batch_stock_needs()` sums what the file's not-yet-submitted
  sales take per (item, warehouse); `stock_shortfall` compares it with stock at the batch's earliest posting
  time (no reliance on allow_negative_stock). The preview shows "AL112 @ Ecommerce Out: need 7,536, have 1,749";
  during the run, sales from a short pair are not submitted ("Stock shortfall: …"). CRED is not covered yet
  (its handler reads the files itself).
- The reconcile view flags submitted SIs whose stock rows lack SLE ("Stock Ledger" column).
- Tests: `doctype/ecommerce_bill_import/test_stock_guard.py` (run via
  `bench --site frappe.localhost execute excom.excom.tests.run.run --kwargs '{"module": "ecom_import_tool.ecom_import_tool.doctype.ecommerce_bill_import.test_stock_guard"}'`).
