# Ecom Import Tool – Technical Handbook

**App:** ecom_import_tool  
**Last updated:** 2026  
**Purpose:** Technical reference for developers – what exists, why, impacted modules, and migration implications.

---

## 1. App Overview

Ecom Import Tool extends ERPNext/Frappe with:

- E-commerce MTR (Merchant Tax Report) import from Amazon B2B, Amazon B2C, Flipkart, Jio Mart, Cred
- Automated creation of Sales Invoice (shipment) and return Sales Invoice (credit note) from platform CSV exports
- Ecommerce Mapping for SKU, warehouse, GSTIN, and customer defaults

---

## 2. Module Structure

| Path | Purpose |
|------|---------|
| `ecom_import_tool/doctype/ecommerce_bill_import/` | Main import doctype – MTR parsing, grouping, SI/return creation |
| `ecom_import_tool/doctype/amazon_mtr_b2c/` | Child table schema for Amazon B2C MTR rows |
| `ecom_import_tool/doctype/ecommerce_mapping/` | Platform config (Amazon, etc.), item/warehouse/GSTIN mappings |

---

## 3. Amazon B2C MTR Import Flow

### 3.1 Entry Points

- **Doctype:** Ecommerce Bill Import
- **Method:** `create_sales_invoice_mtr_b2c()` in `ecommerce_bill_import.py`

### 3.2 Flow

1. User uploads CSV (`mtr_b2c_attachment`), selects Amazon + MTR B2C
2. `append_mtr_b2c()` reads CSV with pandas, maps columns to Amazon MTR B2C child table (generic `column_name.strip().lower().replace(' ', '_')`), populates `self.mtr_b2c`
3. `create_sales_invoice_mtr_b2c()`:
   - Groups rows by `invoice_number` (lines 1399–1405)
   - Separates `shipment_items` (transaction_type not Refund/Cancel) and `refund_items` (transaction_type == Refund)
   - Processes shipment items → Sales Invoice
   - **Sub-groups refund items by `credit_note_no`** and creates one return Sales Invoice per unique credit note
   - Uses `custom_ecommerce_invoice_id` = credit note number, `custom_inv_no` = original invoice number

### 3.3 Credit Note Sub-grouping (2026)

- **What:** Refund items within an invoice group are sub-grouped by `credit_note_no`. Each unique credit note creates its own return Sales Invoice.
- **Why:** Amazon can issue multiple credit notes for the same original invoice (e.g. VCJQ-C-160, C-161, … C-165). Previously, only the first credit note was created; the rest were silently lost.
- **Impacted:** `create_sales_invoice_mtr_b2c()` – refund block (lines ~1612–1810).
- **Migration:** None. Re-importing MTR B2C files will now create all credit notes correctly.

### 3.4 Zero-Quantity Refund — Debit Note Approach (2026)

- **What:** Amazon refund rows sometimes arrive with blank or zero quantity (amount-only adjustments). The helper `safe_refund_qty_rate()` prevents `ZeroDivisionError` and returns `qty=0` with `is_zero_qty=True` for these rows.
- **Why:** Amazon MTR reports can contain refund rows where only the monetary amount is populated and quantity is blank/0. Previously this caused a hard crash during import.
- **Document type selection (pre-scan):** Before creating the return document, all refund rows in the group are scanned:
  - **All rows qty=0:** Create a **Debit Note** (`is_debit_note=1`, `is_return=0`, `update_stock=0`, `qty=0`, `rate=abs(amount)`). ERPNext natively supports qty=0 on debit notes — no workaround needed.
  - **All rows qty>0:** Create a normal **Return / Credit Note** (`is_return=1`, `is_debit_note=0`, `update_stock=1`, `qty=-abs(qty)`, `rate=abs(amount)/abs(qty)`).
  - **Mixed (some qty=0, some qty>0):** Create a **Return / Credit Note** (`is_return=1`). Normal rows use standard negative qty; zero-qty rows use `qty=-1`, `rate=abs(amount)` as fallback (since `is_return` and `is_debit_note` are mutually exclusive per India Compliance).
- **`safe_refund_qty_rate()` return values:**
  - `abs(quantity) > 0`: `(-abs_qty, abs_amount / abs_qty, False)`
  - `abs(quantity) == 0` (or blank/None/NaN): `(0, abs_amount, True)`
- **Scope:** Applied to both `create_sales_invoice_mtr_b2b()` and `create_sales_invoice_mtr_b2c()` refund loops.
- **Idempotency:** The existing-document check now uses `custom_ecommerce_invoice_id` + `docstatus=1` without filtering on `is_return`, so both debit notes and returns are detected.
- **GST classification:** India Compliance classifies `is_debit_note` invoices as "Debit Note" in GSTR-1 automatically.
- **Migration:** None. Existing imports are unaffected; only newly imported files benefit.

---

## 4. Key Dependencies

- **Seller GSTIN:** Required for GSTIN mapping. Must exist in Ecommerce Mapping → Ecommerce GSTIN Mapping.
- **Item mapping:** SKU in CSV must map to ERPNext Item via `Ecommerce Mapping.ecom_item_table`.
- **Warehouse mapping:** `warehouse_id` from CSV must map to ERPNext Warehouse, or default is used.

---

## 5. Post-Change Commands

After changes to fields, JS, Vue, or assets:

```bash
bench clear-cache && bench migrate && bench build --app ecom_import_tool && bench clear-cache
```
