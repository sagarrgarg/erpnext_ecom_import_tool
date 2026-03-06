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
