# Ecom Import Tool – Psychological Handbook

**App:** ecom_import_tool  
**Purpose:** Architectural intent, business reasoning, constraints, and anti-patterns. Read this before making changes.

---

## 1. Architectural Intent

Ecom Import Tool is an **e-commerce MTR ingestion layer** that:

- Imports platform-provided MTR (Merchant Tax Report) CSVs
- Creates ERPNext Sales Invoice and return Sales Invoice (credit note) documents that match the platform’s records 1:1
- Supports idempotent re-runs (skips already-created invoices/returns by reference IDs)

---

## 2. Business Reasoning

### 2.1 One Platform Document = One ERP Document

**Intent:** Each platform invoice and each platform credit note should map to exactly one ERPNext Sales Invoice or return. No collapsing multiple credit notes into one.

**Constraint:** When Amazon (or other platforms) issue multiple credit notes for the same original invoice, each credit note must create its own return Sales Invoice. Do not batch or merge them.

### 2.2 Grouping Strategy

**Intent:** Rows are grouped for processing. Shipment rows use `invoice_number`; refund rows must use `credit_note_no` so that each unique credit note is processed separately.

### 2.3 Idempotency

**Intent:** Re-importing the same MTR file should not create duplicate invoices or returns. Existing submitted documents are detected by `custom_ecommerce_invoice_id` (for returns) or `custom_inv_no` (for shipments).

### 2.4 Zero-Quantity Refund Handling (Debit Note)

**Intent:** Amazon refund rows with blank/zero quantity represent amount-only adjustments (e.g. goodwill credits). They must not crash the import and must not silently be skipped.

**Preferred approach:** Use ERPNext's native `is_debit_note` flag ("Is Rate Adjustment Entry (Debit Note)") on the Sales Invoice. This is purpose-built for qty=0 entries — `validate_qty_is_not_zero()` is skipped, `amount = flt(rate)` is computed directly, and India Compliance classifies it correctly as "Debit Note" in GSTR-1.

**Constraint:** `is_return` and `is_debit_note` are **mutually exclusive** on a single Sales Invoice (India Compliance throws an error if both are set). Therefore:
- When all rows in a refund group have qty=0 → create a Debit Note.
- When rows are mixed (some qty=0, some qty>0) → create a Return and use `qty=-1` for the zero-qty rows as fallback.

**Rationale:** A zero-quantity refund has no stock implication; it is purely a financial adjustment. The debit note approach gives correct accounting classification without artificial qty values.

---

## 3. Constraints

- **Do not collapse multiple credit notes into one** – each `credit_note_no` gets its own return.
- **Do not skip refund rows with empty `credit_note_no` silently** – report as error for visibility.
- **Handbook drift** – After logic changes, update both `technical_handbook.md` and `psychological_handbook.md`.

---

## 4. Anti-Patterns to Avoid

1. **Treating all refund items in an invoice group as one block** – Amazon can issue multiple credit notes per invoice; sub-group by `credit_note_no`.
2. **Silent exclusion of rows** – Missing Credit Note No or invalid data should surface as import errors.
3. **Hardcoded account/tax heads** – Use Ecommerce Mapping and platform config where available.
4. **Dividing by quantity without a zero guard** – Always use `safe_refund_qty_rate()` for refund lines to prevent `ZeroDivisionError` on blank/zero qty rows.

---

## 5. When Adding or Changing Import Logic

1. Read both handbooks.
2. Ensure 1:1 mapping between platform documents and ERP documents.
3. Add or adjust grouping only after verifying platform data structure.
4. Update both handbooks.
