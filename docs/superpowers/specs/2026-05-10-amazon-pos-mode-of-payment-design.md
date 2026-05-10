# Amazon POS Mode of Payment + Shared SI Builder

**Status:** Approved (2026-05-10) — implementation in progress
**Scope:** Amazon B2B sales + credit/debit notes, Amazon B2C sales + credit/debit notes
**Out of scope (this iteration):** Stock Transfer, Flipkart, CRED, JioMart

## 1. Goals

1. Add `mode_of_payment` field on Ecommerce Mapping (Link → Mode of Payment, required, unique across mappings).
2. Validate at mapping save: linked MoP must have at least one Account row with `default_account` set.
3. Refactor Amazon B2B sales, B2B credit/debit notes, B2C sales, B2C credit/debit notes onto a shared SI/CN builder. Six near-identical loops collapse into one.
4. Apply `is_pos=1`, `pos_profile=""`, `payments=[mop, 100%]` inline as part of header build, before the FIRST `save()`. No second save just for is_pos.
5. Stock Transfer untouched (inter-company, no real payment movement).
6. Code shape ready for Flipkart/CRED/JioMart port: each platform passes its own `tax_field_map`, `customer_resolver`, plus its `mode_of_payment` from its own Ecommerce Mapping when ported later.

## 2. Non-goals

- No backfill for existing Amazon SIs in DB. Payment entries get added going forward only.
- No new Mode of Payment auto-creation. User configures MoP + Account in ERPNext UI.
- No changes to Stock Transfer SI/PI/PR/DN flow.
- No changes to error reporting / FY-prefix logic / find_existing_amazon_doc / cn_groups grouping. Those stay as-is.

## 3. Schema change — Ecommerce Mapping

`ecom_import_tool/ecom_import_tool/doctype/ecommerce_mapping/ecommerce_mapping.json`

Add to `field_order` after `income_account`:

```
"income_account",
"mode_of_payment",
"column_break_ybgs",
```

Add to `fields[]`:

```json
{
  "fieldname": "mode_of_payment",
  "fieldtype": "Link",
  "options": "Mode of Payment",
  "label": "Mode of Payment",
  "reqd": 1,
  "description": "Linked Mode of Payment for POS-style 100% settlement on every Sales Invoice / Credit Note created from this mapping. Must be unique per mapping; must have a default Account configured for at least one company."
}
```

Migration: `bench migrate` adds the column. Existing Ecommerce Mapping docs will have NULL — first save after migrate forces the user to set it (mandatory).

## 4. Validation — `EcommerceMapping.validate()`

`ecom_import_tool/ecom_import_tool/doctype/ecommerce_mapping/ecommerce_mapping.py`

```python
def validate(self):
    if not self.mode_of_payment:
        # frappe handles via reqd=1, but defensive.
        frappe.throw(_("Mode of Payment is mandatory."))

    # Uniqueness across mappings.
    dup = frappe.db.get_value("Ecommerce Mapping", {
        "mode_of_payment": self.mode_of_payment,
        "name": ["!=", self.name],
    }, "name")
    if dup:
        frappe.throw(_(
            "Mode of Payment '{0}' is already used by Ecommerce Mapping '{1}'. "
            "Each mapping must have its own MoP for clean reconciliation."
        ).format(self.mode_of_payment, dup))

    # MoP has at least one accounts row with a default_account set.
    mop = frappe.get_doc("Mode of Payment", self.mode_of_payment)
    has_account = any(r.default_account for r in (mop.accounts or []))
    if not has_account:
        frappe.throw(_(
            "Mode of Payment '{0}' has no Default Account configured. "
            "Open it and set Default Account for at least one company."
        ).format(self.mode_of_payment))
```

## 5. POS payment helper

Module-level in `ecommerce_bill_import.py`, beside `find_existing_amazon_doc`.

**Important constraint surfaced by ERPNext code review:** ERPNext's
`set_payment_amounts()` auto-fills `payments[0].amount = grand_total` only
for non-return Sales Invoices. For `is_return=1`, ERPNext's
`verify_payment_amount_is_negative()` (apps/erpnext sales_invoice.py:351)
requires payment amounts to already be negative — it validates, doesn't
auto-set the sign. Same applies to `is_debit_note=1`.

Conclusion: we set `amount` explicitly to `si.grand_total` (which is
already negative for returns when items have negative qty). This requires
calling `apply_pos_payment` AFTER the first `save()` (so `grand_total` is
populated). The existing two-save dance handles this naturally — POS
goes between save 1 and save 2, no extra save.

When `grand_total == 0` (e.g. some zero-qty zero-rate debit notes),
ERPNext (sales_invoice.py:542) only requires `payments` rows when
`grand_total > 0`. We can safely skip the payment row in that case.

```python
def apply_pos_payment(si, mode_of_payment):
    """Mark a Sales Invoice as POS-settled 100% via the given Mode of Payment.

    Must be called AFTER si.save() so si.grand_total is computed. Caller then
    saves once more to validate the POS state.

    Skipped (no-op) when:
      - mode_of_payment is empty (defensive)
      - si.grand_total == 0 (ERPNext allows empty payments when grand_total=0)
    """
    if not mode_of_payment:
        return
    if not flt(si.grand_total):
        # grand_total may legitimately be zero on zero-qty zero-rate debit notes.
        # ERPNext sales_invoice.py:542 only requires payments when grand_total > 0.
        return
    si.is_pos = 1
    si.pos_profile = ""
    si.set("payments", [])
    si.append("payments", {
        "mode_of_payment": mode_of_payment,
        # Explicit amount = grand_total. Already negative for returns.
        "amount": flt(si.grand_total),
    })
```

## 6. Shared Amazon SI builder

Three module-level helpers replace the duplicated `frappe.new_doc("Sales Invoice")` blocks in B2B sale, B2B CN, B2C sale, B2C CN.

### 6a. `_amazon_init_si_header`

Builds an unsaved Sales Invoice doc. Reuses an existing draft if provided (the result of `find_existing_amazon_si(..., docstatus=0)`).

```python
def _amazon_init_si_header(*, mapping, customer, posting_dt, ecom_name,
                           is_return, is_debit_note, return_against,
                           ecommerce_operator, amazon_type, ecommerce_gstin,
                           update_stock, draft_doc):
    """Returns an unsaved Sales Invoice with header fields set.
    Caller appends items + tax rows, then calls _amazon_save_and_submit."""
    if draft_doc:
        si = draft_doc
        si.is_return = 1 if is_return else 0
        si.is_debit_note = 1 if is_debit_note else 0
        si.ecommerce_gstin = ecommerce_gstin
        return si

    si = frappe.new_doc("Sales Invoice")
    si.flags.ignore_pricing_rule = 1
    si.customer = customer
    si.set_posting_time = 1
    si.posting_date = posting_dt.date()
    si.posting_time = posting_dt.time()
    si.custom_ecommerce_operator = ecommerce_operator
    si.custom_ecommerce_type = amazon_type
    si.taxes_and_charges = ""
    si.taxes = []
    si.update_stock = update_stock
    si.is_return = 1 if is_return else 0
    si.is_debit_note = 1 if is_debit_note else 0
    if return_against:
        si.return_against = return_against
    si.ecommerce_gstin = ecommerce_gstin
    if not frappe.db.exists("Sales Invoice", ecom_name):
        si._ecom_name = ecom_name
    return si
```

### 6b. `_amazon_append_si_line`

Appends one item + accumulates tax rows. `taxes` is a list of `(tax_type, rate, amount, account_head)` tuples. Tax rows are deduped by `account_head` — if a row exists, its `tax_amount` is incremented.

```python
def _amazon_append_si_line(si, *, item_code, qty, rate, hsn_code, description,
                           warehouse, income_account, custom_ecom_item_id,
                           taxes, is_free_item=False, margin_amount=0,
                           tax_rate_scalar=None):
    """Append a single item line + roll up taxes onto si.taxes."""
    item_row = {
        "item_code": item_code,
        "qty": qty,
        "rate": rate,
        "price_list_rate": rate,
        "gst_hsn_code": hsn_code,
        "description": description,
        "warehouse": warehouse,
        "income_account": income_account,
        "custom_ecom_item_id": custom_ecom_item_id,
    }
    if tax_rate_scalar is not None:
        # Sales Invoice Item.tax_rate is a scalar field used by some downstream
        # reports / GST hooks. Distinct from the per-row `rate` inside taxes[].
        item_row["tax_rate"] = tax_rate_scalar
    if margin_amount:
        item_row["margin_type"] = "Amount"
        item_row["margin_rate_or_amount"] = margin_amount
    if is_free_item:
        item_row["is_free_item"] = 1
    si.append("items", item_row)

    for tax_type, tax_rate, tax_amount, acc_head in taxes:
        if not tax_amount:
            continue
        existing = next((t for t in si.taxes if t.account_head == acc_head), None)
        if existing:
            existing.tax_amount += tax_amount
            existing.rate = tax_rate
        else:
            si.append("taxes", {
                "charge_type": "On Net Total",
                "account_head": acc_head,
                "rate": tax_rate,
                "tax_amount": tax_amount,
                "description": tax_type,
            })
```

### 6c. `_amazon_save_and_submit`

```python
def _amazon_save_and_submit(si, *, mode_of_payment, due_date=None):
    """Save (so grand_total computes), clear item_tax_template/rate, apply POS
    with the now-known grand_total, save again to validate POS, submit.
    Returns the saved si."""
    si.save(ignore_permissions=True)
    for it in si.items:
        it.item_tax_template = ""
        it.item_tax_rate = frappe._dict()
    apply_pos_payment(si, mode_of_payment)
    if due_date:
        si.due_date = due_date
    si.save(ignore_permissions=True)
    si.submit()
    return si
```

The two-save dance is preserved because Frappe's GST tax-template hooks
re-attach `item_tax_template` on first save; clearing + saving again
silences that. POS is applied between the two saves so `si.grand_total`
is populated and `payments[0].amount` can be set explicitly to it
(which is negative for returns, as required by ERPNext's
`verify_payment_amount_is_negative()`).

## 6d. Caller skeleton

The helpers do NOT own these caller responsibilities — they stay
inline in each `create_sales_invoice_mtr_*` method:

- Resolving `itemcode`, `warehouse`, `location`, `com_address` per row
  (warehouse mapping table lookup with default fallback).
- Mutating `si.location`, `si.set_warehouse`, `si.company_address`,
  `si.place_of_supply`, `si.ecommerce_gstin` from per-row data when
  the SI header didn't have them yet.
- Setting `warehouse_mapping_missing = True` and the per-row error
  accumulators (`error_log`, `errors[]`).
- Counting successful row appends via `items_added` / `ritems_added`.

A condensed skeleton (illustrative, not the literal final code):

```python
si = _amazon_init_si_header(
    mapping=amazon, customer=customer, posting_dt=invoice_dt,
    ecom_name=qualified_invoice_no,
    is_return=False, is_debit_note=False, return_against=None,
    ecommerce_operator=self.ecommerce_mapping,
    amazon_type=self.amazon_type,
    ecommerce_gstin=mapped_ecommerce_gstin,
    update_stock=1,
    draft_doc=frappe.get_doc("Sales Invoice", existing_si_draft) if existing_si_draft else None,
)

items_added = []
for idx, child_row in shipment_items:
    try:
        # caller resolves: itemcode, warehouse, place_of_supply, header mutations
        ...
        _amazon_append_si_line(
            si,
            item_code=itemcode, qty=qty, rate=rate, hsn_code=hsn_code,
            description=child_row.item_description, warehouse=warehouse,
            income_account=amazon.income_account,
            custom_ecom_item_id=child_row.shipment_item_id,
            is_free_item=(str(child_row.transaction_type) == "FreeReplacement"),
            margin_amount=flt(child_row.item_promo_discount),
            taxes=[
                ("CGST", flt(child_row.cgst_rate), flt(child_row.cgst_tax), "Output Tax CGST - KGOPL"),
                ("SGST", flt(child_row.sgst_rate) + flt(child_row.utgst_rate),
                  flt(child_row.sgst_tax) + flt(child_row.utgst_tax), "Output Tax SGST - KGOPL"),
                ("IGST", flt(child_row.igst_rate), flt(child_row.igst_tax), "Output Tax IGST - KGOPL"),
            ],
            tax_rate_scalar=flt(child_row.total_tax_amount),
        )
        items_added.append(itemcode)
    except Exception as e:
        error_log.append(invoice_no)
        errors.append({"idx": idx, "invoice_id": invoice_no, "message": str(e)})

if items_added and not warehouse_mapping_missing and invoice_no not in error_log:
    _amazon_save_and_submit(si, mode_of_payment=amazon.mode_of_payment, due_date=getdate(today()))
    success_count += len(shipment_items)
    frappe.db.commit()
```

The architect agent flagged that `tax_rate` (a scalar field on
`Sales Invoice Item` — different from the per-tax `rate` inside
`taxes`) was being silently dropped from the helper. It's accepted
explicitly via `tax_rate_scalar`.

## 7. Per-method changes

### 7a. `create_sales_invoice_mtr_b2b`

Sales loop:
- Existing pre-save block (~lines 1196–1227 in current file) collapses to:
  - Compute `_inv_dt`, `qualified_invoice_no`, `existing_si_draft`, `existing_si`, `mapped_ecommerce_gstin`, `customer` exactly as today.
  - `si = _amazon_init_si_header(...)` with the header params.
  - Per-row loop — replace the current item_row dict + tax tuples with `_amazon_append_si_line(si, item_code=..., taxes=[...])`. Same per-row logic for `place_of_supply` resolution and warehouse fallback stays inline (it mutates `si` directly).
  - Replace the two-save + submit block with `_amazon_save_and_submit(si, mode_of_payment=amazon.mode_of_payment, due_date=getdate(today()))`.

CN/DN loop:
- Existing per-CN body inside `cn_groups` swaps the new_doc/draft_return branches for `_amazon_init_si_header(..., is_return=use_debit_note==False, is_debit_note=use_debit_note, return_against=existing_si if not use_debit_note else None, update_stock=0 if use_debit_note else 1)`.
- Per-row item append uses `_amazon_append_si_line` with `safe_refund_qty_rate` results for qty/rate.
- Final save+submit uses `_amazon_save_and_submit`.

### 7b. `create_sales_invoice_mtr_b2c`

Same changes as B2B — sales loop and CN/DN per-CN body. `customer = default_non_company_customer` (resolved once at top of method as today).

## 8. Code shape for future ports

Future Flipkart/CRED/JioMart port (separate PR) will:
1. Read `mapping.mode_of_payment` once at top of method.
2. Use the same three helpers — `_amazon_init_si_header` will be renamed `_init_ecom_si_header` (parametric on `ecommerce_operator` and `amazon_type` already, just rename the function and the `amazon_type` param to `ecom_subtype` or similar).
3. Pass platform-specific tax tuples to `_amazon_append_si_line`. Field names differ between platforms (Amazon uses `cgst_tax`/`sgst_tax`/`igst_tax`; Flipkart uses `cgst_amount`/`sgst_amount`/`igst_amount`; CRED yet another set), so the caller is responsible for assembling the tuples — the helper just accepts them.

We don't pre-rename in this iteration because rename without a caller is churn. When Flipkart's port lands, rename in the same commit so the diff shows both the rename and the second consumer.

## 9. Stock Transfer

No change. `create_invoice_or_delivery_note` uses Delivery Note + Purchase Receipt for non-taxable transfers. `is_pos` does not apply to inter-company internal customer flows. The shared helpers above accept `is_pos` opt-out via `mode_of_payment=None` if Stock Transfer ever needs to call them, but for now Stock Transfer keeps its dedicated code path.

## 10. Migration / rollout

1. Edit `ecommerce_mapping.json` to add `mode_of_payment` and `mode_of_payment` to `field_order`.
2. Edit `ecommerce_mapping.py`'s `validate()` with the three checks.
3. `bench migrate` adds the DB column.
4. User opens existing "Amazon" Ecommerce Mapping → required field forces them to pick a Mode of Payment. Validation catches uniqueness + missing account.
5. Add helpers to `ecommerce_bill_import.py`.
6. Refactor `create_sales_invoice_mtr_b2b` and `create_sales_invoice_mtr_b2c` (sales + CN/DN loops) to call the helpers.
7. Restart bench. Run a small Amazon B2C re-import on a test file to confirm:
   - SI gets `is_pos=1`, `pos_profile=""`, single payment row at MoP, paid_amount = grand_total, outstanding_amount = 0.
   - Credit notes get a negative payment row matching their negative grand_total.
   - Existing skip behavior + import_summary banner unchanged.

## 11. Risk / regressions

- The two-save dance for clearing `item_tax_template` is sensitive. Validate
  via test import that grand_total + tax allocation match prior runs (no GST
  calc drift).
- POS row's `amount` is set explicitly to `si.grand_total` after save 1.
  If Frappe's GST template clearing in save 2 changes `grand_total`,
  `payments[0].amount` may lag. Save 2's `set_payment_amounts()` re-syncs
  for non-return SIs but NOT for returns. Test path: B2C credit note where
  GST template clearing changes the tax total — verify final
  `paid_amount == grand_total`.
- `is_pos=1` does NOT require `posting_date <= today()` (verified — no
  POS-specific past-date check in ERPNext v15). Backdated Amazon imports work.
- `pos_profile = ""` is graceful in v15 (`set_pos_fields()` returns early
  if no profile, no crash).
- india_compliance e-invoice generation reads `is_pos` (e_invoice.py:755)
  to determine paid_amount reporting. Verify e-invoice JSON for a POS SI
  matches the non-POS shape the GST portal expects.
- business_needed_solutions reads `is_pos` only inside `update_child_items`
  for Sales Order / Purchase Order (NOT Sales Invoice) — no impact.
- `make_sales_return()` flow is NOT used by this codebase — credit notes are
  built directly. So we don't need to mirror its `-1 * paid_amount` logic.
  The explicit `amount = grand_total` assignment is the equivalent.

## 12. Testing checklist

- [ ] `bench migrate` adds column without error.
- [ ] Editing existing Ecommerce Mapping without setting `mode_of_payment` throws.
- [ ] Saving two mappings with the same MoP throws on the second.
- [ ] Saving a mapping with a MoP that has no `accounts` rows throws.
- [ ] B2C re-import of an already-existing file → still skips, banner shows correct counts.
- [ ] B2C fresh import → SIs have `is_pos=1`, `payments[0].mode_of_payment` matches, `paid_amount == grand_total`, `outstanding_amount == 0`.
- [ ] B2C credit note → `is_return=1`, `payments[0].amount` negative matching grand_total.
- [ ] B2C debit note (zero-qty) → `is_debit_note=1`, payment row valid against zero or negative grand_total.
- [ ] B2B sales + multi-CN refund → each CN gets its own SI with correct payment row.
- [ ] Stock Transfer import unchanged (no payment rows).
- [ ] Flipkart import unchanged (no MoP applied yet).
- [ ] India Compliance e-invoice JSON for a POS SI is well-formed and
  reports paid_amount correctly (manual check via India Compliance UI).
- [ ] B2B refund with multiple credit_note_no values still produces one
  return doc per CN (`cn_groups` logic preserved).
