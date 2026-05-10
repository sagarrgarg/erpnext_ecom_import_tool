# Amazon POS Mode of Payment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a per-mapping Mode of Payment to Ecommerce Mapping; mark every Amazon B2B/B2C Sales Invoice (sale + credit/debit note) as POS settled 100% by that MoP, with header/line/save logic factored into three reusable helpers ready for Flipkart port later.

**Architecture:** Three module-level helpers (`_amazon_init_si_header`, `_amazon_append_si_line`, `_amazon_save_and_submit`) plus `apply_pos_payment` consolidate ~1100 lines of duplicated SI-build logic across the four Amazon create paths. POS application happens between the two existing saves so `grand_total` is known when setting `payments[0].amount` (required by ERPNext for `is_return=1` returns where the auto-fill path doesn't set the negative sign).

**Tech Stack:** Frappe v15, ERPNext v15, MariaDB. Python 3.10+. Tab indentation, 110 char lines (per `CLAUDE.md`).

**Spec:** `docs/superpowers/specs/2026-05-10-amazon-pos-mode-of-payment-design.md`

---

## File Structure

| Action | Path | Responsibility |
|---|---|---|
| Modify | `ecom_import_tool/ecom_import_tool/doctype/ecommerce_mapping/ecommerce_mapping.json` | Add `mode_of_payment` field |
| Modify | `ecom_import_tool/ecom_import_tool/doctype/ecommerce_mapping/ecommerce_mapping.py` | Add `validate()` checks (mandatory, unique, MoP has account) |
| Modify | `ecom_import_tool/ecom_import_tool/doctype/ecommerce_bill_import/ecommerce_bill_import.py` | Add 4 helpers + refactor B2B/B2C sales+CN/DN loops |

No new files. No JS changes (the field is auto-rendered by Frappe form).

---

### Task 1: Add `mode_of_payment` field to Ecommerce Mapping doctype

**Files:**
- Modify: `ecom_import_tool/ecom_import_tool/doctype/ecommerce_mapping/ecommerce_mapping.json`

- [ ] **Step 1: Inspect current field order**

Open the file and locate `field_order` and `fields[]` blocks. Confirm `income_account` appears in `field_order` and has a fields[] entry.

- [ ] **Step 2: Add to field_order**

Insert `"mode_of_payment"` immediately after `"income_account"` in the `field_order` array.

Before:
```json
"income_account",
"column_break_ybgs",
```
After:
```json
"income_account",
"mode_of_payment",
"column_break_ybgs",
```

- [ ] **Step 3: Add field definition**

Insert this block in `fields[]` immediately after the `income_account` entry:

```json
{
  "fieldname": "mode_of_payment",
  "fieldtype": "Link",
  "label": "Mode of Payment",
  "options": "Mode of Payment",
  "reqd": 1,
  "description": "Linked Mode of Payment for POS-style 100% settlement on every Sales Invoice / Credit Note created from this mapping. Must be unique per mapping; must have a default Account configured for at least one company."
},
```

- [ ] **Step 4: Run migrate**

Run: `cd /home/ubuntu/frappe-bench-new && bench migrate 2>&1 | tail -10`
Expected: migrate completes; `Updating customizations for ...` lines, then `Executing 'after_migrate' hooks...`. No traceback.

- [ ] **Step 5: Verify column exists**

Run:
```bash
cd /home/ubuntu/frappe-bench-new && bench --site erpnextkgopl.local console <<'PY'
import frappe
print("mode_of_payment in meta:", "mode_of_payment" in [f.fieldname for f in frappe.get_meta("Ecommerce Mapping").fields])
PY
```
Expected: `mode_of_payment in meta: True`.

- [ ] **Step 6: Commit**

```bash
git add ecom_import_tool/ecom_import_tool/doctype/ecommerce_mapping/ecommerce_mapping.json
git commit -m "feat(ecommerce_mapping): add mode_of_payment link field"
```

---

### Task 2: Add validate() checks on Ecommerce Mapping

**Files:**
- Modify: `ecom_import_tool/ecom_import_tool/doctype/ecommerce_mapping/ecommerce_mapping.py`

- [ ] **Step 1: Read existing class**

Open the file. Note whether a `validate()` method already exists. If yes, we extend it; if no, we add it.

- [ ] **Step 2: Replace / add validate method**

If `validate()` already exists, append the three checks after the existing body. If not, add this method to the class:

```python
def validate(self):
	from frappe import _

	if not self.mode_of_payment:
		# Defensive — reqd=1 in JSON should already throw on save.
		frappe.throw(_("Mode of Payment is mandatory."))

	# Uniqueness across mappings.
	dup = frappe.db.get_value(
		"Ecommerce Mapping",
		{"mode_of_payment": self.mode_of_payment, "name": ["!=", self.name]},
		"name",
	)
	if dup:
		frappe.throw(
			_(
				"Mode of Payment '{0}' is already used by Ecommerce Mapping '{1}'. "
				"Each mapping must have its own MoP for clean reconciliation."
			).format(self.mode_of_payment, dup)
		)

	# MoP must have at least one Accounts row with default_account set.
	mop = frappe.get_doc("Mode of Payment", self.mode_of_payment)
	has_account = any(getattr(r, "default_account", None) for r in (mop.accounts or []))
	if not has_account:
		frappe.throw(
			_(
				"Mode of Payment '{0}' has no Default Account configured. "
				"Open it and set Default Account for at least one company."
			).format(self.mode_of_payment)
		)
```

If `frappe` is not already imported at the top of the file, ensure `import frappe` is present.

- [ ] **Step 3: Manual smoke test — uniqueness**

Through the UI: open the existing "Amazon" Ecommerce Mapping. Set `mode_of_payment` to an existing Mode of Payment (e.g. "Cash"). Save → should succeed.

Open "Flipkart" Ecommerce Mapping. Set the SAME MoP. Save → should throw "is already used by Ecommerce Mapping 'Amazon'."

- [ ] **Step 4: Manual smoke test — missing account**

Pick a Mode of Payment that has an empty `accounts` table (or create a fresh one in the UI). Set it on a mapping. Save → should throw "has no Default Account configured."

- [ ] **Step 5: Manual smoke test — happy path**

Reset Flipkart's MoP to a different one (with accounts configured). Save → should succeed.

- [ ] **Step 6: Commit**

```bash
git add ecom_import_tool/ecom_import_tool/doctype/ecommerce_mapping/ecommerce_mapping.py
git commit -m "feat(ecommerce_mapping): validate mode_of_payment is unique and has a default account"
```

---

### Task 3: Add `apply_pos_payment` helper

**Files:**
- Modify: `ecom_import_tool/ecom_import_tool/doctype/ecommerce_bill_import/ecommerce_bill_import.py`

- [ ] **Step 1: Locate insertion point**

Find the line `def find_existing_amazon_doc(...)` (currently around line 395). The new helper goes immediately after `find_existing_amazon_si` (the convenience wrapper that follows `find_existing_amazon_doc`). Confirm `flt` is already imported at the top of the file (it is, around line 17).

- [ ] **Step 2: Insert helper**

Add this function after `find_existing_amazon_si`:

```python
def apply_pos_payment(si, mode_of_payment):
	"""Mark a Sales Invoice as POS-settled 100% via the given Mode of Payment.

	Must be called AFTER si.save() so si.grand_total is computed. Caller then
	saves once more to validate the POS state.

	Skipped (no-op) when:
	  - mode_of_payment is empty (defensive)
	  - si.grand_total == 0 (ERPNext only requires payments when grand_total > 0)
	"""
	if not mode_of_payment:
		return
	if not flt(si.grand_total):
		return
	si.is_pos = 1
	si.pos_profile = ""
	si.set("payments", [])
	si.append("payments", {
		"mode_of_payment": mode_of_payment,
		# Explicit amount = grand_total — already negative for is_return=1.
		# ERPNext's verify_payment_amount_is_negative() requires this for returns.
		"amount": flt(si.grand_total),
	})
```

- [ ] **Step 3: Syntax check**

Run:
```bash
cd /home/ubuntu/frappe-bench-new && env/bin/python -c "
import ast
ast.parse(open('apps/ecom_import_tool/ecom_import_tool/ecom_import_tool/doctype/ecommerce_bill_import/ecommerce_bill_import.py').read())
print('SYNTAX OK')
"
```
Expected: `SYNTAX OK`.

- [ ] **Step 4: Commit**

```bash
git add ecom_import_tool/ecom_import_tool/doctype/ecommerce_bill_import/ecommerce_bill_import.py
git commit -m "feat(ecom_import): add apply_pos_payment helper"
```

---

### Task 4: Add `_amazon_init_si_header` helper

**Files:**
- Modify: `ecom_import_tool/ecom_import_tool/doctype/ecommerce_bill_import/ecommerce_bill_import.py`

- [ ] **Step 1: Insert helper after apply_pos_payment**

```python
def _amazon_init_si_header(*, customer, posting_dt, ecom_name, is_return,
                           is_debit_note, return_against, ecommerce_operator,
                           amazon_type, ecommerce_gstin, update_stock,
                           draft_doc=None):
	"""Build (or reuse a draft) Sales Invoice header.

	Returns the unsaved Sales Invoice doc with header fields set. Caller is
	responsible for appending items / taxes (via _amazon_append_si_line) and
	mutating header-level state from per-row data (place_of_supply, location,
	set_warehouse, company_address) inside the per-row loop.
	"""
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

- [ ] **Step 2: Syntax check**

Run:
```bash
cd /home/ubuntu/frappe-bench-new && env/bin/python -c "
import ast
ast.parse(open('apps/ecom_import_tool/ecom_import_tool/ecom_import_tool/doctype/ecommerce_bill_import/ecommerce_bill_import.py').read())
print('SYNTAX OK')
"
```
Expected: `SYNTAX OK`.

- [ ] **Step 3: Commit**

```bash
git add ecom_import_tool/ecom_import_tool/doctype/ecommerce_bill_import/ecommerce_bill_import.py
git commit -m "feat(ecom_import): add _amazon_init_si_header helper"
```

---

### Task 5: Add `_amazon_append_si_line` helper

**Files:**
- Modify: `ecom_import_tool/ecom_import_tool/doctype/ecommerce_bill_import/ecommerce_bill_import.py`

- [ ] **Step 1: Insert helper after _amazon_init_si_header**

```python
def _amazon_append_si_line(si, *, item_code, qty, rate, hsn_code, description,
                           warehouse, income_account, custom_ecom_item_id,
                           taxes, is_free_item=False, margin_amount=0,
                           tax_rate_scalar=None):
	"""Append one Sales Invoice item row + roll up taxes onto si.taxes.

	`taxes` is a list of `(tax_type, rate, amount, account_head)` tuples.
	Tax rows with amount==0 are skipped. Multiple lines targeting the same
	`account_head` accumulate into a single si.taxes row.
	"""
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

- [ ] **Step 2: Syntax check**

Run:
```bash
cd /home/ubuntu/frappe-bench-new && env/bin/python -c "
import ast
ast.parse(open('apps/ecom_import_tool/ecom_import_tool/ecom_import_tool/doctype/ecommerce_bill_import/ecommerce_bill_import.py').read())
print('SYNTAX OK')
"
```
Expected: `SYNTAX OK`.

- [ ] **Step 3: Commit**

```bash
git add ecom_import_tool/ecom_import_tool/doctype/ecommerce_bill_import/ecommerce_bill_import.py
git commit -m "feat(ecom_import): add _amazon_append_si_line helper"
```

---

### Task 6: Add `_amazon_save_and_submit` helper

**Files:**
- Modify: `ecom_import_tool/ecom_import_tool/doctype/ecommerce_bill_import/ecommerce_bill_import.py`

- [ ] **Step 1: Insert helper after _amazon_append_si_line**

```python
def _amazon_save_and_submit(si, *, mode_of_payment, due_date=None):
	"""Save (so grand_total computes), clear item_tax_template/rate, apply POS
	with the now-known grand_total, save again to validate POS, submit.

	Returns the saved (and submitted) si.
	"""
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

- [ ] **Step 2: Syntax check**

Run:
```bash
cd /home/ubuntu/frappe-bench-new && env/bin/python -c "
import ast
ast.parse(open('apps/ecom_import_tool/ecom_import_tool/ecom_import_tool/doctype/ecommerce_bill_import/ecommerce_bill_import.py').read())
print('SYNTAX OK')
"
```
Expected: `SYNTAX OK`.

- [ ] **Step 3: Commit**

```bash
git add ecom_import_tool/ecom_import_tool/doctype/ecommerce_bill_import/ecommerce_bill_import.py
git commit -m "feat(ecom_import): add _amazon_save_and_submit helper"
```

---

### Task 7: Refactor B2C sales loop to use the helpers

**Files:**
- Modify: `ecom_import_tool/ecom_import_tool/doctype/ecommerce_bill_import/ecommerce_bill_import.py` (B2C sales section, currently `create_sales_invoice_mtr_b2c` ~lines 1782–1944)

This task converts ONE loop body. We do B2C sales first because it's simpler than B2B and has no Customer-by-GSTIN logic.

- [ ] **Step 1: Locate the SI build block**

Inside `create_sales_invoice_mtr_b2c`, find the block that begins after the `existing_si` skip and ends at `success_count += len(shipment_items)` for the sales path. The current block starts roughly at `if shipment_items:` and contains `frappe.new_doc("Sales Invoice")`, the per-row loop, and the two-save dance.

- [ ] **Step 2: Replace the new_doc / draft branch**

Replace the `if existing_si_draft: ... else: ... si.update_stock = 1` block with a single call:

```python
draft_doc = frappe.get_doc("Sales Invoice", existing_si_draft) if existing_si_draft else None
si = _amazon_init_si_header(
	customer=val,
	posting_dt=parse_export_datetime(items_data[0][1].get("invoice_date")),
	ecom_name=qualified_invoice_no,
	is_return=False,
	is_debit_note=False,
	return_against=None,
	ecommerce_operator=self.ecommerce_mapping,
	amazon_type=self.amazon_type,
	ecommerce_gstin=mapped_ecommerce_gstin,
	update_stock=1,
	draft_doc=draft_doc,
)
```

If `parse_export_datetime` returns None, the header helper will fail since `posting_dt.date()` is called inside. Keep the existing pre-check that raises `Invalid Invoice Date` BEFORE calling the helper:

```python
invoice_dt = parse_export_datetime(items_data[0][1].get("invoice_date"))
if not invoice_dt:
	raise Exception(f"Invalid Invoice Date: {items_data[0][1].get('invoice_date')}")
```

then pass `posting_dt=invoice_dt` to the helper.

- [ ] **Step 3: Replace the per-row si.append("items", {...}) and si.append("taxes", ...) blocks**

Inside the per-row `for idx, child_row in shipment_items:` loop, KEEP the resolution of `itemcode`, `warehouse`/`location`/`com_address`, the warehouse fallback logic, the `place_of_supply` resolution, and the `si.location`/`si.set_warehouse`/`si.company_address`/`si.ecommerce_gstin` mutations.

Replace ONLY the item_row dict + tax tuple loop with one call:

```python
qty = flt(child_row.quantity)
rate = (flt(child_row.tax_exclusive_gross) / qty) if qty else 0
hsn_code = frappe.db.get_value("Item", itemcode, "gst_hsn_code")

_amazon_append_si_line(
	si,
	item_code=itemcode,
	qty=qty,
	rate=rate,
	hsn_code=hsn_code,
	description=child_row.item_description,
	warehouse=warehouse,
	income_account=amazon.income_account,
	custom_ecom_item_id=child_row.shipment_item_id,
	is_free_item=(str(child_row.transaction_type) == "FreeReplacement"),
	margin_amount=flt(child_row.item_promo_discount),
	tax_rate_scalar=flt(child_row.total_tax_amount),
	taxes=[
		("CGST", flt(child_row.cgst_rate), flt(child_row.cgst_tax), "Output Tax CGST - KGOPL"),
		("SGST",
		 flt(child_row.sgst_rate) + flt(child_row.utgst_rate),
		 flt(child_row.sgst_tax) + flt(child_row.utgst_tax),
		 "Output Tax SGST - KGOPL"),
		("IGST", flt(child_row.igst_rate), flt(child_row.igst_tax), "Output Tax IGST - KGOPL"),
	],
)

if child_row.shipment_item_id:
	existing_item_ids.add(child_row.shipment_item_id)
items_append.append(itemcode)
```

Keep the surrounding `try / except item_error: error_log.append(invoice_no); errors.append({...})` wrapping intact.

- [ ] **Step 4: Replace the save + submit block**

Replace the existing block:

```python
if len(items_append) > 0 and not warehouse_mapping_missing:
	si.save(ignore_permissions=True)
	for j in si.items:
		j.item_tax_template = ""
		j.item_tax_rate = frappe._dict()
	si.save(ignore_permissions=True)

if invoice_no not in error_log:
	si.submit()
	frappe.db.commit()
	existing_si = si.name
	success_count += len(shipment_items)
```

with:

```python
if items_append and not warehouse_mapping_missing and invoice_no not in error_log:
	_amazon_save_and_submit(si, mode_of_payment=amazon.mode_of_payment, due_date=getdate(today()))
	existing_si = si.name
	success_count += len(shipment_items)
	frappe.db.commit()
```

- [ ] **Step 5: Syntax check**

Run:
```bash
cd /home/ubuntu/frappe-bench-new && env/bin/python -c "
import ast
ast.parse(open('apps/ecom_import_tool/ecom_import_tool/ecom_import_tool/doctype/ecommerce_bill_import/ecommerce_bill_import.py').read())
print('SYNTAX OK')
"
```
Expected: `SYNTAX OK`.

- [ ] **Step 6: Restart workers + smoke test**

Restart bench (kill the honcho master, run `bench start`), then upload a small B2C MTR file with 2-3 shipment rows to a fresh Ecommerce Bill Import, click Start Import. After it finishes:

```bash
cd /home/ubuntu/frappe-bench-new && bench --site erpnextkgopl.local console <<'PY'
import frappe
si = frappe.get_all("Sales Invoice",
	filters={"custom_ecommerce_operator": "Amazon", "is_return": 0, "docstatus": 1},
	order_by="creation desc", limit=1, fields=["name"])[0]["name"]
d = frappe.get_doc("Sales Invoice", si)
print("name:", d.name)
print("is_pos:", d.is_pos, "pos_profile:", repr(d.pos_profile))
print("grand_total:", d.grand_total, "paid_amount:", d.paid_amount, "outstanding:", d.outstanding_amount)
print("payments:", [(p.mode_of_payment, p.amount) for p in d.payments])
PY
```

Expected: `is_pos=1`, `pos_profile=""`, `paid_amount == grand_total`, `outstanding_amount == 0`, payments table has one row at the configured MoP with amount equal to grand_total.

- [ ] **Step 7: Commit**

```bash
git add ecom_import_tool/ecom_import_tool/doctype/ecommerce_bill_import/ecommerce_bill_import.py
git commit -m "refactor(b2c-sales): use shared SI helpers + apply POS payment"
```

---

### Task 8: Refactor B2C credit-note loop to use the helpers

**Files:**
- Modify: `ecom_import_tool/ecom_import_tool/doctype/ecommerce_bill_import/ecommerce_bill_import.py` (B2C `cn_groups` block in `create_sales_invoice_mtr_b2c`, currently ~lines 1983–2200)

- [ ] **Step 1: Locate the per-CN body**

Inside `create_sales_invoice_mtr_b2c`, find the `for credit_note_no, cn_refund_items in cn_groups.items():` loop. The block begins with `all_zero_qty = ...` and ends at `success_count += len(cn_refund_items)`.

- [ ] **Step 2: Replace the new_doc / draft branch**

Locate the existing `if draft_return: ... else: si_return = frappe.new_doc("Sales Invoice"); ... if not frappe.db.exists("Sales Invoice", qualified_cn_no): si_return._ecom_name = qualified_cn_no` block.

Compute `posting_dt` outside the helper (the existing parse_export_datetime + None check stays):

```python
credit_note_dt = parse_export_datetime(cn_refund_items[0][1].get("credit_note_date"))
if not credit_note_dt:
	raise Exception(f"Invalid Credit Note Date: {cn_refund_items[0][1].get('credit_note_date')}")
```

Then replace the entire if/else creation block with:

```python
draft_doc = frappe.get_doc("Sales Invoice", draft_return) if draft_return else None
si_return = _amazon_init_si_header(
	customer=val,
	posting_dt=credit_note_dt,
	ecom_name=qualified_cn_no,
	is_return=not use_debit_note,
	is_debit_note=use_debit_note,
	return_against=existing_si if not use_debit_note else None,
	ecommerce_operator=self.ecommerce_mapping,
	amazon_type=self.amazon_type,
	ecommerce_gstin=mapped_ecommerce_gstin,
	update_stock=0 if use_debit_note else 1,
	draft_doc=draft_doc,
)
```

- [ ] **Step 3: Replace per-row item append**

Inside `for idx, child_row in cn_refund_items:`, keep itemcode/warehouse/place_of_supply resolution and header mutations intact. Replace the `si_return.append("items", {...})` and the inline tax tuple loop with:

```python
refund_qty, refund_rate, is_zero_qty = safe_refund_qty_rate(
	child_row.quantity, child_row.tax_exclusive_gross
)
if use_debit_note:
	line_qty, line_rate = 0, refund_rate
elif is_zero_qty:
	line_qty, line_rate = -1, refund_rate
else:
	line_qty, line_rate = refund_qty, refund_rate

hsn_code = frappe.db.get_value("Item", itemcode, "gst_hsn_code")

_amazon_append_si_line(
	si_return,
	item_code=itemcode,
	qty=line_qty,
	rate=line_rate,
	hsn_code=hsn_code,
	description=child_row.item_description,
	warehouse=warehouse,
	income_account=amazon.income_account,
	custom_ecom_item_id=child_row.shipment_item_id,
	margin_amount=flt(child_row.item_promo_discount),
	tax_rate_scalar=flt(child_row.total_tax_amount),
	taxes=[
		("CGST", flt(child_row.cgst_rate), flt(child_row.cgst_tax), "Output Tax CGST - KGOPL"),
		("SGST",
		 flt(child_row.sgst_rate) + flt(child_row.utgst_rate),
		 flt(child_row.sgst_tax) + flt(child_row.utgst_tax),
		 "Output Tax SGST - KGOPL"),
		("IGST", flt(child_row.igst_rate), flt(child_row.igst_tax), "Output Tax IGST - KGOPL"),
	],
)
if child_row.shipment_item_id:
	existing_return_item_ids.add(child_row.shipment_item_id)
ritems_append.append(itemcode)
```

Keep the surrounding `try/except item_error` block.

- [ ] **Step 4: Replace the save + submit block**

Replace:

```python
try:
	if len(ritems_append) > 0 and not warehouse_mapping_missing:
		si_return.save(ignore_permissions=True)
		for j in si_return.items:
			j.item_tax_template = ""
			j.item_tax_rate = frappe._dict()
		si_return.save(ignore_permissions=True)

		if invoice_no not in si_error:
			si_return.submit()
			frappe.db.commit()
			success_count += len(cn_refund_items)
except Exception as submit_error:
	for idx, _ in cn_refund_items:
		errors.append({
			"idx": idx,
			"invoice_id": invoice_no,
			"message": f"Error submitting refund invoice: {submit_error}"
		})
```

with:

```python
try:
	if ritems_append and not warehouse_mapping_missing and invoice_no not in si_error:
		_amazon_save_and_submit(si_return, mode_of_payment=amazon.mode_of_payment, due_date=getdate(today()))
		frappe.db.commit()
		success_count += len(cn_refund_items)
except Exception as submit_error:
	for idx, _ in cn_refund_items:
		errors.append({
			"idx": idx,
			"invoice_id": invoice_no,
			"message": f"Error submitting refund invoice: {submit_error}"
		})
```

- [ ] **Step 5: Syntax check**

Run:
```bash
cd /home/ubuntu/frappe-bench-new && env/bin/python -c "
import ast
ast.parse(open('apps/ecom_import_tool/ecom_import_tool/ecom_import_tool/doctype/ecommerce_bill_import/ecommerce_bill_import.py').read())
print('SYNTAX OK')
"
```
Expected: `SYNTAX OK`.

- [ ] **Step 6: Smoke test — credit note**

Restart bench. Use a B2C MTR file containing at least one Refund row with a non-empty `Credit Note No`. Import it. Then:

```bash
cd /home/ubuntu/frappe-bench-new && bench --site erpnextkgopl.local console <<'PY'
import frappe
cn = frappe.get_all("Sales Invoice",
	filters={"custom_ecommerce_operator": "Amazon", "is_return": 1, "docstatus": 1},
	order_by="creation desc", limit=1, fields=["name"])[0]["name"]
d = frappe.get_doc("Sales Invoice", cn)
print("name:", d.name)
print("is_pos:", d.is_pos, "is_return:", d.is_return)
print("grand_total:", d.grand_total, "paid_amount:", d.paid_amount, "outstanding:", d.outstanding_amount)
print("payments:", [(p.mode_of_payment, p.amount) for p in d.payments])
PY
```

Expected: `is_pos=1`, `is_return=1`, `paid_amount == grand_total` (both negative), payment amount negative.

- [ ] **Step 7: Smoke test — zero-qty debit note**

If the test file contains a zero-qty refund (`use_debit_note` path), verify a debit note was created:

```bash
cd /home/ubuntu/frappe-bench-new && bench --site erpnextkgopl.local console <<'PY'
import frappe
dn = frappe.get_all("Sales Invoice",
	filters={"custom_ecommerce_operator": "Amazon", "is_debit_note": 1, "docstatus": 1},
	order_by="creation desc", limit=1, fields=["name"])
if dn:
	d = frappe.get_doc("Sales Invoice", dn[0]["name"])
	print("name:", d.name, "grand_total:", d.grand_total, "is_pos:", d.is_pos)
	print("payments:", [(p.mode_of_payment, p.amount) for p in d.payments])
else:
	print("no debit note in this batch — skip")
PY
```

Expected (when present): `is_pos=1`. If `grand_total == 0`, `payments` is empty (per `apply_pos_payment` zero-grand-total guard). Otherwise payment amount equals grand_total.

- [ ] **Step 8: Commit**

```bash
git add ecom_import_tool/ecom_import_tool/doctype/ecommerce_bill_import/ecommerce_bill_import.py
git commit -m "refactor(b2c-refund): use shared SI helpers + apply POS payment to CN/DN"
```

---

### Task 9: Refactor B2B sales loop to use the helpers

**Files:**
- Modify: `ecom_import_tool/ecom_import_tool/doctype/ecommerce_bill_import/ecommerce_bill_import.py` (`create_sales_invoice_mtr_b2b` sales path, currently ~lines 1255–1432)

B2B differs from B2C in two places: the `customer` resolution (B2B looks up by GSTIN, may auto-create Customer + Address — KEEP this logic AS-IS, unchanged), and the `place_of_supply` is gated by `if status != "Active"`. Everything else mirrors Task 7.

- [ ] **Step 1: Replace the new_doc / draft branch**

Same shape as Task 7 step 2, but pass `customer=customer` (the B2B-resolved customer variable). Keep the existing `parse_export_datetime` + None guard upstream of the helper call.

```python
invoice_dt = parse_export_datetime(items_data[0][1].get("invoice_date"))
if not invoice_dt:
	raise Exception(f"Invalid Invoice Date: {items_data[0][1].get('invoice_date')}")

draft_doc = frappe.get_doc("Sales Invoice", existing_si_draft) if existing_si_draft else None
si = _amazon_init_si_header(
	customer=customer,
	posting_dt=invoice_dt,
	ecom_name=qualified_invoice_no,
	is_return=False,
	is_debit_note=False,
	return_against=None,
	ecommerce_operator=self.ecommerce_mapping,
	amazon_type=self.amazon_type,
	ecommerce_gstin=mapped_ecommerce_gstin,
	update_stock=1,
	draft_doc=draft_doc,
)
```

- [ ] **Step 2: Replace per-row item append**

Inside `for idx, child_row in shipment_items:`, keep itemcode/warehouse resolution + the `if status != "Active":` place_of_supply block. Replace the item dict + tax tuples with `_amazon_append_si_line(...)` exactly as in Task 7 step 3.

- [ ] **Step 3: Replace save + submit**

```python
if items_append and not warehouse_mapping_missing and invoice_no not in error_log:
	_amazon_save_and_submit(si, mode_of_payment=amazon.mode_of_payment, due_date=getdate(today()))
	existing_si = si.name
	success_count += len(shipment_items)
	frappe.db.commit()
```

- [ ] **Step 4: Syntax check**

Run:
```bash
cd /home/ubuntu/frappe-bench-new && env/bin/python -c "
import ast
ast.parse(open('apps/ecom_import_tool/ecom_import_tool/ecom_import_tool/doctype/ecommerce_bill_import/ecommerce_bill_import.py').read())
print('SYNTAX OK')
"
```
Expected: `SYNTAX OK`.

- [ ] **Step 5: Smoke test — B2B sales**

Restart bench. Import a B2B MTR file. Verify the latest B2B SI has `is_pos=1` and a payment row matching `grand_total`:

```bash
cd /home/ubuntu/frappe-bench-new && bench --site erpnextkgopl.local console <<'PY'
import frappe
si = frappe.get_all("Sales Invoice",
	filters={"custom_ecommerce_operator": "Amazon", "custom_ecommerce_type": "MTR B2B",
	         "is_return": 0, "docstatus": 1},
	order_by="creation desc", limit=1, fields=["name"])[0]["name"]
d = frappe.get_doc("Sales Invoice", si)
print("name:", d.name, "is_pos:", d.is_pos, "grand_total:", d.grand_total,
      "paid:", d.paid_amount, "outstanding:", d.outstanding_amount)
print("payments:", [(p.mode_of_payment, p.amount) for p in d.payments])
PY
```

Expected: same shape as B2C — `is_pos=1`, `paid == grand_total`, `outstanding == 0`.

- [ ] **Step 6: Commit**

```bash
git add ecom_import_tool/ecom_import_tool/doctype/ecommerce_bill_import/ecommerce_bill_import.py
git commit -m "refactor(b2b-sales): use shared SI helpers + apply POS payment"
```

---

### Task 10: Refactor B2B credit-note loop to use the helpers

**Files:**
- Modify: `ecom_import_tool/ecom_import_tool/doctype/ecommerce_bill_import/ecommerce_bill_import.py` (B2B `cn_groups` block in `create_sales_invoice_mtr_b2b`, currently ~lines 1442–1660)

- [ ] **Step 1: Replace the new_doc / draft branch**

Mirror Task 8 step 2. Same params, same `posting_dt` source from `cn_refund_items[0]`, same `is_return / is_debit_note / return_against / update_stock` derivation, plus `customer=customer`.

- [ ] **Step 2: Replace per-row item append**

Mirror Task 8 step 3. Keep the B2B-specific `if status != "Active"` place_of_supply gate inside the per-row try.

- [ ] **Step 3: Replace save + submit**

```python
if items_append and not warehouse_mapping_missing and invoice_no not in si_return_error:
	_amazon_save_and_submit(si_return, mode_of_payment=amazon.mode_of_payment, due_date=getdate(today()))
	frappe.db.commit()
	success_count += len(cn_refund_items)
```

(B2B uses `items_append` and `si_return_error`; B2C uses `ritems_append` and `si_error`. Verify by reading the surrounding code which name applies.)

- [ ] **Step 4: Syntax check**

Run:
```bash
cd /home/ubuntu/frappe-bench-new && env/bin/python -c "
import ast
ast.parse(open('apps/ecom_import_tool/ecom_import_tool/ecom_import_tool/doctype/ecommerce_bill_import/ecommerce_bill_import.py').read())
print('SYNTAX OK')
"
```
Expected: `SYNTAX OK`.

- [ ] **Step 5: Smoke test — B2B refund**

Restart bench. Import a B2B MTR file with a Refund row + non-empty Credit Note No. Verify CN created with `is_pos=1` and negative payment amount.

```bash
cd /home/ubuntu/frappe-bench-new && bench --site erpnextkgopl.local console <<'PY'
import frappe
cn = frappe.get_all("Sales Invoice",
	filters={"custom_ecommerce_operator": "Amazon", "custom_ecommerce_type": "MTR B2B",
	         "is_return": 1, "docstatus": 1},
	order_by="creation desc", limit=1, fields=["name"])
if cn:
	d = frappe.get_doc("Sales Invoice", cn[0]["name"])
	print("name:", d.name, "is_pos:", d.is_pos, "grand_total:", d.grand_total,
	      "paid:", d.paid_amount)
	print("payments:", [(p.mode_of_payment, p.amount) for p in d.payments])
else:
	print("no B2B CN in this batch — skip")
PY
```

Expected: `is_pos=1`, `paid_amount == grand_total`, payment amount negative.

- [ ] **Step 6: Commit**

```bash
git add ecom_import_tool/ecom_import_tool/doctype/ecommerce_bill_import/ecommerce_bill_import.py
git commit -m "refactor(b2b-refund): use shared SI helpers + apply POS payment to CN/DN"
```

---

### Task 11: Idempotency + multi-CN regression check

**Files:** none (testing only)

- [ ] **Step 1: Re-import the SAME B2B/B2C file used in Task 7+8+9+10 smoke tests**

Create a new Ecommerce Bill Import doc, attach the same file, click Start Import.

Expected: import status = `Success`, banner says `Amazon B2B/B2C: 0 newly created, N already existed (skipped)`. No new SIs created. No errors.

- [ ] **Step 2: B2B file with multiple credit_note_no values**

If a test file with multiple distinct CNs against the same invoice is available, import it. Verify each unique credit_note_no produced its own return SI:

```bash
cd /home/ubuntu/frappe-bench-new && bench --site erpnextkgopl.local console <<'PY'
import frappe
# replace ORDER_INVOICE with the B2B invoice number that has multiple CNs
INV = "REPLACE-ME"
cns = frappe.db.sql_list("""
	SELECT name FROM `tabSales Invoice`
	WHERE custom_ecommerce_operator='Amazon' AND custom_ecommerce_type='MTR B2B'
	  AND is_return=1 AND ecom_order_id LIKE %s
""", (f"%{INV}%",))
print("CNs created:", cns)
PY
```

Expected: list contains as many SIs as there are distinct credit_note_no values in the source file (NOT collapsed into one).

- [ ] **Step 3: GST verification spot-check**

Pick one POS SI created in Task 7-10, open in the UI. Verify the GST tax row totals match what the source MTR row reported (no double-counting from the two-save / GST template clear).

- [ ] **Step 4: India compliance e-invoice spot-check (manual)**

If the company has India Compliance e-invoicing enabled, generate the e-invoice JSON for one POS SI through the UI. Confirm the JSON's `PayDtls` block reports the paid amount correctly. (Manual UI step — no automated assertion.)

- [ ] **Step 5: Commit any post-test fixes (if needed)**

If any regression surfaced, fix it in a follow-up task and commit. Otherwise, no commit at this step.

---

### Task 12: Update CLAUDE.md with the new field

**Files:**
- Modify: `CLAUDE.md` (project root in `ecom_import_tool/`)

- [ ] **Step 1: Append a section under "Doctypes Modified (via hooks)"**

Add a brief note under existing doctype documentation:

```markdown
## Ecommerce Mapping (one row per platform)

- `mode_of_payment` (Link → Mode of Payment, required, unique per mapping). Drives is_pos=1 on every Amazon SI/CN/DN created from this mapping. The MoP must have a Default Account configured for at least one company.
```

- [ ] **Step 2: Commit**

```bash
git add CLAUDE.md
git commit -m "docs(claude): document mode_of_payment requirement on Ecommerce Mapping"
```

---

### Task 13: Push

- [ ] **Step 1: Verify branch is clean**

Run: `git status`
Expected: working tree clean.

- [ ] **Step 2: Push**

Run: `git push`
Expected: commits land on `upstream/main`.

---

## Self-Review

**Spec coverage:**
- Spec §3 (schema) → Task 1.
- Spec §4 (validation) → Task 2.
- Spec §5 (apply_pos_payment) → Task 3.
- Spec §6a (init_si_header) → Task 4.
- Spec §6b (append_si_line) → Task 5.
- Spec §6c (save_and_submit) → Task 6.
- Spec §6d (caller skeleton) → demonstrated by Tasks 7-10.
- Spec §7 (per-method changes) → Tasks 7-10.
- Spec §10 (rollout) → Tasks 1-12 sequenced as steps; Task 11 is the rollout verification.
- Spec §11 (risk) → covered by Task 11 step 3 (GST drift) and step 4 (e-invoice).
- Spec §12 (testing checklist) → mapped onto Task 1-11 smoke tests.
- Spec §9 (Stock Transfer untouched) → no task touches `create_invoice_or_delivery_note`. Confirmed.

**Placeholder scan:** no TBD/TODO/"implement later" tokens in the plan.

**Type consistency:** helper signatures used in Tasks 7-10 match the definitions in Tasks 4-6 (named args: `customer`, `posting_dt`, `ecom_name`, `is_return`, `is_debit_note`, `return_against`, `ecommerce_operator`, `amazon_type`, `ecommerce_gstin`, `update_stock`, `draft_doc` for init; `item_code`, `qty`, `rate`, `hsn_code`, `description`, `warehouse`, `income_account`, `custom_ecom_item_id`, `taxes`, `is_free_item`, `margin_amount`, `tax_rate_scalar` for line; `mode_of_payment`, `due_date` for save).
