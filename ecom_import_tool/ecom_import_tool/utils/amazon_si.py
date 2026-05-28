# Copyright (c) 2026, Sagar Ratan Garg and contributors
# For license information, please see license.txt

"""Shared Sales Invoice helpers for Amazon ecommerce imports.

Three layered helpers + a POS payment helper, called from
`create_sales_invoice_mtr_b2b` / `create_sales_invoice_mtr_b2c` in
the Ecommerce Bill Import doctype:

  * apply_pos_payment(si, mode_of_payment) — mark SI as POS-settled 100%.
  * _amazon_init_si_header(...)            — build the unsaved SI doc.
  * _amazon_append_si_line(...)            — append one item + roll up taxes.
  * _amazon_save_and_submit(...)           — two-save dance + POS + submit.

These helpers are designed to be platform-agnostic in shape so future
Flipkart / CRED / JioMart ports can reuse them with their own param maps.
The `_amazon_*` prefix will become `_ecom_*` once the second consumer
lands; until then it carries the historical name.
"""

import json

import frappe
from frappe.utils import flt, getdate, today


def normalize_tax_rate(rate):
	"""Normalize tax rate to percentage ERPNext expects (5 for 5%).

	Ecommerce CSVs vary — some send 0.05, others send 5. Anything in
	(0, 1) is treated as a fraction and scaled up.
	"""
	rate = flt(rate)
	if 0 < rate < 1:
		return rate * 100
	return rate


def apply_pos_payment(si, mode_of_payment):
	"""Mark a Sales Invoice as POS-settled 100% via the given Mode of Payment.

	Must be called AFTER si.save() so si.grand_total / si.rounded_total are
	computed. Caller then saves once more to validate the POS state.

	Behavior:
	  - Prefer rounded_total when non-zero (it's the after-rounding amount the
	    customer actually pays); fall back to grand_total otherwise.
	  - flags.ignore_pos_profile = True so ERPNext's set_pos_fields() does NOT
	    auto-fetch a default POS Profile on save (we want pos_profile blank).

	Skipped (no-op) when:
	  - mode_of_payment is empty (defensive — Ecommerce Mapping validation
	    should prevent this, but caller might be Stock Transfer or a future path)
	  - settle_amount == 0 (ERPNext only requires payments when grand_total > 0)
	"""
	if not mode_of_payment:
		return
	settle_amount = flt(si.rounded_total) or flt(si.grand_total)
	if not settle_amount:
		return
	# ERPNext enforces sign on POS payments: is_return=1 requires amount<0,
	# is_return=0 requires amount>0 (sales_invoice.py:346-351). Force the
	# payment sign to match is_return so an upstream hook leaving grand_total
	# in an unexpected sign (e.g. India Compliance touching totals on
	# before_save) can't break the SI submit.
	want_negative = bool(si.get("is_return"))
	settle_amount = -abs(settle_amount) if want_negative else abs(settle_amount)
	si.is_pos = 1
	si.pos_profile = ""
	# Block ERPNext set_pos_fields() from backfilling a default POS Profile.
	si.flags.ignore_pos_profile = True
	si.set("payments", [])
	si.append("payments", {
		"mode_of_payment": mode_of_payment,
		"amount": settle_amount,
	})


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
		# Re-stamp return_against + update_stock on the reused draft. An earlier
		# failed pass may have left return_against=None (because the shipment
		# SI wasn't submitted yet) — without this re-stamp the credit note stays
		# a standalone return and triggers ERPNext's "Value cannot be negative
		# for Incoming Rate" validation when items have negative stock_qty.
		si.update_stock = update_stock
		if return_against:
			si.return_against = return_against
		else:
			si.return_against = None
		# Clear stale items / taxes / payments so the caller re-populates with
		# the current run's sign + classification. Old drafts can carry items
		# from a prior import where qty was positive (pre-is_return fix) or
		# taxes were classified the other way — keeping them means the reused
		# draft's grand_total carries the wrong sign and trips "Amount must be
		# negative" on the POS payment validation. The caller's per-row loop
		# is the source of truth for what belongs in this credit note.
		si.set("items", [])
		si.set("taxes", [])
		si.set("payments", [])
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


def _amazon_append_si_line(si, *, item_code, qty, rate, hsn_code, description,
                           warehouse, income_account, custom_ecom_item_id,
                           taxes, is_free_item=False, margin_amount=0,
                           tax_rate_scalar=None):
	"""Append one Sales Invoice item row + roll up taxes onto si.taxes.

	`taxes` is a list of `(tax_type, rate, amount, account_head)` tuples.
	Tax rows with amount==0 are skipped. Multiple lines targeting the same
	`account_head` accumulate into a single si.taxes row.
	"""
	# Per-item billed rates so _BilledTaxCalc.update_item_tax_map can re-stamp
	# them on every recompute. Skip zero-amount taxes (the CSV had no charge
	# under that head for this row).
	billed_rates = {}
	for tax_type, tax_rate, tax_amount, acc_head in taxes:
		if not tax_amount:
			continue
		billed_rates[acc_head] = normalize_tax_rate(tax_rate)

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
		# Clear template so the Item-master tax schedule (which may have moved
		# to a post-rate-change template) doesn't leak into item_tax_rate.
		"item_tax_template": "",
		"item_tax_rate": json.dumps(billed_rates) if billed_rates else "{}",
	}
	if tax_rate_scalar is not None:
		item_row["tax_rate"] = tax_rate_scalar
	if margin_amount:
		item_row["margin_type"] = "Amount"
		item_row["margin_rate_or_amount"] = margin_amount
	if is_free_item:
		item_row["is_free_item"] = 1
	appended_item = si.append("items", item_row)

	if billed_rates:
		rates_map = si.flags.setdefault("billed_item_tax_rates", {})
		rates_map[str(appended_item.idx)] = billed_rates

	for tax_type, tax_rate, tax_amount, acc_head in taxes:
		if not tax_amount:
			continue
		normalized_rate = normalize_tax_rate(tax_rate)
		existing = next((t for t in si.taxes if t.account_head == acc_head), None)
		if existing:
			existing.tax_amount += tax_amount
			existing.rate = normalized_rate
		else:
			si.append("taxes", {
				"charge_type": "On Net Total",
				"account_head": acc_head,
				"rate": normalized_rate,
				"tax_amount": tax_amount,
				"description": tax_type,
				# tax_exclusive_gross / taxable_value on every ecom MTR export is
				# the PRE-tax basic rate; taxes are added ON TOP. Clear both flags
				# explicitly so GST template hooks can't toggle them on save.
				"included_in_print_rate": 0,
				"included_in_paid_amount": 0,
			})


def _amazon_save_and_submit(si, *, mode_of_payment, due_date=None):
	"""Save (so grand_total computes), clear item_tax_template/rate, apply POS
	with the now-known grand_total, save again to validate POS, submit.

	If clearing item_tax_template shifted grand_total on save 2, re-sync the
	POS payment amount to the new total and save once more so paid_amount ==
	grand_total and outstanding_amount == 0. Only fires the extra save when
	the drift is actually nonzero.

	Returns the saved (and submitted) si.
	"""
	si.save(ignore_permissions=True)
	for it in si.items:
		it.item_tax_template = ""
		it.item_tax_rate = frappe._dict()
	# India Compliance auto-applies a state-aware Sales Taxes and Charges
	# Template on save when the customer has a gst_category (B2B path).
	# The template rows carry included_in_print_rate=1, which makes
	# ERPNext treat item.rate as tax-inclusive and back-extract tax —
	# wrong because our rates come from tax_exclusive_gross (already
	# pre-tax). Force the flag to 0 on every tax row so save 2 computes
	# net_amount = rate * qty and tax = net_amount * tax_rate%.
	# Keep taxes_and_charges intact so IC's tax_amount stays consistent
	# with the template's per-item GST rate (clearing it would leave the
	# SI without proper tax classification on save 2).
	for t in (si.get("taxes") or []):
		t.included_in_print_rate = 0
		t.included_in_paid_amount = 0
	# Save once more BEFORE applying POS so India Compliance's
	# update_gst_details (before_save) and _BilledTaxCalc (validate) settle
	# the tax row and grand_total. Otherwise apply_pos_payment locks in a
	# payment amount derived from save-1's grand_total which can drift
	# after save 2's recompute, tripping validate_pos_return with
	# "Total payments amount can't be greater than X".
	si.save(ignore_permissions=True)
	apply_pos_payment(si, mode_of_payment)
	if due_date:
		si.due_date = due_date
	si.save(ignore_permissions=True)

	# Re-sync POS payment if save 2 drifted grand_total. Happens on CRED
	# where GST item_tax_template clearing redistributes line-level taxes
	# slightly, leaving a 40-50 rs residual outstanding.
	if mode_of_payment and si.get("payments") and flt(si.outstanding_amount):
		target = flt(si.rounded_total) or flt(si.grand_total)
		# Same sign-guard as apply_pos_payment — keep target on the side of
		# zero ERPNext expects for this SI's is_return flag.
		if target:
			want_negative = bool(si.get("is_return"))
			target = -abs(target) if want_negative else abs(target)
		if target and flt(si.payments[0].amount) != target:
			si.payments[0].amount = target
			si.save(ignore_permissions=True)

	si.submit()
	return si
