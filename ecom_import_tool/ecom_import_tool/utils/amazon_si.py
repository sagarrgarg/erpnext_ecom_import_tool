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

import frappe
from frappe.utils import flt, getdate, today


def apply_pos_payment(si, mode_of_payment):
	"""Mark a Sales Invoice as POS-settled 100% via the given Mode of Payment.

	Must be called AFTER si.save() so si.grand_total is computed. Caller then
	saves once more to validate the POS state.

	Skipped (no-op) when:
	  - mode_of_payment is empty (defensive — Ecommerce Mapping validation
	    should prevent this, but caller might be Stock Transfer or a future path)
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
		# Explicit amount = grand_total. Already negative for is_return=1.
		# ERPNext's verify_payment_amount_is_negative() requires this for returns.
		"amount": flt(si.grand_total),
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
