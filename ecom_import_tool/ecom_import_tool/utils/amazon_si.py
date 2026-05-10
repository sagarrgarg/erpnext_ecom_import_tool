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
