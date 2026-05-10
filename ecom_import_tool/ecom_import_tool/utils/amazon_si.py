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
