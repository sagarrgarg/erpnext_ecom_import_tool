# Copyright (c) 2026, Sagar Ratan Garg and contributors
# For license information, please see license.txt

import frappe
from frappe import _
from frappe.model.document import Document


class IndiaEcommerceRecoSettings(Document):
	def validate(self):
		# Output accounts are mandatory because every ecom-imported Sales
		# Invoice carries CGST/SGST/IGST tax rows. Input accounts only matter
		# for the Amazon stock-transfer PI/PR pair; we leave them optional so
		# output-only operators (Flipkart / CRED / JioMart) can skip them.
		for fieldname, label in (
			("output_cgst_account", "Output CGST Account"),
			("output_sgst_account", "Output SGST Account"),
			("output_igst_account", "Output IGST Account"),
		):
			if not self.get(fieldname):
				frappe.throw(_("{0} is mandatory.").format(_(label)))


def get_settings():
	"""Cached fetch for the singleton. Use this everywhere instead of
	`frappe.get_single` so reads inside hot loops stay free.
	"""
	return frappe.get_cached_doc("India Ecommerce Reco Settings")


def get_account(kind):
	"""Resolve a GST account head by kind: one of
	'output_cgst' / 'output_sgst' / 'output_igst' /
	'input_cgst'  / 'input_sgst'  / 'input_igst'.

	Throws if a required (output_*) account is missing on the singleton —
	imports must not silently fall through to None for an account_head.
	"""
	settings = get_settings()
	field = f"{kind}_account"
	value = settings.get(field)
	if not value:
		frappe.throw(
			_(
				"{0} is not configured on India Ecommerce Reco Settings. "
				"Open the settings and fill the GST account heads before importing."
			).format(_(field.replace("_", " ").title()))
		)
	return value


def get_sales_taxes_template(*, inter_state):
	"""Return the configured Sales Taxes and Charges Template for the given
	flow direction (inter-state → IGST template, intra-state → CGST+SGST
	template), or None if not configured.
	"""
	settings = get_settings()
	return (
		settings.sales_taxes_template_inter_state
		if inter_state
		else settings.sales_taxes_template_intra_state
	) or None


def get_purchase_taxes_template(*, inter_state):
	"""Return the configured Purchase Taxes and Charges Template for the
	given flow direction, or None if not configured.
	"""
	settings = get_settings()
	return (
		settings.purchase_taxes_template_inter_state
		if inter_state
		else settings.purchase_taxes_template_intra_state
	) or None
