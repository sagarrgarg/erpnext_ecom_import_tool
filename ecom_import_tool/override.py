import json

from erpnext.accounts.doctype.purchase_invoice.purchase_invoice import PurchaseInvoice
from erpnext.accounts.doctype.sales_invoice.sales_invoice import SalesInvoice
from erpnext.controllers.taxes_and_totals import calculate_taxes_and_totals as _BaseCalc
from erpnext.stock.doctype.delivery_note.delivery_note import DeliveryNote
from erpnext.stock.doctype.purchase_receipt.purchase_receipt import PurchaseReceipt


def _force_ecom_name(doc):
	# Frappe runs before_insert BEFORE set_new_name. set_new_name wipes
	# doc.name (naming.py:154) unless flags.name_set is True, then applies
	# the naming_series. Setting the flag pins our ecommerce ID as the
	# final docname.
	ecom_name = getattr(doc, "_ecom_name", None)
	if ecom_name:
		doc.name = ecom_name
		doc.flags.name_set = True


def _force_ecom_tax_settings(doc):
	"""For ecom-imported docs, force tax rows to inc_print=0 and clear
	taxes_and_charges before every validate / calc pass.

	Marketplace CSVs (Amazon B2B/B2C/Stock Transfer, Flipkart, CRED, JioMart)
	export `taxable_value` as a PRE-tax basis — our import passes
	`rate = taxable_value / qty` and stamps tax rows with our explicit
	accounts. If the company's Sales template happens to have
	`included_in_print_rate=1` (rate treated as tax-INCLUSIVE), ERPNext
	would derive net = rate / (1 + tax%) instead of net = rate, and the
	inter-company PI/PR pair (whose Purchase template has inc_print=0)
	would compute a different net from the same row — tripping BNS'
	SI-PI parity check with diffs like SI taxable 671.24 vs PI 751.79.

	Gated on `custom_ecommerce_operator` being set so non-ecom docs (manual
	SIs, regular sales) keep whatever the user configured on their template.
	"""
	if not doc.get("custom_ecommerce_operator"):
		return
	doc.taxes_and_charges = ""
	for t in doc.get("taxes") or []:
		t.included_in_print_rate = 0
		t.included_in_paid_amount = 0


class _BilledTaxCalc(_BaseCalc):
	"""Honor caller-stamped per-item tax rates from the source CSV.

	`doc.flags.billed_item_tax_rates` is `{str(item.idx): {account_head: rate}}`.
	Standard `update_item_tax_map` rebuilds `item.item_tax_rate` from
	`item.item_tax_template`, which would discard CSV-billed rates whenever
	the ERPNext Item Tax Template has moved on to a new GST schedule. This
	override re-stamps `item.item_tax_rate` from the stashed billed rates
	every time calculate runs, so the SI ends up taxed at the rate that was
	actually invoiced on the marketplace.
	"""

	def update_item_tax_map(self):
		billed = self.doc.flags.get("billed_item_tax_rates") if self.doc.flags else None
		if not billed:
			return super().update_item_tax_map()
		for item in self.doc.items:
			cached = billed.get(str(item.idx))
			item.item_tax_rate = json.dumps(cached) if cached else "{}"


class CustomSalesInvoice(SalesInvoice):
	def before_insert(self):
		_force_ecom_name(self)

	def validate(self):
		_force_ecom_tax_settings(self)
		super().validate()

	def calculate_taxes_and_totals(self):
		_force_ecom_tax_settings(self)
		if self.flags.get("billed_item_tax_rates"):
			_BilledTaxCalc(self)
		else:
			_BaseCalc(self)


class CustomDeliveryNote(DeliveryNote):
	def before_insert(self):
		_force_ecom_name(self)

	def validate(self):
		_force_ecom_tax_settings(self)
		super().validate()


class CustomPurchaseReceipt(PurchaseReceipt):
	def before_insert(self):
		_force_ecom_name(self)

	def validate(self):
		_force_ecom_tax_settings(self)
		super().validate()


class CustomPurchaseInvoice(PurchaseInvoice):
	def before_insert(self):
		_force_ecom_name(self)

	def validate(self):
		_force_ecom_tax_settings(self)
		super().validate()
