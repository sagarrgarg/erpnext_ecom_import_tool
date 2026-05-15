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

	def calculate_taxes_and_totals(self):
		if self.flags.get("billed_item_tax_rates"):
			_BilledTaxCalc(self)
		else:
			_BaseCalc(self)


class CustomDeliveryNote(DeliveryNote):
	def before_insert(self):
		_force_ecom_name(self)


class CustomPurchaseReceipt(PurchaseReceipt):
	def before_insert(self):
		_force_ecom_name(self)


class CustomPurchaseInvoice(PurchaseInvoice):
	def before_insert(self):
		_force_ecom_name(self)
