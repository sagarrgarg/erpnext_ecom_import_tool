from erpnext.accounts.doctype.purchase_invoice.purchase_invoice import PurchaseInvoice
from erpnext.accounts.doctype.sales_invoice.sales_invoice import SalesInvoice
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


class CustomSalesInvoice(SalesInvoice):
	def before_insert(self):
		_force_ecom_name(self)


class CustomDeliveryNote(DeliveryNote):
	def before_insert(self):
		_force_ecom_name(self)


class CustomPurchaseReceipt(PurchaseReceipt):
	def before_insert(self):
		_force_ecom_name(self)


class CustomPurchaseInvoice(PurchaseInvoice):
	def before_insert(self):
		_force_ecom_name(self)
