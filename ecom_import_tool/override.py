from erpnext.accounts.doctype.purchase_invoice.purchase_invoice import PurchaseInvoice
from erpnext.accounts.doctype.sales_invoice.sales_invoice import SalesInvoice
from erpnext.stock.doctype.delivery_note.delivery_note import DeliveryNote
from erpnext.stock.doctype.purchase_receipt.purchase_receipt import PurchaseReceipt


class CustomSalesInvoice(SalesInvoice):
	def before_insert(self):
		if getattr(self, '_ecom_name', None):
			self.name = self._ecom_name


class CustomDeliveryNote(DeliveryNote):
	def before_insert(self):
		if getattr(self, '_ecom_name', None):
			self.name = self._ecom_name


class CustomPurchaseReceipt(PurchaseReceipt):
	def before_insert(self):
		if getattr(self, '_ecom_name', None):
			self.name = self._ecom_name


class CustomPurchaseInvoice(PurchaseInvoice):
	def before_insert(self):
		if getattr(self, '_ecom_name', None):
			self.name = self._ecom_name
