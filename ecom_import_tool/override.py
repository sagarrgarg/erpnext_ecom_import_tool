



from erpnext.accounts.doctype.purchase_invoice.purchase_invoice import PurchaseInvoice
from erpnext.accounts.doctype.sales_invoice.sales_invoice import SalesInvoice
from erpnext.stock.doctype.delivery_note.delivery_note import DeliveryNote
from erpnext.stock.doctype.purchase_receipt.purchase_receipt import PurchaseReceipt


class CustomSalesInvoice(SalesInvoice):
    def before_insert(self):
        if self.custom_ecommerce_invoice_id:
            self.name=self.custom_ecommerce_invoice_id






class CustomDeliveryNote(DeliveryNote):
    def before_insert(self):
        if self.custom_inv_no:
            self.name=self.custom_inv_no



class CustomPurchaseReceipt(PurchaseReceipt):
    def before_insert(self):
        if self.custom_inv_no:
            self.name=self.custom_inv_no


class CustomPurchaseInvoice(PurchaseInvoice):
    def before_insert(self):
        if self.custom_inv_no:
            self.name=self.custom_inv_no