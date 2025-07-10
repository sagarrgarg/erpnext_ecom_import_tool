



from erpnext.accounts.doctype.sales_invoice.sales_invoice import SalesInvoice


class CustomSalesInvoice(SalesInvoice):
    def before_insert(self):
        if self.custom_ecommerce_invoice_id:
            self.name=self.custom_ecommerce_invoice_id
