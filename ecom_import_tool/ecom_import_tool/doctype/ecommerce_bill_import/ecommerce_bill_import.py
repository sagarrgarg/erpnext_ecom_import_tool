# Copyright (c) 2025, Sagar Ratan Garg and contributors
# For license information, please see license.txt

import html

from india_compliance.gst_india.utils.gstin_info import get_gstin_info
import frappe
from frappe import _
from frappe.auth import today
from frappe.model.document import Document
from frappe.core.doctype.data_import.importer import Importer
import pandas as pd
import io
import json
from datetime import datetime, timedelta

from frappe.utils.data import get_time
from frappe.utils.file_manager import get_file_path
from frappe.utils import flt, getdate

def normalize_state_key(state):
    if not state:
        return ""
    key = str(state).strip().lower()
    key = " ".join(key.split())      # collapse multiple spaces/newlines
    key = key.replace("&", "and")    # optional, helps for "&" cases
    return key


def normalize_warehouse_id(warehouse_id):
    """Normalize external warehouse id.

    Returns an empty string for blank/NA values so callers can treat it as missing.
    """
    if warehouse_id is None:
        return ""

    warehouse_id_str = str(warehouse_id).strip()
    if not warehouse_id_str:
        return ""

    warehouse_id_lower = warehouse_id_str.lower()
    if warehouse_id_lower in {"na", "n/a", "nan", "none", "null", "-", "0", "0.0"}:
        return ""

    return warehouse_id_str

state_code_dict = {
    "jammu and kashmir": "01-Jammu and Kashmir",
    "jammu & kashmir": "01-Jammu and Kashmir",

    "himachal pradesh": "02-Himachal Pradesh",
    "punjab": "03-Punjab",
    "chandigarh": "04-Chandigarh",
    "uttarakhand": "05-Uttarakhand",
    "haryana": "06-Haryana",
    "delhi": "07-Delhi",
    "rajasthan": "08-Rajasthan",
    "uttar pradesh": "09-Uttar Pradesh",
    "bihar": "10-Bihar",
    "sikkim": "11-Sikkim",
    "arunachal pradesh": "12-Arunachal Pradesh",
    "nagaland": "13-Nagaland",
    "manipur": "14-Manipur",
    "mizoram": "15-Mizoram",
    "tripura": "16-Tripura",
    "meghalaya": "17-Meghalaya",
    "assam": "18-Assam",
    "west bengal": "19-West Bengal",
    "jharkhand": "20-Jharkhand",
    "odisha": "21-Odisha",
    "chhattisgarh": "22-Chhattisgarh",
    "madhya pradesh": "23-Madhya Pradesh",
    "gujarat": "24-Gujarat",

    # ✅ Post-2020 merged UT
    "dadra and nagar haveli and daman and diu": "26-Dadra and Nagar Haveli and Daman and Diu",
    "dadra & nagar haveli & daman & diu": "26-Dadra and Nagar Haveli and Daman and Diu",

    "maharashtra": "27-Maharashtra",
    "karnataka": "29-Karnataka",
    "goa": "30-Goa",
    "lakshadweep": "31-Lakshadweep Islands",
    "lakshadweep islands": "31-Lakshadweep Islands",
    "kerala": "32-Kerala",
    "tamil nadu": "33-Tamil Nadu",
    "puducherry": "34-Puducherry",
    "andaman and nicobar islands": "35-Andaman and Nicobar Islands",
    "telangana": "36-Telangana",
    "andhra pradesh": "37-Andhra Pradesh",

    # ✅ New UT after J&K reorganisation
    "ladakh": "38-Ladakh",

    # ✅ Export / special cases
    "other countries": "96-Other Countries",
    "other territory": "97-Other Territory"
}


class EcommerceBillImport(Document):
	def validate(self):
		if not self.ecommerce_mapping:
			frappe.throw(_("Please select an Ecommerce Mapping"))

	def before_save(self):
		if self.get("__islocal"):
			if self.ecommerce_mapping=="Amazon":
				if self.amazon_type=="MTR B2B":
					self.show_preview()
				elif self.amazon_type=="MTR B2C":
					self.append_mtr_b2c()
				else:
					self.append_stock_transfer_attachment()
			if self.ecommerce_mapping=="CRED":
				self.cred_append()
			if self.ecommerce_mapping=="Flipkart":
				self.append_flipkart()
			if self.ecommerce_mapping=="Jiomart":
				self.append_jio_mart()


	
	

	@frappe.whitelist()
	def create_invoice(self):
		frappe.msgprint("Data Import Started")
		# self.invoice_creation()
		
		job = frappe.enqueue(
		self.invoice_creation,
		queue='long',
		timeout=10000
		)

		return job.id






	def invoice_creation(self):
		frappe.msgprint("Data Import Started")
		if self.ecommerce_mapping=="Amazon":
			if self.amazon_type=="MTR B2B":
				self.create_sales_invoice_mtr_b2b()
				frappe.msgprint("Amazon Data Import Finished")
			elif self.amazon_type=="MTR B2C":
				self.create_sales_invoice_mtr_b2c()
				frappe.msgprint("Amazon Data Import Finished")
			elif self.amazon_type=="Stock Transfer":
				self.create_invoice_or_delivery_note()
				frappe.msgprint("Amazon Data Import Finished")
		if self.ecommerce_mapping=="CRED":
			self.create_cred_sales_invoice()
			frappe.msgprint("Cred Data Import Finished")
		if self.ecommerce_mapping=="Flipkart":
			self.create_flipkart_sales_invoice()
			frappe.msgprint("Flipkart Data Import Finished")
		if self.ecommerce_mapping=="Jiomart":
			self.create_jio_mart()
			frappe.msgprint("Jiomart Data Import Finished")
			

	def show_preview(self):
		self.mtr_b2b=[]
		if self.mtr_b2b_attachment:
			import numpy as np

			def clean(val):
				if pd.isna(val):
					return 0 if isinstance(val, (int, float, np.number)) else ""
				try:
					float_val = float(val)
					return float_val
				except (ValueError, TypeError):
					return "" if pd.isna(val) else val

			csv_file_url = self.mtr_b2b_attachment
			filename = csv_file_url.split('/files/')[-1]
			csv_file_path = get_file_path(filename)

			try:
				df = pd.read_csv(csv_file_path)
			except FileNotFoundError:
				frappe.throw(f"File not found: {csv_file_path}")
			except Exception as e:
				frappe.throw(f"Error reading CSV: {str(e)}")

			for index, row in df.iterrows():
				child_row = self.append("mtr_b2b", {})

				child_row.seller_gstin = clean(row.get('Seller Gstin'))
				child_row.invoice_number = clean(row.get('Invoice Number'))
				child_row.invoice_date = clean(row.get('Invoice Date'))
				child_row.transaction_type = clean(row.get('Transaction Type'))
				child_row.order_id = clean(row.get('Order Id'))
				child_row.shipment_id = clean(row.get('Shipment Id'))
				child_row.shipment_date = clean(row.get('Shipment Date'))
				child_row.order_date = clean(row.get('Order Date'))
				child_row.shipment_item_id = clean(row.get('Shipment Item Id'))
				child_row.quantity = clean(row.get('Quantity'))
				child_row.item_description = clean(row.get('Item Description'))
				child_row.asin = clean(row.get('Asin'))
				child_row.hsnsac = clean(row.get('Hsn/sac'))
				child_row.sku = clean(row.get('Sku'))
				child_row.product_tax_code = clean(row.get('Product Tax Code'))
				child_row.bill_from_city = clean(row.get('Bill From City'))
				child_row.bill_from_state = clean(row.get('Bill From State'))
				child_row.bill_from_country = clean(row.get('Bill From Country'))
				child_row.bill_from_postal_code = clean(row.get('Bill From Postal Code'))
				child_row.ship_from_city = clean(row.get('Ship From City'))
				child_row.ship_from_state = clean(row.get('Ship From State'))
				child_row.ship_from_country = clean(row.get('Ship From Country'))
				child_row.ship_from_postal_code = clean(row.get('Ship From Postal Code'))
				child_row.ship_to_city = clean(row.get('Ship To City'))
				child_row.ship_to_state = clean(row.get('Ship To State'))
				child_row.ship_to_country = clean(row.get('Ship To Country'))
				child_row.ship_to_postal_code = clean(row.get('Ship To Postal Code'))
				child_row.invoice_amount = clean(row.get('Invoice Amount'))
				child_row.tax_exclusive_gross = clean(row.get('Tax Exclusive Gross'))
				child_row.total_tax_amount = clean(row.get('Total Tax Amount'))
				child_row.cgst_rate = clean(row.get('Cgst Rate'))
				child_row.sgst_rate = clean(row.get('Sgst Rate'))
				child_row.utgst_rate = clean(row.get('Utgst Rate'))
				child_row.igst_rate = clean(row.get('Igst Rate'))
				child_row.compensatory_cess_rate = clean(row.get('Compensatory Cess Rate'))
				child_row.principal_amount = clean(row.get('Principal Amount'))
				child_row.principal_amount_basis = clean(row.get('Principal Amount Basis'))
				child_row.cgst_tax = clean(row.get('Cgst Tax'))
				child_row.sgst_tax = clean(row.get('Sgst Tax'))
				child_row.utgst_tax = clean(row.get('Utgst Tax'))
				child_row.igst_tax = clean(row.get('Igst Tax'))
				child_row.compensatory_cess_tax = clean(row.get('Compensatory Cess Tax'))
				child_row.shipping_amount = clean(row.get('Shipping Amount'))
				child_row.shipping_amount_basis = clean(row.get('Shipping Amount Basis'))
				child_row.shipping_cgst_tax = clean(row.get('Shipping Cgst Tax'))
				child_row.shipping_sgst_tax = clean(row.get('Shipping Sgst Tax'))
				child_row.shipping_utgst_tax = clean(row.get('Shipping Utgst Tax'))
				child_row.shipping_igst_tax = clean(row.get('Shipping Igst Tax'))
				child_row.shipping_cess_tax = clean(row.get('Shipping Cess Tax'))
				child_row.gift_wrap_amount = clean(row.get('Gift Wrap Amount'))
				child_row.gift_wrap_amount_basis = clean(row.get('Gift Wrap Amount Basis'))
				child_row.gift_wrap_cgst_tax = clean(row.get('Gift Wrap Cgst Tax'))
				child_row.gift_wrap_sgst_tax = clean(row.get('Gift Wrap Sgst Tax'))
				child_row.gift_wrap_utgst_tax = clean(row.get('Gift Wrap Utgst Tax'))
				child_row.gift_wrap_igst_tax = clean(row.get('Gift Wrap Igst Tax'))
				child_row.gift_wrap_compensatory_cess_tax = clean(row.get('Gift Wrap Compensatory Cess Tax'))
				child_row.item_promo_discount = clean(row.get('Item Promo Discount'))
				child_row.item_promo_discount_basis = clean(row.get('Item Promo Discount Basis'))
				child_row.item_promo_tax = clean(row.get('Item Promo Tax'))
				child_row.shipping_promo_discount = clean(row.get('Shipping Promo Discount'))
				child_row.shipping_promo_discount_basis = clean(row.get('Shipping Promo Discount Basis'))
				child_row.shipping_promo_tax = clean(row.get('Shipping Promo Tax'))
				child_row.gift_wrap_promo_discount = clean(row.get('Gift Wrap Promo Discount'))
				child_row.gift_wrap_promo_discount_basis = clean(row.get('Gift Wrap Promo Discount Basis'))
				child_row.gift_wrap_promo_tax = clean(row.get('Gift Wrap Promo Tax'))
				child_row.tcs_cgst_rate = clean(row.get('Tcs Cgst Rate'))
				child_row.tcs_cgst_amount = clean(row.get('Tcs Cgst Amount'))
				child_row.tcs_sgst_rate = clean(row.get('Tcs Sgst Rate'))
				child_row.tcs_sgst_amount = clean(row.get('Tcs Sgst Amount'))
				child_row.tcs_utgst_rate = clean(row.get('Tcs Utgst Rate'))
				child_row.tcs_utgst_amount = clean(row.get('Tcs Utgst Amount'))
				child_row.tcs_igst_rate = clean(row.get('Tcs Igst Rate'))
				child_row.tcs_igst_amount = clean(row.get('Tcs Igst Amount'))
				child_row.warehouse_id = clean(row.get('Warehouse Id'))
				child_row.fulfillment_channel = clean(row.get('Fulfillment Channel'))
				child_row.payment_method_code = clean(row.get('Payment Method Code'))
				child_row.bill_to_city = clean(row.get('Bill To City'))
				child_row.bill_to_state = clean(row.get('Bill To State'))
				child_row.bill_to_country = clean(row.get('Bill To Country'))
				child_row.bill_to_postalcode = clean(row.get('Bill To Postalcode'))
				child_row.customer_bill_to_gstid = clean(row.get('Customer Bill To Gstid'))
				child_row.customer_ship_to_gstid = clean(row.get('Customer Ship To Gstid'))
				child_row.buyer_name = clean(row.get('Buyer Name'))
				child_row.credit_note_no = clean(row.get('Credit Note No'))
				child_row.credit_note_date = clean(row.get('Credit Note Date'))
				child_row.irn_number = clean(row.get('Irn Number'))
				child_row.irn_filing_status = clean(row.get('Irn Filing Status'))
				child_row.irn_date = clean(row.get('Irn Date'))
				child_row.irn_error_code = clean(row.get('Irn Error Code'))
			if self.mtr_b2b:
					# Use getdate to handle ERPNext date parsing
					self.mtr_b2b.sort(
						key=lambda x: getdate(x.invoice_date) if x.invoice_date else frappe.utils.getdate("1900-01-01")
				)

	def append_mtr_b2c(self):
		self.mtr_b2c = []
		if self.mtr_b2c_attachment:
			import numpy as np
			import pandas as pd  # make sure pandas is imported if it's not already
			from frappe.utils.data import getdate

			def clean(val):
				if pd.isna(val):
					return 0 if isinstance(val, (int, float, np.number)) else ""
				try:
					float_val = float(val)
					return float_val
				except (ValueError, TypeError):
					return "" if pd.isna(val) else val

			csv_file_url = self.mtr_b2c_attachment
			filename = csv_file_url.split('/files/')[-1]
			csv_file_path = get_file_path(filename)

			try:
				df = pd.read_csv(csv_file_path)
			except FileNotFoundError:
				frappe.throw(f"File not found: {csv_file_path}")
			except Exception as e:
				frappe.throw(f"Error reading CSV: {str(e)}")

			for index, row in df.iterrows():
				child_row = self.append("mtr_b2c", {})
				for column_name in df.columns:
					fieldname = column_name.strip().lower().replace(' ', '_')
					value = row[column_name]
					if fieldname in [d.fieldname for d in frappe.get_meta('Amazon MTR B2C').fields]:
						child_row.set(fieldname, clean(value))
				# Set HSNSAC
				child_row.set("hsnsac", clean(row.get('Hsn/sac')))

			# Sort the child table by invoice_date ascending
			if self.mtr_b2c:
				# Use getdate to handle ERPNext date parsing
				self.mtr_b2c.sort(
					key=lambda x: getdate(x.invoice_date) if x.invoice_date else frappe.utils.getdate("1900-01-01")
            )

	


	def append_stock_transfer_attachment(self):
		self.stock_transfer=[]
		if self.stock_transfer_attachment:
			import numpy as np

			def clean(val):
				if pd.isna(val):
					return 0 if isinstance(val, (int, float, np.number)) else ""
				try:
					float_val = float(val)
					return float_val
				except (ValueError, TypeError):
					return "" if pd.isna(val) else val

			
			csv_file_url = self.stock_transfer_attachment
			filename = csv_file_url.split('/files/')[-1]
			csv_file_path = get_file_path(filename)

			try:
				df = pd.read_csv(csv_file_path)
			except FileNotFoundError:
				frappe.throw(f"File not found: {csv_file_path}")
			except Exception as e:
				frappe.throw(f"Error reading CSV: {str(e)}")

			for index, row in df.iterrows():
				child_row = self.append("stock_transfer", {})
				for column_name in df.columns:
					# Clean the column name to match ERPNext fieldname conventions
					fieldname = column_name.strip().lower().replace(' ', '_')
					value = row[column_name]

					# If the field exists on the child table, set it
					if fieldname in [d.fieldname for d in frappe.get_meta('Amazon Stock Transfer').fields]:
						child_row.set(fieldname, clean(value))
						print("fieldname5667e787e8e89",value)
					child_row.set("hsnsac", clean(clean(row.get('Hsn/sac'))))

			if self.stock_transfer:
				# Use getdate to handle ERPNext date parsing
				self.stock_transfer.sort(
					key=lambda x: getdate(x.invoice_date) if x.invoice_date else frappe.utils.getdate("1900-01-01")
            )
				
	
	def cred_append(self):
		self.cred_items = []
		self.cred=[]
		if self.cred_attach:
			import pandas as pd
			import numpy as np
			import frappe
			from frappe.utils.file_manager import get_file_path
			import os

			# Helper function to clean and convert Excel values
			def clean(val):
				if pd.isna(val):
					return ""

				# Convert Excel serial to date string if in valid range
				if isinstance(val, (int, float)) and 30000 < val < 50000:
					try:
						return (datetime(1899, 12, 30) + timedelta(days=val)).strftime("%Y-%m-%d")
					except:
						pass

				# If it's already a datetime object
				if isinstance(val, datetime):
					return val.strftime("%Y-%m-%d")

				try:
					return str(val).strip()
				except Exception:
					return str(val)


			print(f"Attachment URL: {self.cred_attach}")

			xl_file_url = self.cred_attach
			filename = xl_file_url.split('/files/')[-1]
			print(f"Filename extracted: {filename}")

			xl_file_path = get_file_path(filename)
			print(f"File path: {xl_file_path}")

			if not os.path.exists(xl_file_path):
				frappe.throw(f"File not found at path: {xl_file_path}")

			# Read Excel file using appropriate header row
			df = pd.read_excel(xl_file_path, sheet_name=1)
			df2 = pd.read_excel(xl_file_path, sheet_name=0)

			# Get child table doctype name
			child_doctype = None
			for field in frappe.get_meta(self.doctype).fields:
				if field.fieldname == 'cred_items':
					child_doctype = field.options
					break

			if not child_doctype:
				frappe.throw("Could not find child table doctype for 'cred_items'")


			# Get child table fields
			child_meta = frappe.get_meta(child_doctype)
			valid_fields = [field.fieldname for field in child_meta.fields]

			success_count = 0
			for index, row in df.iterrows():

				child_row = self.append("cred_items", {})
				mapped_count = 0

				for column_name in df.columns:
					fieldname = column_name.strip().lower().replace(' ', '_')
					value = row[column_name]

					if fieldname in valid_fields:
						child_row.set(fieldname, clean(value))
						mapped_count += 1
				if mapped_count > 0:
					success_count += 1
			for index, row in df2.iterrows():
				child_row2 = self.append("cred", {})
				mapped_count = 0
				for column_name in df2.columns:
					fieldname = column_name.strip().lower().replace(' ', '_')
					value = row[column_name]
					child_row2.set(fieldname, clean(value))
					mapped_count += 1

				if mapped_count > 0:
					success_count += 1

			print(f"Total rows successfully mapped: {success_count}")

	def append_flipkart(self):
		import pandas as pd
		import frappe
		from frappe.utils.file_manager import get_file_path

		def clean(val):
			"""Cleans cell value by removing spaces and surrounding quotes."""
			if pd.isna(val):
				return ""
			val = str(val).strip()
			while (val.startswith('"') and val.endswith('"')) or (val.startswith("'") and val.endswith("'")):
				val = val[1:-1].strip()
			return val

		# Check if file attached
		if not self.flipkart_attach:
			frappe.throw("Please attach a Flipkart CSV file before importing.")

		# Get full path
		try:
			filename = self.flipkart_attach.split("/files/")[-1]
			file_path = get_file_path(filename)
		except Exception as e:
			frappe.throw(f"Unable to find or access the file: {str(e)}")

		# Load CSV
		try:
			df = pd.read_csv(file_path)
		except Exception as e:
			frappe.throw(f"Failed to read CSV file: {str(e)}")

		# Reset child table
		self.set("flipkart_items", [])

		# Get valid fieldnames from child DocType
		valid_fields = [d.fieldname for d in frappe.get_meta("Flipkart Items").fields]

		# Iterate through rows
		for _, row in df.iterrows():
			child = self.append("flipkart_items", {})
			for column in df.columns:
				fieldname = column.strip().lower().replace(" ", "_")
				if fieldname in valid_fields:
					child.set(fieldname, clean(row[column]))

			# Handle specific fields explicitly
			child.set("product_titledescription", clean(row.get("Product Title/Description", "")))
			child.set("order_shipped_from_state", clean(row.get("Order Shipped From (State)", "")))
			child.set("price_after_discount", clean(row.get("Price after discount (Price before discount-Total discount)", "")))
			child.set("final_invoice_amount", clean(row.get("Final Invoice Amount (Price after discount+Shipping Charges)", "")))
			child.set("taxable_value", clean(row.get("Taxable Value (Final Invoice Amount -Taxes)", "")))
			child.set("sgst_rate", clean(row.get("SGST Rate (or UTGST as applicable)", "")))
			child.set("sgst_amount", clean(row.get("SGST Amount (Or UTGST as applicable)", "")))
			child.set("customers_billing_pincode", clean(row.get("Customer's Billing Pincode", "")))
			child.set("customers_billing_state", clean(row.get("Customer's Billing State", "")))
			child.set("customers_delivery_pincode", clean(row.get("Customer's Delivery Pincode", "")))
			child.set("customers_delivery_state", clean(row.get("Customer's Delivery State", "")))
			child.set("is_shopsy_order", clean(row.get("Is Shopsy Order?", "")))


	

					
	def append_jio_mart(self):
		self.jio_mart_items = []
		if self.jio_mart_attach:
			import numpy as np
			import pandas as pd  # make sure pandas is imported if it's not already
			from frappe.utils.data import getdate

			def clean(val):
				if pd.isna(val):
					return 0 if isinstance(val, (int, float, np.number)) else ""
				try:
					float_val = float(val)
					return float_val
				except (ValueError, TypeError):
					return "" if pd.isna(val) else val

			csv_file_url = self.jio_mart_attach
			filename = csv_file_url.split('/files/')[-1]
			csv_file_path = get_file_path(filename)

			try:
				df = pd.read_csv(csv_file_path)
			except FileNotFoundError:
				frappe.throw(f"File not found: {csv_file_path}")
			except Exception as e:
				frappe.throw(f"Error reading CSV: {str(e)}")

			for index, row in df.iterrows():
				child_row = self.append("jio_mart_items", {})
				for column_name in df.columns:
					fieldname = column_name.strip().lower().replace(' ', '_')
					value = row[column_name]
					if fieldname in [d.fieldname for d in frappe.get_meta('Jio Mart').fields]:
						child_row.set(fieldname, clean(value))
				# Set HSNSAC
				child_row.set("taxable_value", clean(row.get('Taxable Value (Final Invoice Amount -Taxes)')))
				child_row.set("final_invoice_amount_offer_price_minus_seller_coupon_amount", clean(row.get('Final Invoice Amount (Offer Price minus Seller Coupon Amount)')))
				child_row.set("product_titledescription", clean(row.get('Product Title/Description')))
				child_row.set("fsn__product_id", clean(row.get('FSN / Product ID')))
				child_row.set("salesale_reversal_tcs_date", clean(row.get('Sale/Sale reversal TCS date')))
				child_row.set("order_shipped_from_state", clean(row.get('Order Shipped From (State)')))
				child_row.set("order_billed_from_state", clean(row.get('Order Billed From (State)')))
				child_row.set("customers_billing_pincode", clean(row.get("Customer's Billing Pincode")))
				child_row.set("customers_billing_state", clean(row.get("Customer's Billing State")))
				child_row.set("customers_delivery_pincode", clean(row.get("Customer's Delivery Pincode")))
				child_row.set("customers_delivery_state", clean(row.get("Customer's Delivery State")))
				child_row.set("sgst_rate_or_utgst_as_applicable", clean(row.get("SGST Rate (or UTGST as applicable)")))
				child_row.set("sgst_amount_or_utgst_as_applicable", clean(row.get("SGST Amount (Or UTGST as applicable)")))



			# Sort the child table by invoice_date ascending
			if self.jio_mart_items:
				# Use getdate to handle ERPNext date parsing
				self.jio_mart_items.sort(
					key=lambda x: getdate(x.buyer_invoice_date) if x.buyer_invoice_date else frappe.utils.getdate("1900-01-01")
            )

	@frappe.whitelist()
	def create_sales_invoice_mtr_b2b(self):
		from frappe.utils import today, getdate, flt
		import json

		error_names=[]
		errors = []
		success_count = 0
		invoice_groups = {}

		# Group rows by Invoice
		for idx, child_row in enumerate(self.mtr_b2b, 1):
			invoice_no = child_row.invoice_number
			if invoice_no not in invoice_groups:
				invoice_groups[invoice_no] = []
			invoice_groups[invoice_no].append((idx, child_row))

		total_invoices = len(invoice_groups) or 1  # avoid div-by-zero

		# 🔹 Initial realtime update
		frappe.publish_realtime(
			"data_import_progress",
			{"progress": 0, "message": f"Starting Amazon B2B import (0/{total_invoices})"},
			user=frappe.session.user
		)

		# Process each invoice group
		for count, (invoice_no, items_data) in enumerate(invoice_groups.items(), start=1):
			try:
				shipment_items = [x for x in items_data if x[1].get("transaction_type") not in ["Refund","Cancel"]]
				refund_items = [x for x in items_data if x[1].get("transaction_type") == "Refund"]
				status=None
				gst_details={}
				customer = frappe.db.get_value("Customer", {"gstin": items_data[0][1].get("customer_bill_to_gstid")}, "name")
				if not customer:
					if len(str(items_data[0][1].get("customer_bill_to_gstid")))==15:
						gst_details=get_gstin_info(items_data[0][1].get("customer_bill_to_gstid"))
						status=gst_details.get("status")
					if status=="Active":
						cus = frappe.new_doc("Customer")
						cus.gstin = items_data[0][1].get("customer_bill_to_gstid")
						cus.customer_name = gst_details.get("business_name")
						cus.gst_category=gst_details.get("gst_category")
						cus.customer_group="Amazon B2b"
						cus.save(ignore_permissions=True)
						customer = cus.name
						if len(gst_details.get("all_addresses"))>0:
							count_addr=0
							for add in gst_details.get("all_addresses"):
								count_addr+=1
								address=frappe.new_doc("Address")
								address.address_type="Billing"
								address.title=str(gst_details.get("business_name"))+"-"+str(count_addr)
								address.address_line1=add.get("address_line1")
								address.address_line2=add.get("address_line2")
								address.city=add.get("city")
								address.state=add.get("state")
								address.country=add.get("country")
								address.pincode=add.get("pincode")
								address.is_primary_address=1
								address.is_shipping_address=1
								address.gstin=items_data[0][1].get("customer_bill_to_gstid")
								address.append("links",{
									"link_doctype":"Customer",
									"link_name":customer
								})
								address.save(ignore_permissions=True)
					else:
						customer=frappe.db.get_value("Ecommerce Mapping", {"platform": "Amazon"}, "default_non_company_customer")

				if not customer:
					customer=frappe.db.get_value("Ecommerce Mapping", {"platform": "Amazon"}, "default_non_company_customer")

				existing_si_draft = frappe.db.get_value("Sales Invoice", {"custom_inv_no": invoice_no, "docstatus": 0, "is_return": 0}, "name")
				existing_si = frappe.db.get_value("Sales Invoice", {"custom_inv_no": invoice_no, "docstatus": 1, "is_return": 0}, "name")

				amazon = frappe.get_doc("Ecommerce Mapping", {"platform": "Amazon"})
				error_log=[]
				warehouse_mapping_missing = False
				
				if shipment_items:
					exists_in_item = frappe.db.sql("""
						SELECT sii.name FROM `tabSales Invoice Item` sii
						JOIN `tabSales Invoice` si ON sii.parent = si.name
						WHERE sii.custom_ecom_item_id = %s AND si.docstatus != 1 AND si.is_return = 0
						""", items_data[0][1].get("shipment_item_id"))
					if exists_in_item:
						# 🔹 Per-invoice group progress update before continue
						percent = int((count / total_invoices) * 100)
						frappe.publish_realtime(
							"data_import_progress",
							{"progress": percent, "message": f"Processed {count}/{total_invoices} invoices"},
							user=frappe.session.user
						)
						continue
					try:
						if existing_si_draft:
							si = frappe.get_doc("Sales Invoice", existing_si_draft)
						else:
							si = frappe.new_doc("Sales Invoice")
							si.customer = customer
							si.set_posting_time=1
							# Parse the datetime and add 2 seconds
							# invoice_datetime = datetime.strptime(str(items_data[0][1].get("invoice_date")), '%Y-%m-%d %H:%M:%S') if isinstance(items_data[0][1].get("invoice_date"), str) else items_data[0][1].get("invoice_date")
							# invoice_datetime_plus_2 = invoice_datetime + timedelta(seconds=2)
							si.posting_date = getdate(items_data[0][1].get("invoice_date"))
							si.posting_time = get_time(items_data[0][1].get("invoice_date"))
							si.custom_inv_no = invoice_no
							si.custom_ecommerce_invoice_id=invoice_no
							si.__newname=invoice_no
							si.custom_ecommerce_operator=self.ecommerce_mapping
							si.custom_ecommerce_type=self.amazon_type
							si.taxes = []
							si.update_stock = 1
						items_append=[]
						for idx, child_row in shipment_items:
							try:
								itemcode = next((i.erp_item for i in amazon.ecom_item_table if i.ecom_item_id == child_row.get(amazon.ecom_sku_column_header)), None)
								if not itemcode:
									error_names.append(invoice_no)
									raise Exception(f"Item mapping not found for SKU: {child_row.get(amazon.ecom_sku_column_header)}")
								warehouse, location, com_address = None, None, None
								for wh_map in amazon.ecommerce_warehouse_mapping:
									print("##############################2345",child_row.warehouse_id)
									if wh_map.ecom_warehouse_id == child_row.warehouse_id:
										warehouse = wh_map.erp_warehouse
										location = wh_map.location
										com_address = wh_map.erp_address
										break
								print("##########################",child_row.warehouse_id,warehouse)
								if not warehouse:
									print("##############################AJ",warehouse)
									warehouse_mapping_missing = True
									error_names.append(invoice_no)
									raise Exception(f"Warehouse Mapping not found for Warehouse Id: {child_row.warehouse_id}")

								ecommerce_gstin = None
								for gstin in amazon.ecommerce_gstin_mapping:
									if gstin.ecommerce_operator_gstin == child_row.seller_gstin:
										# ecommerce_gstin = gstin.ecommerce_operator_gstin
										break

								if location:
									si.location = location
								if warehouse:
									si.set_warehouse = warehouse

								si.company_address = com_address
								si.ecommerce_gstin = ecommerce_gstin
								hsn_code=frappe.db.get_value("Item",itemcode,"gst_hsn_code")
								if status!="Active":
									if child_row.ship_to_state:
										state=child_row.ship_to_state
										if not state_code_dict.get(str(state.lower())):
											error_names.append(invoice_no)
											raise Exception(f"State name Is Wrong Please Check")
										si.place_of_supply=state_code_dict.get(str(state.lower()))

								si.append("items", {
									"item_code": itemcode,
									"qty": flt(child_row.quantity),
									"rate": flt(child_row.tax_exclusive_gross)/flt(child_row.quantity),
									"description": child_row.item_description,
									"warehouse": warehouse,
									"gst_hsn_code":hsn_code,
									"tax_rate": flt(child_row.total_tax_amount),
									"margin_type": "Amount" if flt(child_row.item_promo_discount) > 0 else None,
									"margin_rate_or_amount": flt(child_row.item_promo_discount),
									"income_account": amazon.income_account,
									"is_free_item": 1 if str(child_row.transaction_type) == "FreeReplacement" else 0
								})
								items_append.append(itemcode)
								for tax_type,rate, amount, acc_head in [
									("CGST", flt(child_row.cgst_rate),flt(child_row.cgst_tax), "Output Tax CGST - KGOPL"),
									("SGST",flt(child_row.sgst_rate)+flt(child_row.utgst_rate), flt(child_row.sgst_tax)+flt(child_row.utgst_tax), "Output Tax SGST - KGOPL"),
									("IGST", flt(child_row.igst_rate),flt(child_row.igst_tax) ,"Output Tax IGST - KGOPL")
									]:
										if amount>0:
											existing_tax = next((t for t in si.taxes if t.account_head == acc_head), None)
											if existing_tax:
												existing_tax.tax_amount += amount
											else:
												si.append("taxes", {
													"charge_type": "On Net Total",
													"account_head": acc_head,
													"rate":rate,
													"tax_amount": amount,
													"description": tax_type
												})
							except Exception as item_error:
								error_log.append(invoice_no)
								errors.append({
									"idx": idx,
									"invoice_id": invoice_no,
									"message": f"Shipment item error: {str(item_error)}"
								})
						if len(items_append)>0 and not warehouse_mapping_missing:
							si.save(ignore_permissions=True)
							for j in si.items:
								j.item_tax_template = ""
								j.item_tax_rate = frappe._dict()
							si.save(ignore_permissions=True)
						if invoice_no not in error_log:
							si.submit()
							existing_si = si.name
							success_count += len(shipment_items)
						
					except Exception as ship_err:
						for idx, _ in shipment_items:
							errors.append({
								"idx": idx,
								"invoice_id": invoice_no,
								"message": f"Shipment processing error: {str(ship_err)}"
							})

				if refund_items and existing_si_draft and not existing_si and not warehouse_mapping_missing:
					draft_si = frappe.get_doc("Sales Invoice", existing_si_draft)
					if draft_si.custom_inv_no not in error_log:
						draft_si.submit()
						existing_si = draft_si.name

				si_return_error=[]
				if refund_items and not warehouse_mapping_missing:
					exists_in_item = frappe.db.sql("""
						SELECT sii.name FROM `tabSales Invoice Item` sii
						JOIN `tabSales Invoice` si ON sii.parent = si.name
						WHERE sii.custom_ecom_item_id = %s AND si.docstatus != 1 AND si.is_return = 1
						""", child_row.shipment_item_id)
					if exists_in_item:
						# 🔹 Per-invoice group progress update before continue
						percent = int((count / total_invoices) * 100)
						frappe.publish_realtime(
							"data_import_progress",
							{"progress": percent, "message": f"Processed {count}/{total_invoices} invoices"},
							user=frappe.session.user
						)
						continue
					try:
						# if not existing_si:
						# 	si_return_error.append(invoice_no)
						# 	errors.append({
						# 		"idx": refund_items[0][0],
						# 		"invoice_id": invoice_no,
						# 		"message": f"Refund requested but original submitted invoice not found for {invoice_no}."
						# 	})

						si_return = frappe.new_doc("Sales Invoice")
						si_return.is_return = 1
						si_return.custom_ecommerce_operator=self.ecommerce_mapping
						si_return.custom_ecommerce_type=self.amazon_type
						si_return.customer = customer
						si_return.set_posting_time=1
						# Parse the datetime and add 1 minute for returns
						si_return.posting_date = getdate(refund_items[0][1].get("credit_note_date"))
						si_return.posting_time = get_time(refund_items[0][1].get("credit_note_date"))
						si_return.custom_ecommerce_invoice_id=refund_items[0][1].get("credit_note_no")
						si_return.__newname = refund_items[0][1].get("credit_note_no")
						si_return.custom_inv_no = invoice_no
						si_return.update_stock = 1
						items_append=[]
						for idx, child_row in refund_items:
							try:
								itemcode = next((i.erp_item for i in amazon.ecom_item_table if i.ecom_item_id == child_row.get(amazon.ecom_sku_column_header)), None)
								if not itemcode:
									error_names.append(invoice_no)
									raise Exception(f"Item mapping not found for SKU: {child_row.get(amazon.ecom_sku_column_header)}")
								warehouse, location, com_address = None, None, None
								for wh_map in amazon.ecommerce_warehouse_mapping:
									if wh_map.ecom_warehouse_id == child_row.warehouse_id:
										warehouse = wh_map.erp_warehouse
										location = wh_map.location
										com_address = wh_map.erp_address
										break

								if not warehouse:
									warehouse_mapping_missing = True
									error_names.append(invoice_no)
									raise Exception(f"Warehouse Mapping not found for Warehouse Id: {child_row.warehouse_id}")

								ecommerce_gstin = None
								for gstin in amazon.ecommerce_gstin_mapping:
									if gstin.ecommerce_operator_gstin == child_row.seller_gstin:
										# ecommerce_gstin = gstin.ecommerce_operator_gstin
										break
								if status!="Active":
									if child_row.ship_to_state:
										state=child_row.ship_to_state
										if not state_code_dict.get(str(state.lower())):
											error_names.append(invoice_no)
											raise Exception(f"State name Is Wrong Please Check")
										si_return.place_of_supply=state_code_dict.get(str(state.lower()))

								if not si_return.location:
									si_return.location = location
								if not si_return.set_warehouse:
									si_return.set_warehouse = warehouse

								si_return.company_address = com_address
								si_return.ecommerce_gstin = ecommerce_gstin
								hsn_code=frappe.db.get_value("Item",itemcode,"gst_hsn_code")

								si_return.append("items", {
									"item_code": itemcode,
									"qty": -abs(flt(child_row.quantity)),
									"rate": abs(flt(child_row.tax_exclusive_gross)) / abs(flt(child_row.quantity)),
									"description": child_row.item_description,
									"gst_hsn_code":hsn_code,
									"warehouse": warehouse,
									"tax_rate": flt(child_row.total_tax_amount),
									"margin_type": "Amount" if flt(child_row.item_promo_discount) > 0 else None,
									"margin_rate_or_amount": flt(child_row.item_promo_discount),
									"income_account": amazon.income_account,
								})
								for tax_type,rate, amount, acc_head in [
									("CGST", flt(child_row.cgst_rate),flt(child_row.cgst_tax), "Output Tax CGST - KGOPL"),
									("SGST",flt(child_row.sgst_rate)+flt(child_row.utgst_rate), flt(child_row.sgst_tax)+flt(child_row.utgst_tax), "Output Tax SGST - KGOPL"),
									("IGST", flt(child_row.igst_rate),flt(child_row.igst_tax) ,"Output Tax IGST - KGOPL")
									]:
										if amount>0:
											existing_tax = next((t for t in si_return.taxes if t.account_head == acc_head), None)
											if existing_tax:
												existing_tax.tax_amount += amount
											else:
												si_return.append("taxes", {
													"charge_type": "On Net Total",
													"account_head": acc_head,
													"rate":rate,
													"tax_amount": amount,
													"description": tax_type
												})
								items_append.append(invoice_no)
							except Exception as item_error:
								si_return_error.append(invoice_no)
								errors.append({
									"idx": idx,
									"invoice_id": invoice_no,
									"message": f"Refund item error: {str(item_error)}"
								})
						if len(items_append)>0 and not warehouse_mapping_missing:
							si_return.save(ignore_permissions=True)
							for j in si_return.items:
								j.item_tax_template = ""
								j.item_tax_rate = frappe._dict()
							si_return.save(ignore_permissions=True)

						if invoice_no not in si_return_error:
							si_return.submit()
							success_count += len(refund_items)
					except Exception as refund_err:
						for idx, _ in refund_items:
							errors.append({
								"idx": idx,
								"invoice_id": invoice_no,
								"message": f"Shipment item error: {refund_err}"
							})

			except Exception as e:
				for idx, _ in items_data:
					errors.append({
						"idx": idx,
						"invoice_id": invoice_no,
						"message": f"Invoice processing error: {str(e)}"
					})

			# 🔹 Realtime progress update after each invoice group
			percent = int((count / total_invoices) * 100)
			frappe.publish_realtime(
				"data_import_progress",
				{"progress": percent, "message": f"Processed {count}/{total_invoices} invoices"},
				user=frappe.session.user
			)

		# -------- Final Summary --------
		if errors:
			self.status = "Partial Success" if success_count else "Error"
			indicator = "orange" if success_count else "red"
			frappe.msgprint(f"{success_count} items processed, {len(errors)} failed. Check error HTML for details.", indicator=indicator, alert=True)
		else:
			self.error_html = ""
			self.status = "Success"
			frappe.msgprint(f"All {success_count} items processed successfully!", indicator="green")

		self.error_json = str(json.dumps(errors))
		self.save()

		# 🔹 Final realtime update
		frappe.publish_realtime(
			"data_import_progress",
			{"progress": 100, "message": "Amazon B2B Import Completed ✅"},
			user=frappe.session.user
		)

		return success_count

	
	
	@frappe.whitelist()
	def create_sales_invoice_mtr_b2c(self):
		val = frappe.db.get_value(
			"Ecommerce Mapping",
			{"platform": "Amazon"},
			"default_non_company_customer"
		)

		errors, error_names = [], []
		success_count = 0
		invoice_groups = {}

		# -------- Group Rows by Invoice --------
		for idx, child_row in enumerate(self.mtr_b2c, 1):
			invoice_no = child_row.invoice_number
			if invoice_no not in invoice_groups:
				invoice_groups[invoice_no] = []
			invoice_groups[invoice_no].append((idx, child_row))

		total_invoices = len(invoice_groups)

		# -------- Process Each Invoice Group --------
		for count, (invoice_no, items_data) in enumerate(invoice_groups.items(), start=1):
			try:
				shipment_items = [x for x in items_data if x[1].get("transaction_type") not in ["Refund", "Cancel"]]
				refund_items = [x for x in items_data if x[1].get("transaction_type") == "Refund"]

				existing_si_draft = frappe.db.get_value("Sales Invoice", {"custom_inv_no": invoice_no, "docstatus": 0}, "name")
				existing_si = frappe.db.get_value("Sales Invoice", {"custom_inv_no": invoice_no, "docstatus": 1}, "name")
				amazon = frappe.get_doc("Ecommerce Mapping", {"platform": "Amazon"})
				warehouse_mapping_missing = False

				# -------- Shipment Items --------
				if shipment_items:
					if existing_si_draft:
						si = frappe.get_doc("Sales Invoice", existing_si_draft)
					else:
						si = frappe.new_doc("Sales Invoice")
						si.customer = val
						si.set_posting_time=1
						# Parse the datetime and add 2 seconds
						# invoice_datetime = datetime.strptime(str(items_data[0][1].get("invoice_date")), '%Y-%m-%d %H:%M:%S') if isinstance(items_data[0][1].get("invoice_date"), str) else items_data[0][1].get("invoice_date")
						# invoice_datetime_plus_2 = invoice_datetime + timedelta(seconds=2)
						si.posting_date = getdate(items_data[0][1].get("invoice_date"))
						si.posting_time = get_time(items_data[0][1].get("invoice_date"))
						si.custom_inv_no = invoice_no
						si.custom_ecommerce_invoice_id = invoice_no
						si.__newname = invoice_no
						si.custom_ecommerce_operator = self.ecommerce_mapping
						si.custom_ecommerce_type = self.amazon_type
						si.taxes_and_charges = ""
						si.update_stock = 1

					items_append = []
					for idx, child_row in shipment_items:
						try:
							exists_in_item = frappe.db.sql("""
								SELECT sii.name FROM `tabSales Invoice Item` sii
								JOIN `tabSales Invoice` si ON sii.parent = si.name
								WHERE sii.custom_ecom_item_id = %s AND si.docstatus != 1 AND si.is_return = 0
							""", child_row.shipment_item_id)
							if exists_in_item:
								continue

							itemcode = next(
								(i.erp_item for i in amazon.ecom_item_table if i.ecom_item_id == child_row.get(amazon.ecom_sku_column_header)),
								None
							)
							if not itemcode:
								error_names.append(invoice_no)
								raise Exception(f"Item mapping not found for SKU: {child_row.get(amazon.ecom_sku_column_header)}")

							# ---- Warehouse mapping ----
							warehouse, location, com_address = None, None, None
							warehouse_id = normalize_warehouse_id(child_row.warehouse_id)
							for wh_map in amazon.ecommerce_warehouse_mapping:
								if wh_map.ecom_warehouse_id == warehouse_id:
									warehouse = wh_map.erp_warehouse
									location = wh_map.location
									com_address = wh_map.erp_address
									break
							if not warehouse:
								if not warehouse_id:
									warehouse = amazon.default_company_warehouse
									location = amazon.default_company_location
									com_address = amazon.default_company_address
								else:
									warehouse_mapping_missing = True
									raise Exception(f"Warehouse Mapping not found for Warehouse Id: {warehouse_id}")

							# ---- GSTIN Mapping ----
							ecommerce_gstin = None
							for gstin in amazon.ecommerce_gstin_mapping:
								if gstin.ecommerce_operator_gstin == child_row.seller_gstin:
									ecommerce_gstin = gstin.ecommerce_operator_gstin

							if not si.location:
								si.location = location
							if not si.set_warehouse:
								si.set_warehouse = warehouse
							si.company_address = com_address
							if child_row.ship_to_state:
								state = child_row.ship_to_state
								if not state_code_dict.get(str(state.lower())):
									error_names.append(invoice_no)
									raise Exception(f"State name Is Wrong Please Check")
								si.place_of_supply = state_code_dict.get(str(state.lower()))
							si.ecommerce_gstin = ecommerce_gstin

							# ---- Append Item ----
							hsn_code = frappe.db.get_value("Item", itemcode, "gst_hsn_code")
							si.append("items", {
								"item_code": itemcode,
								"qty": flt(child_row.quantity),
								"rate": flt(child_row.tax_exclusive_gross) / flt(child_row.quantity),
								"description": child_row.item_description,
								"warehouse": warehouse,
								"gst_hsn_code": hsn_code,
								"tax_rate": flt(child_row.total_tax_amount),
								"margin_type": "Amount" if flt(child_row.item_promo_discount) > 0 else None,
								"margin_rate_or_amount": flt(child_row.item_promo_discount),
								"income_account": amazon.income_account,
								"is_free_item": 1 if str(child_row.transaction_type) == "FreeReplacement" else 0
							})
							items_append.append(itemcode)

							# ---- Taxes ----
							for tax_type, rate, amount, acc_head in [
								("CGST", flt(child_row.cgst_rate), flt(child_row.cgst_tax), "Output Tax CGST - KGOPL"),
								("SGST", flt(child_row.sgst_rate) + flt(child_row.utgst_rate), flt(child_row.sgst_tax) + flt(child_row.utgst_tax), "Output Tax SGST - KGOPL"),
								("IGST", flt(child_row.igst_rate), flt(child_row.igst_tax), "Output Tax IGST - KGOPL")
							]:
								if amount > 0:
									existing_tax = next((t for t in si.taxes if t.account_head == acc_head), None)
									if existing_tax:
										existing_tax.tax_amount += amount
									else:
										si.append("taxes", {
											"charge_type": "On Net Total",
											"account_head": acc_head,
											"rate": rate * 100,
											"tax_amount": amount,
											"description": tax_type
										})
						except Exception as item_error:
							error_names.append(invoice_no)
							errors.append({
								"idx": idx,
								"invoice_id": invoice_no,
								"message": f"Shipment item error: {item_error}"
							})

					try:
						if len(items_append) > 0 and not warehouse_mapping_missing:
							si.save(ignore_permissions=True)
							for j in si.items:
								j.item_tax_template = ""
								j.item_tax_rate = frappe._dict()
							si.save(ignore_permissions=True)

							if invoice_no not in error_names:
								si.submit()
								existing_si = si.name
								success_count += len(shipment_items)
					except Exception as submit_error:
						for idx, _ in shipment_items:
							errors.append({
								"idx": idx,
								"invoice_id": invoice_no,
								"message": f"Error submitting shipment invoice: {submit_error}"
							})

				# -------- Draft Submit Before Refund --------
				if refund_items and existing_si_draft and not existing_si and not warehouse_mapping_missing:
					try:
						draft_si = frappe.get_doc("Sales Invoice", existing_si_draft)
						if invoice_no not in error_names:
							draft_si.submit()
							existing_si = draft_si.name
					except Exception as e:
						errors.append({
							"idx": refund_items[0][0],
							"invoice_id": invoice_no,
							"message": f"Failed to submit draft invoice before refund: {e}"
						})
						continue

				# -------- Refund Items --------
				if refund_items and not warehouse_mapping_missing:
					# if not existing_si:
					# 	error_names.append(invoice_no)
					# 	errors.append({
					# 		"idx": refund_items[0][0],
					# 		"invoice_id": invoice_no,
					# 		"message": f"Refund requested but original submitted invoice not found for {invoice_no}."
					# 	})
					# 	continue
					ritems_append = []
					si_return = frappe.new_doc("Sales Invoice")
					si_return.is_return = 1
					si_return.customer = val
					si_return.set_posting_time=1

					# Parse the datetime and add 1 minute for returns
					si_return.posting_date = getdate(refund_items[0][1].get("credit_note_date"))
					si_return.posting_time = get_time(refund_items[0][1].get("credit_note_date"))
					si_return.custom_ecommerce_operator = self.ecommerce_mapping
					si_return.custom_ecommerce_type = self.amazon_type
					si_return.custom_inv_no = invoice_no
					si_return.custom_ecommerce_invoice_id = refund_items[0][1].get("credit_note_no")
					si_return.__newname = refund_items[0][1].get("credit_note_no")
					si_return.taxes = []
					si_return.update_stock = 1
					print("#################(489595099505050)",refund_items[0][1].get("credit_note_no"))

					si_error = []
					for idx, child_row in refund_items:
						try:
							print("@@@@@@@@@@@@@@@@@@@@@@@@@@@",child_row.credit_note_no)
							
							exists_in_item = frappe.db.sql("""
								SELECT sii.name FROM `tabSales Invoice Item` sii
								JOIN `tabSales Invoice` si ON sii.parent = si.name
								WHERE sii.custom_ecom_item_id = %s AND si.docstatus != 1 AND si.is_return = 1
							""", child_row.shipment_item_id)
							if exists_in_item:
								continue

							itemcode = next(
								(i.erp_item for i in amazon.ecom_item_table if i.ecom_item_id == child_row.get(amazon.ecom_sku_column_header)),
								None
							)
							if not itemcode:
								si_error.append(invoice_no)
								raise Exception(f"Item mapping not found for SKU: {child_row.get(amazon.ecom_sku_column_header)}")

							warehouse, location, com_address = None, None, None
							warehouse_id = normalize_warehouse_id(child_row.warehouse_id)
							for wh_map in amazon.ecommerce_warehouse_mapping:
								if wh_map.ecom_warehouse_id == warehouse_id:
									warehouse = wh_map.erp_warehouse
									location = wh_map.location
									com_address = wh_map.erp_address
									break
							if not warehouse:
								if not warehouse_id:
									warehouse = amazon.default_company_warehouse
									location = amazon.default_company_location
									com_address = amazon.default_company_address
								else:
									warehouse_mapping_missing = True
									raise Exception(f"Warehouse Mapping not found for Warehouse Id: {warehouse_id}")

							ecommerce_gstin = None
							for gstin in amazon.ecommerce_gstin_mapping:
								if gstin.ecommerce_operator_gstin == child_row.seller_gstin:
									ecommerce_gstin = gstin.ecommerce_operator_gstin

							if not si_return.location:
								si_return.location = location
							if not si_return.set_warehouse:
								si_return.set_warehouse = warehouse
							si_return.company_address = com_address
							if child_row.ship_to_state:
								state = child_row.ship_to_state
								if not state_code_dict.get(str(state.lower())):
									si_error.append(invoice_no)
									raise Exception(f"State name Is Wrong Please Check")
								si_return.place_of_supply = state_code_dict.get(str(state.lower()))
							si_return.ecommerce_gstin = ecommerce_gstin

							hsn_code = frappe.db.get_value("Item", itemcode, "gst_hsn_code")
							si_return.append("items", {
								"item_code": itemcode,
								"qty": -abs(flt(child_row.quantity)),
								"rate": abs(flt(child_row.tax_exclusive_gross)) / abs(flt(child_row.quantity)),
								"description": child_row.item_description,
								"warehouse": warehouse,
								"gst_hsn_code": hsn_code,
								"income_account": amazon.income_account,
								"tax_rate": flt(child_row.total_tax_amount),
								"margin_type": "Amount" if flt(child_row.item_promo_discount) > 0 else None,
								"margin_rate_or_amount": flt(child_row.item_promo_discount),
								"custom_ecom_item_id": child_row.shipment_item_id
							})
							ritems_append.append(itemcode)

							for tax_type, rate, amount, acc_head in [
								("CGST", flt(child_row.cgst_rate), flt(child_row.cgst_tax), "Output Tax CGST - KGOPL"),
								("SGST", flt(child_row.sgst_rate) + flt(child_row.utgst_rate), flt(child_row.sgst_tax) + flt(child_row.utgst_tax), "Output Tax SGST - KGOPL"),
								("IGST", flt(child_row.igst_rate), flt(child_row.igst_tax), "Output Tax IGST - KGOPL")
							]:
								if amount > 0:
									existing_tax = next((t for t in si_return.taxes if t.account_head == acc_head), None)
									if existing_tax:
										existing_tax.tax_amount += amount
									else:
										si_return.append("taxes", {
											"charge_type": "On Net Total",
											"account_head": acc_head,
											"rate": rate * 100,
											"tax_amount": amount,
											"description": tax_type
										})
						except Exception as item_error:
							si_error.append(invoice_no)
							errors.append({
								"idx": idx,
								"invoice_id": invoice_no,
								"message": f"Refund item error: {item_error}"
							})

					try:
						if len(ritems_append)>0 and not warehouse_mapping_missing:
							si_return.save(ignore_permissions=True)
							for j in si_return.items:
								j.item_tax_template = ""
								j.item_tax_rate = frappe._dict()
							si_return.save(ignore_permissions=True)

							if invoice_no not in si_error:
								si_return.submit()
								success_count += len(refund_items)
					except Exception as submit_error:
						for idx, _ in refund_items:
							errors.append({
								"idx": idx,
								"invoice_id": invoice_no,
								"message": f"Error submitting refund invoice: {submit_error}"
							})

			except Exception as e:
				for idx, _ in items_data:
					errors.append({
						"idx": idx,
						"invoice_id": invoice_no,
						"message": f"Invoice processing error: {str(e)}"
					})

			# ---- 🔹 Update realtime progress ----
			update_progress(count, total_invoices, f"Processed {count}/{total_invoices} invoices")

		# -------- Final Summary --------
		if errors:
			self.status = "Partial Success" if success_count else "Error"
			indicator = "orange" if success_count else "red"
			frappe.msgprint(f"{success_count} items processed, {len(errors)} failed. Check error HTML for details.", indicator=indicator, alert=True)
		else:
			self.error_html = ""
			self.status = "Success"
			frappe.msgprint(f"All {success_count} items processed successfully!", indicator="green")

		self.error_json = str(json.dumps(errors))
		self.save()

		# ---- 🔹 Final 100% Update ----
		update_progress(total_invoices, total_invoices, "Amazon B2C Import Completed ✅")

		return success_count

	
	@frappe.whitelist()
	def create_invoice_or_delivery_note(self):
		from frappe.utils import flt, today, getdate
		import json

		ecommerce_mapping = frappe.get_doc("Ecommerce Mapping", {"platform": "Amazon"})
		customer = ecommerce_mapping.internal_company_customer
		errors = []
		success_count = 0
		invoice_groups = {}

		# Group rows by invoice number
		for idx, row in enumerate(self.stock_transfer, 1):
			invoice_no = row.invoice_number
			invoice_groups.setdefault(invoice_no, []).append((idx, row))

		total_invoices = len(invoice_groups)

		# Loop through invoice groups
		for count, (invoice_no, group_rows) in enumerate(invoice_groups.items(), start=1):
			try:
				is_taxable = any(flt(row.igst_rate) > 0 for _, row in group_rows)
				doctype = "Sales Invoice" if is_taxable else "Delivery Note"
				doctype_m = "Purchase Invoice" if is_taxable else "Purchase Receipt"

				existing_name = frappe.db.get_value(doctype, {
					"custom_inv_no": invoice_no,
					"is_return": 0,
					"docstatus": ["!=", 2]
				}, "name")

				existing_name_purchase = frappe.db.get_value(doctype_m, {
					"custom_inv_no": invoice_no,
					"is_return": 0,
					"docstatus": ["!=", 2]
				}, "name")

				if existing_name:
					existing_doc = frappe.get_doc(doctype, existing_name)
					if existing_doc.docstatus == 0:
						existing_doc.submit()
				if existing_name_purchase:
					existing_doc_pur = frappe.get_doc(doctype_m, existing_name_purchase)
					if existing_doc_pur.docstatus == 0:
						existing_doc_pur.submit()

				# -------- Create Sales Invoice or Delivery Note --------
				if not existing_name:
					doc = frappe.new_doc(doctype)
					doc.customer = customer
					doc.set_posting_time=1
					# Parse the datetime and add 2 seconds
					# invoice_datetime = datetime.strptime(str(group_rows[0][1].get("invoice_date")), '%Y-%m-%d %H:%M:%S') if isinstance(group_rows[0][1].get("invoice_date"), str) else group_rows[0][1].get("invoice_date")
					# invoice_datetime_plus_2 = invoice_datetime + timedelta(seconds=2)
					doc.posting_date = getdate(group_rows[0][1].get("invoice_date"))
					doc.posting_time = get_time(group_rows[0][1].get("invoice_date"))
					doc.custom_inv_no = invoice_no
					doc.custom_ecommerce_operator = self.ecommerce_mapping
					doc.custom_ecommerce_type = self.amazon_type
					doc.taxes = [] if is_taxable else None
					doc.update_stock = 1 if is_taxable else None
					doc.set_warehouse = "" if not is_taxable else None
					doc.__newname = invoice_no
					doc.items = []

					for idx, row in group_rows:
						item_code = next((e_item.erp_item for e_item in ecommerce_mapping.ecom_item_table
							if e_item.ecom_item_id == row.get(ecommerce_mapping.ecom_sku_column_header)), None)
						if not item_code:
							raise Exception(f"Item mapping not found for SKU {row.sku}")

						wh = next((wh for wh in ecommerce_mapping.ecommerce_warehouse_mapping
							if wh.ecom_warehouse_id == row.ship_from_fc), None)
						if not wh:
							raise Exception(f"Warehouse mapping not found for FC {row.ship_from_fc}")

						doc.location = wh.location
						doc.company_address = wh.erp_address
						if row.ship_to_state:
							state=row.ship_to_state
							if not state_code_dict.get(str(state.lower())):
								raise Exception(f"State name Is Wrong Please Check")
							doc.place_of_supply = state_code_dict.get(str(row.ship_to_state).lower())

						doc.append("items", {
							"item_code": item_code,
							"qty": flt(row.quantity),
							"rate": flt(row.taxable_value),
							"warehouse": wh.erp_warehouse
						})

						if is_taxable:
							doc.custom_ecommerce_invoice_id = invoice_no
							for tax_type, rate, amount, acc_head in [
								("CGST", flt(row.cgst_rate), flt(row.cgst_amount), "Output Tax CGST - KGOPL"),
								("SGST", flt(row.sgst_rate) + flt(row.utgst_rate), flt(row.sgst_amount) + flt(row.utgst_amount), "Output Tax SGST - KGOPL"),
								("IGST", flt(row.igst_rate), flt(row.igst_amount), "Output Tax IGST - KGOPL")
							]:
								if amount > 0:
									existing_tax = next((t for t in doc.taxes if t.account_head == acc_head), None)
									if existing_tax:
										existing_tax.tax_amount += amount
									else:
										doc.append("taxes", {
											"charge_type": "On Net Total",
											"account_head": acc_head,
											"rate": rate * 100,
											"tax_amount": amount,
											"description": tax_type
										})

					doc.save(ignore_permissions=True)
					for j in doc.items:
						j.item_tax_template = ""
						j.item_tax_rate = frappe._dict()

					doc.save(ignore_permissions=True)
					doc.submit()
					success_count += len(group_rows)
					frappe.msgprint(f"{doc.doctype} {doc.name} created for Invoice No {invoice_no}")

				# -------- Inter-company: Purchase Invoice or Receipt --------
				print("######################",existing_name_purchase)
				if not existing_name_purchase:
					pi_doc = frappe.new_doc("Purchase Invoice" if is_taxable else "Purchase Receipt")
					pi_doc.supplier = ecommerce_mapping.inter_company_supplier
					pi_doc.set_posting_time=1
					# Parse the datetime and add 2 seconds
					# invoice_datetime = datetime.strptime(str(group_rows[0][1].get("invoice_date")), '%Y-%m-%d %H:%M:%S') if isinstance(group_rows[0][1].get("invoice_date"), str) else group_rows[0][1].get("invoice_date")
					# invoice_datetime_plus_2 = invoice_datetime + timedelta(seconds=2)
					pi_doc.posting_date = getdate(group_rows[0][1].get("invoice_date"))
					pi_doc.posting_time = get_time(group_rows[0][1].get("invoice_date"))
					pi_doc.custom_inv_no = invoice_no
					pi_doc.customer = customer
					pi_doc.custom_ecommerce_operator = self.ecommerce_mapping
					pi_doc.custom_ecommerce_type = self.amazon_type
					pi_doc.__newname = invoice_no
					if is_taxable:
						pi_doc.bill_no = invoice_no
					warehouse = None
					location = None
					com_address = None
					for idx, row in group_rows:
						item_code = next((e_item.erp_item for e_item in ecommerce_mapping.ecom_item_table
							if e_item.ecom_item_id == row.get(ecommerce_mapping.ecom_sku_column_header)), None)
						if not item_code:
							raise Exception(f"Item mapping not found for SKU {row.sku}")

						wh = next((wh for wh in ecommerce_mapping.ecommerce_warehouse_mapping
							if wh.ecom_warehouse_id == row.ship_from_fc), None)
						if not wh:
							raise Exception(f"Warehouse mapping not found for FC {row.ship_from_fc}")
						if wh:
							warehouse = wh.erp_warehouse
							location = wh.location
							com_address = wh.erp_address

						if not row.ship_from_fc:
							warehouse=ecommerce_mapping.default_company_warehouse
							location = ecommerce_mapping.default_company_location
							com_address = ecommerce_mapping.default_company_address

						print("#################################677478",com_address)
						pi_doc.location = location
						pi_doc.billing_address = com_address
					
						# if row.ship_to_state:
						# 	pi_doc.place_of_supply = state_code_dict.get(str(row.ship_to_state).lower())

						pi_doc.append("items", {
							"item_code": item_code,
							"qty": flt(row.quantity),
							"rate": flt(row.taxable_value),
							"warehouse": warehouse,
						})

						if is_taxable:
							pi_doc.custom_ecommerce_invoice_id = invoice_no
							for tax_type, rate, amount, acc_head in [
								("CGST", flt(row.cgst_rate), flt(row.cgst_amount), "Input Tax CGST - KGOPL"),
								("SGST", flt(row.sgst_rate) + flt(row.utgst_rate), flt(row.sgst_amount) + flt(row.utgst_amount), "Input Tax SGST - KGOPL"),
								("IGST", flt(row.igst_rate), flt(row.igst_amount), "Input Tax IGST - KGOPL")
							]:
								if amount > 0:
									existing_tax = next((t for t in pi_doc.taxes if t.account_head == acc_head), None)
									if existing_tax:
										existing_tax.tax_amount += amount
									else:
										pi_doc.append("taxes", {
											"charge_type": "On Net Total",
											"account_head": acc_head,
											"rate": rate * 100,
											"tax_amount": amount,
											"description": tax_type
										})

					pi_doc.save(ignore_permissions=True)
					# for j in pi_doc.items:
					# 	j.item_tax_template = ""
					# 	j.item_tax_rate = frappe._dict()

					# pi_doc.save(ignore_permissions=True)
					# print("####################################666",)
					pi_doc.submit()

			except Exception as e:
				for idx, row in group_rows:
					errors.append({
						"idx": idx,
						"invoice_id": invoice_no,
						"message": f"{str(e)}"
					})

			# 🔹 Realtime progress update after each invoice group
			percent = int((count / total_invoices) * 100)
			frappe.publish_realtime(
				"data_import_progress",
				{"progress": percent, "message": f"Processed {count}/{total_invoices} invoices"},
				user=frappe.session.user
			)

		# -------- Final status update --------
		self.error_json = json.dumps(errors) if errors else ""
		self.status = "Partial Success" if errors and success_count else "Error" if errors else "Success"
		self.save()

		# 🔹 Final realtime update
		frappe.publish_realtime(
			"data_import_progress",
			{"progress": 100, "message": "Amazon Stock Transfer Import Completed ✅"},
			user=frappe.session.user
		)

		return success_count


		
	@frappe.whitelist()
	def create_flipkart_sales_invoice(self):
		from frappe.utils import flt, getdate

		errors = []
		si_invoice = []
		return_invoice = []

		customer = frappe.db.get_value("Ecommerce Mapping", {"platform": "Flipkart"}, "default_non_company_customer")
		flipkart = frappe.get_doc("Ecommerce Mapping", "Flipkart")

		def get_item_code(ecom_sku):
			for jk in flipkart.ecom_item_table:
				if jk.ecom_item_id == ecom_sku:
					return jk.erp_item
			return None

		def get_warehouse_info(warehouse_id):
			warehouse_id = normalize_warehouse_id(warehouse_id)
			if not warehouse_id:
				return flipkart.default_company_warehouse, flipkart.default_company_location, flipkart.default_company_address
			for wh in flipkart.ecommerce_warehouse_mapping:
				if wh.ecom_warehouse_id == warehouse_id:
					return wh.erp_warehouse, wh.location, wh.erp_address
			raise Exception(f"Warehouse Mapping not found for Warehouse Id: {warehouse_id}")

		def get_gstin(seller_gstin):
			for gst in flipkart.ecommerce_gstin_mapping:
				if gst.ecommerce_operator_gstin == seller_gstin:
					return gst.ecommerce_operator_gstin
			return None

		# ---------- SALES ----------
		sale_groups = {}
		for row in self.flipkart_items:
			if row.event_sub_type != "Sale":
				continue

			invoice_key = row.order_id
			if not invoice_key:
				errors.append({
					"idx": row.idx,
					"invoice_id": row.buyer_invoice_id,
					"event": row.event_sub_type,
					"message": "Missing Order ID (order_id) for Sale row"
				})
				continue

			sale_groups.setdefault(invoice_key, []).append(row)

		for invoice_key, rows in sale_groups.items():
			group_errors = False
			items_appended = 0

			try:
				existing = frappe.db.get_value("Sales Invoice", {
					"custom_inv_no": invoice_key,
					"is_return": 0,
					"docstatus": 1
				}, "name")
				if existing:
					continue

				draft_name = frappe.db.get_value("Sales Invoice", {
					"custom_inv_no": invoice_key,
					"is_return": 0,
					"docstatus": 0
				}, "name")

				if draft_name:
					si = frappe.get_doc("Sales Invoice", draft_name)
				else:
					first = rows[0]
					warehouse, location, company_address = get_warehouse_info(first.warehouse_id)
					ecommerce_gstin = get_gstin(first.seller_gstin)

					si = frappe.new_doc("Sales Invoice")
					si.customer = customer
					si.set_posting_time = 1
					si.posting_date = getdate(first.buyer_invoice_date)
					si.custom_inv_no = invoice_key
					si.custom_ecommerce_operator = self.ecommerce_mapping
					si.custom_ecommerce_type = self.amazon_type
					si.taxes_and_charges = ""
					si.update_stock = 1

					if first.customers_billing_state:
						state = first.customers_billing_state
						if not state_code_dict.get(str(state).lower()):
							raise Exception("State name Is Wrong Please Check")
						si.place_of_supply = state_code_dict.get(str(state).lower())

					si.company_address = company_address
					si.ecommerce_gstin = ecommerce_gstin
					si.location = location
					si.custom_ecommerce_invoice_id = first.buyer_invoice_id
					si.__newname = first.buyer_invoice_id

				existing_item_ids = {
					d.get("custom_ecom_item_id")
					for d in (si.get("items") or [])
					if d.get("custom_ecom_item_id")
				}

				for row in rows:
					try:
						if row.order_item_id in existing_item_ids:
							continue

						# Skip if order_item_id already exists in non-submitted Sales Invoice Item
						exists_in_item = frappe.db.sql("""
							SELECT sii.name FROM `tabSales Invoice Item` sii
							JOIN `tabSales Invoice` si ON sii.parent = si.name
							WHERE sii.custom_ecom_item_id = %s AND si.docstatus != 1 AND si.is_return = 0
						""", row.order_item_id)
						if exists_in_item:
							continue

						item_code = get_item_code(row.get(flipkart.ecom_sku_column_header))
						if not item_code:
							raise Exception(f"Item mapping not found for SKU: {row.get(flipkart.ecom_sku_column_header)}")

						warehouse, location, company_address = get_warehouse_info(row.warehouse_id)
						ecommerce_gstin = get_gstin(row.seller_gstin)

						# Fill missing headers (draft invoices)
						if not si.company_address:
							si.company_address = company_address
						if not si.location:
							si.location = location
						if not si.ecommerce_gstin:
							si.ecommerce_gstin = ecommerce_gstin
						if not si.place_of_supply and row.customers_billing_state:
							state = row.customers_billing_state
							if not state_code_dict.get(str(state).lower()):
								raise Exception("State name Is Wrong Please Check")
							si.place_of_supply = state_code_dict.get(str(state).lower())
						if not si.custom_ecommerce_invoice_id and row.buyer_invoice_id:
							si.custom_ecommerce_invoice_id = row.buyer_invoice_id
							si.__newname = row.buyer_invoice_id

						item_name = frappe.db.get_value("Item", item_code, "item_name")
						hsn_code = frappe.db.get_value("Item", item_code, "gst_hsn_code")

						qty = flt(row.item_quantity)
						rate = (flt(row.taxable_value) / qty) if qty else 0

						item_row = {
							"item_code": item_code,
							"item_name": item_name,
							"qty": qty,
							"rate": rate,
							"price_list_rate": rate,
							"gst_hsn_code": hsn_code,
							"description": row.product_titledescription,
							"warehouse": warehouse,
							"income_account": flipkart.income_account,
							"custom_ecom_item_id": row.order_item_id
						}

						si.append("items", item_row)
						existing_item_ids.add(row.order_item_id)
						items_appended += 1

						for tax_type, tax_rate, amount, acc_head in [
							("CGST", flt(row.cgst_rate), flt(row.cgst_amount), "Output Tax CGST - KGOPL"),
							("SGST", flt(row.sgst_rate), flt(row.sgst_amount), "Output Tax SGST - KGOPL"),
							("IGST", flt(row.igst_rate), flt(row.igst_amount), "Output Tax IGST - KGOPL")
						]:
							if amount:
								existing_tax = next((t for t in si.taxes if t.account_head == acc_head), None)
								if existing_tax:
									existing_tax.tax_amount += amount
								else:
									si.append("taxes", {
										"charge_type": "On Net Total",
										"rate": tax_rate,
										"account_head": acc_head,
										"tax_amount": amount,
										"description": tax_type
									})
					except Exception as row_error:
						group_errors = True
						errors.append({
							"idx": row.idx,
							"invoice_id": row.buyer_invoice_id,
							"event": row.event_sub_type,
							"message": str(row_error)
						})

				if items_appended > 0:
					si.save(ignore_permissions=True)
					for j in si.items:
						j.item_tax_template = ""
						j.item_tax_rate = frappe._dict()
					si.due_date = getdate(today())
					si.save(ignore_permissions=True)

				if not group_errors and si.docstatus == 0 and si.items:
					si_invoice.append(si.name)

			except Exception as e:
				for row in rows:
					errors.append({
						"idx": row.idx,
						"invoice_id": row.buyer_invoice_id,
						"event": row.event_sub_type,
						"message": str(e)
					})

		# Submit Sales Invoices
		for sii in si_invoice:
			try:
				frappe.get_doc("Sales Invoice", sii).submit()
			except Exception as e:
				errors.append({
					"idx": "",
					"invoice_id": sii,
					"event": "Sale",
					"message": f"Submit failed: {str(e)}"
				})

		# ---------- RETURNS ----------
		return_groups = {}
		for row in self.flipkart_items:
			if row.event_sub_type != "Return":
				continue

			invoice_key = row.order_id
			if not invoice_key:
				errors.append({
					"idx": row.idx,
					"invoice_id": row.buyer_invoice_id,
					"event": row.event_sub_type,
					"message": "Missing Order ID (order_id) for Return row"
				})
				continue

			return_groups.setdefault(invoice_key, []).append(row)

		for invoice_key, rows in return_groups.items():
			group_errors = False
			items_appended = 0

			try:
				existing_return = frappe.db.get_value("Sales Invoice", {
					"custom_inv_no": invoice_key,
					"is_return": 1,
					"docstatus": 1
				}, "name")
				if existing_return:
					continue

				original_inv = frappe.db.get_value("Sales Invoice", {
					"custom_inv_no": invoice_key,
					"is_return": 0,
					"docstatus": 1
				}, "name")
				# if not original_inv:
				# 	raise Exception(f"Original invoice not found or not submitted for Order ID: {invoice_key}")

				draft_name = frappe.db.get_value("Sales Invoice", {
					"custom_inv_no": invoice_key,
					"is_return": 1,
					"docstatus": 0
				}, "name")

				if draft_name:
					si = frappe.get_doc("Sales Invoice", draft_name)
					si.is_return = 1
				else:
					first = rows[0]
					warehouse, location, company_address = get_warehouse_info(first.warehouse_id)
					ecommerce_gstin = get_gstin(first.seller_gstin)

					si = frappe.new_doc("Sales Invoice")
					si.customer = customer
					si.set_posting_time = 1
					si.posting_date = getdate(first.buyer_invoice_date)
					si.custom_inv_no = invoice_key
					si.custom_ecommerce_operator = self.ecommerce_mapping
					si.custom_ecommerce_type = self.amazon_type
					si.taxes_and_charges = ""
					si.update_stock = 1
					si.company_address = company_address
					if first.customers_billing_state:
						state = first.customers_billing_state
						if not state_code_dict.get(str(state).lower()):
							raise Exception("State name Is Wrong Please Check")
						si.place_of_supply = state_code_dict.get(str(state).lower())
					si.ecommerce_gstin = ecommerce_gstin
					si.location = location
					si.is_return = 1
					si.custom_ecommerce_invoice_id = first.buyer_invoice_id
					si.__newname = first.buyer_invoice_id

				existing_item_ids = {
					d.get("custom_ecom_item_id")
					for d in (si.get("items") or [])
					if d.get("custom_ecom_item_id")
				}

				for row in rows:
					try:
						if row.order_item_id in existing_item_ids:
							continue

						exists_in_item = frappe.db.sql("""
							SELECT sii.name FROM `tabSales Invoice Item` sii
							JOIN `tabSales Invoice` si ON sii.parent = si.name
							WHERE sii.custom_ecom_item_id = %s AND si.docstatus != 1 AND si.is_return = 1
						""", row.order_item_id)
						if exists_in_item:
							continue

						item_code = get_item_code(row.get(flipkart.ecom_sku_column_header))
						if not item_code:
							raise Exception(f"Item mapping not found for SKU: {row.get(flipkart.ecom_sku_column_header)}")

						warehouse, location, company_address = get_warehouse_info(row.warehouse_id)
						ecommerce_gstin = get_gstin(row.seller_gstin)

						if not si.company_address:
							si.company_address = company_address
						if not si.location:
							si.location = location
						if not si.ecommerce_gstin:
							si.ecommerce_gstin = ecommerce_gstin
						if not si.place_of_supply and row.customers_billing_state:
							state = row.customers_billing_state
							if not state_code_dict.get(str(state).lower()):
								raise Exception("State name Is Wrong Please Check")
							si.place_of_supply = state_code_dict.get(str(state).lower())
						if not si.custom_ecommerce_invoice_id and row.buyer_invoice_id:
							si.custom_ecommerce_invoice_id = row.buyer_invoice_id
							si.__newname = row.buyer_invoice_id

						item_name = frappe.db.get_value("Item", item_code, "item_name")
						hsn_code = frappe.db.get_value("Item", item_code, "gst_hsn_code")

						qty_abs = abs(flt(row.item_quantity))
						item_row = {
							"item_code": item_code,
							"item_name": item_name,
							"gst_hsn_code": hsn_code,
							"qty": -qty_abs,
							"rate": abs(flt(row.taxable_value)) / qty_abs if qty_abs else 0,
							"price_list_rate": abs(flt(row.taxable_value)) / qty_abs if qty_abs else 0,
							"description": row.product_titledescription,
							"warehouse": warehouse,
							"custom_ecom_item_id": row.order_item_id
						}

						si.append("items", item_row)
						existing_item_ids.add(row.order_item_id)
						items_appended += 1

						for tax_type, tax_rate, amount, acc_head in [
							("CGST", flt(row.cgst_rate), flt(row.cgst_amount), "Output Tax CGST - KGOPL"),
							("SGST", flt(row.sgst_rate), flt(row.sgst_amount), "Output Tax SGST - KGOPL"),
							("IGST", flt(row.igst_rate), flt(row.igst_amount), "Output Tax IGST - KGOPL")
						]:
							if amount:
								existing_tax = next((t for t in si.taxes if t.account_head == acc_head), None)
								if existing_tax:
									existing_tax.tax_amount += amount
								else:
									si.append("taxes", {
										"charge_type": "On Net Total",
										"rate": tax_rate,
										"account_head": acc_head,
										"tax_amount": amount,
										"description": tax_type
									})
					except Exception as row_error:
						group_errors = True
						errors.append({
							"idx": row.idx,
							"invoice_id": row.buyer_invoice_id,
							"event": row.event_sub_type,
							"message": str(row_error)
						})

				if items_appended > 0:
					si.save(ignore_permissions=True)
					for j in si.items:
						j.item_tax_template = ""
						j.item_tax_rate = frappe._dict()
					si.due_date = getdate(today())
					si.save(ignore_permissions=True)

				if not group_errors and si.docstatus == 0 and si.items:
					return_invoice.append(si.name)

			except Exception as e:
				for row in rows:
					errors.append({
						"idx": row.idx,
						"invoice_id": row.buyer_invoice_id,
						"event": row.event_sub_type,
						"message": str(e)
					})

		# Submit Return Invoices
		for sii in return_invoice:
			try:
				frappe.get_doc("Sales Invoice", sii).submit()
			except Exception as e:
				errors.append({
					"idx": "",
					"invoice_id": sii,
					"event": "Return",
					"message": f"Submit failed: {str(e)}"
				})

		self.error_json = str(json.dumps(errors))
		if len(errors) == 0:
			self.status = "Success"
		elif len(self.flipkart_items) != len(errors):
			self.status = "Partial Success"
		else:
			self.status = "Error"

		self.save(ignore_permissions=True)

		return {
			"status": "partial" if errors else "success",
			"errors": errors
		}



		
	def create_cred_sales_invoice(self):
		

		from frappe.utils import flt, getdate

		errors = []
		si_items = []
		si_return_items = []

		val = frappe.db.get_value("Ecommerce Mapping", {"platform": "Cred"}, "default_non_company_customer")
		amazon = frappe.get_doc("Ecommerce Mapping", {"name": "Cred"})

		# Shipment Invoice
		for i in self.cred:
			try:
				if i.order_status in ["CANCELLED", "RTO"]:
					continue

				si_inv = frappe.db.get_value("Sales Invoice", {"custom_inv_no": i.order_item_id, "is_return": 0, "docstatus": 1}, "name")
				if si_inv:
					errors.append({
						"idx": i.idx,
						"invoice_id": i.order_item_id,
						"event": "Invoice Already Exist",
						"message": "Invoice Already Exist"
					})
					continue
				si_inv_draft = frappe.db.get_value("Sales Invoice", {"custom_inv_no": i.order_item_id, "is_return": 0, "docstatus": 0}, "name")

				itemcode = next((jk.erp_item for jk in amazon.ecom_item_table if jk.ecom_item_id == i.get(str(amazon.ecom_sku_column_header))), None)
				warehouse_data = next((kk for kk in amazon.ecommerce_warehouse_mapping if kk.ecom_warehouse_id == i.warehouse_location_code), None)
				if not itemcode or not warehouse_data:
					errors.append({
						"idx": i.idx,
						"invoice_id": i.order_item_id,
						"event": "Create Shipment",
						"message": "Missing item code or warehouse mapping"
					})
					continue

				warehouse = warehouse_data.erp_warehouse
				location = warehouse_data.location
				com_address = warehouse_data.erp_address
				customer_address_in_state=None
				customer_address_out_state=None
				if not warehouse:
					warehouse=amazon.default_company_warehouse
					location=amazon.default_company_location
					com_address=amazon.default_company_address
					# customer_address_in_state=amazon.customer_address_in_state
					# customer_address_out_state=amazon.customer_address_out_state

				gstin_data = next((gstin for gstin in amazon.ecommerce_gstin_mapping if gstin.ecommerce_operator_gstin == i.seller_gstin), None)
				ecommerce_gstin = gstin_data.ecommerce_operator_gstin if gstin_data else ""

				si = frappe.new_doc("Sales Invoice") if not si_inv else frappe.get_doc("Sales Invoice", si_inv_draft)
				si.customer = val
				si.set_posting_time = 1
				# Parse the datetime and add 2 seconds
				# order_datetime = datetime.strptime(str(i.order_date_time), '%Y-%m-%d %H:%M:%S') if isinstance(i.order_date_time, str) else i.order_date_time
				# order_datetime_plus_2 = order_datetime + timedelta(seconds=2)
				si.posting_date = getdate(i.order_date_time)
				# si.posting_time = get_time(i.order_date_time)
				si.custom_inv_no = i.order_item_id
				if i.destination_address_state:
					state=i.destination_address_state
					if not state_code_dict.get(str(state.lower())):
						raise Exception(f"State name Is Wrong Please Check")
					si.place_of_supply=state_code_dict.get(str(state.lower()))
				si.taxes_and_charges = ""
				si.custom_ecommerce_operator=self.ecommerce_mapping
				si.custom_ecommerce_type=self.amazon_type
				si.taxes = []
				si.update_stock = 1
				si.location = location
				si.set_warehouse = warehouse
				si.company_address = com_address
				si.ecommerce_gstin = ecommerce_gstin
				# si.due_date=getdate(i.order_item_id)
				si.custom_ecommerce_invoice_id=i.order_item_id
				si.__newname=i.order_item_id
				hsn_code=frappe.db.get_value("Item",itemcode,"gst_hsn_code")
				si.append("items", {
					"item_code": itemcode,
					"gst_hsn_code": hsn_code if hsn_code else None,
					"qty": 1,
					"rate": flt(i.net_gmv),
					"description": i.product_name,
					"warehouse": warehouse,
					"income_account": amazon.income_account,
					"item_tax_template": ""
				})
				# print("################^&&&&&&&",i.order_item_id,tax_amt)
				tax_rate=flt(i.gst_rate_on_gmv)*100
				tax_amt = flt(i.gmv)*(tax_rate/100)
				if i.source_address_state == i.destination_address_state:
					if tax_amt > 0:
						si.customer_address=customer_address_in_state
						si.append("taxes", {"charge_type": "On Net Total", "account_head": "Output Tax CGST - KGOPL","tax_amount": flt(tax_amt) / 2,"rate":tax_rate/2, "description": "CGST"})
						si.append("taxes", {"charge_type": "On Net Total", "account_head": "Output Tax SGST - KGOPL","tax_amount": flt(tax_amt) / 2,"rate":tax_rate/2,"description": "SGST"})
				else:
					if tax_amt > 0:
						si.customer_address=customer_address_out_state
						si.append("taxes", {"charge_type": "On Net Total", "account_head": "Output Tax IGST - KGOPL","tax_amount": flt(tax_amt),"rate":tax_rate,"description": "IGST"})
			
				si.save(ignore_permissions=True)
				for j in si.items:
					j.item_tax_template = ""
					j.item_tax_rate = frappe._dict()

				si.due_date = getdate(today())

				si.save(ignore_permissions=True)
				si_items.append(si.name)

			except Exception as e:
				errors.append({
					"idx": i.idx,
					"invoice_id": i.order_item_id,
					"event": "Create Shipment",
					"message": str(e)
				})

		for si in si_items:
			try:
				doc = frappe.get_doc("Sales Invoice", si)
				doc.submit()
			except Exception as e:
				errors.append({
					"idx": None,
					"invoice_id": si,
					"event": "Submit Shipment",
					"message": e
				})

		# Return Invoice
		for i in self.cred_items:
			try:
				if i.order_status in ["CANCELLED", "RTO"]:
					continue

				si_inv = frappe.db.get_value("Sales Invoice", {"custom_inv_no": i.cred_order_item_id, "is_return": 1, "docstatus": 1}, "name")
				if si_inv:
					continue


				original_si_inv = frappe.db.get_value("Sales Invoice", {"custom_inv_no": i.cred_order_item_id, "is_return": 0, "docstatus": 1}, "name")
				# if not original_si_inv:
				# 	errors.append({
				# 		"idx": i.idx,
				# 		"invoice_id": i.cred_order_item_id,
				# 		"event": "Original Invoice Not Found",
				# 		"message": "Original Invoice Not  Found"
				# 	})
				# 	continue
				si_inv_draft = frappe.db.get_value("Sales Invoice", {"custom_inv_no": i.cred_order_item_id, "is_return": 1, "docstatus": 0}, "name")

				itemcode = next((jk.erp_item for jk in amazon.ecom_item_table if jk.ecom_item_id == i.get(str(amazon.ecom_sku_column_header))), None)
				warehouse_data = next((kk for kk in amazon.ecommerce_warehouse_mapping if kk.ecom_warehouse_id == i.warehouse_location_code), None)
				if not itemcode or not warehouse_data:
					errors.append({
						"idx": i.idx,
						"invoice_id": i.cred_order_item_id,
						"event": "Create Return",
						"message": "Missing item code or warehouse mapping"
					})
					continue
				print("#########################",itemcode)
				warehouse = warehouse_data.erp_warehouse
				location = warehouse_data.location
				com_address = warehouse_data.erp_address
				# customer_address_in_state=warehouse_data.customer_address_in_state
				# customer_address_out_state=warehouse_data.customer_address_out_state
				if not warehouse:
					warehouse=amazon.default_company_warehouse
					location=amazon.default_company_location
					
					com_address=amazon.default_company_address
					# customer_address_in_state=amazon.customer_address_in_state
					# customer_address_out_state=amazon.customer_address_out_state
				# company_gstin = frappe.db.get_value("Address", com_address, "gstin")
				gstin_data = next((gstin for gstin in amazon.ecommerce_gstin_mapping if gstin.ecommerce_operator_gstin == i.seller_gstin), None)
				ecommerce_gstin=None
				if gstin_data:
					ecommerce_gstin = gstin_data.ecommerce_operator_gstin

				si = frappe.new_doc("Sales Invoice") if not si_inv else frappe.get_doc("Sales Invoice", si_inv_draft)
				si.customer = val
				si.set_posting_time = 1
				if i.customer_state:
					state=i.customer_state
					if not state_code_dict.get(str(state.lower())):
						raise Exception(f"State name Is Wrong Please Check")
					si.place_of_supply=state_code_dict.get(str(state.lower()))
				# Parse the datetime and add 1 minute for returns
				# refund_datetime = datetime.strptime(str(i.refund_date_time), '%Y-%m-%d %H:%M:%S') if isinstance(i.refund_date_time, str) else i.refund_date_time
				# refund_datetime_plus_1min = refund_datetime + timedelta(minutes=1)
				si.posting_date = getdate(i.refund_date_time)
				# si.posting_time = refund_datetime_plus_1min.time()
				si.custom_ecommerce_operator=self.ecommerce_mapping
				si.custom_ecommerce_type=self.amazon_type
				si.custom_inv_no = i.cred_order_item_id
				si.taxes_and_charges = ""
				si.taxes = []
				si.update_stock = 1
				si.location = location
				si.set_warehouse = warehouse
				si.company_address = com_address
				si.ecommerce_gstin = ecommerce_gstin
				si.is_return=1
				si.custom_ecommerce_invoice_id="CR"+str(i.cred_order_item_id)
				si.__newname="CR"+str(i.cred_order_item_id)
				tax_amt = flt(i.gmv) *flt(i.gst_rate)
				hsn_code=frappe.db.get_value("Item",itemcode,"gst_hsn_code")
				si.append("items", {
					"item_code": itemcode,
					"gst_hsn_code": hsn_code if hsn_code else None,
					"qty": -1,
					"rate": abs(flt(i.gmv) - tax_amt),
					"description": i.product_name,
					"warehouse": warehouse,
					"income_account": amazon.income_account
				})

				tax_rate=flt(i.gst_rate)*100
				tax_amt = flt(i.gmv)*(tax_rate/100)
				if i.warehouse_state == i.customer_state:
					if tax_amt > 0:
						si.customer_address=customer_address_in_state
						si.append("taxes", {"charge_type": "On Net Total", "account_head": "Output Tax CGST - KGOPL","tax_amount": flt(tax_amt) / 2,"rate":tax_rate/2, "description": "CGST"})
						si.append("taxes", {"charge_type": "On Net Total", "account_head": "Output Tax SGST - KGOPL","tax_amount": flt(tax_amt) / 2,"rate":tax_rate/2,"description": "SGST"})
				else:
					if tax_amt > 0:
						si.customer_address=customer_address_out_state
						si.append("taxes", {"charge_type": "On Net Total", "account_head": "Output Tax IGST - KGOPL","tax_amount": flt(tax_amt),"rate":tax_rate,"description": "IGST"})
			
				
				si.save(ignore_permissions=True)
				for j in si.items:
					j.item_tax_template = ""
					j.item_tax_rate = frappe._dict()
				si.due_date=getdate(today())
				si.save(ignore_permissions=True)
				si_return_items.append(si.name)

			except Exception as e:
				errors.append({
					"idx": i.idx,
					"invoice_id": i.cred_order_item_id,
					"event": "Create Return",
					"message": str(e)
				})

		for si in si_return_items:
			try:
				doc = frappe.get_doc("Sales Invoice", si)
				doc.submit()
			except Exception as e:
				errors.append({
					"idx": None,
					"invoice_id": si,
					"event": "Submit Return",
					"message": e
				})

		# Save all errors in test_json
		if errors:
			self.error_json = frappe.as_json(errors)
			if len(errors) == 0:
				self.status = "Success"
			elif len(self.flipkart_items) != len(errors):
				self.status = "Partial Success"
			else:
				self.status = "Error"

			self.save(ignore_permissions=True)


	def create_jio_mart(self):
		

		from frappe.utils import flt, getdate

		errors = []
		si_invoice = []
		return_invoice = []

		customer = frappe.db.get_value("Ecommerce Mapping", {"platform": "Jiomart"}, "default_non_company_customer")
		jiomart = frappe.get_doc("Ecommerce Mapping", "Jiomart")

		def get_item_code(ecom_sku):
			for jk in jiomart.ecom_item_table:
				if jk.ecom_item_id == ecom_sku:
					return jk.erp_item
			return None

		def get_warehouse_info():
			return jiomart.default_company_warehouse, jiomart.default_company_location, jiomart.default_company_address

		def get_gstin(seller_gstin):
			# company_gstin = frappe.db.get_value("Address", company_address, "gstin")
			for gst in jiomart.ecommerce_gstin_mapping:
				if gst.ecommerce_operator_gstin == seller_gstin:
					return gst.ecommerce_operator_gstin
			return None

		# ---------- SALES ----------
		sale_groups = {}
		for row in self.jio_mart_items:
			if row.type != "shipment":
				continue

			invoice_key = row.original_invoice_id
			if not invoice_key:
				errors.append({
					"idx": row.idx,
					"invoice_id": row.buyer_invoice_id,
					"event": row.type,
					"message": "Missing Original Invoice ID (original_invoice_id) for shipment row"
				})
				continue

			sale_groups.setdefault(invoice_key, []).append(row)

		for invoice_key, rows in sale_groups.items():
			group_errors = False
			items_appended = 0

			try:
				existing = frappe.db.get_value("Sales Invoice", {
					"custom_inv_no": invoice_key,
					"is_return": 0,
					"docstatus": 1
				}, "name")
				if existing:
					continue

				draft_name = frappe.db.get_value("Sales Invoice", {
					"custom_inv_no": invoice_key,
					"is_return": 0,
					"docstatus": 0
				}, "name")

				warehouse, location, company_address = get_warehouse_info()

				if draft_name:
					si = frappe.get_doc("Sales Invoice", draft_name)
				else:
					first = rows[0]
					ecommerce_gstin = get_gstin(first.seller_gstin)

					si = frappe.new_doc("Sales Invoice")
					si.customer = customer
					si.set_posting_time = 1
					si.posting_date = getdate(first.buyer_invoice_date)
					si.custom_inv_no = invoice_key
					si.custom_ecommerce_operator = self.ecommerce_mapping
					si.custom_ecommerce_type = self.amazon_type
					if first.customers_billing_state:
						state = first.customers_billing_state
						if not state_code_dict.get(str(state).lower()):
							raise Exception("State name Is Wrong Please Check")
						si.place_of_supply = state_code_dict.get(str(state).lower())
					si.taxes_and_charges = ""
					si.update_stock = 1
					si.company_address = company_address
					si.custom_ecommerce_invoice_id = first.buyer_invoice_id
					si.__newname = first.buyer_invoice_id
					si.ecommerce_gstin = ecommerce_gstin
					si.location = location

				existing_item_ids = {
					d.get("custom_ecom_item_id")
					for d in (si.get("items") or [])
					if d.get("custom_ecom_item_id")
				}

				for row in rows:
					try:
						if row.order_item_id in existing_item_ids:
							continue

						# Skip if order_item_id already exists in non-submitted Sales Invoice Item
						exists_in_item = frappe.db.sql("""
							SELECT sii.name FROM `tabSales Invoice Item` sii
							JOIN `tabSales Invoice` si ON sii.parent = si.name
							WHERE sii.custom_ecom_item_id = %s AND si.docstatus != 1 AND si.is_return = 0
						""", row.order_item_id)
						if exists_in_item:
							continue

						item_code = get_item_code(row.get(jiomart.ecom_sku_column_header))
						if not item_code:
							raise Exception(f"Item mapping not found for SKU: {row.get(jiomart.ecom_sku_column_header)}")

						item_name = frappe.db.get_value("Item", item_code, "item_name")
						hsn_code = frappe.db.get_value("Item", item_code, "gst_hsn_code")

						item_row = {
							"item_code": item_code,
							"item_name": item_name,
							"qty": flt(row.item_quantity),
							"rate": flt(row.taxable_value),
							"gst_hsn_code": hsn_code,
							"description": row.product_titledescription,
							"warehouse": warehouse,
							"margin_type": "Amount",
							"margin_rate_or_amount": flt(row.seller_coupon_amount),
							"income_account": jiomart.income_account,
							"custom_ecom_item_id": row.order_item_id
						}

						# Fill missing headers (draft invoices)
						ecommerce_gstin = get_gstin(row.seller_gstin)
						if not si.company_address:
							si.company_address = company_address
						if not si.location:
							si.location = location
						if not si.ecommerce_gstin:
							si.ecommerce_gstin = ecommerce_gstin
						if not si.place_of_supply and row.customers_billing_state:
							state = row.customers_billing_state
							if not state_code_dict.get(str(state).lower()):
								raise Exception("State name Is Wrong Please Check")
							si.place_of_supply = state_code_dict.get(str(state).lower())
						if not si.custom_ecommerce_invoice_id and row.buyer_invoice_id:
							si.custom_ecommerce_invoice_id = row.buyer_invoice_id
							si.__newname = row.buyer_invoice_id

						si.append("items", item_row)
						existing_item_ids.add(row.order_item_id)
						items_appended += 1

						for tax_type, rate, amount, acc_head in [
							("CGST", row.cgst_rate, flt(row.cgst_amount), "Output Tax CGST - KGOPL"),
							("SGST", row.sgst_rate_or_utgst_as_applicable, flt(row.sgst_amount_or_utgst_as_applicable), "Output Tax SGST - KGOPL"),
							("IGST", row.igst_rate, flt(row.igst_amount), "Output Tax IGST - KGOPL")
						]:
							if amount:
								existing_tax = next((t for t in si.taxes if t.account_head == acc_head), None)
								if existing_tax:
									existing_tax.tax_amount += amount
								else:
									si.append("taxes", {
										"charge_type": "On Net Total",
										"rate": rate,
										"account_head": acc_head,
										"tax_amount": amount,
										"description": tax_type
									})
					except Exception as row_error:
						group_errors = True
						errors.append({
							"idx": row.idx,
							"invoice_id": row.buyer_invoice_id,
							"event": row.type,
							"message": str(row_error)
						})

				if items_appended > 0:
					si.save(ignore_permissions=True)
					for j in si.items:
						j.item_tax_template = ""
						j.item_tax_rate = frappe._dict()
					si.due_date = getdate(today())
					si.save(ignore_permissions=True)

				if not group_errors and si.docstatus == 0 and si.items:
					si_invoice.append(si.name)

			except Exception as e:
				for row in rows:
					errors.append({
						"idx": row.idx,
						"invoice_id": row.buyer_invoice_id,
						"event": row.type,
						"message": str(e)
					})

		# Submit Sales Invoices
		for sii in si_invoice:
			try:
				frappe.get_doc("Sales Invoice", sii).submit()
			except Exception as e:
				errors.append({
					"idx": "",
					"invoice_id": sii,
					"event": "Sale",
					"message": f"Submit failed: {str(e)}"
				})

		# ---------- RETURNS ----------
		return_groups = {}
		for row in self.jio_mart_items:
			if row.event_type != "return":
				continue

			invoice_key = row.original_invoice_id
			if not invoice_key:
				errors.append({
					"idx": row.idx,
					"invoice_id": row.buyer_invoice_id,
					"event": row.event_type,
					"message": "Missing Original Invoice ID (original_invoice_id) for return row"
				})
				continue

			return_groups.setdefault(invoice_key, []).append(row)

		for invoice_key, rows in return_groups.items():
			group_errors = False
			items_appended = 0

			try:
				existing_return = frappe.db.get_value("Sales Invoice", {
					"custom_inv_no": invoice_key,
					"is_return": 1,
					"docstatus": 1
				}, "name")
				if existing_return:
					continue

				original_inv = frappe.db.get_value("Sales Invoice", {
					"custom_inv_no": invoice_key,
					"is_return": 0,
					"docstatus": 1
				}, "name")
				# if not original_inv:
				# 	raise Exception(f"Original invoice not found or not submitted for Invoice ID: {invoice_key}")

				draft_name = frappe.db.get_value("Sales Invoice", {
					"custom_inv_no": invoice_key,
					"is_return": 1,
					"docstatus": 0
				}, "name")

				warehouse, location, company_address = get_warehouse_info()

				if draft_name:
					si = frappe.get_doc("Sales Invoice", draft_name)
					si.is_return = 1
				else:
					first = rows[0]
					ecommerce_gstin = get_gstin(first.seller_gstin)

					si = frappe.new_doc("Sales Invoice")
					si.customer = customer
					si.set_posting_time = 1
					si.posting_date = getdate(first.buyer_invoice_date)
					si.custom_inv_no = invoice_key
					si.custom_ecommerce_operator = self.ecommerce_mapping
					si.custom_ecommerce_type = self.amazon_type
					si.taxes_and_charges = ""
					si.update_stock = 1
					si.company_address = company_address
					si.ecommerce_gstin = ecommerce_gstin
					si.location = location
					si.is_return = 1
					si.custom_ecommerce_invoice_id = first.buyer_invoice_id
					si.__newname = first.buyer_invoice_id
					if first.customers_billing_state:
						state = first.customers_billing_state
						if not state_code_dict.get(str(state).lower()):
							raise Exception("State name Is Wrong Please Check")
						si.place_of_supply = state_code_dict.get(str(state).lower())

				existing_item_ids = {
					d.get("custom_ecom_item_id")
					for d in (si.get("items") or [])
					if d.get("custom_ecom_item_id")
				}

				for row in rows:
					try:
						if row.order_item_id in existing_item_ids:
							continue

						exists_in_item = frappe.db.sql("""
							SELECT sii.name FROM `tabSales Invoice Item` sii
							JOIN `tabSales Invoice` si ON sii.parent = si.name
							WHERE sii.custom_ecom_item_id = %s AND si.docstatus != 1 AND si.is_return = 1
						""", row.order_item_id)
						if exists_in_item:
							continue

						item_code = get_item_code(row.get(jiomart.ecom_sku_column_header))
						if not item_code:
							raise Exception(f"Item mapping not found for SKU: {row.get(jiomart.ecom_sku_column_header)}")

						item_name = frappe.db.get_value("Item", item_code, "item_name")
						hsn_code = frappe.db.get_value("Item", item_code, "gst_hsn_code")

						item_row = {
							"item_code": item_code,
							"item_name": item_name,
							"qty": -abs(flt(row.item_quantity)),
							"rate": abs(flt(row.taxable_value)),
							"gst_hsn_code": hsn_code,
							"description": row.product_titledescription,
							"warehouse": warehouse,
							"margin_type": "Amount",
							"margin_rate_or_amount": flt(row.seller_coupon_amount),
							"income_account": jiomart.income_account,
							"custom_ecom_item_id": row.order_item_id
						}

						ecommerce_gstin = get_gstin(row.seller_gstin)
						if not si.company_address:
							si.company_address = company_address
						if not si.location:
							si.location = location
						if not si.ecommerce_gstin:
							si.ecommerce_gstin = ecommerce_gstin
						if not si.place_of_supply and row.customers_billing_state:
							state = row.customers_billing_state
							if not state_code_dict.get(str(state).lower()):
								raise Exception("State name Is Wrong Please Check")
							si.place_of_supply = state_code_dict.get(str(state).lower())
						if not si.custom_ecommerce_invoice_id and row.buyer_invoice_id:
							si.custom_ecommerce_invoice_id = row.buyer_invoice_id
							si.__newname = row.buyer_invoice_id

						si.append("items", item_row)
						existing_item_ids.add(row.order_item_id)
						items_appended += 1

						for tax_type, rate, amount, acc_head in [
							("CGST", row.cgst_rate, flt(row.cgst_amount), "Output Tax CGST - KGOPL"),
							("SGST", row.sgst_rate_or_utgst_as_applicable, flt(row.sgst_amount_or_utgst_as_applicable), "Output Tax SGST - KGOPL"),
							("IGST", row.igst_rate, flt(row.igst_amount), "Output Tax IGST - KGOPL")
						]:
							if amount:
								existing_tax = next((t for t in si.taxes if t.account_head == acc_head), None)
								if existing_tax:
									existing_tax.tax_amount += amount
								else:
									si.append("taxes", {
										"charge_type": "On Net Total",
										"rate": rate,
										"account_head": acc_head,
										"tax_amount": amount,
										"description": tax_type
									})
					except Exception as row_error:
						group_errors = True
						errors.append({
							"idx": row.idx,
							"invoice_id": row.buyer_invoice_id,
							"event": row.event_type,
							"message": str(row_error)
						})

				if items_appended > 0:
					si.save(ignore_permissions=True)
					for j in si.items:
						j.item_tax_template = ""
						j.item_tax_rate = frappe._dict()
					si.due_date = getdate(today())
					si.save(ignore_permissions=True)

				if not group_errors and si.docstatus == 0 and si.items:
					return_invoice.append(si.name)

			except Exception as e:
				for row in rows:
					errors.append({
						"idx": row.idx,
						"invoice_id": row.buyer_invoice_id,
						"event": row.event_type,
						"message": str(e)
					})

		# Submit Return Invoices
		for sii in return_invoice:
			try:
				frappe.get_doc("Sales Invoice", sii).submit()
			except Exception as e:
				errors.append({
					"idx": "",
					"invoice_id": sii,
					"event": "Return",
					"message": f"Submit failed: {str(e)}"
				})

		self.error_json = str(json.dumps(errors))
		if len(errors) == 0:
			self.status = "Success"
		elif len(self.jio_mart_items) != len(errors):
			self.status = "Partial Success"
		else:
			self.status = "Error"

		self.save(ignore_permissions=True)

		return {
			"status": "partial" if errors else "success",
			"errors": errors
		}

		
def generate_error_html(errors):
    """Generate HTML table for errors"""
    html_content = '''
    <div style="margin: 20px 0;">
        <h4 style="color: #d73527; margin-bottom: 10px;">Sales Invoice Creation Errors</h4>
        <table style="width: 100%; border-collapse: collapse; border: 1px solid #ddd;">
            <thead>
                <tr style="background-color: #f8f9fa;">
                    <th style="border: 1px solid #ddd; padding: 8px 12px; text-align: left; font-weight: 600;">Row No</th>
                    <th style="border: 1px solid #ddd; padding: 8px 12px; text-align: left; font-weight: 600;">Invoice No</th>
                    <th style="border: 1px solid #ddd; padding: 8px 12px; text-align: left; font-weight: 600;">Error</th>
                </tr>
            </thead>
            <tbody>
    '''
    
    for error in errors:
        html_content += f'''
                <tr>
                    <td style="border: 1px solid #ddd; padding: 8px 12px;">{error['idx']}</td>
                    <td style="border: 1px solid #ddd; padding: 8px 12px;">{error['invoice_id']}</td>
                    <td style="border: 1px solid #ddd; padding: 8px 12px; color: #d73527;">{html.escape(error['message'])}</td>
                </tr>
        '''
    
    html_content += '''
            </tbody>
        </table>
    </div>
    '''
    
    return html_content





def update_progress(current, total, message="Processing..."):
    """Send realtime progress to the frontend"""
    percent = int((current / total) * 100) if total else 100
    frappe.publish_realtime(
        "data_import_progress",
        {"progress": percent, "message": message},
        user=frappe.session.user
    )
