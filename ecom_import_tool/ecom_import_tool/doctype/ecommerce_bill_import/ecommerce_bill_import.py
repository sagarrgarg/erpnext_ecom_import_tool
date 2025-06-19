# Copyright (c) 2025, Sagar Ratan Garg and contributors
# For license information, please see license.txt

import html
import frappe
from frappe import _
from frappe.auth import today
from frappe.model.document import Document
from frappe.core.doctype.data_import.importer import Importer
import pandas as pd
import io
from datetime import datetime, timedelta

from frappe.utils.file_manager import get_file_path
from frappe.utils import flt, getdate


class EcommerceBillImport(Document):
	def validate(self):
		if not self.ecommerce_mapping:
			frappe.throw(_("Please select an Ecommerce Mapping"))

	def before_save(self):
		if self.import_file and not self.status:
			self.status = "Pending"
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

	def start_import(self):
		if not self.import_file:
			frappe.throw(_("Please upload a file to import"))

		# Get mapping details
		mapping = frappe.get_doc("Ecommerce Mapping", self.ecommerce_mapping)
		
		# Process the file based on platform type
		if self.get_file_content():
			try:
				# Process the file
				processed_data = self.process_file(mapping)
				
				# Create temporary csv file for standard import
				temp_file = self.create_temp_import_file(processed_data)
				
				# Create Data Import doc
				data_import = self.create_data_import(temp_file)
				
				# Start the import
				data_import.start_import()
				
				self.status = "Success"
				self.save()
				return True
			except Exception as e:
				self.status = "Error"
				self.save()
				frappe.log_error(f"Ecom Import Failed: {str(e)}", self.name)
				return False
	
	def get_file_content(self):
		return frappe.get_all("File", filters={"file_url": self.import_file}, fields=["file_name", "content"])[0]
	
	def process_file(self, mapping):
		"""Process the uploaded file based on mapping configuration"""
		file_content = self.get_file_content()
		
		if not file_content:
			frappe.throw(_("Unable to read file content"))
		
		file_data = file_content.get("content")
		
		# Read the file using pandas
		df = pd.read_csv(io.StringIO(file_data)) if file_content.get("file_name").endswith(".csv") else pd.read_excel(io.BytesIO(file_data))
		
		# Apply mapping transformations
		processed_df = self.apply_mapping(df, mapping)
		
		return processed_df
	
	def apply_mapping(self, df, mapping):
		"""Apply the configured mappings to the dataframe"""
		# Get item mappings
		item_mappings = {d.ecom_sku: d.erp_item for d in mapping.ecom_item_table}
		
		# Get warehouse mappings
		warehouse_mappings = {d.ecommerce_warehouse: d.erp_warehouse for d in mapping.ecommerce_warehouse_mapping}
		
		# Get GSTIN mappings
		gstin_mappings = {d.ecommerce_gstin: d.erp_gstin for d in mapping.ecommerce_gstin_mapping}
		
		# Apply transformations based on platform type
		if mapping.platform == "Amazon":
			return self.process_amazon_data(df, item_mappings, warehouse_mappings, gstin_mappings, mapping)
		else:
			return self.process_generic_data(df, item_mappings, warehouse_mappings, gstin_mappings)
	
	def process_amazon_data(self, df, item_mappings, warehouse_mappings, gstin_mappings, mapping):
		"""Process Amazon-specific data format"""
		# Implementation for Amazon data formats
		
		# For Amazon-specific processing based on amazon_type field
		if self.amazon_type == "MTR B2B":
			# B2B specific transformations
			return self.process_amazon_b2b(df, item_mappings, warehouse_mappings, gstin_mappings, mapping)
		elif self.amazon_type == "MTR B2C":
			# B2C specific transformations
			return self.process_amazon_b2c(df, item_mappings, warehouse_mappings, gstin_mappings, mapping)
		elif self.amazon_type == "Stock Transfer":
			# Stock transfer specific transformations
			return self.process_amazon_stock_transfer(df, item_mappings, warehouse_mappings, gstin_mappings, mapping)
		
		# Default - if no specific type is set
		return df
	@frappe.whitelist()
	def create_invoice(self):
		if self.ecommerce_mapping=="Amazon":
			if self.amazon_type=="MTR B2B":
				self.create_sales_invoice_mtr_b2b()
			elif self.amazon_type=="MTR B2C":
				self.create_sales_invoice_mtr_b2c()
			else:
				self.create_invoice_or_delivery_note()
		if self.ecommerce_mapping=="CRED":
			pass
		if self.ecommerce_mapping=="Flipkart":
			self.create_flipkart_sales_invoice()
			



	def process_amazon_b2c(self, df, item_mappings, warehouse_mappings, gstin_mappings, mapping):
		"""Process Amazon B2C data format"""
		# Expected columns in Amazon B2C report:
		# order-id, order-item-id, purchase-date, payments-date, reporting-date,
		# promise-date, days-past-promise, buyer-email, buyer-name, buyer-phone-number,
		# sku, product-name, quantity-purchased, quantity-shipped, quantity-to-ship,
		# ship-service-level, recipient-name, ship-address-1, ship-address-2, 
		# ship-address-3, ship-city, ship-state, ship-postal-code, ship-country,
		# item-price, item-tax, shipping-price, shipping-tax, gift-wrap-price, 
		# gift-wrap-tax, item-promotion-discount, ship-promotion-discount, etc.
		
		# Create a new dataframe for Sales Invoice
		sales_invoices = []
		
		# Group by order-id to create one invoice per order
		order_groups = df.groupby('order-id')
		
		# Track processed orders to avoid duplication
		processed_orders = set()
		
		for order_id, order_data in order_groups:
			if order_id in processed_orders:
				continue
				
			processed_orders.add(order_id)
			
			# Get the first row for customer info
			first_row = order_data.iloc[0]
			
			# Basic validation
			if pd.isna(first_row['buyer-email']) or pd.isna(first_row['buyer-name']):
				frappe.log_error(f"Missing customer information for order {order_id}", "Amazon B2C Import")
				continue
			
			# Format purchase date
			purchase_date = first_row['purchase-date']
			try:
				purchase_date = pd.to_datetime(purchase_date).strftime("%Y-%m-%d")
			except:
				purchase_date = datetime.now().strftime("%Y-%m-%d")
			
			# Customer info
			customer = mapping.default_non_company_customer
			
			# Create invoice header
			invoice = {
				"doctype": "Sales Invoice",
				"naming_series": "ACC-SINV-.YYYY.-",
				"customer": customer,
				"posting_date": purchase_date,
				"due_date": purchase_date,
				"amazon_order_id": order_id,
				"is_pos": 0,
				"update_stock": 1,
				"items": []
			}
			
			# Add items
			for idx, row in order_data.iterrows():
				sku = row['sku']
				quantity = row['quantity-shipped'] if not pd.isna(row['quantity-shipped']) else row['quantity-purchased']
				
				if pd.isna(sku) or pd.isna(quantity) or float(quantity) <= 0:
					continue
					
				# Map SKU to item code
				item_code = item_mappings.get(sku)
				if not item_code:
					frappe.log_error(f"SKU {sku} not found in mapping for order {order_id}", "Amazon B2C Import")
					continue
				
				# Calculate rates
				rate = float(row['item-price']) if not pd.isna(row['item-price']) else 0
				tax_amount = float(row['item-tax']) if not pd.isna(row['item-tax']) else 0
				
				# Get warehouse
				warehouse = next(iter(warehouse_mappings.values()), None)
				
				# Create item
				item = {
					"item_code": item_code,
					"qty": float(quantity),
					"rate": rate,
					"amount": rate * float(quantity),
					"warehouse": warehouse,
					"amazon_sku": sku,
					"amazon_order_item_id": row['order-item-id']
				}
				
				invoice["items"].append(item)
			
			# Add shipping charges if present
			has_shipping = False
			shipping_total = 0
			for idx, row in order_data.iterrows():
				shipping_price = float(row['shipping-price']) if not pd.isna(row['shipping-price']) else 0
				if shipping_price > 0:
					has_shipping = True
					shipping_total += shipping_price
			
			if has_shipping:
				# Add shipping as an item
				shipping_item = {
					"item_code": "Shipping Charges", # Replace with your shipping item code
					"qty": 1,
					"rate": shipping_total,
					"amount": shipping_total,
					"is_shipping_charge": 1
				}
				invoice["items"].append(shipping_item)
			
			# Only add invoice if it has items
			if invoice["items"]:
				sales_invoices.append(invoice)
		
		# Convert to dataframe
		if sales_invoices:
			# Create a dataframe for import
			invoices_df = pd.json_normalize(sales_invoices, 'items', ['doctype', 'naming_series', 'customer', 
				'posting_date', 'due_date', 'amazon_order_id', 'is_pos', 'update_stock'], 
				record_prefix='items_')
			
			# Count the payloads
			self.payload_count = len(sales_invoices)
			
			return invoices_df
		
		return pd.DataFrame()
	
	def process_amazon_b2b(self, df, item_mappings, warehouse_mappings, gstin_mappings, mapping):
		"""Process Amazon B2B data format"""
		# Implementation for B2B specific format
		# This would be implemented similarly to B2C but with B2B specific logic
		
		# Placeholder for now
		frappe.log_error("Amazon B2B import not yet implemented", "Amazon Import")
		return pd.DataFrame()
	
	def process_amazon_stock_transfer(self, df, item_mappings, warehouse_mappings, gstin_mappings, mapping):
		"""Process Amazon Stock Transfer data format"""
		# Implementation for Stock Transfer specific format
		
		# Placeholder for now
		frappe.log_error("Amazon Stock Transfer import not yet implemented", "Amazon Import") 
		return pd.DataFrame()
	
	def process_generic_data(self, df, item_mappings, warehouse_mappings, gstin_mappings):
		"""Process generic e-commerce data format"""
		# Generic transformations for other platforms
		return df
	
	def create_temp_import_file(self, df):
		"""Create a temporary CSV file for the standard import process"""
		csv_content = df.to_csv(index=False)
		
		# Create a temporary file
		file_doc = frappe.new_doc("File")
		file_doc.file_name = f"temp_import_{self.name}.csv"
		file_doc.content = csv_content
		file_doc.is_private = 1
		file_doc.attached_to_doctype = self.doctype
		file_doc.attached_to_name = self.name
		file_doc.save()
		
		return file_doc.file_url
	
	def create_data_import(self, file_url):
		"""Create a Data Import document to use the standard import process"""
		# Determine what doctype to import to based on amazon_type or other logic
		reference_doctype = "Sales Invoice" # Example - adjust based on your requirements
		
		data_import = frappe.new_doc("Data Import")
		data_import.reference_doctype = reference_doctype
		data_import.import_type = "Insert New Records"
		data_import.import_file = file_url
		data_import.submit_after_import = self.submit_after_import
		data_import.mute_emails = 1
		data_import.save()
		
		return data_import


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
			# df = pd.read_excel(xl_file_path, header=2)
			df = pd.read_excel(xl_file_path, sheet_name=2)
			df2 = pd.read_excel(xl_file_path, sheet_name=1)

			print(f"Excel read successfully. Shape: {df.shape}")
			print(f"Columns: {list(df.columns)}")

			# Get child table doctype name
			child_doctype = None
			for field in frappe.get_meta(self.doctype).fields:
				if field.fieldname == 'cred_items':
					child_doctype = field.options
					break

			if not child_doctype:
				frappe.throw("Could not find child table doctype for 'cred_items'")

			print(f"Child DocType: {child_doctype}")

			# Get child table fields
			child_meta = frappe.get_meta(child_doctype)
			valid_fields = [field.fieldname for field in child_meta.fields]
			print(f"Valid child table fields: {valid_fields}")

			success_count = 0
			for index, row in df.iterrows():
				print(f"\n--- Processing Row {index + 1} ---")

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
				print("##########################",row)
				child_row2 = self.append("cred", {})
				mapped_count = 0
				for column_name in df2.columns:
					fieldname = column_name.strip().lower().replace(' ', '_')
					value = row[column_name]
					# print("#########################",fieldname,value)
					child_row2.set(fieldname, clean(value))
					mapped_count += 1

				if mapped_count > 0:
					success_count += 1

			print(f"Total rows successfully mapped: {success_count}")

	def append_flipkart(self):
		self.flipkart_items = []
		if self.flipkart_attach:
			import pandas as pd
			import numpy as np
			import frappe
			from frappe.utils.file_manager import get_file_path
			import os

			def clean(val):
				if pd.isna(val):
					return ""
				try:
					return str(val).strip()
				except Exception:
					return ""

			print(f"Attachment URL: {self.flipkart_attach}")

			xl_file_url = self.flipkart_attach
			filename = xl_file_url.split('/files/')[-1]
			print(f"Filename extracted: {filename}")

			xl_file_path = get_file_path(filename)
			print(f"File path: {xl_file_path}")

			if not os.path.exists(xl_file_path):
				frappe.throw(f"File not found at path: {xl_file_path}")

			# Read Excel file using appropriate header row
			# df = pd.read_excel(xl_file_path, header=2)
			df = pd.read_excel(xl_file_path, sheet_name=1)
			df_cash = pd.read_excel(xl_file_path, sheet_name=2)

			
			# Get child table fields
			child_meta = frappe.get_meta("Flipkart Items")
			valid_fields = [field.fieldname for field in child_meta.fields]

			for index, row in df.iterrows():
				print("#########################45656",row)
				print(f"\n--- Processing Row {index + 1} ---")

				child_row = self.append("flipkart_items", {})

				for column_name in df.columns:
					print("###########################3",column_name)
					fieldname = column_name.strip().lower().replace(' ', '_')
					value = row[column_name]

					if fieldname in valid_fields:
						child_row.set(fieldname, clean(value))
					child_row.set("product_titledescription",clean(row.get("Product Title/Description", "")))
			child_meta = frappe.get_meta("Flipkart Transaction Items")
			valid_fields = [field.fieldname for field in child_meta.fields]

			for index, row in df_cash.iterrows():

				child_row = self.append("flipkart_cashback", {})

				for column_name in df_cash.columns:
					fieldname = column_name.strip().lower().replace(' ', '_')
					value = row[column_name]

					if fieldname in valid_fields:
						child_row.set(fieldname, clean(value))

					child_row.set("product_titledescription",clean(row.get("Product Title/Description", "")))
					







	@frappe.whitelist()
	def create_sales_invoice_mtr_b2b(self):
		val = frappe.db.get_value("Ecommerce Mapping", {"platform": "Amazon"}, "default_non_company_customer")

		# Initialize error tracking
		errors = []
		success_count = 0
		
		# Dictionary to group items by invoice number
		invoice_groups = {}
		
		# Group items by invoice number first
		for idx, child_row in enumerate(self.mtr_b2b, 1):
			invoice_no = child_row.invoice_number
			if invoice_no not in invoice_groups:
				invoice_groups[invoice_no] = []
			invoice_groups[invoice_no].append((idx, child_row))

		# Process each invoice group
		for invoice_no, items_data in invoice_groups.items():
			try:
				customer=frappe.db.get_value("Customer",{"gstin":items_data[0][1].get("customer_bill_to_gstid")},"name")
				if not customer:
					cus=frappe.new_doc("Customer")
					cus.gstin=items_data[0][1].get("customer_bill_to_gstid")
					cus.customer_name=items_data[0][1].get("buyer_name")
					cus.save(ignore_permissions=True)
					customer=cus.name
				# Check if Sales Invoice already exists with this custom_inv_no
				existing_si_draft = frappe.db.get_value("Sales Invoice", 
												{"custom_inv_no": invoice_no, "docstatus": 0,"is_return": 0}, 
												"name")
				existing_si = frappe.db.get_value("Sales Invoice", 
												{"custom_inv_no": invoice_no, "docstatus": 1,"is_return": 0}, 
												"name")

				
				if existing_si:
					doc=frappe.get_doc("Sales Invoice",existing_si)
					if items_data[0][1].get("transaction_type")=="Cancel":
						doc.cancel()
						continue

						
				if existing_si_draft:
					# Load existing invoice
					si = frappe.get_doc("Sales Invoice", existing_si)
					frappe.msgprint(f"Found existing invoice {si.name} for invoice number {invoice_no}. Appending items.")
				else:
					# Create new invoice
					si = frappe.new_doc('Sales Invoice')
					if items_data[0][1].get("transaction_type")=="Refund":
						si.is_return=1
						si.return_against=existing_si
					
					# Basic Info - only set for new invoices
					si.customer = customer
					si.posting_date = getdate(today())
					si.custom_inv_no = invoice_no
					si.taxes_and_charges = ""
					si.taxes = []
					si.update_stock = 0
					
					frappe.msgprint(f"Creating new invoice for invoice number {invoice_no}.")

				# Get Amazon mapping once per invoice
				amazon = frappe.get_doc("Ecommerce Mapping", {"platform": "Amazon"})
				
				# Process all items for this invoice
				total_tax_amount = 0
				cgst_rate = 0
				sgst_rate = 0
				igst_rate = 0
				utgst_rate = 0
				warehouse = None
				location = None
				com_address=None
				ecommerce_gstin=None
				
				for idx, child_row in items_data:
					try:
						# Get item code
						itemcode = None
						for jk in amazon.ecom_item_table:
							if jk.ecom_item_id == child_row.get(amazon.ecom_sku_column_header):
								itemcode = jk.erp_item
								break
						
						# Get warehouse and location
						for kk in amazon.ecommerce_warehouse_mapping:
							if kk.ecom_warehouse_id == child_row.warehouse_id:
								warehouse = kk.erp_warehouse
								location = kk.location
								com_address=kk.erp_address
								break
						for gstin in amazon.ecommerce_gstin_mapping:
							company_gstin=frappe.db.get_value("Address",com_address,"gstin")
							if gstin.erp_company_gstin==company_gstin:
								ecommerce_gstin=gstin.ecommerce_operator_gstin
								break
						# Set warehouse and location for the invoice (if not already set)
						if not si.location:
							si.location = location
						if not si.set_warehouse:
							si.set_warehouse = warehouse

						si.company_address=com_address
						si.ecommerce_gstin=ecommerce_gstin
						# Add Item Row
						si.append("items", {
							"item_code": itemcode,
							"qty": -flt(child_row.quantity) if items_data[0][1].get("transaction_type")=="Refund" else flt(child_row.quantity),
							"rate": flt(child_row.tax_exclusive_gross),
							"description": child_row.item_description,
							"warehouse": warehouse,
							"tax_rate": flt(child_row.total_tax_amount),
							"margin_type": "Amount" if flt(child_row.item_promo_discount)>0 else None,
							"margin_rate_or_amount":flt(child_row.item_promo_discount)

						})
						
						# Accumulate tax rates (assuming same tax rates for all items in same invoice)
						if items_data[0][1].get("transaction_type")=="Refund":
							cgst_rate = -flt(child_row.cgst_tax)
							sgst_rate = -flt(child_row.sgst_tax)
							igst_rate = -flt(child_row.igst_tax)
							utgst_rate = -flt(child_row.utgst_tax)

						else:
							cgst_rate = flt(child_row.cgst_tax)
							sgst_rate = flt(child_row.sgst_tax)
							igst_rate = flt(child_row.igst_tax)
							utgst_rate = flt(child_row.utgst_tax)
						
						
					except Exception as item_error:
						error_msg = str(item_error)
						errors.append({
							'row_no': idx,
							'invoice_no': invoice_no,
							'error': f"Item processing error: {error_msg}"
						})
						frappe.log_error(
							message=f"Error processing item for row {idx}, Invoice: {invoice_no}. Error: {error_msg}",
							title="Sales Invoice Item Processing Error"
						)

				# Update or add taxes for the invoice
				if cgst_rate or sgst_rate or igst_rate:
					# For new invoices, add taxes; for existing invoices, update existing taxes
					if not existing_si:
						# New invoice - add taxes
						if cgst_rate > 0:
							si.append("taxes", {
								"charge_type": "On Net Total",
								"account_head": "Output Tax CGST - KGOPL",
								"rate": cgst_rate,
								"description": "CGST"
							})
						if sgst_rate > 0 or utgst_rate > 0:
							si.append("taxes", {
								"charge_type": "On Net Total",
								"account_head": "Output Tax SGST - KGOPL",
								"rate": sgst_rate + utgst_rate,
								"description": "SGST"
							})
						if igst_rate > 0:
							si.append("taxes", {
								"charge_type": "On Net Total",
								"account_head": "Output Tax IGST - KGOPL",
								"rate": igst_rate,
								"description": "IGST"
							})
					else:
						# Existing invoice - update existing tax rates if they exist
						# The tax amounts will be recalculated automatically by ERPNext based on new net total
						cgst_found = False
						sgst_found = False
						igst_found = False
						
						for tax_row in si.taxes:
							if "CGST" in tax_row.description and cgst_rate > 0:
								tax_row.rate = cgst_rate
								cgst_found = True
							elif "SGST" in tax_row.description and (sgst_rate > 0 or utgst_rate > 0):
								tax_row.rate = sgst_rate + utgst_rate
								sgst_found = True
							elif "IGST" in tax_row.description and igst_rate > 0:
								tax_row.rate = igst_rate
								igst_found = True
						
						# Add missing tax rows if they don't exist
						if cgst_rate > 0 and not cgst_found:
							si.append("taxes", {
								"charge_type": "On Net Total",
								"account_head": "Output Tax CGST - KGOPL",
								"rate": cgst_rate,
								"description": "CGST"
							})
						if (sgst_rate > 0 or utgst_rate > 0) and not sgst_found:
							si.append("taxes", {
								"charge_type": "On Net Total",
								"account_head": "Output Tax SGST - KGOPL",
								"rate": sgst_rate + utgst_rate,
								"description": "SGST"
							})
						if igst_rate > 0 and not igst_found:
							si.append("taxes", {
								"charge_type": "On Net Total",
								"account_head": "Output Tax IGST - KGOPL",
								"rate": igst_rate,
								"description": "IGST"
							})

				# Save and Submit the invoice after processing all items
				si.save(ignore_permissions=True)
				si.submit()
				
				success_count += len(items_data)  # Count all items in this invoice
				frappe.msgprint(f"Sales Invoice {si.name} processed successfully with {len(items_data)} items.")

			except Exception as e:
				# Capture error details for the entire invoice
				error_msg = str(e)
				
				# Add error for all items in this failed invoice
				for idx, child_row in items_data:
					errors.append({
						'row_no': idx,
						'invoice_no': invoice_no,
						'error': f"Invoice processing error: {error_msg}"
					})
				
				# Log the error for debugging
				frappe.log_error(
					message=f"Error creating/updating Sales Invoice for Invoice: {invoice_no}. Error: {error_msg}",
					title="Sales Invoice Processing Error"
				)

		# Determine status and update fields
		total_rows = len(self.mtr_b2b)

		if errors:
			error_html = generate_error_html(errors)
			self.error_html = error_html
			
			# Set status based on results
			if success_count == 0:
				# All rows failed
				self.status = "Error"
				frappe.msgprint(
					f"All {total_rows} invoice items failed processing. Check Error HTML field for details.",
					indicator="red",
					alert=True
				)
			else:
				# Some succeeded, some failed
				self.status = "Partial Success"
				frappe.msgprint(
					f"Process completed with partial success. {success_count} items processed successfully, {len(errors)} failed. Check Error HTML field for details.",
					indicator="orange",
					alert=True
				)
		else:
			# All succeeded
			self.error_html = ""
			self.status = "Success"
			frappe.msgprint(
				f"All {success_count} items processed successfully across {len(invoice_groups)} invoices!",
				indicator="green"
			)

		# Save the document with updated status and error_html
		self.save()

		return success_count
	
	
	@frappe.whitelist()
	def create_sales_invoice_mtr_b2c(self):
		val = frappe.db.get_value("Ecommerce Mapping", {"platform": "Amazon"}, "default_non_company_customer")

		# Initialize error tracking
		errors = []
		success_count = 0
		
		# Dictionary to group items by invoice number
		invoice_groups = {}
		
		# Group items by invoice number first
		for idx, child_row in enumerate(self.mtr_b2c, 1):
			invoice_no = child_row.invoice_number
			if invoice_no not in invoice_groups:
				invoice_groups[invoice_no] = []
			invoice_groups[invoice_no].append((idx, child_row))

		# Process each invoice group
		for invoice_no, items_data in invoice_groups.items():
			try:
				existing_si_draft = frappe.db.get_value("Sales Invoice", 
												{"custom_inv_no": invoice_no, "docstatus": 0}, 
												"name")
				existing_si = frappe.db.get_value("Sales Invoice", 
												{"custom_inv_no": invoice_no, "docstatus": 1}, 
												"name")
				if existing_si:
					doc=frappe.get_doc("Sales Invoice",existing_si)
					if items_data[0][1].get("transaction_type")=="Cancel":
						doc.cancel()
						continue

						
				if existing_si_draft:
					# Load existing invoice
					si = frappe.get_doc("Sales Invoice", existing_si)
					frappe.msgprint(f"Found existing invoice {si.name} for invoice number {invoice_no}. Appending items.")
				else:
					# Create new invoice
					si = frappe.new_doc('Sales Invoice')
					if items_data[0][1].get("transaction_type")=="Refund":
						si.is_return=1
						si.return_against=existing_si
					# Basic Info - only set for new invoices
					si.customer = val
					si.posting_date = getdate(today())
					si.custom_inv_no = invoice_no
					si.taxes_and_charges = ""
					si.taxes = []
					si.update_stock = 0
					
					frappe.msgprint(f"Creating new invoice for invoice number {invoice_no}.")

				# Get Amazon mapping once per invoice
				amazon = frappe.get_doc("Ecommerce Mapping", {"platform": "Amazon"})
				
				# Process all items for this invoice
				total_tax_amount = 0
				cgst_rate = 0
				sgst_rate = 0
				igst_rate = 0
				utgst_rate = 0
				warehouse = None
				location = None
				com_address=None
				ecommerce_gstin=None
				
				for idx, child_row in items_data:
					try:
						# Get item code
						itemcode = None
						for jk in amazon.ecom_item_table:
							if jk.ecom_item_id == child_row.get(amazon.ecom_sku_column_header):
								itemcode = jk.erp_item
								break
						
						# Get warehouse and location
						for kk in amazon.ecommerce_warehouse_mapping:
							if kk.ecom_warehouse_id == child_row.warehouse_id:
								warehouse = kk.erp_warehouse
								location = kk.location
								com_address=kk.erp_address
								break
						for gstin in amazon.ecommerce_gstin_mapping:
							company_gstin=frappe.db.get_value("Address",com_address,"gstin")
							if gstin.erp_company_gstin==company_gstin:
								ecommerce_gstin=gstin.ecommerce_operator_gstin
								break
						# Set warehouse and location for the invoice (if not already set)
						if not si.location:
							si.location = location
						if not si.set_warehouse:
							si.set_warehouse = warehouse

						si.company_address=com_address
						si.ecommerce_gstin=ecommerce_gstin

						# Add Item Row
						si.append("items", {
							"item_code": itemcode,
							"qty": -flt(child_row.quantity) if items_data[0][1].get("transaction_type")=="Refund" else flt(child_row.quantity),
							"rate": flt(child_row.tax_exclusive_gross),
							"description": child_row.item_description,
							"warehouse": warehouse,
							"tax_rate": flt(child_row.total_tax_amount),
							"margin_type": "Amount" if flt(child_row.item_promo_discount)>0 else None,
							"margin_rate_or_amount":flt(child_row.item_promo_discount)
						})
						
						# Accumulate tax rates (assuming same tax rates for all items in same invoice)
						if items_data[0][1].get("transaction_type")=="Refund":
							cgst_rate = -flt(child_row.cgst_tax)
							sgst_rate = -flt(child_row.sgst_tax)
							igst_rate = -flt(child_row.igst_tax)
							utgst_rate = -flt(child_row.utgst_tax)

						else:
							cgst_rate = flt(child_row.cgst_tax)
							sgst_rate = flt(child_row.sgst_tax)
							igst_rate = flt(child_row.igst_tax)
							utgst_rate = flt(child_row.utgst_tax)
						
					except Exception as item_error:
						error_msg = str(item_error)
						errors.append({
							'row_no': idx,
							'invoice_no': invoice_no,
							'error': f"Item processing error: {error_msg}"
						})
						frappe.log_error(
							message=f"Error processing item for row {idx}, Invoice: {invoice_no}. Error: {error_msg}",
							title="Sales Invoice Item Processing Error"
						)

				# Update or add taxes for the invoice
				if cgst_rate or sgst_rate or igst_rate:
					# For new invoices, add taxes; for existing invoices, update existing taxes
					if not existing_si:
						# New invoice - add taxes
						if cgst_rate > 0:
							si.append("taxes", {
								"charge_type": "On Net Total",
								"account_head": "Output Tax CGST - KGOPL",
								"tax_amount": cgst_rate,
								"description": "CGST"
							})
						if sgst_rate > 0 or utgst_rate > 0:
							si.append("taxes", {
								"charge_type": "On Net Total",
								"account_head": "Output Tax SGST - KGOPL",
								"tax_amount": sgst_rate + utgst_rate,
								"description": "SGST"
							})
						if igst_rate > 0:
							si.append("taxes", {
								"charge_type": "On Net Total",
								"account_head": "Output Tax IGST - KGOPL",
								"tax_amount": igst_rate,
								"description": "IGST"
							})
					else:
						# Existing invoice - update existing tax rates if they exist
						# The tax amounts will be recalculated automatically by ERPNext based on new net total
						cgst_found = False
						sgst_found = False
						igst_found = False
						
						for tax_row in si.taxes:
							if "CGST" in tax_row.description and cgst_rate > 0:
								tax_row.tax_amount += cgst_rate
								cgst_found = True
							elif "SGST" in tax_row.description and (sgst_rate > 0 or utgst_rate > 0):
								tax_row.tax_amount += sgst_rate + utgst_rate
								sgst_found = True
							elif "IGST" in tax_row.description and igst_rate > 0:
								tax_row.tax_amount += igst_rate
								igst_found = True
						
						# Add missing tax rows if they don't exist
						if cgst_rate > 0 and not cgst_found:
							si.append("taxes", {
								"charge_type": "On Net Total",
								"account_head": "Output Tax CGST - KGOPL",
								"rate": cgst_rate,
								"description": "CGST"
							})
						if (sgst_rate > 0 or utgst_rate > 0) and not sgst_found:
							si.append("taxes", {
								"charge_type": "On Net Total",
								"account_head": "Output Tax SGST - KGOPL",
								"rate": sgst_rate + utgst_rate,
								"description": "SGST"
							})
						if igst_rate > 0 and not igst_found:
							si.append("taxes", {
								"charge_type": "On Net Total",
								"account_head": "Output Tax IGST - KGOPL",
								"rate": igst_rate,
								"description": "IGST"
							})

				# Save and Submit the invoice after processing all items
				si.save(ignore_permissions=True)
				si.submit()
				
				success_count += len(items_data)  # Count all items in this invoice
				frappe.msgprint(f"Sales Invoice {si.name} processed successfully with {len(items_data)} items.")

			except Exception as e:
				# Capture error details for the entire invoice
				error_msg = str(e)
				
				# Add error for all items in this failed invoice
				for idx, child_row in items_data:
					errors.append({
						'row_no': idx,
						'invoice_no': invoice_no,
						'error': f"Invoice processing error: {error_msg}"
					})
				
				# Log the error for debugging
				frappe.log_error(
					message=f"Error creating/updating Sales Invoice for Invoice: {invoice_no}. Error: {error_msg}",
					title="Sales Invoice Processing Error"
				)

		# Determine status and update fields
		total_rows = len(self.mtr_b2b)

		if errors:
			error_html = generate_error_html(errors)
			self.error_html = error_html
			
			# Set status based on results
			if success_count == 0:
				# All rows failed
				self.status = "Error"
				frappe.msgprint(
					f"All {total_rows} invoice items failed processing. Check Error HTML field for details.",
					indicator="red",
					alert=True
				)
			else:
				# Some succeeded, some failed
				self.status = "Partial Success"
				frappe.msgprint(
					f"Process completed with partial success. {success_count} items processed successfully, {len(errors)} failed. Check Error HTML field for details.",
					indicator="orange",
					alert=True
				)
		else:
			# All succeeded
			self.error_html = ""
			self.status = "Success"
			frappe.msgprint(
				f"All {success_count} items processed successfully across {len(invoice_groups)} invoices!",
				indicator="green"
			)

		# Save the document with updated status and error_html
		self.save()

		return success_count
	
	@frappe.whitelist()
	def create_invoice_or_delivery_note(self):
		from frappe.utils import flt, today, getdate

		ecommerce_mapping = frappe.get_doc("Ecommerce Mapping", {"platform": "Amazon"})
		customer = frappe.db.get_value("Ecommerce Mapping", {"platform": "Amazon"}, "default_non_company_customer")

		errors = []
		success_count = 0
		invoice_groups = {}

		# Group by invoice number
		for idx, row in enumerate(self.mtr_b2c, 1):
			invoice_no = row.invoice_number
			if invoice_no not in invoice_groups:
				invoice_groups[invoice_no] = []
			invoice_groups[invoice_no].append((idx, row))

		for invoice_no, group_rows in invoice_groups.items():
			try:
				is_taxable = any(flt(row.igst_rate) > 0 for _, row in group_rows)

				# Create either Sales Invoice or Delivery Note
				doc = frappe.new_doc("Sales Invoice" if is_taxable else "Delivery Note")

				if is_taxable:
					doc.customer = customer
					doc.custom_inv_no = invoice_no
					doc.posting_date = getdate(today())
					doc.taxes_and_charges = ""
					doc.taxes = []
					doc.update_stock = 1
				else:
					doc.customer = customer
					doc.posting_date = getdate(today())
					doc.set_warehouse = ""
					doc.items = []

				for idx, row in group_rows:
					# Get item code from Ecommerce Mapping
					item_code = None
					for e_item in ecommerce_mapping.ecom_item_table:
						if e_item.ecom_item_id == row.sku:
							item_code = e_item.erp_item
							break
					if not item_code:
						raise Exception(f"Item mapping not found for SKU {row.sku}")

					# Get warehouse mapping
					warehouse = None
					for wh in ecommerce_mapping.ecommerce_warehouse_mapping:
						if wh.ecom_warehouse_id == row.ship_from_fc:
							warehouse = wh.erp_warehouse
							break
					if not warehouse:
						raise Exception(f"Warehouse mapping not found for FC {row.ship_from_fc}")

					doc.append("items", {
						"item_code": item_code,
						"qty": flt(row.quantity),
						"rate": flt(row.taxable_value),
						"warehouse": warehouse,
						"description": row.sku,
					})

				doc.save(ignore_permissions=True)
				doc.submit()
				success_count += len(group_rows)
				frappe.msgprint(f"{doc.doctype} {doc.name} created for Invoice No {invoice_no}")

			except Exception as e:
				for idx, row in group_rows:
					errors.append({
						'row_no': idx,
						'invoice_no': invoice_no,
						'error': f"{str(e)}"
					})
				frappe.log_error(f"Failed for Invoice {invoice_no}: {str(e)}", "Invoice/Delivery Note Error")

		if errors:
			self.error_html = generate_error_html(errors)
			self.status = "Partial Success" if success_count else "Error"
		else:
			self.error_html = ""
			self.status = "Success"

		self.save()
		return success_count

		
	def create_flipkart_sales_invoice(self):
		from frappe.utils import flt, getdate

		customer = frappe.db.get_value("Ecommerce Mapping", {"platform": "Flipkart"}, "default_non_company_customer")
		flipkart = frappe.get_doc("Ecommerce Mapping", "Flipkart")
		si_invoice=[]
		for i in self.flipkart_items:
			# Skip return if already created
			if i.event_sub_type =="Sale":
				original_inv= frappe.db.get_value("Sales Invoice", {
					"custom_inv_no": i.buyer_invoice_id,
					"is_return": 0,
					"docstatus":1
				}, "name")
				if original_inv:
					continue

				original_inv_draft = frappe.db.get_value("Sales Invoice", {
					"custom_inv_no": i.buyer_invoice_id,
					"is_return": 0 ,"docstatus":0
				}, "name")

				# Get item code from Flipkart SKU mapping
				item_code = None
				for jk in flipkart.ecom_item_table:
					if jk.ecom_item_id == i.get(flipkart.ecom_sku_column_header):
						item_code = jk.erp_item
						break

				# Get warehouse/location/address mapping
				warehouse, location, company_address = None, None, None
				for wh in flipkart.ecommerce_warehouse_mapping:
					if wh.ecom_warehouse_id == i.warehouse_id:
						warehouse = wh.erp_warehouse
						location = wh.location
						company_address = wh.erp_address
						break

				# Get GSTIN mapping
				ecommerce_gstin = None
				company_gstin = frappe.db.get_value("Address", company_address, "gstin")
				for gst in flipkart.ecommerce_gstin_mapping:
					if gst.erp_company_gstin == company_gstin:
						ecommerce_gstin = gst.ecommerce_operator_gstin
						break

				# Common item row
				item_row = {
					"item_code": item_code,
					"qty": flt(i.item_quantity),
					"rate": flt(i.price_before_discount),
					"description": i.product_titledescription,
					"warehouse": warehouse,
					"margin_type": "Amount",
					"margin_rate_or_amount": flt(i.total_discount),
				}

				# If return or no invoice yet → create new invoice
				if not original_inv_draft:
					si = frappe.new_doc("Sales Invoice")
					si.customer = customer
					si.set_posting_time = 1
					si.posting_date = getdate(i.buyer_invoice_date)
					si.custom_inv_no = i.buyer_invoice_id
					si.taxes_and_charges = ""
					si.update_stock = 0
					si.company_address = company_address
					si.ecommerce_gstin = ecommerce_gstin
					si.location = location

					si.append("items", item_row)

					# Add tax rows
					if i.cgst_amount > 0:
						si.append("taxes", {
							"charge_type": "On Net Total",
							"account_head": "Output Tax CGST - KGOPL",
							"tax_amount": flt(i.cgst_amount),
							"description": "CGST"
						})
					if i.sgst_amount > 0:
						si.append("taxes", {
							"charge_type": "On Net Total",
							"account_head": "Output Tax SGST - KGOPL",
							"tax_amount": flt(i.sgst_amount),
							"description": "SGST"
						})
					if i.igst_amount > 0:
						si.append("taxes", {
							"charge_type": "On Net Total",
							"account_head": "Output Tax IGST - KGOPL",
							"tax_amount": flt(i.igst_amount),
							"description": "IGST"
						})

					si.save()
					si_invoice.append(si.name)

				else:
					# Update existing invoice (non-return)
					si = frappe.get_doc("Sales Invoice", original_inv)
					si.append("items", item_row)

					# Update tax values
					for tax_row in si.taxes:
						if "CGST" in tax_row.description and i.cgst_amount > 0:
							tax_row.tax_amount += flt(i.cgst_amount)
						elif "SGST" in tax_row.description and i.sgst_amount > 0:
							tax_row.tax_amount += flt(i.sgst_amount)
						elif "IGST" in tax_row.description and i.igst_amount > 0:
							tax_row.tax_amount += flt(i.igst_amount)

					si.save()
					si_invoice.append(si.name)
		for sii in si_invoice:
			doc=frappe.get_doc("Sales Invoice",sii)
			doc.submit()
		return_invoice=[]
		for i in self.flipkart_items:
			if i.event_sub_type == "Return":
				existing_return = frappe.db.get_value("Sales Invoice", {
					"custom_inv_no": i.buyer_invoice_id,
					"is_return": 1,
					"docstatus":1
				}, "name")
				if existing_return:
					continue

				# Get original invoice for matching or for return linkage
				original_inv = frappe.db.get_value("Sales Invoice", {
					"custom_inv_no": i.buyer_invoice_id,
					"is_return": 0 ,"docstatus":1
				}, "name")
				return_draft = frappe.db.get_value("Sales Invoice", {
					"custom_inv_no": i.buyer_invoice_id,
					"is_return": 1 ,"docstatus":0
				}, "name")

				# Get item code from Flipkart SKU mapping
				item_code = None
				for jk in flipkart.ecom_item_table:
					if jk.ecom_item_id == i.get(flipkart.ecom_sku_column_header):
						item_code = jk.erp_item
						break

				# Get warehouse/location/address mapping
				warehouse, location, company_address = None, None, None
				for wh in flipkart.ecommerce_warehouse_mapping:
					if wh.ecom_warehouse_id == i.warehouse_id:
						warehouse = wh.erp_warehouse
						location = wh.location
						company_address = wh.erp_address
						break

				# Get GSTIN mapping
				ecommerce_gstin = None
				company_gstin = frappe.db.get_value("Address", company_address, "gstin")
				for gst in flipkart.ecommerce_gstin_mapping:
					if gst.erp_company_gstin == company_gstin:
						ecommerce_gstin = gst.ecommerce_operator_gstin
						break

				# Common item row
				item_row = {
					"item_code": item_code,
					"qty": -flt(i.item_quantity),
					"rate": flt(i.price_before_discount),
					"description": i.product_titledescription,
					"warehouse": warehouse,
					"margin_type": "Amount",
					"margin_rate_or_amount": flt(i.total_discount),
				}

				# If return or no invoice yet → create new invoice
				if original_inv and not return_draft:
					si = frappe.new_doc("Sales Invoice")
					si.customer = customer
					si.set_posting_time = 1
					si.posting_date = getdate(i.buyer_invoice_date)
					si.custom_inv_no = i.buyer_invoice_id
					si.taxes_and_charges = ""
					si.update_stock = 0
					si.company_address = company_address
					si.ecommerce_gstin = ecommerce_gstin
					si.location = location

					# Handle return linking
					si.is_return = 1
					if original_inv:
						si.return_against = original_inv
				
					si.append("items", item_row)

					# Add tax rows
					if i.cgst_amount > 0:
						si.append("taxes", {
							"charge_type": "On Net Total",
							"account_head": "Output Tax CGST - KGOPL",
							"tax_amount": flt(i.cgst_amount),
							"description": "CGST"
						})
					if i.sgst_amount > 0:
						si.append("taxes", {
							"charge_type": "On Net Total",
							"account_head": "Output Tax SGST - KGOPL",
							"tax_amount": flt(i.sgst_amount),
							"description": "SGST"
						})
					if i.igst_amount > 0:
						si.append("taxes", {
							"charge_type": "On Net Total",
							"account_head": "Output Tax IGST - KGOPL",
							"tax_amount": flt(i.igst_amount),
							"description": "IGST"
						})

					si.save()
					return_invoice.append(si.name)

				else:
					# Update existing invoice (non-return)
					si = frappe.get_doc("Sales Invoice", original_inv)
					si.append("items", item_row)

					# Update tax values
					for tax_row in si.taxes:
						if "CGST" in tax_row.description and i.cgst_amount > 0:
							tax_row.tax_amount += flt(i.cgst_amount)
						elif "SGST" in tax_row.description and i.sgst_amount > 0:
							tax_row.tax_amount += flt(i.sgst_amount)
						elif "IGST" in tax_row.description and i.igst_amount > 0:
							tax_row.tax_amount += flt(i.igst_amount)

					si.save()
					return_invoice.append(si.name)
		for sii in return_invoice:
			doc=frappe.get_doc("Sales Invoice",sii)
			doc.submit()
			


				
	def create_cred_sales_invoice(self):
		val = frappe.db.get_value("Ecommerce Mapping", {"platform": "Cred"}, "default_non_company_customer")
		si_items=[]
		for i in self.cred:
			if i.order_status==["CANCELLED","RTO"]:
				continue

			si_inv=frappe.db.get_value("Sales Invoice",{"custom_inv_no":i.order_item_id,"is_return":0,"docstatus":1},"name")
			si_inv_draft=frappe.db.get_value("Sales Invoice",{"custom_inv_no":i.order_item_id,"is_return":0 ,"docstatus":0},"name")
			
			if not si_inv:
				si=frappe.new_doc("Sales Invoice")
				# Basic Info - only set for new invoices
				si.customer = val
				si.set_posting_time=1
				si.posting_date = getdate(i.order_date_time)
				si.custom_inv_no = i.order_item_id
				si.taxes_and_charges = ""
				si.taxes = []
				si.update_stock = 0
				amazon=frappe.get_doc("Ecommerce Mapping",{"name":"Cred"})
				itemcode = None
				for jk in amazon.ecom_item_table:
					if jk.ecom_item_id == i.get(str(amazon.ecom_sku_column_header)):
						itemcode = jk.erp_item
						break
				
				# Get warehouse and location
				for kk in amazon.ecommerce_warehouse_mapping:
					if kk.ecom_warehouse_id == i.warehouse_location_code:
						warehouse = kk.erp_warehouse
						location = kk.location
						com_address=kk.erp_address
						break
				for gstin in amazon.ecommerce_gstin_mapping:
					company_gstin=frappe.db.get_value("Address",com_address,"gstin")
					if gstin.erp_company_gstin==company_gstin:
						ecommerce_gstin=gstin.ecommerce_operator_gstin
						break
				# Set warehouse and location for the invoice (if not already set)
				if not si.location:
					si.location = location
				if not si.set_warehouse:
					si.set_warehouse = warehouse

				si.company_address=com_address
				si.ecommerce_gstin=ecommerce_gstin
				# for jk in self.flipkart_cashback:
				# 	if i.order_item_id==jk.order_item_id:
				si.append("items",{
					"item_code":itemcode,
					"qty": 1,
					"rate": flt(i.net_gmv),
					"description": i.product_name,
					"warehouse": warehouse
				})
				if i.source_address_state==i.destination_address_state:

					if (flt(i.gmv)-flt(i.net_gmv))>0:
						si.append("taxes", {
							"charge_type": "On Net Total",
							"account_head": "Output Tax CGST - KGOPL",
							"tax_amount": (flt(i.gmv)-flt(i.net_gmv))/2,
							"description": "CGST"
						})
					if (flt(i.gmv)-flt(i.net_gmv))>0:
						si.append("taxes", {
							"charge_type": "On Net Total",
							"account_head": "Output Tax SGST - KGOPL",
							"tax_amount": (flt(i.gmv)-flt(i.net_gmv))/2,
							"description": "SGST"
						})
				else:
					if i.igst_rate > 0:
						si.append("taxes", {
							"charge_type": "On Net Total",
							"account_head": "Output Tax IGST - KGOPL",
							"tax_amount": (flt(i.gmv)-flt(i.net_gmv)),
							"description": "IGST"
						})
				si.save()
				si_items.append(si.name)
				# si.submit()
			elif si_inv_draft:
				si=frappe.get_doc("Sales Invoice",si_inv)
				for tax_row in si.taxes:
					if "CGST" in tax_row.description and i.cgst_rate > 0:
						tax_row.tax_amount += (flt(i.gmv)-flt(i.net_gmv))/2
					elif "SGST" in tax_row.description and i.sgst_rate > 0:
						tax_row.tax_amount += (flt(i.gmv)-flt(i.net_gmv))/2
					elif "IGST" in tax_row.description and i.igst_rate > 0:
						tax_row.tax_amount += (flt(i.gmv)-flt(i.net_gmv))

				si.append("items",{
					"item_code":itemcode,
					"qty": 1,
					"rate": flt(i.net_gmv),
					"description": i.product_name,
					"warehouse": warehouse
				})
				si.save()
				si_items.append(si.name)
				# si.submit()
		if len(si_items)>0:
			for si in si_items:
				doc=frappe.get_doc("Sales Invoice",si)
				doc.submit()
		si_return_items=[]
		for i in self.cred_items:
			if i.order_status in ["CANCELLED","RTO"]:
				continue

			si_inv=frappe.db.get_value("Sales Invoice",{"custom_inv_no":i.order_item_id,"is_return":1,"docstatus":1},"name")
			si_inv_draft=frappe.db.get_value("Sales Invoice",{"custom_inv_no":i.order_item_id,"is_return":1 ,"docstatus":0},"name")
			
			if not si_inv:
				si=frappe.new_doc("Sales Invoice")
				# Basic Info - only set for new invoices
				si.customer = val
				si.set_posting_time=1
				si.posting_date = getdate(i.refund_date_time)
				si.custom_inv_no =i.order_item_id
				si.taxes_and_charges = ""
				si.taxes = []
				si.update_stock = 0
				amazon=frappe.get_doc("Ecommerce Mapping",{"name":"Cred"})
				itemcode = None
				for jk in amazon.ecom_item_table:
					if jk.ecom_item_id == i.get(str(amazon.ecom_sku_column_header)):
						itemcode = jk.erp_item
						break
				
				# Get warehouse and location
				for kk in amazon.ecommerce_warehouse_mapping:
					if kk.ecom_warehouse_id == i.warehouse_location_code:
						warehouse = kk.erp_warehouse
						location = kk.location
						com_address=kk.erp_address
						break
				for gstin in amazon.ecommerce_gstin_mapping:
					company_gstin=frappe.db.get_value("Address",com_address,"gstin")
					if gstin.erp_company_gstin==company_gstin:
						ecommerce_gstin=gstin.ecommerce_operator_gstin
						break
				# Set warehouse and location for the invoice (if not already set)
				if not si.location:
					si.location = location
				if not si.set_warehouse:
					si.set_warehouse = warehouse

				si.company_address=com_address
				si.ecommerce_gstin=ecommerce_gstin
				# for jk in self.flipkart_cashback:
				# 	if i.order_item_id==jk.order_item_id:
				si.append("items",{
					"item_code":itemcode,
					"qty": -1,
					"rate": flt(i.net_gmv),
					"description": i.product_name,
					"warehouse": warehouse
				})
				if i.source_address_state==i.destination_address_state:

					if (flt(i.gmv)-flt(i.net_gmv))>0:
						si.append("taxes", {
							"charge_type": "On Net Total",
							"account_head": "Output Tax CGST - KGOPL",
							"tax_amount": (flt(i.gmv)-flt(i.net_gmv))/2,
							"description": "CGST"
						})
					if (flt(i.gmv)-flt(i.net_gmv))>0:
						si.append("taxes", {
							"charge_type": "On Net Total",
							"account_head": "Output Tax SGST - KGOPL",
							"tax_amount": (flt(i.gmv)-flt(i.net_gmv))/2,
							"description": "SGST"
						})
				else:
					if i.igst_rate > 0:
						si.append("taxes", {
							"charge_type": "On Net Total",
							"account_head": "Output Tax IGST - KGOPL",
							"tax_amount": (flt(i.gmv)-flt(i.net_gmv)),
							"description": "IGST"
						})
				si.save()
				si_return_items.append(si.name)
				# si.submit()
			elif si_inv_draft:
				si=frappe.get_doc("Sales Invoice",si_inv)
				for tax_row in si.taxes:
					if "CGST" in tax_row.description and i.cgst_rate > 0:
						tax_row.tax_amount += (flt(i.gmv)-flt(i.net_gmv))/2
					elif "SGST" in tax_row.description and i.sgst_rate > 0:
						tax_row.tax_amount += (flt(i.gmv)-flt(i.net_gmv))/2
					elif "IGST" in tax_row.description and i.igst_rate > 0:
						tax_row.tax_amount += (flt(i.gmv)-flt(i.net_gmv))

				si.append("items",{
					"item_code":itemcode,
					"qty": 1,
					"rate": flt(i.net_gmv),
					"description": i.product_name,
					"warehouse": warehouse
				})
				si.save()
				si_return_items.append(si.name)
				# si.submit()
		if len(si_return_items)>0:
			for si in si_return_items:
				doc=frappe.get_doc("Sales Invoice",si)
				doc.submit()

		
		
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
                    <td style="border: 1px solid #ddd; padding: 8px 12px;">{error['row_no']}</td>
                    <td style="border: 1px solid #ddd; padding: 8px 12px;">{error['invoice_no']}</td>
                    <td style="border: 1px solid #ddd; padding: 8px 12px; color: #d73527;">{html.escape(error['error'])}</td>
                </tr>
        '''
    
    html_content += '''
            </tbody>
        </table>
    </div>
    '''
    
    return html_content

