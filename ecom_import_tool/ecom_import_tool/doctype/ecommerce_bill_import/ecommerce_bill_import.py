# Copyright (c) 2025, Sagar Ratan Garg and contributors
# For license information, please see license.txt

import frappe
from frappe import _
from frappe.model.document import Document
from frappe.core.doctype.data_import.importer import Importer
import pandas as pd
import io
from datetime import datetime


class EcommerceBillImport(Document):
	def validate(self):
		if not self.ecommerce_mapping:
			frappe.throw(_("Please select an Ecommerce Mapping"))

	def before_save(self):
		if self.import_file and not self.status:
			self.status = "Pending"

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
