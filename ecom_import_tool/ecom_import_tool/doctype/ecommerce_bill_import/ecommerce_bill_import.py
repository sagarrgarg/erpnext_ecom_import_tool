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


def clean_csv_cell(val):
	"""Normalize a CSV cell to a safe string.

	Why this exists:
	- E-commerce exports often contain long numeric IDs (order_item_id, shipment ids, etc.).
	- If pandas infers numeric types, those IDs can become scientific notation (e.g. 4.36e+17)
	  and lose precision. That later causes rows/items to be treated as duplicates and skipped.

	Strategy:
	- Read CSV with dtype=str and disable NA parsing.
	- Then trim/strip quotes and normalize common "null-ish" strings.
	"""
	if val is None:
		return ""

	s = str(val).strip()
	if not s:
		return ""

	if s.lower() in {"nan", "none", "null"}:
		return ""

	while (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
		s = s[1:-1].strip()

	# Convert integer-like floats (e.g. "123.0") to "123"
	if s.endswith(".0") and s[:-2].lstrip("-").isdigit():
		s = s[:-2]

	return s


def parse_export_datetime(value):
	"""Parse export date/datetime with a day-first preference (DD-MM-YYYY).

	Why this exists:
	- Some platform exports provide dates like "01-12-2025" which are day-first.
	- `getdate()` can interpret ambiguous strings as month-first depending on settings.
	- For e-commerce imports we want deterministic India-style parsing.
	"""
	if not value:
		return None

	if isinstance(value, datetime):
		return value

	# Pandas Timestamp / Excel-derived value support
	if hasattr(value, "to_pydatetime"):
		try:
			return value.to_pydatetime()
		except Exception:
			pass

	s = clean_csv_cell(value)
	if not s:
		return None

	s = s.replace("\u00a0", " ").strip()

	# Try common day-first formats first
	for fmt in (
		"%d-%m-%Y %H:%M:%S",
		"%d-%m-%Y %H:%M",
		"%d/%m/%Y %H:%M:%S",
		"%d/%m/%Y %H:%M",
		"%d-%m-%Y",
		"%d/%m/%Y",
		"%d-%m-%y %H:%M:%S",
		"%d-%m-%y %H:%M",
		"%d/%m/%y %H:%M:%S",
		"%d/%m/%y %H:%M",
		"%d-%m-%y",
		"%d/%m/%y",
		"%Y-%m-%d %H:%M:%S",
		"%Y-%m-%d %H:%M",
		"%Y/%m/%d %H:%M:%S",
		"%Y/%m/%d %H:%M",
		"%Y-%m-%d",
		"%Y/%m/%d",
	):
		try:
			return datetime.strptime(s, fmt)
		except Exception:
			pass

	# Common case: extra suffixes; try trimming to 19 chars (YYYY-MM-DD HH:MM:SS / DD-MM-YYYY HH:MM:SS)
	s19 = s[:19]
	for fmt in ("%d-%m-%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%d/%m/%Y %H:%M:%S"):
		try:
			return datetime.strptime(s19, fmt)
		except Exception:
			pass

	# Last resort: dateutil parser with dayfirst=True
	try:
		from dateutil import parser as date_parser

		return date_parser.parse(s, dayfirst=True)
	except Exception:
		return None


def parse_export_date(value):
	"""Return a date from an export value (date or datetime string)."""
	dt = parse_export_datetime(value)
	return dt.date() if dt else None


def parse_export_time(value):
	"""Return a time from an export value (date or datetime string)."""
	dt = parse_export_datetime(value)
	return dt.time() if dt else None


def normalize_tax_rate(rate):
	"""Normalize tax rate to the percentage ERPNext expects (e.g. 5 for 5%).

	Some sources provide 0.05 (fraction) and some provide 5 (percent).
	"""
	rate = flt(rate)
	if 0 < rate < 1:
		return rate * 100
	return rate


def resolve_ecommerce_gstin_from_mapping(ecommerce_mapping, seller_gstin):
	"""Resolve `Sales Invoice.ecommerce_gstin` from `Ecommerce Mapping.ecommerce_gstin_mapping`.

	The `Ecommerce Mapping` doctype contains a child table `ecommerce_gstin_mapping` with fields:
	- `erp_company_gstin` (Link to `GSTIN`, autoname is `field:gstin`)
	- `ecommerce_operator_gstin` (E-commerce operator GSTIN)

	Files from different platforms may provide either the seller/company GSTIN or the operator GSTIN.
	To keep imports deterministic, we support both lookup keys:
	- If `seller_gstin` matches `ecommerce_operator_gstin` -> return `ecommerce_operator_gstin`
	- If `seller_gstin` matches `erp_company_gstin` -> return `ecommerce_operator_gstin`

	Args:
		ecommerce_mapping (Document): Ecommerce Mapping document (Amazon/Flipkart/Jiomart/Cred)
		seller_gstin (str): GSTIN value found in the import row

	Returns:
		str | None: The Ecommerce Operator GSTIN to set on the Sales Invoice, else None.
	"""
	if not ecommerce_mapping:
		return None

	gstin = (str(seller_gstin).strip().upper() if seller_gstin is not None else "")
	if not gstin:
		return None

	for row in (getattr(ecommerce_mapping, "ecommerce_gstin_mapping", None) or []):
		operator_gstin_raw = row.ecommerce_operator_gstin or ""
		operator_gstin = operator_gstin_raw.strip().upper()
		company_gstin = (row.erp_company_gstin or "").strip().upper()
		if gstin == operator_gstin or gstin == company_gstin:
			if not operator_gstin:
				return None

			# India Compliance validates `Sales Invoice.ecommerce_gstin` as a TCS (Tax Collector)
			# GSTIN. That means the 14th character must be "C" (see `india_compliance.gst_india.constants.TCS`).
			#
			# Validate here so the user gets a clear mapping-error pointing to the exact value.
			try:
				from india_compliance.gst_india.utils import validate_gstin as _validate_gstin

				operator_gstin = _validate_gstin(
					operator_gstin, label="E-commerce GSTIN", is_tcs_gstin=True
				)
			except Exception:
				frappe.throw(
					_(
						"Invalid Ecommerce Operator (TCS) GSTIN in Ecommerce Mapping '{mapping}'. "
						"Mapped value: '{operator_gstin}'. Seller GSTIN from file: '{seller_gstin}'. "
						"Please update '{mapping}' -> Ecommerce GSTIN Mapping to a valid TCS GSTIN (14th character must be 'C')."
					).format(
						mapping=ecommerce_mapping.name,
						operator_gstin=operator_gstin_raw,
						seller_gstin=seller_gstin,
					),
					title=_("Invalid GSTIN Mapping"),
				)

			return operator_gstin or None

	return None

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
    "chattisgarh": "22-Chhattisgarh",  # common misspelling seen in e-commerce exports
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

	def _publish_progress(self, *, current=None, total=None, progress=None, message="", phase=None):
		"""Publish realtime progress for long-running imports.

		This intentionally mimics the UX pattern of Frappe's Data Import:
		- Backend publishes realtime events during background jobs (RQ worker)
		- Frontend listens and shows a dashboard progress bar
		"""
		payload = {
			"doctype": self.doctype,
			"docname": self.name,
			"message": message or "",
		}
		if current is not None:
			payload["current"] = int(current)
		if total is not None:
			payload["total"] = int(total) if int(total) else 0
		if progress is not None:
			payload["progress"] = int(progress)
		if phase:
			payload["phase"] = phase

		frappe.publish_realtime("data_import_progress", payload, user=frappe.session.user)

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
			# Read CSV as strings to preserve long IDs exactly (avoid scientific notation)
			def clean(val):
				return clean_csv_cell(val)

			csv_file_url = self.mtr_b2b_attachment
			filename = csv_file_url.split('/files/')[-1]
			csv_file_path = get_file_path(filename)

			try:
				df = pd.read_csv(
					csv_file_path,
					dtype=str,
					keep_default_na=False,
					na_filter=False,
				)
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
			# Sort by invoice date for stable grouping/processing downstream
			if self.mtr_b2b:
					self.mtr_b2b.sort(
						key=lambda x: parse_export_date(x.invoice_date) or frappe.utils.getdate("1900-01-01")
				)

	def append_mtr_b2c(self):
		self.mtr_b2c = []
		if self.mtr_b2c_attachment:
			from frappe.utils.data import getdate

			def clean(val):
				return clean_csv_cell(val)

			csv_file_url = self.mtr_b2c_attachment
			filename = csv_file_url.split('/files/')[-1]
			csv_file_path = get_file_path(filename)

			try:
				df = pd.read_csv(
					csv_file_path,
					dtype=str,
					keep_default_na=False,
					na_filter=False,
				)
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
					key=lambda x: parse_export_date(x.invoice_date) or frappe.utils.getdate("1900-01-01")
            )

	


	def append_stock_transfer_attachment(self):
		self.stock_transfer=[]
		if self.stock_transfer_attachment:
			def clean(val):
				return clean_csv_cell(val)

			
			csv_file_url = self.stock_transfer_attachment
			filename = csv_file_url.split('/files/')[-1]
			csv_file_path = get_file_path(filename)

			try:
				df = pd.read_csv(
					csv_file_path,
					dtype=str,
					keep_default_na=False,
					na_filter=False,
				)
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
					key=lambda x: parse_export_date(x.invoice_date) or frappe.utils.getdate("1900-01-01")
            )
				
	
	def cred_append(self):
		"""Parse the attached CRED file.

		CRED has evolved over time (older Excel exports vs newer CSV exports). For CSV,
		we intentionally DO NOT populate the hidden child tables to avoid heavy document
		payloads; invoice creation reads the CSV directly in the background job.
		"""
		self.cred_items = []
		self.cred = []
		if not self.cred_attach:
			return

		import os
		import pandas as pd
		from frappe.utils.file_manager import get_file_path

		file_url = self.cred_attach
		filename = file_url.split("/files/")[-1]
		file_path = get_file_path(filename)

		if not os.path.exists(file_path):
			frappe.throw(f"File not found at path: {file_path}")

		ext = os.path.splitext(filename)[1].lower()
		if ext == ".csv":
			# CSV: validate only (invoice creation reads CSV directly in background job)
			try:
				df = pd.read_csv(
					file_path,
					dtype=str,
					keep_default_na=False,
					na_filter=False,
				)
			except Exception as e:
				frappe.throw(f"Error reading CRED CSV: {str(e)}")

			self.payload_count = len(df)
			return

		# ---------------- Legacy Excel (kept for backward compatibility) ----------------

		def clean(val):
			"""Normalize Excel cell values to string/date."""
			if pd.isna(val):
				return ""

			# Convert Excel serial to date string if in valid range
			if isinstance(val, (int, float)) and 30000 < val < 50000:
				try:
					return (datetime(1899, 12, 30) + timedelta(days=val)).strftime("%Y-%m-%d")
				except Exception:
					pass

			if isinstance(val, datetime):
				return val.strftime("%Y-%m-%d")

			try:
				return str(val).strip()
			except Exception:
				return str(val)

		df_returns = pd.read_excel(file_path, sheet_name=1)
		df_sales = pd.read_excel(file_path, sheet_name=0)

		return_child_doctype = frappe.get_meta(self.doctype).get_field("cred_items").options
		sale_child_doctype = frappe.get_meta(self.doctype).get_field("cred").options

		return_fields = {f.fieldname for f in frappe.get_meta(return_child_doctype).fields}
		sale_fields = {f.fieldname for f in frappe.get_meta(sale_child_doctype).fields}

		for _, row in df_returns.iterrows():
			child_row = self.append("cred_items", {})
			for column_name in df_returns.columns:
				fieldname = column_name.strip().lower().replace(" ", "_")
				if fieldname in return_fields:
					child_row.set(fieldname, clean(row[column_name]))

		for _, row in df_sales.iterrows():
			child_row = self.append("cred", {})
			for column_name in df_sales.columns:
				fieldname = column_name.strip().lower().replace(" ", "_")
				if fieldname in sale_fields:
					child_row.set(fieldname, clean(row[column_name]))

	def append_flipkart(self):
		import pandas as pd
		import frappe
		from frappe.utils.file_manager import get_file_path

		def clean(val):
			"""Normalize CSV cell values.

			Important: Flipkart exports contain long numeric IDs (e.g. Order Item ID).
			If pandas infers numeric types, those IDs can turn into scientific notation
			(e.g. 4.36e+17) and lose precision, which later causes invoices to be skipped.
			"""
			if val is None:
				return ""
			val = str(val).strip()
			if not val:
				return ""
			if val.lower() in {"nan", "none", "null"}:
				return ""
			while (val.startswith('"') and val.endswith('"')) or (val.startswith("'") and val.endswith("'")):
				val = val[1:-1].strip()
			# Convert integer-like floats (e.g. "123.0") to "123"
			if val.endswith(".0") and val[:-2].lstrip("-").isdigit():
				val = val[:-2]
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
			# Read everything as string to preserve long IDs exactly (avoid scientific notation)
			df = pd.read_csv(
				file_path,
				dtype=str,
				keep_default_na=False,
				na_filter=False,
			)
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
			from frappe.utils.data import getdate

			def clean(val):
				return clean_csv_cell(val)

			csv_file_url = self.jio_mart_attach
			filename = csv_file_url.split('/files/')[-1]
			csv_file_path = get_file_path(filename)

			try:
				df = pd.read_csv(
					csv_file_path,
					dtype=str,
					keep_default_na=False,
					na_filter=False,
				)
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
					key=lambda x: parse_export_date(x.buyer_invoice_date) or frappe.utils.getdate("1900-01-01")
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
			# Cancelled rows often have an empty invoice number; skip them to avoid errors
			invoice_no = (child_row.invoice_number or "").strip()
			if not invoice_no:
				continue

			invoice_groups.setdefault(invoice_no, []).append((idx, child_row))

		total_invoices = len(invoice_groups) or 1  # avoid div-by-zero

		# 🔹 Initial realtime update
		self._publish_progress(
			current=0,
			total=total_invoices,
			progress=0,
			message=f"Starting Amazon B2B import (0/{total_invoices})",
			phase="amazon_mtr_b2b",
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
				# If the sales invoice is already submitted, don't recreate it. Refunds (credit notes)
				# are handled below independently.
				if shipment_items and existing_si:
					shipment_items = []
				
				if shipment_items:
					try:
						# Ecommerce GSTIN is mandatory. Resolve it once per invoice group from mapping table.
						mapped_ecommerce_gstin = resolve_ecommerce_gstin_from_mapping(
							amazon, shipment_items[0][1].seller_gstin
						)
						if not mapped_ecommerce_gstin:
							raise Exception(
								f"Ecommerce GSTIN mapping missing for Seller GSTIN: {shipment_items[0][1].seller_gstin} "
								f"(Invoice No: {invoice_no}). Please add it in Ecommerce Mapping '{amazon.name}' -> Ecommerce GSTIN Mapping."
							)

						if existing_si_draft:
							si = frappe.get_doc("Sales Invoice", existing_si_draft)
						else:
							si = frappe.new_doc("Sales Invoice")
							si.customer = customer
							si.set_posting_time=1
							# Parse the datetime and add 2 seconds
							# invoice_datetime = datetime.strptime(str(items_data[0][1].get("invoice_date")), '%Y-%m-%d %H:%M:%S') if isinstance(items_data[0][1].get("invoice_date"), str) else items_data[0][1].get("invoice_date")
							# invoice_datetime_plus_2 = invoice_datetime + timedelta(seconds=2)
							invoice_dt = parse_export_datetime(items_data[0][1].get("invoice_date"))
							if not invoice_dt:
								raise Exception(f"Invalid Invoice Date: {items_data[0][1].get('invoice_date')}")
							si.posting_date = invoice_dt.date()
							si.posting_time = invoice_dt.time()
							si.custom_inv_no = invoice_no
							si.custom_ecommerce_invoice_id=invoice_no
							# Avoid duplicate primary key errors if an invoice with this name already exists
							existing_by_name = frappe.db.exists("Sales Invoice", invoice_no)
							if not existing_by_name:
								si.__newname = invoice_no
							si.custom_ecommerce_operator=self.ecommerce_mapping
							si.custom_ecommerce_type=self.amazon_type
							si.taxes = []
							si.update_stock = 1

						# Always set ecommerce_gstin from mapping (required for GST reporting)
						si.ecommerce_gstin = mapped_ecommerce_gstin

						# De-duplicate within this invoice (do NOT skip across other invoices)
						existing_item_ids = {
							d.get("custom_ecom_item_id")
							for d in (si.get("items") or [])
							if d.get("custom_ecom_item_id")
						}
						items_append=[]
						for idx, child_row in shipment_items:
							try:
								shipment_item_id = child_row.shipment_item_id
								if shipment_item_id and shipment_item_id in existing_item_ids:
									continue

								itemcode = next((i.erp_item for i in amazon.ecom_item_table if i.ecom_item_id == child_row.get(amazon.ecom_sku_column_header)), None)
								if not itemcode:
									error_names.append(invoice_no)
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
										error_names.append(invoice_no)
										raise Exception(f"Warehouse Mapping not found for Warehouse Id: {warehouse_id}")

								if location:
									si.location = location
								if warehouse:
									si.set_warehouse = warehouse

								si.company_address = com_address
								si.ecommerce_gstin = mapped_ecommerce_gstin
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
									"is_free_item": 1 if str(child_row.transaction_type) == "FreeReplacement" else 0,
									"custom_ecom_item_id": shipment_item_id,
								})
								if shipment_item_id:
									existing_item_ids.add(shipment_item_id)
								items_append.append(itemcode)
								for tax_type,rate, amount, acc_head in [
									("CGST", flt(child_row.cgst_rate),flt(child_row.cgst_tax), "Output Tax CGST - KGOPL"),
									("SGST",flt(child_row.sgst_rate)+flt(child_row.utgst_rate), flt(child_row.sgst_tax)+flt(child_row.utgst_tax), "Output Tax SGST - KGOPL"),
									("IGST", flt(child_row.igst_rate),flt(child_row.igst_tax) ,"Output Tax IGST - KGOPL")
									]:
										if amount:
											rate = normalize_tax_rate(rate)
											existing_tax = next((t for t in si.taxes if t.account_head == acc_head), None)
											if existing_tax:
												existing_tax.tax_amount += amount
												existing_tax.rate = rate
											else:
												si.append("taxes", {
													"charge_type": "On Net Total",
													"account_head": acc_head,
													"rate": rate,
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
							frappe.db.commit()
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
						frappe.db.commit()
						existing_si = draft_si.name

				si_return_error=[]
				if refund_items and not warehouse_mapping_missing:
					try:
						# if not existing_si:
						# 	si_return_error.append(invoice_no)
						# 	errors.append({
						# 		"idx": refund_items[0][0],
						# 		"invoice_id": invoice_no,
						# 		"message": f"Refund requested but original submitted invoice not found for {invoice_no}."
						# 	})

						credit_note_no = refund_items[0][1].get("credit_note_no")
						if not credit_note_no:
							si_return_error.append(invoice_no)
							errors.append({
								"idx": refund_items[0][0],
								"invoice_id": invoice_no,
								"message": "Missing Credit Note No for refund row(s)"
							})
							raise Exception("Missing Credit Note No")

						# Skip if this credit note return invoice already exists (idempotent re-runs)
						existing_return = frappe.db.get_value(
							"Sales Invoice",
							{"custom_ecommerce_invoice_id": credit_note_no, "is_return": 1, "docstatus": 1},
							"name",
						)
						if existing_return:
							# Return invoice already created for this credit note; treat as processed
							percent = int((count / total_invoices) * 100) if total_invoices else 100
							self._publish_progress(
								current=count,
								total=total_invoices,
								progress=percent,
								message=f"Processed {count}/{total_invoices} invoices",
								phase="amazon_mtr_b2b",
							)
							frappe.db.commit()
							continue

						# Ecommerce GSTIN is mandatory for returns too
						mapped_ecommerce_gstin = resolve_ecommerce_gstin_from_mapping(
							amazon, refund_items[0][1].seller_gstin
						)
						if not mapped_ecommerce_gstin:
							raise Exception(
								f"Ecommerce GSTIN mapping missing for Seller GSTIN: {refund_items[0][1].seller_gstin} "
								f"(Credit Note No: {credit_note_no}, Invoice No: {invoice_no}). "
								f"Please add it in Ecommerce Mapping '{amazon.name}' -> Ecommerce GSTIN Mapping."
							)

						draft_return = frappe.db.get_value(
							"Sales Invoice",
							{"custom_ecommerce_invoice_id": credit_note_no, "is_return": 1, "docstatus": 0},
							"name",
						)

						if draft_return:
							si_return = frappe.get_doc("Sales Invoice", draft_return)
							si_return.is_return = 1
						else:
							si_return = frappe.new_doc("Sales Invoice")
							si_return.is_return = 1
							si_return.custom_ecommerce_operator = self.ecommerce_mapping
							si_return.custom_ecommerce_type = self.amazon_type
							si_return.customer = customer
							si_return.set_posting_time = 1
							# Parse the datetime and add 1 minute for returns
							credit_note_dt = parse_export_datetime(refund_items[0][1].get("credit_note_date"))
							if not credit_note_dt:
								raise Exception(
									f"Invalid Credit Note Date: {refund_items[0][1].get('credit_note_date')}"
								)
							si_return.posting_date = credit_note_dt.date()
							si_return.posting_time = credit_note_dt.time()
							si_return.custom_ecommerce_invoice_id = credit_note_no
							# Avoid duplicate primary key errors if an invoice with this name already exists
							existing_by_name = frappe.db.exists("Sales Invoice", credit_note_no)
							if not existing_by_name:
								si_return.__newname = credit_note_no
							si_return.custom_inv_no = invoice_no
							si_return.taxes = []
							si_return.update_stock = 1

						# Always set ecommerce_gstin from mapping (required for GST reporting)
						si_return.ecommerce_gstin = mapped_ecommerce_gstin

						# De-duplicate within this return invoice only
						existing_return_item_ids = {
							d.get("custom_ecom_item_id")
							for d in (si_return.get("items") or [])
							if d.get("custom_ecom_item_id")
						}
						items_append=[]
						for idx, child_row in refund_items:
							try:
								shipment_item_id = child_row.shipment_item_id
								if shipment_item_id and shipment_item_id in existing_return_item_ids:
									continue

								itemcode = next((i.erp_item for i in amazon.ecom_item_table if i.ecom_item_id == child_row.get(amazon.ecom_sku_column_header)), None)
								if not itemcode:
									error_names.append(invoice_no)
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
										error_names.append(invoice_no)
										raise Exception(f"Warehouse Mapping not found for Warehouse Id: {warehouse_id}")
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
								si_return.ecommerce_gstin = mapped_ecommerce_gstin
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
									"custom_ecom_item_id": shipment_item_id,
								})
								if shipment_item_id:
									existing_return_item_ids.add(shipment_item_id)
								for tax_type,rate, amount, acc_head in [
									("CGST", flt(child_row.cgst_rate),flt(child_row.cgst_tax), "Output Tax CGST - KGOPL"),
									("SGST",flt(child_row.sgst_rate)+flt(child_row.utgst_rate), flt(child_row.sgst_tax)+flt(child_row.utgst_tax), "Output Tax SGST - KGOPL"),
									("IGST", flt(child_row.igst_rate),flt(child_row.igst_tax) ,"Output Tax IGST - KGOPL")
									]:
										if amount:
											rate = normalize_tax_rate(rate)
											existing_tax = next((t for t in si_return.taxes if t.account_head == acc_head), None)
											if existing_tax:
												existing_tax.tax_amount += amount
												existing_tax.rate = rate
											else:
												si_return.append("taxes", {
													"charge_type": "On Net Total",
													"account_head": acc_head,
													"rate": rate,
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
							frappe.db.commit()
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
			self._publish_progress(
				current=count,
				total=total_invoices,
				progress=percent,
				message=f"Processed {count}/{total_invoices} invoices",
				phase="amazon_mtr_b2b",
			)
			# Commit after each invoice to reduce memory load
			frappe.db.commit()

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
		self._publish_progress(
			current=total_invoices,
			total=total_invoices,
			progress=100,
			message="Amazon B2B Import Completed ✅",
			phase="amazon_mtr_b2b",
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
			# Cancelled rows often have an empty invoice number; skip them to avoid errors
			invoice_no = (child_row.invoice_number or "").strip()
			if not invoice_no:
				continue

			invoice_groups.setdefault(invoice_no, []).append((idx, child_row))

		expected_invoices = len(invoice_groups)
		total_invoices = expected_invoices or 1  # avoid div-by-zero for progress

		# 🔹 Initial progress update (Data Import-style dashboard progress)
		self._publish_progress(
			current=0,
			total=expected_invoices or 1,
			progress=0,
			message=f"Starting Amazon B2C import (0/{expected_invoices})" if expected_invoices else "Starting Amazon B2C import",
			phase="amazon_mtr_b2c",
		)

		# -------- Process Each Invoice Group --------
		for count, (invoice_no, items_data) in enumerate(invoice_groups.items(), start=1):
			try:
				shipment_items = [x for x in items_data if x[1].get("transaction_type") not in ["Refund", "Cancel"]]
				refund_items = [x for x in items_data if x[1].get("transaction_type") == "Refund"]

				existing_si_draft = frappe.db.get_value("Sales Invoice", {"custom_inv_no": invoice_no, "docstatus": 0}, "name")
				existing_si = frappe.db.get_value("Sales Invoice", {"custom_inv_no": invoice_no, "docstatus": 1}, "name")
				amazon = frappe.get_doc("Ecommerce Mapping", {"platform": "Amazon"})
				warehouse_mapping_missing = False
				# If the sales invoice is already submitted, don't recreate it. Refunds (credit notes)
				# are handled below independently.
				if shipment_items and existing_si:
					shipment_items = []

				# -------- Shipment Items --------
				if shipment_items:
					# Ecommerce GSTIN is mandatory. Resolve it once per invoice group from mapping table.
					mapped_ecommerce_gstin = resolve_ecommerce_gstin_from_mapping(
						amazon, shipment_items[0][1].seller_gstin
					)
					if not mapped_ecommerce_gstin:
						raise Exception(
							f"Ecommerce GSTIN mapping missing for Seller GSTIN: {shipment_items[0][1].seller_gstin} "
							f"(Invoice No: {invoice_no}). Please add it in Ecommerce Mapping '{amazon.name}' -> Ecommerce GSTIN Mapping."
						)

					if existing_si_draft:
						si = frappe.get_doc("Sales Invoice", existing_si_draft)
					else:
						si = frappe.new_doc("Sales Invoice")
						si.customer = val
						si.set_posting_time=1
						# Parse the datetime and add 2 seconds
						# invoice_datetime = datetime.strptime(str(items_data[0][1].get("invoice_date")), '%Y-%m-%d %H:%M:%S') if isinstance(items_data[0][1].get("invoice_date"), str) else items_data[0][1].get("invoice_date")
						# invoice_datetime_plus_2 = invoice_datetime + timedelta(seconds=2)
						invoice_dt = parse_export_datetime(items_data[0][1].get("invoice_date"))
						if not invoice_dt:
							raise Exception(f"Invalid Invoice Date: {items_data[0][1].get('invoice_date')}")
						si.posting_date = invoice_dt.date()
						si.posting_time = invoice_dt.time()
						si.custom_inv_no = invoice_no
						si.custom_ecommerce_invoice_id = invoice_no
						# Avoid duplicate primary key errors if an invoice with this name already exists
						existing_by_name = frappe.db.exists("Sales Invoice", invoice_no)
						if not existing_by_name:
							si.__newname = invoice_no
						si.custom_ecommerce_operator = self.ecommerce_mapping
						si.custom_ecommerce_type = self.amazon_type
						si.taxes_and_charges = ""
						si.update_stock = 1

					# Always set ecommerce_gstin from mapping (required for GST reporting)
					si.ecommerce_gstin = mapped_ecommerce_gstin

					# De-duplicate within this invoice (do NOT skip across other invoices)
					existing_item_ids = {
						d.get("custom_ecom_item_id")
						for d in (si.get("items") or [])
						if d.get("custom_ecom_item_id")
					}

					items_append = []
					for idx, child_row in shipment_items:
						try:
							shipment_item_id = child_row.shipment_item_id
							if shipment_item_id and shipment_item_id in existing_item_ids:
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
							si.ecommerce_gstin = mapped_ecommerce_gstin

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
								"is_free_item": 1 if str(child_row.transaction_type) == "FreeReplacement" else 0,
								"custom_ecom_item_id": shipment_item_id,
							})
							if shipment_item_id:
								existing_item_ids.add(shipment_item_id)
							items_append.append(itemcode)

							# ---- Taxes ----
							for tax_type, rate, amount, acc_head in [
								("CGST", flt(child_row.cgst_rate), flt(child_row.cgst_tax), "Output Tax CGST - KGOPL"),
								("SGST", flt(child_row.sgst_rate) + flt(child_row.utgst_rate), flt(child_row.sgst_tax) + flt(child_row.utgst_tax), "Output Tax SGST - KGOPL"),
								("IGST", flt(child_row.igst_rate), flt(child_row.igst_tax), "Output Tax IGST - KGOPL")
							]:
								if amount:
									rate = normalize_tax_rate(rate)
									existing_tax = next((t for t in si.taxes if t.account_head == acc_head), None)
									if existing_tax:
										existing_tax.tax_amount += amount
										existing_tax.rate = rate
									else:
										si.append("taxes", {
											"charge_type": "On Net Total",
											"account_head": acc_head,
											"rate": rate,
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
								frappe.db.commit()
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
							frappe.db.commit()
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
					credit_note_no = refund_items[0][1].get("credit_note_no")
					if not credit_note_no:
						errors.append({
							"idx": refund_items[0][0],
							"invoice_id": invoice_no,
							"message": "Missing Credit Note No for refund row(s)"
						})
						continue

					# Skip if this credit note return invoice already exists (idempotent re-runs)
					existing_return = frappe.db.get_value(
						"Sales Invoice",
						{"custom_ecommerce_invoice_id": credit_note_no, "is_return": 1, "docstatus": 1},
						"name",
					)
					if existing_return:
						# Return invoice already created for this credit note; treat as processed
						percent = int((count / total_invoices) * 100) if total_invoices else 100
						self._publish_progress(
							current=count,
							total=total_invoices,
							progress=percent,
							message=f"Processed {count}/{total_invoices} invoices",
							phase="amazon_mtr_b2c",
						)
						frappe.db.commit()
						continue

					# Ecommerce GSTIN is mandatory for returns too
					mapped_ecommerce_gstin = resolve_ecommerce_gstin_from_mapping(
						amazon, refund_items[0][1].seller_gstin
					)
					if not mapped_ecommerce_gstin:
						raise Exception(
							f"Ecommerce GSTIN mapping missing for Seller GSTIN: {refund_items[0][1].seller_gstin} "
							f"(Credit Note No: {credit_note_no}, Invoice No: {invoice_no}). "
							f"Please add it in Ecommerce Mapping '{amazon.name}' -> Ecommerce GSTIN Mapping."
						)

					draft_return = frappe.db.get_value(
						"Sales Invoice",
						{"custom_ecommerce_invoice_id": credit_note_no, "is_return": 1, "docstatus": 0},
						"name",
					)

					ritems_append = []
					si_error = []
					if draft_return:
						si_return = frappe.get_doc("Sales Invoice", draft_return)
						si_return.is_return = 1
					else:
						si_return = frappe.new_doc("Sales Invoice")
						si_return.is_return = 1
						si_return.customer = val
						si_return.set_posting_time = 1

						# Parse the datetime and add 1 minute for returns
						credit_note_dt = parse_export_datetime(refund_items[0][1].get("credit_note_date"))
						if not credit_note_dt:
							raise Exception(
								f"Invalid Credit Note Date: {refund_items[0][1].get('credit_note_date')}"
							)
						si_return.posting_date = credit_note_dt.date()
						si_return.posting_time = credit_note_dt.time()
						si_return.custom_ecommerce_operator = self.ecommerce_mapping
						si_return.custom_ecommerce_type = self.amazon_type
						si_return.custom_inv_no = invoice_no
						si_return.custom_ecommerce_invoice_id = credit_note_no
						# Avoid duplicate primary key errors if an invoice with this name already exists
						existing_by_name = frappe.db.exists("Sales Invoice", credit_note_no)
						if not existing_by_name:
							si_return.__newname = credit_note_no
						si_return.taxes = []
						si_return.update_stock = 1

					# Always set ecommerce_gstin from mapping (required for GST reporting)
					si_return.ecommerce_gstin = mapped_ecommerce_gstin

					# De-duplicate within this return invoice only
					existing_return_item_ids = {
						d.get("custom_ecom_item_id")
						for d in (si_return.get("items") or [])
						if d.get("custom_ecom_item_id")
					}
					for idx, child_row in refund_items:
						try:
							shipment_item_id = child_row.shipment_item_id
							if shipment_item_id and shipment_item_id in existing_return_item_ids:
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
							si_return.ecommerce_gstin = mapped_ecommerce_gstin

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
								"custom_ecom_item_id": shipment_item_id
							})
							if shipment_item_id:
								existing_return_item_ids.add(shipment_item_id)
							ritems_append.append(itemcode)

							for tax_type, rate, amount, acc_head in [
								("CGST", flt(child_row.cgst_rate), flt(child_row.cgst_tax), "Output Tax CGST - KGOPL"),
								("SGST", flt(child_row.sgst_rate) + flt(child_row.utgst_rate), flt(child_row.sgst_tax) + flt(child_row.utgst_tax), "Output Tax SGST - KGOPL"),
								("IGST", flt(child_row.igst_rate), flt(child_row.igst_tax), "Output Tax IGST - KGOPL")
							]:
								if amount:
									rate = normalize_tax_rate(rate)
									existing_tax = next((t for t in si_return.taxes if t.account_head == acc_head), None)
									if existing_tax:
										existing_tax.tax_amount += amount
										existing_tax.rate = rate
									else:
										si_return.append("taxes", {
											"charge_type": "On Net Total",
											"account_head": acc_head,
											"rate": rate,
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
								frappe.db.commit()
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
			percent = int((count / total_invoices) * 100) if total_invoices else 100
			self._publish_progress(
				current=count,
				total=total_invoices,
				progress=percent,
				message=f"Processed {count}/{total_invoices} invoices",
				phase="amazon_mtr_b2c",
			)
			# Commit after each invoice to reduce memory load
			frappe.db.commit()

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
		self._publish_progress(
			current=total_invoices,
			total=total_invoices,
			progress=100,
			message="Amazon B2C Import Completed ✅",
			phase="amazon_mtr_b2c",
		)

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
			# Cancelled rows can have an empty invoice number; skip them to avoid creating empty groups
			invoice_no = (row.invoice_number or "").strip()
			if not invoice_no:
				continue

			invoice_groups.setdefault(invoice_no, []).append((idx, row))

		expected_invoices = len(invoice_groups)
		total_invoices = expected_invoices or 1  # avoid div-by-zero for progress

		# 🔹 Initial progress update (Data Import-style dashboard progress)
		self._publish_progress(
			current=0,
			total=expected_invoices or 1,
			progress=0,
			message=f"Starting Amazon Stock Transfer import (0/{expected_invoices})" if expected_invoices else "Starting Amazon Stock Transfer import",
			phase="amazon_stock_transfer",
		)

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
					invoice_dt = parse_export_datetime(group_rows[0][1].get("invoice_date"))
					if not invoice_dt:
						raise Exception(f"Invalid Invoice Date: {group_rows[0][1].get('invoice_date')}")
					doc.posting_date = invoice_dt.date()
					doc.posting_time = invoice_dt.time()
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

						qty = flt(row.quantity)
						# In Amazon exports, taxable_value is typically the line total (not unit rate).
						# ERPNext expects `rate` to be per-unit.
						rate = (flt(row.taxable_value) / qty) if qty else 0

						doc.append("items", {
							"item_code": item_code,
							"qty": qty,
							"rate": rate,
							"warehouse": wh.erp_warehouse
						})

						if is_taxable:
							doc.custom_ecommerce_invoice_id = invoice_no
							for tax_type, rate, amount, acc_head in [
								("CGST", flt(row.cgst_rate), flt(row.cgst_amount), "Output Tax CGST - KGOPL"),
								("SGST", flt(row.sgst_rate) + flt(row.utgst_rate), flt(row.sgst_amount) + flt(row.utgst_amount), "Output Tax SGST - KGOPL"),
								("IGST", flt(row.igst_rate), flt(row.igst_amount), "Output Tax IGST - KGOPL")
							]:
								if amount:
									rate = normalize_tax_rate(rate)
									existing_tax = next((t for t in doc.taxes if t.account_head == acc_head), None)
									if existing_tax:
										existing_tax.tax_amount += amount
										existing_tax.rate = rate
									else:
										doc.append("taxes", {
											"charge_type": "On Net Total",
											"account_head": acc_head,
											"rate": rate,
											"tax_amount": amount,
											"description": tax_type
										})

					doc.save(ignore_permissions=True)
					for j in doc.items:
						j.item_tax_template = ""
						j.item_tax_rate = frappe._dict()

					doc.save(ignore_permissions=True)
					doc.submit()
					frappe.db.commit()
					success_count += len(group_rows)
					frappe.msgprint(f"{doc.doctype} {doc.name} created for Invoice No {invoice_no}")

				# -------- Inter-company: Purchase Invoice or Receipt --------
				if not existing_name_purchase:
					pi_doc = frappe.new_doc("Purchase Invoice" if is_taxable else "Purchase Receipt")
					pi_doc.supplier = ecommerce_mapping.inter_company_supplier
					pi_doc.set_posting_time=1
					# Parse the datetime and add 2 seconds
					# invoice_datetime = datetime.strptime(str(group_rows[0][1].get("invoice_date")), '%Y-%m-%d %H:%M:%S') if isinstance(group_rows[0][1].get("invoice_date"), str) else group_rows[0][1].get("invoice_date")
					# invoice_datetime_plus_2 = invoice_datetime + timedelta(seconds=2)
					invoice_dt = parse_export_datetime(group_rows[0][1].get("invoice_date"))
					if not invoice_dt:
						raise Exception(f"Invalid Invoice Date: {group_rows[0][1].get('invoice_date')}")
					pi_doc.posting_date = invoice_dt.date()
					pi_doc.posting_time = invoice_dt.time()
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

						pi_doc.location = location
						pi_doc.billing_address = com_address
					
						# if row.ship_to_state:
						# 	pi_doc.place_of_supply = state_code_dict.get(str(row.ship_to_state).lower())

						qty = flt(row.quantity)
						rate = (flt(row.taxable_value) / qty) if qty else 0

						pi_doc.append("items", {
							"item_code": item_code,
							"qty": qty,
							"rate": rate,
							"warehouse": warehouse,
						})

						if is_taxable:
							pi_doc.custom_ecommerce_invoice_id = invoice_no
							for tax_type, rate, amount, acc_head in [
								("CGST", flt(row.cgst_rate), flt(row.cgst_amount), "Input Tax CGST - KGOPL"),
								("SGST", flt(row.sgst_rate) + flt(row.utgst_rate), flt(row.sgst_amount) + flt(row.utgst_amount), "Input Tax SGST - KGOPL"),
								("IGST", flt(row.igst_rate), flt(row.igst_amount), "Input Tax IGST - KGOPL")
							]:
								if amount:
									rate = normalize_tax_rate(rate)
									existing_tax = next((t for t in pi_doc.taxes if t.account_head == acc_head), None)
									if existing_tax:
										existing_tax.tax_amount += amount
										existing_tax.rate = rate
									else:
										pi_doc.append("taxes", {
											"charge_type": "On Net Total",
											"account_head": acc_head,
											"rate": rate,
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
					frappe.db.commit()

			except Exception as e:
				for idx, row in group_rows:
					errors.append({
						"idx": idx,
						"invoice_id": invoice_no,
						"message": f"{str(e)}"
					})

			# 🔹 Realtime progress update after each invoice group
			percent = int((count / total_invoices) * 100)
			self._publish_progress(
				current=count,
				total=total_invoices,
				progress=percent,
				message=f"Processed {count}/{total_invoices} invoices",
				phase="amazon_stock_transfer",
			)

		# -------- Final status update --------
		self.error_json = json.dumps(errors) if errors else ""
		self.status = "Partial Success" if errors and success_count else "Error" if errors else "Success"
		self.save()

		# 🔹 Final realtime update
		self._publish_progress(
			current=total_invoices,
			total=total_invoices,
			progress=100,
			message="Amazon Stock Transfer Import Completed ✅",
			phase="amazon_stock_transfer",
		)

		return success_count


		
	@frappe.whitelist()
	def create_flipkart_sales_invoice(self):
		from frappe.utils import flt, getdate

		errors = []
		si_invoice = []
		return_invoice = []
		sale_existing_count = 0
		sale_submitted_count = 0
		return_existing_count = 0
		return_submitted_count = 0

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
			gstin = resolve_ecommerce_gstin_from_mapping(flipkart, seller_gstin)
			if not gstin:
				raise Exception(
					f"Ecommerce GSTIN mapping missing for Seller GSTIN: {seller_gstin}. "
					f"Please add it in Ecommerce Mapping '{flipkart.name}' -> Ecommerce GSTIN Mapping."
				)
			return gstin

		# ---------- SALES ----------
		sale_groups = {}
		for row in self.flipkart_items:
			if row.event_sub_type != "Sale":
				continue

			invoice_key = row.buyer_invoice_id
			if not invoice_key:
				errors.append({
					"idx": row.idx,
					"invoice_id": row.buyer_invoice_id,
					"event": row.event_sub_type,
					"message": "Missing Buyer Invoice ID (buyer_invoice_id) for Sale row"
				})
				continue

			sale_groups.setdefault(invoice_key, []).append(row)

		expected_sale_invoices = len(sale_groups)
		total_sale_invoices = expected_sale_invoices or 1
		sale_count = 0

		# 🔹 Initial progress update for sales
		self._publish_progress(
			current=0,
			total=total_sale_invoices,
			progress=0,
			message=f"Starting Flipkart import - Sales (0/{total_sale_invoices})",
			phase="flipkart_sales",
		)

		for invoice_key, rows in sale_groups.items():
			sale_count += 1
			group_errors = False
			items_appended = 0

			try:
				existing = frappe.db.get_value("Sales Invoice", {
					"custom_inv_no": invoice_key,
					"is_return": 0,
					"docstatus": 1
				}, "name")
				if existing:
					sale_existing_count += 1
					# 🔹 Progress update before continue (no commit - will commit at end)
					percent = int((sale_count / total_sale_invoices) * 50)
					self._publish_progress(
						current=sale_count,
						total=total_sale_invoices,
						progress=percent,
						message=f"Processed {sale_count}/{total_sale_invoices} sale invoices (skipped existing)",
						phase="flipkart_sales",
					)
					continue

				draft_name = frappe.db.get_value("Sales Invoice", {
					"custom_inv_no": invoice_key,
					"is_return": 0,
					"docstatus": 0
				}, "name")

				if draft_name:
					si = frappe.get_doc("Sales Invoice", draft_name)
					# Ecommerce GSTIN is mandatory (enforced for draft re-runs too)
					si.ecommerce_gstin = get_gstin(rows[0].seller_gstin)
				else:
					first = rows[0]
					warehouse, location, company_address = get_warehouse_info(first.warehouse_id)
					ecommerce_gstin = get_gstin(first.seller_gstin)

					si = frappe.new_doc("Sales Invoice")
					si.customer = customer
					si.set_posting_time = 1
					si.posting_date = parse_export_date(first.buyer_invoice_date) or getdate(first.buyer_invoice_date)
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
					# Don't set __newname if invoice with that name already exists
					existing_by_name = frappe.db.exists("Sales Invoice", first.buyer_invoice_id)
					if not existing_by_name:
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

						item_code = get_item_code(row.get(flipkart.ecom_sku_column_header))
						if not item_code:
							raise Exception(f"Item mapping not found for SKU: {row.get(flipkart.ecom_sku_column_header)}")

						warehouse, location, company_address = get_warehouse_info(row.warehouse_id)
						row_ecommerce_gstin = get_gstin(row.seller_gstin)

						# Fill missing headers (draft invoices)
						if not si.company_address:
							si.company_address = company_address
						if not si.location:
							si.location = location
						if not si.ecommerce_gstin:
							si.ecommerce_gstin = row_ecommerce_gstin
						elif si.ecommerce_gstin != row_ecommerce_gstin:
							raise Exception(
								f"Multiple GSTINs detected for Buyer Invoice ID {invoice_key}: "
								f"{si.ecommerce_gstin} vs {row_ecommerce_gstin}"
							)
						if not si.place_of_supply and row.customers_billing_state:
							state = row.customers_billing_state
							if not state_code_dict.get(str(state).lower()):
								raise Exception("State name Is Wrong Please Check")
							si.place_of_supply = state_code_dict.get(str(state).lower())
						if not si.custom_ecommerce_invoice_id and row.buyer_invoice_id:
							si.custom_ecommerce_invoice_id = row.buyer_invoice_id
							# Don't set __newname if invoice with that name already exists
							existing_by_name = frappe.db.exists("Sales Invoice", row.buyer_invoice_id)
							if not existing_by_name:
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

				if items_appended > 0 and not group_errors:
					si.save(ignore_permissions=True)
					for j in si.items:
						j.item_tax_template = ""
						j.item_tax_rate = frappe._dict()
					si.due_date = getdate(today())
					si.save(ignore_permissions=True)

				if not group_errors and si.docstatus == 0 and si.items:
					si_invoice.append(si.name)
				elif not group_errors and not si.items:
					errors.append({
						"idx": rows[0].idx if rows else "",
						"invoice_id": invoice_key,
						"event": "Sale",
						"message": "No items were added for this Buyer Invoice ID. Check Order Item ID parsing (scientific notation / precision loss) and duplicates."
					})

			except Exception as e:
				for row in rows:
					errors.append({
						"idx": row.idx,
						"invoice_id": row.buyer_invoice_id,
						"event": row.event_sub_type,
						"message": str(e)
					})

			# 🔹 Progress update after each sale invoice group (no commit - will commit at end)
			percent = int((sale_count / total_sale_invoices) * 50)  # Sales take first 50% of progress
			self._publish_progress(
				current=sale_count,
				total=total_sale_invoices,
				progress=percent,
				message=f"Processed {sale_count}/{total_sale_invoices} sale invoices",
				phase="flipkart_sales",
			)

		# Submit Sales Invoices (no commits during loop - will commit at end)
		for sii in si_invoice:
			try:
				frappe.get_doc("Sales Invoice", sii).submit()
				sale_submitted_count += 1
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

			invoice_key = row.buyer_invoice_id
			if not invoice_key:
				errors.append({
					"idx": row.idx,
					"invoice_id": row.buyer_invoice_id,
					"event": row.event_sub_type,
					"message": "Missing Buyer Invoice ID (buyer_invoice_id) for Return row"
				})
				continue

			return_groups.setdefault(invoice_key, []).append(row)

		expected_return_invoices = len(return_groups)
		total_return_invoices = expected_return_invoices or 1
		return_count = 0

		# 🔹 Progress update for returns (starts at 50%)
		self._publish_progress(
			current=0,
			total=total_return_invoices,
			progress=50,
			message=f"Starting Returns (0/{total_return_invoices})",
			phase="flipkart_returns",
		)

		for invoice_key, rows in return_groups.items():
			return_count += 1
			group_errors = False
			items_appended = 0

			try:
				existing_return = frappe.db.get_value("Sales Invoice", {
					"custom_inv_no": invoice_key,
					"is_return": 1,
					"docstatus": 1
				}, "name")
				if existing_return:
					return_existing_count += 1
					# 🔹 Progress update before continue (no commit - will commit at end)
					percent = 50 + int((return_count / total_return_invoices) * 50)
					self._publish_progress(
						current=return_count,
						total=total_return_invoices,
						progress=percent,
						message=f"Processed {return_count}/{total_return_invoices} return invoices (skipped existing)",
						phase="flipkart_returns",
					)
					continue

				draft_name = frappe.db.get_value("Sales Invoice", {
					"custom_inv_no": invoice_key,
					"is_return": 1,
					"docstatus": 0
				}, "name")

				if draft_name:
					si = frappe.get_doc("Sales Invoice", draft_name)
					si.is_return = 1
					# Ecommerce GSTIN is mandatory (enforced for draft re-runs too)
					si.ecommerce_gstin = get_gstin(rows[0].seller_gstin)
				else:
					first = rows[0]
					warehouse, location, company_address = get_warehouse_info(first.warehouse_id)
					ecommerce_gstin = get_gstin(first.seller_gstin)

					si = frappe.new_doc("Sales Invoice")
					si.customer = customer
					si.set_posting_time = 1
					si.posting_date = parse_export_date(first.buyer_invoice_date) or getdate(first.buyer_invoice_date)
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
					# Don't set __newname if invoice with that name already exists
					existing_by_name = frappe.db.exists("Sales Invoice", first.buyer_invoice_id)
					if not existing_by_name:
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

						item_code = get_item_code(row.get(flipkart.ecom_sku_column_header))
						if not item_code:
							raise Exception(f"Item mapping not found for SKU: {row.get(flipkart.ecom_sku_column_header)}")

						warehouse, location, company_address = get_warehouse_info(row.warehouse_id)
						row_ecommerce_gstin = get_gstin(row.seller_gstin)

						if not si.company_address:
							si.company_address = company_address
						if not si.location:
							si.location = location
						if not si.ecommerce_gstin:
							si.ecommerce_gstin = row_ecommerce_gstin
						elif si.ecommerce_gstin != row_ecommerce_gstin:
							raise Exception(
								f"Multiple GSTINs detected for Buyer Invoice ID {invoice_key}: "
								f"{si.ecommerce_gstin} vs {row_ecommerce_gstin}"
							)
						if not si.place_of_supply and row.customers_billing_state:
							state = row.customers_billing_state
							if not state_code_dict.get(str(state).lower()):
								raise Exception("State name Is Wrong Please Check")
							si.place_of_supply = state_code_dict.get(str(state).lower())
						if not si.custom_ecommerce_invoice_id and row.buyer_invoice_id:
							si.custom_ecommerce_invoice_id = row.buyer_invoice_id
							# Don't set __newname if invoice with that name already exists
							existing_by_name = frappe.db.exists("Sales Invoice", row.buyer_invoice_id)
							if not existing_by_name:
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

				if items_appended > 0 and not group_errors:
					si.save(ignore_permissions=True)
					for j in si.items:
						j.item_tax_template = ""
						j.item_tax_rate = frappe._dict()
					si.due_date = getdate(today())
					si.save(ignore_permissions=True)

				if not group_errors and si.docstatus == 0 and si.items:
					return_invoice.append(si.name)
				elif not group_errors and not si.items:
					errors.append({
						"idx": rows[0].idx if rows else "",
						"invoice_id": invoice_key,
						"event": "Return",
						"message": "No items were added for this Buyer Invoice ID (Return). Check Order Item ID parsing (scientific notation / precision loss) and duplicates."
					})

			except Exception as e:
				for row in rows:
					errors.append({
						"idx": row.idx,
						"invoice_id": row.buyer_invoice_id,
						"event": row.event_sub_type,
						"message": str(e)
					})

			# 🔹 Progress update after each return invoice group (no commit - will commit at end)
			percent = 50 + int((return_count / total_return_invoices) * 50)  # Returns take last 50% of progress
			self._publish_progress(
				current=return_count,
				total=total_return_invoices,
				progress=percent,
				message=f"Processed {return_count}/{total_return_invoices} return invoices",
				phase="flipkart_returns",
			)

		# Submit Return Invoices (no commits during loop - will commit at end)
		for sii in return_invoice:
			try:
				frappe.get_doc("Sales Invoice", sii).submit()
				return_submitted_count += 1
			except Exception as e:
				errors.append({
					"idx": "",
					"invoice_id": sii,
					"event": "Return",
					"message": f"Submit failed: {str(e)}"
				})

		# 🔹 Commit all changes at the end (like submit_after_import in data import)
		frappe.db.commit()

		self.error_json = str(json.dumps(errors))
		expected_total = expected_sale_invoices + expected_return_invoices
		completed_total = sale_existing_count + sale_submitted_count + return_existing_count + return_submitted_count

		if expected_total == 0:
			self.status = "Error"
		elif completed_total == expected_total and not errors:
			self.status = "Success"
		elif completed_total == 0:
			self.status = "Error"
		else:
			self.status = "Partial Success"

		self.save(ignore_permissions=True)

		# 🔹 Final progress update
		self._publish_progress(
			current=expected_total,
			total=expected_total,
			progress=100,
			message="Flipkart Import Completed ✅",
			phase="flipkart",
		)

		return {
			"status": "success" if self.status == "Success" else "partial",
			"errors": errors,
			"summary": {
				"expected_sale_invoices": expected_sale_invoices,
				"expected_return_invoices": expected_return_invoices,
				"sale_existing": sale_existing_count,
				"sale_submitted": sale_submitted_count,
				"return_existing": return_existing_count,
				"return_submitted": return_submitted_count,
			},
		}



		
	def create_cred_sales_invoice(self):
		"""Create Sales Invoices from the CRED CSV export.

		This implementation is based on the CRED CSV columns you shared:
		- Uses **EE Invoice No** (fallback Invoice_id/Suborder No) to group rows into one Sales Invoice
		- Sets invoice datetime from **Printed At** (fallback **Confirmed At**, then Invoice Date/Order Date)
		- Uses **Item Quantity** + **Item Price Excluding Tax** to build per-unit rate
		- Adds GST taxes using provided **tax** / **Tax Rate** values

		We parse the CSV inside the background job (RQ worker) to avoid bloating the parent
		document with hidden child tables.
		"""
		import os
		import re
		import pandas as pd
		from frappe.utils.file_manager import get_file_path
		from frappe.utils import get_datetime

		errors = []

		if not self.cred_attach:
			frappe.throw("Please attach the CRED CSV file.")

		file_url = self.cred_attach
		filename = file_url.split("/files/")[-1]
		file_path = get_file_path(filename)

		if not os.path.exists(file_path):
			frappe.throw(f"File not found at path: {file_path}")

		# --- Load mapping and customer ---
		cred_mapping = frappe.get_doc("Ecommerce Mapping", {"name": "Cred"})
		customer = frappe.db.get_value(
			"Ecommerce Mapping", {"platform": "Cred"}, "default_non_company_customer"
		)
		if not customer:
			frappe.throw("Default Non Company Customer is not set in Ecommerce Mapping for Cred.")

		# --- Read CSV as strings (prevents scientific notation / precision loss) ---
		try:
			df = pd.read_csv(
				file_path,
				dtype=str,
				keep_default_na=False,
				na_filter=False,
			)
		except Exception as e:
			frappe.throw(f"Error reading CRED CSV: {str(e)}")

		def normalize_col(col_name: str) -> str:
			col_name = (str(col_name) or "").strip().lower()
			col_name = re.sub(r"[^a-z0-9]+", "_", col_name).strip("_")
			return col_name

		col_map = {normalize_col(c): c for c in df.columns}

		def get_cell(row, key: str) -> str:
			col = col_map.get(key)
			if not col:
				return ""
			return clean_csv_cell(row.get(col))

		def parse_dt(value: str):
			value = clean_csv_cell(value)
			if not value:
				return None
			# CRED examples: "05/12/25 15:18"
			for fmt in (
				"%d/%m/%y %H:%M",
				"%d/%m/%Y %H:%M",
				"%d/%m/%y %H:%M:%S",
				"%d/%m/%Y %H:%M:%S",
				"%d/%m/%y",
				"%d/%m/%Y",
			):
				try:
					return datetime.strptime(value, fmt)
				except Exception:
					pass
			try:
				return get_datetime(value)
			except Exception:
				return None

		def parse_rate(rate_str):
			rate = flt(rate_str)
			# Sometimes exporters use 0.05 instead of 5
			if 0 < rate < 1:
				rate = rate * 100
			return rate

		def get_place_of_supply(state_name: str):
			key = normalize_state_key(state_name)
			return state_code_dict.get(key)

		def resolve_invoice_datetime(row):
			# Keep Printed At as invoice datetime; fallback Confirmed At (as requested)
			return (
				parse_dt(get_cell(row, "printed_at"))
				or parse_dt(get_cell(row, "confirmed_at"))
				or parse_dt(get_cell(row, "invoice_date"))
				or parse_dt(get_cell(row, "order_date"))
			)

		def get_invoice_no(row):
			return (
				# Prefer Invoice_id (stable internal id), fallback to EE Invoice No
				get_cell(row, "invoice_id")
				or get_cell(row, "ee_invoice_no")
				or get_cell(row, "suborder_no")
				or get_cell(row, "reference_code")
			)

		def is_cancelled_row(row):
			status = get_cell(row, "order_status").upper()
			ship_status = get_cell(row, "shipping_status").upper()
			cancelled_at = get_cell(row, "cancelled_at")
			return (
				status in {"CANCELLED", "CANCELED", "RTO"}
				or ship_status in {"CANCELLED", "CANCELED", "RTO"}
				or bool(cancelled_at)
			)

		# --- Build invoice groups ---
		invoice_groups = {}
		for row_idx, row in df.iterrows():
			if is_cancelled_row(row):
					continue

			invoice_no = get_invoice_no(row)
			if not invoice_no:
					continue

			invoice_groups.setdefault(invoice_no, []).append((row_idx + 1, row))

		expected_invoices = len(invoice_groups)
		total_invoices = expected_invoices or 1

		self._publish_progress(
			current=0,
			total=total_invoices,
			progress=0,
			message=f"Starting CRED import (0/{total_invoices})",
			phase="cred_shipments",
		)

		success_invoices = 0

		def get_item_code(ecom_sku: str):
			for row in cred_mapping.ecom_item_table:
				if row.ecom_item_id == ecom_sku:
					return row.erp_item
			return None

		def resolve_sku_for_mapping(row):
			# Allow Ecommerce Mapping to specify which CSV column is the SKU key
			configured = (cred_mapping.ecom_sku_column_header or "").strip()
			if configured:
				configured_key = normalize_col(configured)
				value = get_cell(row, configured_key)
				if value:
					return value

			# Fallbacks (common CRED CSV columns)
			return (
				get_cell(row, "accounting_sku")
				or get_cell(row, "sku")
				or get_cell(row, "marketplace_sku")
				or get_cell(row, "listing_ref_no")
			)

		for count, (invoice_no, rows) in enumerate(invoice_groups.items(), start=1):
			try:
				existing_submitted = frappe.db.get_value(
					"Sales Invoice",
					{"custom_inv_no": invoice_no, "is_return": 0, "docstatus": 1},
					"name",
				)
				if existing_submitted:
					percent = int((count / total_invoices) * 100)
					self._publish_progress(
						current=count,
						total=total_invoices,
						progress=percent,
						message=f"Processed {count}/{total_invoices} invoices (skipped existing)",
						phase="cred_shipments",
					)
					continue

				draft_name = frappe.db.get_value(
					"Sales Invoice",
					{"custom_inv_no": invoice_no, "is_return": 0, "docstatus": 0},
					"name",
				)

				first_idx, first_row = rows[0]
				seller_gstin = get_cell(first_row, "seller_gst_num") or get_cell(first_row, "seller_gst_num")
				if not seller_gstin:
					raise Exception("Missing Seller GST Num")

				ecommerce_gstin = resolve_ecommerce_gstin_from_mapping(cred_mapping, seller_gstin)
				if not ecommerce_gstin:
					raise Exception(
						f"Ecommerce GSTIN mapping missing for Seller GSTIN: {seller_gstin}. "
						f"Please add it in Ecommerce Mapping '{cred_mapping.name}' -> Ecommerce GSTIN Mapping."
					)

				invoice_dt = resolve_invoice_datetime(first_row)
				if not invoice_dt:
					raise Exception("Missing invoice datetime (Printed At / Confirmed At / Invoice Date / Order Date)")

				shipping_state = get_cell(first_row, "shipping_state") or get_cell(first_row, "billing_state")
				place_of_supply = get_place_of_supply(shipping_state)
				if not place_of_supply:
					raise Exception(f"State name Is Wrong Please Check: {shipping_state!r}")

				client_location = get_cell(first_row, "client_location")
				wh_map = next(
					(
						w
						for w in (cred_mapping.ecommerce_warehouse_mapping or [])
						if (w.ecom_warehouse_id or "").strip() == client_location
					),
					None,
				)
				warehouse = (wh_map.erp_warehouse if wh_map and wh_map.erp_warehouse else cred_mapping.default_company_warehouse)
				location = (wh_map.location if wh_map and wh_map.location else cred_mapping.default_company_location)
				company_address = (wh_map.erp_address if wh_map and wh_map.erp_address else cred_mapping.default_company_address)

				if not warehouse:
					raise Exception(f"Warehouse mapping missing for Client Location: {client_location!r}")

				if draft_name:
					si = frappe.get_doc("Sales Invoice", draft_name)
				else:
					si = frappe.new_doc("Sales Invoice")

				si.customer = customer
				si.set_posting_time = 1
				si.posting_date = invoice_dt.date()
				si.posting_time = invoice_dt.time()
				si.custom_inv_no = invoice_no
				si.custom_ecommerce_operator = self.ecommerce_mapping
				si.custom_ecommerce_type = self.amazon_type
				si.custom_ecommerce_invoice_id = invoice_no

				# Avoid duplicate primary key errors
				existing_by_name = frappe.db.exists("Sales Invoice", invoice_no)
				if not existing_by_name:
					si.__newname = invoice_no

				si.taxes_and_charges = ""
				si.update_stock = 1
				si.location = location
				si.set_warehouse = warehouse
				si.company_address = company_address
				si.ecommerce_gstin = ecommerce_gstin
				si.place_of_supply = place_of_supply

				# De-duplicate within this invoice using CRED's Suborder No / Reference Code
				existing_item_ids = {
					d.get("custom_ecom_item_id")
					for d in (si.get("items") or [])
					if d.get("custom_ecom_item_id")
				}

				# Accumulate taxes (Actual) so amounts match the CSV exactly.
				tax_totals = {"cgst": 0.0, "sgst": 0.0, "igst": 0.0}
				first_tax_rate = 0.0

				customer_state_code = (place_of_supply.split("-")[0] if place_of_supply else "")
				seller_state_code = (str(seller_gstin)[:2] if str(seller_gstin)[:2].isdigit() else "")

				for row_idx, row in rows:
					sku_value = resolve_sku_for_mapping(row)
					if not sku_value:
						raise Exception("Missing SKU (Accounting Sku / SKU / Marketplace Sku)")

					item_code = get_item_code(sku_value)
					if not item_code:
						raise Exception(f"Item mapping not found for SKU: {sku_value}")

					item_id = get_cell(row, "suborder_no") or get_cell(row, "reference_code") or f"{invoice_no}::{sku_value}"
					if item_id and item_id in existing_item_ids:
						continue

					qty = flt(get_cell(row, "item_quantity") or get_cell(row, "suborder_quantity") or 1)
					if qty <= 0:
						raise Exception(f"Invalid Item Quantity: {qty}")

					taxable_total = flt(get_cell(row, "item_price_excluding_tax"))
					if taxable_total <= 0:
						raise Exception("Missing/invalid Item Price Excluding Tax")

					rate = taxable_total / qty if qty else 0

					product_name = get_cell(row, "product_name")
					hsn_code = frappe.db.get_value("Item", item_code, "gst_hsn_code")

					si.append(
						"items",
						{
							"item_code": item_code,
							"qty": qty,
							"rate": rate,
							"description": product_name,
					"warehouse": warehouse,
							"gst_hsn_code": hsn_code,
							"income_account": cred_mapping.income_account,
							"custom_ecom_item_id": item_id,
						},
					)
					if item_id:
						existing_item_ids.add(item_id)

					row_tax_rate = parse_rate(get_cell(row, "tax_rate"))
					row_tax_amount = flt(get_cell(row, "tax"))
					if row_tax_amount <= 0 and row_tax_rate and taxable_total:
						row_tax_amount = taxable_total * (row_tax_rate / 100)

					if row_tax_rate and not first_tax_rate:
						first_tax_rate = row_tax_rate

					if row_tax_amount > 0:
						if seller_state_code and customer_state_code and seller_state_code == customer_state_code:
							tax_totals["cgst"] += row_tax_amount / 2
							tax_totals["sgst"] += row_tax_amount / 2
				else:
							tax_totals["igst"] += row_tax_amount

				# Reset and apply taxes (Actual) based on totals
				si.taxes = []
				if tax_totals["cgst"] > 0:
					si.append(
						"taxes",
						{
							"charge_type": "Actual",
							"account_head": "Output Tax CGST - KGOPL",
							"rate": (first_tax_rate / 2) if first_tax_rate else 0,
							"tax_amount": tax_totals["cgst"],
							"description": "CGST",
						},
					)
				if tax_totals["sgst"] > 0:
					si.append(
						"taxes",
						{
							"charge_type": "Actual",
							"account_head": "Output Tax SGST - KGOPL",
							"rate": (first_tax_rate / 2) if first_tax_rate else 0,
							"tax_amount": tax_totals["sgst"],
							"description": "SGST",
						},
					)
				if tax_totals["igst"] > 0:
					si.append(
						"taxes",
						{
							"charge_type": "Actual",
							"account_head": "Output Tax IGST - KGOPL",
							"rate": first_tax_rate or 0,
							"tax_amount": tax_totals["igst"],
							"description": "IGST",
						},
					)
			
				# Save + submit (deterministic per invoice group)
				si.save(ignore_permissions=True)
				for it in si.items:
					it.item_tax_template = ""
					it.item_tax_rate = frappe._dict()
				si.due_date = getdate(today())
				si.save(ignore_permissions=True)
				si.submit()
				frappe.db.commit()
				success_invoices += 1

			except Exception as e:
				frappe.db.rollback()
				errors.append(
					{
						"idx": first_idx if "first_idx" in locals() else None,
						"invoice_id": invoice_no,
						"event": "CRED Import",
						"message": str(e),
					}
				)

			percent = int((count / total_invoices) * 100)
			self._publish_progress(
				current=count,
				total=total_invoices,
				progress=percent,
				message=f"Processed {count}/{total_invoices} invoices",
				phase="cred_shipments",
			)

		# Final status + progress
		self.error_json = json.dumps(errors) if errors else ""
		if errors and success_invoices:
				self.status = "Partial Success"
		elif errors and not success_invoices:
				self.status = "Error"
		else:
			self.status = "Success"

			self.save(ignore_permissions=True)

		self._publish_progress(
			current=total_invoices,
			total=total_invoices,
			progress=100,
			message="CRED Import Completed ✅",
			phase="cred",
		)

		return {"status": self.status, "errors": errors, "success_invoices": success_invoices}


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
			"""Resolve optional Ecommerce Operator (TCS) GSTIN for JioMart.

			JioMart imports should not fail if GSTIN mapping is missing or invalid.
			If mapping isn't found or doesn't pass TCS GSTIN validation, return None and
			allow invoice creation to proceed without `ecommerce_gstin`.
			"""
			gstin = (str(seller_gstin).strip().upper() if seller_gstin is not None else "")
			if not gstin:
				return None

			operator_gstin = None
			for row in (getattr(jiomart, "ecommerce_gstin_mapping", None) or []):
				mapped_operator = (row.ecommerce_operator_gstin or "").strip().upper()
				mapped_company = (row.erp_company_gstin or "").strip().upper()
				if gstin == mapped_operator or gstin == mapped_company:
					operator_gstin = mapped_operator
					break

			if not operator_gstin:
				return None

			# Validate as TCS GSTIN; if invalid, ignore and proceed without ecommerce_gstin
			try:
				from india_compliance.gst_india.utils import validate_gstin as _validate_gstin

				return _validate_gstin(operator_gstin, label="E-commerce GSTIN", is_tcs_gstin=True)
			except Exception:
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

		total_sale_invoices = len(sale_groups) or 1
		sale_count = 0

		# 🔹 Initial progress update for sales
		self._publish_progress(
			current=0,
			total=total_sale_invoices,
			progress=0,
			message=f"Starting JioMart import - Sales (0/{total_sale_invoices})",
			phase="jiomart_sales",
		)

		for invoice_key, rows in sale_groups.items():
			sale_count += 1
			group_errors = False
			items_appended = 0

			try:
				existing = frappe.db.get_value("Sales Invoice", {
					"custom_inv_no": invoice_key,
					"is_return": 0,
					"docstatus": 1
				}, "name")
				if existing:
					# Sales invoice already submitted; treat as processed and keep progress moving
					percent = int((sale_count / total_sale_invoices) * 50) if total_sale_invoices else 50
					self._publish_progress(
						current=sale_count,
						total=total_sale_invoices,
						progress=percent,
						message=f"Processed {sale_count}/{total_sale_invoices} sale invoices (skipped existing)",
						phase="jiomart_sales",
					)
					frappe.db.commit()
					continue

				draft_name = frappe.db.get_value("Sales Invoice", {
					"custom_inv_no": invoice_key,
					"is_return": 0,
					"docstatus": 0
				}, "name")

				warehouse, location, company_address = get_warehouse_info()

				if draft_name:
					si = frappe.get_doc("Sales Invoice", draft_name)
					# Optional for JioMart: set if resolvable, else clear
					si.ecommerce_gstin = get_gstin(rows[0].seller_gstin) or ""
				else:
					first = rows[0]
					ecommerce_gstin = get_gstin(first.seller_gstin)

					si = frappe.new_doc("Sales Invoice")
					si.customer = customer
					si.set_posting_time = 1
					si.posting_date = parse_export_date(first.buyer_invoice_date) or getdate(first.buyer_invoice_date)
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
					# Avoid duplicate primary key errors if an invoice with this name already exists
					existing_by_name = frappe.db.exists("Sales Invoice", first.buyer_invoice_id)
					if not existing_by_name:
						si.__newname = first.buyer_invoice_id
					si.ecommerce_gstin = ecommerce_gstin or ""
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

						item_code = get_item_code(row.get(jiomart.ecom_sku_column_header))
						if not item_code:
							raise Exception(f"Item mapping not found for SKU: {row.get(jiomart.ecom_sku_column_header)}")

						item_name = frappe.db.get_value("Item", item_code, "item_name")
						hsn_code = frappe.db.get_value("Item", item_code, "gst_hsn_code")

						qty = flt(row.item_quantity)
						# JioMart export taxable_value is a line total; ERPNext expects per-unit rate
						rate = (flt(row.taxable_value) / qty) if qty else 0

						item_row = {
							"item_code": item_code,
							"item_name": item_name,
							"qty": qty,
							"rate": rate,
							"gst_hsn_code": hsn_code,
							"description": row.product_titledescription,
							"warehouse": warehouse,
							"margin_type": "Amount",
							"margin_rate_or_amount": flt(row.seller_coupon_amount),
							"income_account": jiomart.income_account,
							"custom_ecom_item_id": row.order_item_id
						}

						# Fill missing headers (draft invoices)
						row_ecommerce_gstin = get_gstin(row.seller_gstin)
						if not si.company_address:
							si.company_address = company_address
						if not si.location:
							si.location = location
						# Optional for JioMart:
						# - if we can resolve a single GSTIN, set it
						# - if rows disagree, clear it and proceed (don't block submission)
						if row_ecommerce_gstin:
							if not si.ecommerce_gstin:
								si.ecommerce_gstin = row_ecommerce_gstin
							elif si.ecommerce_gstin != row_ecommerce_gstin:
								si.ecommerce_gstin = ""
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

			# 🔹 Progress update after each sale invoice group
			percent = int((sale_count / total_sale_invoices) * 50)  # Sales take first 50% of progress
			self._publish_progress(
				current=sale_count,
				total=total_sale_invoices,
				progress=percent,
				message=f"Processed {sale_count}/{total_sale_invoices} sale invoices",
				phase="jiomart_sales",
			)
			frappe.db.commit()

		# Submit Sales Invoices
		for sii in si_invoice:
			try:
				frappe.get_doc("Sales Invoice", sii).submit()
				frappe.db.commit()
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

		total_return_invoices = len(return_groups) or 1
		return_count = 0

		# 🔹 Progress update for returns (starts at 50%)
		self._publish_progress(
			current=0,
			total=total_return_invoices,
			progress=50,
			message=f"Starting Returns (0/{total_return_invoices})",
			phase="jiomart_returns",
		)

		for invoice_key, rows in return_groups.items():
			return_count += 1
			group_errors = False
			items_appended = 0

			try:
				existing_return = frappe.db.get_value("Sales Invoice", {
					"custom_inv_no": invoice_key,
					"is_return": 1,
					"docstatus": 1
				}, "name")
				if existing_return:
					# Return invoice already submitted; treat as processed and keep progress moving
					percent = 50 + (int((return_count / total_return_invoices) * 50) if total_return_invoices else 50)
					self._publish_progress(
						current=return_count,
						total=total_return_invoices,
						progress=percent,
						message=f"Processed {return_count}/{total_return_invoices} return invoices (skipped existing)",
						phase="jiomart_returns",
					)
					frappe.db.commit()
					continue

				draft_name = frappe.db.get_value("Sales Invoice", {
					"custom_inv_no": invoice_key,
					"is_return": 1,
					"docstatus": 0
				}, "name")

				warehouse, location, company_address = get_warehouse_info()

				if draft_name:
					si = frappe.get_doc("Sales Invoice", draft_name)
					si.is_return = 1
					# Optional for JioMart: set if resolvable, else clear
					si.ecommerce_gstin = get_gstin(rows[0].seller_gstin) or ""
				else:
					first = rows[0]
					ecommerce_gstin = get_gstin(first.seller_gstin)

					si = frappe.new_doc("Sales Invoice")
					si.customer = customer
					si.set_posting_time = 1
					si.posting_date = parse_export_date(first.buyer_invoice_date) or getdate(first.buyer_invoice_date)
					si.custom_inv_no = invoice_key
					si.custom_ecommerce_operator = self.ecommerce_mapping
					si.custom_ecommerce_type = self.amazon_type
					si.taxes_and_charges = ""
					si.update_stock = 1
					si.company_address = company_address
					si.ecommerce_gstin = ecommerce_gstin or ""
					si.location = location
					si.is_return = 1
					si.custom_ecommerce_invoice_id = first.buyer_invoice_id
					# Avoid duplicate primary key errors if an invoice with this name already exists
					existing_by_name = frappe.db.exists("Sales Invoice", first.buyer_invoice_id)
					if not existing_by_name:
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

						item_code = get_item_code(row.get(jiomart.ecom_sku_column_header))
						if not item_code:
							raise Exception(f"Item mapping not found for SKU: {row.get(jiomart.ecom_sku_column_header)}")

						item_name = frappe.db.get_value("Item", item_code, "item_name")
						hsn_code = frappe.db.get_value("Item", item_code, "gst_hsn_code")

						qty_abs = abs(flt(row.item_quantity))
						# Return: rate must be per-unit, qty negative
						rate = (abs(flt(row.taxable_value)) / qty_abs) if qty_abs else 0

						item_row = {
							"item_code": item_code,
							"item_name": item_name,
							"qty": -qty_abs,
							"rate": rate,
							"gst_hsn_code": hsn_code,
							"description": row.product_titledescription,
							"warehouse": warehouse,
							"margin_type": "Amount",
							"margin_rate_or_amount": flt(row.seller_coupon_amount),
							"income_account": jiomart.income_account,
							"custom_ecom_item_id": row.order_item_id
						}

						row_ecommerce_gstin = get_gstin(row.seller_gstin)
						if not si.company_address:
							si.company_address = company_address
						if not si.location:
							si.location = location
						if row_ecommerce_gstin:
							if not si.ecommerce_gstin:
								si.ecommerce_gstin = row_ecommerce_gstin
							elif si.ecommerce_gstin != row_ecommerce_gstin:
								si.ecommerce_gstin = ""
						if not si.place_of_supply and row.customers_billing_state:
							state = row.customers_billing_state
							if not state_code_dict.get(str(state).lower()):
								raise Exception("State name Is Wrong Please Check")
							si.place_of_supply = state_code_dict.get(str(state).lower())
						if not si.custom_ecommerce_invoice_id and row.buyer_invoice_id:
							si.custom_ecommerce_invoice_id = row.buyer_invoice_id
							# Avoid duplicate primary key errors if an invoice with this name already exists
							existing_by_name = frappe.db.exists("Sales Invoice", row.buyer_invoice_id)
							if not existing_by_name:
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

			# 🔹 Progress update after each return invoice group
			percent = 50 + int((return_count / total_return_invoices) * 50)  # Returns take last 50% of progress
			self._publish_progress(
				current=return_count,
				total=total_return_invoices,
				progress=percent,
				message=f"Processed {return_count}/{total_return_invoices} return invoices",
				phase="jiomart_returns",
			)
			frappe.db.commit()

		# Submit Return Invoices
		for sii in return_invoice:
			try:
				frappe.get_doc("Sales Invoice", sii).submit()
				frappe.db.commit()
			except Exception as e:
				errors.append({
					"idx": "",
					"invoice_id": sii,
					"event": "Return",
					"message": f"Submit failed: {str(e)}"
				})

		# 🔹 Final progress update
		self._publish_progress(
			progress=100,
			message="JioMart Import Completed ✅",
			phase="jiomart",
		)

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
    """Deprecated helper (kept for backward compatibility)."""
    # Use EcommerceBillImport._publish_progress instead.
    return
