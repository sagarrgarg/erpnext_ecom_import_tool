# Copyright (c) 2025, Sagar Ratan Garg and contributors
# For license information, please see license.txt

import html

from india_compliance.gst_india.utils.gstin_info import get_gstin_info
import frappe
from frappe import _
from frappe.auth import today
from frappe.model.document import Document
from frappe.core.doctype.data_import.importer import Importer
import io
import json
from datetime import datetime, timedelta

from frappe.utils.data import get_time
from frappe.utils import flt, getdate
import os

from ecom_import_tool.ecom_import_tool.utils.amazon_si import (
	apply_pos_payment,
	normalize_tax_rate,
	_amazon_init_si_header,
	_amazon_append_si_line,
	_amazon_save_and_submit,
)


def resolve_file_path(file_url):
	if not file_url:
		frappe.throw("No file attached.")
	filename = file_url.split("/files/")[-1]
	if "/private/files/" in file_url:
		base = frappe.get_site_path("private", "files")
	else:
		base = frappe.get_site_path("public", "files")
	# Reject anything that would escape the files dir (path-traversal hardening).
	# `..` segments and absolute filenames are not legitimate Frappe attachments.
	if ".." in filename.replace("\\", "/").split("/") or filename.startswith(("/", "\\")):
		frappe.throw(f"Invalid file path: {file_url}")
	path = os.path.normpath(os.path.join(base, filename))
	abs_base = os.path.abspath(base)
	if not os.path.abspath(path).startswith(abs_base + os.sep):
		frappe.throw(f"File path resolves outside the files directory: {file_url}")
	if not os.path.exists(path):
		frappe.throw(f"File not found: {path}")
	return path

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
	- Strip leading backtick/apostrophe used by some exports (e.g. CRED) to force Excel text mode.
	"""
	if val is None:
		return ""

	s = str(val).strip()
	if not s:
		return ""

	if s.lower() in {"nan", "none", "null"}:
		return ""

	# Strip surrounding quotes (double or single)
	while (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
		s = s[1:-1].strip()

	# Strip leading backtick or apostrophe used by CRED/Excel to force text mode (e.g. `12345 or '12345)
	while s and s[0] in ("`", "'"):
		s = s[1:].strip()

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


def safe_refund_qty_rate(quantity, tax_exclusive_gross):
	"""Compute safe qty and rate for a refund/return Sales Invoice line.

	Amazon refund rows sometimes have blank or zero quantity (e.g. amount-only
	adjustments).  Dividing by zero would crash the import.

	When qty is zero/blank the caller should create a Debit Note
	(is_debit_note=1) which natively accepts qty=0 in ERPNext.  For mixed
	groups (some rows with qty, some without) the caller falls back to
	qty=-1 on the zero rows since is_return and is_debit_note are mutually
	exclusive on a single Sales Invoice.

	Args:
		quantity: raw quantity value from the MTR row (may be str, None, 0).
		tax_exclusive_gross: tax-exclusive line amount from the MTR row.

	Returns:
		tuple(qty, rate, is_zero_qty):
			qty   – negative quantity for the return line, or 0 for debit note rows.
			rate  – per-unit rate (always positive).
			is_zero_qty – True when original qty was zero/blank.
	"""
	import math

	abs_qty = abs(flt(quantity))
	abs_amount = abs(flt(tax_exclusive_gross))

	if abs_qty and not math.isnan(abs_qty):
		if math.isnan(abs_amount):
			abs_amount = 0
		return -abs_qty, abs_amount / abs_qty, False

	if math.isnan(abs_amount):
		abs_amount = 0

	return 0, abs_amount, True


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
    "pondicherry": "34-Puducherry",
    "andaman and nicobar islands": "35-Andaman and Nicobar Islands",
    "telangana": "36-Telangana",
    "andhra pradesh": "37-Andhra Pradesh",

    # ✅ New UT after J&K reorganisation
    "ladakh": "38-Ladakh",

    # ✅ Export / special cases
    "other countries": "96-Other Countries",
    "other territory": "97-Other Territory"
}


# Reverse lookup: GSTIN state code prefix ("27") → full POS label ("27-Maharashtra")
_gstin_code_to_pos = {v.split("-", 1)[0]: v for v in state_code_dict.values()}


def _fiscal_year_end(posting_date):
	"""Return the Fiscal Year *end* date for posting_date using ERPNext's
	Fiscal Year doctype, or None if it cannot be resolved.
	Honours the company's configured FY (does not assume April-March, doesn't
	rely on the FY's display name which can be customised to anything).
	"""
	if not posting_date:
		return None
	try:
		from erpnext.accounts.utils import get_fiscal_year
		_name, _start, end_date = get_fiscal_year(posting_date)
		return getdate(end_date)
	except Exception:
		return None


def fy_prefix_for(posting_date):
	"""Last 2 digits of the fiscal-year end year (from Fiscal Year doctype).

	  FY 2025-26 → '26', FY 2026-27 → '27'.
	Returns '' if posting_date is missing or no Fiscal Year covers it.
	"""
	end = _fiscal_year_end(posting_date)
	if not end:
		return ""
	return f"{end.year % 100:02d}"


def qualify_with_fy(name, posting_date):
	"""Prefix `name` with the FY-end-year prefix derived from posting_date.

	Idempotent: if `name` already starts with the same prefix and a dash, returns
	it unchanged so the helper is safe to call multiple times.
	Used so Amazon ecom invoice numbers like 'DEL5-2' don't collide across years.
	"""
	prefix = fy_prefix_for(posting_date)
	if not name or not prefix:
		return name
	name_str = str(name)
	if name_str.startswith(f"{prefix}-"):
		return name_str
	return f"{prefix}-{name_str}"


def find_existing_amazon_doc(doctype, name, posting_date, **filters):
	"""Find existing doc of `doctype` trying FY-qualified name first, falling
	back to the legacy unprefixed name *only when the candidate's posting_date
	is in the same fiscal year* (so re-imports of pre-prefix data still match,
	but cross-FY re-uses do not collide).

	Returns the actual stored name found, or None.
	"""
	qualified = qualify_with_fy(name, posting_date)

	found = frappe.db.get_value(doctype, {"name": qualified, **filters}, "name")
	if found:
		return found

	if qualified != name and name:
		candidate = frappe.db.get_value(
			doctype,
			{"name": name, **filters},
			["name", "posting_date"],
			as_dict=True,
		)
		if candidate:
			# Same FY check: legacy match only counts if its posting_date is in the
			# same Fiscal Year as the row we're importing.
			lookup_end = _fiscal_year_end(posting_date)
			candidate_end = _fiscal_year_end(candidate.posting_date)
			if lookup_end and candidate_end and lookup_end == candidate_end:
				return candidate.name
	return None


def find_existing_amazon_si(name, posting_date, **filters):
	"""Sales Invoice convenience wrapper around find_existing_amazon_doc."""
	return find_existing_amazon_doc("Sales Invoice", name, posting_date, **filters)


def resolve_flipkart_pos(state_value, seller_gstin, igst_amt=0, cgst_amt=0, sgst_amt=0):
	"""Resolve place_of_supply for Flipkart rows.

	Flipkart anonymizes buyer info ('-' / 'NA' / blank) on some returns.
	When buyer state is missing, derive POS from the tax pattern:
	  * IGST present, no CGST/SGST → interstate → '97-Other Territory'
	  * CGST/SGST present, no IGST → intra-state → seller GSTIN's state
	Otherwise raise so the row surfaces in the error log.
	"""
	from frappe.utils import flt

	key = normalize_state_key(state_value)
	if key and key not in {"-", "na", "n/a", "nan", "none", "null"}:
		pos = state_code_dict.get(key)
		if not pos:
			raise Exception(f"State name Is Wrong Please Check: {state_value}")
		return pos

	igst = abs(flt(igst_amt))
	cgst = abs(flt(cgst_amt))
	sgst = abs(flt(sgst_amt))

	if igst and not (cgst or sgst):
		return state_code_dict["other territory"]

	if (cgst or sgst) and not igst:
		gstin_code = (seller_gstin or "")[:2]
		pos = _gstin_code_to_pos.get(gstin_code)
		if not pos:
			raise Exception(
				f"Cannot derive place_of_supply: anonymized buyer state and "
				f"unknown seller GSTIN state code '{gstin_code}'"
			)
		return pos

	raise Exception(
		"Cannot determine place_of_supply: buyer state is anonymized "
		"and tax pattern is ambiguous (need either IGST or CGST/SGST)."
	)


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
		pass

	@frappe.whitelist()
	def get_file_preview(self):
		"""Return first 10 rows of attached file as HTML tables for preview."""
		import pandas as pd

		PREVIEW_ROWS = 10
		previews = []

		def df_to_html(df, title):
			if df.empty:
				return ""
			header = "".join(f"<th style='white-space:nowrap;font-size:11px;'>{c}</th>" for c in df.columns)
			rows = ""
			for _, row in df.iterrows():
				cells = "".join(f"<td style='font-size:11px;'>{clean_csv_cell(str(v))}</td>" for v in row.values)
				rows += f"<tr>{cells}</tr>"
			return (
				f"<h5>{title}</h5>"
				f'<div style="overflow-x:auto;max-height:400px;overflow-y:auto;">'
				f'<table class="table table-bordered table-sm" style="font-size:11px;">'
				f"<thead><tr>{header}</tr></thead><tbody>{rows}</tbody></table></div>"
			)

		if self.ecommerce_mapping == "Flipkart" and self.flipkart_attach:
			file_path = resolve_file_path(self.flipkart_attach)
			try:
				sales_df = pd.read_excel(file_path, sheet_name="Sales Report", dtype=str, keep_default_na=False, nrows=PREVIEW_ROWS)
				previews.append(df_to_html(sales_df, "Sales Report (first 10 rows)"))
			except Exception:
				pass
			try:
				cb_df = pd.read_excel(file_path, sheet_name="Cash Back Report", dtype=str, keep_default_na=False, nrows=PREVIEW_ROWS)
				previews.append(df_to_html(cb_df, "Cash Back Report (first 10 rows)"))
			except Exception:
				pass

		elif self.ecommerce_mapping == "Amazon":
			attach = self.mtr_b2b_attachment or self.mtr_b2c_attachment or self.stock_transfer_attachment
			if attach:
				file_path = resolve_file_path(attach)
				try:
					df = pd.read_csv(file_path, dtype=str, keep_default_na=False, na_filter=False, nrows=PREVIEW_ROWS)
					previews.append(df_to_html(df, f"{self.amazon_type or 'Amazon'} (first 10 rows)"))
				except Exception:
					pass

		elif self.ecommerce_mapping == "CRED" and self.cred_attach:
			file_path = resolve_file_path(self.cred_attach)
			try:
				df = pd.read_csv(file_path, dtype=str, keep_default_na=False, na_filter=False, nrows=PREVIEW_ROWS)
				previews.append(df_to_html(df, "CRED (first 10 rows)"))
			except Exception:
				pass

		elif self.ecommerce_mapping == "Jiomart" and self.jio_mart_attach:
			file_path = resolve_file_path(self.jio_mart_attach)
			try:
				df = pd.read_csv(file_path, dtype=str, keep_default_na=False, na_filter=False, nrows=PREVIEW_ROWS)
				previews.append(df_to_html(df, "Jio Mart (first 10 rows)"))
			except Exception:
				pass

		return "<br>".join(previews) if previews else ""

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
		import erpnext.stock.get_item_details as _item_details_module
		_original_insert_item_price = _item_details_module.insert_item_price
		_item_details_module.insert_item_price = lambda *args, **kwargs: None
		try:
			self._parse_attached_file()

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
		finally:
			_item_details_module.insert_item_price = _original_insert_item_price

	def _parse_attached_file(self):
		"""Parse attached file into in-memory child tables for processing.
		Runs inside the background job — data is never persisted to DB."""
		if self.ecommerce_mapping == "Amazon":
			if self.amazon_type == "MTR B2B":
				self.show_preview()
			elif self.amazon_type == "MTR B2C":
				self.append_mtr_b2c()
			elif self.amazon_type == "Stock Transfer":
				self.append_stock_transfer_attachment()
		elif self.ecommerce_mapping == "Flipkart":
			self.append_flipkart()
		elif self.ecommerce_mapping == "Jiomart":
			self.append_jio_mart()

	def _update_import_status(self):
		"""Persist only status and error fields to DB without saving child tables.
		`error_html` is an HTML fieldtype (no DB column) — rendered client-side
		from `error_json`, so we never persist it directly.
		"""
		frappe.db.set_value("Ecommerce Bill Import", self.name, {
			"status": self.status,
			"error_json": getattr(self, "error_json", ""),
			"import_summary": getattr(self, "import_summary", "") or "",
		})

	def _set_import_summary(self, *, created=0, existing=0, failed=0, label=""):
		"""Stash a small JSON breakdown so the JS import log can show
		'X created, Y already existed, Z failed' next to the status banner."""
		try:
			self.import_summary = json.dumps({
				"label": label or "",
				"created": int(created or 0),
				"existing": int(existing or 0),
				"failed": int(failed or 0),
			})
		except Exception:
			self.import_summary = ""

	def _persist_errors(self, errors):
		"""Persist all errors directly on the doc as JSON.

		The client-side `show_import_log` / error-table renderer reads from
		`error_json` and renders the rows. Keep everything on the doc so
		users don't have to hop to a separate Error Log.
		"""
		if not errors:
			self.error_json = ""
			self.error_html = ""
			return

		try:
			self.error_json = json.dumps(errors, default=str)
		except Exception:
			# Last-resort: stringify each entry so a single bad value can't
			# blank out the whole error list.
			safe = []
			for e in errors:
				safe.append({k: str(v) for k, v in (e or {}).items()})
			self.error_json = json.dumps(safe)
		self.error_html = ""

	def show_preview(self):
		import pandas as pd
		self.mtr_b2b=[]
		if self.mtr_b2b_attachment:
			# Read CSV as strings to preserve long IDs exactly (avoid scientific notation)
			def clean(val):
				return clean_csv_cell(val)

			csv_file_path = resolve_file_path(self.mtr_b2b_attachment)

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
		import pandas as pd
		self.mtr_b2c = []
		if self.mtr_b2c_attachment:
			from frappe.utils.data import getdate

			def clean(val):
				return clean_csv_cell(val)

			csv_file_path = resolve_file_path(self.mtr_b2c_attachment)

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
		import pandas as pd
		self.stock_transfer=[]
		if self.stock_transfer_attachment:
			def clean(val):
				return clean_csv_cell(val)

			
			csv_file_path = resolve_file_path(self.stock_transfer_attachment)

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
		"""Parse the attached CRED file and populate child tables for preview.

		CRED has evolved over time (older Excel exports vs newer CSV exports).
		This function handles both formats and populates the `cred` child table for preview.
		"""
		import re
		self.cred_items = []
		self.cred = []
		if not self.cred_attach:
			return

		import pandas as pd

		file_path = resolve_file_path(self.cred_attach)

		def clean(val):
			"""Normalize cell values to string."""
			return clean_csv_cell(val)

		def normalize_col(col_name: str) -> str:
			"""Normalize column name to snake_case for lookup."""
			col_name = (str(col_name) or "").strip().lower()
			col_name = re.sub(r"[^a-z0-9]+", "_", col_name).strip("_")
			return col_name

		ext = os.path.splitext(filename)[1].lower()
		if ext == ".csv":
			# Read CSV as strings (prevents scientific notation / precision loss)
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

			# Build column lookup map
			col_map = {normalize_col(c): c for c in df.columns}

			def get_cell(row, key: str) -> str:
				col = col_map.get(key)
				if not col:
					return ""
				return clean(row.get(col))

			# Get child table fields for validation
			sale_child_doctype = frappe.get_meta(self.doctype).get_field("cred").options
			sale_fields = {f.fieldname for f in frappe.get_meta(sale_child_doctype).fields}

			# Get ecommerce mapping for SKU column header
			ecom_sku_col = None
			if self.ecommerce_mapping:
				try:
					cred_mapping = frappe.get_doc("Ecommerce Mapping", self.ecommerce_mapping)
					configured = (cred_mapping.ecom_sku_column_header or "").strip()
					if configured:
						ecom_sku_col = normalize_col(configured)
				except Exception:
					pass

			def resolve_sku(row):
				"""Resolve SKU using mapping's ecom_sku_column_header with fallback to marketplace_sku."""
				if ecom_sku_col:
					value = get_cell(row, ecom_sku_col)
					if value:
						return value
				# Fallback to marketplace_sku
				return get_cell(row, "marketplace_sku")

			# Map new CSV columns to existing child table fields for preview
			# New CSV format → Cred Items child table
			for _, row in df.iterrows():
				child_row = self.append("cred", {})

				# Map columns (new CSV column → existing field)
				if "seller_gstin" in sale_fields:
					child_row.seller_gstin = get_cell(row, "seller_gst_num")
				if "order_date_time" in sale_fields:
					child_row.order_date_time = get_cell(row, "order_date") or get_cell(row, "printed_at")
				if "order_item_id" in sale_fields:
					child_row.order_item_id = get_cell(row, "ee_invoice_no") or get_cell(row, "suborder_no") or get_cell(row, "reference_code")
				if "order_status" in sale_fields:
					child_row.order_status = get_cell(row, "order_status")
				if "sku_id" in sale_fields:
					child_row.sku_id = resolve_sku(row)
				if "product_name" in sale_fields:
					child_row.product_name = get_cell(row, "product_name")
				if "brand" in sale_fields:
					child_row.brand = get_cell(row, "brand")
				if "tax_rate" in sale_fields:
					child_row.tax_rate = get_cell(row, "tax_rate")
				if "taxable_amount" in sale_fields:
					child_row.taxable_amount = get_cell(row, "item_price_excluding_tax")
				if "tax_amount" in sale_fields:
					child_row.tax_amount = get_cell(row, "tax")
				if "gmv" in sale_fields:
					child_row.gmv = get_cell(row, "order_invoice_amount")
				if "warehouse_location_code" in sale_fields:
					child_row.warehouse_location_code = get_cell(row, "client_location")
				if "destination_address_state" in sale_fields:
					child_row.destination_address_state = get_cell(row, "shipping_state")
				if "destination_pincode" in sale_fields:
					child_row.destination_pincode = get_cell(row, "shipping_zip_code")

			# ---------------- Refund sheet (XLSX) ----------------
			# Build suborder_no -> ee_invoice_no map from the CSV we just parsed.
			# Note: the `Cred` child doctype only stores a single composite `order_item_id`
			# (preferring EE Invoice No), so we build the lookup directly from the CSV df
			# where both columns coexist per row.
			self.cred_refund = []
			if not self.cred_refund_attach:
				return

			suborder_to_ee_inv = {}
			for _, row in df.iterrows():
				sub = clean_csv_cell(get_cell(row, "suborder_no"))
				if sub.startswith("`"):
					sub = sub[1:]
				ee_inv = clean_csv_cell(get_cell(row, "ee_invoice_no"))
				if sub and ee_inv:
					suborder_to_ee_inv[sub] = ee_inv

			refund_path = resolve_file_path(self.cred_refund_attach)
			try:
				rdf = pd.read_excel(refund_path, sheet_name="Refund", dtype=str, keep_default_na=False)
			except (ValueError, KeyError):
				frappe.throw(
					"CRED Refund XLSX is missing the 'Refund' sheet. "
					"Re-export from CRED Mail Report and try again."
				)

			refund_col_map = {normalize_col(c): c for c in rdf.columns}

			def get_refund_cell(row, key: str) -> str:
				col = refund_col_map.get(key)
				if not col:
					return ""
				return clean(row.get(col))

			for _, row in rdf.iterrows():
				sub_id = get_refund_cell(row, "cred_order_item_id")
				if sub_id.startswith("`"):
					sub_id = sub_id[1:]

				refund_dt_raw = get_refund_cell(row, "refund_date_time")
				refund_date = parse_export_date(refund_dt_raw) or (getdate(refund_dt_raw) if refund_dt_raw else None)

				gst_rate_raw = get_refund_cell(row, "gst_rate")
				gst_rate_val = normalize_tax_rate(flt(gst_rate_raw)) if gst_rate_raw else 0

				self.append("cred_refund", {
					"cred_order_item_id": sub_id,
					"refund_date": refund_date,
					"order_status": get_refund_cell(row, "order_status"),
					"gmv": flt(get_refund_cell(row, "gmv")),
					"gst_rate": gst_rate_val,
					"customer_state": get_refund_cell(row, "customer_state"),
					"warehouse_state": get_refund_cell(row, "warehouse_state"),
					"ee_invoice_no": suborder_to_ee_inv.get(sub_id, ""),
				})

			return

		# ---------------- Legacy Excel (kept for backward compatibility) ----------------

		def excel_clean(val):
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
					child_row.set(fieldname, excel_clean(row[column_name]))

		for _, row in df_sales.iterrows():
			child_row = self.append("cred", {})
			for column_name in df_sales.columns:
				fieldname = column_name.strip().lower().replace(" ", "_")
				if fieldname in sale_fields:
					child_row.set(fieldname, excel_clean(row[column_name]))

	def append_flipkart(self):
		import pandas as pd

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

		file_path = resolve_file_path(self.flipkart_attach)

		try:
			df = pd.read_excel(file_path, sheet_name="Sales Report", dtype=str)
		except Exception as e:
			frappe.throw(f"Failed to read Flipkart XLSX: {str(e)}")

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

		self.set("flipkart_cashback", [])
		try:
			cb_df = pd.read_excel(file_path, sheet_name="Cash Back Report", dtype=str)
		except (ValueError, KeyError):
			frappe.throw(
				"Flipkart XLSX is missing the 'Cash Back Report' sheet. "
				"Do not rename or remove this sheet — re-export from Flipkart and try again."
			)

		if not cb_df.empty:
			cb_fields = [d.fieldname for d in frappe.get_meta("Flipkart Transaction Items").fields]
			for _, row in cb_df.iterrows():
				child = self.append("flipkart_cashback", {})
				for column in cb_df.columns:
					fieldname = column.strip().lower().replace(" ", "_").replace("(", "").replace(")", "").replace("/", "_").replace("?", "").replace("'", "")
					if fieldname in cb_fields:
						child.set(fieldname, clean(row[column]))
				child.set("credit_note_id_debit_note_id", clean(row.get("Credit Note ID/ Debit Note ID", "")))
				child.set("sgst_rate_or_utgst_as_applicable", clean(row.get("SGST Rate (or UTGST as applicable)", "")))
				child.set("sgst_amount_or_utgst_as_applicable", clean(row.get("SGST Amount (Or UTGST as applicable)", "")))
				child.set("customers_delivery_state", clean(row.get("Customer's Delivery State", "")))
				child.set("is_shopsy_order", clean(row.get("Is Shopsy Order?", "")))

	

					
	def append_jio_mart(self):
		import pandas as pd
		self.jio_mart_items = []
		if self.jio_mart_attach:
			from frappe.utils.data import getdate

			def clean(val):
				return clean_csv_cell(val)

			csv_file_path = resolve_file_path(self.jio_mart_attach)

			try:
				df = pd.read_csv(
					csv_file_path,
					dtype=str,
					keep_default_na=False,
					na_filter=False,
				)
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
		existing_shipment_count = 0
		existing_refund_count = 0
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

				# Amazon reuses invoice numbers across fiscal years; FY-qualify the
				# name so re-imports of next-FY data with the same invoice_no don't
				# silently match a prior-year SI.
				_inv_dt = parse_export_datetime((shipment_items or refund_items or items_data)[0][1].get("invoice_date"))
				_inv_posting_date = _inv_dt.date() if _inv_dt else None
				qualified_invoice_no = qualify_with_fy(invoice_no, _inv_posting_date)

				existing_si_draft = find_existing_amazon_si(invoice_no, _inv_posting_date, docstatus=0, is_return=0)
				existing_si = find_existing_amazon_si(invoice_no, _inv_posting_date, docstatus=1, is_return=0)

				amazon = frappe.get_doc("Ecommerce Mapping", {"platform": "Amazon"})
				error_log=[]
				warehouse_mapping_missing = False
				# If the sales invoice is already submitted, don't recreate it. Refunds (credit notes)
				# are handled below independently.
				if shipment_items and existing_si:
					existing_shipment_count += len(shipment_items)
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

						invoice_dt = parse_export_datetime(items_data[0][1].get("invoice_date"))
						if not invoice_dt:
							raise Exception(f"Invalid Invoice Date: {items_data[0][1].get('invoice_date')}")

						draft_doc = frappe.get_doc("Sales Invoice", existing_si_draft) if existing_si_draft else None
						si = _amazon_init_si_header(
							customer=customer,
							posting_dt=invoice_dt,
							ecom_name=qualified_invoice_no,
							is_return=False,
							is_debit_note=False,
							return_against=None,
							ecommerce_operator=self.ecommerce_mapping,
							amazon_type=self.amazon_type,
							ecommerce_gstin=mapped_ecommerce_gstin,
							update_stock=1,
							draft_doc=draft_doc,
						)

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
								if status!="Active":
									if child_row.ship_to_state:
										state=child_row.ship_to_state
										if not state_code_dict.get(str(state.lower())):
											error_names.append(invoice_no)
											raise Exception(f"State name Is Wrong Please Check")
										si.place_of_supply=state_code_dict.get(str(state.lower()))

								qty = flt(child_row.quantity)
								rate = (flt(child_row.tax_exclusive_gross) / qty) if qty else 0
								hsn_code = frappe.db.get_value("Item", itemcode, "gst_hsn_code")

								_amazon_append_si_line(
									si,
									item_code=itemcode,
									qty=qty,
									rate=rate,
									hsn_code=hsn_code,
									description=child_row.item_description,
									warehouse=warehouse,
									income_account=amazon.income_account,
									custom_ecom_item_id=child_row.shipment_item_id,
									is_free_item=(str(child_row.transaction_type) == "FreeReplacement"),
									margin_amount=flt(child_row.item_promo_discount),
									tax_rate_scalar=flt(child_row.total_tax_amount),
									taxes=[
										("CGST", flt(child_row.cgst_rate), flt(child_row.cgst_tax), "Output Tax CGST - KGOPL"),
										("SGST",
										 flt(child_row.sgst_rate) + flt(child_row.utgst_rate),
										 flt(child_row.sgst_tax) + flt(child_row.utgst_tax),
										 "Output Tax SGST - KGOPL"),
										("IGST", flt(child_row.igst_rate), flt(child_row.igst_tax), "Output Tax IGST - KGOPL"),
									],
								)
								if child_row.shipment_item_id:
									existing_item_ids.add(child_row.shipment_item_id)
								items_append.append(itemcode)
							except Exception as item_error:
								error_log.append(invoice_no)
								errors.append({
									"idx": idx,
									"invoice_id": invoice_no,
									"message": f"Shipment item error: {str(item_error)}"
								})
						if items_append and not warehouse_mapping_missing and invoice_no not in error_log:
							_amazon_save_and_submit(si, mode_of_payment=amazon.mode_of_payment, due_date=getdate(today()))
							existing_si = si.name
							success_count += len(shipment_items)
							frappe.db.commit()

					except Exception as ship_err:
						for idx, _ in shipment_items:
							errors.append({
								"idx": idx,
								"invoice_id": invoice_no,
								"message": f"Shipment processing error: {str(ship_err)}"
							})

				if refund_items and existing_si_draft and not existing_si and not warehouse_mapping_missing:
					draft_si = frappe.get_doc("Sales Invoice", existing_si_draft)
					if draft_si.name not in error_log:
						draft_si.submit()
						frappe.db.commit()
						existing_si = draft_si.name

				si_return_error=[]
				if refund_items and not warehouse_mapping_missing:
					# Sub-group refund items by credit_note_no — Amazon B2B can have
					# multiple distinct credit notes against the same invoice and
					# they each become their own return doc.
					cn_groups = {}
					for x in refund_items:
						cn = (x[1].get("credit_note_no") or "").strip()
						if cn:
							cn_groups.setdefault(cn, []).append(x)
						else:
							si_return_error.append(invoice_no)
							errors.append({
								"idx": x[0],
								"invoice_id": invoice_no,
								"message": "Missing Credit Note No for refund row",
							})

					for credit_note_no, cn_refund_items in cn_groups.items():
						try:
							# Pre-scan: determine if all rows in this credit note group have zero qty
							all_zero_qty = all(
								safe_refund_qty_rate(r[1].quantity, r[1].tax_exclusive_gross)[2]
								for r in cn_refund_items
							)
							use_debit_note = all_zero_qty

							# FY-qualify the credit note name (Amazon reuses CN numbers
							# across years).
							_cn_dt = parse_export_datetime(cn_refund_items[0][1].get("credit_note_date"))
							_cn_posting_date = _cn_dt.date() if _cn_dt else None
							qualified_cn_no = qualify_with_fy(credit_note_no, _cn_posting_date)

							# Skip if this credit note already exists (idempotent re-runs)
							existing_return = find_existing_amazon_si(credit_note_no, _cn_posting_date, docstatus=1)
							if existing_return:
								existing_refund_count += len(cn_refund_items)
								continue

							# Ecommerce GSTIN is mandatory for returns too
							mapped_ecommerce_gstin = resolve_ecommerce_gstin_from_mapping(
								amazon, cn_refund_items[0][1].seller_gstin
							)
							if not mapped_ecommerce_gstin:
								raise Exception(
									f"Ecommerce GSTIN mapping missing for Seller GSTIN: {cn_refund_items[0][1].seller_gstin} "
									f"(Credit Note No: {credit_note_no}, Invoice No: {invoice_no}). "
									f"Please add it in Ecommerce Mapping '{amazon.name}' -> Ecommerce GSTIN Mapping."
								)

							draft_return = find_existing_amazon_si(credit_note_no, _cn_posting_date, docstatus=0)

							credit_note_dt = parse_export_datetime(cn_refund_items[0][1].get("credit_note_date"))
							if not credit_note_dt:
								raise Exception(
									f"Invalid Credit Note Date: {cn_refund_items[0][1].get('credit_note_date')}"
								)

							draft_doc = frappe.get_doc("Sales Invoice", draft_return) if draft_return else None
							si_return = _amazon_init_si_header(
								customer=customer,
								posting_dt=credit_note_dt,
								ecom_name=qualified_cn_no,
								is_return=not use_debit_note,
								is_debit_note=use_debit_note,
								return_against=existing_si if not use_debit_note else None,
								ecommerce_operator=self.ecommerce_mapping,
								amazon_type=self.amazon_type,
								ecommerce_gstin=mapped_ecommerce_gstin,
								update_stock=0 if use_debit_note else 1,
								draft_doc=draft_doc,
							)

							si_return.ecommerce_gstin = mapped_ecommerce_gstin

							# De-duplicate within this return invoice only
							existing_return_item_ids = {
								d.get("custom_ecom_item_id")
								for d in (si_return.get("items") or [])
								if d.get("custom_ecom_item_id")
							}
							items_append=[]
							for idx, child_row in cn_refund_items:
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

									refund_qty, refund_rate, is_zero_qty = safe_refund_qty_rate(
										child_row.quantity, child_row.tax_exclusive_gross
									)
									if use_debit_note:
										line_qty, line_rate = 0, refund_rate
									elif is_zero_qty:
										line_qty, line_rate = -1, refund_rate
									else:
										line_qty, line_rate = refund_qty, refund_rate

									hsn_code = frappe.db.get_value("Item", itemcode, "gst_hsn_code")

									_amazon_append_si_line(
										si_return,
										item_code=itemcode,
										qty=line_qty,
										rate=line_rate,
										hsn_code=hsn_code,
										description=child_row.item_description,
										warehouse=warehouse,
										income_account=amazon.income_account,
										custom_ecom_item_id=shipment_item_id,
										margin_amount=flt(child_row.item_promo_discount),
										tax_rate_scalar=flt(child_row.total_tax_amount),
										taxes=[
											("CGST", flt(child_row.cgst_rate), flt(child_row.cgst_tax), "Output Tax CGST - KGOPL"),
											("SGST",
											 flt(child_row.sgst_rate) + flt(child_row.utgst_rate),
											 flt(child_row.sgst_tax) + flt(child_row.utgst_tax),
											 "Output Tax SGST - KGOPL"),
											("IGST", flt(child_row.igst_rate), flt(child_row.igst_tax), "Output Tax IGST - KGOPL"),
										],
									)
									if shipment_item_id:
										existing_return_item_ids.add(shipment_item_id)
									items_append.append(itemcode)
								except Exception as item_error:
									si_return_error.append(invoice_no)
									errors.append({
										"idx": idx,
										"invoice_id": invoice_no,
										"message": f"Refund item error: {str(item_error)}"
									})

							if items_append and not warehouse_mapping_missing and invoice_no not in si_return_error:
								_amazon_save_and_submit(
									si_return,
									mode_of_payment=amazon.mode_of_payment,
									due_date=getdate(today()),
								)
								frappe.db.commit()
								success_count += len(cn_refund_items)
						except Exception as refund_err:
							for idx, _ in cn_refund_items:
								errors.append({
									"idx": idx,
									"invoice_id": invoice_no,
									"message": f"Refund item error: {refund_err}"
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
		existing_total = existing_shipment_count + existing_refund_count
		summary_extra = f" ({existing_total} already existed, skipped)" if existing_total else ""
		if errors:
			self.status = "Partial Success" if success_count else "Error"
			indicator = "orange" if success_count else "red"
			frappe.msgprint(
				f"{success_count} items processed{summary_extra}, {len(errors)} failed. "
				"See error table below for details.",
				indicator=indicator,
				alert=True,
			)
		else:
			self.error_html = ""
			if success_count == 0 and existing_total == 0:
				self.status = "Error"
				rows_in_groups = sum(len(v) for v in invoice_groups.values())
				diagnosis = (
					f"No invoices were created. {len(invoice_groups)} invoice group(s) "
					f"covering {rows_in_groups} row(s) parsed from the file, but none "
					f"produced a sales/refund invoice. Likely causes: every row had a "
					f"blank Invoice Number, or every row's Transaction Type was 'Cancel'. "
					f"Check the input CSV."
				)
				errors.append({
					"idx": "",
					"invoice_id": "(no rows processed)",
					"event": "Diagnosis",
					"message": diagnosis,
				})
				frappe.msgprint(diagnosis, indicator="red")
			elif success_count == 0 and existing_total > 0:
				self.status = "Success"
				frappe.msgprint(
					f"Nothing new created. All {existing_total} items already exist as submitted invoices.",
					indicator="blue",
				)
			else:
				self.status = "Success"
				frappe.msgprint(
					f"All {success_count} items processed successfully!{summary_extra}",
					indicator="green",
				)

		self._set_import_summary(
			created=success_count,
			existing=existing_total,
			failed=len(errors),
			label="Amazon B2B",
		)
		self._persist_errors(errors)
		self._update_import_status()

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
		existing_shipment_count = 0
		existing_refund_count = 0
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

				# Amazon reuses invoice numbers (e.g. 'DEL5-2') across fiscal years.
				# Qualify the name with FY end-year prefix so 'DEL5-2' from FY 25-26
				# and FY 26-27 don't collide on Sales Invoice.name.
				_inv_dt = parse_export_datetime((shipment_items or refund_items or items_data)[0][1].get("invoice_date"))
				_inv_posting_date = _inv_dt.date() if _inv_dt else None
				qualified_invoice_no = qualify_with_fy(invoice_no, _inv_posting_date)

				existing_si_draft = find_existing_amazon_si(invoice_no, _inv_posting_date, docstatus=0, is_return=0)
				existing_si = find_existing_amazon_si(invoice_no, _inv_posting_date, docstatus=1, is_return=0)
				amazon = frappe.get_doc("Ecommerce Mapping", {"platform": "Amazon"})
				warehouse_mapping_missing = False
				# If the sales invoice is already submitted, don't recreate it. Refunds (credit notes)
				# are handled below independently.
				if shipment_items and existing_si:
					existing_shipment_count += len(shipment_items)
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

					invoice_dt = parse_export_datetime(items_data[0][1].get("invoice_date"))
					if not invoice_dt:
						raise Exception(f"Invalid Invoice Date: {items_data[0][1].get('invoice_date')}")

					draft_doc = frappe.get_doc("Sales Invoice", existing_si_draft) if existing_si_draft else None
					si = _amazon_init_si_header(
						customer=val,
						posting_dt=invoice_dt,
						ecom_name=qualified_invoice_no,
						is_return=False,
						is_debit_note=False,
						return_against=None,
						ecommerce_operator=self.ecommerce_mapping,
						amazon_type=self.amazon_type,
						ecommerce_gstin=mapped_ecommerce_gstin,
						update_stock=1,
						draft_doc=draft_doc,
					)

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
							_b2c_qty = flt(child_row.quantity)
							_b2c_rate = (flt(child_row.tax_exclusive_gross) / _b2c_qty) if _b2c_qty else 0

							_amazon_append_si_line(
								si,
								item_code=itemcode,
								qty=_b2c_qty,
								rate=_b2c_rate,
								hsn_code=hsn_code,
								description=child_row.item_description,
								warehouse=warehouse,
								income_account=amazon.income_account,
								custom_ecom_item_id=shipment_item_id,
								is_free_item=(str(child_row.transaction_type) == "FreeReplacement"),
								margin_amount=flt(child_row.item_promo_discount),
								tax_rate_scalar=flt(child_row.total_tax_amount),
								taxes=[
									("CGST", flt(child_row.cgst_rate), flt(child_row.cgst_tax), "Output Tax CGST - KGOPL"),
									("SGST",
									 flt(child_row.sgst_rate) + flt(child_row.utgst_rate),
									 flt(child_row.sgst_tax) + flt(child_row.utgst_tax),
									 "Output Tax SGST - KGOPL"),
									("IGST", flt(child_row.igst_rate), flt(child_row.igst_tax), "Output Tax IGST - KGOPL"),
								],
							)
							if shipment_item_id:
								existing_item_ids.add(shipment_item_id)
							items_append.append(itemcode)
						except Exception as item_error:
							error_names.append(invoice_no)
							errors.append({
								"idx": idx,
								"invoice_id": invoice_no,
								"message": f"Shipment item error: {item_error}"
							})

					try:
						if items_append and not warehouse_mapping_missing and invoice_no not in error_names:
							_amazon_save_and_submit(si, mode_of_payment=amazon.mode_of_payment, due_date=getdate(today()))
							existing_si = si.name
							success_count += len(shipment_items)
							frappe.db.commit()
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
					# Sub-group refund items by credit_note_no so each unique credit note creates its own return
					cn_groups = {}
					for x in refund_items:
						cn = (x[1].get("credit_note_no") or "").strip()
						if cn:
							cn_groups.setdefault(cn, []).append(x)
						else:
							errors.append({
								"idx": x[0],
								"invoice_id": invoice_no,
								"message": "Missing Credit Note No for refund row"
							})

					for credit_note_no, cn_refund_items in cn_groups.items():
						# Pre-scan: determine if all rows in this credit note group have zero qty
						all_zero_qty = all(
							safe_refund_qty_rate(r[1].quantity, r[1].tax_exclusive_gross)[2]
							for r in cn_refund_items
						)
						use_debit_note = all_zero_qty

						# Qualify credit note name with FY end-year prefix to avoid
						# cross-FY collisions (Amazon reuses CN numbers each year).
						_cn_dt = parse_export_datetime(cn_refund_items[0][1].get("credit_note_date"))
						_cn_posting_date = _cn_dt.date() if _cn_dt else None
						qualified_cn_no = qualify_with_fy(credit_note_no, _cn_posting_date)

						# Skip if this credit note already exists (idempotent re-runs)
						existing_return = find_existing_amazon_si(credit_note_no, _cn_posting_date, docstatus=1)
						if existing_return:
							existing_refund_count += len(cn_refund_items)
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
							amazon, cn_refund_items[0][1].seller_gstin
						)
						if not mapped_ecommerce_gstin:
							raise Exception(
								f"Ecommerce GSTIN mapping missing for Seller GSTIN: {cn_refund_items[0][1].seller_gstin} "
								f"(Credit Note No: {credit_note_no}, Invoice No: {invoice_no}). "
								f"Please add it in Ecommerce Mapping '{amazon.name}' -> Ecommerce GSTIN Mapping."
							)

						draft_return = find_existing_amazon_si(credit_note_no, _cn_posting_date, docstatus=0)

						ritems_append = []
						si_error = []

						credit_note_dt = parse_export_datetime(cn_refund_items[0][1].get("credit_note_date"))
						if not credit_note_dt:
							raise Exception(
								f"Invalid Credit Note Date: {cn_refund_items[0][1].get('credit_note_date')}"
							)

						draft_doc = frappe.get_doc("Sales Invoice", draft_return) if draft_return else None
						si_return = _amazon_init_si_header(
							customer=val,
							posting_dt=credit_note_dt,
							ecom_name=qualified_cn_no,
							is_return=not use_debit_note,
							is_debit_note=use_debit_note,
							return_against=existing_si if not use_debit_note else None,
							ecommerce_operator=self.ecommerce_mapping,
							amazon_type=self.amazon_type,
							ecommerce_gstin=mapped_ecommerce_gstin,
							update_stock=0 if use_debit_note else 1,
							draft_doc=draft_doc,
						)

						# Always set ecommerce_gstin from mapping (required for GST reporting)
						si_return.ecommerce_gstin = mapped_ecommerce_gstin

						# De-duplicate within this return invoice only
						existing_return_item_ids = {
							d.get("custom_ecom_item_id")
							for d in (si_return.get("items") or [])
							if d.get("custom_ecom_item_id")
						}
						for idx, child_row in cn_refund_items:
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
										raise Exception("State name Is Wrong Please Check")
									si_return.place_of_supply = state_code_dict.get(str(state.lower()))
								si_return.ecommerce_gstin = mapped_ecommerce_gstin

								refund_qty, refund_rate, is_zero_qty = safe_refund_qty_rate(
									child_row.quantity, child_row.tax_exclusive_gross
								)
								if use_debit_note:
									line_qty, line_rate = 0, refund_rate
								elif is_zero_qty:
									line_qty, line_rate = -1, refund_rate
								else:
									line_qty, line_rate = refund_qty, refund_rate

								hsn_code = frappe.db.get_value("Item", itemcode, "gst_hsn_code")

								_amazon_append_si_line(
									si_return,
									item_code=itemcode,
									qty=line_qty,
									rate=line_rate,
									hsn_code=hsn_code,
									description=child_row.item_description,
									warehouse=warehouse,
									income_account=amazon.income_account,
									custom_ecom_item_id=shipment_item_id,
									margin_amount=flt(child_row.item_promo_discount),
									tax_rate_scalar=flt(child_row.total_tax_amount),
									taxes=[
										("CGST", flt(child_row.cgst_rate), flt(child_row.cgst_tax), "Output Tax CGST - KGOPL"),
										("SGST",
										 flt(child_row.sgst_rate) + flt(child_row.utgst_rate),
										 flt(child_row.sgst_tax) + flt(child_row.utgst_tax),
										 "Output Tax SGST - KGOPL"),
										("IGST", flt(child_row.igst_rate), flt(child_row.igst_tax), "Output Tax IGST - KGOPL"),
									],
								)
								if shipment_item_id:
									existing_return_item_ids.add(shipment_item_id)
								ritems_append.append(itemcode)
							except Exception as item_error:
								si_error.append(invoice_no)
								errors.append({
									"idx": idx,
									"invoice_id": invoice_no,
									"message": f"Refund item error: {item_error}"
								})

						try:
							if ritems_append and not warehouse_mapping_missing and invoice_no not in si_error:
								_amazon_save_and_submit(
									si_return,
									mode_of_payment=amazon.mode_of_payment,
									due_date=getdate(today()),
								)
								frappe.db.commit()
								success_count += len(cn_refund_items)
						except Exception as submit_error:
							for idx, _ in cn_refund_items:
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
		existing_total = existing_shipment_count + existing_refund_count
		summary_extra = f" ({existing_total} already existed, skipped)" if existing_total else ""
		if errors:
			self.status = "Partial Success" if success_count else "Error"
			indicator = "orange" if success_count else "red"
			frappe.msgprint(
				f"{success_count} items processed{summary_extra}, {len(errors)} failed. "
				"See error table below for details.",
				indicator=indicator,
				alert=True,
			)
		else:
			self.error_html = ""
			if success_count == 0 and existing_total == 0:
				self.status = "Error"
				# Surface the cause as an error row so it's visible on the doc.
				rows_in_groups = sum(len(v) for v in invoice_groups.values())
				diagnosis = (
					f"No invoices were created. {len(invoice_groups)} invoice group(s) "
					f"covering {rows_in_groups} row(s) parsed from the file, but none "
					f"produced a sales/refund invoice. Likely causes: every row had a "
					f"blank Invoice Number, or every row's Transaction Type was 'Cancel'. "
					f"Check the input CSV."
				)
				errors.append({
					"idx": "",
					"invoice_id": "(no rows processed)",
					"event": "Diagnosis",
					"message": diagnosis,
				})
				frappe.msgprint(diagnosis, indicator="red")
			elif success_count == 0 and existing_total > 0:
				self.status = "Success"
				frappe.msgprint(
					f"Nothing new created. All {existing_total} items already exist as submitted invoices.",
					indicator="blue",
				)
			else:
				self.status = "Success"
				frappe.msgprint(
					f"All {success_count} items processed successfully!{summary_extra}",
					indicator="green",
				)

		self._set_import_summary(
			created=success_count,
			existing=existing_total,
			failed=len(errors),
			label="Amazon B2C",
		)
		self._persist_errors(errors)
		self._update_import_status()

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
		existing_count = 0
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

				# Amazon reuses invoice numbers across fiscal years; FY-qualify both
				# the inter-company SI/DN and PI/PR names so cross-FY re-uses don't
				# collide. is_return=0 filter retained for backward compat.
				_inv_dt = parse_export_datetime(group_rows[0][1].get("invoice_date"))
				_inv_posting_date = _inv_dt.date() if _inv_dt else None
				qualified_invoice_no = qualify_with_fy(invoice_no, _inv_posting_date)

				existing_name = find_existing_amazon_doc(
					doctype, invoice_no, _inv_posting_date,
					is_return=0, docstatus=["!=", 2],
				)
				existing_name_purchase = find_existing_amazon_doc(
					doctype_m, invoice_no, _inv_posting_date,
					is_return=0, docstatus=["!=", 2],
				)

				if existing_name:
					existing_doc = frappe.get_doc(doctype, existing_name)
					if existing_doc.docstatus == 0:
						existing_doc.submit()
					else:
						existing_count += len(group_rows)
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
					doc.custom_ecommerce_operator = self.ecommerce_mapping
					doc.custom_ecommerce_type = self.amazon_type
					doc.taxes = [] if is_taxable else None
					doc.update_stock = 1 if is_taxable else None
					doc.set_warehouse = "" if not is_taxable else None
					if not frappe.db.exists(doctype, qualified_invoice_no):
						doc._ecom_name = qualified_invoice_no
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

						tax_tuples = [
							("CGST", flt(row.cgst_rate), flt(row.cgst_amount),
							 "Output Tax CGST - KGOPL"),
							("SGST", flt(row.sgst_rate) + flt(row.utgst_rate),
							 flt(row.sgst_amount) + flt(row.utgst_amount),
							 "Output Tax SGST - KGOPL"),
							("IGST", flt(row.igst_rate), flt(row.igst_amount),
							 "Output Tax IGST - KGOPL"),
						] if is_taxable else []

						_amazon_append_si_line(
							doc,
							item_code=item_code,
							qty=qty,
							rate=rate,
							hsn_code=frappe.db.get_value("Item", item_code, "gst_hsn_code") or "",
							description="",
							warehouse=wh.erp_warehouse,
							income_account="",
							custom_ecom_item_id="",
							taxes=tax_tuples,
						)

					_amazon_save_and_submit(doc, mode_of_payment=None)
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
					pi_doc.customer = customer
					pi_doc.custom_ecommerce_operator = self.ecommerce_mapping
					pi_doc.custom_ecommerce_type = self.amazon_type
					_pi_doctype = "Purchase Invoice" if is_taxable else "Purchase Receipt"
					if not frappe.db.exists(_pi_doctype, qualified_invoice_no):
						pi_doc._ecom_name = qualified_invoice_no
					if is_taxable:
						pi_doc.bill_no = qualified_invoice_no
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

						tax_tuples = [
							("CGST", flt(row.cgst_rate), flt(row.cgst_amount),
							 "Input Tax CGST - KGOPL"),
							("SGST", flt(row.sgst_rate) + flt(row.utgst_rate),
							 flt(row.sgst_amount) + flt(row.utgst_amount),
							 "Input Tax SGST - KGOPL"),
							("IGST", flt(row.igst_rate), flt(row.igst_amount),
							 "Input Tax IGST - KGOPL"),
						] if is_taxable else []

						_amazon_append_si_line(
							pi_doc,
							item_code=item_code,
							qty=qty,
							rate=rate,
							hsn_code=frappe.db.get_value("Item", item_code, "gst_hsn_code") or "",
							description="",
							warehouse=warehouse,
							income_account="",
							custom_ecom_item_id="",
							taxes=tax_tuples,
						)

					_amazon_save_and_submit(pi_doc, mode_of_payment=None)
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
			frappe.db.commit()

		# -------- Final status update --------
		summary_extra = f" ({existing_count} already existed, skipped)" if existing_count else ""
		if errors:
			self.status = "Partial Success" if success_count else "Error"
			indicator = "orange" if success_count else "red"
			frappe.msgprint(
				f"{success_count} items processed{summary_extra}, {len(errors)} failed. "
				"See error table below for details.",
				indicator=indicator,
				alert=True,
			)
		else:
			if success_count == 0 and existing_count == 0:
				self.status = "Error"
				rows_in_groups = sum(len(v) for v in invoice_groups.values())
				diagnosis = (
					f"No documents were created. {len(invoice_groups)} invoice group(s) "
					f"covering {rows_in_groups} row(s) parsed from the file, but none "
					f"produced an inter-company SI/DN or PI/PR. Check Invoice Number "
					f"and Transaction Type columns."
				)
				errors.append({
					"idx": "",
					"invoice_id": "(no rows processed)",
					"event": "Diagnosis",
					"message": diagnosis,
				})
				frappe.msgprint(diagnosis, indicator="red")
			elif success_count == 0 and existing_count > 0:
				self.status = "Success"
				frappe.msgprint(
					f"Nothing new created. All {existing_count} items already exist as submitted documents.",
					indicator="blue",
				)
			else:
				self.status = "Success"
				frappe.msgprint(
					f"All {success_count} items processed successfully!{summary_extra}",
					indicator="green",
				)

		self._set_import_summary(
			created=success_count,
			existing=existing_count,
			failed=len(errors),
			label="Amazon Stock Transfer",
		)
		self._persist_errors(errors)
		self._update_import_status()

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
		sale_existing_count = 0
		sale_submitted_count = 0
		return_existing_count = 0
		return_submitted_count = 0

		customer = frappe.db.get_value("Ecommerce Mapping", {"platform": "Flipkart"}, "default_non_company_customer")
		flipkart = frappe.get_doc("Ecommerce Mapping", "Flipkart")

		# Build cashback lookup by (order_item_id, document_sub_type) for merging into sales items
		cashback_by_item = {}
		for cb_row in (self.flipkart_cashback or []):
			if cb_row.order_item_id and cb_row.document_sub_type:
				cashback_by_item[(cb_row.order_item_id, cb_row.document_sub_type)] = cb_row

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
					"name": invoice_key,
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
					"name": invoice_key,
					"is_return": 0,
					"docstatus": 0
				}, "name")

				first = rows[0]
				posting_date_val = parse_export_date(first.buyer_invoice_date) or getdate(first.buyer_invoice_date)
				posting_dt = datetime.combine(posting_date_val, datetime.min.time())

				warehouse, location, company_address = get_warehouse_info(first.warehouse_id)
				ecommerce_gstin = get_gstin(first.seller_gstin)

				draft_doc = frappe.get_doc("Sales Invoice", draft_name) if draft_name else None
				si = _amazon_init_si_header(
					customer=customer,
					posting_dt=posting_dt,
					ecom_name=invoice_key,
					is_return=False,
					is_debit_note=False,
					return_against=None,
					ecommerce_operator=self.ecommerce_mapping,
					amazon_type=self.amazon_type or "",
					ecommerce_gstin=ecommerce_gstin,
					update_stock=1,
					draft_doc=draft_doc,
				)

				# Flipkart-specific header mutations not covered by the helper.
				if not si.company_address:
					si.company_address = company_address
				if not si.location:
					si.location = location

				# Place of supply uses Flipkart-specific resolver (handles anonymized buyer state).
				if not si.place_of_supply:
					state = first.customers_delivery_state or first.customers_billing_state
					si.place_of_supply = resolve_flipkart_pos(
						state,
						first.seller_gstin,
						igst_amt=sum(flt(r.igst_amount) for r in rows),
						cgst_amt=sum(flt(r.cgst_amount) for r in rows),
						sgst_amt=sum(flt(r.sgst_amount) for r in rows),
					)

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
						if not si.place_of_supply:
							state = row.customers_delivery_state or row.customers_billing_state
							si.place_of_supply = resolve_flipkart_pos(
								state,
								row.seller_gstin,
								igst_amt=row.igst_amount,
								cgst_amt=row.cgst_amount,
								sgst_amt=row.sgst_amount,
							)
						if si.is_new() and not getattr(si, '_ecom_name', None) and row.buyer_invoice_id:
							# Don't set _ecom_name if invoice with that name already exists
							existing_by_name = frappe.db.exists("Sales Invoice", row.buyer_invoice_id)
							if not existing_by_name:
								si._ecom_name = row.buyer_invoice_id

						hsn_code = frappe.db.get_value("Item", item_code, "gst_hsn_code")

						qty = flt(row.item_quantity)
						taxable = flt(row.taxable_value)
						cgst_amt = flt(row.cgst_amount)
						sgst_amt = flt(row.sgst_amount)
						igst_amt = flt(row.igst_amount)

						cb = cashback_by_item.get((row.order_item_id, "Sale"))
						if cb:
							taxable += flt(cb.taxable_value)
							cgst_amt += flt(cb.cgst_amount)
							sgst_amt += flt(cb.sgst_amount_or_utgst_as_applicable)
							igst_amt += flt(cb.igst_amount)

						rate = (taxable / qty) if qty else 0

						_amazon_append_si_line(
							si,
							item_code=item_code,
							qty=qty,
							rate=rate,
							hsn_code=hsn_code,
							description=row.product_titledescription,
							warehouse=warehouse,
							income_account=flipkart.income_account,
							custom_ecom_item_id=row.order_item_id,
							taxes=[
								("CGST", flt(row.cgst_rate), cgst_amt, "Output Tax CGST - KGOPL"),
								("SGST", flt(row.sgst_rate), sgst_amt, "Output Tax SGST - KGOPL"),
								("IGST", flt(row.igst_rate), igst_amt, "Output Tax IGST - KGOPL"),
							],
						)
						existing_item_ids.add(row.order_item_id)
						items_appended += 1
					except Exception as row_error:
						group_errors = True
						errors.append({
							"idx": row.idx,
							"invoice_id": row.buyer_invoice_id,
							"event": row.event_sub_type,
							"message": str(row_error)
						})

				if items_appended > 0 and not group_errors:
					order_ids = set(r.order_id for r in rows if r.order_id)
					if order_ids:
						si.ecom_order_id = ", ".join(sorted(order_ids))
					try:
						_amazon_save_and_submit(
							si,
							mode_of_payment=flipkart.mode_of_payment,
							due_date=getdate(today()),
						)
						sale_submitted_count += 1
						frappe.db.commit()
					except Exception as submit_error:
						errors.append({
							"idx": "",
							"invoice_id": invoice_key,
							"event": "Sale",
							"message": f"Submit failed: {str(submit_error)}",
						})
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

			frappe.db.commit()

			percent = int((sale_count / total_sale_invoices) * 50)
			self._publish_progress(
				current=sale_count,
				total=total_sale_invoices,
				progress=percent,
				message=f"Processed {sale_count}/{total_sale_invoices} sale invoices",
				phase="flipkart_sales",
			)

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
					"name": invoice_key,
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
					"name": invoice_key,
					"is_return": 1,
					"docstatus": 0
				}, "name")

				first = rows[0]
				posting_date_val = parse_export_date(first.buyer_invoice_date) or getdate(first.buyer_invoice_date)
				posting_dt = datetime.combine(posting_date_val, datetime.min.time())

				warehouse, location, company_address = get_warehouse_info(first.warehouse_id)
				ecommerce_gstin = get_gstin(first.seller_gstin)

				draft_doc = frappe.get_doc("Sales Invoice", draft_name) if draft_name else None
				si = _amazon_init_si_header(
					customer=customer,
					posting_dt=posting_dt,
					ecom_name=invoice_key,
					is_return=True,
					is_debit_note=False,
					return_against=None,  # Flipkart returns don't link to a specific submitted SI
					ecommerce_operator=self.ecommerce_mapping,
					amazon_type=self.amazon_type or "",
					ecommerce_gstin=ecommerce_gstin,
					update_stock=1,
					draft_doc=draft_doc,
				)
				# Preserve Flipkart-specific header mutations not covered by helper:
				if not si.company_address:
					si.company_address = company_address
				if not si.location:
					si.location = location

				# Place of supply uses Flipkart-specific resolver (handles anonymized buyer state).
				if not si.place_of_supply:
					state = first.customers_delivery_state or first.customers_billing_state
					si.place_of_supply = resolve_flipkart_pos(
						state,
						first.seller_gstin,
						igst_amt=sum(flt(r.igst_amount) for r in rows),
						cgst_amt=sum(flt(r.cgst_amount) for r in rows),
						sgst_amt=sum(flt(r.sgst_amount) for r in rows),
					)

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
						if not si.place_of_supply:
							state = row.customers_delivery_state or row.customers_billing_state
							si.place_of_supply = resolve_flipkart_pos(
								state,
								row.seller_gstin,
								igst_amt=row.igst_amount,
								cgst_amt=row.cgst_amount,
								sgst_amt=row.sgst_amount,
							)
						if si.is_new() and not getattr(si, '_ecom_name', None) and row.buyer_invoice_id:
							# Don't set _ecom_name if invoice with that name already exists
							existing_by_name = frappe.db.exists("Sales Invoice", row.buyer_invoice_id)
							if not existing_by_name:
								si._ecom_name = row.buyer_invoice_id

						hsn_code = frappe.db.get_value("Item", item_code, "gst_hsn_code")

						qty_abs = abs(flt(row.item_quantity))
						taxable = abs(flt(row.taxable_value))
						cgst_amt = flt(row.cgst_amount)
						sgst_amt = flt(row.sgst_amount)
						igst_amt = flt(row.igst_amount)

						cb = cashback_by_item.get((row.order_item_id, "Return"))
						if cb:
							taxable += abs(flt(cb.taxable_value))
							cgst_amt += flt(cb.cgst_amount)
							sgst_amt += flt(cb.sgst_amount_or_utgst_as_applicable)
							igst_amt += flt(cb.igst_amount)

						rate = taxable / qty_abs if qty_abs else 0

						_amazon_append_si_line(
							si,
							item_code=item_code,
							qty=-qty_abs,
							rate=rate,
							hsn_code=hsn_code,
							description=row.product_titledescription,
							warehouse=warehouse,
							income_account=flipkart.income_account,
							custom_ecom_item_id=row.order_item_id,
							taxes=[
								("CGST", flt(row.cgst_rate), cgst_amt, "Output Tax CGST - KGOPL"),
								("SGST", flt(row.sgst_rate), sgst_amt, "Output Tax SGST - KGOPL"),
								("IGST", flt(row.igst_rate), igst_amt, "Output Tax IGST - KGOPL"),
							],
						)
						existing_item_ids.add(row.order_item_id)
						items_appended += 1
					except Exception as row_error:
						group_errors = True
						errors.append({
							"idx": row.idx,
							"invoice_id": row.buyer_invoice_id,
							"event": row.event_sub_type,
							"message": str(row_error)
						})

				if items_appended > 0 and not group_errors:
					order_ids = set(r.order_id for r in rows if r.order_id)
					if order_ids:
						si.ecom_order_id = ", ".join(sorted(order_ids))
					try:
						_amazon_save_and_submit(
							si,
							mode_of_payment=flipkart.mode_of_payment,
							due_date=getdate(today()),
						)
						return_submitted_count += 1
						frappe.db.commit()
					except Exception as submit_error:
						errors.append({
							"idx": "",
							"invoice_id": invoice_key,
							"event": "Return",
							"message": f"Submit failed: {str(submit_error)}",
						})
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

			frappe.db.commit()

			percent = 50 + int((return_count / total_return_invoices) * 50)
			self._publish_progress(
				current=return_count,
				total=total_return_invoices,
				progress=percent,
				message=f"Processed {return_count}/{total_return_invoices} return invoices",
				phase="flipkart_returns",
			)

		self._persist_errors(errors)
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

		self._update_import_status()

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

		This implementation is based on the CRED CSV columns:
		- Groups rows by **EE Invoice No** (fallback Invoice_id / Suborder No) into one Sales Invoice
		- Sets invoice datetime from **Printed At** (fallback **Confirmed At**, then Invoice Date / Order Date)
		- Uses **Item Quantity** + **Item Price Excluding Tax** to build per-unit rate
		- Adds GST taxes using provided **tax** / **Tax Rate** values (Actual charge type)
		- Skips cancelled rows (Order Status / Shipping Status / Cancelled At)

		We parse the CSV inside the background job (RQ worker) to avoid bloating the parent
		document with hidden child tables.
		"""
		import re
		import pandas as pd

		errors = []

		file_path = resolve_file_path(self.cred_attach)

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
			"""Normalize column name to snake_case for lookup."""
			col_name = (str(col_name) or "").strip().lower()
			col_name = re.sub(r"[^a-z0-9]+", "_", col_name).strip("_")
			return col_name

		col_map = {normalize_col(c): c for c in df.columns}

		def get_cell(row, key: str) -> str:
			"""Get a cell value by normalized column key, cleaned."""
			col = col_map.get(key)
			if not col:
				return ""
			return clean_csv_cell(row.get(col))

		# --- Build XLSX warehouse lookup (optional) ---
		# CSV's Client Location is region-level (e.g. DELHI). The XLSX Cred Mail
		# Report's Sales sheet carries the per-warehouse code (e.g. MEV110035B)
		# in Warehouse_Location_Code, joined to CSV via CRED_Order_Item_Id ↔
		# Suborder No. We prefer XLSX when present, fall back to CSV otherwise.
		xlsx_warehouse_by_order_item = {}
		if self.cred_refund_attach:
			try:
				xlsx_path = resolve_file_path(self.cred_refund_attach)
				sdf = pd.read_excel(xlsx_path, sheet_name="Sales", dtype=str, keep_default_na=False)

				# Strip backtick defensively in case CRED uses the same prefix on XLSX side.
				def _strip_tick(v):
					s = (str(v) or "").strip()
					return s[1:] if s.startswith("`") else s

				for _, srow in sdf.iterrows():
					cred_oid = _strip_tick(srow.get("CRED_Order_Item_Id", ""))
					wlc = (srow.get("Warehouse_Location_Code", "") or "").strip()
					if cred_oid and wlc:
						xlsx_warehouse_by_order_item[cred_oid] = wlc
			except Exception:
				# XLSX may be missing the Sales sheet or unreadable — fall back to CSV.
				xlsx_warehouse_by_order_item = {}

		def get_place_of_supply(state_name: str):
			"""Resolve state name to place_of_supply code using state_code_dict."""
			key = normalize_state_key(state_name)
			return state_code_dict.get(key)

		def resolve_invoice_datetime(row):
			"""Resolve invoice datetime: Printed At > Confirmed At > Invoice Date > Order Date."""
			return (
				parse_export_datetime(get_cell(row, "printed_at"))
				or parse_export_datetime(get_cell(row, "confirmed_at"))
				or parse_export_datetime(get_cell(row, "invoice_date"))
				or parse_export_datetime(get_cell(row, "order_date"))
			)

		def get_invoice_no(row):
			"""Resolve invoice grouping key: EE Invoice No > Invoice_id > Suborder No > Reference Code."""
			return (
				get_cell(row, "ee_invoice_no")
				or get_cell(row, "invoice_id")
				or get_cell(row, "suborder_no")
				or get_cell(row, "reference_code")
			)

		def is_cancelled_row(row):
			"""Check if a row should be skipped as cancelled."""
			status = get_cell(row, "order_status").upper()
			ship_status = get_cell(row, "shipping_status").upper()
			cancelled_at = get_cell(row, "cancelled_at")
			return (
				status in {"CANCELLED", "CANCELED", "RTO"}
				or ship_status in {"CANCELLED", "CANCELED", "RTO"}
				or bool(cancelled_at)
			)

		def get_item_code(ecom_sku: str):
			"""Look up ERP item code from mapping by ecom SKU."""
			for mapping_row in cred_mapping.ecom_item_table:
				if mapping_row.ecom_item_id == ecom_sku:
					return mapping_row.erp_item
			return None

		def resolve_sku_for_mapping(row):
			"""Resolve SKU value from row using configured ecom_sku_column_header with fallback to marketplace_sku."""
			# Use Ecommerce Mapping's ecom_sku_column_header field
			configured = (cred_mapping.ecom_sku_column_header or "").strip()
			if configured:
				configured_key = normalize_col(configured)
				value = get_cell(row, configured_key)
				if value:
					return value

			# Fallback to marketplace_sku
			return get_cell(row, "marketplace_sku")

		# --- Build invoice groups (skip cancelled rows) ---
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
		success_refunds = 0
		existing_count = 0
		existing_refund_count = 0

		for count, (invoice_no, rows) in enumerate(invoice_groups.items(), start=1):
			first_idx = None
			try:
				# Skip if already submitted
				existing_submitted = frappe.db.get_value(
					"Sales Invoice",
					{"name": invoice_no, "is_return": 0, "docstatus": 1},
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
					existing_count += 1
					continue

				# Check for draft to resume
				draft_name = frappe.db.get_value(
					"Sales Invoice",
					{"name": invoice_no, "is_return": 0, "docstatus": 0},
					"name",
				)

				first_idx, first_row = rows[0]

				# --- Resolve header fields ---
				seller_gstin = get_cell(first_row, "seller_gst_num")
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

				# --- Resolve warehouse: prefer XLSX Warehouse_Location_Code (granular,
				# e.g. MEV110035B); fall back to CSV's Client Location (region-level,
				# e.g. DELHI) if no XLSX attached or no per-order match.
				csv_suborder = (get_cell(first_row, "suborder_no") or "").strip()
				if csv_suborder.startswith("`"):
					csv_suborder = csv_suborder[1:]
				warehouse_code = (
					xlsx_warehouse_by_order_item.get(csv_suborder)
					or get_cell(first_row, "client_location")
				)
				wh_map = next(
					(
						w
						for w in (cred_mapping.ecommerce_warehouse_mapping or [])
						if (w.ecom_warehouse_id or "").strip() == warehouse_code
					),
					None,
				)
				warehouse = (wh_map.erp_warehouse if wh_map and wh_map.erp_warehouse else cred_mapping.default_company_warehouse)
				location = (wh_map.location if wh_map and wh_map.location else cred_mapping.default_company_location)
				company_address = (wh_map.erp_address if wh_map and wh_map.erp_address else cred_mapping.default_company_address)

				if not warehouse:
					raise Exception(
						f"Warehouse mapping missing for warehouse code: {warehouse_code!r} "
						f"(CRED Order Item: {csv_suborder!r}). Add it in Ecommerce Mapping "
						f"'{cred_mapping.name}' -> Warehouse Mapping, or set a default warehouse."
					)

				# --- Create or resume Sales Invoice ---
				posting_dt = datetime.combine(invoice_dt.date(), invoice_dt.time())
				draft_doc = frappe.get_doc("Sales Invoice", draft_name) if draft_name else None
				si = _amazon_init_si_header(
					customer=customer,
					posting_dt=posting_dt,
					ecom_name=invoice_no,
					is_return=False,
					is_debit_note=False,
					return_against=None,
					ecommerce_operator=self.ecommerce_mapping,
					amazon_type="",
					ecommerce_gstin=ecommerce_gstin,
					update_stock=1,
					draft_doc=draft_doc,
				)

				# CRED-specific header mutations not covered by the helper.
				si.location = location
				si.set_warehouse = warehouse
				si.company_address = company_address
				si.place_of_supply = place_of_supply

				# De-duplicate within this invoice using CRED's Suborder No / Reference Code
				existing_item_ids = {
					d.get("custom_ecom_item_id")
					for d in (si.get("items") or [])
					if d.get("custom_ecom_item_id")
				}

				# --- Tax split decision (intra-state vs inter-state) ---
				customer_state_code = (place_of_supply.split("-")[0] if place_of_supply else "")
				seller_state_code = (str(seller_gstin)[:2] if str(seller_gstin)[:2].isdigit() else "")
				is_intra_state = (seller_state_code and customer_state_code and seller_state_code == customer_state_code)

				# --- Process item rows ---
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

					# --- Tax calculation per row ---
					row_tax_rate = normalize_tax_rate(flt(get_cell(row, "tax_rate")))
					row_tax_amount = flt(get_cell(row, "tax"))

					# If tax amount missing but rate present, compute from taxable total
					if row_tax_amount <= 0 and row_tax_rate and taxable_total:
						row_tax_amount = taxable_total * (row_tax_rate / 100)

					# Split row tax into CGST/SGST or IGST tuples for the shared helper.
					if row_tax_amount > 0 and is_intra_state:
						half_rate = (row_tax_rate / 2) if row_tax_rate else 0
						half_amount = row_tax_amount / 2
						row_taxes = [
							("CGST", half_rate, half_amount, "Output Tax CGST - KGOPL"),
							("SGST", half_rate, half_amount, "Output Tax SGST - KGOPL"),
							("IGST", 0, 0, "Output Tax IGST - KGOPL"),
						]
					elif row_tax_amount > 0:
						row_taxes = [
							("CGST", 0, 0, "Output Tax CGST - KGOPL"),
							("SGST", 0, 0, "Output Tax SGST - KGOPL"),
							("IGST", row_tax_rate, row_tax_amount, "Output Tax IGST - KGOPL"),
						]
					else:
						row_taxes = []

					_amazon_append_si_line(
						si,
						item_code=item_code,
						qty=qty,
						rate=rate,
						hsn_code=hsn_code,
						description=product_name,
						warehouse=warehouse,
						income_account=cred_mapping.income_account,
						custom_ecom_item_id=item_id,
						taxes=row_taxes,
					)
					if item_id:
						existing_item_ids.add(item_id)

				# --- Check we have items ---
				if not si.items:
					raise Exception("No items were added for this invoice (all duplicates or invalid)")

				# --- Save + submit (deterministic per invoice group) ---
				_amazon_save_and_submit(
					si,
					mode_of_payment=cred_mapping.mode_of_payment,
					due_date=getdate(today()),
				)
				frappe.db.commit()
				success_invoices += 1

			except Exception as e:
				frappe.db.rollback()
				errors.append(
					{
						"idx": first_idx,
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

		# -------- REFUND credit notes --------
		# Iterates self.cred_refund (populated from the CRED Mail Report Refund
		# sheet on validate). One CN per parent EE Invoice No, named
		# "<EE_INV>RT". Idempotent: re-runs skip already-submitted CNs. Refund
		# rows whose parent SI is not yet submitted are skipped silently — they
		# will be picked up on a later import once the sales side lands.
		refund_groups = {}
		for r in (self.cred_refund or []):
			ee = (r.ee_invoice_no or "").strip()
			if not ee:
				errors.append({
					"idx": r.idx,
					"invoice_id": r.cred_order_item_id or "",
					"event": "Refund",
					"message": "No EE Invoice No found for this refund (CSV had no matching parent).",
				})
				continue
			parent = frappe.db.get_value(
				"Sales Invoice",
				{"name": ee, "docstatus": 1},
				"name",
			)
			if not parent:
				# Parent SI not yet submitted — skip silently. Will be picked up
				# on the next refund import once sales for that EE Inv land.
				continue
			refund_groups.setdefault(ee, []).append(r)

		# Resolve the generic "refund line item" once. CRED's Ecommerce Mapping
		# does not have a dedicated refund-item field, so we fall back to:
		#   1. cashback_offer_item (closest "generic CRED item")
		#   2. first erp_item in ecom_item_table
		# If neither is set, raise per-group below.
		default_refund_item = None
		if getattr(cred_mapping, "cashback_offer_item", None):
			default_refund_item = cred_mapping.cashback_offer_item
		elif cred_mapping.ecom_item_table:
			first_map = next(
				(m.erp_item for m in cred_mapping.ecom_item_table if m.erp_item),
				None,
			)
			default_refund_item = first_map

		for ee_invoice_no, refunds in refund_groups.items():
			cn_name = f"{ee_invoice_no}RT"
			if frappe.db.exists("Sales Invoice", {"name": cn_name, "docstatus": 1}):
				existing_refund_count += 1
				continue
			try:
				if not default_refund_item:
					raise Exception(
						"Set CRED refund item on Ecommerce Mapping "
						"(cashback_offer_item or first erp_item in ecom_item_table)."
					)

				# Earliest refund date in the group becomes posting datetime.
				refund_dates = [getdate(r.refund_date) for r in refunds if r.refund_date]
				if not refund_dates:
					raise Exception("No refund_date on any refund row in this group.")
				first_dt = min(refund_dates)
				posting_dt = datetime.combine(first_dt, datetime.min.time())

				# Inherit GSTIN / place-of-supply / company_address / location
				# from the parent SI for consistency.
				parent_si = frappe.get_doc("Sales Invoice", ee_invoice_no)
				ecommerce_gstin = parent_si.ecommerce_gstin
				place_of_supply = parent_si.place_of_supply
				company_address = parent_si.company_address
				location = parent_si.location

				cn = _amazon_init_si_header(
					customer=customer,
					posting_dt=posting_dt,
					ecom_name=cn_name,
					is_return=True,
					is_debit_note=False,
					return_against=ee_invoice_no,
					ecommerce_operator=self.ecommerce_mapping,
					amazon_type="",
					ecommerce_gstin=ecommerce_gstin,
					update_stock=1,
					draft_doc=None,
				)
				cn.location = location
				cn.set_warehouse = cred_mapping.default_company_warehouse
				cn.company_address = company_address
				cn.place_of_supply = place_of_supply

				hsn_code = frappe.db.get_value("Item", default_refund_item, "gst_hsn_code")

				for r in refunds:
					gmv = flt(r.gmv)
					gst_rate = flt(r.gst_rate)  # already normalized to percent by T2
					tax_amt_total = gmv * gst_rate / 100.0
					cust_state = (r.customer_state or "").strip().upper()
					wh_state = (r.warehouse_state or "").strip().upper()
					intra = bool(cust_state) and cust_state == wh_state

					if intra:
						half_rate = gst_rate / 2.0
						half_amt = tax_amt_total / 2.0
						row_taxes = [
							("CGST", half_rate, half_amt, "Output Tax CGST - KGOPL"),
							("SGST", half_rate, half_amt, "Output Tax SGST - KGOPL"),
							("IGST", 0, 0, "Output Tax IGST - KGOPL"),
						]
					else:
						row_taxes = [
							("CGST", 0, 0, "Output Tax CGST - KGOPL"),
							("SGST", 0, 0, "Output Tax SGST - KGOPL"),
							("IGST", gst_rate, tax_amt_total, "Output Tax IGST - KGOPL"),
						]

					_amazon_append_si_line(
						cn,
						item_code=default_refund_item,
						qty=-1,
						rate=gmv,
						hsn_code=hsn_code,
						description=f"CRED refund - {r.cred_order_item_id} ({r.order_status})",
						warehouse=cred_mapping.default_company_warehouse,
						income_account=cred_mapping.income_account,
						custom_ecom_item_id=r.cred_order_item_id,
						taxes=row_taxes,
					)

				_amazon_save_and_submit(
					cn,
					mode_of_payment=cred_mapping.mode_of_payment,
					due_date=getdate(today()),
				)
				success_refunds += 1
				frappe.db.commit()

			except Exception as e:
				frappe.db.rollback()
				for r in refunds:
					errors.append({
						"idx": r.idx,
						"invoice_id": ee_invoice_no,
						"event": "Refund",
						"message": f"CN creation failed: {e}",
					})

		# --- Final status + progress ---
		self._persist_errors(errors)
		total_success = success_invoices + success_refunds
		if errors and total_success:
			self.status = "Partial Success"
		elif errors and not total_success:
			self.status = "Error"
		else:
			self.status = "Success"

		self._set_import_summary(
			created=total_success,
			existing=existing_count + existing_refund_count,
			failed=len(errors),
			label="CRED",
		)
		self._update_import_status()

		self._publish_progress(
			current=total_invoices,
			total=total_invoices,
			progress=100,
			message=f"CRED Import Completed ({success_invoices} sales + {success_refunds} refunds)",
			phase="cred",
		)

		return {
			"status": self.status,
			"errors": errors,
			"success_invoices": success_invoices,
			"success_refunds": success_refunds,
		}


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
					"name": invoice_key,
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
					"name": invoice_key,
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
					si.flags.ignore_pricing_rule = 1
					si.customer = customer
					si.set_posting_time = 1
					si.posting_date = parse_export_date(first.buyer_invoice_date) or getdate(first.buyer_invoice_date)
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
					if not frappe.db.exists("Sales Invoice", first.buyer_invoice_id):
						si._ecom_name = first.buyer_invoice_id
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
						if not si.place_of_supply:
							state = row.customers_delivery_state or row.customers_billing_state
							if state:
								if not state_code_dict.get(str(state).lower()):
									raise Exception("State name Is Wrong Please Check")
								si.place_of_supply = state_code_dict.get(str(state).lower())
						if si.is_new() and not getattr(si, '_ecom_name', None) and row.buyer_invoice_id:
							if not frappe.db.exists("Sales Invoice", row.buyer_invoice_id):
								si._ecom_name = row.buyer_invoice_id

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
					order_ids = set(r.order_id for r in rows if r.order_id)
					if order_ids:
						si.ecom_order_id = ", ".join(sorted(order_ids))
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
					"name": invoice_key,
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
					"name": invoice_key,
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
					si.flags.ignore_pricing_rule = 1
					si.customer = customer
					si.set_posting_time = 1
					si.posting_date = parse_export_date(first.buyer_invoice_date) or getdate(first.buyer_invoice_date)
					si.custom_ecommerce_operator = self.ecommerce_mapping
					si.custom_ecommerce_type = self.amazon_type
					si.taxes_and_charges = ""
					si.update_stock = 1
					si.company_address = company_address
					si.ecommerce_gstin = ecommerce_gstin or ""
					si.location = location
					si.is_return = 1
					if not frappe.db.exists("Sales Invoice", first.buyer_invoice_id):
						si._ecom_name = first.buyer_invoice_id
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
						if not si.place_of_supply:
							state = row.customers_delivery_state or row.customers_billing_state
							if state:
								if not state_code_dict.get(str(state).lower()):
									raise Exception("State name Is Wrong Please Check")
								si.place_of_supply = state_code_dict.get(str(state).lower())
						if si.is_new() and not getattr(si, '_ecom_name', None) and row.buyer_invoice_id:
							# Avoid duplicate primary key errors if an invoice with this name already exists
							existing_by_name = frappe.db.exists("Sales Invoice", row.buyer_invoice_id)
							if not existing_by_name:
								si._ecom_name = row.buyer_invoice_id

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
					order_ids = set(r.order_id for r in rows if r.order_id)
					if order_ids:
						si.ecom_order_id = ", ".join(sorted(order_ids))
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

		self._persist_errors(errors)
		if len(errors) == 0:
			self.status = "Success"
		elif len(self.jio_mart_items) != len(errors):
			self.status = "Partial Success"
		else:
			self.status = "Error"

		self._update_import_status()

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
