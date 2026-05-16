# Copyright (c) 2026, Sagar Ratan Garg and contributors
# For license information, please see license.txt

"""Post-import reconciliation: compare CSV-side taxable/tax against the
submitted Sales Invoices for the same Ecommerce Bill Import.

Each platform iterator groups child-table rows by the ecom invoice id used
to name the SI on import, sums taxable_value + tax_amount + grand_total
from the CSV side, and the common comparator fetches the SI by that name
(FY-qualified for Amazon, raw for Flipkart / CRED / JioMart) to compute
the variance.

Entry point: `reconcile_ecommerce_bill_import(doc)` — called from the
whitelisted method on Ecommerce Bill Import.
"""

import frappe
from frappe.utils import flt, getdate

# Per-invoice rounding tolerance. Two-decimal CSVs vs ERPNext's net_total
# computed from per-unit rate × qty can drift ~0.01 per line.
TOLERANCE = 0.10


def _csv_group(invoice_no, posting_date, type_, taxable, tax, total):
	return {
		"type": type_,
		"ecom_invoice_no": invoice_no,
		"posting_date": posting_date,
		"csv_taxable": flt(taxable, 2),
		"csv_tax": flt(tax, 2),
		"csv_total": flt(total, 2),
	}


def _parse_dt(raw):
	if not raw:
		return None
	try:
		return getdate(raw)
	except Exception:
		return None


# ---------- Amazon (B2B + B2C) ----------

def _iter_amazon(rows):
	"""Yield CSV groups for Amazon MTR rows (B2B and B2C share the same shape).

	Sales: group by invoice_number (transaction_type != Refund).
	Refunds: group by credit_note_no (transaction_type == Refund).
	"""
	sales = {}
	refunds = {}
	for row in rows:
		txn = (row.get("transaction_type") or "").strip()
		taxable = flt(row.get("tax_exclusive_gross"))
		tax = flt(row.get("total_tax_amount"))
		total = flt(row.get("invoice_amount"))
		if txn == "Refund":
			cn = (row.get("credit_note_no") or "").strip()
			if not cn:
				continue
			bucket = refunds.setdefault(cn, {"taxable": 0, "tax": 0, "total": 0, "dt": None})
			bucket["taxable"] += taxable
			bucket["tax"] += tax
			bucket["total"] += total
			if not bucket["dt"]:
				bucket["dt"] = _parse_dt(row.get("credit_note_date"))
		else:
			inv = (row.get("invoice_number") or "").strip()
			if not inv:
				continue
			bucket = sales.setdefault(inv, {"taxable": 0, "tax": 0, "total": 0, "dt": None})
			bucket["taxable"] += taxable
			bucket["tax"] += tax
			bucket["total"] += total
			if not bucket["dt"]:
				bucket["dt"] = _parse_dt(row.get("invoice_date"))

	for inv, b in sales.items():
		yield _csv_group(inv, b["dt"], "Sale", b["taxable"], b["tax"], b["total"])
	for cn, b in refunds.items():
		yield _csv_group(cn, b["dt"], "Refund", b["taxable"], b["tax"], b["total"])


def _resolve_si_name_amazon(group):
	"""Amazon SI names are FY-qualified."""
	from ecom_import_tool.ecom_import_tool.doctype.ecommerce_bill_import.ecommerce_bill_import import (
		qualify_with_fy,
	)
	return qualify_with_fy(group["ecom_invoice_no"], group["posting_date"])


# ---------- Flipkart ----------

def _iter_flipkart_items(rows):
	"""Flipkart sales side: rows where event_sub_type == 'Sale'. Group by
	buyer_invoice_id, sum taxable_value and per-tax-head amounts."""
	sales = {}
	for row in rows:
		event = (row.get("event_sub_type") or "").strip()
		if event != "Sale":
			continue
		inv = (row.get("buyer_invoice_id") or "").strip()
		if not inv:
			continue
		taxable = flt(row.get("taxable_value"))
		tax = flt(row.get("cgst_amount")) + flt(row.get("sgst_amount")) + flt(row.get("igst_amount"))
		total = flt(row.get("final_invoice_amount"))
		bucket = sales.setdefault(inv, {"taxable": 0, "tax": 0, "total": 0, "dt": None})
		bucket["taxable"] += taxable
		bucket["tax"] += tax
		bucket["total"] += total
		if not bucket["dt"]:
			bucket["dt"] = _parse_dt(row.get("buyer_invoice_date") or row.get("order_approval_date"))
	for inv, b in sales.items():
		yield _csv_group(inv, b["dt"], "Sale", b["taxable"], b["tax"], b["total"])


def _iter_flipkart_transactions(rows):
	"""Flipkart refund/cancellation: credit_note_id_debit_note_id as the SI
	name. taxable_value + cgst/sgst/igst amounts already negative."""
	refunds = {}
	for row in rows:
		cn = (row.get("credit_note_id_debit_note_id") or "").strip()
		if not cn:
			continue
		taxable = flt(row.get("taxable_value"))
		tax = (
			flt(row.get("cgst_amount"))
			+ flt(row.get("sgst_amount_or_utgst_as_applicable"))
			+ flt(row.get("igst_amount"))
		)
		total = flt(row.get("invoice_amount"))
		bucket = refunds.setdefault(cn, {"taxable": 0, "tax": 0, "total": 0, "dt": None})
		bucket["taxable"] += taxable
		bucket["tax"] += tax
		bucket["total"] += total
		if not bucket["dt"]:
			bucket["dt"] = _parse_dt(row.get("invoice_date"))
	for cn, b in refunds.items():
		yield _csv_group(cn, b["dt"], "Refund", b["taxable"], b["tax"], b["total"])


def _resolve_si_name_raw(group):
	"""Flipkart / JioMart / CRED Sales: SI name is the raw ecom invoice id
	(no FY prefix)."""
	return group["ecom_invoice_no"]


# ---------- JioMart ----------

def _iter_jiomart(rows):
	"""JioMart: sales rows have event_sub_type 'Sale' (or empty), refund
	rows are 'Return'. Group by buyer_invoice_id / original_invoice_id."""
	sales = {}
	refunds = {}
	for row in rows:
		event = (row.get("event_sub_type") or "").strip().lower()
		taxable = flt(row.get("taxable_value"))
		tax = (
			flt(row.get("cgst_amount"))
			+ flt(row.get("sgst_amount_or_utgst_as_applicable"))
			+ flt(row.get("igst_amount"))
		)
		total = flt(row.get("buyer_invoice_amount"))
		if event in ("return", "refund", "cancellation"):
			inv = (row.get("original_invoice_id") or row.get("buyer_invoice_id") or "").strip()
			if not inv:
				continue
			bucket = refunds.setdefault(inv, {"taxable": 0, "tax": 0, "total": 0, "dt": None})
		else:
			inv = (row.get("buyer_invoice_id") or "").strip()
			if not inv:
				continue
			bucket = sales.setdefault(inv, {"taxable": 0, "tax": 0, "total": 0, "dt": None})
		bucket["taxable"] += taxable
		bucket["tax"] += tax
		bucket["total"] += total
		if not bucket["dt"]:
			bucket["dt"] = _parse_dt(row.get("buyer_invoice_date") or row.get("order_approval_date"))

	for inv, b in sales.items():
		yield _csv_group(inv, b["dt"], "Sale", b["taxable"], b["tax"], b["total"])
	for inv, b in refunds.items():
		yield _csv_group(inv, b["dt"], "Refund", b["taxable"], b["tax"], b["total"])


# ---------- CRED ----------

def _iter_cred_sales(rows):
	"""CRED sales: `cred` child table. The EE invoice no lives in
	`cred_order_item_id`. taxable_amount + tax_amount columns hold the
	per-row GST split."""
	sales = {}
	for row in rows:
		inv = (row.get("cred_order_item_id") or "").strip()
		if not inv:
			continue
		taxable = flt(row.get("taxable_amount"))
		tax = flt(row.get("tax_amount"))
		total = taxable + tax
		bucket = sales.setdefault(inv, {"taxable": 0, "tax": 0, "total": 0, "dt": None})
		bucket["taxable"] += taxable
		bucket["tax"] += tax
		bucket["total"] += total
		if not bucket["dt"]:
			bucket["dt"] = _parse_dt(row.get("order_date_time"))
	for inv, b in sales.items():
		yield _csv_group(inv, b["dt"], "Sale", b["taxable"], b["tax"], b["total"])


def _iter_cred_refund(rows):
	"""CRED refunds: cred_refund child table. Credit note named <EE_INV>RT."""
	refunds = {}
	for row in rows:
		ee = (row.get("ee_invoice_no") or "").strip()
		if not ee:
			continue
		cn = f"{ee}RT"
		taxable = -abs(flt(row.get("taxable_amount")))
		tax = -abs(flt(row.get("tax_amount")))
		total = taxable + tax
		bucket = refunds.setdefault(cn, {"taxable": 0, "tax": 0, "total": 0, "dt": None})
		bucket["taxable"] += taxable
		bucket["tax"] += tax
		bucket["total"] += total
		if not bucket["dt"]:
			bucket["dt"] = _parse_dt(row.get("refund_date_time") or row.get("return_date_time"))
	for cn, b in refunds.items():
		yield _csv_group(cn, b["dt"], "Refund", b["taxable"], b["tax"], b["total"])


# ---------- Dispatch ----------

def _csv_groups_for(doc):
	"""Yield (csv_group, si_name_resolver) for every grouped CSV invoice on the doc."""
	platform = frappe.db.get_value("Ecommerce Mapping", doc.ecommerce_mapping, "platform") or ""

	if platform == "Amazon" or doc.ecommerce_mapping == "Amazon":
		for grp in _iter_amazon(doc.mtr_b2b or []):
			yield grp, _resolve_si_name_amazon
		for grp in _iter_amazon(doc.mtr_b2c or []):
			yield grp, _resolve_si_name_amazon
	elif platform == "Flipkart" or doc.ecommerce_mapping == "Flipkart":
		for grp in _iter_flipkart_items(doc.flipkart_items or []):
			yield grp, _resolve_si_name_raw
		# flipkart_cashback table holds the transactions used for CN names
		for grp in _iter_flipkart_transactions(getattr(doc, "flipkart_cashback", []) or []):
			yield grp, _resolve_si_name_raw
	elif platform == "Jiomart" or doc.ecommerce_mapping == "Jiomart":
		for grp in _iter_jiomart(doc.jio_mart_items or []):
			yield grp, _resolve_si_name_raw
	elif platform in ("Cred", "CRED") or doc.ecommerce_mapping in ("Cred", "CRED"):
		for grp in _iter_cred_sales(doc.cred or []):
			yield grp, _resolve_si_name_raw
		for grp in _iter_cred_refund(getattr(doc, "cred_refund", []) or []):
			yield grp, _resolve_si_name_raw


def reconcile_ecommerce_bill_import(doc):
	"""Build the reconciliation rows for the given Ecommerce Bill Import.

	Each row carries CSV-side totals, the resolved SI name, SI-side totals,
	docstatus label, and the per-column variance (CSV minus SI). Rows where
	|variance| > TOLERANCE on any column are flagged with `match=False` so
	the UI can highlight them.
	"""
	out = []
	for group, resolver in _csv_groups_for(doc):
		si_name = resolver(group)
		si = _fetch_si(si_name)
		row = {
			**group,
			"si_name": si_name,
			"si_status": si["status"] if si else "Missing",
			"si_taxable": si["net_total"] if si else 0.0,
			"si_tax": si["taxes"] if si else 0.0,
			"si_total": si["grand_total"] if si else 0.0,
		}
		row["var_taxable"] = flt(row["csv_taxable"] - row["si_taxable"], 2)
		row["var_tax"] = flt(row["csv_tax"] - row["si_tax"], 2)
		row["var_total"] = flt(row["csv_total"] - row["si_total"], 2)
		row["match"] = (
			si is not None
			and abs(row["var_taxable"]) <= TOLERANCE
			and abs(row["var_tax"]) <= TOLERANCE
			and abs(row["var_total"]) <= TOLERANCE
		)
		out.append(row)
	out.sort(key=lambda r: (r["match"], r["type"], r["ecom_invoice_no"]))
	return out


_STATUS_LABEL = {0: "Draft", 1: "Submitted", 2: "Cancelled"}


def _fetch_si(name):
	if not name:
		return None
	row = frappe.db.get_value(
		"Sales Invoice",
		name,
		["docstatus", "net_total", "total_taxes_and_charges", "grand_total"],
		as_dict=True,
	)
	if not row:
		return None
	return {
		"status": _STATUS_LABEL.get(row.docstatus, "Unknown"),
		"net_total": flt(row.net_total, 2),
		"taxes": flt(row.total_taxes_and_charges, 2),
		"grand_total": flt(row.grand_total, 2),
	}
