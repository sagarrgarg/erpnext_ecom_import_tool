# Copyright (c) 2026, Sagar Ratan Garg and contributors
# For license information, please see license.txt

"""Stock safety for ecommerce imports.

1. ``submit_checked`` submits a document and then checks that every stock row
   has a Stock Ledger Entry. If one is missing, or the submit raises, the
   document goes back to draft (savepoint rollback) instead of staying
   submitted with half its ledger.
2. ``PostingClock`` gives each document of one import run its own posting
   time (+1 second per document on the platform's date), so ERPNext never sees
   many stock vouchers at one timestamp (each would queue its own full repost).
3. ``batch_stock_needs`` / ``stock_shortfall`` compare what a sales batch takes
   out of each (item, warehouse) with the stock there at the batch's earliest
   posting time. The preview shows the gap; during the run, sales touching a
   short (item, warehouse) are not submitted.

The run-wide state (clock, short pairs) lives in ``frappe.flags`` and is set
by ``start_run`` / cleared by ``end_run`` around one import.
"""

from datetime import date, datetime, time, timedelta

import frappe
from frappe.utils import cint, flt, getdate, get_time

NO_STOCK_LEDGER = "Submit failed: no stock ledger"
STOCK_SHORTFALL = "Stock shortfall"

# Stock vouchers whose stock rows must each have a Stock Ledger Entry.
# Invoices only when update_stock=1; notes/receipts always.
_ITEM_TABLE = {
	"Sales Invoice": "Sales Invoice Item",
	"Purchase Invoice": "Purchase Invoice Item",
	"Delivery Note": "Delivery Note Item",
	"Purchase Receipt": "Purchase Receipt Item",
}
_ALWAYS_STOCK = ("Delivery Note", "Purchase Receipt")
_OUTGOING = ("Sales Invoice", "Delivery Note")

_FLAG_CLOCK = "ecom_posting_clock"
_FLAG_SHORT = "ecom_stock_short"


class NoStockLedgerError(frappe.ValidationError):
	pass


class StockShortfallError(frappe.ValidationError):
	pass


# ---------------------------------------------------------------------------
# Run state
# ---------------------------------------------------------------------------

def start_run(short_rows=None):
	"""Begin one import run: a fresh posting clock and the batch's shortfall."""
	frappe.flags[_FLAG_CLOCK] = PostingClock()
	frappe.flags[_FLAG_SHORT] = {(r["item_code"], r["warehouse"]): r for r in (short_rows or [])}


def end_run():
	frappe.flags.pop(_FLAG_CLOCK, None)
	frappe.flags.pop(_FLAG_SHORT, None)


# ---------------------------------------------------------------------------
# 1. Submit, then prove the stock ledger exists
# ---------------------------------------------------------------------------

def _uses_stock(doctype, update_stock):
	if doctype in _ALWAYS_STOCK:
		return True
	return doctype in _ITEM_TABLE and cint(update_stock)


def stock_ledger_gaps(doctype, names):
	"""For submitted stock vouchers, return {name: {"expected": n, "missing": [rows]}}
	listing the stock-item rows that have no active Stock Ledger Entry.
	Docs without gaps (or that don't move stock) are left out."""
	names = [n for n in (names or []) if n]
	item_table = _ITEM_TABLE.get(doctype)
	if not names or not item_table:
		return {}

	stock_filter = "" if doctype in _ALWAYS_STOCK else "AND p.update_stock = 1"
	rows = frappe.db.sql(
		f"""
		SELECT ii.parent, ii.name, ii.idx, ii.item_code, ii.warehouse, ii.qty
		FROM `tab{item_table}` ii
		JOIN `tab{doctype}` p ON p.name = ii.parent
		JOIN `tabItem` i ON i.name = ii.item_code
		WHERE ii.parent IN %(names)s AND p.docstatus = 1 {stock_filter}
			AND i.is_stock_item = 1 AND ii.qty != 0
		""",
		{"names": names},
		as_dict=True,
	)
	if not rows:
		return {}

	posted = {
		(r.voucher_no, r.voucher_detail_no)
		for r in frappe.db.sql(
			"""
			SELECT voucher_no, voucher_detail_no FROM `tabStock Ledger Entry`
			WHERE voucher_type = %(dt)s AND voucher_no IN %(names)s AND is_cancelled = 0
			""",
			{"dt": doctype, "names": names},
			as_dict=True,
		)
	}

	gaps = {}
	for r in rows:
		entry = gaps.setdefault(r.parent, {"expected": 0, "missing": []})
		entry["expected"] += 1
		if (r.parent, r.name) not in posted:
			entry["missing"].append(r)
	return {name: g for name, g in gaps.items() if g["missing"]}


def stock_ledger_gap(doc):
	"""{"expected", "missing"} for one submitted doc, or None when every stock
	row has an active Stock Ledger Entry."""
	if not _uses_stock(doc.doctype, doc.get("update_stock")):
		return None
	return stock_ledger_gaps(doc.doctype, [doc.name]).get(doc.name)


def describe_gap(missing, expected):
	items = sorted({r.item_code for r in missing})
	shown = ", ".join(items[:5]) + (f" +{len(items) - 5} more" if len(items) > 5 else "")
	return f"{len(missing)} of {expected} stock rows have no Stock Ledger Entry ({shown})"


def submit_checked(doc):
	"""Submit ``doc``. Sales that would take stock from a short (item, warehouse)
	are refused before submit. After submit, every stock row must have a Stock
	Ledger Entry; otherwise (or if submit raised) the document is rolled back to
	draft and the error is raised for the caller to log."""
	_refuse_if_short(doc)

	save_point = "ecom_submit"
	frappe.db.savepoint(save_point)
	try:
		doc.submit()
		gap = stock_ledger_gap(doc)
		if gap:
			raise NoStockLedgerError(f"{NO_STOCK_LEDGER}: {describe_gap(gap['missing'], gap['expected'])}")
	except Exception:
		_roll_back_submit(doc, save_point)
		raise
	try:
		frappe.db.release_savepoint(save_point)
	except Exception:
		pass
	return doc


def _roll_back_submit(doc, save_point):
	try:
		frappe.db.rollback(save_point=save_point)
	except Exception:
		# A deadlock or lock timeout ends the whole transaction (the savepoint
		# is gone). Callers commit after every document, so nothing else is lost.
		frappe.db.rollback()
	doc.docstatus = 0
	for child in doc.get_all_children():
		child.docstatus = 0
	try:
		frappe.clear_document_cache(doc.doctype, doc.name)
	except Exception:
		pass


# ---------------------------------------------------------------------------
# 2. One posting time per document
# ---------------------------------------------------------------------------

class PostingClock:
	"""Hands out distinct posting datetimes for one run.

	Each document keeps its platform date; its time is the platform time, moved
	on by whole seconds until it is free. Times already taken by submitted
	stock vouchers on that date count as used, so a second run on the same day
	carries on after the first. The date never changes: at 23:59:59 the clock
	stops moving rather than spill into the next day.
	"""

	def __init__(self):
		self.used = set()
		self._seeded = set()

	def next(self, dt):
		if not dt:
			return dt
		if not isinstance(dt, datetime):
			dt = datetime.combine(getdate(dt), time.min)
		dt = dt.replace(microsecond=0, tzinfo=None)
		self._seed(dt.date())
		day = dt.date()
		while dt in self.used:
			nxt = dt + timedelta(seconds=1)
			if nxt.date() != day:
				break
			dt = nxt
		self.used.add(dt)
		return dt

	def _seed(self, day):
		if day in self._seeded:
			return
		self._seeded.add(day)
		for doctype in _ITEM_TABLE:
			for t in frappe.get_all(
				doctype,
				filters={"posting_date": day, "docstatus": 1},
				pluck="posting_time",
			):
				if t is not None:
					self.used.add(datetime.combine(day, get_time(t)).replace(microsecond=0))


def distinct_posting_dt(dt):
	"""The run's next free posting datetime for ``dt`` (unchanged outside a run)."""
	clock = frappe.flags.get(_FLAG_CLOCK)
	return clock.next(dt) if clock else dt


def stamp_posting(doc, dt):
	"""Set ``doc``'s posting date/time to the run's next free slot for ``dt``."""
	dt = distinct_posting_dt(dt)
	if not dt:
		return doc
	if not isinstance(dt, datetime):
		dt = datetime.combine(getdate(dt), time.min)
	doc.set_posting_time = 1
	doc.posting_date = dt.date()
	doc.posting_time = dt.time().strftime("%H:%M:%S")
	return doc


# ---------------------------------------------------------------------------
# 3. Stock before sales
# ---------------------------------------------------------------------------

def stock_shortfall(needs, at_dt):
	"""Compare ``needs`` {(item_code, warehouse): qty} with stock at ``at_dt``.

	Returns rows {item_code, warehouse, need, have} where need > have, for stock
	items only. ``allow_negative_stock`` plays no part."""
	if not needs or not at_dt:
		return []
	from erpnext.stock.utils import get_stock_balance

	if not isinstance(at_dt, datetime):
		at_dt = datetime.combine(getdate(at_dt), time.min)
	items = {item for item, _wh in needs}
	stock_items = set(
		frappe.get_all("Item", filters={"name": ["in", list(items)], "is_stock_item": 1}, pluck="name")
	)
	out = []
	for (item_code, warehouse), need in sorted(needs.items()):
		if item_code not in stock_items or not warehouse or flt(need) <= 0:
			continue
		have = flt(get_stock_balance(item_code, warehouse, at_dt.date(), at_dt.time()))
		if flt(need) > have + 1e-6:
			out.append({"item_code": item_code, "warehouse": warehouse, "need": flt(need), "have": have})
	return out


def _qty(value):
	value = flt(value)
	return f"{value:,.0f}" if value == int(value) else f"{value:,.2f}"


def _warehouse_label(warehouse):
	return frappe.db.get_value("Warehouse", warehouse, "warehouse_name") or warehouse


def format_shortfall(row):
	"""'AL112 @ Ecommerce Out: need 7,536, have 1,749'"""
	return (
		f"{row['item_code']} @ {_warehouse_label(row['warehouse'])}: "
		f"need {_qty(row['need'])}, have {_qty(row['have'])}"
	)


def shortfall_html(rows, at_dt):
	"""Preview block for the stock check."""
	when = frappe.utils.format_datetime(at_dt) if at_dt else ""
	if not rows:
		return (
			"<div class='alert alert-success' style='font-size:12px;'>"
			f"Stock check: every stock item in this file is in stock at {frappe.utils.escape_html(when)}.</div>"
		)
	lines = "".join(
		f"<li>{frappe.utils.escape_html(format_shortfall(r))}</li>" for r in rows
	)
	return (
		"<div class='alert alert-danger' style='font-size:12px;'>"
		f"<b>Stock shortfall at {frappe.utils.escape_html(when)}</b> (the batch's first sale). "
		"Sales of these items from these warehouses will not be submitted until the stock is booked:"
		f"<ul style='margin:6px 0 0 0;'>{lines}</ul></div>"
	)


def _refuse_if_short(doc):
	short = frappe.flags.get(_FLAG_SHORT)
	if not short or doc.doctype not in _OUTGOING or cint(doc.get("is_return")):
		return
	if not _uses_stock(doc.doctype, doc.get("update_stock")):
		return
	hits = []
	for item in doc.get("items") or []:
		row = short.get((item.item_code, item.warehouse))
		if row and row not in hits:
			hits.append(row)
	if hits:
		raise StockShortfallError(f"{STOCK_SHORTFALL}: " + "; ".join(format_shortfall(r) for r in hits))


def add_need(needs, item_code, warehouse, qty):
	if item_code and warehouse and flt(qty) > 0:
		key = (item_code, warehouse)
		needs[key] = flt(needs.get(key)) + flt(qty)


def earliest(current, dt):
	if not dt:
		return current
	if not isinstance(dt, datetime):
		if isinstance(dt, date):
			dt = datetime.combine(dt, time.min)
		else:
			return current
	return dt if current is None or dt < current else current
