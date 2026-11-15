# Copyright (c) 2026, Sagar Ratan Garg and Contributors
# See license.txt

"""Stock safety of the import run (utils/stock_guard.py):

* distinct posting times for documents that share one platform timestamp,
* a stock invoice whose Stock Ledger Entries are missing after submit is
  reported as failed (and rolled back to draft), never as a success,
* the stock-before-sales preview flags an item whose batch quantity exceeds
  the stock at the batch's first sale.
"""

from datetime import datetime
from unittest.mock import patch

import frappe
from frappe.tests.utils import FrappeTestCase
from frappe.utils import add_days, flt, nowdate

from ecom_import_tool.ecom_import_tool.utils import stock_guard
from ecom_import_tool.ecom_import_tool.utils.amazon_si import (
	_amazon_init_si_header,
	_amazon_save_and_submit,
)

COMPANY = "Test Company"
TAG = "_EIT SG"  # unique prefix so parallel suites on the site don't collide


def _abbr():
	return frappe.get_cached_value("Company", COMPANY, "abbr")


def _warehouse():
	name = f"{TAG} Ecommerce Out - {_abbr()}"
	if not frappe.db.exists("Warehouse", name):
		frappe.get_doc({
			"doctype": "Warehouse",
			"warehouse_name": f"{TAG} Ecommerce Out",
			"company": COMPANY,
		}).insert(ignore_permissions=True)
	return name


def _item(code):
	if not frappe.db.exists("Item", code):
		frappe.get_doc({
			"doctype": "Item",
			"item_code": code,
			"item_name": code,
			"item_group": frappe.db.get_value("Item Group", {"is_group": 0}, "name") or "All Item Groups",
			"stock_uom": "Nos",
			"is_stock_item": 1,
			"gst_hsn_code": frappe.db.get_value("GST HSN Code", {}, "name"),
		}).insert(ignore_permissions=True)
	return code


def _customer():
	name = f"{TAG} Buyer"
	if not frappe.db.exists("Customer", name):
		frappe.get_doc({
			"doctype": "Customer",
			"customer_name": name,
			"customer_group": frappe.db.get_value("Customer Group", {"is_group": 0}, "name"),
			"territory": frappe.db.get_value("Territory", {"is_group": 0}, "name"),
		}).insert(ignore_permissions=True)
	return name


def _receive(item_code, warehouse, qty, posting_date, rate=100):
	"""Add ``qty`` to the bin with a Stock Reconciliation (this site's
	warehousesuite blocks Material Receipts that change stock value)."""
	from erpnext.stock.utils import get_stock_balance

	current = flt(get_stock_balance(item_code, warehouse, posting_date, "00:00:00"))
	sr = frappe.get_doc({
		"doctype": "Stock Reconciliation",
		"purpose": "Stock Reconciliation",
		"company": COMPANY,
		"set_posting_time": 1,
		"posting_date": posting_date,
		"posting_time": "00:00:00",
		"items": [{
			"item_code": item_code,
			"warehouse": warehouse,
			"qty": current + qty,
			"valuation_rate": rate,
		}],
	})
	sr.insert(ignore_permissions=True)
	sr.submit()
	return sr


class TestDistinctPostingTimes(FrappeTestCase):
	def tearDown(self):
		stock_guard.end_run()

	def _header(self, dt):
		return _amazon_init_si_header(
			customer="x", posting_dt=dt, ecom_name=f"{TAG}-{frappe.generate_hash(length=6)}",
			is_return=False, is_debit_note=False, return_against=None,
			ecommerce_operator="Flipkart", amazon_type="", ecommerce_gstin="", update_stock=1,
		)

	def test_three_invoices_in_one_row_group_get_three_times(self):
		"""Flipkart stamps every invoice of a day at 00:00:00; within one run each
		gets its own second, in file order, on the platform's date."""
		stock_guard.start_run()
		midnight = datetime(2031, 10, 10, 0, 0, 0)  # a date with no submitted vouchers
		sis = [self._header(midnight) for _ in range(3)]

		self.assertEqual([str(s.posting_time) for s in sis], ["00:00:00", "00:00:01", "00:00:02"])
		self.assertTrue(all(str(s.posting_date) == "2031-10-10" for s in sis))
		self.assertTrue(all(s.set_posting_time for s in sis))

	def test_clock_keeps_the_platform_date(self):
		clock = stock_guard.PostingClock()
		late = datetime(2031, 10, 11, 23, 59, 59)
		self.assertEqual(clock.next(late), late)
		self.assertEqual(clock.next(late).date(), late.date())

	def test_outside_a_run_times_are_unchanged(self):
		dt = datetime(2031, 10, 12, 10, 50, 38)
		self.assertEqual(stock_guard.distinct_posting_dt(dt), dt)


def _stub_other_apps_location_rule():
	"""location_based_series (installed on the dev site, not on every site)
	demands a fully set-up Location on each invoice; that is not what these
	tests are about, so its validation is stubbed while they run."""
	try:
		import location_based_series.events.validation  # noqa: F401
	except ImportError:
		return None
	p = patch("location_based_series.events.validation.validate_doc", lambda *a, **k: None)
	p.start()
	return p


class TestStockLedgerAfterSubmit(FrappeTestCase):
	@classmethod
	def setUpClass(cls):
		super().setUpClass()
		cls._lbs_patch = _stub_other_apps_location_rule()
		cls.warehouse = _warehouse()
		cls.item = _item(f"{TAG} AL112")
		cls.customer = _customer()
		_receive(cls.item, cls.warehouse, 10, add_days(nowdate(), -2))

	@classmethod
	def tearDownClass(cls):
		if cls._lbs_patch:
			cls._lbs_patch.stop()
		super().tearDownClass()

	def tearDown(self):
		stock_guard.end_run()

	def _si(self):
		si = frappe.new_doc("Sales Invoice")
		si.company = COMPANY
		si.customer = self.customer
		si.set_posting_time = 1
		si.posting_date = nowdate()
		si.posting_time = "10:00:00"
		si.update_stock = 1
		si.set_warehouse = self.warehouse
		si.append("items", {"item_code": self.item, "qty": 2, "rate": 150, "warehouse": self.warehouse})
		return si

	def test_ok_submit_keeps_stock_ledger(self):
		si = _amazon_save_and_submit(self._si(), mode_of_payment=None)
		self.assertEqual(si.docstatus, 1)
		self.assertIsNone(stock_guard.stock_ledger_gap(si))

	def test_missing_sle_after_submit_is_a_failure(self):
		"""update_stock_ledger silently writes nothing: the submit must not count
		as a success. The invoice goes back to draft with no GL left behind."""
		si = self._si()
		from erpnext.accounts.doctype.sales_invoice.sales_invoice import SalesInvoice

		with patch.object(SalesInvoice, "update_stock_ledger", lambda self, *a, **k: None):
			with self.assertRaises(stock_guard.NoStockLedgerError) as ctx:
				_amazon_save_and_submit(si, mode_of_payment=None)

		self.assertTrue(str(ctx.exception).startswith("Submit failed: no stock ledger"))
		self.assertEqual(frappe.db.get_value("Sales Invoice", si.name, "docstatus"), 0)
		self.assertEqual(si.docstatus, 0)
		self.assertFalse(frappe.db.count("GL Entry", {"voucher_no": si.name, "is_cancelled": 0}))

	def test_reconcile_view_shows_old_gap(self):
		"""A past import whose SLE is gone shows up in the reconcile view."""
		from ecom_import_tool.ecom_import_tool.utils.reconcile import _mark_stock_ledger_gaps

		si = _amazon_save_and_submit(self._si(), mode_of_payment=None)
		frappe.db.delete("Stock Ledger Entry", {"voucher_no": si.name})
		rows = [{"si_name": si.name, "si_status": "Submitted", "match": True}]
		_mark_stock_ledger_gaps(rows)
		self.assertFalse(rows[0]["match"])
		self.assertIn("1 of 1 stock rows have no Stock Ledger Entry", rows[0]["stock_ledger"])

	def test_sale_from_short_warehouse_is_not_submitted(self):
		stock_guard.start_run([{"item_code": self.item, "warehouse": self.warehouse, "need": 50, "have": 10}])
		si = self._si()
		with self.assertRaises(stock_guard.StockShortfallError) as ctx:
			_amazon_save_and_submit(si, mode_of_payment=None)
		self.assertIn("need 50, have 10", str(ctx.exception))
		self.assertEqual(frappe.db.get_value("Sales Invoice", si.name, "docstatus"), 0)


class TestStockShortfallPreview(FrappeTestCase):
	@classmethod
	def setUpClass(cls):
		super().setUpClass()
		cls.warehouse = _warehouse()
		cls.short_item = _item(f"{TAG} SHORT")
		cls.ok_item = _item(f"{TAG} OK")
		start = add_days(nowdate(), -5)
		_receive(cls.short_item, cls.warehouse, 1749, start)
		_receive(cls.ok_item, cls.warehouse, 100, start)
		# Stock that arrives after the batch's first sale does not count.
		_receive(cls.short_item, cls.warehouse, 10000, add_days(nowdate(), -1))
		cls.batch_start = datetime.combine(frappe.utils.getdate(add_days(nowdate(), -3)), datetime.min.time())

	def test_batch_quantity_above_stock_is_flagged(self):
		needs = {}
		for _ in range(4):
			stock_guard.add_need(needs, self.short_item, self.warehouse, 1884)  # 7,536 in all
		stock_guard.add_need(needs, self.ok_item, self.warehouse, 40)

		rows = stock_guard.stock_shortfall(needs, self.batch_start)

		self.assertEqual([r["item_code"] for r in rows], [self.short_item])
		self.assertEqual(flt(rows[0]["need"]), 7536)
		self.assertEqual(flt(rows[0]["have"]), 1749)
		self.assertEqual(
			stock_guard.format_shortfall(rows[0]),
			f"{self.short_item} @ {TAG} Ecommerce Out: need 7,536, have 1,749",
		)
		html = stock_guard.shortfall_html(rows, self.batch_start)
		self.assertIn("Stock shortfall", html)
		self.assertIn("need 7,536, have 1,749", html)

	def test_batch_within_stock_passes(self):
		needs = {}
		stock_guard.add_need(needs, self.ok_item, self.warehouse, 100)
		self.assertEqual(stock_guard.stock_shortfall(needs, self.batch_start), [])
		self.assertIn("in stock", stock_guard.shortfall_html([], self.batch_start))
