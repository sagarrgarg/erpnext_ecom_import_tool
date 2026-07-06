# Copyright (c) 2025, Sagar Ratan Garg and Contributors
# See license.txt

from frappe.tests.utils import FrappeTestCase

from ecom_import_tool.ecom_import_tool.doctype.ecommerce_bill_import.ecommerce_bill_import import (
	purchase_ecom_name,
	safe_refund_qty_rate,
)


class TestEcommerceBillImport(FrappeTestCase):
	pass


class TestSafeRefundQtyRate(FrappeTestCase):
	"""Tests for the safe_refund_qty_rate helper used in Amazon refund imports.

	When is_zero_qty is True the caller creates a Debit Note (is_debit_note=1,
	qty=0) or, in mixed groups, falls back to qty=-1 on the zero rows.
	"""

	def test_positive_qty_returns_normal_values(self):
		qty, rate, is_zero_qty = safe_refund_qty_rate(3, 300)
		self.assertEqual(qty, -3)
		self.assertAlmostEqual(rate, 100.0)
		self.assertFalse(is_zero_qty)

	def test_negative_qty_uses_absolute(self):
		qty, rate, is_zero_qty = safe_refund_qty_rate(-2, -500)
		self.assertEqual(qty, -2)
		self.assertAlmostEqual(rate, 250.0)
		self.assertFalse(is_zero_qty)

	def test_zero_qty_returns_zero(self):
		"""Zero qty should return qty=0 so caller can create a debit note."""
		qty, rate, is_zero_qty = safe_refund_qty_rate(0, 150)
		self.assertEqual(qty, 0)
		self.assertAlmostEqual(rate, 150.0)
		self.assertTrue(is_zero_qty)

	def test_blank_string_qty_returns_zero(self):
		qty, rate, is_zero_qty = safe_refund_qty_rate("", 200)
		self.assertEqual(qty, 0)
		self.assertAlmostEqual(rate, 200.0)
		self.assertTrue(is_zero_qty)

	def test_none_qty_returns_zero(self):
		qty, rate, is_zero_qty = safe_refund_qty_rate(None, 99.50)
		self.assertEqual(qty, 0)
		self.assertAlmostEqual(rate, 99.50)
		self.assertTrue(is_zero_qty)

	def test_nan_string_qty_returns_zero(self):
		"""flt('nan') returns float('nan') which is truthy; helper must detect it."""
		qty, rate, is_zero_qty = safe_refund_qty_rate("nan", 400)
		self.assertEqual(qty, 0)
		self.assertAlmostEqual(rate, 400.0)
		self.assertTrue(is_zero_qty)

	def test_negative_amount_with_zero_qty(self):
		qty, rate, is_zero_qty = safe_refund_qty_rate(0, -350)
		self.assertEqual(qty, 0)
		self.assertAlmostEqual(rate, 350.0)
		self.assertTrue(is_zero_qty)

	def test_both_zero(self):
		qty, rate, is_zero_qty = safe_refund_qty_rate(0, 0)
		self.assertEqual(qty, 0)
		self.assertAlmostEqual(rate, 0.0)
		self.assertTrue(is_zero_qty)

	def test_string_qty_normal(self):
		qty, rate, is_zero_qty = safe_refund_qty_rate("5", "1000")
		self.assertEqual(qty, -5)
		self.assertAlmostEqual(rate, 200.0)
		self.assertFalse(is_zero_qty)

	def test_fractional_qty(self):
		qty, rate, is_zero_qty = safe_refund_qty_rate(0.5, 100)
		self.assertEqual(qty, -0.5)
		self.assertAlmostEqual(rate, 200.0)
		self.assertFalse(is_zero_qty)

	def test_debit_note_scenario_all_zero(self):
		"""Simulate pre-scan: all rows zero qty -> all_zero_qty should be True."""
		rows = [(0, 100), ("", 200), (None, 50)]
		all_zero = all(safe_refund_qty_rate(q, a)[2] for q, a in rows)
		self.assertTrue(all_zero)

	def test_mixed_scenario(self):
		"""Simulate pre-scan: mixed rows -> all_zero_qty should be False."""
		rows = [(2, 100), (0, 200)]
		all_zero = all(safe_refund_qty_rate(q, a)[2] for q, a in rows)
		self.assertFalse(all_zero)

	def test_all_normal_scenario(self):
		"""Simulate pre-scan: all rows have qty -> all_zero_qty should be False."""
		rows = [(3, 300), (1, 100)]
		all_zero = all(safe_refund_qty_rate(q, a)[2] for q, a in rows)
		self.assertFalse(all_zero)


class TestPurchaseEcomName(FrappeTestCase):
	"""Tests for purchase_ecom_name — the inter-company stock-transfer purchase
	leg (PI/PR) must NOT share a docname with the sales leg (SI/DN), otherwise
	both post to the GL under the same voucher_no and can't be told apart.
	"""

	def test_taxable_gets_pi_prefix(self):
		self.assertEqual(purchase_ecom_name("26-DEL5-2", True), "PI-26-DEL5-2")

	def test_non_taxable_gets_pr_prefix(self):
		self.assertEqual(purchase_ecom_name("26-DEL5-2", False), "PR-26-DEL5-2")

	def test_never_equals_sales_name(self):
		sales_name = "26-DEL5-2"
		self.assertNotEqual(purchase_ecom_name(sales_name, True), sales_name)
		self.assertNotEqual(purchase_ecom_name(sales_name, False), sales_name)

	def test_idempotent_pi(self):
		self.assertEqual(purchase_ecom_name("PI-26-DEL5-2", True), "PI-26-DEL5-2")

	def test_idempotent_pr(self):
		self.assertEqual(purchase_ecom_name("PR-26-DEL5-2", False), "PR-26-DEL5-2")

	def test_blank_passthrough(self):
		self.assertEqual(purchase_ecom_name("", True), "")

	def test_none_passthrough(self):
		self.assertIsNone(purchase_ecom_name(None, False))
