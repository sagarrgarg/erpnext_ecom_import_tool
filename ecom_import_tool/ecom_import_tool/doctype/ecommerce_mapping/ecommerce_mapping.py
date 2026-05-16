# Copyright (c) 2025, Sagar Ratan Garg and contributors
# For license information, please see license.txt

import frappe
from frappe.model.document import Document


class EcommerceMapping(Document):
	def validate(self):
		from frappe import _

		if not self.mode_of_payment:
			frappe.throw(_("Mode of Payment is mandatory."))

		dup = frappe.db.get_value(
			"Ecommerce Mapping",
			{"mode_of_payment": self.mode_of_payment, "name": ["!=", self.name]},
			"name",
		)
		if dup:
			frappe.throw(
				_(
					"Mode of Payment '{0}' is already used by Ecommerce Mapping '{1}'. "
					"Each mapping must have its own MoP for clean reconciliation."
				).format(self.mode_of_payment, dup)
			)

		mop = frappe.get_doc("Mode of Payment", self.mode_of_payment)
		has_account = any(getattr(r, "default_account", None) for r in (mop.accounts or []))
		if not has_account:
			frappe.throw(
				_(
					"Mode of Payment '{0}' has no Default Account configured. "
					"Open it and set Default Account for at least one company."
				).format(self.mode_of_payment)
			)
