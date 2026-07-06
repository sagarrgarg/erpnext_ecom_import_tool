import frappe


def execute():
	"""Drop the legacy autoname='prompt' Property Setters for Sales Invoice /
	Delivery Note / Purchase Invoice / Purchase Receipt.

	Naming of ecom-imported docs is pinned by `_force_ecom_name` in override.py
	(sets doc.name + doc.flags.name_set = True in before_insert), which makes
	Frappe skip set_new_name entirely — so `prompt` was never needed for the
	import to name docs correctly.

	Leaving `prompt` in place forced every *manual* SI/DN/PI/PR to prompt for a
	name and, worse, overrode location_based_series' location-based naming
	series (its meta autoname / autoname doc_event) because a DocType-level
	Property Setter wins over the DocType's own autoname at meta-build time.

	The app source no longer ships these setters, but sync_customizations only
	inserts/updates Property Setters and never deletes, so already-migrated
	sites keep the stale DB records — this patch removes them. Deleting the
	Property Setter is enough: it is applied at meta-build time and does not
	touch the DocType row, so clearing the cache restores standard /
	location_based_series naming for manual docs.
	"""
	for doctype in ("Sales Invoice", "Delivery Note", "Purchase Invoice", "Purchase Receipt"):
		ps_name = f"{doctype}-main-autoname"
		value = frappe.db.get_value("Property Setter", ps_name, "value")
		# Guard on value so we never clobber a non-prompt autoname another app set.
		if value and value.strip().lower() == "prompt":
			frappe.delete_doc("Property Setter", ps_name, ignore_permissions=True)

	frappe.clear_cache()
