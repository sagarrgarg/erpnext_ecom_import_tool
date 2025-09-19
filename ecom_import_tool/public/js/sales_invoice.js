frappe.ui.form.on("Sales Invoice", {
    refresh: function(frm) {
        if (!frm.doc.custom_ecommerce_operator) {
            // Hide the field by label using jQuery
            $('label.control-label:contains("Name")')
                .closest('.form-group')
                .hide();
        } else {
            $('label.control-label:contains("Name")')
                .closest('.form-group')
                .show();
        }
    },
    setup: function(frm) {
        if (!frm.doc.custom_ecommerce_operator) {
            // Hide the field by label using jQuery
            $('label.control-label:contains("Name")')
                .closest('.form-group')
                .hide();
        } else {
            $('label.control-label:contains("Name")')
                .closest('.form-group')
                .show();
        }
    }
});