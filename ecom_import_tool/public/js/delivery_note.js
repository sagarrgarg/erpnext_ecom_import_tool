frappe.ui.form.on("Delivery Note", {
    refresh: function(frm) {
        toggleNameField(frm);
    },
    setup: function(frm) {
        toggleNameField(frm);
    }
});

function toggleNameField(frm) {
    // Find label whose text is exactly "Name"
    $('label.control-label').filter(function() {
        return $(this).text().trim() === "Name";
    }).closest('.form-group')
      .toggle(!!frm.doc.custom_ecommerce_operator);
}
