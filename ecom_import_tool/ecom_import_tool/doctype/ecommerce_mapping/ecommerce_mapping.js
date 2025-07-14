// Copyright (c) 2025, Sagar Ratan Garg and contributors
// For license information, please see license.txt

frappe.ui.form.on('Ecommerce Mapping', {
    refresh: function(frm) {
        format_links(frm);
    }
});

frappe.ui.form.on('Ecommerce Item Mapping', {
    erp_item: function(frm, cdt, cdn) {
        format_links(frm);
    }
});

function format_links(frm) {
    setTimeout(() => {
        frm.fields_dict.ecom_item_table.grid.wrapper.find('.grid-row').each(function() {
            let row = $(this);
            let row_doc = row.data('doc');
            
            if (row_doc && row_doc.erp_item) {
                let link_field = row.find('[data-fieldname="erp_item"]');
                let link_code = row_doc.erp_item;
                let link_name = row_doc.item_name || ''; // Replace with actual name field
                
                // Format as "Item_code:item_name"
                let display_text = link_code + ':' + link_name;
                link_field.find('a').text(display_text);
            }
        });
    }, 100);
}