// Copyright (c) 2025, Sagar Ratan Garg and contributors
// For license information, please see license.txt

frappe.ui.form.on("Ecommerce Bill Import", {
	refresh(frm) {
		
	
		// if(frm.doc.status=="Pending" || frm.doc.status=="Partial Success" || frm.doc.status=="Error" ){
		// Add import button if file is uploaded
			frm.add_custom_button(__("Start Import"), function() {
				frm.call({
					method: "create_invoice",
					doc: frm.doc,
					callback: function(r) {
					frm.refresh()
					}
				});
			}).addClass("btn-primary");

		// }
		if(frm.doc.ecommerce_mapping){
		frappe.model.get_value('Ecommerce Mapping', {'name': frm.doc.ecommerce_mapping}, 'platform', function(value) {
			if(value.platform=="Amazon"){
				frm.set_df_property("cred_attach", "hidden", 1);
				frm.set_df_property("cred", "hidden", 1);
				frm.set_df_property("cred_items", "hidden", 1);
				frm.set_df_property("amazon_type", "hidden", 0);
				frm.set_df_property("flipkart_attach", "hidden", 1);
				frm.set_df_property("flipkart_items", "hidden", 1);
				frm.set_df_property("jio_mart_attach", "hidden", 1);
				frm.set_df_property("jio_mart_items", "hidden", 1);
			}
			else if(value.platform=="CRED"){
				frm.set_df_property("cred_attach", "hidden", 0);
				frm.set_df_property("cred", "hidden", 0);
				frm.set_df_property("cred_items", "hidden", 0);
				frm.set_df_property("amazon_type", "hidden", 1);
				frm.set_df_property("flipkart_attach", "hidden", 1);
				frm.set_df_property("flipkart_items", "hidden", 1);
				frm.set_df_property("jio_mart_attach", "hidden", 1);
				frm.set_df_property("jio_mart_items", "hidden", 1);
				

			}
			else if(value.platform=="Flipkart"){
				frm.set_df_property("cred_attach", "hidden", 1);
				frm.set_df_property("cred", "hidden", 1);
				frm.set_df_property("cred_items", "hidden", 1);
				frm.set_df_property("amazon_type", "hidden", 1);
				frm.set_df_property("flipkart_attach", "hidden", 0);
				frm.set_df_property("flipkart_items", "hidden", 0);
				frm.set_df_property("jio_mart_attach", "hidden", 1);
				frm.set_df_property("jio_mart_items", "hidden", 1);
				
			}
			else if(value.platform=="Jiomart"){
				frm.set_df_property("cred_attach", "hidden", 1);
				frm.set_df_property("cred", "hidden", 1);
				frm.set_df_property("cred_items", "hidden", 1);
				frm.set_df_property("amazon_type", "hidden", 1);
				frm.set_df_property("flipkart_attach", "hidden", 1);
				frm.set_df_property("flipkart_items", "hidden", 1);
				frm.set_df_property("jio_mart_attach", "hidden", 0);
				frm.set_df_property("jio_mart_items", "hidden", 0);
				
			}

		})
	}
		
		
		// Show import log if available
		if (frm.doc.status && frm.doc.status !== "Pending") {
			frm.trigger("show_import_log");
		}
		if (frm.doc.error_json) {
			let rawStr = frm.doc.error_json;
			let error;
			try {
				error = JSON.parse(rawStr);
			} catch (e) {
				console.error("JSON parsing failed:", e);
				frappe.msgprint("Error parsing error_json. Please check format.");
				return;
			}
		
			let html = `<table class="table table-bordered">
				<thead><tr>
					<th>Idx</th>
					<th>Invoice ID</th>
					<th>Event</th>
					<th>Error Message</th>
				</tr></thead><tbody>`;
		
			error.forEach(row => {
				html += `<tr>
					<td>${row.idx ?? ''}</td>
					<td>${row.invoice_id}</td>
					<td>${row.event}</td>
					<td style="color:red;">${frappe.utils.escape_html(row.message)}</td>
				</tr>`;
			});
		
			html += `</tbody></table>`;
			frm.fields_dict.error_html.$wrapper.html(html);
			frm.refresh_field("error_html");
		}

	},
	
	
	ecommerce_mapping:function(frm){
		frappe.model.get_value('Ecommerce Mapping', {'name': frm.doc.ecommerce_mapping}, 'platform', function(value) {
			if(value.platform=="Amazon"){
				frm.set_df_property("cred_attach", "hidden", 1);
				frm.set_df_property("cred", "hidden", 1);
				frm.set_df_property("cred_items", "hidden", 1);
				frm.set_df_property("amazon_type", "hidden", 0);
				frm.set_df_property("flipkart_attach", "hidden", 1);
				frm.set_df_property("flipkart_items", "hidden", 1);
				frm.set_df_property("jio_mart_attach", "hidden", 1);
				frm.set_df_property("jio_mart_items", "hidden", 1);
			}
			else if(value.platform=="CRED"){
				frm.set_df_property("cred_attach", "hidden", 0);
				frm.set_df_property("cred", "hidden", 0);
				frm.set_df_property("cred_items", "hidden", 0);
				frm.set_df_property("amazon_type", "hidden", 1);
				frm.set_df_property("flipkart_attach", "hidden", 1);
				frm.set_df_property("flipkart_items", "hidden", 1);
				frm.set_df_property("jio_mart_attach", "hidden", 1);
				frm.set_df_property("jio_mart_items", "hidden", 1);
				frm.set_value("amazon_type","")
				frm.refresh_field("amazon_type")
			}
			else if(value.platform=="Flipkart"){
				frm.set_df_property("cred_attach", "hidden", 1);
				frm.set_df_property("cred", "hidden", 1);
				frm.set_df_property("cred_items", "hidden", 1);
				frm.set_df_property("amazon_type", "hidden", 1);
				frm.set_df_property("flipkart_attach", "hidden", 0);
				frm.set_df_property("flipkart_items", "hidden", 0);
				frm.set_df_property("jio_mart_attach", "hidden", 1);
				frm.set_df_property("jio_mart_items", "hidden", 1);
				frm.set_value("amazon_type","")
				frm.refresh_field("amazon_type")
			}
			else if(value.platform=="Jiomart"){
				frm.set_df_property("cred_attach", "hidden", 1);
				frm.set_df_property("cred", "hidden", 1);
				frm.set_df_property("cred_items", "hidden", 1);
				frm.set_df_property("amazon_type", "hidden", 1);
				frm.set_df_property("flipkart_attach", "hidden", 1);
				frm.set_df_property("flipkart_items", "hidden", 1);
				frm.set_df_property("jio_mart_attach", "hidden", 0);
				frm.set_df_property("jio_mart_items", "hidden", 0);
				frm.set_value("amazon_type","")
				frm.refresh_field("amazon_type")
			}

		})
	},
	show_import_log: function(frm) {
		// Show import log
		$(frm.fields_dict.import_log_preview.wrapper).empty();
		
		let status_color = {
			"Success": "green",
			"Partial Success": "blue",
			"Error": "red",
			"Timed Out": "orange"
		};
		
		let status_icon = {
			"Success": "fa fa-check",
			"Partial Success": "fa fa-exclamation",
			"Error": "fa fa-times",
			"Timed Out": "fa fa-clock-o"
		};
		
		let html = `
			<div class="import-log">
				<div class="alert alert-${status_color[frm.doc.status] || 'blue'}">
					<i class="${status_icon[frm.doc.status] || 'fa fa-info'}"></i>
					Import Status: <strong>${frm.doc.status}</strong>
				</div>
		`;
		
		if (frm.doc.payload_count) {
			html += `<p>Processed ${frm.doc.payload_count} records</p>`;
		}
		
		html += `
				<p>To view detailed log, please check the Error Log or Import Log report.</p>
			</div>
		`;
		
		$(frm.fields_dict.import_log_preview.wrapper).html(html);
	}
});


frappe.realtime.on("data_import_progress", (data) => {
    frappe.show_progress("Invoice Creation", data.progress, 100, data.message);

    if (data.progress === 100) {
        frappe.hide_progress();
    }
});