// Copyright (c) 2025, Sagar Ratan Garg and contributors
// For license information, please see license.txt

// frappe.ui.form.on("Ecommerce Bill Import", {
// 	refresh(frm) {

// 	},
// });

frappe.ui.form.on("Ecommerce Bill Import", {
	refresh(frm) {
		// Add import button if file is uploaded
			frm.add_custom_button(__("Start Import"), function() {
				frm.call({
					method: "create_invoice",
					doc: frm.doc,
					callback: function(r) {
						if (r.message) {
							frappe.show_alert({
								message: __("Import started successfully"),
								indicator: "green"
							});
							frm.reload_doc();
						} else {
							frappe.show_alert({
								message: __("Import failed. Please check the logs."),
								indicator: "red"
							});
						}
					}
				});
			}).addClass("btn-primary");
		
		
		// Show preview when file is uploaded
		if (frm.doc.import_file) {
			frm.trigger("show_preview");
		}
		
		// Show import log if available
		if (frm.doc.status && frm.doc.status !== "Pending") {
			frm.trigger("show_import_log");
		}
	},
	
	// ecommerce_mapping: function(frm) {
	// 	if (frm.doc.ecommerce_mapping) {
	// 		frappe.call({
	// 			method: "frappe.client.get",
	// 			args: {
	// 				doctype: "Ecommerce Mapping",
	// 				name: frm.doc.ecommerce_mapping
	// 			},
	// 			callback: function(r) {
	// 				if (r.message) {
	// 					// Check if the platform is Amazon
	// 					if (r.message.platform === "Amazon") {
	// 						frm.set_df_property("amazon_type", "hidden", 0);
	// 						frm.set_df_property("amazon_type", "reqd", 1);
	// 					} else {
	// 						frm.set_df_property("amazon_type", "hidden", 1);
	// 						frm.set_df_property("amazon_type", "reqd", 0);
	// 						frm.set_value("amazon_type", "");
	// 					}
	// 				}
	// 			}
	// 		});
	// 	} else {
	// 		frm.set_df_property("amazon_type", "hidden", 1);
	// 		frm.set_df_property("amazon_type", "reqd", 0);
	// 		frm.set_value("amazon_type", "");
	// 	}
	// },
	
	show_preview: function(frm) {
		// Show a preview of the uploaded file
		$(frm.fields_dict.import_preview.wrapper).empty();
		
		frm.call({
			method: "frappe.client.get_value",
			args: {
				doctype: "File",
				filters: {
					file_url: frm.doc.import_file
				},
				fieldname: ["file_name", "file_url"]
			},
			callback: function(r) {
				if (r.message) {
					let file_name = r.message.file_name;
					let file_url = r.message.file_url;
					
					if (file_name.endsWith(".csv") || file_name.endsWith(".xlsx") || file_name.endsWith(".xls")) {
						$(frm.fields_dict.import_preview.wrapper).html(`
							<div class="table-responsive">
								<h5>File: ${file_name}</h5>
								<p>File is ready for import. Click "Start Import" to begin processing.</p>
								<p>Please make sure you have selected the correct Ecommerce Mapping and Amazon Type (if applicable).</p>
							</div>
						`);
					} else {
						$(frm.fields_dict.import_preview.wrapper).html(`
							<div class="alert alert-warning">
								Unsupported file type. Please upload a CSV or Excel file.
							</div>
						`);
					}
				}
			}
		});
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
