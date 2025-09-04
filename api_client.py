import os
import requests
import json
from datetime import datetime, timedelta
from urllib.parse import urlparse, urlencode, quote, quote
from config import (
    RZN1_BASE_URL, RZN1_ENDPOINTS, RZN1_GET_REPORTS_ENDPOINT, RZN1_WAREHOUSE,
    TEMP_DOWNLOAD_DIR, get_date_prefix
)
from logger_config import setup_logger, get_default_log_file

# Setup logger
logger = setup_logger('api_client', get_default_log_file('api_client'))
RZN1_WAREHOUSE = "up090_lko_mat"
class DualServiceAPIClient:
    def __init__(self, tokens):
        """
        Initialize API client for RZN1 service
        Args:
            tokens (dict): Dictionary with 'rzn1' token
        """
        self.tokens = tokens
        self.sessions = {}
        self.warehouse = RZN1_WAREHOUSE  # Set warehouse attribute
        
        # Create session for RZN1 service
        if 'rzn1' in tokens and tokens['rzn1']:
            session = requests.Session()
            # Use the dynamically generated token from authentication
            session.headers.update({
                'Authorization': tokens['rzn1'],  # Use the actual token from auth
                'warehouse': RZN1_WAREHOUSE
            })
            self.sessions['rzn1'] = session
            logger.info(f"Initialized RZN1 session with warehouse: {RZN1_WAREHOUSE}")
            logger.info(f"Using token: {tokens['rzn1'][:20]}...")  # Log first 20 chars for verification
        else:
            raise ValueError("RZN1 token is required")
    
    def _get_service_config(self, service):
        """Get configuration for specified service"""
        if service == 'rzn1':
            return RZN1_BASE_URL, RZN1_ENDPOINTS, RZN1_GET_REPORTS_ENDPOINT
        else:
            raise ValueError("Service must be 'rzn1'")
    
    def _get_yesterday_date(self):
        """
        Get yesterday's date in YYYY-MM-DD format
        """
        yesterday = datetime.now() - timedelta(days=1)
        return yesterday.strftime("%Y-%m-%d")
    
    def _get_report_params(self, report_type):
        """
        Get specific parameters for different report types matching the exact curl commands
        """
        yesterday = self._get_yesterday_date()
        
        # Warehouse list from the curl command
        warehouse_list = [
            "hr009_pla_ls1", "up096_bab_ls1", "up097_ali_ls1", "up098_sag_ls1", 
            "up099_ban_ls1", "up100_aso_ls1", "up101_ach_ls1", "up102_man_ls1", 
            "up103_shi_ls1", "up104_der_ls1", "up105_dos_ls1", "up106_miy_ls1", 
            "up107_jac_ls1", "up108_kum_ls1", "up109_rac_ls1", "up110_lal_hm1", 
            "up111_bel_hm1", "up112_gos_hm1", "hr007_rjv_ls1", "up061_kur_ls1", 
            "up083_fat_ls1", "up081_hat_ls1", "up070_tik_ls1", "up064_bha_ls1", 
            "up054_kur_ls1", "up087_maw_ls1", "up077_bik_ls1", "up076_hai_ls1", 
            "up073_gbx_ls1", "up069_mah_ls1", "up044_jas_ls1", "up051_has_ls1", 
            "up067_jag_ls1", "up057_lam_ls1", "up079_bhi_ls1", "up080_tar_ls1", 
            "up082_mus_ls1", "up090_lko_mat", "up075_ran_ls1", "up043_gau_ls1"
        ]
        
        # Report-specific parameters matching exact curl commands
        if report_type == 'order_summary':
            return {
                'id': '100',
                'columns': [
                    "Cancelled Order Qty", "OrderStatus", "DispatchType", "WAC", "Created By", 
                    "Order Reference", "Order Date", "Cancelled By", "Cancelled By User", 
                    "Customer Name", "Customer GST", "Customer Phone Number", "Order Type", 
                    "Slot From", "Slot To", "SKU Desc", "SKU Code", "SKU Category", 
                    "SKU Sub Category", "SKU Weight", "SKU Brand", "Order Qty", "Mrp", 
                    "Unit Price", "Discount", "Customer Po Number", "OrderAmount(w/o-Tax)", 
                    "CGST", "IGST", "SGST", "CESS", "OrderTaxAmount", "TotalOrderAmount", 
                    "Shipping Taxable Amt", "ShippingTaxAmount", "Price", 
                    "Drop Ship PO Reference", "Currency", "Payment Mode", "Invoice Number", 
                    "Challan number", "UnfulfilledQuantity", "InvoiceAmount", 
                    "InvoiceAmount(w/o-tax)", "InvoiceTaxAmount", "InvoiceCGSTAmount", 
                    "InvoiceSGSTAmount", "InvoiceIGSTAmount", "InvoiceCESSAmount", 
                    "InvoiceDate", "PicklistConfirmationDate", "Margin Amt", 
                    "Invoice_quantity", "TotalProcurementPrice", "ProcurementPrice", 
                    "Picklist Details", "Order Fields", "ShippingAmount", "Order_Hold_Status"
                ],
                'Warehouse': warehouse_list,
                'From Date': yesterday,
                'To Date': '',
                'Order Reference': '',
                'Customer Name': '',
                'Order Type': '',
                'SKU Code': ''
            }
        elif report_type == 'sales_return':
            return {
                'id': '95',
                'columns': [
                    "Order Reference", "Order Date", "Customer Id", "Customer Name", 
                    "Customer Pincode", "Customer Country", "Invoice / Challan Number", 
                    "Invoice Date", "Return Id", "Reference Type", "Return Type", 
                    "Return Date", "Credit Note Date", "Credit Note Number", "Sku Code", 
                    "Sku Reference", "Sku Description", "Sku Category", "Sku Sub Category", 
                    "Sku Brand", "Weight", "Unit Price", "HSN Code", "Quantity", 
                    "CreditNoteAmount(w/o-tax)", "CGST", "SGST", "IGST", "CESS", 
                    "SGSTAmount", "IGSTAmount", "CESSAmount", "TaxPercentage", 
                    "CreditNoteTaxAmount", "TotalCreditNoteAmount", "Accepted User", 
                    "Customer State", "Customer GST Number", "GST Number", "Reason", 
                    "ExtraFields", "CGSTAmount"
                ],
                'Warehouse': warehouse_list,
                'Order Reference': '',
                'Customer Id': '',
                'Invoice / Challan Number': '',
                'Return Id': '',
                'Reference Type': '',
                'Credit Note Number': '',
                'Sku Code': '',
                'From Date': yesterday,
                'To Date': ''
            }
        elif report_type == 'batch_level_inventory':
            return {
                'id': '13',
                'columns': [
                    "SKU Code", "SKU Reference", "SKU Category", "SKU Sub Category", 
                    "SKU Brand", "Product Description", "SKU Class", "Status", 
                    "Batch No", "Manufactured Date", "Expiry Date", "MRP", "Price", 
                    "Vendor Batch No", "Restest Date", "Re-evaluation Date", 
                    "Best Before Date", "Inspection Lot Number", "Weight", 
                    "Batch Reference", "Zone", "Location", "Total Quantity", 
                    "Reserved_Quantity", "Available Quantity"
                ],
                'Warehouse': [RZN1_WAREHOUSE],  # Single warehouse for batch inventory
                'SKU Code': '',
                'SKU Category': '',
                'Zone': '',
                'Location': ''
            }
        elif report_type == 'open_order_summary':
            return {
                'id': '145',
                'columns': [
                    "Cancelled Order Qty", "OrderStatus", "Created By", "Order Reference", 
                    "Order Date", "Cancelled By", "Cancelled By User", "Customer Name", 
                    "Customer GST", "Customer Phone Number", "Order Type", "Slot From", 
                    "Slot To", "SKU Desc", "SKU Code", "SKU Category", "SKU Sub Category", 
                    "SKU Weight", "SKU Brand", "Order Qty", "Mrp", "Unit Price", 
                    "Customer Po Number", "Customer Reference", "Open Order quantity", 
                    "Allocation Details"
                ],
                'Warehouse': [RZN1_WAREHOUSE],  # Single warehouse for open orders
                'From Date': "2025-09-01",  # Use specific date like in curl
                'To Date': '',
                'Order Reference': '',
                'Customer Name': '',
                'Order Type': '',
                'SKU Code': ''
            }
        elif report_type == 'closing_stock':
            return {
                'id': '13',  # Same as batch level inventory but for all warehouses
                'columns': [
                    "SKU Code", "SKU Reference", "SKU Category", "SKU Sub Category", 
                    "SKU Brand", "Product Description", "SKU Class", "Status", 
                    "Batch No", "Manufactured Date", "Expiry Date", "MRP", "Price", 
                    "Vendor Batch No", "Restest Date", "Re-evaluation Date", 
                    "Best Before Date", "Inspection Lot Number", "Weight", 
                    "Batch Reference", "Zone", "Location", "Total Quantity", 
                    "Reserved_Quantity", "Available Quantity"
                ],
                'Warehouse': warehouse_list,  # All warehouses for closing stock
                'SKU Code': '',
                'SKU Category': '',
                'Zone': '',
                'Location': ''
            }
        else:
            # Default parameters for other report types
            return {
                'From Date': yesterday,
                'To Date': ''
            }
    
    def generate_report(self, service, report_type, custom_params=None):
        """
        Generate a report for the specified service and type
        Args:
            service (str): 'rzn1'
            report_type (str): Type of report to generate
            custom_params (dict): Optional custom parameters to override defaults
        Returns:
            bool: True if report generation was successful
        """
        base_url, endpoints, _ = self._get_service_config(service)
        
        if report_type not in endpoints:
            raise ValueError(f"Report type '{report_type}' not available for {service}")
        
        endpoint = endpoints[report_type]
        url = f"{base_url}{endpoint}"
        
        # Get default parameters for this report type
        params = self._get_report_params(report_type)
        
        # Override with custom parameters if provided
        if custom_params:
            params.update(custom_params)
        
        try:
            logger.info(f"Generating {report_type} report for {service.upper()}...")
            logger.info(f"Using URL: {url}")
            
            if report_type in ['order_summary', 'sales_return', 'closing_stock', 'batch_level_inventory', 'open_order_summary']:
                # Build the exact URL as in the curl command - arrays must be JSON strings
                # Use separators to remove spaces from JSON (to match curl format)
                columns_json = json.dumps(params['columns'], separators=(',', ':'))
                warehouse_json = json.dumps(params['Warehouse'], separators=(',', ':'))
                
                # Build parameters exactly as in curl
                url_params = {
                    'id': params['id'],
                    'columns': columns_json,
                    'Warehouse': warehouse_json,
                }
                
                # Add date parameters for order_summary and sales_return
                if report_type in ['order_summary', 'sales_return']:
                    url_params.update({
                        'From Date': params['From Date'],
                        'To Date': params.get('To Date', ''),
                    })
                
                # Add report-specific parameters
                if report_type == 'order_summary':
                    url_params.update({
                        'Order Reference': params.get('Order Reference', ''),
                        'Customer Name': params.get('Customer Name', ''),
                        'Order Type': params.get('Order Type', ''),
                        'SKU Code': params.get('SKU Code', '')
                    })
                elif report_type == 'sales_return':
                    url_params.update({
                        'Order Reference': params.get('Order Reference', ''),
                        'Customer Id': params.get('Customer Id', ''),
                        'Invoice / Challan Number': params.get('Invoice / Challan Number', ''),
                        'Return Id': params.get('Return Id', ''),
                        'Reference Type': params.get('Reference Type', ''),
                        'Credit Note Number': params.get('Credit Note Number', ''),
                        'Sku Code': params.get('Sku Code', '')
                    })
                elif report_type == 'closing_stock':
                    url_params.update({
                        'SKU Code': params.get('SKU Code', ''),
                        'SKU Category': params.get('SKU Category', ''),
                        'Zone': params.get('Zone', ''),
                        'Location': params.get('Location', '')
                    })
                elif report_type == 'batch_level_inventory':
                    url_params.update({
                        'SKU Code': params.get('SKU Code', ''),
                        'SKU Category': params.get('SKU Category', ''),
                        'Zone': params.get('Zone', ''),
                        'Location': params.get('Location', '')
                    })
                elif report_type == 'open_order_summary':
                    url_params.update({
                        'From Date': params['From Date'],
                        'To Date': params.get('To Date', ''),
                        'Order Reference': params.get('Order Reference', ''),
                        'Customer Name': params.get('Customer Name', ''),
                        'Order Type': params.get('Order Type', ''),
                        'SKU Code': params.get('SKU Code', '')
                    })
                
                # Build query string with proper encoding
                url_parts = []
                for key, value in url_params.items():
                    if value or value == '':  # Include empty strings as in curl
                        url_parts.append(f"{key}={quote(str(value))}")
                
                query_string = '&'.join(url_parts)
                full_url = f"{url}?{query_string}"
                
                logger.info(f"Making GET request to: {full_url[:200]}...")  # Log first 200 chars
                
                # Make GET request as per curl command
                response = self.sessions[service].get(full_url)
                
            else:
                # For other report types, use similar approach or adapt as needed
                logger.info("Using JSON POST approach for other report types")
                response = self.sessions[service].post(url, json=params)
            response.raise_for_status()
            
            logger.info(f"Successfully initiated {report_type} report generation for {service.upper()}")
            logger.info(f"Response status: {response.status_code}")
            
            # Log response if it's JSON
            try:
                response_data = response.json()
                logger.info(f"Response data: {response_data}")
            except:
                logger.info(f"Response text: {response.text[:500]}")  # First 500 chars
            
            return True
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error generating {report_type} report for {service}: {str(e)}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"Response status: {e.response.status_code}")
                logger.error(f"Response content: {e.response.text}")
            raise
    
    def get_available_reports(self, service):
        """
        Get list of available reports for the specified service
        Args:
            service (str): 'rzn1'
        Returns:
            list: List of available report links
        """
        base_url, _, reports_endpoint = self._get_service_config(service)
        base_url = base_url.rstrip('/')  # Remove trailing slash if present
        url = f"{base_url}{reports_endpoint}"
        
        try:
            logger.info(f"Fetching available reports for {service.upper()}...")
            response = self.sessions[service].get(url)
            response.raise_for_status()
            
            # Log the raw response to understand the structure
            logger.info(f"Raw response: {response.text[:500]}")
            
            response_data = response.json()
            
            # Handle different possible response structures
            if isinstance(response_data, list):
                reports = response_data
            elif isinstance(response_data, dict):
                # Try different possible keys
                reports = response_data.get('reports', [])
                if not reports:
                    reports = response_data.get('data', [])
                if not reports:
                    reports = response_data.get('results', [])
                if not reports and 'result' in response_data:
                    reports = response_data['result']
            else:
                reports = []
            
            logger.info(f"Found {len(reports)} available reports for {service.upper()}")
            return reports
        except requests.exceptions.RequestException as e:
            logger.error(f"Error getting available reports for {service}: {str(e)}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"Response status: {e.response.status_code}")
                logger.error(f"Response content: {e.response.text}")
            raise
    
    def download_file(self, url, local_path, service=None):
        """
        Download a file from a URL to a local path
        Args:
            url (str): URL to download from
            local_path (str): Local path to save the file
            service (str): Service name (not used for S3 downloads)
        """
        try:
            logger.info(f"Downloading file to {local_path}...")
            
            # For S3 URLs, use a direct request without authentication headers
            # as the URL is pre-signed
            if 's3.amazonaws.com' in url:
                response = requests.get(url, stream=True)
            else:
                # For other URLs, use the authenticated session
                response = self.sessions[service].get(url, stream=True)
            
            response.raise_for_status()
            
            with open(local_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            logger.info(f"Successfully downloaded {local_path}")
            return local_path
        except requests.exceptions.RequestException as e:
            logger.error(f"Error downloading file: {str(e)}")
            raise
    
    def download_report_by_name(self, service, report_name_pattern, local_filename):
        """
        Download a specific report by matching its name pattern
        Args:
            service (str): 'rzn1'
            report_name_pattern (str): Pattern to match in report name
            local_filename (str): Local filename to save as
        Returns:
            str: Path to downloaded file or None if not found
        """
        reports = self.get_available_reports(service)
        
        # Look for completed reports matching the pattern
        for report in reports:
            report_name = report.get('name', '').lower()
            report_status = report.get('status', '').lower()
            
            # Match the pattern and ensure the report is completed
            if (report_name_pattern.lower() in report_name and 
                report_status == 'completed'):
                
                download_url = report.get('generated_file')
                if download_url:
                    logger.info(f"Found completed report: {report.get('name')} (ID: {report.get('id')})")
                    logger.info(f"Created: {report.get('creation_date')}")
                    
                    local_path = os.path.join(TEMP_DOWNLOAD_DIR, local_filename)
                    return self.download_file(download_url, local_path, service)
        
        # If no completed report found, log available reports for debugging
        logger.info(f"No completed report found matching pattern '{report_name_pattern}'")
        logger.info("Available reports:")
        for report in reports[:5]:  # Show first 5 reports
            logger.info(f"  - {report.get('name')} (Status: {report.get('status')}, ID: {report.get('id')})")
        
        return None
    
    def download_latest_completed_report(self, service, report_name_pattern, local_filename):
        """
        Download the latest completed report matching the pattern
        Args:
            service (str): 'rzn1'
            report_name_pattern (str): Pattern to match in report name
            local_filename (str): Local filename to save as
        Returns:
            str: Path to downloaded file or None if not found
        """
        reports = self.get_available_reports(service)
        
        # Debug: Log all available reports with their IDs to help identify the correct one
        logger.info(f"DEBUG: All available reports for pattern '{report_name_pattern}':")
        for report in reports[:10]:  # Show first 10 reports
            logger.info(f"  ID: {report.get('id')}, Name: '{report.get('name')}', Status: {report.get('status')}")
        
        # Filter for completed reports matching the pattern
        matching_reports = []
        for report in reports:
            report_name = report.get('name', '').lower()
            report_status = report.get('status', '').lower()
            
            # More flexible pattern matching - handle "order summary" vs "order_summary" 
            # and "sales return" vs "sales_return"
            pattern_normalized = report_name_pattern.lower().replace('_', ' ')
            name_normalized = report_name.replace('_', ' ')
            
            # Special handling for different report types with exact matching
            if pattern_normalized == 'sales return':
                pattern_matches = ('sales return' in name_normalized and 
                                 'open' not in name_normalized)  # Exclude "Open" reports
            elif pattern_normalized == 'order summary':
                # Exact match for ORDER SUMMARY to avoid confusion with Open Order Summary
                pattern_matches = (name_normalized == 'order summary' or 
                                 'order summary' in name_normalized and 
                                 'open' not in name_normalized)  # Exclude "Open Order Summary"
            elif pattern_normalized == 'closing stock':
                # Closing stock generates "Batch Level Inventory Report"
                pattern_matches = ('batch level inventory' in name_normalized)
            elif pattern_normalized == 'batch level inventory':
                pattern_matches = ('batch level inventory' in name_normalized)
            elif pattern_normalized == 'open order summary':
                pattern_matches = ('open order summary' in name_normalized)
            else:
                pattern_matches = pattern_normalized in name_normalized
            
            if (pattern_matches and 
                report_status == 'completed' and 
                report.get('generated_file')):
                matching_reports.append(report)
        
        if not matching_reports:
            logger.warning(f"No completed reports found matching pattern '{report_name_pattern}' for {service.upper()}")
            return None
        
        # Sort by ID (assuming higher ID means more recent) and get the latest
        latest_report = max(matching_reports, key=lambda r: r.get('id', 0))
        
        logger.info(f"Downloading latest completed report: {latest_report.get('name')} (ID: {latest_report.get('id')})")
        logger.info(f"Created: {latest_report.get('creation_date')}")
        
        download_url = latest_report.get('generated_file')
        local_path = os.path.join(TEMP_DOWNLOAD_DIR, local_filename)
        return self.download_file(download_url, local_path, service)
    
    def download_latest_completed_report_by_id(self, service, report_id, report_name_pattern, local_filename):
        """
        Download the latest completed report matching the report ID first, then name pattern as fallback
        Args:
            service (str): 'rzn1'
            report_id (str): Report ID to match (e.g., '100', '145')
            report_name_pattern (str): Pattern to match in report name as fallback
            local_filename (str): Local filename to save as
        Returns:
            str: Path to downloaded file or None if not found
        """
        reports = self.get_available_reports(service)
        
        # Debug: Log all available reports with their IDs to help identify the correct one
        logger.info(f"DEBUG: Looking for report ID '{report_id}' or name pattern '{report_name_pattern}':")
        for report in reports[:10]:  # Show first 10 reports
            logger.info(f"  ID: {report.get('id')}, Name: '{report.get('name')}', Status: {report.get('status')}")
        
        # First, try to find by exact report ID
        for report in reports:
            if (str(report.get('id')) == str(report_id) and 
                report.get('status', '').lower() == 'completed' and 
                report.get('generated_file')):
                
                download_url = report.get('generated_file')
                logger.info(f"Found completed report by ID: {report.get('name')} (ID: {report.get('id')})")
                logger.info(f"Created: {report.get('creation_date')}")
                
                local_path = os.path.join(TEMP_DOWNLOAD_DIR, local_filename)
                return self.download_file(download_url, local_path, service)
        
        # If not found by ID, fall back to name pattern matching
        logger.warning(f"No completed report found with ID '{report_id}', trying name pattern '{report_name_pattern}'")
        return self.download_latest_completed_report(service, report_name_pattern, local_filename)
