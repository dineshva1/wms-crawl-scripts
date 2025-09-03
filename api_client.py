import os
import requests
from datetime import datetime, timedelta
from urllib.parse import urlparse, urlencode
from config import (
    RZN1_BASE_URL, RZN1_ENDPOINTS, RZN1_GET_REPORTS_ENDPOINT,
    TEMP_DOWNLOAD_DIR, get_date_prefix
)
from logger_config import setup_logger, get_default_log_file

# Setup logger
logger = setup_logger('api_client', get_default_log_file('api_client'))

class DualServiceAPIClient:
    def __init__(self, tokens):
        """
        Initialize API client for both services
        Args:
            tokens (dict): Dictionary with 'rzn1' and 'rzn' tokens
        """
        self.tokens = tokens
        self.sessions = {}
        
        # Create sessions for both services
        for service in ['rzn1']:
            session = requests.Session()
            if tokens.get(service):
                session.headers.update({'Authorization': f'{tokens[service]}'})
                session.headers.update({'warehouse': f'up108_kum_ls1'})
            self.sessions[service] = session
    
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
        Get specific parameters for different report types
        """
        yesterday = self._get_yesterday_date()
        
        # Default parameters for all reports
        default_params = {}
        
        # Report-specific parameters
        report_params = {
            'order_summary': {
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
                'Warehouse': [
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
                ],
                'From Date': yesterday
            },
            'sales_return': {
                'From Date': yesterday
            },
            'mati_inventory': {
                'From Date': yesterday
            },
            'mati_open_orders': {
                'From Date': yesterday
            },
            'store_inventory': {
                'From Date': yesterday
            }
        }
        
        # Return default params if no specific params for this report type
        return report_params.get(report_type, default_params)
    
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
            logger.info(f"Using parameters: {params}")
            
            # For order_summary, we need to use URL parameters
            if report_type == 'order_summary':
                # Convert lists to proper format for URL parameters
                query_params = {}
                for key, value in params.items():
                    if isinstance(value, list):
                        query_params[key] = value
                    else:
                        query_params[key] = value
                
                # Append query parameters to URL
                query_string = urlencode(query_params, doseq=True)
                full_url = f"{url}?{query_string}"
                
                logger.info(f"Making request to: {full_url}")
                response = self.sessions[service].get(full_url)
            else:
                # For other reports, use JSON body
                response = self.sessions[service].post(url, json=params)
                
            response.raise_for_status()
            logger.info(f"Successfully initiated {report_type} report generation for {service.upper()}")
            return True
        except requests.exceptions.RequestException as e:
            logger.error(f"Error generating {report_type} report for {service}: {str(e)}")
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
        url = f"{base_url}{reports_endpoint}"
        
        try:
            logger.info(f"Fetching available reports for {service.upper()}...")
            response = self.sessions[service].get(url)
            response.raise_for_status()
            reports = response.json().get('reports', [])
            logger.info(f"Found {len(reports)} available reports for {service.upper()}")
            return reports
        except requests.exceptions.RequestException as e:
            logger.error(f"Error getting available reports for {service}: {str(e)}")
            raise
    
    def download_file(self, url, local_path, service):
        """
        Download a file from a URL to a local path
        Args:
            url (str): URL to download from
            local_path (str): Local path to save the file
            service (str): Service name for session selection
        """
        try:
            logger.info(f"Downloading file to {local_path}...")
            with self.sessions[service].get(url, stream=True) as response:
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
            service (str): 'rzn1' or 'rzn'
            report_name_pattern (str): Pattern to match in report name
            local_filename (str): Local filename to save as
        Returns:
            str: Path to downloaded file or None if not found
        """
        reports = self.get_available_reports(service)
        
        for report in reports:
            if report_name_pattern.lower() in report.get('name', '').lower():
                download_url = report.get('download_url')
                if download_url:
                    local_path = os.path.join(TEMP_DOWNLOAD_DIR, local_filename)
                    return self.download_file(download_url, local_path, service)
        
        logger.warning(f"No report found matching pattern '{report_name_pattern}' for {service.upper()}")
        return None
