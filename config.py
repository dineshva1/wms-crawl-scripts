import os
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables from .env file
load_dotenv()

# RZN1 Service Configuration
RZN1_BASE_URL = os.getenv('RZN1_BASE_URL')
RZN1_AUTH_ENDPOINT = os.getenv('RZN1_AUTH_ENDPOINT')
RZN1_CLIENT_ID = os.getenv('RZN1_CLIENT_ID')
RZN1_CLIENT_SECRET = os.getenv('RZN1_CLIENT_SECRET')
RZN1_GET_REPORTS_ENDPOINT = os.getenv('RZN1_GET_REPORTS_ENDPOINT')

# API Endpoints for RZN1
RZN1_ENDPOINTS = {
    'order_summary': os.getenv('RZN1_ORDER_SUMMARY_ENDPOINT', '/order-summary'),
    'sales_return': os.getenv('RZN1_SALES_RETURN_ENDPOINT', '/sales-return'),
    'mati_inventory': os.getenv('RZN1_MATI_INVENTORY_ENDPOINT', '/mati-inventory'),
    'mati_open_orders': os.getenv('RZN1_MATI_OPEN_ORDERS_ENDPOINT', '/mati-open-orders'),
    'store_inventory': os.getenv('RZN1_STORE_INVENTORY_ENDPOINT', '/store-inventory')
}

# API Endpoints for RZN
RZN_ENDPOINTS = {
    'order_summary': os.getenv('RZN_ORDER_SUMMARY_ENDPOINT', '/order-summary'),
    'sales_return': os.getenv('RZN_SALES_RETURN_ENDPOINT', '/sales-return'),
    'fdb_inventory': os.getenv('RZN_FDB_INVENTORY_ENDPOINT', '/fdb-inventory'),
    'fdb_open_orders': os.getenv('RZN_FDB_OPEN_ORDERS_ENDPOINT', '/fdb-open-orders'),
    'rbl_inventory': os.getenv('RZN_RBL_INVENTORY_ENDPOINT', '/rbl-inventory'),
    'store_inventory': os.getenv('RZN_STORE_INVENTORY_ENDPOINT', '/store-inventory')
}

# AWS Configuration
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')

# File Configuration
TEMP_DOWNLOAD_DIR = 'temp_downloads'

def get_date_prefix():
    """Generate date prefix for filenames"""
    return datetime.now().strftime("%Y%m%d")

# Create temp directory if it doesn't exist
os.makedirs(TEMP_DOWNLOAD_DIR, exist_ok=True)
