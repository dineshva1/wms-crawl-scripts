# Dual-Service Report Workflow with AWS S3 Integration

A comprehensive Python application that handles authentication with multiple services (RZN1), generates reports, downloads them with proper naming conventions, uploads to AWS S3, and executes processor scripts for order summary, inventory, and closing stock data.

## Features

- **Multi-Service Authentication**: Secure authentication with RZN1 service
- **Report Generation**: Automated generation of multiple report types
- **Standardized Naming**: Consistent file naming with date prefixes
- **AWS S3 Integration**: Upload generated reports to S3
- **Temporary File Management**: Cleanup of local files after successful S3 upload
- **Sequential Processing**: Automated execution of processor scripts
- **Comprehensive Logging**: Detailed process tracking with timestamps
- **Error Handling**: Robust exception handling and validation
- **Command-Line Options**: Configurable wait times and processor execution

## Architecture

### Module Structure

```
Dual-Service Workflow
├── auth.py                      # Authentication with RZN1 service
├── api_client.py                # API client for report generation and download
├── s3_utils.py                  # AWS S3 upload functionality
├── logger_config.py             # Centralized logging configuration
├── main.py                      # Main workflow orchestration
├── rzn1_order_summary_processor.py    # Order summary data processing
├── rzn1_inventory_summary_processor.py # Inventory data processing
├── rzn1_closing_stock_processor.py     # Closing stock data processing
└── .env                         # Environment configuration
```

## Setup

### 1. Create Virtual Environment

```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure Environment Variables

Copy the example environment file and configure it:
```bash
cp .env.example .env
```

Edit `.env` with your service credentials and AWS configuration:
```bash
# RZN1 Service Configuration
RZN1_BASE_URL=https://example-rzn1.com
RZN1_AUTH_URL=https://example-rzn1.com/o/token/
RZN1_CLIENT_ID=your-rzn1-client-id
RZN1_CLIENT_SECRET=your-rzn1-client-secret
RZN1_GENERATE_REPORT_URL=https://example-rzn1.com/api/v1/reports/generate
RZN1_GET_REPORT_URL=https://example-rzn1.com/api/v1/reports/download

# File processing configuration
ORDER_SUMMARY_FILENAME=ORDER_SUMMARY
SALES_RETURNS_FILENAME=SALES_RETURN
MATI_INVENTORY_FILENAME=MATI_INVENTORY
MATI_OPEN_ORDERS_FILENAME=MATI_OPEN_ORDERS
FDB_INVENTORY_FILENAME=FDB_INVENTORY
FDB_OPEN_ORDERS_FILENAME=FDB_OPEN_ORDERS
RBL_INVENTORY_FILENAME=RBL_INVENTORY
STORE_INVENTORY_FILENAME=STORE_INVENTORY

# S3 paths
INPUT_PREFIX=rzn1/raw
OUTPUT_PREFIX=rzn1/processed
MTD_PREFIX=rzn1/report/main/sales

# AWS Configuration
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key
AWS_REGION=ap-south-1
BUCKET_NAME=your-bucket-name
```

## Usage

### Running the Application

Ensure your virtual environment is activated and run:

```bash
source .venv/bin/activate
python3 main.py
```

The application will automatically:
1. Load configuration from `.env` file
2. Authenticate with RZN1 service
3. Generate reports for order summary, inventory, and more
4. Download reports with standardized naming
5. Upload reports to AWS S3
6. Clean up temporary files
7. Execute processor scripts sequentially

### Command-Line Options

```bash
python3 main.py --skip-processors  # Skip running processor scripts
python3 main.py --wait-time 60     # Set custom wait time between API calls (in seconds)
```

### Manual Configuration

You can also use the processor programmatically:

```python
from order_summary_processor import OrderSummaryProcessor
import os
from dotenv import load_dotenv

load_dotenv()

# Initialize processor
processor = OrderSummaryProcessor(
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_REGION'),
    bucket_name=os.getenv('BUCKET_NAME')
)

# Run complete pipeline
results = processor.process_complete_pipeline(
    order_summary_filename="ORDER_SUMMARY20250830",
    sales_returns_filename="SALES_RETURN20250830",
    input_prefix="rzn1/raw/20250830",
    output_prefix="rzn1/processed/20250830",
    output_filenames={
        "complete": "ORDER_SUMMARY_COMPLETE20250830.csv",
        "up": "ORDER_SUMMARY_UP20250830.csv",
        "hr": "ORDER_SUMMARY_HR20250830.csv"
    },
    mtd_prefix="rzn1/report/main/sales"
)
```

## Workflow Process

The application follows a comprehensive workflow:

### Step 1: Authentication
- Securely authenticates with RZN1 service using client credentials
- Obtains and manages access tokens for API calls

### Step 2: Report Generation
- Initiates report generation for multiple report types:
  - Order Summary
  - Sales Return
  - MATI Inventory
  - MATI Open Orders
  - FDB Inventory
  - FDB Open Orders
  - RBL Inventory
  - Store Inventory
- Handles API requests with proper error handling and logging

### Step 3: Report Download
- Retrieves generated report links from the service
- Downloads reports with standardized naming convention
- Applies date prefixes to all downloaded files

### Step 4: AWS S3 Upload
- Uploads all downloaded reports to configured S3 bucket
- Maintains organized folder structure in S3
- Verifies successful uploads with logging

### Step 5: Temporary File Cleanup
- Removes local copies of downloaded reports after successful S3 upload
- Ensures efficient disk space management

### Step 6: Sequential Processing
- Executes processor scripts in sequence:
  1. `rzn1_order_summary_processor.py` - Processes order summary data
  2. `rzn1_inventory_summary_processor.py` - Processes inventory data
  3. `rzn1_closing_stock_processor.py` - Processes closing stock data
- Captures and logs output from each processor
- Handles processor execution with proper error handling

## Output Files

The application generates and processes the following files:

### Downloaded Reports
```
/local/path/to/downloads/
├── rzn1_order_summary_{YYYYMMDD}.csv       # Order summary report
├── rzn1_sales_return_{YYYYMMDD}.csv        # Sales return report
├── rzn1_mati_inventory_{YYYYMMDD}.csv      # MATI inventory report
├── rzn1_mati_open_orders_{YYYYMMDD}.csv    # MATI open orders report
├── rzn1_fdb_inventory_{YYYYMMDD}.csv       # FDB inventory report
├── rzn1_fdb_open_orders_{YYYYMMDD}.csv     # FDB open orders report
├── rzn1_rbl_inventory_{YYYYMMDD}.csv       # RBL inventory report
└── rzn1_store_inventory_{YYYYMMDD}.csv     # Store inventory report
```

### S3 Uploaded Files
```
s3://your-bucket/rzn1/raw/{YYYYMMDD}/
├── rzn1_order_summary_{YYYYMMDD}.csv       # Order summary report
├── rzn1_sales_return_{YYYYMMDD}.csv        # Sales return report
├── rzn1_mati_inventory_{YYYYMMDD}.csv      # MATI inventory report
└── ...                                     # Other reports
```

### Processed Output Files
```
s3://your-bucket/rzn1/processed/{YYYYMMDD}/
├── ORDER_SUMMARY_COMPLETE{YYYYMMDD}.csv    # Complete processed dataset
├── ORDER_SUMMARY_UP{YYYYMMDD}.csv          # UP region data only
├── ORDER_SUMMARY_HR{YYYYMMDD}.csv          # HR region data only
└── ...                                     # Other processed files
```

### Excel Files (MTD Management)
```
s3://your-bucket/rzn1/report/main/sales/
├── Jan_Sales_Data_2025.xlsx                # January MTD data
├── Feb_Sales_Data_2025.xlsx                # February MTD data
└── ...                                     # Monthly files as needed
```

## Requirements

- Python 3.7+
- requests >= 2.28.0
- pandas >= 1.5.0
- boto3 >= 1.26.0
- openpyxl >= 3.0.0
- python-dotenv >= 1.0.0
- AWS S3 access permissions

## Key Features

1. **Multi-Service Support**: Authentication and API calls to RZN1 service
2. **Comprehensive Report Generation**: Generates multiple report types
3. **Standardized File Naming**: Consistent naming with date prefixes
4. **Temporary File Management**: Cleanup after successful S3 upload
5. **Sequential Processing**: Automated execution of processor scripts
6. **Robust Error Handling**: Comprehensive exception handling and logging
7. **Environment Configuration**: Secure credential management via .env files
8. **Command-Line Options**: Configurable wait times and processor execution
9. **S3 Integration**: Complete cloud-based processing pipeline

## Error Handling

The application includes comprehensive error handling for:
- S3 connection and permission issues
- Missing files or required columns
- Data type conversion errors
- Excel file read/write operations
- Network connectivity issues

## Logging

All operations are logged with timestamps and severity levels:
- **INFO**: Normal processing steps and progress indicators
- **ERROR**: Failures and exceptions with detailed error messages
- **WARNING**: Non-critical issues and fallback operations

## Security Best Practices

- Environment variables for sensitive credentials
- No hardcoded AWS keys in source code
- Least-privilege S3 permissions recommended
- Virtual environment isolation
- .env files excluded from version control
