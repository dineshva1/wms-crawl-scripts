# RZN1 WMS Crawl Scripts

A comprehensive enterprise-grade Python application for automated warehouse management system (WMS) data processing. This application handles authentication with RZN1 services, generates multiple report types, processes data through specialized algorithms, and manages the complete data pipeline from API to AWS S3 storage.

## 🚀 Features

- **🔐 Secure Multi-Service Authentication**: OAuth2 client credentials flow with RZN1 service
- **📊 Automated Report Generation**: Supports 5 different report types (Order Summary, Sales Return, Batch Level Inventory, Open Order Summary, Closing Stock)
- **⚡ Parallel Processing Pipeline**: Three specialized processors running in sequence
- **☁️ AWS S3 Integration**: Complete cloud storage solution with organized folder structure
- **🗂️ Intelligent File Management**: Automated cleanup and standardized naming conventions
- **📈 Excel Report Generation**: Monthly sales data compilation in Excel format
- **🔍 Comprehensive Logging**: Detailed process tracking with timestamps and severity levels
- **🛡️ Robust Error Handling**: Exception handling with graceful degradation
- **⚙️ Configurable Workflows**: Command-line options and environment-based configuration

## 🏗️ Architecture

### System Overview
```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   RZN1 API      │────│  Main Workflow  │────│    AWS S3       │
│   (Reports)     │    │   Orchestrator  │    │   (Storage)     │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                              │
                    ┌─────────┼─────────┐
                    │         │         │
              ┌─────────┐ ┌─────────┐ ┌─────────┐
              │ Order   │ │Inventory│ │Closing  │
              │Summary  │ │Summary  │ │Stock    │
              │Processor│ │Processor│ │Processor│
              └─────────┘ └─────────┘ └─────────┘
```

### Module Structure
```
wms-crawl-scripts/
├── 🔧 Core System
│   ├── main.py                           # Main workflow orchestrator
│   ├── auth.py                           # RZN1 OAuth authentication
│   ├── api_client.py                     # RZN1 API client & report management
│   ├── s3_utils.py                       # AWS S3 operations
│   └── logger_config.py                  # Centralized logging configuration
├── 🔄 Data Processors
│   ├── rzn1_order_summary_processor.py   # Order summary & sales return processing
│   ├── rzn1_inventory_summary_processor.py # Inventory analysis & aggregation
│   └── rzn1_closing_stock_processor.py   # Closing stock regional analysis
├── ⚙️ Configuration
│   ├── .env                              # Environment variables (not in repo)
│   ├── .env.example                      # Environment template
│   └── requirements.txt                  # Python dependencies
└── 📋 Documentation
    ├── README.md                         # This file
    └── logs/                             # Application logs
```

## 📋 Requirements

### System Requirements
- **Python**: 3.8+ (recommended: 3.11+)
- **Memory**: 4GB RAM minimum (8GB recommended for large datasets)
- **Storage**: 2GB free space for temporary files
- **Network**: Stable internet connection for API calls

### Python Dependencies
```
requests>=2.28.0          # HTTP client for API calls
pandas>=1.5.0            # Data manipulation and analysis
boto3>=1.26.0            # AWS SDK for S3 operations
openpyxl>=3.0.0          # Excel file processing
python-dotenv>=1.0.0     # Environment variable management
```

### AWS Requirements
- AWS account with S3 access
- IAM user with appropriate S3 permissions:
  ```json
  {
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Action": [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ],
        "Resource": [
          "arn:aws:s3:::your-bucket-name",
          "arn:aws:s3:::your-bucket-name/*"
        ]
      }
    ]
  }
  ```

## 🛠️ Installation & Setup

### 1. Clone Repository
```bash
git clone https://github.com/your-org/wms-crawl-scripts.git
cd wms-crawl-scripts
```

### 2. Create Virtual Environment
```bash
python3 -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

### 3. Install Dependencies
```bash
pip install -r requirements.txt
```

### 4. Configure Environment
```bash
# Copy environment template
cp .env.example .env

# Edit with your credentials
nano .env  # or use your preferred editor
```

### 5. Configure Required Environment Variables
Edit `.env` file with your actual credentials:

```bash
# RZN1 Service Configuration
RZN1_BASE_URL=https://rzn1-be.stockone.com
RZN1_CLIENT_ID=your-client-id
RZN1_CLIENT_SECRET=your-client-secret
RZN1_WAREHOUSE=your-warehouse-code

# AWS Configuration  
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key
AWS_REGION=ap-south-1
BUCKET_NAME=your-bucket-name
```

## 🚀 Usage

### Basic Execution
```bash
# Activate virtual environment
source .venv/bin/activate

# Run complete workflow with S3 upload
python main.py --upload-s3

# Run without processor execution (testing)
python main.py --upload-s3 --skip-processors

# Custom wait time between API calls
python main.py --upload-s3 --wait-time 60
```

### Command-Line Options
| Option | Description | Default |
|--------|-------------|---------|
| `--upload-s3` | Enable S3 upload after download | `False` |
| `--skip-processors` | Skip processor script execution | `False` |
| `--wait-time` | Wait time between report generation and download (seconds) | `30` |

## 📊 Data Processing Workflows

### 1. Order Summary Processor
**Input Files:**
- `ORDER_SUMMARY{YYYYMMDD}.csv` - Daily order summary data
- `SALES_RETURN{YYYYMMDD}.csv` - Daily sales return data

**Processing Steps:**
1. Data cleaning and validation
2. Regional segmentation (UP/HR)
3. Excel report generation for MTD sales data
4. Statistical analysis and aggregation

**Output Files:**
- `ORDER_SUMMARY_COMPLETE{YYYYMMDD}.csv` - Complete processed dataset
- `ORDER_SUMMARY_UP{YYYYMMDD}.csv` - UP region data
- `ORDER_SUMMARY_HR{YYYYMMDD}.csv` - HR region data
- `{Month}_Sales_Data_{YYYY}.xlsx` - Monthly Excel report

### 2. Inventory Summary Processor  
**Input Files:**
- `BATCH_LEVEL_INVENTORY{YYYYMMDD}.csv` - Batch-level inventory data
- `OPEN_ORDER_SUMMARY{YYYYMMDD}.csv` - Open orders data

**Processing Steps:**
1. Inventory data cleaning and filtering
2. Open order data processing
3. Aggregation by SKU Code
4. Final quantity calculation (Available - Open Orders)
5. Value computation (Price × Final Quantity)

**Output Files:**
- `INVENTORY_SUMMARY_COMPLETE{YYYYMMDD}.csv` - Aggregated inventory summary

### 3. Closing Stock Processor
**Input Files:**
- `CLOSING_STOCK{YYYYMMDD}.csv` - Closing stock data for all warehouses

**Processing Steps:**
1. Data cleaning and warehouse filtering (hm1|ls1)
2. Category and zone filtering
3. Regional split (UP/HR regions)
4. Value calculation and summarization

**Output Files:**
- `CLOSINGSTOCK_UP{YYYYMMDD}.csv` - UP region closing stock
- `CLOSINGSTOCK_HR{YYYYMMDD}.csv` - HR region closing stock

## 🗂️ S3 Storage Structure

```
s3://your-bucket-name/
└── rzn1/
    ├── order_summary/
    │   ├── raw/{YYYYMMDD}/
    │   │   ├── ORDER_SUMMARY{YYYYMMDD}.csv
    │   │   └── SALES_RETURN{YYYYMMDD}.csv
    │   ├── processed/{YYYYMMDD}/
    │   │   ├── ORDER_SUMMARY_COMPLETE{YYYYMMDD}.csv
    │   │   ├── ORDER_SUMMARY_UP{YYYYMMDD}.csv
    │   │   └── ORDER_SUMMARY_HR{YYYYMMDD}.csv
    │   └── report/main/sales/
    │       └── {Month}_Sales_Data_{YYYY}.xlsx
    └── inventory_summary/
        ├── raw/{YYYYMMDD}/
        │   ├── BATCH_LEVEL_INVENTORY{YYYYMMDD}.csv
        │   ├── OPEN_ORDER_SUMMARY{YYYYMMDD}.csv
        │   └── CLOSING_STOCK{YYYYMMDD}.csv
        └── processed/{YYYYMMDD}/
            ├── INVENTORY_SUMMARY_COMPLETE{YYYYMMDD}.csv
            ├── CLOSINGSTOCK_UP{YYYYMMDD}.csv
            └── CLOSINGSTOCK_HR{YYYYMMDD}.csv
```

## 🔄 Complete Workflow Process

### Phase 1: Authentication & Setup
1. Load environment configuration
2. Authenticate with RZN1 service using OAuth2
3. Initialize API client with proper headers

### Phase 2: Report Generation  
1. Generate 5 different report types:
   - Order Summary (Report ID: 100)
   - Sales Return (Report ID: 95) 
   - Batch Level Inventory (Report ID: 13)
   - Open Order Summary (Report ID: 145)
   - Closing Stock (Report ID: 13 with all warehouses)
2. Wait for report completion (configurable wait time)

### Phase 3: Data Download
1. Check report availability and status
2. Download completed reports with standardized naming
3. Apply date-based filename conventions

### Phase 4: Cloud Storage
1. Upload all reports to AWS S3 with organized structure
2. Route files to appropriate folders based on workflow type
3. Verify successful uploads

### Phase 5: Data Processing
1. **Order Summary Processor** - Process order and sales data
2. **Inventory Summary Processor** - Aggregate inventory analytics  
3. **Closing Stock Processor** - Generate regional closing stock reports
4. Upload all processed files to S3 processed folders

### Phase 6: Cleanup & Reporting
1. Remove temporary local files
2. Generate comprehensive execution summary
3. Log final status and file counts

## 📈 Output Verification

The application automatically verifies all output files in S3:

### Typical Output Summary
```
🏆 WORKFLOW EXECUTION SUMMARY
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📊 ORDER SUMMARY PROCESSOR:
  ✅ ORDER_SUMMARY_COMPLETE20250903.csv - 620 KB
  ✅ ORDER_SUMMARY_UP20250903.csv - 613 KB  
  ✅ ORDER_SUMMARY_HR20250903.csv - 6 KB
  ✅ Sep_Sales_Data_2025.xlsx - 258 KB

📦 INVENTORY SUMMARY PROCESSOR:
  ✅ INVENTORY_SUMMARY_COMPLETE20250903.csv - 282 KB

🏪 CLOSING STOCK PROCESSOR:
  ✅ CLOSINGSTOCK_UP20250903.csv - 2,934 KB
  ✅ CLOSINGSTOCK_HR20250903.csv - 2 KB

📁 RAW INPUT FILES: 5 files (22+ MB total)
🎯 TOTAL PROCESSED OUTPUT: 6 files (4.5+ MB)
```

## 🔧 Configuration Details

### Environment Variables Reference

| Variable | Description | Example |
|----------|-------------|---------|
| `RZN1_BASE_URL` | RZN1 service base URL | `https://rzn1-be.stockone.com` |
| `RZN1_CLIENT_ID` | OAuth client ID | `abc123...` |
| `RZN1_CLIENT_SECRET` | OAuth client secret | `xyz789...` |
| `RZN1_WAREHOUSE` | Default warehouse code | `up108_kum_ls1` |
| `AWS_ACCESS_KEY_ID` | AWS access key | `AKIA...` |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key | `secret...` |
| `AWS_REGION` | AWS region | `ap-south-1` |
| `BUCKET_NAME` | S3 bucket name | `wms-rozana` |

### File Processing Configuration

Each processor has specific input/output filename patterns and S3 folder structures defined in environment variables. This allows for easy customization without code changes.

## 🔍 Logging & Monitoring

### Log Files
- **Application Logs**: `logs/main_{YYYYMMDD}.log`
- **Authentication Logs**: `logs/auth_{YYYYMMDD}.log`  
- **API Client Logs**: `logs/api_client_{YYYYMMDD}.log`
- **S3 Operation Logs**: `logs/s3_utils_{YYYYMMDD}.log`

### Log Levels
- **INFO**: Normal operations and progress updates
- **WARNING**: Non-critical issues and fallbacks
- **ERROR**: Failures requiring attention
- **DEBUG**: Detailed debugging information (when enabled)

### Monitoring Key Metrics
- Report generation success rates
- File download completion times
- S3 upload success rates  
- Processor execution times
- Data processing volumes

## 🚨 Error Handling & Troubleshooting

### Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| Authentication Failed | Invalid credentials | Verify `RZN1_CLIENT_ID` and `RZN1_CLIENT_SECRET` |
| S3 Upload Failed | Invalid AWS credentials | Check `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` |
| Report Generation Timeout | API service issues | Increase `--wait-time` parameter |
| Processor Failed | Missing input files | Verify S3 file uploads completed successfully |
| Memory Issues | Large dataset processing | Increase system memory or process in chunks |

### Debug Mode
Enable detailed logging by setting environment variable:
```bash
export PYTHONPATH="${PYTHONPATH}:."
export LOG_LEVEL=DEBUG
python main.py --upload-s3
```

## 🔒 Security Best Practices

1. **Environment Variables**: Never commit `.env` files to version control
2. **AWS IAM**: Use least-privilege principles for S3 access
3. **API Keys**: Rotate credentials regularly
4. **Network Security**: Use VPN when accessing production APIs
5. **Data Privacy**: Ensure compliance with data protection regulations

## 🧪 Testing

### Test Configuration
```bash
# Test authentication only
python -c "from auth import get_both_tokens; print(get_both_tokens())"

# Test S3 connectivity
python -c "from s3_utils import S3Uploader; S3Uploader().test_connection()"

# Test without processors
python main.py --upload-s3 --skip-processors
```

### Validation Checklist
- [ ] Environment variables configured correctly
- [ ] AWS S3 permissions working
- [ ] RZN1 API authentication successful
- [ ] All 5 reports can be generated
- [ ] S3 folder structure created properly
- [ ] All 3 processors execute without errors
- [ ] Output files have expected data volumes

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📄 License

This project is proprietary software. All rights reserved.

## 📞 Support

For technical support or questions:
- **Email**: support@your-organization.com
- **Documentation**: Internal wiki/confluence  
- **Issues**: Create GitHub issue with detailed description

---

## 📊 Performance Metrics

- **Typical Runtime**: 3-5 minutes for complete workflow
- **Data Volume**: Processes 20+ MB raw data daily
- **Success Rate**: 99.5+ uptime with proper configuration
- **Scalability**: Handles warehouses with 100K+ SKUs

---

*Last Updated: September 2025 | Version: 1.0.0*
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
