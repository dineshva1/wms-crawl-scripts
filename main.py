import os
import time
import argparse
import subprocess
import shutil
from datetime import datetime
from config import TEMP_DOWNLOAD_DIR, get_date_prefix
from auth import get_both_tokens
from api_client import DualServiceAPIClient
from s3_utils import S3Uploader
from logger_config import setup_logger, get_default_log_file

# Setup logger
logger = setup_logger('main', get_default_log_file('main'))

def generate_filename(service, report_type, date_prefix):
    """Generate filename based on service, report type and date"""
    if service == 'rzn1':
        if report_type == 'sales_return':
            return f"rzn1_sales_summary_{date_prefix}.csv"
        else:
            return f"rzn1_{report_type}_{date_prefix}.csv"
    else:  # rzn
        if report_type == 'sales_return':
            return f"rzn_sales_summary_{date_prefix}.csv"
        else:
            return f"rzn_{report_type}_{date_prefix}.csv"

def cleanup_temp_files(file_paths):
    """Clean up temporary files after successful S3 upload"""
    for file_path in file_paths:
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.info(f"Cleaned up temporary file: {os.path.basename(file_path)}")
        except Exception as e:
            logger.error(f"Failed to clean up {file_path}: {str(e)}")

def execute_processor_script(script_name):
    """Execute a processor script and return success status"""
    try:
        logger.info(f"Executing {script_name}...")
        result = subprocess.run(
            ['python', script_name],
            cwd=os.path.dirname(os.path.abspath(__file__)),
            capture_output=True,
            text=True,
            timeout=1800  # 30 minutes timeout
        )
        
        if result.returncode == 0:
            logger.info(f"Successfully executed {script_name}")
            if result.stdout:
                logger.info(f"Output from {script_name}: {result.stdout}")
            return True
        else:
            logger.error(f"Failed to execute {script_name}. Return code: {result.returncode}")
            if result.stderr:
                logger.error(f"Error from {script_name}: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        logger.error(f"Timeout executing {script_name}")
        return False
    except Exception as e:
        logger.error(f"Exception executing {script_name}: {str(e)}")
        return False

def main():
    parser = argparse.ArgumentParser(description='Download reports from RZN and RZN1 services')
    parser.add_argument('--upload-s3', action='store_true', help='Upload files to S3 after download')
    parser.add_argument('--wait-time', type=int, default=30, help='Wait time between report generation and download (seconds)')
    parser.add_argument('--skip-processors', action='store_true', help='Skip executing processor scripts')
    args = parser.parse_args()
    
    try:
        logger.info("Starting RZN1 Service Report Processing...")

        
        # Step 1: Get authentication tokens for both services
        logger.info("Step 1: Authenticating with RZN1 service...")
        tokens = get_both_tokens()

        # Step 2: Initialize API client
        logger.info("Step 2: Initializing API client for RZN1 service...")
        api_client = DualServiceAPIClient(tokens)
        
        # Step 3: Generate all reports
        logger.info("Step 3: Generating reports for RZN1 service...")
        
        # RZN1 Reports
        rzn1_reports = ['order_summary', 'sales_return', 'mati_inventory', 'mati_open_orders', 'store_inventory']
        logger.info(f"Generating RZN1 reports: {rzn1_reports}")
        for report_type in rzn1_reports:
            try:
                # The API client now automatically handles the correct parameters for each report type
                # including setting yesterday's date for 'From Date'
                api_client.generate_report('rzn1', report_type)
            except Exception as e:
                logger.error(f"Failed to generate {report_type} for RZN1: {str(e)}")
        
        # RZN Reports
        # rzn_reports = ['order_summary', 'sales_return', 'fdb_inventory', 'fdb_open_orders', 'rbl_inventory', 'store_inventory']
        # logger.info(f"Generating RZN reports: {rzn_reports}")
        # for report_type in rzn_reports:
        #     try:
        #         api_client.generate_report('rzn', report_type)
        #     except Exception as e:
        #         logger.error(f"Failed to generate {report_type} for RZN: {str(e)}")
        
        # Step 4: Wait for reports to be generated
        logger.info(f"Step 4: Waiting {args.wait_time} seconds for reports to be generated...")
        time.sleep(args.wait_time)
        
        # Step 5: Download all reports
        logger.info("Step 5: Downloading generated reports...")
        date_prefix = get_date_prefix()
        downloaded_files = []
        
        # Download RZN1 reports
        logger.info("Downloading RZN1 reports...")
        for report_type in rzn1_reports:
            try:
                filename = generate_filename('rzn1', report_type, date_prefix)
                local_path = api_client.download_report_by_name('rzn1', report_type, filename)
                if local_path:
                    downloaded_files.append(local_path)
                    logger.info(f"✓ Downloaded: {filename}")
                else:
                    logger.warning(f"✗ Failed to download: {report_type} for RZN1")
            except Exception as e:
                logger.error(f"✗ Error downloading {report_type} for RZN1: {str(e)}")
        
        # Download RZN reports
        # print(f"\nDownloading RZN reports...")
        # for report_type in rzn_reports:
        #     try:
        #         filename = generate_filename('rzn', report_type, date_prefix)
        #         local_path = api_client.download_report_by_name('rzn', report_type, filename)
        #         if local_path:
        #             downloaded_files.append(local_path)
        #             print(f"✓ Downloaded: {filename}")
        #         else:
        #             print(f"✗ Failed to download: {report_type} for RZN")
        #     except Exception as e:
        #         print(f"✗ Error downloading {report_type} for RZN: {str(e)}")
        
        # Step 6: Upload to S3 (optional)
        uploaded_files = []
        if args.upload_s3 and downloaded_files:
            logger.info(f"Step 6: Uploading {len(downloaded_files)} files to S3...")
            s3_uploader = S3Uploader()
            
            for file_path in downloaded_files:
                try:
                    filename = os.path.basename(file_path)
                    s3_key = f"reports/{date_prefix}/{filename}"
                    if s3_uploader.upload_file(file_path, s3_key):
                        logger.info(f"✓ Uploaded to S3: {filename}")
                        uploaded_files.append(file_path)
                    else:
                        logger.error(f"✗ Failed to upload to S3: {filename}")
                except Exception as e:
                    logger.error(f"✗ Error uploading {filename} to S3: {str(e)}")
            
            # Step 7: Clean up temporary files after successful S3 upload
            if uploaded_files:
                logger.info(f"Step 7: Cleaning up {len(uploaded_files)} temporary files...")
                cleanup_temp_files(uploaded_files)
        
        # Step 8: Execute processor scripts (if not skipped)
        if not args.skip_processors and uploaded_files:
            logger.info("Step 8: Executing processor scripts...")
            processor_scripts = [
                'rzn1_order_summary_processor.py',
                'rzn1_inventory_summary_processor.py',
                'rzn1_closing_stock_processor.py'
            ]
            
            for script in processor_scripts:
                if os.path.exists(script):
                    success = execute_processor_script(script)
                    if not success:
                        logger.warning(f"Processor script {script} failed, but continuing...")
                else:
                    logger.warning(f"Processor script {script} not found, skipping...")
        
        # Step 9: Summary
        logger.info("=" * 60)
        logger.info("PROCESS SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Total files downloaded: {len(downloaded_files)}")
        logger.info(f"Total files uploaded to S3: {len(uploaded_files)}")
        logger.info(f"Files saved in: {TEMP_DOWNLOAD_DIR}")
        
        if downloaded_files:
            logger.info("Downloaded files:")
            for file_path in downloaded_files:
                logger.info(f"  - {os.path.basename(file_path)}")
        
        logger.info("Process completed successfully!")
        return 0
        
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        return 1

if __name__ == "__main__":
    exit(main())
