import pandas as pd
import boto3
from datetime import datetime, date
from io import StringIO, BytesIO
import logging
from typing import Dict, Tuple, Optional
import os
from dotenv import load_dotenv
import re

# Configure logging with timestamp
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


class InventorySummaryProcessor:
    """
    A class to process closing stock report data with AWS S3 integration.
    
    This class handles:
    - Fetching CSV files from S3
    - Cleaning and filtering inventory data
    - Processing closing stock reports
    - Aggregating data by regions (UP/HR)
    - Uploading processed files back to S3
    """
    
    def __init__(self, aws_access_key_id: str = None, aws_secret_access_key: str = None, region_name: str = 'ap-south-1', bucket_name: str = None):
        self.bucket_name = bucket_name
        self.region_name = region_name
        
        # Initialize S3 client
        if aws_access_key_id and aws_secret_access_key:
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                region_name=region_name
            )
        else:
            raise ValueError("AWS credentials are required")
        
        self.excluded_categories = [
            "Accessories", "Apparel", "Asset", "Capex", 
            "Clothing And Accessories", "Consumables", "Footwears", "Rajeev Colony_CxEC Lite"
        ]
        
        self.excluded_zone_keywords = ['damage', 'expiry', 'expire', 'qc', 'short']
    
    def find_file_with_prefix(self, file_prefix: str, search_prefix: str) -> str:
        """
        Find a file in S3 that starts with the given prefix within a folder.
        """
        try:
            search_path = f"{search_prefix.rstrip('/')}/{file_prefix}" if search_prefix else file_prefix
            logger.info(f"Searching for files starting with: {file_prefix} in prefix: {search_prefix}")
            
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=search_path
            )
            
            if 'Contents' not in response or not response['Contents']:
                raise FileNotFoundError(f"No files found starting with: {search_path}")
            
            # Get the first matching file
            found_file = response['Contents'][0]['Key']
            logger.info(f"Found file: {found_file}")
            
            return found_file
            
        except Exception as e:
            logger.error(f"File Not Found in the source. Try again \n ref: {file_prefix} in {search_prefix} is not found: {str(e)}")
            raise
    
    def fetch_csv_from_s3(self, file_key: str) -> pd.DataFrame:
        """
        Fetch a CSV file from S3 and return as pandas DataFrame.
        """
        try:
            logger.info(f"Fetching file from S3: s3://{self.bucket_name}/{file_key}")
            
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=file_key)
            csv_content = response['Body'].read().decode('utf-8')
            
            df = pd.read_csv(StringIO(csv_content))
            logger.info(f"Successfully loaded CSV with shape: {df.shape}")
            
            return df
            
        except Exception as e:
            logger.error(f"Error fetching CSV from S3: {str(e)}")
            raise
    
    def upload_csv_to_s3(self, df: pd.DataFrame, filename: str, output_prefix: str) -> bool:
        """
        Upload a pandas DataFrame as CSV to S3 with specified prefix.
        
        Args:
            df: pandas DataFrame to upload
            filename: Name of the file to upload
            output_prefix: S3 prefix/folder where to save the CSV
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Convert DataFrame to CSV string
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)
            csv_content = csv_buffer.getvalue()
            
            # Create the full S3 key
            s3_key = f"{output_prefix.rstrip('/')}/{filename}" if output_prefix else filename
            
            # Upload to S3
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=csv_content.encode('utf-8'),
                ContentType='text/csv'
            )
            
            logger.info(f"Successfully uploaded {filename} to s3://{self.bucket_name}/{s3_key}")
            return True
            
        except Exception as e:
            logger.error(f"Error uploading {filename} to S3: {str(e)}")
            return False
    
    def clean_batch_level_inventory_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and filter batch level inventory data.
        """
        try:
            logger.info(f"Starting data cleaning. Initial shape: {df.shape}")
            
            # Defensive: Check required columns
            required_cols = ['Warehouse', 'SKU Code', 'Product Description', 'SKU Category', 'SKU Sub Category', 'Zone', 'Available Quantity', 'Price']
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                raise ValueError(f"Missing required columns: {missing_cols}")
            
            # Standardize text
            df['Warehouse'] = df['Warehouse'].str.strip().str.lower()
            df = df.rename(columns={"Product Description": "SKU Description"})
            df['SKU Code'] = df['SKU Code'].str.replace(r'(?i)loose', '', regex=True)
            df['SKU Description'] = df['SKU Description'].str.strip()
            df['Zone'] = df['Zone'].str.strip()
            
            # Filter valid warehouses
            df = df[df['Warehouse'].str.contains(r'hm1|ls1',case=False, na=False)]
            logger.info(f"After Warehouse filter: {df.shape}")
            
            # Remove FR and CAP SKUs
            df = df[~df['SKU Code'].str.upper().str.startswith('FR', na=False)]
            df = df[~df['SKU Code'].str.upper().str.startswith('CAP', na=False)]
            logger.info(f"After removing FR and CAP SKUs: {df.shape}")
            
            # Remove excluded categories
            df = df[~df['SKU Category'].isin(self.excluded_categories)]
            logger.info(f"After SKU Category filter: {df.shape}")
            
            # Remove bad zones
            zone_pattern = '|'.join([re.escape(z) for z in self.excluded_zone_keywords])
            df = df[~df['Zone'].str.lower().str.contains(zone_pattern,case=False, na=False)]
            logger.info(f"After zone filter: {df.shape}")
            
            # Convert to numerics and calculate final value
            df['Available Quantity'] = pd.to_numeric(df['Available Quantity'], errors='coerce').fillna(0)            
            df['Price'] = pd.to_numeric(df['Price'], errors='coerce').fillna(0)
            # Restore uppercase warehouse
            df['Warehouse'] = df['Warehouse'].str.upper()
            df['Value'] = df['Available Quantity'] * df['Price']
            logger.info(f"Data cleaning completed. Final shape: {df.shape}")
            import pdb; pdb.set_trace()
            return df[['Warehouse', 'SKU Code', 'SKU Description', 'SKU Category', 'SKU Sub Category', 'Available Quantity', 'Value']]
        except Exception as e:
            logger.error(f"Error in data cleaning: {str(e)}")
            raise
    
    def split_by_regions(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Split data by UP and HR regions.
        """
        df_up = df[df['Warehouse'].str.startswith('UP')].copy()
        df_hr = df[df['Warehouse'].isin(['HR007_RJV_LS1', 'HR009_PLA_LS1'])].copy()
        
        logger.info(f"UP Region shape: {df_up.shape}")
        logger.info(f"Haryana Region shape: {df_hr.shape}")
        
        return df_up, df_hr
       
    def process_complete_pipeline(self, batch_level_inventory_filename: str, input_prefix: str, output_prefix: str, output_filenames: dict) -> dict:
        """
        Execute the complete inventory processing pipeline.
        """
        try:
            logger.info("Starting Inventory Summary Processing Pipeline")
            
            # Find file in the specified input prefix
            batch_level_inventory_key = self.find_file_with_prefix(batch_level_inventory_filename, input_prefix)
            
            # Fetch the found file
            df_batch_level_inventory = self.fetch_csv_from_s3(batch_level_inventory_key)
            
            # Step 2: Clean closing stock data
            df_batch_level_inventory_cleaned = self.clean_batch_level_inventory_data(df_batch_level_inventory)
            df_up, df_hr = self.split_by_regions(df_batch_level_inventory_cleaned)
            datasets = {
                "up": df_up,
                "hr": df_hr
            }
            # Step 5: Upload all processed files to S3
            upload_results = {}
            for file_type, df in datasets.items():
                filename = output_filenames[file_type]
                upload_results[filename] = self.upload_csv_to_s3(df, filename, output_prefix)
                logger.info(f"Uploaded {filename}: {len(df)} rows")
            
            logger.info("Pipeline completed successfully")
            return upload_results
            
        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}")
            raise


def main():
    """
    Main function to demonstrate usage of InventorySummaryProcessor.
    """
    load_dotenv()
    
    try:
        processor = InventorySummaryProcessor(
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_REGION'),
            bucket_name=os.getenv('BUCKET_NAME')
        )
        
        now = datetime.now()
        date_suffix = now.strftime("%Y%m%d")

        batch_level_inventory_filename = f"{os.getenv('BATCH_LEVEL_CLOSINGSTOCK_FILENAME', 'Batch_Level_Inventory_Report')}{date_suffix}"
        
        output_filenames = {
            "up": f"{os.getenv('CLOSINGSTOCK_OUTPUT_UP_FILENAME', 'CLOSINGSTOCK_UP').upper()}{date_suffix}.csv",
            "hr": f"{os.getenv('CLOSINGSTOCK_OUTPUT_HR_FILENAME', 'CLOSINGSTOCK_HR').upper()}{date_suffix}.csv"
        }
        
        results = processor.process_complete_pipeline(
            batch_level_inventory_filename=batch_level_inventory_filename,
            input_prefix=f"{os.getenv('CLOSINGSTOCK_INPUT_PREFIX', 'rzn1/inventory_summary/raw').rstrip('/')}/{date_suffix}",
            output_prefix=f"{os.getenv('CLOSINGSTOCK_OUTPUT_PREFIX', 'rzn1/inventory_summary/processed').rstrip('/')}/{date_suffix}",
            output_filenames=output_filenames
        )
        
        print("Upload Status:")
        for filename, success in results.items():
            status = "SUCCESS" if success else "FAILED"
            print(f"{status}: {filename}")
            
    except Exception as e:
        logger.error(f"Main execution failed: {str(e)}")
        raise


if __name__ == "__main__":
    main()