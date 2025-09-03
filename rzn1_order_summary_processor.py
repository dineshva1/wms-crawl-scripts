import pandas as pd
import boto3
from datetime import datetime, date
from io import StringIO, BytesIO
import logging
from typing import Dict, Tuple, Optional
import os
from dotenv import load_dotenv
import calendar

# Configure logging with timestamp
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


class OrderSummaryProcessor:
    """
    A class to process order summary and sales return data with AWS S3 integration.
    
    This class handles:
    - Fetching CSV files from S3
    - Cleaning and filtering order data
    - Processing sales returns
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
            "Accessories", "Apparel", "Asset", "Capex", "Clothing And Accessories", "Consumables", "Footwears", "Rajeev Colony_CxEC Lite"
        ]
        
        # Direct column mapping between order summary and sales returns
        self.column_map = {
            'SKU Code': 'Sku Code',
            'Invoice Number': 'Invoice / Challan Number'
        }
    def find_file_with_prefix(self, file_prefix: str, search_prefix: str) -> str:
        """
        Find a file in S3 that starts with the given prefix within a folder.
        
        Args:
            file_prefix: File name prefix to search for (e.g., ORDER_SUMMARY20250829)
            search_prefix: S3 prefix/folder to search within
            
        Returns:
            str: Full S3 key of the found file
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
            logger.error(f"Error searching for files with prefix {file_prefix} in {search_prefix}: {str(e)}")
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
            file_key = f"{output_prefix.rstrip('/')}/{filename}" if output_prefix else filename
            logger.info(f"Uploading CSV to S3: s3://{self.bucket_name}/{file_key}")
            
            # Convert DataFrame to CSV string
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)
            csv_content = csv_buffer.getvalue()
            
            # Upload to S3
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=file_key,
                Body=csv_content.encode('utf-8'),
                ContentType='text/csv'
            )
            
            logger.info(f"Successfully uploaded CSV with {len(df)} rows to {file_key}")
            return True
            
        except Exception as e:
            logger.error(f"Error uploading CSV to S3: {str(e)}")
            return False
    
    def clean_order_summary_data(self, df_os: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and filter the order summary data.
        """
        logger.info(f"Starting order summary cleaning. Initial shape: {df_os.shape}")
        
        # Standardize warehouse names
        df_os['Warehouse'] = df_os['Warehouse'].str.strip().str.lower()
        
        # Filter warehouses (keep only hm1 and ls1)
        df_os = df_os[df_os['Warehouse'].str.contains(r'hm1|ls1', na=False)].copy()
        logger.info(f"After warehouse filter: {df_os.shape}")
        
        # Clean SKU Code (remove 'loose')
        df_os['SKU Code'] = df_os['SKU Code'].str.replace(r'(?i)loose', '', regex=True)
        
        # Remove unwanted SKU codes (FR and CAP prefixes)
        df_os = df_os[~df_os['SKU Code'].str.upper().str.startswith('FR', na=False)]
        df_os = df_os[~df_os['SKU Code'].str.upper().str.startswith('CAP', na=False)]
        logger.info(f"After removing FR CAP SKUs and replacing loose: {df_os.shape}")
        
        # Remove excluded categories
        df_os = df_os[~df_os['SKU Category'].isin(self.excluded_categories)]
        logger.info(f"After SKU category filter: {df_os.shape}")
        
        # Remove orders with 'st' in Order Reference
        df_os = df_os[~df_os['Order Reference'].str.lower().str.contains('st', na=False)]
        logger.info(f"After order reference filter: {df_os.shape}")
        
        # Remove cancelled orders
        df_os = df_os[~df_os['OrderStatus'].str.lower().eq('cancelled')]
        logger.info(f"After order status filter: {df_os.shape}")
        
        # Initialize return columns
        df_os['Return Qty'] = 0
        df_os['Return Value'] = 0
        return df_os
    
    def process_sales_returns(self, df_os: pd.DataFrame, df_sr: pd.DataFrame) -> pd.DataFrame:
        """
        Process sales returns and merge with order summary data.
        """
        logger.info(f"Processing sales returns. Sales return shape: {df_sr.shape}")
        
        # Clean SKU Code in sales returns (ensure consistent formatting)
        df_sr['Sku Code'] = df_sr['Sku Code'].astype(str).str.strip().str.replace(r'(?i)loose', '', regex=True)
        
        # Clean Invoice/Challan Number (remove any whitespace/formatting issues)
        df_sr['Invoice / Challan Number'] = df_sr['Invoice / Challan Number'].astype(str).str.strip()
        
        # Create merge key for sales returns
        df_sr['Merge'] = df_sr['Invoice / Challan Number'] + df_sr['Sku Code']
        
        # Debug: Check for duplicates in sales returns before aggregation
        logger.info(f"Sales returns before aggregation: {len(df_sr)} rows")
        logger.info(f"Unique merge keys in sales returns: {df_sr['Merge'].nunique()}")
        
        # Aggregate sales returns by merge key - ensure we're not double-counting
        sr_lookup_agg = df_sr.groupby('Merge', as_index=False).agg({
            'Quantity': 'sum',
            'TotalCreditNoteAmount': 'sum'
        })
        
        logger.info(f"Sales returns after aggregation: {len(sr_lookup_agg)} rows")
        
        # Clean order summary data for merge
        df_os['Invoice Number'] = df_os['Invoice Number'].astype(str).str.strip()
        df_os['SKU Code'] = df_os['SKU Code'].astype(str).str.strip()
        
        # Create merge key for order summary
        df_os['Merge_temp'] = df_os['Invoice Number'] + df_os['SKU Code']
        
        # Debug: Check merge key matches
        os_merge_keys = set(df_os['Merge_temp'].unique())
        sr_merge_keys = set(sr_lookup_agg['Merge'].unique())
        matching_keys = os_merge_keys.intersection(sr_merge_keys)
        logger.info(f"Order summary unique merge keys: {len(os_merge_keys)}")
        logger.info(f"Sales returns unique merge keys: {len(sr_merge_keys)}")
        logger.info(f"Matching merge keys: {len(matching_keys)}")
        
        # Merge with sales returns
        df_os = df_os.merge(sr_lookup_agg, left_on='Merge_temp', right_on='Merge', how='left')
        logger.info(f"After return column merge: {df_os.shape}")
        
        # Update return columns with proper null handling
        df_os['Return Qty'] = pd.to_numeric(df_os['Quantity'], errors='coerce').fillna(0)
        df_os['Return Value'] = pd.to_numeric(df_os['TotalCreditNoteAmount'], errors='coerce').fillna(0)
        
        # Debug: Check return values
        total_return_value = df_os['Return Value'].sum()
        logger.info(f"Total return value after merge: {total_return_value}")
        
        # Clean up temporary columns
        df_os = df_os.drop(columns=['Merge_temp', 'Merge', 'Quantity', 'TotalCreditNoteAmount'])
        return df_os
    
    def calculate_net_sales(self, df_os: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate net sales values after returns.
        """
        # Ensure numeric columns - using correct column names from working script
        for col in ['Invoice Number', 'InvoiceAmount', 'Invoice quantity']:
            if col not in df_os.columns:
                raise ValueError(f"[❌] Missing required column: {col}")
            df_os[col] = pd.to_numeric(df_os[col], errors='coerce').fillna(0)
        
        # Calculate net sales - using correct column names
        df_os['Sales Qty'] = df_os['Invoice quantity'] - df_os['Return Qty']
        df_os['Sales Value'] = df_os['InvoiceAmount'] - df_os['Return Value']
        
        # Restore uppercase warehouse names
        df_os['Warehouse'] = df_os['Warehouse'].str.upper()

        # Normalize order date
        df_os['Order Date'] = pd.to_datetime(df_os['Order Date'], errors='coerce')
        df_os['Order Date'] = df_os['Order Date'].dt.normalize()
        return df_os
    
    def split_by_regions(self, df_os: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Split data by UP and HR regions.
        """
        df_up = df_os[df_os['Warehouse'].str.startswith('UP')].copy()
        df_hr = df_os[df_os['Warehouse'].str.startswith('HR')].copy()
        logger.info(f"Split data UP: {df_up.shape} HR: {df_hr.shape}")
        return df_up, df_hr
    
    def aggregate_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Prepare data with required columns without aggregation to preserve individual records.
        """
        # Create a copy to avoid modifying the original dataframe
        result_df = df.copy()
        
        # Create the Merge column by concatenating Warehouse and SKU Code
        result_df['Merge'] = result_df['Warehouse'] + result_df['SKU Code']
        
        # Rename SKU Desc to SKU Description
        result_df = result_df.rename(columns={'SKU Desc': 'SKU Description'})
        
        # Define the required columns in the specified order
        column_order = ['Merge', 'Warehouse', 'SKU Code', 'SKU Description', 'Order Date', 'SKU Category', 'SKU Sub Category', 'Sales Qty', 'Sales Value']
        # Return only the required columns without any aggregation
        return result_df[column_order]
    
    def mtd_data(self, df_os_final: pd.DataFrame, mtd_prefix: str) -> bool:
        """
        Handle Month-Till-Date data operations with AWS S3.
        """
        try:
            # Generate current month filename
            current_date = datetime.now()
            month_name = calendar.month_abbr[current_date.month]  # e.g., 'Sep'
            year = current_date.year
            filename = f"{month_name}_Sales_Data_{year}.xlsx"
            
            # Construct full S3 key
            s3_key = f"{mtd_prefix.rstrip('/')}/{filename}"
            
            logger.info(f"Processing MTD data for file: {filename}")
            logger.info(f"S3 key: {s3_key}")

            df_new = df_os_final.copy()

            if 'Merge' not in df_new.columns:
                logger.error("'Merge' column not found in df_os_final")
                raise ValueError("'Merge' column not found in df_os_final")
            
            if 'Order Date' not in df_new.columns:
                logger.error("'Order Date' column not found in df_os_final")
                raise ValueError("'Order Date' column not found in df_os_final")
            
            # Convert Order Date to string format for concatenation
            df_new['Order Date'] = pd.to_datetime(df_new['Order Date']).dt.strftime('%Y-%m-%d')
            df_new['uniqueID'] = df_new['Merge'].astype(str) + '_' + df_new['Order Date'].astype(str)
            
            logger.info(f"Created uniqueID for {len(df_new)} new records")
            
            # Check if file exists in S3
            file_exists = False
            try:
                self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
                file_exists = True
                logger.info(f"File {filename} exists in S3, will merge data")
            except Exception as e:
                logger.info(f"File {filename} does not exist in S3, will create new file")
            
            if file_exists:
                # Download existing file and merge data
                logger.info("Downloading existing Excel file from S3")
                response = self.s3_client.get_object(Bucket=self.bucket_name, Key=s3_key)
                excel_content = response['Body'].read()
                
                # Read existing Excel file
                existing_df = pd.read_excel(BytesIO(excel_content))
                logger.info(f"Existing file has {len(existing_df)} records")
                
                # Handle backward compatibility - create uniqueID for existing data if missing
                if 'uniqueID' not in existing_df.columns:
                    logger.info("uniqueID column not found in existing file, creating it...")
                    
                    # Check if required columns exist in existing file
                    if 'Merge' not in existing_df.columns or 'Order Date' not in existing_df.columns:
                        logger.warning("Required columns missing in existing file. Replacing entire file with new data.")
                        combined_df = df_new.copy()
                    else:
                        # Create uniqueID for existing data
                        existing_df['Order Date'] = pd.to_datetime(existing_df['Order Date']).dt.strftime('%Y-%m-%d')
                        existing_df['uniqueID'] = existing_df['Merge'].astype(str) + '_' + existing_df['Order Date'].astype(str)
                        logger.info(f"Created uniqueID for {len(existing_df)} existing records")
                        
                        # Remove rows from existing_df that have matching uniqueID values in df_new
                        existing_df_filtered = existing_df[~existing_df['uniqueID'].isin(df_new['uniqueID'])]
                        
                        # Combine filtered existing data with new data
                        combined_df = pd.concat([existing_df_filtered, df_new], ignore_index=True)
                        
                        logger.info(f"Merged data: {len(existing_df_filtered)} existing + {len(df_new)} new = {len(combined_df)} total records")
                else:
                    existing_df_filtered = existing_df[~existing_df['uniqueID'].isin(df_new['uniqueID'])]
                    combined_df = pd.concat([existing_df_filtered, df_new], ignore_index=True)
                    
                    logger.info(f"Merged data: {len(existing_df_filtered)} existing + {len(df_new)} new = {len(combined_df)} total records")
                
            else:
                combined_df = df_new.copy()
                logger.info(f"Creating new file with {len(combined_df)} records")
            
            if 'uniqueID' in combined_df.columns:
                combined_df = combined_df.drop(columns=['uniqueID'])
                logger.info("Removed uniqueID column from final output")
            
            excel_buffer = BytesIO()
            with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                combined_df.to_excel(writer, index=False, sheet_name='Sales_Data')
            
            excel_buffer.seek(0)
            
            logger.info(f"Uploading Excel file to S3: {s3_key}")
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=excel_buffer.getvalue(),
                ContentType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            )
            
            logger.info(f"Successfully uploaded {filename} with {len(combined_df)} records")
            return True
            
        except Exception as e:
            logger.error(f"Error in MTD data processing: {str(e)}")
            return False
    
    def process_complete_pipeline(self, order_summary_filename: str, sales_returns_filename: str, input_prefix: str, output_prefix: str, output_filenames: dict, mtd_prefix: str) -> Dict[str, bool]:
        """
        Execute the complete data processing pipeline.
        """
        try:
            logger.info("Starting Order Summary Processing Pipeline")
            
            # Find files in the specified input prefix
            order_summary_key = self.find_file_with_prefix(order_summary_filename, input_prefix)
            sales_returns_key = self.find_file_with_prefix(sales_returns_filename, input_prefix)
            
            # Fetch the found files
            df_os = self.fetch_csv_from_s3(order_summary_key)
            df_sr = self.fetch_csv_from_s3(sales_returns_key)
            
            # Step 2: Clean order summary data
            df_os_cleaned = self.clean_order_summary_data(df_os)
            
            # Step 3: Process sales returns
            df_os_with_returns = self.process_sales_returns(df_os_cleaned, df_sr)
            
            # Step 4: Calculate net sales
            df_os_final = self.calculate_net_sales(df_os_with_returns)
            
            # Step 5: Split by regions
            df_up, df_hr = self.split_by_regions(df_os_final)
            
            # Step 6: Aggregate all datasets
            datasets = {
                "complete": self.aggregate_data(df_os_final),
                "up": self.aggregate_data(df_up),
                "hr": self.aggregate_data(df_hr)
            }
            
            #Step 7: Update MTD Data
            self.mtd_data(datasets["complete"], mtd_prefix)
            
            # Step 8: Upload all processed files to S3
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
    Main function to demonstrate usage of OrderSummaryProcessor.
    """
    load_dotenv()
    
    try:
        processor = OrderSummaryProcessor(
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_REGION'),
            bucket_name=os.getenv('BUCKET_NAME')
        )
        
        now = datetime.now()
        date_suffix = now.strftime("%Y%m%d")  # Format as YYYYMMDD
        
        order_summary_prefix = f"{os.getenv('ORDER_SUMMARY_FILENAME', '').upper()}{date_suffix}"
        sales_returns_prefix = f"{os.getenv('SALES_RETURNS_FILENAME', '').upper()}{date_suffix}"
        
        output_filenames = {
            "complete": f"{os.getenv('ORDER_OUTPUT_COMPLETE_FILENAME', '').upper()}{date_suffix}.csv",
            "up": f"{os.getenv('ORDER_OUTPUT_UP_FILENAME', '').upper()}{date_suffix}.csv",
            "hr": f"{os.getenv('ORDER_OUTPUT_HR_FILENAME', '').upper()}{date_suffix}.csv"
        }
        
        results = processor.process_complete_pipeline(
            order_summary_filename=order_summary_prefix,
            sales_returns_filename=sales_returns_prefix,
            input_prefix=f"{os.getenv('ORDER_SUMMARY_INPUT_PREFIX').rstrip('/')}/{date_suffix}",
            output_prefix=f"{os.getenv('ORDER_SUMMARY_OUTPUT_PREFIX').rstrip('/')}/{date_suffix}",
            output_filenames=output_filenames,
            mtd_prefix=f"{os.getenv('ORDER_SUMMARY_MTD_PREFIX').rstrip('/')}"
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
