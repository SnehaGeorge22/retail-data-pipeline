"""
S3 Upload Script
Uploads generated retail data to AWS S3
"""

import boto3
import os
from datetime import datetime
from pathlib import Path

class S3Uploader:
    def __init__(self, bucket_name, aws_access_key=None, aws_secret_key=None, region='us-east-1'):
        """
        Initialize S3 uploader
        If credentials are None, will use AWS CLI configuration
        """
        if aws_access_key and aws_secret_key:
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=aws_access_key,
                aws_secret_access_key=aws_secret_key,
                region_name=region
            )
        else:
            # Use default credentials from AWS CLI or environment
            self.s3_client = boto3.client('s3', region_name=region)
        
        self.bucket_name = bucket_name
        
    def create_bucket_if_not_exists(self):
        """Create S3 bucket if it doesn't exist"""
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            print(f"✓ Bucket '{self.bucket_name}' already exists")
        except:
            try:
                self.s3_client.create_bucket(Bucket=self.bucket_name)
                print(f"✓ Created bucket '{self.bucket_name}'")
            except Exception as e:
                print(f"✗ Error creating bucket: {e}")
                raise
    
    def upload_file(self, local_path, s3_path):
        """Upload a single file to S3"""
        try:
            self.s3_client.upload_file(local_path, self.bucket_name, s3_path)
            file_size = os.path.getsize(local_path) / 1024  # KB
            print(f"  ✓ Uploaded {local_path} → s3://{self.bucket_name}/{s3_path} ({file_size:.2f} KB)")
            return True
        except Exception as e:
            print(f"  ✗ Error uploading {local_path}: {e}")
            return False
    
    def upload_directory(self, local_dir, s3_prefix):
        """Upload all CSV files from a directory to S3"""
        local_path = Path(local_dir)
        
        if not local_path.exists():
            print(f"✗ Directory '{local_dir}' does not exist")
            return False
        
        csv_files = list(local_path.glob('*.csv'))
        
        if not csv_files:
            print(f"✗ No CSV files found in '{local_dir}'")
            return False
        
        print(f"\nUploading {len(csv_files)} files from '{local_dir}'...")
        print("-" * 60)
        
        success_count = 0
        for csv_file in csv_files:
            # Create S3 path with date partition
            date_str = datetime.now().strftime('%Y-%m-%d')
            s3_path = f"{s3_prefix}/{csv_file.stem}/date={date_str}/{csv_file.name}"
            
            if self.upload_file(str(csv_file), s3_path):
                success_count += 1
        
        print("-" * 60)
        print(f"✓ Successfully uploaded {success_count}/{len(csv_files)} files")
        return success_count == len(csv_files)
    
    def list_files(self, prefix=''):
        """List files in S3 bucket"""
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix
            )
            
            if 'Contents' in response:
                print(f"\nFiles in s3://{self.bucket_name}/{prefix}:")
                print("-" * 60)
                for obj in response['Contents']:
                    size_kb = obj['Size'] / 1024
                    print(f"  {obj['Key']} ({size_kb:.2f} KB)")
                print("-" * 60)
                print(f"Total: {len(response['Contents'])} files")
            else:
                print(f"No files found in s3://{self.bucket_name}/{prefix}")
        except Exception as e:
            print(f"✗ Error listing files: {e}")

def main():
    # Configuration
    BUCKET_NAME = 'retail-pipeline-data-sg1156644' 
    DATA_DIR = 'data'
    S3_PREFIX = 'raw'
    
    print("=" * 60)
    print("S3 Upload Script - Retail Data Pipeline")
    print("=" * 60)
    
    # Initialize uploader
    uploader = S3Uploader(bucket_name=BUCKET_NAME)
    
    # Create bucket if needed
    uploader.create_bucket_if_not_exists()
    
    # Upload data
    success = uploader.upload_directory(DATA_DIR, S3_PREFIX)
    
    if success:
        print("\n✓ All files uploaded successfully!")
        
        # List uploaded files
        uploader.list_files(prefix=S3_PREFIX)
        
        print("\n" + "=" * 60)
        print("Next Steps:")
        print("1. Configure Snowflake external stage pointing to this S3 bucket")
        print("2. Run the Snowflake setup SQL script")
        print("3. Load data into Snowflake tables")
        print("=" * 60)
    else:
        print("\n✗ Some files failed to upload. Please check the errors above.")

if __name__ == "__main__":
    main()