import os
import logging
import boto3
from botocore.exceptions import ClientError

# Get logger for this module
logger = logging.getLogger(__name__)

class S3StorageManager:
    """
    Manager for S3-compatible storage operations (e.g., MinIO, AWS S3).
    Handles bucket presence checks and file uploads.
    """
    def __init__(self, endpoint_url=None, access_key=None, secret_key=None, bucket_name=None, region_name=None):
        """
        Initializes the S3 client using provided parameters or environment variables.
        
        Args:
            endpoint_url (str, optional): S3 endpoint URL.
            access_key (str, optional): S3 access key.
            secret_key (str, optional): S3 secret key.
            bucket_name (str, optional): Name of the bucket.
            region_name (str, optional): S3 region. Defaults to 'us-east-1'.
        """
        # Pull configuration from parameters or environment variables (Args > Env Vars)
        self.endpoint = endpoint_url or os.environ.get("BUCKET_ENDPOINT")
        logger.debug(f"Endpoint: {self.endpoint}")
        self.access_key = access_key or os.environ.get("BUCKET_ACCESS_KEY")
        logger.debug(f"Access Key: {self.access_key}")
        self.secret_key = secret_key or os.environ.get("BUCKET_SECRET_KEY")
        logger.debug(f"Secret Key: {self.secret_key}")
        self.bucket_name = bucket_name or os.environ.get("BUCKET_NAME")
        logger.debug(f"Bucket Name: {self.bucket_name}")
        self.region = region_name or os.environ.get("BUCKET_REGION", "us-east-1")
        logger.debug(f"Region: {self.region}")

        if not all([self.access_key, self.secret_key, self.bucket_name]):
            logger.warning("S3 Storage not fully configured. Missing BUCKET_ACCESS_KEY, BUCKET_SECRET_KEY, or BUCKET_NAME.")
            self.client = None
            return

        try:
            # Initialize S3 client
            self.client = boto3.client(
                's3',
                endpoint_url=self.endpoint,
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                region_name=self.region
            )
            
            # Ensure bucket exists
            self._ensure_bucket_exists()
        except Exception as e:
            logger.error(f"Error initializing S3 Storage: {e}")
            self.client = None

    def _ensure_bucket_exists(self):
        """Internal method to verify the bucket exists and create it if necessary."""
        if not self.client:
            return
        
        try:
            self.client.head_bucket(Bucket=self.bucket_name)
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                logger.info(f"Bucket {self.bucket_name} not found. Creating it...")
                try:
                    self.client.create_bucket(Bucket=self.bucket_name)
                    logger.info(f"Bucket {self.bucket_name} created successfully.")
                except Exception as ce:
                    logger.error(f"Could not create bucket: {ce}")
            else:
                logger.error(f"Error checking bucket: {e}")

    def upload_file(self, local_path, file_name):
        """
        Upload a local file to the configured S3 bucket.
        
        Args:
            local_path (str): The path to the file on the local filesystem.
            file_name (str): The destination name in the bucket.
            
        Returns:
            str: The public/private URL of the uploaded object, or None if upload fails.
        """
        if not self.client:
            logger.debug("S3 Storage not configured. Skipping upload.")
            return None

        try:
            self.client.upload_file(local_path, self.bucket_name, file_name)
            logger.info(f"Successfully uploaded {file_name} to bucket {self.bucket_name}")
            
            if self.endpoint:
                return f"{self.endpoint.rstrip('/')}/{self.bucket_name}/{file_name}"
            return f"https://{self.bucket_name}.s3.{self.region}.amazonaws.com/{file_name}"
        except Exception as e:
            logger.error(f"Error uploading file to S3: {e}")
            return None

    def list_all_files(self):
        """
        List all object keys in the configured S3 bucket.
        
        Returns:
            list: A list of S3 object keys (strings).
        """
        if not self.client:
            return []
        
        try:
            paginator = self.client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=self.bucket_name)
            
            keys = []
            for page in pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        keys.append(obj['Key'])
            return keys
        except Exception as e:
            logger.error(f"Error listing files in S3: {e}")
            return []

    def get_file_content(self, key):
        """
        Download the content of an S3 object as bytes.
        
        Args:
            key (str): The S3 object key.
            
        Returns:
            bytes: The file content, or None if download fails.
        """
        if not self.client:
            return None
        
        try:
            response = self.client.get_object(Bucket=self.bucket_name, Key=key)
            return response['Body'].read()
        except Exception as e:
            logger.error(f"Error reading file content from S3 ({key}): {e}")
            return None

    def delete_file(self, key):
        """
        Delete an object from the configured S3 bucket.
        
        Args:
            key (str): The S3 object key to delete.
            
        Returns:
            bool: True if deletion successful, False otherwise.
        """
        if not self.client:
            return False
            
        try:
            self.client.delete_object(Bucket=self.bucket_name, Key=key)
            logger.info(f"Deleted file from S3: {key}")
            return True
        except Exception as e:
            logger.error(f"Error deleting file from S3 ({key}): {e}")
            return False

    def get_file_url(self, key):
        """
        Construct the public URL for a given S3 key.
        
        Args:
            key (str): The S3 object key.
            
        Returns:
            str: The constructed URL.
        """
        if self.endpoint:
            return f"{self.endpoint.rstrip('/')}/{self.bucket_name}/{key}"
        return f"https://{self.bucket_name}.s3.{self.region}.amazonaws.com/{key}"
