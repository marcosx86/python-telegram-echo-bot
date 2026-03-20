import os
import argparse
import logging
import hashlib
import sys
from database import DatabaseManager, setup_database
from storage import S3StorageManager

# Configure logger for this module
logger = logging.getLogger(__name__)

def calculate_sha256(content):
    """
    Compute the SHA256 hash of a byte string.
    
    Args:
        content (bytes): The file content to hash.
        
    Returns:
        str: The hex digest of the hash.
    """
    sha256_hash = hashlib.sha256()
    sha256_hash.update(content)
    return sha256_hash.hexdigest()

def sync_s3_to_db(db_manager, s3_storage_manager):
    """
    Synchronizes files from S3 bucket to the database.
    Performs deduplication and cleaning of S3 bucket.
    """
    logger.info("Starting S3 to Database sync (Maintenance Mode)...")
    keys = s3_storage_manager.list_all_files()
    logger.info(f"Found {len(keys)} files in S3 bucket.")

    processed = 0
    added = 0
    deleted = 0
    skipped = 0

    for key in keys:
        processed += 1
        # Expected key format: username/file_unique_id.extension
        parts = key.split('/')
        if len(parts) != 2:
            logger.warning(f"Skipping key with unexpected format: {key}")
            skipped += 1
            continue
        
        username = parts[0]
        filename = parts[1]
        file_unique_id = os.path.splitext(filename)[0]
        
        # 1. Get user
        user = db_manager.get_user_by_username(username)
        if not user:
            logger.warning(f"User '{username}' not found in database for key '{key}'. Skipping.")
            skipped += 1
            continue
            
        # 2. Download and hash
        content = s3_storage_manager.get_file_content(key)
        if content is None:
            logger.error(f"Failed to read content for {key}. Skipping.")
            skipped += 1
            continue
            
        if len(content) == 0:
            logger.warning(f"File {key} is empty (0 bytes). Skipping.")
            skipped += 1
            continue

        sha256 = calculate_sha256(content)
        logger.debug(f"Processing {key}: size={len(content)}, hash={sha256[:15]}...")
        
        # 3. Check for duplicates in DB
        existing_file = db_manager.get_file_by_hash(user.id, sha256)
        
        # Determine the URL for this current S3 key to compare with local_path in DB
        current_file_url = s3_storage_manager.get_file_url(key)

        if existing_file:
            # If the "duplicate" found in DB is actually THE SAME S3 FILE, do nothing
            if existing_file.local_path == current_file_url:
                logger.info(f"File {key} is already correctly registered in DB. Skipping.")
                skipped += 1
                continue
                
            # Duplicate content in a DIFFERENT S3 key -> Deduplicate if requested
            logger.info(f"Duplicate content found for {username} (Hash: {sha256[:10]}...).")
            logger.info(f"Existing in DB: {existing_file.local_path}")
            logger.info(f"Current S3 key: {key}")
            
            logger.warning(f"Deleting duplicate S3 object: {key}")
            s3_storage_manager.delete_file(key)
            deleted += 1
        else:
            # Not in DB, register it
            ext = os.path.splitext(filename)[1].lower()
            file_type = 'photo' if ext in ['.jpg', '.jpeg', '.png'] else 'video' if ext in ['.mp4', '.mov'] else 'unknown'
            
            # Use the URL as local_path for consistency with normal bot operation
            db_manager.save_file_metadata(user.telegram_id, file_unique_id, file_unique_id, file_type, sha256, current_file_url)
            logger.info(f"Registered new file in DB: {key} -> {current_file_url}")
            added += 1

    logger.info("-" * 30)
    logger.info("Sync Complete Summary:")
    logger.info(f"Total Processed: {processed}")
    logger.info(f"Added to DB:     {added}")
    logger.info(f"Deleted from S3: {deleted}")
    logger.info(f"Skipped:         {skipped}")
    logger.info("-" * 30)

def main():
    """
    Main entry point for maintenance script.
    """
    parser = argparse.ArgumentParser(description="Maintenance script for the Telegram Echo Bot.")
    parser.add_argument("--database-url", help="Database URL. Falls back to SQLite if not provided.")
    parser.add_argument("--bucket-endpoint", help="S3/MinIO endpoint URL.")
    parser.add_argument("--bucket-access-key", help="S3/MinIO access key.")
    parser.add_argument("--bucket-secret-key", help="S3/MinIO secret key.")
    parser.add_argument("--bucket-name", help="S3/MinIO bucket name.")
    parser.add_argument("--bucket-region", help="S3/MinIO region.")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], help="Set the logging level.")
    parser.add_argument("--sync-s3", action="store_true", help="Maintenance: Sync S3 files to database, deduplicate.")
    args = parser.parse_args()

    # Configure logging
    log_level = getattr(logging, args.log_level.upper())
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Configuration from environment variables (overridden by args if provided)
    db_url = args.database_url or os.environ.get("DATABASE_URL", "sqlite:///./bot_database.db")
    
    # S3 details
    bucket_endpoint = args.bucket_endpoint or os.environ.get("BUCKET_ENDPOINT")
    bucket_name = args.bucket_name or os.environ.get("BUCKET_NAME")
    bucket_access_key = args.bucket_access_key or os.environ.get("BUCKET_ACCESS_KEY")
    bucket_secret_key = args.bucket_secret_key or os.environ.get("BUCKET_SECRET_KEY")
    bucket_region = args.bucket_region or os.environ.get("BUCKET_REGION", "us-east-1")

    # Setup Database
    setup_database(db_url)
    db_manager = DatabaseManager()

    # Initialize S3 Storage Manager
    if args.sync_s3:
        if not all([bucket_name, bucket_access_key, bucket_secret_key]):
            logger.error("Error: --sync-s3 requires BUCKET_NAME, BUCKET_ACCESS_KEY, and BUCKET_SECRET_KEY.")
            sys.exit(1)
            
        s3_storage_manager = S3StorageManager(
            endpoint_url=bucket_endpoint,
            access_key=bucket_access_key,
            secret_key=bucket_secret_key,
            bucket_name=bucket_name,
            region_name=bucket_region
        )
        
        if not s3_storage_manager.client:
            logger.error("Error: Failed to initialize S3 storage client.")
            sys.exit(1)
            
        sync_s3_to_db(db_manager, s3_storage_manager)
    else:
        logger.warning("No maintenance action specified. Use --sync-s3.")

if __name__ == "__main__":
    main()
