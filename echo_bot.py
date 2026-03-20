import os
import json
import argparse
import logging
import telebot
import hashlib
import sys
import tempfile
from telebot import types
from database import DatabaseManager
from storage import S3StorageManager

# Configure logger for this module
logger = logging.getLogger(__name__)

# Directory where files will be stored
FILES_DIR = "files"
if not os.path.exists(FILES_DIR):
    os.makedirs(FILES_DIR)

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


def main():
    """
    Main entry point for the Telegram Echo Bot.
    Parses arguments, validates configuration, and starts the polling loop.
    """
    parser = argparse.ArgumentParser(description="Telegram Echo Bot that prints messages as JSON to console.")
    parser.add_argument("--token", help="Telegram Bot Token. Can also be set via TELEGRAM_BOT_TOKEN environment variable.")
    parser.add_argument("--database-url", help="Database URL. Falls back to SQLite if not provided.")
    parser.add_argument("--bucket-endpoint", help="S3/MinIO endpoint URL.")
    parser.add_argument("--bucket-access-key", help="S3/MinIO access key.")
    parser.add_argument("--bucket-secret-key", help="S3/MinIO secret key.")
    parser.add_argument("--bucket-name", help="S3/MinIO bucket name.")
    parser.add_argument("--bucket-region", help="S3/MinIO region.")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], help="Set the logging level.")
    parser.add_argument("--storage-mode", default="local", choices=["local", "s3", "both"], help="Where to save files: local, s3, or both (default: local)")
    args = parser.parse_args()

    # Configure logging
    log_level = getattr(logging, args.log_level.upper())
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    logger.info(f"Starting bot with log level: {args.log_level}")

    # Configuration from environment variables (overridden by args if provided)
    token = args.token or os.environ.get("TELEGRAM_BOT_TOKEN")
    logger.debug(f"Token: {token}")
    db_url = args.database_url or os.environ.get("DATABASE_URL", "sqlite:///./bot_database.db")
    logger.debug(f"Database URL: {db_url}")
    storage_mode = os.environ.get("STORAGE_MODE", args.storage_mode).lower()
    logger.debug(f"Storage Mode: {storage_mode}")
    
    # S3 details
    bucket_endpoint = args.bucket_endpoint or os.environ.get("BUCKET_ENDPOINT")
    logger.debug(f"Bucket Endpoint: {bucket_endpoint}")
    bucket_name = args.bucket_name or os.environ.get("BUCKET_NAME")
    logger.debug(f"Bucket Name: {bucket_name}")
    bucket_access_key = args.bucket_access_key or os.environ.get("BUCKET_ACCESS_KEY")
    logger.debug(f"Bucket Access Key: {bucket_access_key}")
    bucket_secret_key = args.bucket_secret_key or os.environ.get("BUCKET_SECRET_KEY")
    logger.debug(f"Bucket Secret Key: {bucket_secret_key}")
    bucket_region = args.bucket_region or os.environ.get("BUCKET_REGION", "us-east-1")
    logger.debug(f"Bucket Region: {bucket_region}")

    # 1. Validate Token
    if not token:
        logger.error("Error: TELEGRAM_BOT_TOKEN is required. Provide it via --token or environment variable.")
        sys.exit(1)

    # Validate storage mode
    if storage_mode not in ['local', 's3', 'both']:
        logger.error(f"Error: Invalid storage mode '{storage_mode}'. Choose from 'local', 's3', or 'both'.")
        sys.exit(1)

    # Setup Database
    from database import setup_database
    setup_database(db_url)
    db_manager = DatabaseManager()
    logger.info(f"Database initialized with URL: {db_url}")

    # Initialize S3 Storage if needed (normal operation or sync mode)
    s3_storage_manager = None
    if storage_mode in ['s3', 'both']:
        if not all([bucket_name, bucket_access_key, bucket_secret_key]):
            logger.error("Error: S3 storage (required for mode '%s') requires BUCKET_NAME, BUCKET_ACCESS_KEY, and BUCKET_SECRET_KEY.", storage_mode)
            sys.exit(1)
        
        s3_storage_manager = S3StorageManager(
            endpoint_url=bucket_endpoint,
            access_key=bucket_access_key,
            secret_key=bucket_secret_key,
            bucket_name=bucket_name,
            region_name=bucket_region
        )
        if s3_storage_manager.client:
            logger.info(f"S3 storage configured for bucket: {bucket_name}")
        else:
            logger.error("Error: Failed to initialize S3 storage client.")
    # (S3 Storage Manager initialization completed above)

    # Validate token for bot operation
    if not token:
        logger.error("Error: TELEGRAM_BOT_TOKEN is required for bot operation.")
        sys.exit(1)

    bot = telebot.TeleBot(token)

    @bot.message_handler(content_types=['photo', 'video'])
    def handle_photos_videos(message):
        """Handler for photo and video messages. Downloads, hashes, and uploads the media."""
        # Register/Update user
        user = db_manager.register_user(message.from_user)
        
        # Extract file info
        file_id = ""
        file_unique_id = ""
        file_type = message.content_type
        
        if file_type == 'photo':
            photo = message.photo[-1]
            file_id = photo.file_id
            file_unique_id = photo.file_unique_id
        elif file_type == 'video':
            file_id = message.video.file_id
            file_unique_id = message.video.file_unique_id
            
        try:
            logger.debug(f"Processing {file_type} from {message.from_user.username} (Mode: {storage_mode})")
            file_info = bot.get_file(file_id)
            downloaded_file = bot.download_file(file_info.file_path)
            
            # Compute SHA256
            sha256 = calculate_sha256(downloaded_file)
            logger.debug(f"Computed SHA256: {sha256}")
            
            # Check for duplicates for this user
            existing_file = db_manager.get_file_by_hash(user.id, sha256)
            if existing_file:
                logger.info(f"File already exists for user {user.username} (SHA256: {sha256[:10]}...). Skipping storage.")
                return

            file_extension = os.path.splitext(file_info.file_path)[1]
            # local_filename is used as a Unix-style relative path (user/file) for DB/S3
            local_filename = f"{user.username}/{file_unique_id}{file_extension}"
            
            local_fs_path = None
            db_path = None
            remote_url = None

            # Handle Local Storage
            if storage_mode in ['local', 'both']:
                # For local FS, build the correct OS path (e.g., using \ on Windows)
                local_fs_path = os.path.join(FILES_DIR, local_filename.replace('/', os.sep))
                os.makedirs(os.path.dirname(local_fs_path), exist_ok=True)
                with open(local_fs_path, 'wb') as f:
                    f.write(downloaded_file)
                logger.debug(f"Saved locally to: {local_fs_path}")

            # Standardized Unix-style path for Database
            db_path = f"{FILES_DIR}/{local_filename}"

            # Handle S3 Storage
            if storage_mode in ['s3', 'both'] and s3_storage_manager:
                # To upload to S3, use the local_fs_path if it exists, 
                # otherwise create a temporary file
                upload_source = local_fs_path
                temp_file_path = None
                
                if not upload_source:
                    with tempfile.NamedTemporaryFile(delete=False) as tmp:
                        tmp.write(downloaded_file)
                        temp_file_path = tmp.name
                    upload_source = temp_file_path
                
                try:
                    # S3 handles local_filename (Unix-style) as the key
                    remote_url = s3_storage_manager.upload_file(upload_source, local_filename)
                finally:
                    if temp_file_path and os.path.exists(temp_file_path):
                        os.remove(temp_file_path)
                
                logger.debug(f"Uploaded to S3: {remote_url}")

            # Save to database using the Unix-style db_path
            db_manager.save_file_metadata(user.telegram_id, file_id, file_unique_id, file_type, sha256, db_path)
            
            logger.info(f"Media received: {file_type} processed. Mode: {storage_mode}. Duplicate: No.")
            if remote_url:
                logger.debug(f"Remote URL: {remote_url}")
            
        except Exception as e:
            logger.error(f"Error processing file {file_id}: {e}", exc_info=True)

    @bot.message_handler(func=lambda message: True)
    def echo_all(message):
        """Handler for all other text-based messages. Registers user and echoes JSON to debug logs."""
        # Register/Update user
        db_manager.register_user(message.from_user)
        
        # Convert message object to dictionary
        msg_dict = message.json
        logger.info(f"Message from {message.from_user.username}: {message.text}")
        logger.debug(f"Full message JSON: {json.dumps(msg_dict, indent=4, ensure_ascii=False)}")

    logger.info("Bot is starting polling loop...")
    try:
        bot.infinity_polling()
    except Exception as e:
        logger.error(f"Critical error in polling loop: {e}")

if __name__ == "__main__":
    main()
