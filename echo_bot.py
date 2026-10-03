import os
import json
import argparse
import logging
import telebot
import hashlib
import sys
import tempfile
import re
import requests
import shutil
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


def build_parser():
    """Build command-line argument parser."""
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
    parser.add_argument("--polling-timeout", type=int, default=20, help="Polling timeout (default: 20)")
    parser.add_argument("--polling-interval", type=int, default=0, help="Polling interval (default: 0)")

    x_group = parser.add_mutually_exclusive_group()
    x_group.add_argument("--xwitter-api", help="Xwitter API URL. Can also be set via XWITTER_API environment variable.")
    x_group.add_argument("--x-bridge-api", "--xbridge-api", "--x-bridge", dest="x_bridge_api", help="X-Bridge API URL. Can also be set via X_BRIDGE_API environment variable.")
    return parser


def handle_xwitter_media(bot, message, xwitter_api, storage_mode, s3_storage_manager):
    """Handler for Xwitter API media downloads."""
    url_match = re.search(r'(https?://(?:www\.)?x\.com/[a-zA-Z0-9_]+/status/[0-9]+)', message.text)
    logger.debug(f"URL match result: {url_match}")
    if not url_match:
        return

    target_url = url_match.group(1)
    logger.info(f"Triggering X Media download for {target_url}")
    try:
        bot.send_chat_action(message.chat.id, 'upload_video')
        response = requests.post(xwitter_api, json={"url": target_url}, stream=True)
        if response.status_code == 200:
            content_disposition = response.headers.get("Content-Disposition", "")
            filename = f"xwitter_{message.message_id}.mp4"
            if "filename=" in content_disposition:
                match = re.search(r'filename="?([^"]+)"?', content_disposition)
                if match:
                    filename = match.group(1)

            with tempfile.NamedTemporaryFile(delete=False, suffix=".mp4") as tmp:
                for chunk in response.iter_content(chunk_size=16384):
                    if chunk:
                        tmp.write(chunk)
                tmp_path = tmp.name

            try:
                stored = False
                if storage_mode in ['local', 'both']:
                    local_dir = os.path.join(FILES_DIR, "xwitter_media")
                    os.makedirs(local_dir, exist_ok=True)
                    local_path = os.path.join(local_dir, filename)
                    shutil.copyfile(tmp_path, local_path)
                    logger.info(f"X Media saved locally to: {local_path}")
                    stored = True

                if storage_mode in ['s3', 'both']:
                    if s3_storage_manager:
                        s3_path = f"xwitter_media/permanent/{filename}"
                        upload_url = s3_storage_manager.upload_file(tmp_path, s3_path)
                        if upload_url:
                            logger.info(f"X Media uploaded to S3: {upload_url}")
                            stored = True
                        else:
                            logger.error("Failed to upload to configured bucket")
                    else:
                        logger.error("Storage manager not configured for S3 upload")

                if stored:
                    bot.reply_to(message, "Media file downloaded and stored successfully")
                else:
                    bot.reply_to(message, "Error: Storage manager not configured for upload")
            finally:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
        else:
            logger.debug(f"Response status code: {response.status_code}")
            logger.debug(f"X Media API error text: {response.text}")
            err_msg = "Unknown error"
            try:
                err_data = response.json()
                if "error_message" in err_data:
                    err_msg = err_data["error_message"]
            except Exception:
                pass
            logger.error(f"X Media API error: {response.status_code} - {err_msg}")
            bot.reply_to(message, err_msg)
    except Exception as e:
        logger.error(f"Error calling Xwitter API: {e}", exc_info=True)
        bot.reply_to(message, f"Error processing X URL: {e}")


def handle_x_bridge_media(bot, message, x_bridge_api, storage_mode, s3_storage_manager):
    """
    Downloads media using the python-x-bridge API variant.
    Uses /extract-links for link extraction from text, /download/stream for direct video streaming,
    and /tweet for metadata & photo/image fallback. Saves media according to storage_mode.
    """
    x_pattern = re.compile(
        r'https?://(?:www\.)?(?:x|twitter|fixupx|fxtwitter)\.com/[a-zA-Z0-9_]+/status/\d+',
        re.IGNORECASE,
    )
    if not x_pattern.search(message.text):
        return

    base_url = x_bridge_api.rstrip('/')
    if base_url.endswith('/download/stream'):
        base_url = base_url[:-len('/download/stream')].rstrip('/')
    elif base_url.endswith('/download'):
        base_url = base_url[:-len('/download')].rstrip('/')

    target_urls = []
    try:
        extract_resp = requests.post(
            f"{base_url}/extract-links",
            json={"text": message.text},
            timeout=10,
        )
        if extract_resp.status_code == 200:
            links_data = extract_resp.json().get("links", [])
            for item in links_data:
                url = item.get("canonical_url") or item.get("original_match")
                if url and url not in target_urls:
                    target_urls.append(url)
    except Exception as e:
        logger.warning(f"Error calling X-Bridge /extract-links: {e}. Falling back to regex.")

    if not target_urls:
        for match in x_pattern.finditer(message.text):
            url = match.group(0)
            if url not in target_urls:
                target_urls.append(url)

    if not target_urls:
        return

    for target_url in target_urls:
        logger.info(f"Triggering X-Bridge download for {target_url}")
        try:
            bot.send_chat_action(message.chat.id, 'upload_video')
            stream_endpoint = f"{base_url}/download/stream"
            response = requests.post(stream_endpoint, json={"url": target_url}, stream=True, timeout=60)

            if response.status_code == 200:
                content_disposition = response.headers.get("Content-Disposition", "")
                filename = f"xbridge_{message.message_id}.mp4"
                if "filename=" in content_disposition:
                    match = re.search(r'filename="?([^"]+)"?', content_disposition)
                    if match:
                        filename = match.group(1)

                ext = os.path.splitext(filename)[1] or ".mp4"
                with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as tmp:
                    for chunk in response.iter_content(chunk_size=16384):
                        if chunk:
                            tmp.write(chunk)
                    tmp_path = tmp.name

                try:
                    stored = False
                    if storage_mode in ['local', 'both']:
                        local_dir = os.path.join(FILES_DIR, "x_bridge_media")
                        os.makedirs(local_dir, exist_ok=True)
                        local_path = os.path.join(local_dir, filename)
                        shutil.copyfile(tmp_path, local_path)
                        logger.info(f"X-Bridge media saved locally to: {local_path}")
                        stored = True

                    if storage_mode in ['s3', 'both']:
                        if s3_storage_manager:
                            s3_path = f"x_bridge_media/permanent/{filename}"
                            upload_url = s3_storage_manager.upload_file(tmp_path, s3_path)
                            if upload_url:
                                logger.info(f"X-Bridge media uploaded to S3: {upload_url}")
                                stored = True
                            else:
                                logger.error("Failed to upload X-Bridge media to S3 bucket")
                        else:
                            logger.error("Storage manager not configured for S3 upload")

                    if stored:
                        bot.reply_to(message, "Media file downloaded and stored successfully")
                    else:
                        bot.reply_to(message, "Error: Storage manager not configured for upload")
                finally:
                    if os.path.exists(tmp_path):
                        os.remove(tmp_path)
            else:
                err_msg = "Unknown error"
                try:
                    err_data = response.json()
                    err_msg = err_data.get("error_message") or err_data.get("detail") or "Unknown error"
                except Exception:
                    err_msg = response.text or "Unknown error"

                # If no video found, check /tweet for photo/image media
                if "No video media found" in err_msg:
                    logger.info(f"No video found in post. Checking {base_url}/tweet for photo/image media.")
                    try:
                        tweet_resp = requests.post(f"{base_url}/tweet", json={"url": target_url}, timeout=15)
                        if tweet_resp.status_code == 200:
                            tweet_data = tweet_resp.json()
                            media_items = tweet_data.get("media", [])
                            if media_items:
                                stored_count = 0
                                username = tweet_data.get("username", "x")
                                status_id = tweet_data.get("id", str(message.message_id))
                                for idx, item in enumerate(media_items):
                                    m_url = item.get("url")
                                    m_type = item.get("type", "image")
                                    m_format = item.get("format") or ("jpg" if m_type == "image" else "mp4")
                                    if not m_url:
                                        continue

                                    m_resp = requests.get(m_url, timeout=30)
                                    if m_resp.status_code == 200:
                                        suffix = f"_{idx + 1}" if len(media_items) > 1 else ""
                                        m_filename = f"{username}_{status_id}{suffix}.{m_format}"

                                        with tempfile.NamedTemporaryFile(delete=False, suffix=f".{m_format}") as tmp_m:
                                            tmp_m.write(m_resp.content)
                                            tmp_m_path = tmp_m.name

                                        try:
                                            if storage_mode in ['local', 'both']:
                                                local_dir = os.path.join(FILES_DIR, "x_bridge_media")
                                                os.makedirs(local_dir, exist_ok=True)
                                                shutil.copyfile(tmp_m_path, os.path.join(local_dir, m_filename))
                                                stored_count += 1
                                            if storage_mode in ['s3', 'both'] and s3_storage_manager:
                                                s3_path = f"x_bridge_media/permanent/{m_filename}"
                                                s3_storage_manager.upload_file(tmp_m_path, s3_path)
                                                stored_count += 1
                                        finally:
                                            if os.path.exists(tmp_m_path):
                                                os.remove(tmp_m_path)

                                if stored_count > 0:
                                    bot.reply_to(message, "Media file downloaded and stored successfully")
                                    continue
                                else:
                                    bot.reply_to(message, "Error: Storage manager not configured for upload")
                                    continue
                    except Exception as ex:
                        logger.warning(f"Error querying /tweet for images: {ex}")

                logger.error(f"X-Bridge API error: {response.status_code} - {err_msg}")
                bot.reply_to(message, err_msg)
        except Exception as e:
            logger.error(f"Error calling X-Bridge API: {e}", exc_info=True)
            bot.reply_to(message, f"Error processing X URL: {e}")


def main():
    """
    Main entry point for the Telegram Echo Bot.
    Parses arguments, validates configuration, and starts the polling loop.
    """
    parser = build_parser()
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

    # Polling parameters
    polling_timeout = int(os.environ.get("POLLING_TIMEOUT", args.polling_timeout))
    polling_interval = int(os.environ.get("POLLING_INTERVAL", args.polling_interval))
    logger.debug(f"Polling Timeout: {polling_timeout}, Polling Interval: {polling_interval}")

    xwitter_api = args.xwitter_api or os.environ.get("XWITTER_API")
    x_bridge_api = args.x_bridge_api or os.environ.get("X_BRIDGE_API") or os.environ.get("XBRIDGE_API")
    if xwitter_api and x_bridge_api:
        logger.error("Error: --xwitter-api and --x-bridge-api are mutually exclusive. Please configure only one.")
        sys.exit(1)

    if xwitter_api:
        logger.debug(f"Xwitter API URL configured: {xwitter_api}")
    if x_bridge_api:
        logger.debug(f"X-Bridge API URL configured: {x_bridge_api}")

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
        logger.debug(f"echo_all processing check: message.text exists={bool(message.text)}, xwitter_api={xwitter_api}, x_bridge_api={x_bridge_api}")
        
        if message.text and xwitter_api:
            handle_xwitter_media(bot, message, xwitter_api, storage_mode, s3_storage_manager)
        elif message.text and x_bridge_api:
            handle_x_bridge_media(bot, message, x_bridge_api, storage_mode, s3_storage_manager)

    logger.info(f"Bot is starting polling loop with timeout={polling_timeout} and interval={polling_interval}...")
    try:
        bot.infinity_polling(timeout=polling_timeout, interval=polling_interval)
    except Exception as e:
        logger.error(f"Critical error in polling loop: {e}")

if __name__ == "__main__":
    main()
