# Telegram Echo & Media Storage Bot

A robust Python-based Telegram bot built with `pyTelegramBotAPI` that echoes incoming messages as JSON to the console and automatically stores received media (photos and videos).

## Features

- **Media Handling**: Automatically downloads and stores photos and videos sent to the bot.
- **Flexible Storage Modes**: Choose between `local` disk storage, `S3/MinIO` cloud storage, or `both` simultaneously.
- **Deduplication**: Computes a SHA256 hash per file to prevent storing duplicate content for the same user.
- **Cross-Platform Paths**: Uses Unix-style paths internally (database & S3) and auto-converts to OS-native paths for local disk operations.
- **Robust User Registration**: Finds or creates users by Telegram ID and username, with built-in protection against race conditions.
- **Database Backend**: Tracks users and file metadata (including `created_at` timestamps) using SQLAlchemy with SQLite or PostgreSQL.
- **Auto-Migrations**: Applies database schema updates automatically via Alembic on startup.
- **Centralized Logging**: Configurable log levels (`DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`) for all modules.
- **Strict Validation**: Validates configuration at startup and fails fast with descriptive error messages.
- **Maintenance Tooling**: A dedicated `maintenance.py` script handles background operations (S3 sync, deduplication sweep) without touching the main bot.

## Project Structure

| File / Directory | Purpose |
| :--- | :--- |
| `echo_bot.py` | Main bot entry point — handles polling, media processing, and deduplication. |
| `database.py` | SQLAlchemy models (`User`, `File`) and all database operations. |
| `storage.py` | S3/MinIO storage abstraction (`S3StorageManager`). |
| `maintenance.py` | Standalone maintenance script (S3-to-DB sync, deduplication sweep). |
| `start.sh` | Docker entrypoint — runs Alembic migrations then starts the bot. |
| `alembic/` | Database migration scripts. |
| `files/` | Local storage directory (auto-created at runtime). |

## Installation

1. **Clone the repository**:
   ```bash
   git clone <repo-url>
   cd python-telebot-echo
   ```

2. **Create and activate a virtual environment**:
   ```bash
   python -m venv venv
   .\venv\Scripts\activate  # Windows
   source venv/bin/activate  # Linux/macOS
   ```

3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

## Configuration

Configure the bot using command-line arguments or environment variables. CLI arguments take precedence.

### Bot Settings

| Argument | Environment Variable | Required | Description |
| :--- | :--- | :--- | :--- |
| `--token` | `TELEGRAM_BOT_TOKEN` | **Yes** | Your Telegram Bot Token. |
| `--database-url` | `DATABASE_URL` | No | DB connection string. Defaults to `sqlite:///./bot_database.db`. |
| `--storage-mode` | `STORAGE_MODE` | No | `local`, `s3`, or `both` (default: `local`). |
| `--log-level` | N/A | No | Log verbosity: `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL` (default: `INFO`). |

### S3 / MinIO Settings

Required when `--storage-mode` is `s3` or `both`, and for `maintenance.py --sync-s3`.

| Argument | Environment Variable | Required | Description |
| :--- | :--- | :--- | :--- |
| `--bucket-endpoint` | `BUCKET_ENDPOINT` | No | S3/MinIO custom endpoint URL (omit for AWS). |
| `--bucket-access-key` | `BUCKET_ACCESS_KEY` | **Yes*** | S3 Access Key. |
| `--bucket-secret-key` | `BUCKET_SECRET_KEY` | **Yes*** | S3 Secret Key. |
| `--bucket-name` | `BUCKET_NAME` | **Yes*** | Target bucket name. |
| `--bucket-region` | `BUCKET_REGION` | No | AWS region (default: `us-east-1`). |

## Running the Bot

### Locally

Apply migrations first, then start the bot:
```bash
alembic upgrade head
python echo_bot.py --token <YOUR_TOKEN>
```

With S3 storage enabled:
```bash
python echo_bot.py --token <YOUR_TOKEN> \
  --storage-mode s3 \
  --bucket-name my-bucket \
  --bucket-access-key <KEY> \
  --bucket-secret-key <SECRET> \
  --bucket-endpoint http://localhost:9000
```

### Via Docker

The provided `Dockerfile` and `start.sh` handle migrations and startup automatically:
```bash
docker build -t my-echo-bot .
docker run \
  -e TELEGRAM_BOT_TOKEN=<YOUR_TOKEN> \
  -e STORAGE_MODE=s3 \
  -e BUCKET_NAME=my-bucket \
  -e BUCKET_ACCESS_KEY=<KEY> \
  -e BUCKET_SECRET_KEY=<SECRET> \
  my-echo-bot
```

## Maintenance

`maintenance.py` is a standalone utility for administrative tasks. It connects to the same database and S3 bucket as the bot but exits after completing its task — it never starts the polling loop.

### S3 → Database Sync

Use this to register files already present in S3 that are not yet in the database, and to sweep for hash duplicates:

```bash
python maintenance.py --sync-s3 \
  --database-url <DB_URL> \
  --bucket-name <BUCKET> \
  --bucket-access-key <KEY> \
  --bucket-secret-key <SECRET> \
  --bucket-endpoint http://localhost:9000
```

**What it does:**
1. Lists all objects in the S3 bucket.
2. For each object, downloads the content and computes its SHA256 hash.
3. **Skips** the file if it is already correctly registered in the database (same S3 URL).
4. **Deletes** the file from S3 if its content is a duplicate of another already registered file.
5. **Registers** the file in the database if it is new.
6. Prints a summary of processed / added / deleted / skipped counts.

## Database Migrations

Manage the schema with Alembic:

```bash
# Apply all pending migrations
alembic upgrade head

# Auto-generate a new migration from model changes
$env:DATABASE_URL = "sqlite:///./bot_database.db"
python -m alembic revision --autogenerate -m "description"
```

## License
MIT
