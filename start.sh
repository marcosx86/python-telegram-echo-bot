#!/bin/bash
# Apply database migrations
echo -e "Applying database migrations...\n"
alembic upgrade head

# Start the bot, passing any command-line arguments
echo -e "\nStarting the bot...\n"
exec python echo_bot.py "$@"
