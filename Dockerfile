# Use an official Python runtime as a parent image
FROM python:3.12-slim

# Set the working directory in the container
WORKDIR /app

# Define Build-time Arguments
ARG TELEGRAM_BOT_TOKEN
ARG DATABASE_URL
ARG BUCKET_ENDPOINT
ARG BUCKET_ACCESS_KEY
ARG BUCKET_SECRET_KEY
ARG BUCKET_NAME
ARG BUCKET_REGION
ARG STORAGE_MODE
ARG XWITTER_API
ARG X_BRIDGE_API

# Set Environment Variables (Persisted in the image)
ENV TELEGRAM_BOT_TOKEN=$TELEGRAM_BOT_TOKEN
ENV DATABASE_URL=$DATABASE_URL
ENV BUCKET_ENDPOINT=$BUCKET_ENDPOINT
ENV BUCKET_ACCESS_KEY=$BUCKET_ACCESS_KEY
ENV BUCKET_SECRET_KEY=$BUCKET_SECRET_KEY
ENV BUCKET_NAME=$BUCKET_NAME
ENV BUCKET_REGION=$BUCKET_REGION
ENV STORAGE_MODE=$STORAGE_MODE
ENV XWITTER_API=$XWITTER_API
ENV X_BRIDGE_API=$X_BRIDGE_API

# Install system dependencies if needed (e.g., for psycopg2)
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy the requirements file into the container
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY . .

# Ensure start.sh and files directory are ready
RUN chmod +x start.sh && mkdir -p /app/files

# Expose default port
EXPOSE 8000

# Run the startup script when the container launches
ENTRYPOINT ["./start.sh"]
