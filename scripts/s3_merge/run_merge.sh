#!/bin/bash

# Check if the correct number of arguments is provided
if [ "$#" -ne 3 ]; then
  echo "Usage: $0 START_DATE END_DATE GROUPING"
  echo "Example: ./run_merge.sh 2025-01-01 2025-12-30 day"
  exit 1
fi

# Define the command arguments
S3_URL="https://s3.us-west-2.amazonaws.com"
AWS_ACCESS_KEY=${AWS_ACCESS_KEY}  # Read from environment variable
AWS_SECRET_KEY=${AWS_SECRET_KEY}  # Read from environment variable
BUCKET_NAME="zivaoneapp"
REGION="us-west-2"
USERNAME="ahmed_test"
DB_NAME="zivaring"
TABLE_NAME="activitydetails"

# Assign command-line arguments
START_DATE="$1"
END_DATE="$2"
GROUPING="$3"

# Validate START_DATE and END_DATE format
if ! [[ "$START_DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || ! [[ "$END_DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
  echo "Error: START_DATE and END_DATE must be in the format YYYY-MM-DD."
  exit 1
fi

# Validate GROUPING value
if [[ "$GROUPING" != "hour" && "$GROUPING" != "day" && "$GROUPING" != "month" && "$GROUPING" != "year" ]]; then
  echo "Error: GROUPING must be one of: hour, day, month, year."
  exit 1
fi

# Execute the Python script with the provided arguments
python3 main.py \
  "$S3_URL" "$AWS_ACCESS_KEY" "$AWS_SECRET_KEY" \
  "$BUCKET_NAME" "$REGION" "$USERNAME" "$DB_NAME" "$TABLE_NAME" \
  "$START_DATE" "$END_DATE" "$GROUPING"
