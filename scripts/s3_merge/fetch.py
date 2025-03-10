import os
import re
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from datetime import datetime

DOWNLOAD_FILES_DIR = "downloaded_files"


def fetch_parquet_files(
    s3_url,
    aws_access_key,
    aws_secret_key,
    bucket_name,
    region,
    username,
    db_name,
    table_name,
    start_date,
    end_date,
    grouping,
):
    """
    Fetch Parquet files from an S3 bucket based on the provided parameters and grouping.

    Args:
        s3_url (str): The S3 endpoint URL.
        aws_access_key (str): AWS access key.
        aws_secret_key (str): AWS secret key.
        bucket_name (str): Name of the S3 bucket.
        region (str): AWS region.
        username (str): Username for filtering files.
        db_name (str): Database name for filtering files.
        table_name (str): Table name for filtering files.
        start_date (str): Start date for filtering files (YYYY-MM-DD).
        end_date (str): End date for filtering files (YYYY-MM-DD).
        grouping (str): Grouping level (hour, day, month, year).

    Returns:
        list: List of tuples containing local file paths and their corresponding datetime info and remote s3 paths.
    """
    # Initialize the S3 client
    s3_client = boto3.client(
        "s3",
        endpoint_url=s3_url,
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=region,
    )

    # Define the prefix for filtering files
    s3_prefix = f"{username}/{db_name}/{table_name}/"

    # List objects in the bucket with the specified prefix
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=s3_prefix)
        if "Contents" not in response:
            print(f"No files found in bucket {bucket_name} with prefix {s3_prefix}")
            return [], []
    except (NoCredentialsError, PartialCredentialsError) as e:
        print(f"Error fetching files from S3: {e}")
        return [], []

    # Define regex patterns for each grouping level
    regex_patterns = {
        "minute": re.compile(
            r"(\d{4})-(\d{2})-(\d{2})_(\d{2})-(\d{2})"
        ),  # yyyy-mm-dd_hh-min
        "hour": re.compile(r"(\d{4})-(\d{2})-(\d{2})_(\d{2})"),  # yyyy-mm-dd_hh
        "day": re.compile(r"(\d{4})-(\d{2})-(\d{2})"),  # yyyy-mm-dd
        "month": re.compile(r"(\d{4})-(\d{2})"),  # yyyy-mm
        "year": re.compile(r"(\d{4})"),  # yyyy
    }

    # Filter files based on the date range and grouping
    filtered_files = []
    for obj in response["Contents"]:
        file_key = obj["Key"]
        # Extract the filename from the file key
        filename = os.path.basename(file_key)

        # Extract the datetime part from the filename
        if filename.startswith(f"{table_name}_"):
            datetime_part = filename[len(f"{table_name}_") : -len(".parquet")]

            # Match the datetime part with the appropriate regex pattern
            match = None
            if grouping == "hour":
                match = regex_patterns["minute"].match(
                    datetime_part
                )  # Minute-level files
            elif grouping == "day":
                match = regex_patterns["hour"].match(datetime_part)  # Hourly files
            elif grouping == "month":
                match = regex_patterns["day"].match(datetime_part)  # Daily files
            elif grouping == "year":
                match = regex_patterns["year"].match(datetime_part)  # Yearly files

            if match:
                # Extract date components from the match
                if grouping == "year":
                    year = match.group(1)
                    file_date = f"{year}-01-01"  # Use January 1st as the default date
                else:
                    year, month, day = match.group(1), match.group(2), match.group(3)
                    file_date = f"{year}-{month}-{day}"

                # Check if the file date is within the specified range
                if start_date <= file_date <= end_date:
                    # Parse the full datetime from the filename
                    try:
                        if grouping == "hour":
                            dt = datetime.strptime(datetime_part, "%Y-%m-%d_%H-%M")
                        elif grouping == "day":
                            dt = datetime.strptime(datetime_part, "%Y-%m-%d_%H")
                        elif grouping == "month":
                            dt = datetime.strptime(datetime_part, "%Y-%m-%d")
                        elif grouping == "year":
                            dt = datetime.strptime(datetime_part, "%Y-%m")
                        filtered_files.append((file_key, dt))
                    except ValueError as e:
                        print(f"Error parsing datetime for file {file_key}: {e}")
                        continue

    # Download the filtered files to a local directory
    local_files = []
    s3_paths = []
    os.makedirs(DOWNLOAD_FILES_DIR, exist_ok=True)

    for file_key, dt in filtered_files:
        local_file_path = os.path.join(DOWNLOAD_FILES_DIR, os.path.basename(file_key))
        try:
            s3_client.download_file(bucket_name, file_key, local_file_path)
            local_files.append((local_file_path, dt))
            s3_paths.append(file_key)
            print(f"Downloaded file: {file_key} to {local_file_path}")
        except Exception as e:
            print(f"Error downloading file {file_key}: {e}")

    return local_files, s3_paths
