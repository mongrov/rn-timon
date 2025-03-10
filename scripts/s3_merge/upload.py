import boto3
import os
import pyarrow.parquet as pq
import pyarrow as pa
from io import BytesIO
from botocore.exceptions import ClientError


def upload_to_s3(
    s3_url,
    aws_access_key,
    aws_secret_key,
    bucket_name,
    region,
    output_dir,
    username,
    db_name,
    table_name,
    grouping,
    key,
):
    """
    Upload merged Parquet files from a local directory to an S3 bucket.
    If the file already exists in S3, it will be downloaded, merged with the local file,
    and then re-uploaded.

    Args:
        s3_url (str): The S3 endpoint URL.
        aws_access_key (str): AWS access key.
        aws_secret_key (str): AWS secret key.
        bucket_name (str): Name of the S3 bucket.
        region (str): AWS region.
        output_dir (str): Local directory containing the merged Parquet files.
        username (str): Username for the S3 prefix.
        db_name (str): Database name for the S3 prefix.
        table_name (str): Table name for the S3 prefix.
        grouping (str): Grouping level (hour, day, month).
        key (str): Grouping key (e.g., "2025-02-10_10" for hour grouping).

    Returns:
        str: S3 key for the uploaded file.
    """
    # Initialize the S3 client
    s3_client = boto3.client(
        "s3",
        endpoint_url=s3_url,
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=region,
    )

    # Construct the S3 key based on the grouping
    if grouping == "hour":
        # For hour grouping, the key is in the format "yyyy-mm-dd_hh"
        date_part, _ = key.split("_")
        year, month, day = date_part.split("-")
        s3_key = f"{username}/{db_name}/{table_name}/{year}/{month}/{day}/{table_name}_{key}.parquet"
    elif grouping == "day":
        # For day grouping, the key is in the format "yyyy-mm-dd"
        year, month, day = key.split("-")
        s3_key = f"{username}/{db_name}/{table_name}/{year}/{month}/{table_name}_{key}.parquet"
    elif grouping == "month":
        # For month grouping, the key is in the format "yyyy-mm"
        year, month = key.split("-")
        s3_key = f"{username}/{db_name}/{table_name}/{year}/{table_name}_{key}.parquet"
    elif grouping == "year":
        s3_key = f"{username}/{db_name}/{table_name}/{table_name}_{key}.parquet"

    # Path to the local merged file
    merged_file_path = os.path.join(output_dir, f"{table_name}_{key}.parquet")

    # Check if the file already exists in S3
    try:
        s3_client.head_object(Bucket=bucket_name, Key=s3_key)
        print(f"File already exists in S3: s3://{bucket_name}/{s3_key}")

        # Download the existing file from S3
        response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
        existing_file = response["Body"].read()
        existing_table = pq.read_table(BytesIO(existing_file))

        # Read the local file
        local_table = pq.read_table(merged_file_path)

        # Align schemas by reordering columns
        existing_table = existing_table.select(local_table.column_names)
        local_table = local_table.select(local_table.column_names)

        # Merge the existing file with the local file
        merged_table = pa.concat_tables([existing_table, local_table])

        # Save the merged file locally
        pq.write_table(merged_table, merged_file_path)
        print(
            f"Merged local file with existing S3 file and saved to: {merged_file_path}"
        )

    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            print(
                f"File does not exist in S3. Uploading new file: s3://{bucket_name}/{s3_key}"
            )
        else:
            print(f"Error checking or downloading file from S3: {e}")
            return None

    # Upload the merged file (or new file) to S3
    try:
        s3_client.upload_file(merged_file_path, bucket_name, s3_key)
        print(f"Uploaded file: {merged_file_path} to s3://{bucket_name}/{s3_key}")
        return s3_key
    except Exception as e:
        print(f"Error uploading file {merged_file_path} to S3: {e}")
        return None
