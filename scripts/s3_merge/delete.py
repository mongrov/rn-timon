import boto3


def delete_files_from_s3(
    s3_url,
    aws_access_key,
    aws_secret_key,
    bucket_name,
    region,
    s3_paths,
):
    """
    Delete files from S3 after they have been merged and uploaded.

    Args:
        s3_url (str): The S3 endpoint URL.
        aws_access_key (str): AWS access key.
        aws_secret_key (str): AWS secret key.
        bucket_name (str): Name of the S3 bucket.
        region (str): AWS region.
        s3_paths (list): List containing s3 remote file paths.
    """
    # Initialize the S3 client
    s3_client = boto3.client(
        "s3",
        endpoint_url=s3_url,
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=region,
    )

    # Delete each file in the s3_paths list
    for s3_file_path in s3_paths:
        try:
            s3_client.delete_object(Bucket=bucket_name, Key=s3_file_path)
            print(f"Deleted file from cloud: s3://{bucket_name}/{s3_file_path}")
        except Exception as e:
            print(f"Error deleting file s3://{bucket_name}/{s3_file_path}: {e}")
