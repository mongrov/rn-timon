import os
import argparse

from fetch import fetch_parquet_files
from merge import merge_files
from upload import upload_to_s3
from delete import delete_files_from_s3

DOWNLOAD_FILES_DIR = "downloaded_files"


def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(
        description="Fetch and merge Parquet files from S3."
    )
    parser.add_argument("s3_url", type=str, help="The S3 endpoint URL.")
    parser.add_argument("aws_access_key", type=str, help="AWS access key.")
    parser.add_argument("aws_secret_key", type=str, help="AWS secret key.")
    parser.add_argument("bucket_name", type=str, help="Name of the S3 bucket.")
    parser.add_argument("region", type=str, help="AWS region.")
    parser.add_argument("username", type=str, help="Username for filtering files.")
    parser.add_argument("db_name", type=str, help="Database name for filtering files.")
    parser.add_argument("table_name", type=str, help="Table name for filtering files.")
    parser.add_argument(
        "start_date", type=str, help="Start date for filtering files (YYYY-MM-DD)."
    )
    parser.add_argument(
        "end_date", type=str, help="End date for filtering files (YYYY-MM-DD)."
    )
    parser.add_argument(
        "grouping",
        type=str,
        help="Enum(hour, day, month, year) grouping of parquet files",
    )

    # Parse command-line arguments
    args = parser.parse_args()

    if args.grouping not in ["hour", "day", "month", "year"]:
        print("grouping should be enum(hour, day, month, year)")
        return

    # Fetch Parquet files from S3
    local_paths, s3_paths = fetch_parquet_files(
        args.s3_url,
        args.aws_access_key,
        args.aws_secret_key,
        args.bucket_name,
        args.region,
        args.username,
        args.db_name,
        args.table_name,
        args.start_date,
        args.end_date,
        args.grouping,
    )

    if not local_paths:
        print("No files to merge.")
        exit(1)

    output_dir = "merged_files"
    os.makedirs(output_dir, exist_ok=True)

    # Merge files based on grouping
    if args.grouping == "hour":
        merge_success = merge_files(
            local_paths,
            output_dir,
            lambda dt: dt.strftime("%Y-%m-%d_%H"),
            args.table_name,
        )
        key = local_paths[0][1].strftime("%Y-%m-%d_%H")  # Format: yyyy-mm-dd_hh
    elif args.grouping == "day":
        merge_success = merge_files(
            local_paths, output_dir, lambda dt: dt.strftime("%Y-%m-%d"), args.table_name
        )
        key = local_paths[0][1].strftime("%Y-%m-%d")  # Format: yyyy-mm-dd
    elif args.grouping == "month":
        merge_success = merge_files(
            local_paths, output_dir, lambda dt: dt.strftime("%Y-%m"), args.table_name
        )
        key = local_paths[0][1].strftime("%Y-%m")  # Format: yyyy-mm
    elif args.grouping == "year":
        merge_success = merge_files(
            local_paths, output_dir, lambda dt: dt.strftime("%Y"), args.table_name
        )
        key = local_paths[0][1].strftime("%Y")  # Format: yyyy

    # Upload merged files back to S3
    if merge_success:
        uploaded_key = upload_to_s3(
            args.s3_url,
            args.aws_access_key,
            args.aws_secret_key,
            args.bucket_name,
            args.region,
            output_dir,
            args.username,
            args.db_name,
            args.table_name,
            args.grouping,
            key,
        )
    else:
        uploaded_key = None

    # Delete input files from S3 only if merging and uploading succeeded
    if merge_success and uploaded_key:
        delete_files_from_s3(
            args.s3_url,
            args.aws_access_key,
            args.aws_secret_key,
            args.bucket_name,
            args.region,
            s3_paths,
        )
    else:
        print(
            "Skipping deletion of input files due to errors during merging or uploading."
        )


if __name__ == "__main__":
    main()
