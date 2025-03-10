# Overview

This script is designed to fetch, merge, and manage Parquet files stored in an S3 bucket. It supports grouping files at different levels of granularity (`hour`, `day`, `month`, and `year`) and performs the following operations:

1. **Fetching Files**:
   - The script connects to an S3 bucket using the provided AWS credentials and fetches Parquet files based on the specified prefix, date range, and grouping level.
   - It uses regex patterns to match filenames and extract datetime information for filtering.

2. **Merging Files**:
   - Files are merged based on the specified grouping level:
     - **Hourly**: Merges minute-level files into hourly files.
     - **Daily**: Merges hourly-level files into daily files.
     - **Monthly**: Merges daily-level files into monthly files.
     - **Yearly**: Merges monthly-level files into yearly files.
   - The merged files are saved locally in the `merged_files` directory.

3. **Uploading Merged Files**:
   - After merging, the script uploads the merged files back to the S3 bucket under a directory structure that reflects the grouping level. The structure is organized hierarchically based on the granularity of the merged files:
     - **Minute-Level Files** (not merged, but the starting point for "hour" grouping and represent the lowest granularity):
       ```
       s3://<bucket_name>/<username>/<db_name>/<table_name>/YYYY/MM/DD/HH/<file>.parquet
       ```

     - **Hour-Level Files** (merged from minute-level files):
       ```
       s3://<bucket_name>/<username>/<db_name>/<table_name>/YYYY/MM/DD/<file>.parquet
       ```

     - **Day-Level Files** (merged from hour-level files):
       ```
       s3://<bucket_name>/<username>/<db_name>/<table_name>/YYYY/MM/<file>.parquet
       ```

     - **Month-Level Files** (merged from day-level files):
       ```
       s3://<bucket_name>/<username>/<db_name>/<table_name>/YYYY/<file>.parquet
       ```

     - **Year-Level Files** (merged from month-level files):
       ```
       s3://<bucket_name>/<username>/<db_name>/<table_name>/<file>.parquet
       ```

   - The directory structure ensures that files are organized logically and can be easily queried or processed based on their granularity.

4. **Handling Merged File Conflicts**
  - When uploading merged files to S3, if a file with the same key (e.g., timestamp grouping) already exists, the script:  
    1. Downloads the existing file from S3.  
    2. Merges it with the local aggregated file(hourly, daily or whatever grouping we run to produce the file).
    3. Uploads the updated file back to S3, replacing the existing one.  

  If the file does not exist in S3, the local file is uploaded as a new file. This ensures data integrity and avoids conflicts when running periodic aggregations on overlapping datasets.

5. **Deleting Original Files**:
   - If the merge and upload operations are successful, the original files are deleted from the S3 bucket to free up space and make sure we don't produce dupicated files in case we run the same aggregation again.

# Setup Guide

## Setting Up the Python Virtual Environment

1. Create a virtual environment:
   ```sh
   python3 -m venv venv
   ```

2. Activate the virtual environment:
   - On macOS/Linux:
     ```sh
     source venv/bin/activate
     ```
   - On Windows:
     ```sh
     venv\Scripts\activate
     ```

## Installing Dependencies

Once the virtual environment is activated, install the required dependencies:
```sh
pip install -r requirements.txt
```

## Running the Script

### Setting Up AWS Credentials
To configure your AWS credentials, run the following commands in your terminal:

```sh
export AWS_ACCESS_KEY="your_access_key"
export AWS_SECRET_KEY="your_secret_key"
```
Replace `your_access_key` and `your_secret_key` with your actual AWS credentials.

### Executing the Merge Script

Once your credentials are set, you can run the merge script by executing the following command with the required arguments:

```sh
./run_merge.sh START_DATE END_DATE GROUPING
```

#### Arguments:
- `START_DATE`: The start date for the data processing in `YYYY-MM-DD` format.
- `END_DATE`: The end date for the data processing in `YYYY-MM-DD` format.
- `GROUPING`: The grouping interval for the data. Must be one of: `hour`, `day`, `month`, or `year`.

#### Example:
```sh
./run_merge.sh 2025-01-01 2025-12-30 day
```
This command will process data from `2025-01-01` to `2025-12-30`, grouped by `day`.

### Additional Notes
- Ensure the script (`run_merge.sh`) has executable permissions. If not, grant them using:
  ```sh
  chmod +x run_merge.sh
  ```
- If you encounter any issues, verify your AWS credentials and ensure they have the required permissions.
- Ensure the dates are in the correct `YYYY-MM-DD` format.
- The `GROUPING` value must be one of the allowed options (`hour`, `day`, `month`, `year`).
