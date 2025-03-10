import os
import shutil
import pyarrow.parquet as pq
import pyarrow as pa

DOWNLOAD_FILES_DIR = "downloaded_files"


def merge_files(file_paths, output_path, group_key, table_name):
    """
    Merge multiple Parquet files into a single Parquet file using pyarrow, grouped by a specific key.

    Args:
        file_paths (list): List of tuples containing file paths and their corresponding datetime info.
        output_path (str): Directory to save the merged Parquet files.
        group_key (function): Function to extract the grouping key from the datetime.
        table_name (str): Table name for the merged file naming.

    Returns:
        bool: True if merging is successful, False otherwise.
    """
    try:
        # Group files by the provided key
        grouped_files = {}
        for file_path, dt in file_paths:
            key = group_key(dt)
            if key not in grouped_files:
                grouped_files[key] = []
            grouped_files[key].append(file_path)

        # Merge files for each group
        for key, files in grouped_files.items():
            # Read all Parquet files into PyArrow Tables
            tables = []
            for file_path in files:
                print(f"Reading file: {file_path}")
                table = pq.read_table(file_path)
                tables.append(table)

            # Find the common columns across all tables
            common_columns = set(tables[0].schema.names)
            for table in tables[1:]:
                common_columns.intersection_update(table.schema.names)

            # Convert the set to a list for consistent ordering
            common_columns = list(common_columns)

            # Reorder columns in each table to match the common columns
            reordered_tables = []
            for table in tables:
                # Select only the common columns
                reordered_table = table.select(common_columns)
                reordered_tables.append(reordered_table)

            # Concatenate all PyArrow Tables into a single Table
            merged_table = pa.concat_tables(reordered_tables)

            # Write the merged Table to a new Parquet file
            merged_file_path = os.path.join(output_path, f"{table_name}_{key}.parquet")
            print(f"Writing merged file to: {merged_file_path}")
            pq.write_table(merged_table, merged_file_path)

        print("Merge completed successfully!")
        return True

    except Exception as e:
        print(f"Error during merging: {e}")
        return False

    finally:
        # Cleanup: Delete the downloaded_files directory after merging
        if os.path.exists(DOWNLOAD_FILES_DIR):
            print(f"Deleting directory: {DOWNLOAD_FILES_DIR}")
            shutil.rmtree(DOWNLOAD_FILES_DIR)
