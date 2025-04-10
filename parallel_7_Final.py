import boto3
import pandas as pd
import io
import zipfile
import threading
from concurrent.futures import ThreadPoolExecutor

# Configuration
bucket_name = "p3data"
source_1_prefix = "adf_comparision/engine"
source_2_prefix = "adf_comparision/noeprice"

csv_primary_keys = ["col1", "col2", "col5"]
csv_columns = None  # Or list like ["col1", "col2", "col4", "col5"]

use_multithreading = True  # Set False for sequential

# Initialize session
def get_s3_client(profile_name='default'):
    session = boto3.Session(profile_name=profile_name)
    return session.client('s3')

s3 = get_s3_client(profile_name='your_profile_name_here')

# List .zip files in a source
def list_zip_files(prefix):
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    return [content['Key'] for content in response.get('Contents', []) if content['Key'].endswith('.zip')]

# Read CSVs inside a zip file
def read_zip_from_s3(zip_key):
    zip_obj = s3.get_object(Bucket=bucket_name, Key=zip_key)
    zip_data = zipfile.ZipFile(io.BytesIO(zip_obj['Body'].read()))
    
    csv_files = {}
    for filename in zip_data.namelist():
        if filename.endswith('.csv'):
            with zip_data.open(filename) as f:
                df = pd.read_csv(f)
                csv_files[filename] = df
    return csv_files

# Compare two DataFrames
def compare_csvs(df1, df2, file_name):
    summary = {
        'Missing Columns in File2': [],
        'Missing Columns in File1': [],
        'Missing Rows in File2': 0,
        'Extra Rows in File2': 0,
        'Duplicate Rows in File1': 0,
        'Duplicate Rows in File2': 0
    }
    diff_summary = []

    # Filter columns
    if csv_columns:
        df1 = df1[csv_columns]
        df2 = df2[csv_columns]
    
    # Columns check
    missing_in_file2 = list(set(df1.columns) - set(df2.columns))
    missing_in_file1 = list(set(df2.columns) - set(df1.columns))
    summary['Missing Columns in File2'] = missing_in_file2
    summary['Missing Columns in File1'] = missing_in_file1
    
    common_columns = list(set(df1.columns).intersection(set(df2.columns)))

    # Drop rows with NaNs in primary keys
    df1 = df1.dropna(subset=csv_primary_keys)
    df2 = df2.dropna(subset=csv_primary_keys)

    # Set primary key index
    df1.set_index(csv_primary_keys, inplace=True)
    df2.set_index(csv_primary_keys, inplace=True)

    # Remove duplicates
    summary['Duplicate Rows in File1'] = df1.index.duplicated().sum()
    summary['Duplicate Rows in File2'] = df2.index.duplicated().sum()
    
    df1 = df1[~df1.index.duplicated()]
    df2 = df2[~df2.index.duplicated()]
    
    # Row comparison
    missing_rows = df1.index.difference(df2.index)
    extra_rows = df2.index.difference(df1.index)
    summary['Missing Rows in File2'] = len(missing_rows)
    summary['Extra Rows in File2'] = len(extra_rows)

    # Compare matching rows
    common_idx = df1.index.intersection(df2.index)
    for idx in common_idx:
        row1 = df1.loc[idx]
        row2 = df2.loc[idx]
        for col in common_columns:
            val1 = row1[col] if col in row1 else None
            val2 = row2[col] if col in row2 else None
            if pd.isnull(val1) and pd.isnull(val2):
                continue
            if val1 != val2:
                diff_summary.append({
                    'PrimaryKey': idx,
                    'Column': col,
                    'File1_Value': val1,
                    'File2_Value': val2,
                    'Status': 'Mismatch'
                })
    
    diff_df = pd.DataFrame(diff_summary)
    return diff_df, summary

# Match and compare files
def process_file_pair(file1_name, file2_name, df1, df2):
    print(f"Comparing {file1_name} with {file2_name}")
    return compare_csvs(df1, df2, file1_name)

# Entry point
def run_comparison():
    source1_zips = list_zip_files(source_1_prefix)
    source2_zips = list_zip_files(source_2_prefix)

    def process_zip_pair(zip1_key, zip2_key):
        file_diffs = []
        file_summary = {}
        zip1_csvs = read_zip_from_s3(zip1_key)
        zip2_csvs = read_zip_from_s3(zip2_key)

        common_csvs = set(zip1_csvs.keys()).intersection(zip2_csvs.keys())
        for csv_file in common_csvs:
            df1 = zip1_csvs[csv_file]
            df2 = zip2_csvs[csv_file]
            diff_df, summary = compare_csvs(df1, df2, csv_file)
            if not diff_df.empty:
                file_diffs.append(diff_df)
            file_summary[csv_file] = summary
        return pd.concat(file_diffs) if file_diffs else pd.DataFrame(), file_summary

    zip_pairs = list(zip(source1_zips, source2_zips))

    all_diffs = []
    all_summaries = {}

    if use_multithreading:
        with ThreadPoolExecutor() as executor:
            results = list(executor.map(lambda pair: process_zip_pair(*pair), zip_pairs))
    else:
        results = [process_zip_pair(zip1, zip2) for zip1, zip2 in zip_pairs]

    for diff_df, summary in results:
        if not diff_df.empty:
            all_diffs.append(diff_df)
        all_summaries.update(summary)

    final_diff_df = pd.concat(all_diffs) if all_diffs else pd.DataFrame()
    return final_diff_df, all_summaries

# Run
if __name__ == "__main__":
    diff_df, summary = run_comparison()
    print("Comparison Summary:")
    print(summary)
    if not diff_df.empty:
        print(diff_df.head())
    else:
        print("No differences found.")
