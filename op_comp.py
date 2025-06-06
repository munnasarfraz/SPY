import boto3
import pandas as pd
import io
import zipfile
import threading
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
from datetime import datetime
import re
import html
import configparser
import os
import webbrowser
import numpy as np

# Configuration and Constants
pd.set_option('mode.chained_assignment', None)

# Reading config files
config = configparser.ConfigParser()
config.read('config.ini')

# [settings]
project_name = config['settings']['project_name']
project_logo = config['settings']['project_logo']

# [report]
output_dir = config['report']['output_dir']
output_file = config['report']['output_file']

download_local = config.getboolean('download', 'download_local')

# [keys]
csv_primary_keys = config['keys']['primary_key_columns']
csv_columns = config['keys']['columns']

csv_primary_keys = [col.strip() for col in csv_primary_keys.split(',')] if csv_primary_keys else []
csv_columns = [col.strip() for col in csv_columns.split(',')] if csv_columns else None

# [aws]
bucket_name = config['aws']['bucket_name']
source_1_prefix = config['aws']['source_1_prefix']
source_2_prefix = config['aws']['source_2_prefix']

# [threading]
use_multithreading_reading = config.getboolean('threading', 'use_multithreading_reading')
use_multithreading_comparision = config.getboolean('threading', 'use_multithreading_comparision')

# [report_custom]
include_passed = config.getboolean('report_custom', 'include_passed')
include_missing_files = config.getboolean('report_custom', 'include_missing_files')
include_extra_files = config.getboolean('report_custom', 'include_extra_files')

global_percentage = config['global_col']['global_percentage']
global_percentage = [col.strip() for col in global_percentage.split(',')] if global_percentage else []


# Utility Functions
def create_dir(_dir_path):
    if _dir_path and not os.path.exists(_dir_path):
        os.makedirs(_dir_path)
        return True
    return True

def is_numeric(val):
    try:
        float(val)
        return True
    except (ValueError, TypeError):
        return False

def normalize_value(val):
    """Normalize values for comparison"""
    if pd.isna(val) or val is None or val == np.nan:
        return np.nan
    if isinstance(val, (int, float)):
        return float(val)
    if isinstance(val, str):
        val = val.strip()
        if val.lower() in ('null', 'none', 'nan', ''):
            return np.nan
        try:
            return float(val) if '.' in val else int(val)
        except ValueError:
            return val.lower()
    return val

def values_equal(val1, val2):
    """Enhanced value comparison with type normalization"""
    val1 = normalize_value(val1)
    val2 = normalize_value(val2)
    
    if pd.isna(val1) and pd.isna(val2):
        return True
    if pd.isna(val1) or pd.isna(val2):
        return False
    return val1 == val2

def normalize_filename(filename):
    return re.sub(r'\d{8}_\d{4}', '', os.path.basename(filename))

print_lock = threading.Lock()

def thread_safe_print(*args, **kwargs):
    with print_lock:
        print(*args, **kwargs)

# S3 Functions
def get_s3_client(profile_name='p3-dev'):
    session = boto3.session.Session(profile_name=profile_name)
    return session.client('s3')

s3 = get_s3_client() if not download_local else None

def list_zip_files(prefix, download_local):
    if download_local:
        folder = os.path.join("downloads", prefix)
        if not os.path.exists(folder):
            raise FileNotFoundError(f"Local folder not found: {folder}")
        return [os.path.join(folder, f) for f in os.listdir(folder) if f.endswith('.zip')]
    else:
        try:
            response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
            return [item['Key'] for item in response.get('Contents', []) if item['Key'].endswith('.zip')]
        except Exception as e:
            thread_safe_print(f"❌ {type(e).__name__}: {e}")
            return []

def read_zip_from_local(zip_path):
    csvs = {}
    try:
        with zipfile.ZipFile(zip_path, 'r') as z:
            for file_name in z.namelist():
                if file_name.endswith('.csv'):
                    with z.open(file_name) as f:
                        df = pd.read_csv(f)
                        csvs[file_name] = df
    except Exception as e:
        thread_safe_print(f"❌ Error reading local ZIP {zip_path}: {e}")
    return csvs

def read_zip_from_s3(zip_key):
    zip_obj = s3.get_object(Bucket=bucket_name, Key=zip_key)
    zip_data = zipfile.ZipFile(io.BytesIO(zip_obj['Body'].read()))
    csv_files = {}
    for filename in zip_data.namelist():
        if filename.endswith('.csv'):
            with zip_data.open(filename) as f:
                df = pd.read_csv(f, low_memory=False)
                csv_files[filename] = df
    return csv_files

def read_zip_from_s3_or_local(zip_key, download_local):
    return read_zip_from_local(zip_key) if download_local else read_zip_from_s3(zip_key)

def read_all_csvs_by_source(zip_keys, source_name, download_local, use_multithreading=True):
    all_csvs = {}
    store_lock = threading.Lock()

    def read_zip_and_store(zip_key):
        try:
            csvs = read_zip_from_s3_or_local(zip_key, download_local)
            for name, df in csvs.items():
                with store_lock:
                    if name not in all_csvs:
                        all_csvs[name] = df
                    else:
                        thread_safe_print(f"⚠️ Duplicate CSV: {name} from {zip_key}")
        except Exception as e:
            thread_safe_print(f"❌ Failed to read {zip_key}: {e}")

    if use_multithreading:
        with ThreadPoolExecutor(max_workers=64) as executor:
            list(tqdm(executor.map(read_zip_and_store, zip_keys), total=len(zip_keys),
                      desc=f"Reading ZIPs from {source_name}", unit="zip"))
    else:
        for zip_key in tqdm(zip_keys, desc=f"Reading ZIPs from {source_name}", unit="zip"):
            read_zip_and_store(zip_key)

    return all_csvs

# Comparison Functions
def compare_csvs(df1, df2, file_name):
    """Enhanced CSV comparison with detailed discrepancy tracking"""
    summary = {
        'Missing Columns in Neoprice': [],  # Changed from File2 to Neoprice
        'Missing Columns in Engine': [],    # Changed from File1 to Engine
        'Missing Rows in Neoprice': 0,
        'Extra Rows in Neoprice': 0,
        'Duplicate Rows in Engine': 0,
        'Duplicate Rows in Neoprice': 0,
        'Total Fields Compared': 0,
        'Number of Discrepancies': 0,
        'Field Mismatches': 0,
        'Failure %': 0.0,
        'Pass %': 0.0,
        'Status': 'PASS'
    }
    diff_summary = []

    # Select only specified columns if provided
    if csv_columns:
        df1 = df1[csv_columns]
        df2 = df2[csv_columns]

    # Track column differences with updated names
    summary['Missing Columns in Neoprice'] = list(set(df1.columns) - set(df2.columns))
    summary['Missing Columns in Engine'] = list(set(df2.columns) - set(df1.columns))

    common_columns = list(set(df1.columns).intersection(set(df2.columns)))
    if not common_columns:
        print(f"No common columns to compare in {file_name}")
        return pd.DataFrame(), summary

    # Reset indices and track original row numbers
    df1 = df1.reset_index(drop=True)
    df2 = df2.reset_index(drop=True)
    df1['_original_row'] = df1.index + 1
    df2['_original_row'] = df2.index + 1

    # Clean primary keys
    for key in csv_primary_keys:
        df1[key] = df1[key].astype(str).str.strip()
        df2[key] = df2[key].astype(str).str.strip()
    
    # Convert relevant columns to categorical where appropriate
    for col in csv_primary_keys:
        if len(df1) > 0 and df1[col].nunique() / len(df1) < 0.5:  # If cardinality is < 50%
            df1[col] = df1[col].astype('category')
            df2[col] = df2[col].astype('category')

    # Remove rows with null primary keys
    df1 = df1.dropna(subset=csv_primary_keys)
    df2 = df2.dropna(subset=csv_primary_keys)

    # Set primary keys as index and SORT the index
    df1 = df1.set_index(csv_primary_keys).sort_index()
    df2 = df2.set_index(csv_primary_keys).sort_index()

    # Track duplicates
    summary['Duplicate Rows in Engine'] = df1.index.duplicated().sum()
    summary['Duplicate Rows in Neoprice'] = df2.index.duplicated().sum()
    
    # Get duplicate row details - FIXED PERFORMANCE WARNING
    dup_rows_engine = df1[df1.index.duplicated(keep=False)]
    dup_rows_neoprice = df2[df2.index.duplicated(keep=False)]
    
    # Add duplicate rows to diff summary - optimized access
    for pk in dup_rows_engine.index.unique():
        row_numbers = dup_rows_engine.loc[pk]['_original_row'].tolist()
        diff_summary.append({
            'PrimaryKey': pk,
            'Column': 'DUPLICATE_ROW',
            'Engine_Value': f"Duplicate ({len(row_numbers)} occurrences)",  # Changed from File1
            'Neoprice_Value': '',                                           # Changed from File2
            'RowNum_Engine': ', '.join(map(str, row_numbers)),              # Changed from File1
            'RowNum_Neoprice': '',                                          # Changed from File2
            'Status': 'Duplicate in Engine'                                 # Changed from File1
        })
    
    for pk in dup_rows_neoprice.index.unique():
        row_numbers = dup_rows_neoprice.loc[pk]['_original_row'].tolist()
        diff_summary.append({
            'PrimaryKey': pk,
            'Column': 'DUPLICATE_ROW',
            'Engine_Value': '',                                             # Changed from File1
            'Neoprice_Value': f"Duplicate ({len(row_numbers)} occurrences)", # Changed from File2
            'RowNum_Engine': '',                                            # Changed from File1
            'RowNum_Neoprice': ', '.join(map(str, row_numbers)),            # Changed from File2
            'Status': 'Duplicate in Neoprice'                               # Changed from File2
        })

    # Remove duplicates for further comparison
    df1 = df1[~df1.index.duplicated()]
    df2 = df2[~df2.index.duplicated()]

    # Track missing and extra rows with updated names
    missing_in_neoprice = df1.index.difference(df2.index)
    extra_in_neoprice = df2.index.difference(df1.index)
    
    summary['Missing Rows in Neoprice'] = len(missing_in_neoprice)
    summary['Extra Rows in Neoprice'] = len(extra_in_neoprice)

    # Add missing/extra rows to diff summary with updated names
    for pk in missing_in_neoprice:
        diff_summary.append({
            'PrimaryKey': pk,
            'Column': 'MISSING_ROW',
            'Engine_Value': 'Exists',       # Changed from File1
            'Neoprice_Value': 'Missing',    # Changed from File2
            'RowNum_Engine': df1.loc[pk, '_original_row'],  # Changed access method
            'RowNum_Neoprice': '',
            'Status': 'Missing in Neoprice' # Changed from File2
        })
    
    for pk in extra_in_neoprice:
        diff_summary.append({
            'PrimaryKey': pk,
            'Column': 'EXTRA_ROW',
            'Engine_Value': 'Missing',      # Changed from File1
            'Neoprice_Value': 'Exists',     # Changed from File2
            'RowNum_Engine': '',
            'RowNum_Neoprice': df2.loc[pk, '_original_row'],  # Changed access method
            'Status': 'Extra in Neoprice'  # Changed from File2
        })

    # Compare common rows
    common_idx = df1.index.intersection(df2.index)
    total_fields = 0
    mismatches = 0

    for idx in tqdm(common_idx, desc=f"Comparing rows ({file_name})", unit="rows", ncols=100):
        # Use .loc with tuple for MultiIndex or single value for single index
        if isinstance(df1.index, pd.MultiIndex):
            row1 = df1.loc[tuple(idx)] if isinstance(idx, (list, tuple)) else df1.loc[idx]
            row2 = df2.loc[tuple(idx)] if isinstance(idx, (list, tuple)) else df2.loc[idx]
        else:
            row1 = df1.loc[idx]
            row2 = df2.loc[idx]
            
        row1_number = int(row1['_original_row'])
        row2_number = int(row2['_original_row'])

        for col in common_columns:
            val1 = row1.get(col, None)
            val2 = row2.get(col, None)
            total_fields += 1
            
            if not values_equal(val1, val2):
                mismatches += 1
                diff_summary.append({
                    'PrimaryKey': idx,
                    'Column': col,
                    'Engine_Value': val1,       # Changed from File1
                    'Neoprice_Value': val2,     # Changed from File2
                    'RowNum_Engine': row1_number,
                    'RowNum_Neoprice': row2_number,
                    'Status': 'Mismatch'
                })

    summary['Total Fields Compared'] = total_fields
    summary['Field Mismatches'] = mismatches
    
    # Calculate total discrepancies (including missing/extra rows and duplicates)
    missing_rows = len(missing_in_neoprice)
    extra_rows = len(extra_in_neoprice)
    duplicates = summary['Duplicate Rows in Engine'] + summary['Duplicate Rows in Neoprice']
    field_mismatches = mismatches

    total_discrepancies = missing_rows + extra_rows + duplicates + field_mismatches
    summary['Number of Discrepancies'] = total_discrepancies

    # Calculate total data points (rows from both files + compared fields)
    total_data_points = len(df1) + len(df2) + total_fields

    # Calculate failure percentage
    if total_data_points > 0:
        failure_percent = (total_discrepancies / total_data_points) * 100
        # Force minimum percentage when discrepancies exist
        if failure_percent == 0 and total_discrepancies > 0:
            failure_percent = 0.000001
        summary['Failure %'] = round(failure_percent, 6)
        summary['Pass %'] = round(100 - summary['Failure %'], 6)
    else:
        summary['Failure %'] = 0.0
        summary['Pass %'] = 100.0
    
    # Set overall status
    if total_discrepancies > 0:
        summary['Status'] = 'FAIL'
        summary['Note'] = f'❌ Found {total_discrepancies} discrepancies'
    else:
        summary['Status'] = 'PASS'
        summary['Note'] = '✅ No comparison issues, files are identical'

    diff_df = pd.DataFrame(diff_summary)
    return diff_df, summary

def compare_all_csvs(all_csvs_source1, all_csvs_source2, use_multithreading=True):
    normalized_source1 = {normalize_filename(k): k for k in all_csvs_source1}
    normalized_source2 = {normalize_filename(k): k for k in all_csvs_source2}

    common_csvs = set(normalized_source1) & set(normalized_source2)
    missing_in_source2 = set(normalized_source1) - set(normalized_source2)
    missing_in_source1 = set(normalized_source2) - set(normalized_source1)

    all_diffs = []
    all_summaries = {}

    def process_csv_pair(csv_name):
        df1 = all_csvs_source1[normalized_source1[csv_name]]
        df2 = all_csvs_source2[normalized_source2[csv_name]]
        return csv_name, compare_csvs(df1, df2, csv_name)

    if use_multithreading:
        with ThreadPoolExecutor(max_workers=64) as executor:
            results = list(executor.map(process_csv_pair, common_csvs))
    else:
        results = [process_csv_pair(name) for name in common_csvs]

    for csv_name, (diff_df, summary) in results:
        if diff_df.empty:
            summary['Note'] = '✅ No differences'
        else:
            diff_df['File'] = csv_name
            all_diffs.append(diff_df)
        all_summaries[csv_name] = summary

    if missing_in_source2:
        all_summaries["Missing in Source2"] = list(missing_in_source2)
    if missing_in_source1:
        all_summaries["Extra in Source2"] = list(missing_in_source1)

    final_diff_df = pd.concat(all_diffs) if all_diffs else pd.DataFrame()
    return final_diff_df, all_summaries

# Report Generation
def generate_html_report(
    diff_df, 
    summary, 
    report_start_time, 
    output_file,
    source_files_count, 
    destination_files_count, 
    primary_key_columns=csv_primary_keys,
    columns=csv_columns, 
    project_name=project_name,
    project_logo=project_logo,
    include_passed=True,
    include_missing_files=True,
    include_extra_files=True,
    global_percentage=None
):
    report_end_time = datetime.now()
    time_taken = report_end_time - report_start_time
    time_taken_str = str(time_taken).split('.')[0]  # HH:MM:SS

    comparison_rows = ""

    total_fields_compared = 0
    total_discrepancies = 0
    total_missing_files = 0
    total_missing_rows = 0
    total_extra_rows = 0
    total_different_rows = 0
    total_duplicates = 0

    pass_percent_list = []
    fail_percent_list = []
    
    # Initialize global percentage metrics
    global_percentage_metrics = {}
    if global_percentage:
        for col in global_percentage:
            global_percentage_metrics[col] = {
                'total': 0,
                'mismatches': 0,
                'pass_percent': 0.0,
                'fail_percent': 0.0
            }

    # Enhanced function to format values with proper precision
    def format_value(val):
        if pd.isna(val) or val is None or val == np.nan:
            return ""  # Return empty string instead of "NULL"
        try:
            if isinstance(val, (int, float)):
                if abs(val) > 1000 or abs(val) < 0.01:
                    return "{:.4e}".format(val)
                if float(val).is_integer():
                    return str(int(val))
                return "{:.4f}".format(val).rstrip('0').rstrip('.')
        except (ValueError, TypeError):
            pass
        return str(val)

    # Enhanced function to calculate and format differences
    def calculate_diff(val1, val2):
        try:
            num1 = float(val1)
            num2 = float(val2)
            diff = num1 - num2
            if abs(diff) > 1000 or abs(diff) < 0.01:
                return "{:.4e}".format(diff)
            if diff.is_integer():
                return str(int(diff))
            return "{:.4f}".format(diff).rstrip('0').rstrip('.')
        except (ValueError, TypeError):
            return "N/A"

    # Validate required columns exist in diff_df
    required_columns = ['PrimaryKey', 'Status', 'RowNum_Engine', 'RowNum_Neoprice',
                       'Engine_Value', 'Neoprice_Value', 'Column']
    missing_cols = [col for col in required_columns if col not in diff_df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns in diff_df: {missing_cols}")

    # First count all fields and discrepancies (regardless of include_passed)
    for csv_file, file_summary in summary.items():
        if csv_file in ["Missing in Source2", "Extra in Source2"]:
            continue
        if not isinstance(file_summary, dict):
            continue
        
        fields = file_summary.get('Total Fields Compared', 0)
        if fields:
            total_fields_compared += fields
            total_discrepancies += file_summary.get('Number of Discrepancies', 0)
            total_duplicates += file_summary.get('Duplicate Rows in Engine', 0) + file_summary.get('Duplicate Rows in Neoprice', 0)
            pass_percent_list.append(file_summary.get('Pass %', 0.0))
            fail_percent_list.append(file_summary.get('Failure %', 0.0))
            
            if file_summary.get('Status', 'PASS') == "FAIL":
                total_different_rows += file_summary.get('Number of Discrepancies', 0)
            
            total_missing_rows += file_summary.get('Missing Rows in Neoprice', 0)
            total_extra_rows += file_summary.get('Extra Rows in Neoprice', 0)

    # Then generate the report rows
    for csv_file, file_summary in tqdm(summary.items(), desc="Generating report"):
        if csv_file in ["Missing in Source2", "Extra in Source2"]:
            continue
        if not isinstance(file_summary, dict):
            continue

        file_diff_df = diff_df[diff_df['File'] == csv_file] if not diff_df.empty else pd.DataFrame()
        match_status = file_summary.get('Status', 'PASS')

        if match_status == "PASS" and not include_passed:
            continue  # Skip matched files if flag is off

        icon = "<i class='fas fa-check-circle' style='color:green;'></i>" if match_status == "PASS" else "<i class='fas fa-times-circle' style='color: red;'></i>"

        # Calculate global percentage metrics for this file
        if global_percentage and not file_diff_df.empty:
            for col in global_percentage:
                col_mismatches = file_diff_df[(file_diff_df['Column'] == col) & 
                                            (file_diff_df['Status'] == 'Mismatch')].shape[0]
                total_comparisons = file_diff_df[file_diff_df['Column'] == col].shape[0]
                
                if col in global_percentage_metrics:
                    global_percentage_metrics[col]['total'] += total_comparisons
                    global_percentage_metrics[col]['mismatches'] += col_mismatches
                    
                    # Fixed parentheses for pass_percent calculation
                    if global_percentage_metrics[col]['total'] > 0:
                        global_percentage_metrics[col]['pass_percent'] = round(
                            (1 - (global_percentage_metrics[col]['mismatches'] / 
                            global_percentage_metrics[col]['total'])) * 100, 2
                        )
                        global_percentage_metrics[col]['fail_percent'] = round(
                            (global_percentage_metrics[col]['mismatches'] / 
                            global_percentage_metrics[col]['total']) * 100, 2
                        )
                    else:
                        global_percentage_metrics[col]['pass_percent'] = 100.0
                        global_percentage_metrics[col]['fail_percent'] = 0.0

        # Group rows by PrimaryKey and Status to better organize the display
        diff_groups = {}
        if not file_diff_df.empty:
            for _, row in file_diff_df.iterrows():
                key = (row['PrimaryKey'], row['Status'])
                if key not in diff_groups:
                    diff_groups[key] = {
                        'RowNum_Engine': row['RowNum_Engine'],
                        'RowNum_Neoprice': row['RowNum_Neoprice'],
                        'details': []
                    }
                diff_groups[key]['details'].append(row)

        # Build the difference table rows
        diff_table_rows = []
        for (primary_key, status), group in diff_groups.items():
            rowspan = len(group['details'])
            
            # Header row for this discrepancy group
            header_row = f"""
            <tr>
                <td rowspan="{rowspan}" style="vertical-align: top;">
                    <small>{html.escape(str(primary_key))}</small><br>
                    <small>Engine Row: {group['RowNum_Engine'] or '-'} Neoprice Row: {group['RowNum_Neoprice'] or '-'}</small>
                </td>
            """
            
            # First detail row
            first_detail = group['details'][0]
            diff_value = calculate_diff(first_detail['Engine_Value'], first_detail['Neoprice_Value'])
            diff_class = "positive-diff" if diff_value != "N/A" and float(diff_value) > 0 else "negative-diff" if diff_value != "N/A" and float(diff_value) < 0 else ""
            
            header_row += f"""
                <td><small>{html.escape(str(first_detail['Column']))}</small></td>
                <td><small>{html.escape(format_value(first_detail['Engine_Value']))}</small></td>
                <td><small>{html.escape(format_value(first_detail['Neoprice_Value']))}</small></td>
                <td class="numeric-diff {diff_class}"><small>{diff_value}</small></td>
                <td rowspan="{rowspan}" style="vertical-align: middle;"><small>{status}</small></td>
            </tr>
            """
            diff_table_rows.append(header_row)
            
            # Additional detail rows
            for detail in group['details'][1:]:
                diff_value = calculate_diff(detail['Engine_Value'], detail['Neoprice_Value'])
                diff_class = "positive-diff" if diff_value != "N/A" and float(diff_value) > 0 else "negative-diff" if diff_value != "N/A" and float(diff_value) < 0 else ""
                
                diff_table_rows.append(f"""
                <tr>
                    <td>{html.escape(str(detail['Column']))}</td>
                    <td>{html.escape(format_value(detail['Engine_Value']))}</td>
                    <td>{html.escape(format_value(detail['Neoprice_Value']))}</td>
                    <td class="numeric-diff {diff_class}">{diff_value}</td>
                </tr>
                """)

        diff_table = "".join(diff_table_rows)

        if match_status == "FAIL":
            mismatch_details = f"""
                <div>
                    <button class="toggle-button" onclick="toggleVisibility('diff-{csv_file}', this)">+</button>
                    <span class="summary-badge" style="background-color: #f8d7da; color: #721c24; padding: 2px 6px; border-radius: 4px; margin-left: 5px;">
                        {file_summary.get('Number of Discrepancies', 0)} discrepancies
                        {f"| {file_summary.get('Failure %', 0.0):.6f}% failure" if file_summary.get('Number of Discrepancies', 0) > 0 else ""}
                    </span>
                    <div id="diff-{csv_file}" style="display:none; margin-top: 10px;">
                        <table class="diff-table">
                            <thead>
                                <tr>
                                    <th width="50%">Primary Key</th>
                                    <th width="10%">Column</th>
                                    <th width="10%">Engine</th>
                                    <th width="10%">Neoprice</th>
                                    <th width="10%">Diff</th>
                                    <th width="10%">Status</th>
                                </tr>
                            </thead>
                            <tbody>
                                {diff_table}
                            </tbody>
                        </table>
                        <br>
                        <div class="summary-grid">
                            <div class="summary-item">
                                <span class="summary-label">Missing Columns in Engine:</span>
                                <span class="summary-value">{', '.join(file_summary.get('Missing Columns in Engine', [])) or '0'}</span>
                            </div>
                            <div class="summary-item">
                                <span class="summary-label">Missing Columns in Neoprice:</span>
                                <span class="summary-value">{', '.join(file_summary.get('Missing Columns in Neoprice', [])) or '0'}</span>
                            </div>
                            <div class="summary-item">
                                <span class="summary-label">Missing Rows in Neoprice:</span>
                                <span class="summary-value">{file_summary.get('Missing Rows in Neoprice', 0)}</span>
                            </div>
                            <div class="summary-item">
                                <span class="summary-label">Extra Rows in Neoprice:</span>
                                <span class="summary-value">{file_summary.get('Extra Rows in Neoprice', 0)}</span>
                            </div>
                            <div class="summary-item">
                                <span class="summary-label">Duplicates in Engine:</span>
                                <span class="summary-value">{file_summary.get('Duplicate Rows in Engine', 0)}</span>
                            </div>
                            <div class="summary-item">
                                <span class="summary-label">Duplicates in Neoprice:</span>
                                <span class="summary-value">{file_summary.get('Duplicate Rows in Neoprice', 0)}</span>
                            </div>
                        </div>
                    </div>
                </div>
            """
        else:
            mismatch_details = """
                <div style="color: green;">
                    <i class="fas fa-check-circle"></i> The files are identical. No differences were found during the comparison.
                </div>
            """

        comparison_rows += f"""
        <tr>
            <td><i class="fas fa-file-csv" style="color:blue;"></i> {csv_file}</td>
            <td align="center">{icon}</td>
            <td>{mismatch_details}</td>
        </tr>
        """

    if include_missing_files and "Missing in Source2" in summary:
        total_missing_files = len(summary["Missing in Source2"])
        for missing_csv in summary["Missing in Source2"]:
            comparison_rows += f"""
            <tr>
                <td><i class="fas fa-file-csv" style="color:gray;"></i> {missing_csv}</td>
                <td align="center"><i class="fas fa-exclamation-triangle" style="color:orange;"></i></td>
                <td><strong>Missing in Neoprice</strong></td>
            </tr>
            """
    if include_extra_files and "Extra in Source2" in summary:
        for extra_csv in summary["Extra in Source2"]:
            comparison_rows += f"""
            <tr>
                <td><i class="fas fa-file-csv" style="color:gray;"></i> {extra_csv}</td>
                <td align="center"><i class="fas fa-exclamation-triangle" style="color:blue;"></i></td>
                <td><strong>Extra in Neoprice</strong></td>
            </tr>
            """

    if total_fields_compared > 0:
        overall_pass = round((1 - (total_discrepancies / total_fields_compared)) * 100, 5)
    else:
        overall_pass = 100.0
    if total_fields_compared > 0:
        overall_fail = round((total_discrepancies / total_fields_compared) * 100, 5)
    else:
        overall_fail = 0.0

    fields_compared = total_discrepancies - (total_duplicates + total_missing_rows)

    # Build global percentage metrics section if enabled
    global_percentage_section = ""
    if global_percentage:
        global_percentage_section = "<h2>📈 Column-Specific Metrics</h2>"
        global_percentage_section += "<div class='metrics-container'>"
        
        for col, metrics in global_percentage_metrics.items():
            global_percentage_section += f"""
            <div class="metric-card {'pass-metric' if metrics['fail_percent'] == 0 else 'fail-metric'}">
                <div class="metric-value">{metrics['pass_percent']}%</div>
                <div class="metric-label">{col} Pass Rate</div>
                <div class="metric-subtext">
                    {metrics['mismatches']} of {metrics['total']} mismatches
                </div>
            </div>
            """
        
        global_percentage_section += "</div>"

    html_template = f"""<!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <title>{project_name} Report</title>
        <link rel="icon" type="image/x-icon" href="{project_logo}">
        <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/5.15.4/css/all.min.css">
        <style>
            body {{ font-family: Arial, sans-serif; background: #f4f4f9; margin: 0; padding: 0; }}
            .container {{ background: white; margin: 10px auto; padding: 10px; max-width: 98%; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
            header {{ background-color: #173E72; color: white; padding: 5px; text-align: center; }}
            table {{ width: 100%; border-collapse: collapse; margin-top: 15px; word-break: break-word; table-layout: fixed; }}
            th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
            th {{ background: #173E72; color: white; position: sticky; top: 0; }}
            tr:nth-child(even) {{ background: #f9f9f9; }}
            tr:hover {{ background: #f1f1f1; }}
            .toggle-button {{ font-size: 0.8em; padding: 3px 8px; background-color: #0056b3; color: white; border: none; border-radius: 4px; cursor: pointer; }}
            .summary-badge {{ font-weight: bold; }}
            .diff-table th {{ background: #173E72; }}
            .summary-grid {{
                display: grid;
                grid-template-columns: repeat(3, 1fr);
                gap: 10px;
                margin-top: 15px;
            }}
            .summary-item {{
                background: #f8f9fa;
                padding: 8px;
                border-radius: 4px;
                border-left: 4px solid #173E72;
            }}
            .summary-label {{
                font-weight: bold;
                color: #495057;
            }}
            .summary-value {{
                color: #212529;
                margin-left: 5px;
            }}
            .numeric-diff {{
                font-family: monospace;
                text-align: right;
            }}
            .positive-diff {{ color: #d9534f; }}
            .negative-diff {{ color: #5cb85c; }}
            header img {{
                height: 60px;
                width: auto;
                max-width: 200px;
            }}
            .metrics-container {{
                display: grid;
                grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
                gap: 15px;
                margin: 20px 0;
            }}
            .metric-card {{
                background: white;
                border-radius: 8px;
                padding: 15px;
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                text-align: center;
            }}
            .metric-value {{
                font-size: 24px;
                font-weight: bold;
                margin: 5px 0;
            }}
            .metric-label {{
                color: #6c757d;
                font-size: 14px;
            }}
            .metric-subtext {{
                font-size: 12px;
                color: #6c757d;
                margin-top: 5px;
            }}
            .smaller-text {{
                font-size: 0.9em;
            }}
            .pass-metric {{ border-top: 4px solid #28a745; }}
            .fail-metric {{ border-top: 4px solid #dc3545; }}
            .warn-metric {{ border-top: 4px solid #ffc107; }}
            .neutral-metric {{ border-top: 4px solid #66b2ff; }}
            
        </style>
        <script>
            function toggleVisibility(id, btn) {{
                var el = document.getElementById(id);
                if (el.style.display === 'none') {{
                    el.style.display = 'block';
                    btn.innerHTML = '-';
                }} else {{
                    el.style.display = 'none';
                    btn.innerHTML = '+';
                }}
            }}
        </script>
    </head>
    <body>
        <header>
        <table style="border: none; width: 100%;">
            <tr>
                <td style="border: none; text-align: left; width: 20%; padding: 5px 0;">
                    <img src="{project_logo}" alt="{project_name}" style="height: 40px; width: auto;">
                </td>
                <td style="border: none; text-align: center; padding: 5px 0;">
                    <h1 style="margin: 0; font-size: 1.5em;">{project_name} - CSV Comparison Report</h1>
                    <p style="margin: 2px 0 0; font-size: 0.9em;"> <strong>Generated:</strong> {report_start_time.strftime('%Y-%m-%d %H:%M:%S')} | <strong>Duration:</strong> {time_taken_str}</p>
                </td>
                <td style="border: none; text-align: right; width: 20%; padding: 5px 0;">
                    <div style="font-size: 1.2em; font-weight: bold; color: { '#28a745' if overall_fail == 0 else '#dc3545' }">
                        {overall_pass}% Pass
                    </div>
                </td>
            </tr>
        </table>
    </header>
    <div class="container">
        <h2>📊 Key Metrics</h2>
        <div class="metrics-container">
            <div class="metric-card pass-metric">
                <div class="metric-value">{overall_pass}%</div>
                <div class="metric-label">Overall Pass Rate</div>
            </div>
            <div class="metric-card fail-metric">
                <div class="metric-value">{total_discrepancies}</div>
                <div class="metric-label">Total Discrepancies</div>
            </div>
            <div class="metric-card neutral-metric">
                <div class="metric-value">{total_fields_compared}</div>
                <div class="metric-label">Total Fields Compared</div>
            </div>
            <div class="metric-card warn-metric">
                <div class="metric-value">{total_missing_rows}</div>
                <div class="metric-label">Missing Rows</div>
            </div>
            <div class="metric-card warn-metric">
                <div class="metric-value">{total_extra_rows}</div>
                <div class="metric-label">Extra Rows</div>
            </div>
            <div class="metric-card warn-metric">
                <div class="metric-value">{total_duplicates}</div>
                <div class="metric-label">Duplicate Rows</div>
            </div>
            <div class="metric-card warn-metric">
                <div class="metric-value">{fields_compared}</div>
                <div class="metric-label">Field Discrepancies</div>
            </div>
        </div>

        {global_percentage_section}

        <h2>🔍 Comparison Details</h2>
        <ul>
            <li><strong><i class="fas fa-file-alt"></i> Files in Engine:</strong> {source_files_count}</li>
            <li><strong><i class="fas fa-file-alt"></i> Files in Neoprice:</strong> {destination_files_count}</li>
            <li>
                <strong><i class="fas fa-key"></i> Primary Keys:</strong> 
                <button class="toggle-button" onclick="toggleVisibility('primaryKeys', this)">+</button>
                <span id="primaryKeys" style="display:none;">
                    <span class="smaller-text">{', '.join(primary_key_columns)}</span>
                </span>
            </li>
            <li>
                <strong><i class="fas fa-columns"></i> Compared Columns:</strong>
                <button class="toggle-button" onclick="toggleVisibility('comparedColumns', this)">+</button>
                <span id="comparedColumns" style="display:none;">
                    <span class="smaller-text">{"All Columns" if not columns else ', '.join(columns)}</span>
                </span>
            </li>
        </ul>

        <h2>📝 File Comparison Results</h2>
        <table>
            <thead>
                <tr>
                    <th width="20%" nowrap>CSV File</th>
                    <th width="5%">Status</th>
                    <th>Details</th>
                </tr>
            </thead>
            <tbody>
                {comparison_rows}
            </tbody>
        </table>
    </div>
    </body>
    </html>
    """

    with open(output_file, "w", encoding="utf-8") as f:
        f.write(html_template)

# Main Execution Functions
def run_comparison(download_local=True):
    source1_zips = list_zip_files(source_1_prefix, download_local)
    source2_zips = list_zip_files(source_2_prefix, download_local)

    # For testing: limit to first file
    # source1_zips = [source1_zips[0]]
    # source2_zips = [source2_zips[0]]

    all_csvs_source1 = read_all_csvs_by_source(source1_zips, "source1", download_local, use_multithreading_reading)
    all_csvs_source2 = read_all_csvs_by_source(source2_zips, "source2", download_local, use_multithreading_reading)

    diff_df, summary = compare_all_csvs(all_csvs_source1, all_csvs_source2, use_multithreading_comparision)
    list_files = [len(all_csvs_source1), len(all_csvs_source2)]

    return diff_df, summary, list_files

# Main Execution
if __name__ == "__main__":
    # Setup output file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_name, ext = os.path.splitext(output_file)
    extension = ext if ext else ".html"
    output_file = f"{base_name}_{timestamp}{extension}"
    create_dir(output_dir)
    output_file = os.path.join(output_dir, output_file)

    print('---------------- CSV Comparison Started ----------------')
    start_time = datetime.now()
    diff_df, summary, list_files = run_comparison(download_local=download_local)
    generate_html_report(
        diff_df=diff_df,
        summary=summary,
        report_start_time=start_time,
        output_file=output_file,
        source_files_count=list_files[0],
        destination_files_count=list_files[1],
        primary_key_columns=csv_primary_keys,
        columns=csv_columns,
        project_name=project_name,
        project_logo=project_logo,
        include_passed=include_passed,
        include_missing_files=include_missing_files,
        include_extra_files=include_extra_files,
        #global_percentage=['Fare AMT', 'Difference', 'Fare + CIF AMT']
        #global_percentage=['Difference', 'Fare + CIF AMT']
        global_percentage=global_percentage

    )
    webbrowser.open(output_file)
    print('---------------- CSV Comparison Finished ----------------')