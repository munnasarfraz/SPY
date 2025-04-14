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


#reading config files 
config = configparser.ConfigParser()
config.read('config.ini')

#[settings]
project_name = config['settings']['project_name']
project_logo = config['settings']['project_logo']

#[report]
output_dir = config['report']['output_dir']
output_file = config['report']['output_file']

#[keys]
csv_primary_keys = config['keys']['primary_key_columns'].split(',')
csv_columns = config['keys']['columns']
if csv_columns is not None:
    csv_columns = csv_columns.split(',')

#[aws]
bucket_name = config['aws']['bucket_name']
source_1_prefix = config['aws']['source_1_prefix']
source_2_prefix = config['aws']['source_2_prefix']

#[threading]
use_multithreading_reading = config['threading']['use_multithreading_reading']
use_multithreading_comparision = config['threading']['use_multithreading_comparision']

#[report_custom]
include_passed = bool(config['report_custom']['include_passed'])
include_missing_files = bool(config['report_custom']['include_missing_files'])

def create_dir(_dir_path):
    if _dir_path != '':
        if not os.path.exists(_dir_path):
            os.makedirs(_dir_path)
            return True
    return True

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
base_name, ext = os.path.splitext(output_file)
extension = ext if ext else ".html"


output_file = f"{base_name}_{timestamp}{extension}"
create_dir(output_dir)
output_file = '/'+output_dir+'/'+output_file

def normalize_filename(filename):
    # Use regex to remove the date-time part in the format: YYYYMMDD_HHMM
    return re.sub(r'\d{8}_\d{4}', '', filename)

if csv_primary_keys == '':
    csv_primary_keys = None

if csv_columns == '':
    csv_columns = None 

# Initialize session
def get_s3_client(profile_name='p3-dev'):
    session = boto3.session.Session(profile_name='p3-dev')
    return session.client('s3')

s3 = get_s3_client(profile_name='p3-dev')

# List .zip files in a source
def list_zip_files(prefix):
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    return [content['Key'] for content in response.get('Contents', []) if content['Key'].endswith('.zip')]

# Read CSVs inside a zip file
def read_zip_from_s3(zip_key):
    zip_obj = s3.get_object(Bucket=bucket_name, Key=zip_key)
    zip_data = zipfile.ZipFile(io.BytesIO(zip_obj['Body'].read()))
    
    csv_files = {}
    for filename in tqdm(zip_data.namelist(), desc=f"CSVs in {zip_key.split('/')[-1]}", leave=False, unit="csv"):
        #for name, df in tqdm(csvs.items(), desc=f"CSVs in {zip_key.split('/')[-1]}", leave=False, unit="csv"):
        if filename.endswith('.csv'):
            with zip_data.open(filename) as f:
                df = pd.read_csv(f, low_memory=False)
                csv_files[filename] = df
    return csv_files

def compare_csvs(df1, df2, file_name):
    summary = {
        'Missing Columns in File2': [],
        'Missing Columns in File1': [],
        'Missing Rows in File2': 0,
        'Extra Rows in File2': 0,
        'Duplicate Rows in File1': 0,
        'Duplicate Rows in File2': 0,
        'Total Fields Compared': 0,
        'Number of Discrepancies': 0,
        'Failure %': 0.0,
        'Pass %': 0.0
    }
    diff_summary = []

    # Filter columns if specified
    if csv_columns:
        df1 = df1[csv_columns]
        df2 = df2[csv_columns]

    # Identify missing columns
    summary['Missing Columns in File2'] = list(set(df1.columns) - set(df2.columns))
    summary['Missing Columns in File1'] = list(set(df2.columns) - set(df1.columns))

    # Only use common columns for comparison
    common_columns = list(set(df1.columns).intersection(set(df2.columns)))
    if not common_columns:
        print(f"No common columns to compare in {file_name}")
        return pd.DataFrame(), summary

    # Reset index and store original row numbers before filtering/indexing
    df1 = df1.reset_index(drop=True)
    df2 = df2.reset_index(drop=True)
    df1['_original_row'] = df1.index + 1  # 1-based indexing (optional)
    df2['_original_row'] = df2.index + 1

    # Drop rows with missing primary keys
    df1 = df1.dropna(subset=csv_primary_keys)
    df2 = df2.dropna(subset=csv_primary_keys)

    # Set primary key as index
    df1.set_index(csv_primary_keys, inplace=True)
    df2.set_index(csv_primary_keys, inplace=True)

    # Detect and remove duplicates
    summary['Duplicate Rows in File1'] = df1.index.duplicated().sum()
    summary['Duplicate Rows in File2'] = df2.index.duplicated().sum()
    df1 = df1[~df1.index.duplicated()]
    df2 = df2[~df2.index.duplicated()]

    # Compare row existence
    summary['Missing Rows in File2'] = len(df1.index.difference(df2.index))
    summary['Extra Rows in File2'] = len(df2.index.difference(df1.index))

    # Compare matching rows
    common_idx = df1.index.intersection(df2.index)
    total_fields = 0
    mismatches = 0

    #for idx in common_idx:
    for idx in tqdm(common_idx, desc="Comparing rows", unit="rows", ncols=100):
        row1 = df1.loc[idx]
        row2 = df2.loc[idx]
        row1_number = int(row1['_original_row']) if '_original_row' in row1 else None
        row2_number = int(row2['_original_row']) if '_original_row' in row2 else None

        for col in common_columns:
            val1 = row1[col] if col in row1 else None
            val2 = row2[col] if col in row2 else None
            total_fields += 1
            if pd.isnull(val1) and pd.isnull(val2):
                continue
            if val1 != val2:
                mismatches += 1
                diff_summary.append({
                    'PrimaryKey': idx,
                    'Column': col,
                    'File1_Value': val1,
                    'File2_Value': val2,
                    'RowNum_File1': row1_number,
                    'RowNum_File2': row2_number,
                    'Status': 'Mismatch'
                })

    # Summary calculations
    summary['Total Fields Compared'] = total_fields
    summary['Number of Discrepancies'] = mismatches
    summary['Failure %'] = round((mismatches / total_fields) * 100, 2) if total_fields else 0.0
    summary['Pass %'] = round(100 - summary['Failure %'], 2) if total_fields else 0.0

    if mismatches == 0 and summary['Missing Rows in File2'] == 0 and summary['Extra Rows in File2'] == 0:
        summary['Note'] = '✅ No comparison issues, files are identical'

    diff_df = pd.DataFrame(diff_summary)
    return diff_df, summary


def is_numeric(val):
    try:
        float(val)
        return True
    except (ValueError, TypeError):
        return False
    
    

def generate_html_report(
    diff_df, summary, report_start_time, output_file,
    source_files_count, destination_files_count, primary_key_columns,
    columns=None, project_name="CSV Comparator",
    project_logo="https://via.placeholder.com/150",
    include_passed=False,
    include_missing_files=False
):
    report_end_time = datetime.now()
    time_taken = report_end_time - report_start_time
    time_taken_str = str(time_taken).split('.')[0]  # HH:MM:SS

    comparison_rows = ""

    total_fields_compared = 0
    total_discrepancies = 0
    pass_percent_list = []
    fail_percent_list = []

    for csv_file, file_summary in tqdm(summary.items(), desc="Generating report"):
        if csv_file in ["Missing CSVs in Source2", "Extra CSVs in Source2"]:
            continue

        file_diff_df = diff_df[diff_df['File'] == csv_file] if not diff_df.empty else pd.DataFrame()
        match_status = "Mismatch" if not file_diff_df.empty else "Match"

        if match_status == "Match" and not include_passed:
            continue  # Skip matched files if flag is off

        icon = "<i class='fas fa-check-circle' style='color:green;'></i>" if match_status == "Match" else "<i class='fas fa-times-circle' style='color: red;'></i>"

        fields = file_summary.get('Total Fields Compared', 0)
        discrepancies = file_summary.get('Number of Discrepancies', 0)
        pass_percent = file_summary.get('Pass %', 0.0)
        fail_percent = file_summary.get('Failure %', 0.0)

        if fields:
            total_fields_compared += fields
            total_discrepancies += discrepancies
            pass_percent_list.append(pass_percent)
            fail_percent_list.append(fail_percent)

        # Group rows by PrimaryKey
        last_primary_key = None
        primary_key_start_index = None
        rowspan = 1
        diff_table_rows = []

        for idx, row in file_diff_df.iterrows():
            if last_primary_key is None or row['PrimaryKey'] != last_primary_key:
                # Finalize previous PrimaryKey rowspan
                if primary_key_start_index is not None:
                    diff_table_rows[primary_key_start_index] = diff_table_rows[primary_key_start_index].replace('ROWSPAN_PLACEHOLDER', f'rowspan="{rowspan}"')

                # Start a new PrimaryKey block
                last_primary_key = row['PrimaryKey']
                primary_key_start_index = len(diff_table_rows)
                rowspan = 1

                diff_table_rows.append(f"""
                    <tr>
                        <td ROWSPAN_PLACEHOLDER>{html.escape(str(row['PrimaryKey']))}</td>
                        <td>{html.escape(str(row['Column']))}</td>
                        <td>{html.escape(str(row['File1_Value']))} | {html.escape(str(row['RowNum_File1']))}</td>
                        <td>{html.escape(str(row['File2_Value']))} | {html.escape(str(row['RowNum_File2']))}</td>
                        <td>{round(float(row['File1_Value']) - float(row['File2_Value']), 4) if is_numeric(row['File1_Value']) and is_numeric(row['File2_Value']) else ''}</td>
                    </tr>
                """)
            else:
                rowspan += 1
                diff_table_rows.append(f"""
                    <tr>
                        <td>{html.escape(str(row['Column']))}</td>
                        <td>{html.escape(str(row['File1_Value']))} | {html.escape(str(row['RowNum_File1']))}</td>
                        <td>{html.escape(str(row['File2_Value']))} | {html.escape(str(row['RowNum_File2']))}</td>
                        <td>{round(float(row['File1_Value']) - float(row['File2_Value']), 4) if is_numeric(row['File1_Value']) and is_numeric(row['File2_Value']) else ''}</td>
                    </tr>
                """)

        # Final PrimaryKey block rowspan update
        if primary_key_start_index is not None:
            diff_table_rows[primary_key_start_index] = diff_table_rows[primary_key_start_index].replace('ROWSPAN_PLACEHOLDER', f'rowspan="{rowspan}"')

        diff_table = "".join(diff_table_rows)

        if match_status == "Mismatch":
            mismatch_details = f"""
                <div>
                    <button class="toggle-button" onclick="toggleVisibility('diff-{csv_file}', this)">+</button>
                    <div id="diff-{csv_file}" style="display:none;">
                        <strong>Differences:</strong>
                        <table border="1">
                            <tr><th width="50%">Primary Key</th><th width="20%">Column</th><th width="10%">File1 Value</th><th width="10%">File2 Value</th><th width="10%">Diff</th></tr>
                            {diff_table}
                        </table>
                        <br>
                        <strong>Additional Details:</strong>
                        <table border="1">
                            <tr><th>Missing Columns in File1</th><th>Missing Columns in File2</th></tr>
                            <tr>
                                <td>{', '.join(file_summary.get('Missing Columns in File1', [])) or 'None'}</td>
                                <td>{', '.join(file_summary.get('Missing Columns in File2', [])) or 'None'}</td>
                            </tr>
                            <tr><th>Missing Rows in File2</th><th>Extra Rows in File2</th></tr>
                            <tr>
                                <td>{file_summary.get('Missing Rows in File2', 0)}</td>
                                <td>{file_summary.get('Extra Rows in File2', 0)}</td>
                            </tr>
                            <tr><th>Duplicate Rows in File1</th><th>Duplicate Rows in File2</th></tr>
                            <tr>
                                <td>{file_summary.get('Duplicate Rows in File1', 0)}</td>
                                <td>{file_summary.get('Duplicate Rows in File2', 0)}</td>
                            </tr>
                            <tr><th>Total Fields Compared</th><th>Discrepancies</th></tr>
                            <tr>
                                <td>{fields}</td>
                                <td>{discrepancies}</td>
                            </tr>
                            <tr><th>Pass %</th><th>Failure %</th></tr>
                            <tr>
                                <td>{pass_percent}%</td>
                                <td>{fail_percent}%</td>
                            </tr>
                        </table>
                    </div>
                </div>
            """
        else:
            mismatch_details = "✅ The files are identical. No differences were found during the comparison."

        comparison_rows += f"""
        <tr>
            <td><i class="fas fa-file-csv" style="color:blue;"></i> {csv_file}</td>
            <td align="center">{icon}</td>
            <td>{mismatch_details}</td>
        </tr>
        """

    if include_missing_files and "Missing CSVs in Source2" in summary:
        for missing_csv in summary["Missing CSVs in Source2"]:
            comparison_rows += f"""
            <tr>
                <td><i class="fas fa-file-csv" style="color:gray;"></i> {missing_csv}</td>
                <td align="center"><i class="fas fa-exclamation-triangle" style="color:orange;"></i></td>
                <td><strong>Missing in destination</strong></td>
            </tr>
            """

    if "Extra CSVs in Source2" in summary:
        for extra_csv in summary["Extra CSVs in Source2"]:
            comparison_rows += f"""
            <tr>
                <td><i class="fas fa-file-csv" style="color:gray;"></i> {extra_csv}</td>
                <td align="center"><i class="fas fa-exclamation-triangle" style="color:blue;"></i></td>
                <td><strong>Extra in destination</strong></td>
            </tr>
            """

    overall_pass = round(sum(pass_percent_list) / len(pass_percent_list), 2) if pass_percent_list else 0.0
    overall_fail = round(sum(fail_percent_list) / len(fail_percent_list), 2) if fail_percent_list else 0.0

    html_template = f"""<!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <title>{project_name} Report</title>
        <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/5.15.4/css/all.min.css">
        <style>
            body {{ font-family: Arial; background: #f4f4f9; }}
            .container {{ background: white; margin: 20px auto; padding: 20px; max-width: 95%; border-radius: 8px; }}
            header {{ background-color: #173E72; color: white; padding: 10px; text-align: center; }}
            table {{ width: 100%; border-collapse: collapse; margin-top: 15px; word-break: break-word; table-layout: fixed; }}
            th, td {{ border: 1px solid #ccc; padding: 6px; text-align: left; }}
            th {{ background: #173E72; color: white; }}
            tr:nth-child(even) {{ background: #f9f9f9; }}
            .toggle-button {{ font-size: 0.7em; padding: 2px 6px; background-color: #0056b3; color: white; border: none; border-radius: 4px; cursor: pointer; }}
            td {{ vertical-align: top; }}
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
        <h1>{project_name} - CSV Comparison Report</h1>
        <p><strong>Generated:</strong> {report_start_time.strftime('%Y-%m-%d %H:%M:%S')}</p>
    </header>
    <div class="container">
        <h2>📋 Summary</h2>
        <ul>
            <li><strong>Duration:</strong> {time_taken_str}</li>
            <li><strong>Files on Engine :</strong> {source_files_count}</li>
            <li><strong>Files on Neoprice:</strong> {destination_files_count}</li>
            <li><strong>Primary Keys:</strong> {', '.join(primary_key_columns)}</li>
            <li><strong>Compared Columns:</strong> {"All Columns" if not columns else ', '.join(columns)}</li>
        </ul>

        <h2>📊 Overall Summary</h2>
        <ul>
            <li><strong>Total Fields Compared:</strong> {total_fields_compared}</li>
            <li><strong>Total Discrepancies:</strong> {total_discrepancies}</li>
            <li><strong>Overall Pass %:</strong> {overall_pass}%</li>
            <li><strong>Overall Failure %:</strong> {overall_fail}%</li>
        </ul>

        <h2>🧾 Comparison Results</h2>
        <table>
            <thead>
                <tr>
                    <th width="20%">CSV File</th>
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

    with open(output_file, "w", encoding='utf-8') as f:
        f.write(html_template)

    print(f"✅ HTML report written to {output_file}")
    return output_file

# Match and compare files
def process_file_pair(file1_name, file2_name, df1, df2):
    # print(f"Comparing {file1_name} with {file2_name}")
    return compare_csvs(df1, df2, file1_name)

def run_comparison():
    source1_zips = list_zip_files(source_1_prefix)
    # source1_zips = [source1_zips[0]]
    source2_zips = list_zip_files(source_2_prefix)
    # source2_zips = [source2_zips[0]]
    
    all_csvs_source1 = {}
    all_csvs_source2 = {}
    
    def read_all_csvs(zip_keys, store_dict, source_name=''):
        for zip_key in tqdm(zip_keys, desc=f"Reading zip files from {source_name}", unit="zip"):
            try:
                csvs = read_zip_from_s3(zip_key)
                #for name, df in tqdm(csvs.items(), desc=f"CSVs in {zip_key.split('/')[-1]}", leave=False, unit="csv"):
                for name, df in csvs.items():  
                    # Thread-safe write (if store_dict is shared)
                    if name in store_dict:
                        print(f"⚠️ Duplicate CSV found: {name} from {zip_key} — using the first one.")
                    else:
                        store_dict[name] = df
            except Exception as e:
                print(f"❌ Failed to read zip: {zip_key} | Error: {e}")

    store_lock = threading.Lock()
    def read_zip_and_store(zip_key, store_dict, source_name):
        
        try:
            csvs = read_zip_from_s3(zip_key)
            for name, df in csvs.items():
                # Thread-safe write (if store_dict is shared)
                with threading.Lock():
                    if name in store_dict:
                        print(f"⚠️ Duplicate CSV found: {name} from {zip_key} — using the first one.")
                    else:
                        store_dict[name] = df
        except Exception as e:
            print(f"❌ Failed to read zip: {zip_key} | Error: {e}")

    def read_all_csvs_multithreaded(zip_keys, store_dict, source_name):
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [
                executor.submit(read_zip_and_store, zip_key, store_dict, source_name)
                for zip_key in zip_keys
            ]
            for future in futures:
                future.result()  # Wait for all to complete
             
    # Read all CSVs into memory (this could be multithreaded if needed too)
    if use_multithreading_reading:
        read_all_csvs_multithreaded(source1_zips, all_csvs_source1, "source1")
        read_all_csvs_multithreaded(source2_zips, all_csvs_source2, "source2")
    else:
        read_all_csvs(source1_zips, all_csvs_source1, "source1")
        read_all_csvs(source2_zips, all_csvs_source2, "source2")


    # Normalize filenames (remove the date-time part)
    normalized_source1_files = {normalize_filename(file): file for file in all_csvs_source1.keys()}
    normalized_source2_files = {normalize_filename(file): file for file in all_csvs_source2.keys()}

    common_csvs = set(normalized_source1_files.keys()) & set(normalized_source2_files.keys())
    missing_in_source2 = set(normalized_source1_files.keys()) - set(normalized_source2_files.keys())
    missing_in_source1 = set(normalized_source2_files.keys()) - set(normalized_source1_files.keys())

    all_diffs = []
    all_summaries = {}

    def process_csv_pair(csv_name):
        # Get original filenames using normalized names
        original_file1 = normalized_source1_files[csv_name]
        original_file2 = normalized_source2_files[csv_name]

        df1 = all_csvs_source1[original_file1]
        df2 = all_csvs_source2[original_file2]

        return compare_csvs(df1, df2, original_file1)

    if use_multithreading_comparision:
        with ThreadPoolExecutor(max_workers=1000) as executor:
            results = list(executor.map(process_csv_pair, common_csvs))
    else:
        results = [process_csv_pair(csv_name) for csv_name in common_csvs]

    for csv_name, result in zip(common_csvs, results):
        diff_df, summary = result
        if diff_df.empty:
            summary['Note'] = 'No comparision issues, filter are identical'
        else:
            diff_df['File'] = csv_name
            all_diffs.append(diff_df)
        all_summaries[csv_name] = summary

    if missing_in_source2:
        all_summaries["Missing CSVs in Source2"] = list(missing_in_source2)
    if missing_in_source1:
        all_summaries["Extra CSVs in Source2"] = list(missing_in_source1)

    final_diff_df = pd.concat(all_diffs) if all_diffs else pd.DataFrame()
    list_files = [len(all_csvs_source1), len(all_csvs_source2)]
    return final_diff_df, all_summaries, list_files

# Run
if __name__ == "__main__":

    print('Start----------------', datetime.now().strftime("%Y%m%d_%H%M%S"))
    start_time = datetime.now()
    diff_df, summary, list_files = run_comparison()
    output_file = output_dir +'/'+ output_file
    generate_html_report(
        diff_df=diff_df,
        summary=summary,
        report_start_time=start_time,
        output_file = output_file,
        source_files_count=list_files[0],
        destination_files_count=list_files[1],
        primary_key_columns=csv_primary_keys,
        columns=csv_columns,
        project_name = project_name, 
        project_logo = "https://d3i59dyun0two3.cloudfront.net/wp-content/uploads/media/2024/11/29164438/ATPCO_logo_white.png", 
        include_passed = include_passed, 
        include_missing_files = include_missing_files
    )
    print('End----------------', datetime.now().strftime("%Y%m%d_%H%M%S"))


------------------------------------------------------------------
from datetime import datetime
import pandas as pd
import html
from tqdm import tqdm

def is_numeric(value):
    try:
        float(value)
        return True
    except:
        return False

def generate_html_report(
    diff_df, summary, report_start_time, output_file,
    source_files_count, destination_files_count, primary_key_columns,
    columns=None, project_name="CSV Comparator",
    project_logo="https://via.placeholder.com/150",
    include_passed=False,
    include_missing_files=False
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

    pass_percent_list = []
    fail_percent_list = []

    for csv_file, file_summary in tqdm(summary.items(), desc="Generating report"):
        if csv_file in ["Missing CSVs in Source2", "Extra CSVs in Source2"]:
            continue

        file_diff_df = diff_df[diff_df['File'] == csv_file] if not diff_df.empty else pd.DataFrame()
        match_status = "Mismatch" if not file_diff_df.empty else "Match"

        if match_status == "Match" and not include_passed:
            continue  # Skip matched files if flag is off

        icon = "<i class='fas fa-check-circle' style='color:green;'></i>" if match_status == "Match" else "<i class='fas fa-times-circle' style='color: red;'></i>"

        fields = file_summary.get('Total Fields Compared', 0)
        discrepancies = file_summary.get('Number of Discrepancies', 0)
        pass_percent = file_summary.get('Pass %', 0.0)
        fail_percent = file_summary.get('Failure %', 0.0)

        if fields:
            total_fields_compared += fields
            total_discrepancies += discrepancies
            pass_percent_list.append(pass_percent)
            fail_percent_list.append(fail_percent)

        if match_status == "Mismatch":
            total_different_rows += discrepancies

        total_missing_rows += file_summary.get('Missing Rows in File2', 0)
        total_extra_rows += file_summary.get('Extra Rows in File2', 0)

        # Group rows by PrimaryKey
        last_primary_key = None
        primary_key_start_index = None
        rowspan = 1
        diff_table_rows = []

        for idx, row in file_diff_df.iterrows():
            if last_primary_key is None or row['PrimaryKey'] != last_primary_key:
                if primary_key_start_index is not None:
                    diff_table_rows[primary_key_start_index] = diff_table_rows[primary_key_start_index].replace('ROWSPAN_PLACEHOLDER', f'rowspan="{rowspan}"')

                last_primary_key = row['PrimaryKey']
                primary_key_start_index = len(diff_table_rows)
                rowspan = 1

                diff_table_rows.append(f"""
                    <tr>
                        <td ROWSPAN_PLACEHOLDER>{html.escape(str(row['PrimaryKey']))}</td>
                        <td>{html.escape(str(row['Column']))}</td>
                        <td>{html.escape(str(row['File1_Value']))} | {html.escape(str(row['RowNum_File1']))}</td>
                        <td>{html.escape(str(row['File2_Value']))} | {html.escape(str(row['RowNum_File2']))}</td>
                        <td>{round(float(row['File1_Value']) - float(row['File2_Value']), 4) if is_numeric(row['File1_Value']) and is_numeric(row['File2_Value']) else ''}</td>
                    </tr>
                """)
            else:
                rowspan += 1
                diff_table_rows.append(f"""
                    <tr>
                        <td>{html.escape(str(row['Column']))}</td>
                        <td>{html.escape(str(row['File1_Value']))} | {html.escape(str(row['RowNum_File1']))}</td>
                        <td>{html.escape(str(row['File2_Value']))} | {html.escape(str(row['RowNum_File2']))}</td>
                        <td>{round(float(row['File1_Value']) - float(row['File2_Value']), 4) if is_numeric(row['File1_Value']) and is_numeric(row['File2_Value']) else ''}</td>
                    </tr>
                """)

        if primary_key_start_index is not None:
            diff_table_rows[primary_key_start_index] = diff_table_rows[primary_key_start_index].replace('ROWSPAN_PLACEHOLDER', f'rowspan="{rowspan}"')

        diff_table = "".join(diff_table_rows)

        if match_status == "Mismatch":
            mismatch_details = f"""
                <div>
                    <button class="toggle-button" onclick="toggleVisibility('diff-{csv_file}', this)">+</button>
                    <div id="diff-{csv_file}" style="display:none;">
                        <strong>Differences:</strong>
                        <table border="1">
                            <tr><th width="50%">Primary Key</th><th width="20%">Column</th><th width="10%">File1 Value</th><th width="10%">File2 Value</th><th width="10%">Diff</th></tr>
                            {diff_table}
                        </table>
                        <br>
                        <strong>Additional Details:</strong>
                        <table border="1">
                            <tr><th>Missing Columns in File1</th><th>Missing Columns in File2</th></tr>
                            <tr>
                                <td>{', '.join(file_summary.get('Missing Columns in File1', [])) or 'None'}</td>
                                <td>{', '.join(file_summary.get('Missing Columns in File2', [])) or 'None'}</td>
                            </tr>
                            <tr><th>Missing Rows in File2</th><th>Extra Rows in File2</th></tr>
                            <tr>
                                <td>{file_summary.get('Missing Rows in File2', 0)}</td>
                                <td>{file_summary.get('Extra Rows in File2', 0)}</td>
                            </tr>
                            <tr><th>Duplicate Rows in File1</th><th>Duplicate Rows in File2</th></tr>
                            <tr>
                                <td>{file_summary.get('Duplicate Rows in File1', 0)}</td>
                                <td>{file_summary.get('Duplicate Rows in File2', 0)}</td>
                            </tr>
                            <tr><th>Total Fields Compared</th><th>Discrepancies</th></tr>
                            <tr>
                                <td>{fields}</td>
                                <td>{discrepancies}</td>
                            </tr>
                            <tr><th>Pass %</th><th>Failure %</th></tr>
                            <tr>
                                <td>{pass_percent}%</td>
                                <td>{fail_percent}%</td>
                            </tr>
                        </table>
                    </div>
                </div>
            """
        else:
            mismatch_details = "✅ The files are identical. No differences were found during the comparison."

        comparison_rows += f"""
        <tr>
            <td><i class="fas fa-file-csv" style="color:blue;"></i> {csv_file}</td>
            <td align="center">{icon}</td>
            <td>{mismatch_details}</td>
        </tr>
        """

    if include_missing_files and "Missing CSVs in Source2" in summary:
        total_missing_files = len(summary["Missing CSVs in Source2"])
        for missing_csv in summary["Missing CSVs in Source2"]:
            comparison_rows += f"""
            <tr>
                <td><i class="fas fa-file-csv" style="color:gray;"></i> {missing_csv}</td>
                <td align="center"><i class="fas fa-exclamation-triangle" style="color:orange;"></i></td>
                <td><strong>Missing in destination</strong></td>
            </tr>
            """

    if "Extra CSVs in Source2" in summary:
        for extra_csv in summary["Extra CSVs in Source2"]:
            comparison_rows += f"""
            <tr>
                <td><i class="fas fa-file-csv" style="color:gray;"></i> {extra_csv}</td>
                <td align="center"><i class="fas fa-exclamation-triangle" style="color:blue;"></i></td>
                <td><strong>Extra in destination</strong></td>
            </tr>
            """

    overall_pass = round(sum(pass_percent_list) / len(pass_percent_list), 2) if pass_percent_list else 0.0
    overall_fail = round(sum(fail_percent_list) / len(fail_percent_list), 2) if fail_percent_list else 0.0

    html_template = f"""<!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <title>{project_name} Report</title>
        <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/5.15.4/css/all.min.css">
        <style>
            body {{ font-family: Arial; background: #f4f4f9; }}
            .container {{ background: white; margin: 20px auto; padding: 20px; max-width: 95%; border-radius: 8px; }}
            header {{ background-color: #173E72; color: white; padding: 10px; text-align: center; }}
            table {{ width: 100%; border-collapse: collapse; margin-top: 15px; word-break: break-word; table-layout: fixed; }}
            th, td {{ border: 1px solid #ccc; padding: 6px; text-align: left; }}
            th {{ background: #173E72; color: white; }}
            tr:nth-child(even) {{ background: #f9f9f9; }}
            .toggle-button {{ font-size: 0.7em; padding: 2px 6px; background-color: #0056b3; color: white; border: none; border-radius: 4px; cursor: pointer; }}
            td {{ vertical-align: top; }}
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
        <h1>{project_name} - CSV Comparison Report</h1>
        <p><strong>Generated:</strong> {report_start_time.strftime('%Y-%m-%d %H:%M:%S')}</p>
    </header>
    <div class="container">
        <h2>📋 Summary</h2>
        <ul>
            <li><strong>Duration:</strong> {time_taken_str}</li>
            <li><strong>Files on Engine :</strong> {source_files_count}</li>
            <li><strong>Files on Neoprice:</strong> {destination_files_count}</li>
            <li><strong>Primary Keys:</strong> {', '.join(primary_key_columns)}</li>
            <li><strong>Compared Columns:</strong> {"All Columns" if not columns else ', '.join(columns)}</li>
        </ul>

        <h2>📊 Overall Summary</h2>
        <ul>
            <li><strong>Total Fields Compared:</strong> {total_fields_compared}</li>
            <li><strong>Total Discrepancies:</strong> {total_discrepancies}</li>
            <li><strong>Overall Pass %:</strong> {overall_pass}%</li>
            <li><strong>Overall Failure %:</strong> {overall_fail}%</li>
            <li><strong>Total Missing Files:</strong> {total_missing_files}</li>
            <li><strong>Total Missing Rows:</strong> {total_missing_rows}</li>
            <li><strong>Total New Rows:</strong> {total_extra_rows}</li>
            <li><strong>Total Different Rows:</strong> {total_different_rows}</li>
        </ul>

        <h2>🧾 Comparison Results</h2>
        <table>
            <thead>
                <tr>
                    <th width="20%">CSV File</th>
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

------------------------------
<li><strong>Overall Pass %:</strong> <span style="background-color: #d4edda; color: #155724; padding: 2px 6px; border-radius: 4px;">{overall_pass}%</span></li>
<li><strong>Overall Failure %:</strong> <span style="background-color: #f8d7da; color: #721c24; padding: 2px 6px; border-radius: 4px;">{overall_fail}%</span></li>
