def run_comparison():
    source1_zips = list_zip_files(source_1_prefix)
    source2_zips = list_zip_files(source_2_prefix)

    all_csvs_source1 = {}
    all_csvs_source2 = {}

    def read_all_csvs(zip_keys, store_dict):
        for zip_key in zip_keys:
            try:
                csvs = read_zip_from_s3(zip_key)
                for name, df in csvs.items():
                    if name in store_dict:
                        print(f"⚠️ Duplicate CSV found: {name} from {zip_key} — using the first one.")
                    else:
                        store_dict[name] = df
            except Exception as e:
                print(f"❌ Failed to read zip: {zip_key} | Error: {e}")

    # Read all CSVs into memory (this could be multithreaded if needed too)
    read_all_csvs(source1_zips, all_csvs_source1)
    read_all_csvs(source2_zips, all_csvs_source2)

    common_csvs = set(all_csvs_source1.keys()) & set(all_csvs_source2.keys())
    missing_in_source2 = set(all_csvs_source1.keys()) - set(all_csvs_source2.keys())
    missing_in_source1 = set(all_csvs_source2.keys()) - set(all_csvs_source1.keys())

    all_diffs = []
    all_summaries = {}

    def process_csv_pair(csv_name):
        df1 = all_csvs_source1[csv_name]
        df2 = all_csvs_source2[csv_name]
        return compare_csvs(df1, df2, csv_name)

    if use_multithreading:
        with ThreadPoolExecutor() as executor:
            results = list(executor.map(process_csv_pair, common_csvs))
    else:
        results = [process_csv_pair(csv_name) for csv_name in common_csvs]

    for csv_name, result in zip(common_csvs, results):
        diff_df, summary = result
        if not diff_df.empty:
            all_diffs.append(diff_df)
        all_summaries[csv_name] = summary

    if missing_in_source2:
        all_summaries["Missing CSVs in Source2"] = list(missing_in_source2)
    if missing_in_source1:
        all_summaries["Extra CSVs in Source2"] = list(missing_in_source1)

    final_diff_df = pd.concat(all_diffs) if all_diffs else pd.DataFrame()
    return final_diff_df, all_summaries





def read_zip_and_store(zip_key, store_dict):
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

def read_all_csvs_multithreaded(zip_keys, store_dict):
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(read_zip_and_store, zip_key, store_dict) for zip_key in zip_keys]
        for future in futures:
            future.result()  # Wait for all to complete
            




read_all_csvs(source1_zips, all_csvs_source1)
read_all_csvs(source2_zips, all_csvs_source2)


read_all_csvs_multithreaded(source1_zips, all_csvs_source1)
read_all_csvs_multithreaded(source2_zips, all_csvs_source2)



import re

def normalize_filename(filename):
    # Use regex to remove the date-time part in the format: YYYYMMDD_HHMM
    return re.sub(r'\d{8}_\d{4}', '', filename)

def run_comparison():
    source1_zips = list_zip_files(source_1_prefix)
    source2_zips = list_zip_files(source_2_prefix)

    all_csvs_source1 = {}
    all_csvs_source2 = {}

    def read_all_csvs(zip_keys, store_dict):
        for zip_key in zip_keys:
            try:
                csvs = read_zip_from_s3(zip_key)
                for name, df in csvs.items():
                    if name in store_dict:
                        print(f"⚠️ Duplicate CSV found: {name} from {zip_key} — using the first one.")
                    else:
                        store_dict[name] = df
            except Exception as e:
                print(f"❌ Failed to read zip: {zip_key} | Error: {e}")

    # Read all CSVs into memory (this could be multithreaded if needed too)
    read_all_csvs(source1_zips, all_csvs_source1)
    read_all_csvs(source2_zips, all_csvs_source2)

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

    if use_multithreading:
        with ThreadPoolExecutor() as executor:
            results = list(executor.map(process_csv_pair, common_csvs))
    else:
        results = [process_csv_pair(csv_name) for csv_name in common_csvs]

    for csv_name, result in zip(common_csvs, results):
        diff_df, summary = result
        if not diff_df.empty:
            all_diffs.append(diff_df)
        all_summaries[csv_name] = summary

    if missing_in_source2:
        all_summaries["Missing CSVs in Source2"] = list(missing_in_source2)
    if missing_in_source1:
        all_summaries["Extra CSVs in Source2"] = list(missing_in_source1)

    final_diff_df = pd.concat(all_diffs) if all_diffs else pd.DataFrame()
    return final_diff_df, all_summaries




--------------------------------------------------------------------------
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

    for idx in common_idx:
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

---------------------------
from datetime import datetime
from tqdm import tqdm

def generate_html_report(diff_df, summary, report_start_time, output_file, source_files_count, destination_files_count,
                         primary_key_columns, columns=None, project_name="CSV Comparator", project_logo="https://via.placeholder.com/150"):

    report_end_time = datetime.now()
    time_taken = report_end_time - report_start_time
    time_taken_str = str(time_taken).split('.')[0]  # HH:MM:SS format

    comparison_rows = ""

    for csv_file, file_summary in tqdm(summary.items(), desc="Generating report"):
        if csv_file in ["Missing CSVs in Source2", "Extra CSVs in Source2"]:
            continue  # These are handled separately

        file_diff_df = diff_df[diff_df['File'] == csv_file] if not diff_df.empty else pd.DataFrame()

        match_status = "Mismatch" if not file_diff_df.empty else "Match"
        icon = "<i class='fas fa-check-circle' style='color:green;'></i>" if match_status == "Match" else "<i class='fas fa-times-circle' style='color: red;'></i>"

        # Build detailed mismatch section if applicable
        if match_status == "Mismatch":
            diff_table = "".join([
                f"<tr><td>{row['PrimaryKey']}</td><td>{row['Column']}</td><td>{row['File1_Value']}</td><td>{row['File2_Value']}</td></tr>"
                for _, row in file_diff_df.iterrows()
            ])
            mismatch_details = f"""
                <div>
                    <button class="toggle-button" onclick="toggleVisibility('diff-{csv_file}', this)">+</button>
                    <div id="diff-{csv_file}" style="display:none;">
                        <strong>Differences:</strong>
                        <table border="1">
                            <tr><th>Primary Key</th><th>Column</th><th>File1 Value</th><th>File2 Value</th></tr>
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
                                <td>{file_summary.get('Total Fields Compared', 0)}</td>
                                <td>{file_summary.get('Number of Discrepancies', 0)}</td>
                            </tr>
                            <tr><th>Pass %</th><th>Failure %</th></tr>
                            <tr>
                                <td>{file_summary.get('Pass %', 0.0)}%</td>
                                <td>{file_summary.get('Failure %', 0.0)}%</td>
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

    # Handle missing/extra CSVs
    if "Missing CSVs in Source2" in summary:
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

    # HTML content
    html_template = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>{project_name} Report</title>
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/5.15.4/css/all.min.css">
    <style>
        body {{ font-family: Arial; background: #f4f4f9; }}
        .container {{ background: white; margin: 20px auto; padding: 20px; max-width: 1000px; border-radius: 8px; }}
        header {{ background-color: #173E72; color: white; padding: 10px; text-align: center; }}
        table {{ width: 100%; border-collapse: collapse; margin-top: 15px; }}
        th, td {{ border: 1px solid #ccc; padding: 6px; text-align: left; }}
        th {{ background: #173E72; color: white; }}
        tr:nth-child(even) {{ background: #f9f9f9; }}
        .toggle-button {{ font-size: 0.7em; padding: 2px 6px; background-color: #0056b3; color: white; border: none; border-radius: 4px; cursor: pointer; }}
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
    <h2>Summary</h2>
    <ul>
        <li><strong>Duration:</strong> {time_taken_str}</li>
        <li><strong>Source Files:</strong> {source_files_count}</li>
        <li><strong>Destination Files:</strong> {destination_files_count}</li>
        <li><strong>Primary Keys:</strong> {', '.join(primary_key_columns)}</li>
        <li><strong>Compared Columns:</strong> {"All Columns" if not columns else ', '.join(columns)}</li>
    </ul>

    <h2>Comparison Results</h2>
    <table>
        <thead>
            <tr>
                <th>CSV File</th>
                <th>Status</th>
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



----------------------------------
generate_html_report(
        diff_df=diff_df,
        summary=summary,
        report_start_time=start_time,
        output_file="csv_comparison_report.html",
        source_files_count=len(list_zip_files(source_1_prefix)),
        destination_files_count=len(list_zip_files(source_2_prefix)),
        primary_key_columns=primary_key_columns,
        columns=csv_columns
    )

===========================================
import os
import subprocess

# Absolute path to HTML report
report_path = os.path.abspath("csv_comparison_report.html")

# Path to Microsoft Edge (default install location for most Windows systems)
edge_exe_path = r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe"

# If that doesn't exist, try Program Files
if not os.path.exists(edge_exe_path):
    edge_exe_path = r"C:\Program Files\Microsoft\Edge\Application\msedge.exe"

# Launch Edge with the file
if os.path.exists(edge_exe_path):
    subprocess.Popen([edge_exe_path, report_path])
    print(f"✅ Opened report in Edge: {report_path}")
else:
    print("❌ Microsoft Edge not found. Please check installation path.")




>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
from datetime import datetime
from tqdm import tqdm
import pandas as pd

def generate_html_report(diff_df, summary, report_start_time, output_file, source_files_count, destination_files_count,
                         primary_key_columns, columns=None, project_name="CSV Comparator", project_logo="https://via.placeholder.com/150"):

    report_end_time = datetime.now()
    time_taken = report_end_time - report_start_time
    time_taken_str = str(time_taken).split('.')[0]  # HH:MM:SS

    comparison_rows = ""

    # Aggregates for overall summary
    total_fields_compared = 0
    total_discrepancies = 0
    total_pass_percent = 0.0
    total_failure_percent = 0.0
    files_with_stats = 0

    for csv_file, file_summary in tqdm(summary.items(), desc="Generating report"):
        if csv_file in ["Missing CSVs in Source2", "Extra CSVs in Source2"]:
            continue

        file_diff_df = diff_df[diff_df['File'] == csv_file] if not diff_df.empty else pd.DataFrame()

        match_status = "Mismatch" if not file_diff_df.empty else "Match"
        icon = "<i class='fas fa-check-circle' style='color:green;'></i>" if match_status == "Match" else "<i class='fas fa-times-circle' style='color: red;'></i>"

        # Track totals for overall stats
        if 'Total Fields Compared' in file_summary:
            total_fields_compared += file_summary.get('Total Fields Compared', 0)
            total_discrepancies += file_summary.get('Number of Discrepancies', 0)
            total_pass_percent += file_summary.get('Pass %', 0.0)
            total_failure_percent += file_summary.get('Failure %', 0.0)
            files_with_stats += 1

        if match_status == "Mismatch":
            diff_table = "".join([
                f"<tr><td>{row['PrimaryKey']}</td><td>{row['Column']}</td><td>{row['File1_Value']}</td><td>{row['File2_Value']}</td></tr>"
                for _, row in file_diff_df.iterrows()
            ])
            mismatch_details = f"""
                <div>
                    <button class="toggle-button" onclick="toggleVisibility('diff-{csv_file}', this)">+</button>
                    <div id="diff-{csv_file}" style="display:none;">
                        <strong>Differences:</strong>
                        <table border="1">
                            <tr><th>Primary Key</th><th>Column</th><th>File1 Value</th><th>File2 Value</th></tr>
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
                                <td>{file_summary.get('Total Fields Compared', 0)}</td>
                                <td>{file_summary.get('Number of Discrepancies', 0)}</td>
                            </tr>
                            <tr><th>Pass %</th><th>Failure %</th></tr>
                            <tr>
                                <td>{file_summary.get('Pass %', 0.0)}%</td>
                                <td>{file_summary.get('Failure %', 0.0)}%</td>
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

    # Handle missing/extra CSVs
    if "Missing CSVs in Source2" in summary:
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

    # Calculate overall pass/fail %
    overall_pass = round(total_pass_percent / files_with_stats, 2) if files_with_stats else 0.0
    overall_fail = round(total_failure_percent / files_with_stats, 2) if files_with_stats else 0.0

    # HTML
    html_template = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>{project_name} Report</title>
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/5.15.4/css/all.min.css">
    <style>
        body {{ font-family: Arial; background: #f4f4f9; }}
        .container {{ background: white; margin: 20px auto; padding: 20px; max-width: 1000px; border-radius: 8px; }}
        header {{ background-color: #173E72; color: white; padding: 10px; text-align: center; }}
        table {{ width: 100%; border-collapse: collapse; margin-top: 15px; }}
        th, td {{ border: 1px solid #ccc; padding: 6px; text-align: left; }}
        th {{ background: #173E72; color: white; }}
        tr:nth-child(even) {{ background: #f9f9f9; }}
        .toggle-button {{ font-size: 0.7em; padding: 2px 6px; background-color: #0056b3; color: white; border: none; border-radius: 4px; cursor: pointer; }}
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
        <li><strong>Source Files:</strong> {source_files_count}</li>
        <li><strong>Destination Files:</strong> {destination_files_count}</li>
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
                <th>CSV File</th>
                <th>Status</th>
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

