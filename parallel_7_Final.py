from datetime import datetime
from tqdm import tqdm
import pandas as pd
import html

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
    include_passed=True,
    include_missing_in_dest=True
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

        if match_status == "Mismatch":
            diff_table = "".join([
                f"<tr>"
                f"<td>{html.escape(str(row['PrimaryKey']))}</td>"
                f"<td>{html.escape(str(row['Column']))}</td>"
                f"<td>{html.escape(str(row['File1_Value']))} | {row.get('Row_File1', '')}</td>"
                f"<td>{html.escape(str(row['File2_Value']))} | {row.get('Row_File2', '')}</td>"
                f"<td>{round(float(row['File1_Value']) - float(row['File2_Value']), 4) if is_numeric(row['File1_Value']) and is_numeric(row['File2_Value']) else ''}</td>"
                f"</tr>"
                for _, row in file_diff_df.iterrows()
            ])
            mismatch_details = f"""
                <div>
                    <button class="toggle-button" onclick="toggleVisibility('diff-{csv_file}', this)">+</button>
                    <div id="diff-{csv_file}" style="display:none;">
                        <strong>Differences:</strong>
                        <table border="1">
                            <tr><th>Primary Key</th><th>Column</th><th>File1 Value</th><th>File2 Value</th><th>Diff</th></tr>
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

    if include_missing_in_dest and "Missing CSVs in Source2" in summary:
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

    # Final average pass/fail
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
===========================================================

from datetime import datetime
from tqdm import tqdm
import pandas as pd
import html

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
    include_passed=True,
    include_missing_in_dest=True
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
        rowspan = 1  # To keep track of how many rows are grouped under the same PrimaryKey
        diff_table_rows = []

        for idx, row in file_diff_df.iterrows():
            if last_primary_key is None or row['PrimaryKey'] != last_primary_key:
                # For a new PrimaryKey, add rowspan for the previous one (if any)
                if last_primary_key is not None:
                    diff_table_rows[-1] = diff_table_rows[-1].replace('rowspan="1"', f'rowspan="{rowspan}"')

                # Reset for the new PrimaryKey
                last_primary_key = row['PrimaryKey']
                rowspan = 1
            else:
                rowspan += 1

            # Add the new row with or without rowspan
            diff_table_rows.append(f"""
                <tr>
                    <td>{html.escape(str(row['PrimaryKey']))}</td>
                    <td>{html.escape(str(row['Column']))}</td>
                    <td>{html.escape(str(row['File1_Value']))} | {row.get('Row_File1', '')}</td>
                    <td>{html.escape(str(row['File2_Value']))} | {row.get('Row_File2', '')}</td>
                    <td>{round(float(row['File1_Value']) - float(row['File2_Value']), 4) if is_numeric(row['File1_Value']) and is_numeric(row['File2_Value']) else ''}</td>
                </tr>
            """)

        # Apply rowspan for the last primary key in the loop
        if last_primary_key is not None:
            diff_table_rows[-1] = diff_table_rows[-1].replace('rowspan="1"', f'rowspan="{rowspan}"')

        # Combine all rows into a single string
        diff_table = "".join(diff_table_rows)

        if match_status == "Mismatch":
            mismatch_details = f"""
                <div>
                    <button class="toggle-button" onclick="toggleVisibility('diff-{csv_file}', this)">+</button>
                    <div id="diff-{csv_file}" style="display:none;">
                        <strong>Differences:</strong>
                        <table border="1">
                            <tr><th>Primary Key</th><th>Column</th><th>File1 Value</th><th>File2 Value</th><th>Diff</th></tr>
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

    if include_missing_in_dest and "Missing CSVs in Source2" in summary:
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

    # Final average pass/fail
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
