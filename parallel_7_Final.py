import pandas as pd
from datetime import datetime
import webbrowser
import configparser
import os
import concurrent.futures
from datetime import datetime
from tqdm import tqdm

# Read configuration from the config.ini file
config = configparser.ConfigParser()
config.read('config.ini')
project_name = config.get('settings', 'project_name')
project_logo = config.get('settings', 'project_logo')
primary_key_columns = config.get('settings', 'primary_key_columns')
source_folder = config.get('settings', 'source_folder')
destination_folder = config.get('settings', 'destination_folder')
output_dir = config.get('settings', 'output_dir')
output_file = config.get('settings', 'output_file')
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
base_name, ext = os.path.splitext(output_file)
extension = ext if ext else ".html"

output_file = f"{base_name}_{timestamp}{extension}"
if output_dir != '':
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    output_file = output_dir+'/'+output_file

columns = config.get('settings', 'columns')
if columns == '':  # If columns is an empty list
    columns = []
else:
    columns = columns.split(',')


if primary_key_columns == '':  # If columns is an empty list
    primary_key_columns = []
else:
    primary_key_columns = primary_key_columns.split(',')

pk_not_found = {}

def check_file_exists(file_path):
    if not os.path.exists(file_path):
        print(f"Error: The file '{file_path}' does not exist.")
        return False
    return True

# Get all CSV files from the source folder
def get_files_from_folder(folder_path):
    #    return [f for f in os.listdir(folder_path) if f.endswith('.csv')]
    return sorted([f for f in os.listdir(folder_path) if f.endswith('.csv')], reverse=False)

# Function to compare two CSV files
def compare_csv(file1, file2, primary_key_columns, columns):
    if not check_file_exists(file1) or not check_file_exists(file2):
        return None, None


    
    try:
        # Check if columns are specified
        if columns:
            # Try to read the files with the specified columns
            try:
                df1 = pd.read_csv(file1, usecols=columns)
                df2 = pd.read_csv(file2, usecols=columns)
            except ValueError as e:
                print(f"Error reading columns from {file1} and {file2}: {e}")
                return None, None
        else:
            # Read without limiting columns
            df1 = pd.read_csv(file1)
            df2 = pd.read_csv(file2)

        # Check for empty files
        if df1.empty or df2.empty:
            print(f"Error: One of the files is empty: {file1} or {file2}")
            return None, None

        # Ensure primary key columns are present in both dataframes
        if not all(col in df1.columns for col in primary_key_columns):
            print(f"Primary key columns {primary_key_columns} not found in file 1.")
            pk_not_found_file1 = f"Primary key columns {primary_key_columns} not found in file 1."
            pk_not_found[os.path.basename(file1)] = pk_not_found_file1
            return None, None
        if not all(col in df2.columns for col in primary_key_columns):
            print(f"Primary key columns {primary_key_columns} not found in file 2.")
            pk_not_found_file2 = f"Primary key columns {primary_key_columns} not found in file 2."
            pk_not_found[os.path.basename(file2)] = pk_not_found_file2
            return None, None

        # Set primary key columns as index for comparison
        df1.set_index(primary_key_columns, inplace=True)
        df2.set_index(primary_key_columns, inplace=True)

        # Sort by index to ensure proper comparison
        df1.sort_index(inplace=True)
        df2.sort_index(inplace=True)

        diff_summary = []

        # Compare rows and columns
        for idx, row in df1.iterrows():
            if idx in df2.index:
                for col in df1.columns:
                    file1_value = row[col]
                    file2_value = df2.loc[idx, col] if col in df2.columns else None
                    if file1_value != file2_value:
                        diff_summary.append({'PrimaryKey': idx, 'Column': col, 'File1_Value': file1_value, 'File2_Value': file2_value, 'Status': 'Mismatch'})

        missing_columns_in_file2 = set(df1.columns) - set(df2.columns)
        missing_columns_in_file1 = set(df2.columns) - set(df1.columns)
        missing_rows = df1.loc[~df1.index.isin(df2.index)]
        extra_rows = df2.loc[~df2.index.isin(df1.index)]

        duplicate_rows_in_file1 = df1[df1.duplicated(keep=False)]
        duplicate_rows_in_file2 = df2[df2.duplicated(keep=False)]

        diff_df = pd.DataFrame(diff_summary)

        summary = {
            'Missing Columns in File2': missing_columns_in_file2,
            'Missing Columns in File1': missing_columns_in_file1,
            'Missing Rows in File2': missing_rows,
            'Extra Rows in File2': extra_rows,
            'Duplicate Rows in File1': duplicate_rows_in_file1,
            'Duplicate Rows in File2': duplicate_rows_in_file2
            # 'pk_not_found_file1' : pk_not_found_file1,
            # 'pk_not_found_file2' : pk_not_found_file2
        }

        return diff_df, summary
    except Exception as e:
        print(f"Error comparing files {file1} and {file2}: {e}")
        return None, None

# Generate the HTML report (same as in the previous code)
# HTML Report Generation for a Professional Layout

def generate_html_report(file_comparisons, report_start_time, output_file, source_files_count, destination_files_count):
    report_end_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    time_taken = (datetime.now() - report_start_time)
    
    # Calculate hours, minutes, seconds
    seconds = time_taken.total_seconds()
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    seconds = int(seconds % 60)

    # Format time_taken as hours:minutes:seconds
    time_taken_str = f"{hours}:{minutes:02d}:{seconds:02d}"

    # Generate table rows for each comparison
    comparison_rows = ""
    for file1, (diff_df, summary) in file_comparisons.items():
        if diff_df is None:

            if file1 in pk_not_found:
                comparison_rows += f"""
                <tr>
                    <td><i class="fas fa-file-csv" style="color:blue;"></i> {file1}</td>
                    <td align='center'><i class="fas fa-key" style="color:yellow;"></i></td>
                    <td><strong>No PK found:</strong> {pk_not_found[file1]} </td>
                </tr>
                """
            else:
                comparison_rows += f"""
                <tr>
                    <td><i class="fas fa-file-csv" style="color:blue;"></i> {file1}</td>
                    <td align="center"><i class="fas fa-times-circle" style="color:orange;"></i></td>
                    <td><strong>No Comparison:</strong> File {file1} not present in destination folder.</td>                
                </tr>
                """
        else:
            match_status = "Mismatch" if not diff_df.empty else "Match"
            icon = "<i class='fas fa-check-circle' style='color:green;'></i>" if match_status == "Match" else "<i class='fas fa-times-circle' style='color: red;'></i>"

            # Prepare the details based on match/mismatch
            if match_status == "Mismatch":
                mismatch_details = f"""
                <div>
                    Discrepancies were found between the source file and the destination file during the comparison. 
                    <button class="toggle-button" onclick="toggleVisibility('diff-{file1}', this)">+</button>
                    <div id="diff-{file1}" style="display:none;">
                        <strong>Differences:</strong>
                        <table border="1">
                            <tr><th>Primary Key</th><th>Column</th><th>File1 Value</th><th>File2 Value</th></tr>
                            {"".join([f"<tr><td> <i class='fas fa-key primary-key'></i> {row['PrimaryKey']}</td><td nowrap>{row['Column']}</td><td>{row['File1_Value']}</td><td>{row['File2_Value']}</td></tr>" for _, row in diff_df.iterrows()])}
                        </table>
                        <br>
                        <strong>Additional Details:</strong>
                        <table border="1">
                            <tr><th>Missing Columns in Source File</th><th>Missing Columns in Destination File</th></tr>
                            <tr>
                                <td>{', '.join(summary['Missing Columns in File1']) if summary['Missing Columns in File1'] else 'None'}</td>
                                <td>{', '.join(summary['Missing Columns in File2']) if summary['Missing Columns in File2'] else 'None'}</td>
                            </tr>

                            <tr><th>Missing Rows in Source File2</th><th>Extra Rows in File2</th></tr>
                            <tr>
                                <td>{', '.join(summary['Missing Rows in File2'].index.astype(str)) if not summary['Missing Rows in File2'].empty else 'None'}</td>
                                <td>{', '.join(summary['Extra Rows in File2'].index.astype(str)) if not summary['Extra Rows in File2'].empty else 'None'}</td>
                            </tr>

                            <tr><th>Duplicate Rows in Source File</th><th>Duplicate Rows in Destination File</th></tr>
                            <tr>
                                <td>{len(summary['Duplicate Rows in File1'])} rows</td>
                                <td>{len(summary['Duplicate Rows in File2'])} rows</td>
                            </tr>
                        </table>
                    </div>
                </div>
                """
            else:
                mismatch_details = "The files are identical. No differences were found during the comparison."

            comparison_rows += f"""
            <tr>
                <td><i class="fas fa-file-csv" style="color:blue;"></i> {file1}</td>
                <td align="center">{icon}</td>
                <td nowrap>
                   {mismatch_details}
                </td>
            </tr>
            """

    # Complete HTML content with JS for toggling
    html_content = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>CSV Comparison Report</title>
        <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/5.15.4/css/all.min.css"> <!-- FontAwesome -->
        <style>
            body {{
                font-family: Arial, sans-serif;
                margin: 0;
                padding: 0;
                background-color: #f4f4f9;
                color: #333;
                font-size: 0.9em; /* Standard font size for wider screens */
            }}
            .container {{
                width: 90%;
                max-width: 1200px; /* Adjusted for wide screen view */
                margin: 30px auto;
                padding: 15px 25px; /* Adjusted padding for spacious design */
                background: #fff;
                border-radius: 8px;
                box-shadow: 0 4px 8px rgba(0, 0, 0, 0.1);
            }}
            header {{
                background-color: #173E72;
                padding: 8px 15px;  /* Reduced padding for a more compact header */
                color: white;
                text-align: center;
                font-size: 1.2em; /* Slightly smaller font size */
                border-radius: 5px; /* Rounded corners for a more modern look */
                box-shadow: 0 2px 5px rgba(0, 0, 0, 0.1); /* Subtle shadow for depth */
            }}

            header h1 {{
                font-size: 1.6em;  /* Reduced size for a more elegant look */
                font-weight: 600;   /* Slightly bolder font weight for prominence */
                margin: 0;         /* Remove default margin */
            }}

            header p {{
                font-size: 1em;     /* Standard size for the subheading */
                margin-top: 5px;    /* Reduced margin for tighter spacing */
                font-weight: 400;   /* Regular font weight */
            }}

            footer {{
                background-color: #333;
                color: white;
                text-align: center;
                padding: 1px 15px;
                font-size: 0.9em;
                position: fixed;
                bottom: 0;
                width: 100%;
            }}
            h2 {{
                color: #0056b3;
                font-size: 1.2em;
                margin-bottom: 10px;
            }}
            h3 {{
                color: #0056b3;
                font-size: 1.1em;
                margin-bottom: 10px;
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
                margin-top: 15px;
                box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
            }}
            th, td {{
                padding: 5px; /* Reduced padding for lower row height */
                //text-align: left;
                border: 1px solid #ddd;
                font-size: 0.9em;
            }}
            th {{
                background-color: #173E72;
                color: white;
            }}
            tr:nth-child(even) {{
                background-color: #f9f9f9;
            }}
            tr:hover {{
                background-color: #f1f1f1;
            }}
            .button {{
                margin-top: 10px;
                padding: 8px 12px;
                font-size: 1em;
                cursor: pointer;
                background-color: red;
                color: white;
                border: none;
                border-radius: 4px;
                align-items: center;
            }}
            .toggle-button {{
                padding: 2px 4px; /* Even smaller padding */
                font-size: 0.7em;  /* Smaller font size */
                background-color: #0056b3; /* Light blue color */
                color: white;
                border: none;
                border-radius: 4px;
                cursor: pointer;
                width: auto; /* Let the button size adjust to content */
                height: auto; /* Let the button height adjust to content */
                min-width: 20px; /* Optional: Ensure minimum width */
                min-height: 20px; /* Optional: Ensure minimum height */
            }}
             .summary-item {{
                margin-bottom: 5px;
                display: flex;
                align-items: center;
            }}
            .summary-item i {{
                margin-right: 10px;
                font-size: 1.1em;
                color: #173E72;
            }}
            .summary-item strong {{
                color: #173E72;
                font-size: 1.1em;
            }}
            tr {{
                height: 20px; /* Set a fixed height for all rows */
            }}
            tr {{

                align-items: center; /* Vertically align content */
            }}
            header img {{
                height: 60px;  /* Set the height of the logo */
                width: 150px;  /* Set the width of the logo */
            }}
            
        </style>

        <script>
            function toggleVisibility(id, button) {{
                var element = document.getElementById(id);
                if (element.style.display === 'none') {{
                    element.style.display = 'block';
                    button.innerHTML = "-";  // Change button text to [-]
                }} else {{
                    element.style.display = 'none';
                    button.innerHTML = "+";  // Change button text to [+]
                }}
            }}
        </script>
    </head>
    <body>
        <header>
            <table style="border: none;">
                <tr>
                    <td style="border: none;" width="10%">
                        <img src="{project_logo}" alt="{project_name}">
                    </td>
                    <td style="border: none;">
                        <h1>{project_name} - CSV Comparison Report</h1>
                        <p><strong>CSV File Comparison between Source and Destination Folders</strong></p>
                    </td>
                </tr>
            </table>
        </header>

        <div class="container">
            <h2>Summary Overview</h2>
        
            <div class="summary-item">
                <i class="fas fa-calendar-alt"></i> <strong>Generation Timestamp: &nbsp;</strong> {report_start_time.strftime('%Y-%m-%d %H:%M:%S')}
            </div>
            <div class="summary-item">
                <i class="fas fa-clock"></i> <strong>Processing Duration: &nbsp;</strong> {time_taken_str}
            </div>
            <div class="summary-item">
                <i class="fas fa-folder-open"></i> <strong>Source File Count: &nbsp;</strong> {source_files_count}
            </div>
            <div class="summary-item"> 
                <i class="fas fa-folder"></i> <strong>Destination File Count: &nbsp;</strong> {destination_files_count}
            </div>
            <div class="summary-item">
                <i class="fas fa-key"></i> <strong>Record Identification Keys: &nbsp;</strong> {primary_key_columns}
            </div>
            <div class="summary-item">
                <i class="fas fa-columns"></i> <strong>Data Fields: &nbsp;</strong> [{'All Columns' if not columns else ', '.join(columns)}]
            </div>
            <h3>File Comparison Results</h3>
            <table>
                <thead>
                    <tr>
                        <th>File Name</th>
                        <th>Status</th>
                        <th>Details</th>
                    </tr>
                </thead>
                <tbody>
                    {comparison_rows}
                </tbody>
            </table>
        </div>

        <footer>
            <p>&copy; 2025 CSV Comparison Tool. All Rights Reserved.</p>
        </footer>
    </body>
    </html>
    """

    with open(output_file, 'w') as f:
        f.write(html_content)

    return output_file


# Main function to handle multiple files
def compare_multiple_files(source_folder, destination_folder, primary_key_columns, output_file):
    report_start_time = datetime.now()

    source_files = get_files_from_folder(source_folder)
    destination_files = get_files_from_folder(destination_folder)
    files_to_compare = [f for f in source_files if f in destination_files]

    file_comparisons = {}

    # Add "No Comparison" files
    no_comparison_files = [f for f in source_files if f not in destination_files]

    for file in no_comparison_files:
        file_comparisons[file] = (None, None)

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(compare_csv, os.path.join(source_folder, file), os.path.join(destination_folder, file), primary_key_columns, columns) for file in files_to_compare]
        
        results = []
        #for future in concurrent.futures.as_completed(futures):
        for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc="Comparing files", ncols=100):
         
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                print(f"Error comparing files: {e}")
                results.append((None, None))

        for file, (diff_df, summary) in zip(files_to_compare, results):
            file_comparisons[file] = (diff_df, summary)

    # Generate and save the report
    generate_html_report(file_comparisons, report_start_time, output_file, len(source_files), len(destination_files))
    
    # Open the report in the web browser
    # webbrowser.open(output_file)
    webbrowser.open(f'file:///{os.path.abspath(output_file)}')

# Execute file comparison
compare_multiple_files(source_folder, destination_folder, primary_key_columns, output_file)
