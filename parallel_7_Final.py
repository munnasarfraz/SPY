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



