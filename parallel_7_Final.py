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


