def run_comparison():
    source1_zips = list_zip_files(source_1_prefix)
    source2_zips = list_zip_files(source_2_prefix)

    zip1_map = {zip_key.split("/")[-1]: zip_key for zip_key in source1_zips}
    zip2_map = {zip_key.split("/")[-1]: zip_key for zip_key in source2_zips}

    all_zip_names = set(zip1_map.keys()).union(set(zip2_map.keys()))

    all_diffs = []
    all_summaries = {}

    def process_zip_pair(zip_name):
        zip1_key = zip1_map.get(zip_name)
        zip2_key = zip2_map.get(zip_name)

        zip1_csvs = read_zip_from_s3(zip1_key) if zip1_key else {}
        zip2_csvs = read_zip_from_s3(zip2_key) if zip2_key else {}

        file_diffs = []
        file_summary = {}

        common_csvs = set(zip1_csvs.keys()).intersection(set(zip2_csvs.keys()))
        missing_in_1 = set(zip2_csvs.keys()) - set(zip1_csvs.keys())
        missing_in_2 = set(zip1_csvs.keys()) - set(zip2_csvs.keys())

        # Log missing CSVs
        if missing_in_1:
            file_summary['Missing CSVs in File1'] = list(missing_in_1)
        if missing_in_2:
            file_summary['Missing CSVs in File2'] = list(missing_in_2)

        for csv_file in common_csvs:
            df1 = zip1_csvs[csv_file]
            df2 = zip2_csvs[csv_file]
            diff_df, summary = compare_csvs(df1, df2, csv_file)
            if not diff_df.empty:
                file_diffs.append(diff_df)
            file_summary[csv_file] = summary

        return pd.concat(file_diffs) if file_diffs else pd.DataFrame(), file_summary

    if use_multithreading:
        with ThreadPoolExecutor() as executor:
            results = list(executor.map(process_zip_pair, all_zip_names))
    else:
        results = [process_zip_pair(zip_name) for zip_name in all_zip_names]

    for diff_df, summary in results:
        if not diff_df.empty:
            all_diffs.append(diff_df)
        all_summaries.update(summary)

    final_diff_df = pd.concat(all_diffs) if all_diffs else pd.DataFrame()
    return final_diff_df, all_summaries
