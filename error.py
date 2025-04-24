Traceback (most recent call last):                                                                                                                                                  
  File "<frozen runpy>", line 198, in _run_module_as_main
  File "<frozen runpy>", line 88, in _run_code
  File "/usr/lib/python3.12/cProfile.py", line 195, in <module>
    main()
  File "/usr/lib/python3.12/cProfile.py", line 184, in main
    runctx(code, globs, None, options.outfile, options.sort)
  File "/usr/lib/python3.12/cProfile.py", line 21, in runctx
    return _pyprofile._Utils(Profile).runctx(statement, globals, locals,
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/usr/lib/python3.12/profile.py", line 64, in runctx
    prof.runctx(statement, globals, locals)
  File "/usr/lib/python3.12/cProfile.py", line 102, in runctx
    exec(cmd, globals, locals)
  File "csv_compare_mprocess.py", line 1272, in <module>
    diff_df, summary, list_files = run_comparison(download_local=download_local)
                                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "csv_compare_mprocess.py", line 1242, in run_comparison
    diff_df, summary = compare_all_csvs(
                       ^^^^^^^^^^^^^^^^^
  File "csv_compare_mprocess.py", line 648, in compare_all_csvs
    results = list(tqdm(
              ^^^^^^^^^^
  File "/home/ubuntu/.local/lib/python3.12/site-packages/tqdm/std.py", line 1181, in __iter__
    for obj in iterable:
  File "/usr/lib/python3.12/multiprocessing/pool.py", line 873, in next
    raise value
  File "/usr/lib/python3.12/multiprocessing/pool.py", line 540, in _handle_tasks
    put(task)
  File "/usr/lib/python3.12/multiprocessing/connection.py", line 206, in send
    self._send_bytes(_ForkingPickler.dumps(obj))
                     ^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/usr/lib/python3.12/multiprocessing/reduction.py", line 51, in dumps
    cls(buf, protocol).dump(obj)
_pickle.PicklingError: Can't pickle <function process_csv_pair at 0x7318f75b62a0>: attribute lookup process_csv_pair on __main__ failed
