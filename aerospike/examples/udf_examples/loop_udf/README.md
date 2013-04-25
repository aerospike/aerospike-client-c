this is a multi-threaded program which will upload a lua file and execute
a specified function in the file, on a range of records.

There is 1 string parameter passed to the sproc, "read_or_write", which will be "1" 80% of the time
if read/write ratio is set to 80.


Other possible parameters to pass:
-h host [default 127.0.0.1] 
-p port [default 3000]
-n namespace [default test]
-s set [default *all*]
-f udf_file [default lua_files/udf_loop_test.lua]
-x f_name [default udf_loop_test] 
-v is verbose
-r read/write ratio (0-100) [default 80]
-t thread_count [default 8]
-i start_key [default 0]
-j n_keys [default 1000]

