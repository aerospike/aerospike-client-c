-- A very simple arithmetic UDF with no arguments or return value.
-- It adds 1000 to the value in bin 'test-bin' and updates it.
function test_bin_add_1000(rec)
	rec['test-bin'] = rec['test-bin'] + 1000;
	aerospike:update(rec)
end
