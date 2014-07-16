-- A very simple arithmetic UDF with no arguments or return value.
-- It adds 1000 to the value in bin 'test-bin-1' and updates it.
function test_bin_1_add_1000(rec)
	rec['test-bin-1'] = rec['test-bin-1'] + 1000
	aerospike:update(rec)
end

-- A simple arithmetic UDF that has arguments and a return value.
-- It updates the value in the specified bin after performing the arithmetic
-- operation, and returns the resulting bin value.
function bin_transform(rec, bin_name, x, y)
	rec[bin_name] = (rec[bin_name] * x) + y
	aerospike:update(rec)
	return rec[bin_name]
end

