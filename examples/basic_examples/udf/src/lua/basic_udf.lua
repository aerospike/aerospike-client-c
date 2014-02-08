-- A very simple arithmetic UDF with no arguments or return value.
-- It adds 1000 to the value in bin 'test-bin-1' and updates it.
function test_bin_1_add_1000(record)
	record['test-bin-1'] = record['test-bin-1'] + 1000
	aerospike:update(record)
end

-- A simple arithmetic UDF that has arguments and a return value.
-- It updates the value in the specified bin after performing the arithmetic
-- operation, and returns the resulting bin value.
function bin_transform(record, bin_name, x, y)
	record[bin_name] = (record[bin_name] * x) + y
	aerospike:update(record)
	return record[bin_name]
end

