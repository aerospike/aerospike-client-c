-- A simple UDF that just returns the value of the bin 'test-bin'.
local function get_test_bin(record)
	return record['test-bin']
end

-- A simple arithmetic UDF that adds two arguments and returns the result.
local function add(a, b)
	return a + b
end

-- A aggregation UDF that uses the local UDFs above to execute a 'map reduce'
-- operation and return the overall result.
function sum_test_bin(stream)
	return stream : map(get_test_bin) : reduce(add)
end

