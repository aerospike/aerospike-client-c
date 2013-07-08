local function get_test_bin(rec)
	return rec['test-bin']
end

local function add(a,b)
	return a + b
end

function sum_test_bin(stream)
	return stream : map(get_test_bin) : reduce(add)
end

