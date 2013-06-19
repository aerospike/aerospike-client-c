
function getboolean(rec)
    return true
end

function getinteger(rec)
    return 123
end

function getstring(rec)
    return "abc"
end

function getlist(rec)
    return list{1,2,3}
end

function getmap(rec)
    return map {["a"] = 1, ["b"] = 2, ["c"] = 3}
end

function add_strings(rec, a, b)
    return a + b
end

function sum(record, a, b)
    return add(record, a, b)
end

function add_local(rec, a, b)
    return a + b
end

function sum_local(record, a, b)
    return add_local(record, a, b)
end

function delete(record)
	record.bin1 = nil
	record.bin2 = nil
	record.bin3 = nil
	return aerospike:update(record)
end