
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
    return a .. b
end

function sum(record, a, b)
    return add(record, a, b)
end

local function add_local(rec, a, b)
    return a + b
end

function sum_local(record, a, b)
    return add_local(record, a, b)
end

local function difference(record, a, b)
    return a - b
end

function diff(record, a, b)
    return difference(record, a, b)
end

function delete(record)
	record.bin1 = nil
	record.bin2 = nil
	record.bin3 = nil
	return aerospike:update(record)
end

function update_record(record)
	record.bina = "string a"
	record.binb = "string b"
	aerospike:create(record);
	record.bina = nil
	aerospike:update(record)
	record.bina = "ah new string"
	record.binc = "string c"
	return aerospike:update(record)
end

function bad_update(record)
	record.bina = "string a"
	record.binb = "string b"
	aerospike:create(record);
	record.bina = nil
	aerospike:update(record)
	record.bina = "string a"
	record.iamabinwithlengthgreaterthan16 = "I will fail"
	record.bin3 = "string c"
	return aerospike:update(record)
end

function bad_create(record)
	record.bina = "string a"
	record.iamabinwithlengthgreaterthan16 = "I will fail"
	aerospike:create(record)
	record.binb = "string b"
	record.binc = "string c"
	return aerospike:update(record)
end