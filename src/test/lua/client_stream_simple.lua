
local function add(a, b)
    return a + b;
end

local function select(bin) 
    local function f(rec)
        return rec[bin]
    end
    return f;
end

local function one(r)
    return 1;
end

local function filter_none(rec)
	return 1
end

local function map_result(rec)
	return map {idx=rec.c}
end


function count(s)
    return s : map(one) : reduce(add)
end

function sum(s)
    return s : map(select("e")) : reduce(add);
end

function sum_on_match(s, bin, val)

    local function _map(rec)
        if rec[bin] == val then
             return val;
        else
            return 0;
        end
    end

    return s : map(_map) : reduce(add);
end

function filter_passthrough(s)
	return s : filter(filter_none) : map(map_result)
end

function filter_passthrough_digest(s)

	local function map_digest(rec)
		return record.digest(rec)
	end

	return s : filter(filter_none) : map(map_digest)
end
