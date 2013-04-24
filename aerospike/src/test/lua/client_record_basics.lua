
function getboolean(record)
    return true
end

function getfloat(record)
    return 123.987
end

function getinteger(record)
    return 123
end

function getstring(record)
    return "abc"
end

function gettable(record)
    return {1,2,3}
end

function getlist(record)
    return list {1,2,3}
end

function getmap(record)
    return map {["a"] = 1, ["b"] = 2, ["c"] = 3}
end

function concat(record, a, b)
    return a .. b
end

function add(record, a, b)
    return a + b
end

function sum(record, a, b)
    return add(record, a, b)
end

local function difference(record, a, b)
    return a - b
end

function diff(record, a, b)
    return difference(record, a, b)
end
