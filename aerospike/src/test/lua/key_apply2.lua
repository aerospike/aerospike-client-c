
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