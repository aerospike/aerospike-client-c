local function select_vector(rec)
    return rec['vec']
end

local function first(a, b)
    return a
end

function vector_reduce(s)
    return s : map(select_vector) : reduce(first)
end

function echo(rec, v)
    return v
end
