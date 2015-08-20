function get_nil()
    return nil
end

function get_true()
    return true
end

function get_false()
    return false
end

function get_string()
    return "abc"
end

function get_integer()
    return 123
end

function get_map()
    return map {
        a = 1,
        b = 2,
        c = 3
    }
end

function get_list()
    return list {1,2,3}
end

function get_bytes()
	local b = bytes(3)
	bytes.set_string(b, 1, 'zyx')
	return b
end

function modify_bytes(rec, b)
	bytes.set_byte(b, 2, 135)
	return b
end

function get_rec_map(rec)
    local m = map()
	local b = bytes(3)
    m.t = true
    m.f = false
    m.n = nil
    m.i = 123
    m.s = "abc"
    m.l = list{1,2,3}
	bytes.set_string(b, 1, 'zyx')
	m.b = b
    rec['map'] = m
    return rec['map']
end


function write_f(r,b,b2,b3,v)
    r[b] = v
    r[b2] = list {1.2,3.4}
    r[b3] = map { a=5.6, b=7.8}
    if not aerospike:exists(r) then
        aerospike:create(r)
    else
        aerospike:update(r)
    end
end

function read_f(r,b)
    if not aerospike:exists(r) then
        error("record does not exist")
    end
    return r[b]
end
