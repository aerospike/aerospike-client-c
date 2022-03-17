local function put_bin(r,name,value)
    if not aerospike:exists(r) then aerospike:create(r) end
    r[name] = value
    aerospike:update(r)
end

function write_bin(r,name,value)
    put_bin(r,name,value)
end

function write_bin_validate(r,name,value)
    if (value >= 1 and value <= 10) then
		put_bin(r,name,value)
    else
        error("1000:Invalid value") 
    end
end

function update_map(rec, k, v)
    if (not aerospike:exists(rec)) then
        info('record does not exist. creating record.')
        aerospike:create(rec)
        rec.m = map()
        rec.m[k] = 0
    end
    
    info(tostring(rec.m))

    local m = rec.m;

    m['a'] = m[k] + v
    m['b'] = m[k] + v + 1
    m['c'] = m[k] + v + 2

    rec.m = m;

    info(tostring(rec.m))

    info('updating record...')
    if aerospike:update(rec) then
        info('updating record succeeded.')
        info(tostring(rec.m))
        return tostring(rec.m)
    else
        info('updating record failed.')
        return 'failed to update'
    end
end
