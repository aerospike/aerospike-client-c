
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