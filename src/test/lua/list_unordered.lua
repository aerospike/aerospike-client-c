function list_unordered(rec, arg1, arg2)
    local ret1 = map()
    ret1['arg1'] = tostring(arg1)
    ret1['arg2'] = tostring(arg2)
    local l1 = rec['list1']
    list.append(l1, arg1)
    list.append(l1, arg2)
    ret1['l1'] = tostring(l1)
    rec['list1'] = l1
    aerospike:update(rec)
    ret1['rec'] = tostring(rec)
    return ret1
end
