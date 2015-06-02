function map_arg(rec, ...)
    local arg = {n = select("#", ...), ...}
    for i=1, arg.n do
        -- Uncomment to retrieve map values.
        -- info("map: {x: %s, y: %s, z: %s}", tostring(arg[i].x), tostring(arg[i].y), tostring(arg[i].z))
    end
end

function one(rec)
    return 1
end

function ten(rec)
    return 10
end

function add(rec, a, b)
    return a + b
end

function record_exists(rec, a)
    if aerospike:exists(rec) then
        info("record exists!")
        return 1
    else
        info("record doesn't exist!")
        return 0
    end
end

function get_bin_a(rec)
    info("record_bin_a 1");
    local a = 1
    info("record_bin_a 2");
    a = rec['a'];
    info("record_bin_a 3");
    return a;
end


function get_bin(rec, a)
    if aerospike:exists(rec) then
        info("record exists!", tostring(rec["a"]))
    else
        info("record doesn't exist!")
    end
    return 1
end

function add_bins(rec, a, b)
    return rec[a] + rec[b]
end
