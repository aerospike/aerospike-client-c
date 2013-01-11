function do_loop_test(r, k, v)
    info("+:  %s=%s", k or '<null>', v or '<null>' )
    if k then
        info("r: %s=%s", k or '<null>', (r[k] or '<null>') )
        if v then
            r[k] = v
            info("w: %s=%s", k or '<null>', (r[k] or '<null>') )
            if not aerospike:exists(r) then
                aerospike:create(r)
            else
                aerospike:update(r)
            end
        end
    end
    return r[k] or '<null>'
end

function do_noop_loop()
    return 1
end