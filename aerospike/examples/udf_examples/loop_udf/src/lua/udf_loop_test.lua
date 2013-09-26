function do_loop_test(r, k, v)
--  info("+:  %s=%s", k or '<null>', v or '<null>' )
    if k then
--      info("r: %s=%s", k or '<null>', (r[k] or '<null>') )
        if v then
            r[k] = v
--          info("w: %s=%s", k or '<null>', (r[k] or '<null>') )
            if not aerospike:exists(r) then
                aerospike:create(r)
            else
                aerospike:update(r)
            end
        end
    end
    return r[k] or '<null>'
end

function insert(r, k, v)
--  info("+:  %s=%s", k or '<null>', v or '<null>' )
    if k then
--      info("r: %s=%s", k or '<null>', (r[k] or '<null>') )
        if v then
            r[k] = v
--          info("w: %s=%s", k or '<null>', (r[k] or '<null>') )
            if not aerospike:exists(r) then
                aerospike:create(r)
		return 1
	    else
		return 0
            end
	end   
    end
    return -1 
end

function remove(r, k, v)
--  info("+:  %s=%s", k or '<null>', v or '<null>' )
       if aerospike:exists(r) then
          aerospike:remove(r)
	  return 1
       else
	  return 0
       end
end

function update(r, k, v)
--  info("+:  %s=%s", k or '<null>', v or '<null>' )
    if k then
	if v then    
            r[k] = v 
--          info("w: %s=%s", k or '<null>', (r[k] or '<null>') )
            if aerospike:exists(r) then
                aerospike:update(r)
		return 1
	    else
		return 0
            end
	end   
    end
    return -1 
end

function validate(r, k, v)
--  info("+:  %s=%s", k or '<null>', v or '<null>' )
    if k then
--      info("r: %s=%s", k or '<null>', (r[k] or '<null>') )
       if v then
            if aerospike:exists(r) then
		if r[k] ~= v then
		    return 0
		else
		    return 1
		end
            end
	end
    end
    return -1
end

function do_noop_loop()
    return 1
end
