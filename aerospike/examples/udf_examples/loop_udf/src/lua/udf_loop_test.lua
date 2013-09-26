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
-- k is bin_name 
-- v is 'key' stored as bin data 

--  info("+:  %s=%s", k or '<null>', v or '<null>' )
    if k then
--      info("r: %s=%s", k or '<null>', (r[k] or '<null>') )
        if v then

	    r['mylist'] = list {v+1, v+2, v+3}
	    r['mymap'] = map {a=v+1, b=v+2, c=v+3}
	    r['hybrid'] = list{map{a=v+1}, map{b=v+2}}
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

-- note that r[k] has key stored as bin data

--  info("+:  %s=%s", k or '<null>', v or '<null>' )
    if k then
--      info("r: %s=%s", k or '<null>', (r[k] or '<null>') )
            if aerospike:exists(r) then
		    
		    local mylist = r['mylist']
		    local mymap = r['mymap']
		    local hybrid = r['hybrid']

		    if mylist[1] ~= r[k]+1 or mylist[2] ~= r[k]+2 or mylist[3] ~= r[k] +3 then
			    return 0
		    end

		    if mymap['a'] ~= r[k]+1 or mymap['b'] ~= r[k]+2 or mymap['c'] ~= r[k]+3 then
			    return 0
		    end

		   if hybrid[1]['a'] ~= r[k]+1 or hybrid[2]['b'] ~= r[k]+2 then
			    return 0
		   end

		   return 1
	 end
    end
  return -1
end

function do_noop_loop()
    return 1
end
