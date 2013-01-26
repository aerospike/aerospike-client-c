-- Declare this function as local, o/w applying UDF fails
-- as it assumes it to be a global whose definition it cannot find
local function ssplit(prev_str, delimiter)
  local result = { }
  local from  = 1
  local delim_from, delim_to = string.find( prev_str, delimiter, from  )
  while delim_from do
    table.insert( result, string.sub( prev_str, from , delim_from-1 ) )
    from  = delim_to + 1
    delim_from, delim_to = string.find( prev_str, delimiter, from  )
  end
  table.insert( result, string.sub( prev_str, from  ) )
  return result
end

-- bins contain the type: imp or click - and a timestamp
-- make sure we always keep a click

-- read function
function get_campaign(record, y)
  local camps = ssplit( tostring(y), "," );
  local result = map();
  for i,camp in ipairs(camps) do
	local campaign = "camp_" .. camp;
	if (nil ~= record[campaign]) then
	  result[camp] = record[campaign]
	else
	  result[camp] = "NONE"
	end
  end
  result["code"] = "OK"
  return( result );
end

-- write function
function put_behavior(record, y)
  local behavior = tostring(y);
  if (nil == behavior) then return('FAIL'); end
  local b_tab = ssplit(behavior,",");
  local b_name = "camp_" .. b_tab[1];
  if (nil ~= record[b_name]) then
  	local user = ssplit(record[b_name],",");
	if (user[2] == 'click') then
		if (b_tab[1] == 'click') then
        		record[b_name] = b_tab[2] .. ',' .. b_tab[3];
      		end
      		return 'OK';
    	elseif (b_tab[2] > user[2]) then
      		record[b_name] = b_tab[2] .. ',' .. b_tab[3];
    	end
  else
  	record[b_name] = b_tab[2] .. ',' .. b_tab[3]; 
  end
  if (aerospike:exists(record)) then
	c = aerospike:update(record);
  else
	c = aerospike:create(record);
  end
  return "Record creation/update returned "..c;
end

