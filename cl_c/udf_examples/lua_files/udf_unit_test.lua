function printTable(t)
  local k, v;
  for k, v in pairs(t) do
    if (type(v) == "table") then
      print('++++CLIENT_PRINTTABLE: k: ' .. k .. ' vtype: ' .. type(v));
    else
      print('++++CLIENT_PRINTTABLE: k: ' .. k .. ' v: ' .. v);
    end
  end
end

function prefetch_and_print(record)
  print('########CLIENT: PreFetch()');
  record:PreFetch();
  local k,v;
  for k,v in pairs(record.__PreFetchedBins) do
    print('CLIENT: k: ' .. k .. ' vtype: ' .. type(v));
    if (type(v) == "table") then printTable(v) end
  end
end

function do_trim_bin(record, limits)
  local cat2 = record.cats;
  local y = string.len(cat2);
  local myceil = tonumber(limits);
  if (y > myceil) then
    record.cats = 'new string';
  end
  local x = aerospike:update(record);
  if (x == 0) then
	return 'TRIM_BIN: ' .. record.cats;
  else 
	return 'Aerospike update failed with '..x; 
  end
end

function do_update_bin(record, k, v)
  record[k] = v
  info("Value of bin = %s",record[k]);
  local x;
  if not aerospike:exists(record) then
	x = aerospike:create(record);
  else
	x = aerospike:update(record);
  end
  if ( x == 0 ) then
     return 'Bin update/create returned '..x;
  else 
     return 'Bin update failed with '..x;
  end
end

function do_new_bin(record)
  if (record.new_bin == nil) then
     record.new_bin = 'new string'; 
  else
     print('CLIENT: new_bin already exists');
     return('new_bin already exists');
  end
  local x = aerospike:update(record);
  if ( x == 0 ) then
	return 'Successfully added new_bin';
  else 
	return 'Failed to add new_bin %d'..x;
  end
end

function do_delete_bin(record) 
    print('CLIENT: DEL BIN');
    record.bin3 = nil;
  local x = aerospike:update(record); 
  return 'Record updation returned '..x;
end

function do_add_record(record)
   record.lua_bin = "new_value";	
   record.second_bin = "another_value";
   local x = aerospike:create(record);
   return 'Record creation returned '..x;
end

function do_delete_record(record) 
--  record:Delete();
  aerospike:remove(record);
  return 'DELETE_RECORD: ';
end

function do_echo_record(record) 
	return record;
end

function do_read1_record(record)
  if (record.bin1 ~= 'val1') then return 'FAILURE' ; end
  print ('CLIENT: Read bin1 success');
  if (record.bin2 ~= 'val2') then return 'FAILURE' ; end
  print ('CLIENT: Read bin2 success');
  local y = 3;
  local x = "bin"..y;
  if (record[x] ~= 'val3') then return 'FAILURE' ; end
  return 'SUCCESS';
end

function do_noop_function(record)
  print 'CLIENT: This function does not touch any record' 
  return 'OK';
end

-- random runtime crash
function do_undefined_global()
  local x;
  i_dont_exist(x);
  return 'OK';
end

function do_lua_functional_test(record)
  local x = {}
  table.insert(x, "thing1");
  table.insert(x, { "nest1", "nest2" } );
  table.insert(x, 3);
  x.foo = "bar";
  x.foo2 = "bar2";
  if ( #x ~= 3 ) then record.status = "FAIL"; return 'FAIL1'; end
  if ( x[1] ~= "thing1" ) then record.status = "FAIL"; return "FAIL2"; end
  if ( x["foo"] ~= "bar" ) then record.status = "FAIL"; return "FAIL3"; end
  -- add more tests here
  record.status = "OK";
  local ret = aerospike:create(record);
  return 'Creation of record returned '..ret;
end


function do_return_types(record, desired_type)

--  local desired_type = record:GetArg('desired_type');
  if (desired_type == "none") then
    print("none");
    return nil;
  end
  if (desired_type == "p_int_primitive") then
    print("positive int");
    return 5;
  end  
  if (desired_type == "n_int_primitive") then
    print("negative int");
    return -5;
  end  
  if (desired_type == "string_primitive") then
    print("string");
    return "good";
  end  
  if (desired_type == "bin_array") then
    print("bin_array");
    local l = list();
    list.append(l,'have s1');
    list.append(l,55);
    return  l;
  end
  if (desired_type == "bin_nested_list") then
    info("bin_nested_list");
	  local x = list();
    local y = list();
    list.append(x,1);
    list.append(x,'yup');
    list.append(y,'string_resp');
    list.append(y,x);
    return y
  end
  if (desired_type == "bin_map") then
    info("bin_map");
    
    local m = map{};
    m["i"] = 456
    m["s"] = "def"
    m["l"] = list{4,5,6};

    local l = list();
    list.append(l,456);
    list.append(l,"def");

    local x = map{};
    x["i"] = 123;
    x["s"] = "abc";
    x["l"] = l;
    x["m"] = m;
    return x;
  end
end


function do_bin_types(record)
   record.p_int_b = 5;
   record.big_int_b = 1099511627776;
   record.n_int_b = -1;
   record.str_b = "this is a string";
--   record.doc_b = {t1 = 't1 val', t2=77, t3 = {s1="s1 val", s2 = "s2 val"}}; 	
   local x = aerospike:create(record);
   return 'Record creation returned '..x;
end


function do_read_bin_types(record)
   if (record.str_b ~= "this is a string") then
       return 'FAILED str_b';
   end
   if (record.p_int_b ~= 5) then
       return 'FAILED p_int_b'..record.p_int_b;
   end
   if (record.big_int_b ~= 1099511627776) then
       return 'FAILED big_int_b';
   end
   if (record.n_int_b ~= -1) then
       return 'FAILED n_int_b';
   end 
   return 'BIN_TYPES_READ';
end

function do_long_binname(record)
	record.short_bin = "bin 1 value";
	info("short_bin added to record");
	record['very_long_name_that_should_fail'] = "bin 2 value";
	info("very_long_name_that_should_fail added to record");
	record.last_bin = "bin 3 value";
	info("last_bin added to record");
	local x = aerospike:create(record);
 	return "Long binname test returned "..x;	
end	

function do_too_many_bins(record)
   local count = 0, x, y;
   for i = 1,10000 do
      record[i] = i
      y = aerospike:exists(record);
      if ( y == 0 ) then 
	      x = aerospike:create(record); 
      else  
              x = aerospike:update(record);
      end 
      if ( x == 0 ) then 
        count = count + 1;
      else 
      	info("Bin not added for %d with %d",i,x);
      end
   end
   info ("count of objects updated is %d",count);
   return 'Updated '..count..' records';
end	

function game_my_test(record)
   local ret = {}
   if (not aerospike:exists(record)) then
      record.type = 'bar'
      return record.type
   else
      ret['foo'] = record.foo
      ret['name'] = record.CustomerName
      --ret[record.type] = 'wer'
      -- table.foreach(record, function(k, v)
      --                          ret[k] = v
      --                       end
      -- )
      return ret['name'];
   end
end

function game_foreach(record)
   local t = record
   local ret = {}
   table.foreach(t, function(k, v)
                       ret[k] = v
                    end
              )
   return ret
end

function do_copy_record(record)
	local t = record
	return 'No-op for now';
end

function do_updated_copy(record)
	local t = record
	t.c_bin = "new_value"
	t.a_bin = nil
	local x = aerospike:update(record);
	if ( x == 0 ) then
		return "Successfully updated record";
	else 
		return "Recrod updation failed with "..x;
	end
end

function game_echo(record)
    return record;
end
function game_orderid(record)
	return 'Order ID is '..record['OrderID'];
end
function game_meta(record)
   local meta = citrusleaf_meta_set(record)
   return meta
end

function game_inc(record)
   if (not aerospike:exists(record)) then
      record.Quantity = 1
      aerospike:create(record);
   else
      record.Quantity = record.Quantity + 1;
      aerospike:update(record);
   end
   return 'Quantity is '..record.Quantity;
end

function game_double_str(record)
   local ret = {}
   if (not aerospike:exists(record)) then
      record.type = 'x'
      aerospike:create(record)
   else
      record.type = record.type .. record.type
      aerospike:update(record)
   end
--   return 'Record updated with '..record.type;
end



