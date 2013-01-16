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

function do_trim_bin(record)
--  local my_ceil = tonumber(record:GetArg('limits'));
  local my_ceil = (record.limits);
  local cat2 = record.cats;
  local y = string.len(cat2);
  if (y > my_ceil) then
    record.cats = 'new string';
  end
  aerospike:update(record)
  return 'TRIM_BIN: ' .. record.cats;
end

function do_update_bin(record)
--  record.bin_to_change    = "changed by lua at "..os.time(); 
  record.bin_to_change    = "changed by lua";
  print('CLIENT: reverted do_update_bin: ' .. record.bin_to_change);
  aerospike:update(record)
  return {s = 'UPDATED_BIN: ' .. record.bin_to_change };
end

function do_new_bin(record)
  if (record.new_bin == nil) then
     record.new_bin = 'new string'; 
  else
     print('CLIENT: new_bin already exists');
  end
  aerospike:update(record)
  return 'NEW_BIN';
end

function do_delete_bin(record) 
    print('CLIENT: DEL BIN');
    record.bin3 = nil;
  aerospike:update(record); 
  return 'DELETE_BIN: ';
end

function do_add_record(record)
   record.lua_bin = "new_value";	
   record.second_bin = "another_value";
   aerospike:create(record);
   return 'ADD_RECORD';
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
  local x = "bin" .. tostring(3);
  if (record[x] ~= 'val3') then return 'FAILURE' ; end
  return 'SUCCESS';
end

function do_noop_function(record)
  print 'CLIENT: This function does not touch any record'
end

-- random runtime crash
function do_handle_bad_lua_1()
  local x;
  i_dont_exist(x);
  return 'OK';
end

-- try to write a bin name too long
function do_handle_bad_lua_2(record)
  local x;
  record[bin_with_a_really_long_name] = "five";
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
  return 'OK';
end


function do_return_types(record)

  local desired_type = record:GetArg('desired_type');
  if (desired_type == "none") then
    print("none");
    return;
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
    return  {s1 = 'have s1', i1=55};
  end
  if (desired_type == "bin_array_with_nesting") then
    print("bin_array_with_nesting");
  	return {s = 'string_resp ', i=22, t={nesting=1, s='yup'}};
  end
end


function do_bin_types(record)
   record.p_int_b = 5;
   record.big_int_b = 1099511627776;
   record.n_int_b = -1;
   record.str_b = "this is a string";
--   record.doc_b = {t1 = 't1 val', t2=77, t3 = {s1="s1 val", s2 = "s2 val"}}; 	
   return 'BIN_TYPES';
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
	record.very_long_name_that_should_fail = "bin 2 value";
	record.last_bin = "bin 3 value";
	return record
end	

function do_too_many_bins(record)
   for i = 1,10000 do
      record[i] = i
   end
   return record
end	

function game_my_test(record)
   local ret = {}
   if (not record:Exists()) then
      ret['type'] = 'bar'
      record.type = 'bar'
      return ret
   else
      ret['foo'] = record.foo
      ret['name'] = record.CustomerName
      --ret[record.type] = 'wer'
      -- table.foreach(record, function(k, v)
      --                          ret[k] = v
      --                       end
      -- )
      return ret
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
    return t
end

function do_updated_copy(record)
   local t = record
   t.c_bin = "new_value"
   t.a_bin = nil
   aerospike:update(record)
   return t
end

function game_echo(record)
    return record
end

function game_meta(record)
   local meta = citrusleaf_meta_set(record)
   return meta
end

function game_inc(record)
   local ret = {}
   if (not record:Exists()) then
      record.Quantity = 1
   else
      record.Quantity = record.Quantity + 1
   end
   ret['num'] = record.Quantity
   return ret
end

function game_double_str(record)
   local ret = {}
   if (not record:Exists()) then
      record.type = 'x'
   else
      record.type = record.type .. record.type
   end
   ret['type'] = record.type
   return ret
end



