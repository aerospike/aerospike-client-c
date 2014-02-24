
function getboolean(record)
    return true
end

function getfloat(record)
    return 123.987
end

function getinteger(record)
    return 123
end

function getstring(record)
    return "abc"
end

function gettable(record)
    return {1,2,3}
end

function getlist(record)
    return list {1,2,3}
end

function getmap(record)
    return map {["a"] = 1, ["b"] = 2, ["c"] = 3}
end

function concat(record, a, b)
    return a .. b
end

function add(record, a, b)
    return a + b
end

function sum(record, a, b)
    return add(record, a, b)
end

local function difference(record, a, b)
    return a - b
end

function diff(record, a, b)
    return difference(record, a, b)
end

function update(record)
	record.bin = "first string"
	aerospike:create(record)
	record.bin1 = "second string"
	return aerospike:update(record)
end

function bad_update(record)
	record.bina = "string a"
	record.binb = "string b"
	aerospike:create(record);
	record.bina = nil
	aerospike:update(record)
	record.bina = "string a"
	record.iamabinwithlengthgreaterthan16 = "I will fail"
	record.bin3 = "string c"
	return aerospike:update(record)
end

function create_fail(record)
	record.bina = "string a"
	record.iamabinwithlengthgreaterthan16 = "I will fail"
	aerospike:create(record)
	record.binb = "string b"
	record.binc = "string c"
	return aerospike:update(record)
end

function update2(record)
	record.bina = "string a"
	record.binb = "string b"
	aerospike:create(record);
	record.bina = nil
	aerospike:update(record)
	record.bina = "ah new string"
	record.binc = "string c"
	return aerospike:update(record)
end

function delete(record)
	record.bin1 = nil
	record.bin2 = nil
	record.bin3 = nil
	return aerospike:update(record)
end

function delete_create(record)
	aerospike:remove(record)
	record.bina = "string a"
	record.binb = "string b"
	return aerospike:create(record)
end

function return_types(record, desired_type)

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

function gen(rec)
  -- Add a new bin
  rec.bin2 = 100;
  aerospike:update(rec);
  local g = record.gen(rec);
  info("Generation after update is %d",g);
  return g;
end
 
function ttl(rec)
  local t = record.ttl(rec);
  return t;
end 


