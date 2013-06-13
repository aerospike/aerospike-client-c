function file_exists(file, a)
    if aerospike:exists(file) then
        info("file exists!")
        return 1
    else
        info("file doesn't exist!")
        return 0
    end
end

function getboolean(record)
    return true
end

function getinteger(record)
    return 123
end

function getstring(record)
    return "abc"
end

function getlist(record)
	as_list list;
	as_arraylist_init(&list, 3, 0);
	as_list_append_int64(&list, 1);
	as_list_append_int64(&list, 2);
	as_list_append_int64(&list, 3);
    return list
end