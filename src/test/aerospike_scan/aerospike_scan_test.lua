function scan_insert_bin4(record)
	record.bin4 = list{1,2,3}
	aerospike:update(record)
	return true
end

function scan_append_to_bin4(record)
	list.append(record.bin4, 0)
	aerospike:update(record)
	return true
end

function scan_update_bin4(record)
	record.bin4 = list{4,5,6}
	aerospike:update(record)
	return true
end

function scan_getrec(record)
	return record
end

function scan_delete_bin(record)
	local x = record['bin1']

	if (x < 10) then
		x = nil
        else
		x = 123 
        end

	record['bin1'] = x
	aerospike:update(record)
	return true
end

function scan_dummy_read_update_rec(record)
	local x = record['bin1']
	record['bin1'] = x
	aerospike:update(record)
	return true
end

function scan_delete_rec(record)
	aerospike:remove(record)
	return true
end

function scan_noop(record)
	return true
end
