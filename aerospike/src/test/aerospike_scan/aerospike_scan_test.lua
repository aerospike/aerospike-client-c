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

function scan_noop(record)
	return true
end
