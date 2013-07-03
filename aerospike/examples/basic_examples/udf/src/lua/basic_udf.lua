-- A very simple arithmetic UDF.
function test_bin_1_add_1000(record)
  record['test-bin-1'] = record['test-bin-1'] + 1000
  aerospike:update(record)
end
-- A simple UDF that has arguments and a return value.
function bin_transform(record, bin_name, x, y)
  record[bin_name] = (record[bin_name] * x) + y
  aerospike:update(record)
  return record[bin_name]
end

