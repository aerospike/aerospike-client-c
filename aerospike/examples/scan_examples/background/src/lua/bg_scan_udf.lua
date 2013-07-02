-- A very basic update bin udf 
function test_bin_add_1000(record)
  record['test-bin'] = record['test-bin'] + 1000;
  aerospike:update(record)
end
