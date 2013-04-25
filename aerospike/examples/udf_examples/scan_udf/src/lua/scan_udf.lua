-- A very basic update bin udf 
function do_scan_test(record)
--  info("Advertiser id before the update %d", record.advertiser)
  record.advertiser = 5;
  aerospike:update(record)
--  info("Advertiser id after the update %d", record.advertiser)
  return record.advertiser
end
