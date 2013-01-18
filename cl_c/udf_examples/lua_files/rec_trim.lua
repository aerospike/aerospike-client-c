function trim_bin(record) 
  local my_ceil = tonumber(record.__args['limits']); 
  local cat2    = record.cats; 
  --print('cat2'); print(cat2);
  local y       = string.len(cat2); 
  record.cats = "c0,c1"; 
  return 'TRIM_BIN: ' .. record.cats;
end
