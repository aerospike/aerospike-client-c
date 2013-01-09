function do_scan_test(record)
  local bval = record.bin1;
  if ((bval % 3) == 0) then
  	record.bin2 = nil;
  end
  if (bval > 100) then 
  	record:Delete();
  end
end

