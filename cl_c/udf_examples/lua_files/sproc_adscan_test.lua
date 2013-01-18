function do_scan_test(record)
  local bval = record.camp_9;
  if (bval ~= nil) then
    record.camp_9= nil;
  end
  bval = record.camp_1;
  if (bval ~= nil) then 
    record.new_bin = bval;  
  end
  bval = record.camp_5;
  if (bval ~= nil) then
    record:Delete();
  end
end

