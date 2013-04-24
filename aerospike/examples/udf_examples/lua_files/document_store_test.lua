function printTable(name, t, level)
  if (level == 1) then print('CLIENT: printTable(' .. name .. ')'); end
  local k, v;
  local i; local pre ='';
  for i = 1, level do pre = pre .. '>>>>'; end
  for k, v in pairs(t) do
    if (type(v) == "table") then
      print(pre .. 'CLIENT: printTable: TABLE: ' .. k);
      printTable(name, v, level + 1);
    else
      print(pre .. 'CLIENT: printTable: k: ' .. k .. ' v: ' .. v);
    end
  end
  if (level == 1) then print(); end
end

function prefetch_and_print(record)
  print('########CLIENT: PreFetch()');
  record:PreFetch();
  local k,v;
  for k,v in pairs(record.__PreFetchedBins) do
    print('CLIENT: k: ' .. k .. ' vtype: ' .. type(v));
    if (type(v) == "table") then printTable('prefetch_nested', v, 1) end
  end
end

function sp_doc_test(record, limits) 
  print('--------CLIENT: START: sp_doc_test');
  --record:SaveAsJSON();
  local exists  = record:Exists();
  local pf      = false; --pf      = true;
  if (pf) then prefetch_and_print(record) end
  local limits = tonumber(limits);
  local first;
  if (limits == 20) then first = true; else first = false; end

  if (first) then
    print('CLIENT: SETTING: record.t');
    local t = {1,2,3,4,5}; 
    table.insert(t, "thing1");
    local verynested = {11,12,13};
    table.insert(t, verynested);
    record.t = t;
    print('CLIENT: GETTING: record.t'); printTable('record.t', record.t, 1);
  end

  print('CLIENT: GETTING: record.nested');
  printTable('record.nested', record.nested, 1);

  if (first) then
    record.cats = 'new string';
    print('+++++++++++++++++: CLIENT: adding 44 to record.t');
    table.insert(record.t, 44);
    print('+++++++++++++++++: CLIENT: added 44 to record.t');
    record.nested.b = 6;
    record.nested.a = {};
    record.nested.a.z = {};
    record.nested.a.t = {blue = 4};
  else
    print('+++++++++++++++++: CLIENT: doing record.t.tt = 11');
    record.t.tt = 11;
    print('+++++++++++++++++: CLIENT: did record.t.tt = 11');
    print('+++++++++++++++++: CLIENT: adding 22 to record.t');
    table.insert(record.t, 22);
    print('+++++++++++++++++: CLIENT: added 22 to record.t');
    record.nested.x = 111; print('+++++++++++++++++: CLIENT: record.nested.x=111');
    record.nested.y = 222; print('+++++++++++++++++: CLIENT: record.nested.y=222');
    table.insert(record.nested.a.z, 777);
    table.insert(record.nested.a.z, 888);
    table.insert(record.nested.a.z, 999);
    record.nested.a.z['WTF'] = 'AH YEAH';
    for k, v in pairs(record.nested.a.z) do
      print('ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ: CLIENT: k: ' .. k .. ' v: ' .. v);
    end
    for k, v in pairs(record.nested.a.t) do
      print('TTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT: CLIENT: k: ' .. k .. ' v: ' .. v);
    end
  end
  printTable('record.nested', record.nested, 1);
  printTable('record.t',      record.t,      1);
  print('--------CLIENT: END:   sp_doc_test'); print(); print();
  return 'TRIM_BIN: ' .. record.cats;
end
