
__LLCreate = function() print('__LLCreate');
  return {type = "LIST", count = 0, wops = 0, els = {}};
end

__LLEN         = function(self, v)
  return self.count;
end
__RPUSH        = function(self, v) print('__RPUSH: v: ' .. v);
  self.wops           = self.wops + 1;
  local node          = { val = v, wops = self.wops }
  self.els[self.wops] = node;
  self.count          = self.count + 1;
  if (self.head == nil) then self.head = node; self.last = node; return; end
  if (self.last ~= nil) then
    node.prv                     = self.last.wops;
    self.els[self.last.wops].nex = self.wops;
  end
  self.last  = node;
end
__LPUSH        = function(self, v)
  self.wops           = self.wops + 1;
  local node          = { val = v, wops = self.wops }
  self.els[self.wops] = node;
  self.count          = self.count + 1;
  if (self.head == nil) then self.head = node; self.last = node; return; end
  if (self.head ~= nil) then
    node.nex                     = self.head.wops;
    self.els[self.head.wops].prv = self.wops;
  end
  self.head  = node;
end

remove_node  = function(self, cw)
  if (cw == nil) then return; end
  if     (self.head.wops == cw) then
    self.head     = self.els[self.head.nex];
    self.head.prv = nil;
  elseif (self.last.wops == cw) then
    self.last     = self.els[self.last.prv];
    self.last.nex = nil;
  else
    if (self.els[cw].prv ~= nil) then
      self.els[self.els[cw].prv].nex = self.els[cw].nex;
    end
    if (self.els[cw].nex ~= nil) then
      self.els[self.els[cw].nex].prv = self.els[cw].prv;
    end
  end
  self.count   = self.count - 1;
  self.els[cw] = nil;
end
get_next     = function(self, curr, cnt) --print('get_next');
  local ret = {};
  while (true) do
    if (cnt == 0) then return ret; end
    table.insert(ret, curr.val);
    curr = self.els[curr.nex];
    cnt  = cnt - 1;
  end
end
rem_next     = function(self, curr, cnt) --print('rem_next');
  while (true) do
    if (cnt == 0) then return; end
    remove_node(self, curr.wops);
    curr = self.els[curr.nex];
    cnt  = cnt - 1;
  end
end
find_index   = function(self, index, fnc, arg)
  if (math.abs(index) > self.count) then return nil; end
  local curr;
  if (index > 0) then
    curr = self.els[self.head.wops];
    while (true) do
      if (index == 0) then break; end
      curr = self.els[curr.nex];
      index = index - 1;
    end
  else
    curr = self.els[self.last.wops];
    while (true) do
      index = index + 1;
      if (index == 0) then break; end
      curr = self.els[curr.prv];
    end
  end
  if (fnc ~= nil) then return fnc(self, curr, arg);
  else                 return curr.val;
  end
end

__LINDEX       = function(self, index)
  return find_index(self, index);
end
__LRANGE       = function(self, beg, fin)
  return find_index(self, beg, get_next, fin);
end
replace_node = function(self, curr, nval) --print('replace_node');
  curr.val = nval;
end
__LSET         = function(self, index, val)
  return find_index(self, index, replace_node, val);
end
__LTRIM        = function(self, beg, fin) --print('__LTRIM');
  return find_index(self, beg, rem_next, fin);
end

__RPOP         = function(self, v)
  local curr = self.last; remove_node(self, curr.wops); return curr.val;
end
__LPOP         = function(self, v)
  local curr = self.head; remove_node(self, curr.wops); return curr.val;
end

__LREM         = function(self, cnt, val)
  if (self.head == nil) then return nil; end
  if (cnt > 0) then
    local curr = self.head;
    while (curr.nex) do
      if (val == curr.val) then
        remove_node(self, curr.wops); cnt = cnt - 1;
        if (cnt == 0) then break; end
      end
      curr = self.els[curr.nex];
    end
  else
    local curr = self.last;
    while (curr.prv) do
      if (val == curr.val) then
        remove_node(self, curr.wops); cnt = cnt - 1;
        if (cnt == 0) then break; end
      end
      curr = self.els[curr.prv];
    end
  end
end

__DUMP         = function(self)
  if (self.head == nil) then print('__DUMP: count: 0'); return end;
  local i    = 0;
  local curr = self.els[self.head.wops];
  print('__DUMP: count: ' .. self.count);
  while (curr) do
    print('__DUMP:(' .. i .. ') val: ' .. self.els[curr.wops].val);
    i    = i + 1;
    curr = self.els[curr.nex];
  end
end
 
test = function()
  L = __LLCreate();
  __RPUSH(L, 1); __RPUSH(L, 2); __RPUSH(L, 3); __RPUSH(L, 4);
  __RPUSH(L, 5); __RPUSH(L, 5); __RPUSH(L, 5); __RPUSH(L, 5);
  __RPUSH(L, 6); __RPUSH(L, 7); __RPUSH(L, 8);
  __LPUSH(L, 0); __LPUSH(L, -1);
  print('__LLEN: '       .. __LLEN(L));
  print('__LINDEX(2): '  .. __LINDEX(L, 2));
  print('__LINDEX(-6): ' .. __LINDEX(L, -6));
  t = __LRANGE(L, 4, 2);
  for k, v in pairs(t) do
    print('__LRANGE: k: ' .. k .. ' v: ' .. v);
  end
  print('__LPOP: '       .. __LPOP(L));
  print('__LPOP: '       .. __LPOP(L));
  print('__RPOP: '       .. __RPOP(L));
  __LREM(L, 2, 5);
  print('__LLEN: '       .. __LLEN(L));
  __LSET(L, 3, 99);
  __LTRIM(L, 5, 2);
  __DUMP(L);
end
--test();

function LPUSH(record)
  local binname   = record:GetArg('binname');
  local arg1      = record:GetArg('arg1');
  --print('LPUSH: binname: ' .. binname .. ' arg1: ' .. arg1);
  if (record[binname] == nil) then record[binname] = __LLCreate(); end
  __LPUSH(record[binname], arg1);  
  --print('LPUSH'); __DUMP(record[binname]);
  return __LLEN(record[binname]);
end
function RPUSH(record)
  local binname   = record:GetArg('binname');
  local arg1      = record:GetArg('arg1');
  if (record[binname] == nil) then record[binname] = __LLCreate(); end
  __RPUSH(record[binname], arg1);  
  return __LLEN(record[binname]);
end
function LLEN(record)
  local binname   = record:GetArg('binname');
  if (record[binname] == nil) then record[binname] = __LLCreate(); end
  return __LLEN(record[binname]);
end
function LINDEX(record)
  local binname   = record:GetArg('binname');
  local arg1      = record:GetArg('arg1');
  if (record[binname] == nil) then record[binname] = __LLCreate(); end
  return __LINDEX(record[binname], arg1);
end
function LPOP(record)
  local binname   = record:GetArg('binname');
  if (record[binname] == nil) then record[binname] = __LLCreate(); end
  return __LPOP(record[binname]);
end
function RPOP(record)
  local binname   = record:GetArg('binname');
  if (record[binname] == nil) then record[binname] = __LLCreate(); end
  return __RPOP(record[binname]);
end
function LREM(record)
  local binname   = record:GetArg('binname');
  local arg1      = record:GetArg('arg1');
  local arg2      = record:GetArg('arg2');
  if (record[binname] == nil) then record[binname] = __LLCreate(); end
  return __LREM(record[binname], arg1, arg2);
end
function LSET(record)
  local binname   = record:GetArg('binname');
  local arg1      = record:GetArg('arg1');
  local arg2      = record:GetArg('arg2');
  if (record[binname] == nil) then record[binname] = __LLCreate(); end
  return __LSET(record[binname], arg1, arg2);
end
function LTRIM(record)
  local binname   = record:GetArg('binname');
  local arg1      = record:GetArg('arg1');
  local arg2      = record:GetArg('arg2');
  if (record[binname] == nil) then record[binname] = __LLCreate(); end
  __DUMP(record[binname]);
  return __LTRIM(record[binname], arg1, arg2);
end
