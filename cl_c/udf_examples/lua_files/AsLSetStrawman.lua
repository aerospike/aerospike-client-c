-- AS Large Set Strawman V1 -- (Last Update Feb 27, 2013: tjl)
-- Aerospike Set Operations
-- (*) Set Create
-- (*) Set Insert
-- (*) Set Select (and Exists)
-- (*) Set Delete

-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- AS Large Set Utility Functions
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- Get (create) a unique bin name given the current counter.
-- 'LSetBin_XX' will be the individual bins that hold lists of set data
-- ======================================================================
local function getBinName( number )
  local binPrefix = "LSetBin_";
  return binPrefix .. tostring( number );
end

-- ======================================================================
-- setupNewBin: Initialize a new bin -- (the thing that holds a list
-- of user values).
-- Parms:
-- (*) Record
-- (*) Bin Number
-- Return: New Bin Name
-- ======================================================================
local function setupNewBin( record, binNum )
  local mod = "AsLSetStrawman";
  local meth = "setupNewBin()";
  info("[ENTER]: <%s:%s> Bin(%d) \n", mod, meth, binNum );

  local binName = getBinName( binNum );
  record[binName] = list(); -- Create a new list for this new bin

  info("[EXIT]: <%s:%s> BinNum(%d) BinName(%s)\n",mod, meth, binNum, binName );

  return binName;
end

-- ======================================================================
-- The C Version
-- ------------------------------------------------------------
--  The published hash algorithm used in the UNIX ELF format
--  for object files. Accepts a pointer to a string to be hashed
--  and returns an unsigned long.
-- ------------------------------------------------------------
-- unsigned long ElfHash ( const unsigned char *name ) {
--  unsigned long   h = 0, g;
--  while ( *name ) {
--    h = ( h << 4 ) + *name++;
--    if ( g = h & 0xF0000000 )
--      h ^= g >> 24;
--    h &= ~g;
--  }
--  return h;
--}
-- ======================================================================
-- ------------------------------------------------------------
--  The published hash algorithm used in the UNIX ELF format
--  for object files. Accepts a pointer to a string to be hashed
--  and returns an unsigned long.
-- ------------------------------------------------------------
-- local function intHash ( intValue  )
--   local mod = "AsLSetStrawman";
--   local meth = "intHash()";
--   info("[ENTER]: <%s:%s> val(%d) \n", mod, meth, intValue );
--   local h = 0;
--   local g = 0;
--    h = ( h << 4 ) + intValue;
--    if ( g = h and 0xF0000000 ) then
--       h  = h ^ (g >> 24);
--    end
--    h = h bit.band  ~g;
--   info("[EXIT]: <%s:%s> Return (%d) \n", mod, meth, h );
--   return h;
-- end

-- ======================================================================
-- ======================================================================
local function computeSetBin( newValue, distrib )
  local mod = "AsLSetStrawman";
  local meth = "computeSetBin()";
  info("[ENTER]: <%s:%s> val(%s) Dist(%d) \n",
    mod, meth, tostring(newValue), distrib );

  -- TODO: Find a better hash function.
  math.randomseed( newValue );
  local result =  math.random( distrib);
  info("[DEBUG]: <%s:%s> Val(%s) HASH Result(%s) \n",
    mod, meth, tostring(newValue), tostring( result ));

  info("[EXIT]: <%s:%s> Return Result(%s) \n", mod, meth, tostring( result ));
  return result;
end

-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- Scan a List for an item.  Return the item if found.
-- We've added a delete flag that will allow us to remove the element if
-- we choose -- but for now, we are not collapsing the list.
-- Parms:
-- (*) binList: the list of values from the record
-- (*) searchValue: the value we're searching for
-- (*) deleteFlag: if == 1, then replace the found element with nil
-- Return: 0 if not found, Value if found.
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
local function scanList( binList, searchValue, deleteFlag ) 
  local mod = "AsLSetStrawman";
  local meth = "scanList()";
  -- Scan the list for the item, return true if found,
  -- Later, we may return a set of things 
  for i = 1, list.size( binList ), 1 do
    info("[DEBUG]: <%s:%s> It(%d) Comparing SV(%s) with BinV(%s)\n",
    mod, meth, i, tostring(searchValue), tostring(binList[i]));
    if binList[i] == searchValue then
      info("[EXIT]: <%s:%s> Found(%s)\n", mod, meth, tostring(searchValue));
      if( deleteFlag == 1) then
        binList[i] = nil; -- the value is NO MORE
      end
      return searchValue
    end
  end
  info("[EXIT]: <%s:%s> Did NOT Find(%s)\n", mod, meth, tostring(searchValue));
  return 0;
end

-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- AS Large Set Main Functions
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- Requirements/Restrictions (this version).
-- (1) One Set Per Record
--
-- ======================================================================
-- || asLSetCreate ||
-- ======================================================================
-- Create/Initialize a AS LSet structure in a record, using multiple bins
--
-- We will use predetermined BIN names for this initial prototype:
-- 'AsLSetCtrlBin' will be the name of the bin containing the control info
-- 'LSetBin_XX' will be the individual bins that hold lists of set data
-- There can be ONLY ONE set in a record, as we are using preset fixed names
-- for the bin.
-- +========================================================================+
-- | Usr Bin 1 | Usr Bin 2 | o o o | Usr Bin N | Set CTRL BIN | Set Bins... |
-- +========================================================================+
-- Set Ctrl Bin is a Map -- containing control info and the list of
-- bins (each of which has a list) that we're using.
-- Parms:
-- (*) record: The Aerospike Server record on which we operate
-- (*) namespace: The Namespace of the record
-- (*) set: The Set of the record
-- (*) setBinName: The name of the bin for the AS Large Set
-- (*) distrib: The Distribution Factor (how many separate bins) 
--
function asLSetCreate( record, namespace, set, setBinName, distrib )
  local mod = "AsLSetStrawman";
  local meth = "asLSetCreate()";
  info("[ENTER]: <%s:%s> NS(%s) Set(%s) Bin(%s) Dis(%d)\n",
    mod, meth, namespace, set, setBinName, distrib );

  -- Check to see if Set Structure (or anything) is already there,
  -- and if so, error.
  if( record.AsLSetCtrlBin ~= nil ) then
    info("[ERROR EXIT]: <%s:%s> AsLSetCtrlBin Already Exists\n", mod, meth );
    return('AsLSetCtrlBin already exists');
  end

  info("[DEBUG]: <%s:%s> : Initialize SET CTRL Map\n", mod, meth );

  -- If "distrib" is zero, then use our default (16)
  if distrib <= 0 then distrib = 16 end

  -- Define our control information and put it in the record's control bin
  local ctrlMap = map();
  ctrlMap['NameSpace'] = namespace;
  ctrlMap['Set'] = set;
  ctrlMap['ItemCount'] = 0;
  ctrlMap['Modulo'] = distrib;
  record.AsLSetCtrlBin = ctrlMap; -- store in the record

  info("[DEBUG]: <%s:%s> : CTRL Map after Init(%s)\n",
    mod, meth , tostring(ctrlMap));

  -- Create new bins, each with an empty list
  for i = 1, distrib, 1 do
    setupNewBin( record, i );
  end -- for each new bin

  -- All done, store the record
  local rc = -99; -- Use Odd starting Num: so that we know it got changed
  if( not aerospike:exists( record ) ) then
    info("[DEBUG]:<%s:%s>:Create Record()\n", mod, meth );
    rc = aerospike:create( record );
  else
    info("[DEBUG]:<%s:%s>:Update Record()\n", mod, meth );
    rc = aerospike:update( record );
  end

  info("[EXIT]: <%s:%s> : Done.  RC(%d)\n", mod, meth, rc );
  return rc;
end -- function asLSetCreate( record, namespace, set, distrib )

-- ======================================================================
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- || AS Large Set Insert 
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- Insert a value into the set.
-- Take the value, perform a hash and a modulo function to determine which
-- bin list is used, then add to the list.
--
-- We will use predetermined BIN names for this initial prototype
-- 'AsLSetCtrlBin' will be the name of the bin containing the control info
-- 'LSetBin_XX' will be the individual bins that hold lists of data
-- Notice that this means that THERE CAN BE ONLY ONE AS Set object per record.
-- In the final version, this will change -- there will be multiple 
-- AS Set bins per record.  We will switch to a modified bin naming scheme.
--
-- NOTE: Design, V2.  We will cache all data in the FIRST BIN until we
-- reach a certain number N (e.g. 100), and then at N+1 we will create
-- all of the remaining bins in the record and redistribute the numbers, 
-- then insert the 101th value.  That way we save the initial storage
-- cost of small, inactive or dead users.
--
-- +========================================================================+=~
-- | Usr Bin 1 | Usr Bin 2 | o o o | Usr Bin N | Set CTRL BIN | Set Bins... | ~
-- +========================================================================+=~
--    ~=+===========================================+
--    ~ | Set Bin 1 | Set Bin 2 | o o o | Set Bin N |
--    ~=+===========================================+
--            V           V                   V
--        +=======+   +=======+           +=======+
--        |V List |   |V List |           |V List |
--        +=======+   +=======+           +=======+
--
-- Parms:
-- (*) record: the Server record that holds the Large Set Instance
-- (*) setBinName: Bin Name of the Large Set Instance
-- (*) newValue: Value to be inserted into the Large Set
function asLSetInsert( record, setBinName, newValue )
  local mod = "AsLSetStrawman";
  local meth = "asLSetInsert()";
  info("[ENTER]: <%s:%s> SetBin(%s) NewValue(%s)\n",
    mod, meth, setBinName, tostring( newValue ));

  -- Check that the Set Structure is already there, otherwise, error
  if( record.AsLSetCtrlBin == nil ) then
    info("[ERROR EXIT]: <%s:%s> AsLSetCtrlBin does not Exist\n", mod, meth );
    return('AsLSetCtrlBin does not exist');
  end

  -- Find the appropriate bin for the new value and then insert it.
  local ctrlMap = record.AsLSetCtrlBin;
  local modulo = ctrlMap['Modulo'];
  local binNumber = computeSetBin( newValue, modulo );

  local binName = getBinName( binNumber );
  local binList = record[binName];
  if binList == nil then
    info("[ERROR]:<%s:%s> binlist is nill: binname(%s)\n", mod, meth, binName);
  else
    list.append( binList, newValue );
    record[binName] = binList; --  Not sure we have to do this.
  end

  info("[DEBUG]: <%s:%s>:Bin(%s) Now has list(%s)\n",
    mod, meth, binName, tostring(binList) );

  local itemCount = ctrlMap['ItemCount'];
  itemCount = itemCount + 1;
  ctrlMap['ItemCount'] = itemCount;
  record.AsLSetCtrlBin = ctrlMap;
  info("[DEBUG]: <%s:%s> itemCount(%d)\n", mod, meth, itemCount );

  info("[DEBUG]: <%s:%s>Storing Record() with New Value(%s): Map(%s)\n",
    mod, meth, tostring( newValue ), tostring( ctrlMap ) );

  -- All done, store the record
  local rc = -99; -- Use Odd starting Num: so that we know it got changed
  info("[DEBUG]:<%s:%s>:Update Record\n", mod, meth );
  rc = aerospike:update( record );

  info("[EXIT]: <%s:%s> : Done.  RC(%d)\n", mod, meth, rc );
  return rc

end -- function set Insert()

-- ======================================================================
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- || as Large Set Search
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- 
-- Return TRUE if the item exists in the set.
-- So, similar to insert -- take the new value and locate the right bin.
-- Then, scan the bin's list for that item (linear scan).
-- We could test this with a Lua Map to see what it does.
--
-- ======================================================================
function asLSetSearch( record, setBinName, searchValue, exists )
  local mod = "AsLSetStrawman";
  local meth = "asLSetSearch()";
  info("[ENTER]: <%s:%s> Search for Value(%s) \n",
    mod, meth, tostring( searchValue ) );

  -- Check that the Set Structure is already there, otherwise, error
  if( record.AsLSetCtrlBin == nil ) then
    info("[ERROR EXIT]: <%s:%s> AsLSetCtrlBin does not Exist\n", mod, meth );
    return('AsLSetCtrlBin does not exist');
  end

  -- Find the appropriate bin for the Search value
  local ctrlMap = record.AsLSetCtrlBin;
  local modulo = ctrlMap.Modulo;
  local binNumber = computeSetBin( searchValue, modulo );

  local binName = getBinName( binNumber );
  local binList = record[binName];
  local result = scanList( binList, searchValue );

  info("[EXIT]: <%s:%s>: Search Returns (%s) \n",mod,meth,tostring(result));
  return result;
end -- function asLSetSearch()


-- ======================================================================
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- || as Set Delete
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- Find an element (i.e. search) and then remove it from the list.
-- ======================================================================
function asLSetDelete( record, setBinName, deleteValue )
  local mod = "AsLSetStrawman";
  local meth = "asLSetDelete()";
  info("[ENTER]: <%s:%s> Delete Value(%s) \n",
    mod, meth, tostring( deleteValue ) );

  -- Check that the Set Structure is already there, otherwise, error
  if( record.AsLSetCtrlBin == nil ) then
    info("[ERROR EXIT]: <%s:%s> AsLSetCtrlBin does not Exist\n", mod, meth );
    return('AsLSetCtrlBin does not exist');
  end

  -- Find the appropriate bin for the Search value
  local ctrlMap = record.AsLSetCtrlBin;
  local modulo = ctrlMap.Modulo;
  local binNumber = computeSetBin( deleteValue, modulo );

  local binName = getBinName( binNumber );
  local binList = record[binName];
  -- TODO: Add a parm to scanList to NULL OUT the chosen element (but do
  -- not collapse the list).  I assume most users will not want to remove
  -- elements from the set
  local result = scanList( binList, deleteValue, 1);

  info("[DEBUG]: <%s:%s>: Search Result(%s): Now Delete (NOT YET DONE!!!) \n",
    mod, meth, tostring( result ));

  info("[EXIT]: <%s:%s>: Delete Returns (%s) \n",
    mod, meth, tostring( result ));

  return result;

end -- function asLSetDelete()

-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
