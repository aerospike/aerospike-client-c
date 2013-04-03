-- AS Large Set (LSET) Operations
-- Steelman V3.1 -- (Last Update Mar 30, 2013: tjl)
--
-- Please refer to lset_design.lua for architecture and design notes.
--
-- Aerospike Large Set (LSET) Operations
-- (*) lset_create():
--          :: Create the LSET object in the bin specified, using the
--          :: creation arguments passed in (probably a package).
-- (*) lset_insert_with_create():
--          :: Insert an item into the Large Set, Create first if needed.
--          :: Apply the creation package on create.
-- (*) lset_insert(): Insert an item into the Large Set
-- (*) lset_select(): Select an item from the Large Set
-- (*) lset_exists(): Test Existence on an item in the set
-- (*) lset_delete(): Delete an item from the set
-- (*) lset_config(): retrieve all current config settings in map format
-- (*) lset_size():   Return the size (e.g. item count) of the Set

-- ======================================================================
-- TO DO List:
-- ======================================================================
-- TODO: (*) Verity that all serious errors call error('msg') rather than
--           just return.
-- TODO: (*) Do Parameter validation for all external calls.
-- TODO: (*) Add lset_insert_with_create(), which is insert plus the
--           creation parms (in case we have to create).
-- Done: (*) Verify order of operations so that the upper level record
--           is NEVER written before all of the lower level ops have
--           successfully complete.
-- ======================================================================
--
-- ======================================================================
-- || GLOBAL PRINT ||
-- ======================================================================
-- Use this flag to enable/disable global printing (the "detail" level
-- in the server).
-- Usage: GP=F and trace()
-- When "F" is true, the trace() call is executed.  When it is false,
-- the trace() call is NOT executed (regardless of the value of GP)
-- ======================================================================
local GP=true; -- Leave this set to true.
local F=true; -- Set F (flag) to true to turn ON global print

-- ===========================================
-- || GLOBAL VALUES -- Local to this module ||
-- ===========================================
local INSERT = 1; -- flag to scanList to INSERT the value (if not found)
local SCAN = 0;
local DELETE = -1; -- flag to show scanList to DELETE the value, if found
local EMPTY = "__empty__"; -- the value is NO MORE

local DEFAULT_DISTRIB = 31;
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- AS Large Set Utility Functions
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||

-- ======================================================================
-- initializeLSetMap:
-- ======================================================================
-- Set up the LSetMap with the standard (default) values.
-- These values may later be overridden by the user.
-- The structure held in the Record's "LSetBIN" is this map.  This single
-- structure contains ALL of the settings/parameters that drive the LSet
-- behavior.
-- Parms:
-- (*) topRec: The Aerospike Server record on which we operate
-- (*) namespace: The Namespace of the record (topRec)
-- (*) set: The Set of the record (topRec)
-- (*) setBinName: The name of the bin for the AS Large Set
-- (*) distrib: The Distribution Factor (how many separate bins) 
-- Return: The initialized lsetCtrlMap.
-- It is the job of the caller to store in the rec bin and call update()
-- ======================================================================
local function initializeLSetMap(topRec, namespace, set, lsetBinName, distrib )
  local mod = "AsLSetSteelman";
  local meth = "initializeLSetMap()";
  GP=F and trace("[ENTER]: <%s:%s>:: NS(%s) Set(%s) BinName(%s) D(%s)",
    mod, meth, tostring(namespace), tostring(set), tostring(lsetBinName),
    tostring( distrib));

  -- If "distrib" is bad (zero, negative or nil)  then use our default (31)
  -- Best to use a prime number.
  if (distrib == nil or distrib <= 0) then
    distrib = DEFAULT_DISTRIB;
  end

  -- Create the map, and fill it in.
  -- Note: All Field Names start with UPPER CASE.
  local lsetCtrlMap = map();
  lsetCtrlMap.StoreState = 0; -- always start in "compact mode"
  lsetCtrlMap.BinName = setBinName;
  lsetCtrlMap.ItemCount = 0;   -- Count of valid elements
  lsetCtrlMap.TotalCount = 0;  -- Count of both valid and deleted elements
  lsetCtrlMap.Modulo = distrib;
  lsetCtrlMap.ThreshHold = 4; -- Rehash after this many have been inserted

  -- NOTE: Version 2: We will information here about value complexity and
  -- how to find the key.  If values are atomic (e.g. int or string) then
  -- we use the WHOLE value, otherwise we expect a MAP type, which will
  -- have a KEY field in it (which is what we'll search/hash on).
  -- So, when we do type(value), we expect "number", "string" or
  -- "userdata" (which MUST be a map with a KEY field).
  lsetCtrlMap.KeyType = 0; -- assume "atomic" values for now.
  
  GP=F and trace("[ENTER]: <%s:%s>:: lsetCtrlMap(%s)",
                 mod, meth, tostring(lsetCtrlMap));

  return lsetCtrlMap, distrib;
end -- initializeLSetMap()

-- ======================================================================
-- adjustLsetMap:
-- ======================================================================
-- Using the settings supplied by the caller in the Create() call,
-- we adjust the values in the lsetCtrlMap.
-- Parms:
-- (*) lsetCtrlMap: the main LSET Bin value
-- (*) argListMap: Map of User Override LSET Settings 
-- ======================================================================
local function adjustLSetMap( lsetCtrlMap, argListMap )
  local mod = "LSetSteelman";
  local meth = "adjustLSetMap()";
  GP=F and trace("[ENTER]: <%s:%s>:: LSetMap(%s)::\n ArgListMap(%s)",
                 mod, meth, tostring(lsetCtrlMap), tostring( argListMap ));

  -- Iterate thru the argListMap and adjust (override) the map settings 
  -- based on the settings passed in during the stackCreate() call.
  -- CREATE_ARGLIST='{
  -- "Modulo":67
  -- "KeyType":1}'
  for name, value in map.pairs( argListMap ) do
    if name  == "Modulo" then
      if type( value ) == "number" and value > 0 and value < 221 then
        lsetCtrlMap.Modulo = value;
      end
    elseif name == "KeyType" then
      if type( value ) == "number" and value > 0 then
        lsetCtrlMap.KeyType = value;
      end
    end
  end -- foreach arg

  GP=F and trace("[EXIT]: <%s:%s> : CTRL Map after Adjust(%s)",
    mod, meth , tostring(lsetCtrlMap));
  return lsetCtrlMap
end -- adjustLSetMap


-- ======================================================================
-- We use the "CRC32" package for hashing the value in order to distribute
-- the value to the appropriate "sub lists".
-- ======================================================================
local  CRC32 = require('CRC32');
-- ======================================================================
-- Return the hash of "value", with modulo.
-- Notice that we can use ZERO, because this is not an array index
-- (which would be ONE-based for Lua) but is just used as a name.
-- ======================================================================
local function stringHash( value, modulo )
  if value ~= nil and type(value) == "string" then
    return CRC32.Hash( value ) % modulo;
  else
    return 0;
  end
end -- stringHash

-- ======================================================================
-- Return the hash of "value", with modulo
-- Notice that we can use ZERO, because this is not an array index
-- (which would be ONE-based for Lua) but is just used as a name.
-- NOTE: Use a better Hash Function.
-- ======================================================================
local function numberHash( value, modulo )
  local mod = "AsLSetSteelman";
  local meth = "numberHash()";
  local result = 0;
  if value ~= nil and type(value) == "number" then
    -- math.randomseed( value ); return math.random( modulo );
    result = CRC32.Hash( value ) % modulo;
  end
  GP=F and trace("[EXIT]:<%s:%s>HashResult(%s)", mod, meth, tostring(result))
  return result
end -- numberHash

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
-- (*) topRec
-- (*) Bin Number
-- Return: New Bin Name
-- ======================================================================
local function setupNewBin( topRec, binNum )
  local mod = "AsLSetSteelman";
  local meth = "setupNewBin()";
  GP=F and trace("[ENTER]: <%s:%s> Bin(%d) \n", mod, meth, binNum );

  local binName = getBinName( binNum );
  topRec[binName] = list(); -- Create a new list for this new bin

  GP=F and trace("[EXIT]: <%s:%s> BinNum(%d) BinName(%s)\n",
                 mod, meth, binNum, binName );

  return binName;
end -- setupNewBin

-- ======================================================================
-- computeSetBin()
-- Find the right bin for this value.
-- First -- know if we're in "compact" StoreState or "regular" 
-- StoreState.  In compact mode, we ALWAYS look in the single bin.
-- Second -- use the right hash function (depending on the type).
-- And, know if it's an atomic type or complex type.
-- ======================================================================
local function computeSetBin( newValue, lsetCtrlMap )
  local mod = "AsLSetSteelman";
  local meth = "computeSetBin()";
  GP=F and trace("[ENTER]: <%s:%s> val(%s) Map(%s) \n",
                 mod, meth, tostring(newValue), tostring(lsetCtrlMap) );

  -- Check StoreState
  local binNumber  = 0;
  if lsetCtrlMap.StoreState == 0 then
    return 0
  else
    if type(newValue) == "number" then
      binNumber  = numberHash( newValue, lsetCtrlMap.Modulo );
    elseif type(newValue) == "string" then
      binNumber  = stringHash( newValue, lsetCtrlMap.Modulo );
    elseif type(newValue) == "userdata" then
      binNumber  = stringHash( newValue.KEY, lsetCtrlMap.Modulo );
    else -- error case
      warn("[ERROR]<%s:%s>Unexpected Type (should be number, string or map)",
           mod, meth );
      error('ERROR: Incorrect Type for new Large Set value');
    end
  end
  GP=F and trace("[EXIT]: <%s:%s> Val(%s) BinNumber (%d) \n",
                 mod, meth, tostring(newValue), binNumber );

  return binNumber;
end -- computeSetBin()

-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- Scan a List for an item.  Return the item if found.
-- This is COMPLEX SCAN, which means we are comparing the KEY field of the
-- map object in both the value and in the List.
-- We've added a delete flag that will allow us to remove the element if
-- we choose -- but for now, we are not collapsing the list.
-- Parms:
-- (*) binList: the list of values from the record
-- (*) value: the value we're searching for
-- (*) flag:
--     ==> if ==  1 (INSERT): insert the element IF NOT FOUND
--     ==> if ==  0 (SCAN): then return element if found, else return nil
--     ==> if == -1 (DELETE):  then replace the found element with nil
-- Return:
-- For SCAN and DELETE:
--    nil if not found, Value if found.
--   (NOTE: Can't return 0 -- because that might be a valid value)
-- For INSERT:
-- Return 0 if found (and not inserted), otherwise 1 if inserted.
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
local function complexScanList(lsetCtrlMap, binList, value, flag ) 
  local mod = "AsLSetSteelman";
  local meth = "complexScanList()";
  local result = nil;
  -- Scan the list for the item, return true if found,
  -- Later, we may return a set of things 
  for i = 1, list.size( binList ), 1 do
    GP=F and trace("[DEBUG]: <%s:%s> It(%d) Comparing SV(%s) with BinV(%s)\n",
                   mod, meth, i, tostring(value), tostring(binList[i]));
    if binList[i] ~= nil and binList[i] ~= EMPTY and
       binList[i].KEY == value.KEY
    then
      result = binList[i]; -- save the thing we found.
      GP=F and trace("[EARLY EXIT]: <%s:%s> Found(%s)\n",
                     mod, meth, tostring(value));
      if( flag == DELETE ) then
        binList[i] = EMPTY; -- the value is NO MORE
        -- Decrement ItemCount (valid entries) but TotalCount stays the same
        local itemCount = lsetCtrlMap.ItemCount;
        lsetCtrlMap.ItemCount = itemCount - 1;
      elseif flag == INSERT then
        return 0 -- show caller nothing got inserted (don't count it)
      end
      return result
    end
  end -- for each list entry in this binList

  -- Didn't find it.  If INSERT, then append the value to the list
  if flag == INSERT then
    GP=F and trace("[DEBUG]: <%s:%s> INSERTING(%s)\n",
                   mod, meth, tostring(value));
    list.append( binList, value );
    return 1 -- show caller we did an insert
  end
  GP=F and trace("[LATE EXIT]: <%s:%s> Did NOT Find(%s)\n",
    mod, meth, tostring(value));
  return nil;
end -- complexScanList

-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- Scan a List for an item.  Return the item if found.
-- This is SIMPLE SCAN, where we are assuming ATOMIC values.
-- We've added a delete flag that will allow us to remove the element if
-- we choose -- but for now, we are not collapsing the list.
-- Parms:
-- (*) binList: the list of values from the record
-- (*) value: the value we're searching for
-- (*) flag:
--     ==> if ==  1 (INSERT): insert the element IF NOT FOUND
--     ==> if ==  0 (SCAN): then return element if found, else return nil
--     ==> if == -1 (DELETE):  then replace the found element with nil
-- Return:
-- For SCAN and DELETE:
--    nil if not found, Value if found.
--   (NOTE: Can't return 0 -- because that might be a valid value)
-- For INSERT:
-- Return 0 if found (and not inserted), otherwise 1 if inserted.
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
local function simpleScanList(lsetCtrlMap, binList, value, flag ) 
  local mod = "AsLSetSteelman";
  local meth = "simpleScanList()";
  GP=F and trace("[ENTER]: <%s:%s> Looking for V(%s), ListSize(%d) List(%s)",
                 mod, meth, tostring(value), list.size(binList),
                 tostring(binList))

  local result = nil;
  -- Scan the list for the item, return true if found,
  -- Later, we may return a set of things 
  for i = 1, list.size( binList ), 1 do
    GP=F and trace("[DEBUG]: <%s:%s> It(%d) Comparing SV(%s) with BinV(%s)\n",
                   mod, meth, i, tostring(value), tostring(binList[i]));
    if binList[i] ~= nil and binList[i] == value then
      result = binList[i];
      GP=F and trace("[EARLY EXIT]: <%s:%s> Found(%s)\n",
        mod, meth, tostring(value));
      if( flag == DELETE ) then
        binList[i] = EMPTY; -- the value is NO MORE
        -- Decrement ItemCount (valid entries) but TotalCount stays the same
        local itemCount = lsetCtrlMap.ItemCount;
        lsetCtrlMap.ItemCount = itemCount - 1;
      elseif flag == INSERT then
        return 0 -- show caller nothing got inserted (don't count it)
      end
      return result;
    end
  end

  -- Didn't find it.  If INSERT, then append the value to the list
  if flag == INSERT then
    GP=F and trace("[EXIT]: <%s:%s> INSERTING(%s)\n",
                   mod, meth, tostring(value));
    list.append( binList, value );
    return 1 -- show caller we did an insert
  end
  GP=F and trace("[LATE EXIT]: <%s:%s> Did NOT Find(%s)\n",
                 mod, meth, tostring(value));
  return nil;
end -- simpleScanList


-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- Scan a List for an item.  Return the item if found.
-- Since there are two types of scans (simple, complex), we do the test
-- up front and call the appropriate scan type (rather than do the test
-- of which compare to do -- for EACH value.
-- Parms:
-- (*) lsetCtrlMap: the control map -- so we can see the type of key
-- (*) binList: the list of values from the record
-- (*) searchValue: the value we're searching for
-- (*) flag:
--     ==> if == -1 (DELETE):  then replace the found element with nil
--     ==> if ==  0 (SCAN): then return element if found, else return nil
--     ==> if ==  1 (INSERT): insert the element IF NOT FOUND
-- Return: nil if not found, Value if found.
-- (NOTE: Can't return 0 -- because that might be a valid value)
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
local function scanList( lsetCtrlMap, binList, searchValue, flag ) 
  local mod = "AsLSetSteelman";
  local meth = "scanList()";

  if lsetCtrlMap.KeyType == 0 then
    return simpleScanList(lsetCtrlMap, binList, searchValue, flag ) 
  else
    return complexScanList(lsetCtrlMap, binList, searchValue, flag ) 
  end
end


-- ======================================================================
-- localInsert( lsetCtrlMap, newValue, stats )
-- ======================================================================
-- Perform the main work of insert (used by both rehash and insert)
-- Parms:
-- (*) topRec: The top DB Record:
-- (*) lsetCtrlMap: The AsLSet control map
-- (*) newValue: Value to be inserted
-- (*) stats: 1=Please update Counts, 0=Do NOT update counts (rehash)
-- ======================================================================
local function localInsert( topRec, lsetCtrlMap, newValue, stats )
  local mod = "AsLSetSteelman";
  local meth = "localInsert()";
  GP=F and trace("[ENTER]:<%s:%s>Insert(%s)", mod, meth, tostring(newValue));

  -- Notice that "computeSetBin()" will know which number to return, depending
  -- on whether we're in "compact" or "regular" storageState.
  local binNumber = computeSetBin( newValue, lsetCtrlMap );
  local binName = getBinName( binNumber );
  GP=F and trace("[DEBUG]:<%s:%s> Compute:BNum(%s) BName(%s) Val(%s) Map(%s)",
                 mod, meth, tostring(binNumber), tostring(binName),
                 tostring(newValue), tostring(lsetCtrlMap));

  local binList = topRec[binName];

  local insertResult = 0;
  if binList == nil then
    GP=F and warn("[INTERNAL ERROR]:<%s:%s> binList is nil: binName(%s)",
                   mod, meth, tostring( binName ) );
    error('Insert: INTERNAL ERROR: Nil Bin');
  else
    -- Look for the value, and insert if it is not there.
    insertResult = scanList( lsetCtrlMap, binList, newValue, INSERT );
    -- list.append( binList, newValue );
    topRec[binName] = binList;
  end

  GP=F and trace("[DEBUG]: <%s:%s>:Bin(%s) Now has list(%s)",
                 mod, meth, binName, tostring(binList) );

  if stats == 1 and insertResult == 1 then -- Update Stats if success
    local itemCount = lsetCtrlMap.ItemCount;
    local totalCount = lsetCtrlMap.TotalCount;
    lsetCtrlMap.ItemCount = itemCount + 1; -- number of valid items goes up
    lsetCtrlMap.TotalCount = totalCount + 1; -- Total number of items goes up
    GP=F and trace("[DEBUG]: <%s:%s> itemCount(%d)", mod, meth, itemCount );
  end
  topRec['AsLSetCtrlBin'] = lsetCtrlMap;

  GP=F and trace("[EXIT]: <%s:%s>Storing Record() with New Value(%s): Map(%s)",
                 mod, meth, tostring( newValue ), tostring( lsetCtrlMap ) );
    -- No need to return anything
end -- localInsert


-- ======================================================================
-- rehashSet( topRec, setBinName, lsetCtrlMap )
-- ======================================================================
-- When we start in "compact" StoreState (value 0), we eventually have
-- to switch to "regular" state when we get enough values.  So, at some
-- point (StoreThreshHold), we rehash all of the values in the single
-- bin and properly store them in their final resting bins.
-- So -- copy out all of the items from bin 1, null out the bin, and
-- then resinsert them using "regular" mode.
-- Parms:
-- (*) topRec
-- (*) setBinName
-- (*) lsetCtrlMap
-- ======================================================================
local function rehashSet( topRec, setBinName, lsetCtrlMap )
  local mod = "AsLSetSteelman";
  local meth = "rehashSet()";
  GP=F and trace("[ENTER]:<%s:%s> !!!! REHASH !!!! ", mod, meth );
  GP=F and trace("[ENTER]:<%s:%s> !!!! REHASH !!!! ", mod, meth );

  -- Get the list, make a copy, then iterate thru it, re-inserting each one.
  local singleBinName = getBinName( 0 );
  local singleBinList = topRec[singleBinName];
  if singleBinList == nil then
    warn("[INTERNAL ERROR]:<%s:%s> Rehash can't use Empty Bin (%s) list",
         mod, meth, tostring(singleBinName));
    error('BAD BIN 0 LIST for Rehash');
  end
  local listCopy = list.take( singleBinList, list.size( singleBinList ));
  topRec[singleBinName] = nil; -- this will be reset shortly.
  lsetCtrlMap.StoreState = 1; -- now in "regular" (modulo) mode
  
  -- Rebuild. Allocate new lists for all of the bins, then re-insert.
  -- Create ALL of the new bins, each with an empty list
  -- Our "indexing" starts with ZERO, to match the modulo arithmetic.
  local distrib = lsetCtrlMap.Modulo;
  for i = 0, (distrib - 1), 1 do
    setupNewBin( topRec, i );
  end -- for each new bin

  for i = 1, list.size(listCopy), 1 do
    localInsert( topRec, lsetCtrlMap, listCopy[i], 0 ); -- do NOT update counts.
  end

  GP=F and trace("[EXIT]: <%s:%s>", mod, meth );
end -- rehashSet()


-- ======================================================================
-- validateBinName(): Validate that the user's bin name for this large
-- object complies with the rules of Aerospike. Currently, a bin name
-- cannot be larger than 14 characters (a seemingly low limit).
-- ======================================================================
local function validateBinName( binName )
  local mod = "LSetSteelMan";
  local meth = "validateBinName()";
  GP=F and trace("[ENTER]: <%s:%s> validate Bin Name(%s)",
    mod, meth, tostring(binName));

  if binName == nil  then
    error('Bin Name Validation Error: Null BinName');
  elseif type( binName ) ~= "string"  then
    error('Bin Name Validation Error: BinName must be a string');
  elseif string.len( binName ) > 14 then
    error('Bin Name Validation Error: Exceeds 14 characters');
  end
end -- validateBinName

-- ======================================================================
-- validateRecBinAndMap():
-- Check that the topRec, the lsetBinName and lsetMap are valid, otherwise
-- jump out with an error() call.
-- Parms:
-- (*) topRec:
-- ======================================================================
local function validateRecBinAndMap( topRec, binName, mustExist )
    local mod = "LSetSteelMan";
    local meth = "validateRecBinAndMap()";

    GP=F and trace("[ENTER]: <%s:%s>  ", mod, meth );

    if( not aerospike:exists( topRec ) and mustExist == true ) then
        warn("[ERROR EXIT]:<%s:%s>:Missing Record. Exit", mod, meth );
        error('Base Record Does NOT exist');
    end

    -- Verify that the LSET Structure is there: otherwise, error.
    if binName == nil  or type(binName) ~= "string" then
        warn("[ERROR EXIT]: <%s:%s> Bad LSET BIN Parameter", mod, meth );
        error('Bad LSET Bin Parameter');
    end

    -- Validate that binName follows the Aerospike rules
    validateBinName( binName );

    if( topRec[binName] == nil ) then
        warn("[ERROR EXIT]: <%s:%s> LSET_BIN (%s) DOES NOT Exists",
        mod, meth, tostring(binName) );
        error('LSET_BIN Does NOT exist');
    end

    -- check that our bin is (mostly) there
    local lsetMap = topRec[binName]; -- The main lset map
    if lsetMap.Magic ~= "MAGIC" then
        GP=F and
        warn("[ERROR EXIT]: <%s:%s> LSET_BIN (%s) Is Corrupted (no magic)",
            mod, meth, binName );
        error('LSET_BIN Is Corrupted');
    end
end -- validateRecBinAndMap()

-- ======================================================================
-- validateTopRec( topRec, lsetMap )
-- ======================================================================
-- Validate that the top record looks valid:
-- Get the LSET bin from the rec and check for magic
-- Return: "good" or "bad"
-- ======================================================================
local function  validateTopRec( topRec, lsetMap )
  local thisMap = topRec[lsetMap.BinName];
  if thisMap.Magic == "MAGIC" then
    return "good"
  else
    return "bad"
  end
end -- validateTopRec()

-- ======================================================================
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- AS Large Set Main Functions
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- Requirements/Restrictions (this version).
-- (1) One Set Per Record
--
-- ======================================================================
-- || aerospike_lset_create ||
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
-- (*) topRec: The Aerospike Server record on which we operate
-- (*) setBinName: The name of the bin for the AS Large Set
-- (*) distrib: The Distribution Factor (how many separate bins) 
function aerospike_lset_create( topRec, setBinName, distrib )
  local mod = "AsLSetSteelman";
  local meth = "aerospike_lset_create()";
  GP=F and trace("[ENTER]: <%s:%s> Bin(%s) Dis(%d)\n",
                 mod, meth, setBinName, distrib );

  -- Check to see if Set Structure (or anything) is already there,
  -- and if so, error.
  if( topRec['AsLSetCtrlBin'] ~= nil ) then
    GP=F and warn("[ERROR EXIT]: <%s:%s> AsLSetCtrlBin Already Exists",
                   mod, meth );
    error('AsLSetCtrlBin already exists');
  end
  
  -- This will throw and error and jump out of Lua if binName is bad.
  validateBinName( setBinName );

  GP=F and trace("[DEBUG]: <%s:%s> : Initialize SET CTRL Map", mod, meth );
  local lsetCtrlMap;
  lsetCtrlMap, distrib = initializeLSetMap( topRec, setBinName, distrib );

  GP=F and trace("[DEBUG]: <%s:%s> : CTRL Map after Init(%s)",
                 mod, meth , tostring(lsetCtrlMap));

  topRec['AsLSetCtrlBin'] = lsetCtrlMap; -- store in the record

  -- initializeLSetMap always sets lsetCtrlMap.StoreState to 0
  -- At this point there is only one bin
  setupNewBin( topRec, 0 );

  -- All done, store the record
  local rc = -99; -- Use Odd starting Num: so that we know it got changed
  if( not aerospike:exists( topRec ) ) then
    GP=F and trace("[DEBUG]:<%s:%s>:Create Record()", mod, meth );
    rc = aerospike:create( topRec );
  else
    GP=F and trace("[DEBUG]:<%s:%s>:Update Record()", mod, meth );
    rc = aerospike:update( topRec );
  end

  GP=F and trace("[EXIT]: <%s:%s> : Done.  RC(%d)", mod, meth, rc );
  return rc;
end

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
-- ==> The CtrlMap will show which state we are in:
-- (*) StoreState=0: We are in SINGLE BIN state (no hash)
-- (*) StoreState=1: We hash, mod N, then insert (append) into THAT bin.
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
-- (*) topRec: the Server record that holds the Large Set Instance
-- (*) setBinName: The name of the bin for the AS Large Set
-- (*) distrib: The Distribution Factor (how many separate bins) 
-- (*) newValue: Value to be inserted into the Large Set
function aerospike_lset_insert( topRec, setBinName, newValue )
  local mod = "AsLSetSteelman";
  local meth = "aerospike_lset_insert()";
  GP=F and trace("[ENTER]:<%s:%s> SetBin(%s) NewValue(%s)",
                 mod, meth, tostring(setBinName), tostring( newValue ));

  local lsetCtrlMap, distrib;
  
  -- This will throw and error and jump out of Lua if binName is bad.
  validateBinName( setBinName );

  -- Check that the Set Structure is already there, otherwise, error
  if( topRec['AsLSetCtrlBin'] == nil ) then
    warn("[WARNING]: <%s:%s> AsLSetCtrlBin does not Exist:Creating",
         mod, meth );
    lsetCtrlMap, distrib =
      initializeLSetMap( topRec, setBinName, DEFAULT_DISTRIB );
    topRec['AsLSetCtrlBin'] = lsetCtrlMap;
    -- initializeLSetMap always sets lsetCtrlMap.StoreState to 0
    -- At this point there is only one bin
    setupNewBin( topRec, 0 );
  else
    lsetCtrlMap = topRec['AsLSetCtrlBin'];
  end

  -- When we're in "Compact" mode, before each insert, look to see if 
  -- it's time to rehash our single bin into all bins.
  local totalCount = lsetCtrlMap.TotalCount;
  if lsetCtrlMap.StoreState == 0 and totalCount >= lsetCtrlMap.ThreshHold then
    rehashSet( topRec, setBinName, lsetCtrlMap );
  end

  -- Call our local multi-purpose insert() to do the job.(Update Stats)
  localInsert( topRec, lsetCtrlMap, newValue, 1 );

  topRec['AsLSetCtrlBin'] = lsetCtrlMap;
  -- All done, store the record
  local rc = -99; -- Use Odd starting Num: so that we know it got changed
  if( not aerospike:exists( topRec ) ) then
    GP=F and trace("[DEBUG]:<%s:%s>:Create Record()", mod, meth );
    rc = aerospike:create( topRec );
  else
    GP=F and trace("[DEBUG]:<%s:%s>:Update Record()", mod, meth );
    rc = aerospike:update( topRec );
  end

  GP=F and trace("[EXIT]: <%s:%s> : Done.  RC(%d)", mod, meth, rc );
  return rc
end -- function set Insert()

-- ======================================================================
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- || as Large Set Exists
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- 
-- Return 1 item if the item exists in the set, otherwise return 0.
-- We don't want to return "true" and "false" because of Lua Weirdness
-- So, similar to insert -- take the new value and locate the right bin.
-- Then, scan the bin's list for that item (linear scan).
--
-- ======================================================================
function aerospike_lset_exists( topRec, setBinName, searchValue )
  local mod = "AsLSetSteelman";
  local meth = "aerospike_lset_search()";
  GP=F and trace("[ENTER]: <%s:%s> Search for Value(%s)",
                 mod, meth, tostring( searchValue ) );

  -- This will throw and error and jump out of Lua if binName is bad.
  validateBinName( setBinName );

  -- Check that the Set Structure is already there, otherwise, error
  if( topRec['AsLSetCtrlBin'] == nil ) then
    GP=F and warn("[ERROR EXIT]: <%s:%s> AsLSetCtrlBin does not Exist",
                   mod, meth );
    error('AsLSetCtrlBin does not exist');
  end

  -- Find the appropriate bin for the Search value
  local lsetCtrlMap = topRec['AsLSetCtrlBin'];
  local binNumber = computeSetBin( searchValue, lsetCtrlMap );
  local binName = getBinName( binNumber );
  local binList = topRec[binName];
  -- local result = scanList( binList, searchValue, 0 ); -- do NOT delete
  local result = scanList( lsetCtrlMap, binList, searchValue, SCAN );
  if result == nil then
    return 0
  else
    return 1
  end
end -- function aerospike_lset_search()


-- ======================================================================
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- || as Large Set Search
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- 
-- Return the item if the item exists in the set.
-- So, similar to insert -- take the new value and locate the right bin.
-- Then, scan the bin's list for that item (linear scan).
--
-- ======================================================================
function aerospike_lset_search( topRec, setBinName, searchValue )
  local mod = "AsLSetSteelman";
  local meth = "aerospike_lset_search()";
  GP=F and trace("[ENTER]: <%s:%s> Search for Value(%s)",
                 mod, meth, tostring( searchValue ) );


  -- This will throw and error and jump out of Lua if binName is bad.
  validateBinName( setBinName );

  -- Check that the Set Structure is already there, otherwise, error
  if( topRec['AsLSetCtrlBin'] == nil ) then
    GP=F and warn("[ERROR EXIT]: <%s:%s> AsLSetCtrlBin does not Exist",
                   mod, meth );
    error('AsLSetCtrlBin does not exist');
  end

  -- Find the appropriate bin for the Search value
  local lsetCtrlMap = topRec['AsLSetCtrlBin'];
  local binNumber = computeSetBin( searchValue, lsetCtrlMap );
  local binName = getBinName( binNumber );
  local binList = topRec[binName];
  -- local result = scanList( binList, searchValue, 0 ); -- do NOT delete
  local result = scanList( lsetCtrlMap, binList, searchValue, SCAN );

  GP=F and trace("[EXIT]: <%s:%s>: Search Returns (%s)",
                 mod, meth, tostring(result));
    if result == nil then
      return ('Not Found')
    else
      return result;
    end
end -- function aerospike_lset_search()

-- ======================================================================
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- || as Set Delete
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- Find an element (i.e. search) and then remove it from the list.
-- Return the element if found, return nil if not found.
-- ======================================================================
function aerospike_lset_delete( topRec, setBinName, deleteValue )
  local mod = "AsLSetSteelman";
  local meth = "aerospike_lset_delete()";
  GP=F and trace("[ENTER]: <%s:%s> Delete Value(%s)",
                 mod, meth, tostring( deleteValue ) );


  -- This will throw and error and jump out of Lua if binName is bad.
  validateBinName( setBinName );

  -- Check that the Set Structure is already there, otherwise, error
  if( topRec['AsLSetCtrlBin'] == nil ) then
    GP=F and trace("[ERROR EXIT]: <%s:%s> AsLSetCtrlBin does not Exist",
                   mod, meth );
    return('AsLSetCtrlBin does not exist');
  end

  -- Find the appropriate bin for the Search value
  local lsetCtrlMap = topRec['AsLSetCtrlBin'];
  local binNumber = computeSetBin( deleteValue, lsetCtrlMap );

  local binName = getBinName( binNumber );
  local binList = topRec[binName];
  -- Fow now, scanList() will only NULL out the element in a list, but will
  -- not collapse it.  Later, if we see that there are a LOT of nil entries,
  -- we can RESET the set and remove all of the "gas".
  -- local result = scanList( binList, deleteValue, 1);
  local result = scanList( lsetCtrlMap, binList, deleteValue, DELETE );
  -- If we found something, then we need to update the bin and the record.
  if result ~= nil then
    -- We found something -- and marked it nil -- so update the record
    topRec[binName] = binList;
    rc = aerospike:update( topRec );
  end

  GP=F and trace("[EXIT]: <%s:%s>: Delete Returns (%s) \n",
                 mod, meth, tostring( result ));

  return result;
end -- function aerospike_lset_delete()

-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
