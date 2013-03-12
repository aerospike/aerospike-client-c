-- ======================================================================
-- Large Stack Object (LSO) Operations
-- LsoStoneman V1.1 -- (March 7, 2013)
-- ======================================================================
-- LSO Design and Type Comments:
--
-- The LSO value is a new "particle type" that exists ONLY on the server.
-- It is a complex type (it includes infrastructure that is used by
-- server storage), so it can only be viewed or manipulated by Lua and C
-- functions on the server.  It is represented by a Lua MAP object that
-- comprises control information, a directory of records (for "warm data")
-- and a "Cold List Head" ptr to a linked list of directory structures
-- that each point to the records that hold the actual data values.
--
-- LSO Functions Supported
-- (*) stackCreate: Create the LSO structure in the chosen topRec bin
-- (*) stackPush: Push a user value (AS_VAL) onto the stack
-- (*) stackPeek: Read N values from the stack, in LIFO order
-- (*) stackTrim: Release all but the top N values.
-- ==> stackPush and stackPeek() functions each have the option of passing
-- in a Transformation/Filter UDF that modify values before storage or
-- modify and filter values during retrieval.
-- (*) stackPushWithUDF: Push a user value (AS_VAL) onto the stack, 
--     calling the supplied UDF on the value FIRST to transform it before
--     storing it on the stack.
-- (*) stackPeekWithUDF: Retrieve N values from the stack, and for each
--     value, apply the transformation/filter UDF to the value before
--     adding it to the result list.  If the value doesn't pass the
--     filter, the filter returns nil, and thus it would not be added
--     to the result list.
-- ======================================================================
-- TO DO List:
-- TODO: Finish transferWarmCacheList() method. (Implement Cold Cache)
-- TODO: Make this work for both REGULAR and BINARY Mode
-- TODO: hotCacheTransfer(): Make this more efficient
-- TODO: hotCacheInsert(): Must finish Compact Storage
-- TODO: Implement Trim(): Must release storage before record delete.
-- ======================================================================
-- Aerospike Calls:
-- newRec = aerospike:crec_create( topRec )
-- newRec = aerospike:crec_open( topRec, digest)
-- status = aerospike:crec_update( topRec, newRec )
-- status = aerospike:crec_close( topRec, newRec )
-- digest = record.digest( newRec )
-- ======================================================================
-- For additional Documentation, please see LsoDesign.lua
-- ======================================================================
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- || FUNCTION TABLE ||
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- Table of Functions: Used for Transformation and Filter Functions.
-- This is held in UdfFunctionTable.lua.  Look there for details.
-- ======================================================================
-- || GLOBAL PRINT ||
-- ======================================================================
-- Use this flag to enable/disable global printing (the "detail" level
-- in the server).
-- ======================================================================
local GP=true;
local F=false; -- Set F (flag) to true to turn ON global print

-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- LSO Utility Functions
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- These are all local functions to this module and serve various
-- utility and assistance functions.

-- ======================================================================
-- initializeLsoMap:
-- ======================================================================
-- Set up the LSO Map with the standard (default) values.
-- These values may later be overridden by the user.
-- The structure held in the Record's "LSO BIN" is this map.  This single
-- structure contains ALL of the settings/parameters that drive the LSO
-- behavior.
-- ======================================================================
local function initializeLsoMap( topRec, lsoBinName )
  local mod = "LsoStoneman";
  local meth = "initializeLsoMap()";
  GP=F and trace("[ENTER]: <%s:%s>:: LsoBinName(%s)",
    mod, meth, tostring(lsoBinName));

  -- Create the map, and fill it in.
  -- Note: All Field Names start with UPPER CASE.
  local lsoMap = map();
  -- General LSO Parms:
  lsoMap.ItemCount = 0;     -- A count of all items in the stack
  lsoMap.DesignVersion = 1; -- Current version of the code
  lsoMap.Magic = "MAGIC"; -- we will use this to verify we have a valid map
  lsoMap.BinName = lsoBinName; -- Defines the LSO Bin
  lsoMap.NameSpace = "test"; -- Default NS Name -- to be overridden by user
  lsoMap.Set = "set";       -- Default Set Name -- to be overridden by user
  lsoMap.PageMode = "Binary"; -- "List" or "Binary":
  -- LSO Data Record Chunk Settings: Passed into "Chunk Create"
--lsoMap.LdrEntryCountMax = 200;  -- Max # of items in a Data Chunk (List Mode)
  lsoMap.LdrEntryCountMax =  10;  -- Max # of items in a Data Chunk (List Mode)
  lsoMap.LdrByteEntrySize = 18;  -- Byte size of a fixed size Byte Entry
--lsoMap.LdrByteCountMax = 2000; -- Max # of BYTES in a Data Chunk (binary mode)
  lsoMap.LdrByteCountMax =   80; -- Max # of BYTES in a Data Chunk (binary mode)
  -- Hot Entry List Settings: List of User Entries
  lsoMap.HotCacheList = list();
  lsoMap.HotCacheItemCount = 0; -- Number of elements in the Top Cache
--lsoMap.HotCacheMax = 200; -- Max Number for the cache -- when we transfer
  lsoMap.HotCacheMax =  10; -- Max Number for the cache -- when we transfer
--lsoMap.HotCacheTransfer = 100; -- How much to Transfer at a time.
  lsoMap.HotCacheTransfer =  4; -- How much to Transfer at a time.
  -- Warm Digest List Settings: List of Digests of LSO Data Records
  lsoMap.WarmTopFull = 0; -- 1  when the top chunk is full (for the next write)
  lsoMap.WarmCacheList = list();   -- Define a new list for the Warm Stuff
  lsoMap.WarmChunkCount = 0; -- Number of Warm Data Record Chunks
  lsoMap.WarmCacheDirMax = 10000; -- Number of Warm Data Record Chunks
  lsoMap.WarmChunkTransfer = 10; -- Number of Warm Data Record Chunks
  lsoMap.WarmTopChunkEntryCount = 0; -- Count of entries in top warm chunk
  lsoMap.WarmTopChunkByteCount = 0; -- Count of bytes used in top warm Chunk
  -- Cold Directory List Settings: List of Directory Pages
  lsoMap.ColdChunkCount = 0; -- Number of Cold Data Record Chunks
  lsoMap.ColdDirCount = 0; -- Number of Cold Data Record DIRECTORY Chunks
  lsoMap.ColdListHead  = 0;   -- Nothing here yet
  lsoMap.ColdCacheDirMax = 100;  -- Should be a setable parm

  if globalPrint then
    GP=F and trace("[DEBUG]: <%s:%s> : CTRL Map after Init(%s)",
      mod, meth , tostring(lsoMap));
  end

  -- Put our new map in the record, then store the record.
  topRec[lsoBinName] = lsoMap;

  GP=F and trace("[EXIT]:<%s:%s>:", mod, meth );
  return lsoMap
end -- initializeLsoMap


-- ======================================================================
-- adjustLsoMap:
-- ======================================================================
-- Using the settings supplied by the caller in the stackCreate call,
-- we adjust the values in the LsoMap.
-- Parms:
-- (*) lsoMap: the main LSO Bin value
-- (*) argListMap: Map of LSO Settings 
-- ======================================================================
local function adjustLsoMap( lsoMap, argListMap )
  local mod = "LsoStoneman";
  local meth = "adjustLsoMap()";
  GP=F and trace("[ENTER]: <%s:%s>:: LsoMap(%s)::\n ArgListMap(%s)",
    mod, meth, tostring(lsoMap), tostring( argListMap ));

  -- Iterate thru the argListMap and adjust (override) the map settings 
  -- based on the settings passed in during the stackCreate() call.
  GP=F and trace("[DEBUG]: <%s:%s> : Processing Arguments:(%s)",
    mod, meth, tostring(argListMap));

  if type( argListMap.PageMode ) == "string" then
  GP=F and trace("[DEBUG]: <%s:%s> : Processing PageMode", mod, meth );
    -- Verify it's a valid value
    if argListMap.PageMode == "List" or argListMap.PageMode == "Binary" then
      lsoMap.PageMode = argListMap.PageMode;
    end
  end
  if type( argListMap.HotListSize ) == "number" then
    GP=F and trace("[DEBUG]: <%s:%s> : Processing Hot List", mod, meth );
    GP=F and trace("<LINE 148> HotListSize(%s)", tostring(argListMap.HotListSize) );
    if argListMap.HotListSize > 0 then
      lsoMap.HotCacheMax = argListMap.HotListSize;
    end
  end
  if type( argListMap.HotListTransfer ) == "number" then
    GP=F and trace("[DEBUG]: <%s:%s> : Processing Hot List Transfer", mod, meth );
    if argListMap.HotListTransfer > 0 then
      lsoMap.HotCacheTransfer = argListMap.HotListTransfer;
    end
  end
  if type( argListMap.ByteEntrySize ) == "number" then
    GP=F and trace("[DEBUG]: <%s:%s> : Processing ByteEntrySize", mod, meth );
    if argListMap.ByteEntrySize > 0 then
      lsoMap.LdrByteEntrySize = argListMap.ByteEntrySize;
    end
  end
  
  GP=F and trace("[DEBUG]: <%s:%s> : CTRL Map after Adjust(%s)",
    mod, meth , tostring(lsoMap));

  GP=F and trace("[EXIT]:<%s:%s>:Dir Map after Init(%s)", mod,meth,tostring(lsoMap));
  return lsoMap
end -- adjustLsoMap

-- ======================================================================
-- validateTopRec( topRec, lsoMap )
-- ======================================================================
-- Validate that the top record looks valid:
-- Get the LSO bin from the rec and check for magic
-- Return: "good" or "bad"
-- ======================================================================
local function  validateTopRec( topRec, lsoMap )
  local thisMap = topRec[lsoMap.BinName];
  if thisMap.Magic == "MAGIC" then
    return "good"
  else
    return "bad"
  end
end -- validateTopRec()

-- ======================================================================
-- local function lsoSummary( lsoMap ) (DEBUG/Trace Function)
-- ======================================================================
-- For easier debugging and tracing, we will summarize the lsoMap
-- contents -- without printing out the entire thing -- and return it
-- as a string that can be printed.
-- ======================================================================
local function lsoSummary( lsoMap )
  local resultMap             = map();
  resultMap.SUMMARY           = "LSO Summary String";
  resultMap.BinName           = lsoMap.BinName;
  resultMap.NameSpace         = lsoMap.NameSpace;
  resultMap.Set               = lsoMap.Set;
  resultMap.LdrByteCountMax         = lsoMap.LdrByteCountMax;
  resultMap.HotCacheItemCount      = lsoMap.HotCacheItemCount;
  resultMap.HotCacheMax       = lsoMap.HotCacheMax;
  resultMap.HotCacheTransfer  = lsoMap.HotCacheTransfer;
  resultMap.WarmCacheItemCount     = lsoMap.WarmCacheItemCount;
  resultMap.WarmChunkCount    = lsoMap.WarmChunkCount;
  resultMap.WarmCacheDirMax      = lsoMap.WarmCacheDirMax;
  resultMap.ColdItemCount     = lsoMap.ColdItemCount;
  resultMap.ColdChunkCount    = lsoMap.ColdChunkCount;
  resultMap.ColdDirCount      = lsoMap.ColdDirCount;
  resultMap.ItemCount         = lsoMap.ItemCount;

  return tostring( resultMap );
end -- lsoSummary()

-- ======================================================================
-- Summarize the List (usually ResultList) so that we don't create
-- huge amounts of crap in the console.
-- Show Size, First Element, Last Element
-- ======================================================================
local function summarizeList( myList )
  local resultMap = map();
  resultMap.Summary = "Summary of the List";
  local listSize  = list.size( myList );
  resultMap.ListSize = listSize;
  if resultMap.ListSize == 0 then
    resultMap.FirstElement = "List Is Empty";
    resultMap.LastElement = "List Is Empty";
  else
    resultMap.FirstElement = tostring( myList[1] );
    resultMap.LastElement =  tostring( myList[ listSize ] );
  end

  return tostring( resultMap );
end -- summarizeList()

-- ======================================================================
-- ldrChunkSummary( ldrChunk )
-- ======================================================================
-- Print out interesting stats about this LDR Chunk Record
-- ======================================================================
local function  ldrChunkSummary( ldrChunkRecord ) 
  local resultMap = map();
  local ldrCtrlMap = ldrChunkRecord['LdrControlBin'];
  resultMap.PageMode = ldrCtrlMap.PageMode;
  resultMap.Digest   = ldrCtrlMap.Digest;
  resultMap.ListSize = list.size( ldrChunkRecord['LdrListBin'] );
  resultMap.WarmList = ldrChunkRecord['LdrListBin'];
  resultMap.ByteCountMax   = ldrCtrlMap.ByteCountMax;

  return tostring( resultMap );
end -- ldrChunkSummary()

-- ======================================================================
-- warmCacheListChunkCreate( topRec, lsoMap )
-- ======================================================================
-- Create and initialise a new LDR "chunk", load the new digest for that
-- new chunk into the lsoMap (the warm dir list), and return it.
local function   warmCacheListChunkCreate( topRec, lsoMap )
  local mod = "LsoStoneman";
  local meth = "warmCacheListChunkCreate()";
  GP=F and trace("[ENTER]: <%s:%s> ", mod, meth );

  -- Create the Aerospike Record, initialize the bins: Ctrl, List
  -- Note: All Field Names start with UPPER CASE.
  local newLdrChunkRecord = aerospike:crec_create( topRec );
  local ctrlMap = map();
  ctrlMap.ParentDigest = record.digest( topRec );
  ctrlMap.PageMode = lsoMap.PageMode;
  local newChunkDigest = record.digest( newLdrChunkRecord );
  ctrlMap.Digest = newChunkDigest;
  ctrlMap.ListEntryMax = lsoMap.LdrEntryCountMax; -- Max entries in value list
  ctrlMap.ByteEntrySize = lsoMap.LdrByteEntrySize; -- ByteSize of Fixed Entries
  ctrlMap.ByteEntryCount = 0;  -- A count of Byte Entries
  ctrlMap.ByteCountMax = lsoMap.LdrByteCountMax; -- Max # of bytes in ByteArray
  ctrlMap.DesignVersion = lsoMap.DesignVersion;
  ctrlMap.LogInfo = 0;
  -- Assign Control info and List info to the LDR bins
  newLdrChunkRecord['LdrControlBin'] = ctrlMap;
  newLdrChunkRecord['LdrListBin'] = list();

  GP=F and trace("[DEBUG]: <%s:%s> Chunk Create: CTRL Contents(%s)",
    mod, meth, tostring(ctrlMap) );

  aerospike:crec_update( topRec, newLdrChunkRecord );

  -- Add our new chunk (the digest) to the WarmCacheList
  GP=F and trace("[DEBUG]: <%s:%s> Appending NewChunk(%s) to WarmList(%s)",
    mod, meth, tostring(newChunkDigest), tostring(lsoMap.WarmCacheList));
  list.append( lsoMap.WarmCacheList, newChunkDigest );
  GP=F and trace("[DEBUG]: <%s:%s> Post CHunkAppend:NewChunk(%s): LsoMap(%s)",
    mod, meth, tostring(newChunkDigest), tostring(lsoMap));
   
  -- Increment the Warm Count
  local warmChunkCount = lsoMap.WarmChunkCount;
  lsoMap.WarmChunkCount = (warmChunkCount + 1);

  -- Update the top (LSO) record with the newly updated lsoMap.
  topRec[ lsoMap.BinName ] = lsoMap;

  GP=F and trace("[EXIT]: <%s:%s> Return(%s) ",
    mod, meth, ldrChunkSummary(newLdrChunkRecord));
  return newLdrChunkRecord;
end --  warmCacheListChunkCreate()
-- ======================================================================

-- ======================================================================
-- extractHotCacheTransferList( lsoMap );
-- ======================================================================
-- Extract the oldest N elements (as defined in lsoMap) and create a
-- list that we return.  Also, reset the HotCache to exclude these elements.
-- list.drop( mylist, firstN ).
-- Recall that the oldest element in the list is at index 1, and the
-- newest element is at index N (max).
-- ======================================================================
local function extractHotCacheTransferList( lsoMap )
  local mod = "LsoStoneman";
  local meth = "extractHotCacheTransferList()";
  GP=F and trace("[ENTER]: <%s:%s> ", mod, meth );

  -- Get the first N (transfer amount) list elements
  local transAmount = lsoMap.HotCacheTransfer;
  local oldHotCacheList = lsoMap.HotCacheList;
  local newHotCacheList = list();
  local resultList = list.take( oldHotCacheList, transAmount );

  -- Now that the front "transAmount" elements are gone, move the remaining
  -- elements to the front of the array (OldCacheSize - trans).
  for i = 1, list.size(oldHotCacheList) - transAmount, 1 do 
    list.append( newHotCacheList, oldHotCacheList[i+transAmount] );
  end

  GP=F and trace("[DEBUG]: <%s:%s>OldHotCache(%s) NewHotCache(%s)  ResultList(%s) ",
    mod, meth, tostring(oldHotCacheList), tostring(newHotCacheList),
    tostring(resultList));

  -- Point to the new Cache List and update the Hot Count.
  lsoMap.HotCacheList = newHotCacheList;
  oldHotCacheList = nil;
  lsoMap.HotCacheItemCount = lsoMap.HotCacheItemCount - transAmount;

  GP=F and trace("[EXIT]: <%s:%s> ResultList(%s)", mod, meth, summarizeList(resultList));
  return resultList;
end -- extractHotCacheTransferList()

-- ======================================================================
-- updateWarmCountStatistics( lsoMap, topWarmChunk );
-- ======================================================================
-- After an LDR operation, update page statistics.
-- ======================================================================
local function updateWarmCountStatistics( lsoMap, topWarmChunk ) 
  -- TODO: Not sure if this is needed anymore.  Check and remove.
  return 0;
end


-- ======================================================================
-- ldrChunkInsertList( topWarmChunk, lsoMap, listIndex,  insertList )
-- ======================================================================
-- Insert (append) the LIST of values (overflow from the HotCache) 
-- to this chunk's value list.  We start at the position "listIndex"
-- in "insertList".  Note that this call may be a second (or Nth) call,
-- so we are starting our insert in "insertList" from "listIndex", and
-- not implicitly from "1".
-- Parms:
-- (*) ldrChunkRec: Hotest of the Warm Chunk Records
-- (*) lsoMap: the LSO control information
-- (*) listIndex: Index into <insertList> from where we start copying.
-- (*) insertList: The list of elements to be copied in
-- Return: Number of items written
-- ======================================================================
local function ldrChunkInsertList(ldrChunkRec,lsoMap,listIndex,insertList )
  local mod = "LsoStoneman";
  local meth = "ldrChunkInsertList()";
  GP=F and trace("[ENTER]: <%s:%s> Index(%d) List(%s)",
    mod, meth, listIndex, tostring( insertList ) );

  local ldrCtrlMap = ldrChunkRec['LdrControlBin'];
  local ldrValueList = ldrChunkRec['LdrListBin'];
  local chunkIndexStart = list.size( ldrValueList ) + 1;
  local ldrByteArray = ldrChunkRec['LdrBinaryBin']; -- might be nil

  GP=F and trace("[DEBUG]: <%s:%s> Chunk: CTRL(%s) List(%s)",
    mod, meth, tostring( ldrCtrlMap ), tostring( ldrValueList ));

  -- Note: Since the index of Lua arrays start with 1, that makes our
  -- math for lengths and space off by 1. So, we're often adding or
  -- subtracting 1 to adjust.
  local totalItemsToWrite = list.size( insertList ) + 1 - listIndex;
  local itemSlotsAvailable = (ldrCtrlMap.ListEntryMax - chunkIndexStart) + 1;

  -- In the unfortunate case where our accounting is bad and we accidently
  -- opened up this page -- and there's no room -- then just return ZERO
  -- items written, and hope that the caller can deal with that.
  if itemSlotsAvailable <= 0 then
    warn("[DEBUG]: <%s:%s> INTERNAL ERROR: No space available on chunk(%s)",
    mod, meth, tostring( ldrCtrlMap ));
    return 0; -- nothing written
  end

  -- If we EXACTLY fill up the chunk, then we flag that so the next Warm
  -- List Insert will know in advance to create a new chunk.
  if totalItemsToWrite == itemSlotsAvailable then
    lsoMap.WarmTopFull = 1; -- Now, remember to reset on next update.
    GP=F and trace("[DEBUG]: <%s:%s> TotalItems(%d) == SpaceAvail(%d): Top FULL!!",
      mod, meth, totalItemsToWrite, itemSlotsAvailable );
  end

  GP=F and trace("[DEBUG]: <%s:%s> TotalItems(%d) SpaceAvail(%d)",
    mod, meth, totalItemsToWrite, itemSlotsAvailable );

  -- Write only as much as we have space for
  local newItemsStored = totalItemsToWrite;
  if totalItemsToWrite > itemSlotsAvailable then
    newItemsStored = itemSlotsAvailable;
  end

  -- This is List Mode.  Easy.  Just append to the list.
  GP=F and trace("[DEBUG]: <%s:%s>:ListMode: Copying From(%d) to (%d) Amount(%d)",
    mod, meth, listIndex, chunkIndexStart, newItemsStored );

  -- Special case of starting at ZERO -- since we're adding, not
  -- directly indexing the array at zero (Lua arrays start at 1).
  for i = 0, (newItemsStored - 1), 1 do
    list.append( ldrValueList, insertList[i+listIndex] );
  end -- for each remaining entry

  GP=F and trace("[DEBUG]: <%s:%s>: Post Chunk Copy: Ctrl(%s) List(%s)",
    mod, meth, tostring(ldrCtrlMap), tostring(ldrValueList));

  -- Store our modifications back into the Chunk Record Bins
  ldrChunkRec['LdrControlBin'] = ldrCtrlMap;
  ldrChunkRec['LdrListBin'] = ldrValueList;

  GP=F and trace("[EXIT]: <%s:%s> newItemsStored(%d) List(%s) ",
    mod, meth, newItemsStored, tostring( ldrValueList) );
  return newItemsStored;
end -- ldrChunkInsertList()


-- ======================================================================
-- ldrChunkInsertBytes( topWarmChunk, lsoMap, listIndex,  insertList )
-- ======================================================================
-- Insert (append) the LIST of values (overflow from the HotCache) 
-- to this chunk's Byte Array.  We start at the position "listIndex"
-- in "insertList".  Note that this call may be a second (or Nth) call,
-- so we are starting our insert in "insertList" from "listIndex", and
-- not implicitly from "1".
-- This method is similar to its sibling "ldrChunkInsertList()", but rather
-- than add to the entry list in the chunk's 'LdrListBin', it adds to the
-- byte array in the chunk's 'LdrBinaryBin'.
-- Parms:
-- (*) ldrChunkRec: Hotest of the Warm Chunk Records
-- (*) lsoMap: the LSO control information
-- (*) listIndex: Index into <insertList> from where we start copying.
-- (*) insertList: The list of elements to be copied in
-- Return: Number of items written
-- ======================================================================
local function ldrChunkInsertBytes( ldrChunkRec, lsoMap, listIndex, insertList )
  local mod = "LsoStoneman";
  local meth = "ldrChunkInsertBytes()";
  GP=F and trace("[ENTER]: <%s:%s> Index(%d) List(%s)",
    mod, meth, listIndex, tostring( insertList ) );

  local ldrCtrlMap = ldrChunkRec['LdrControlBin'];
  GP=F and trace("[DEBUG]: <%s:%s> Check LDR CTRL MAP(%s)",
    mod, meth, tostring( ldrCtrlMap ) );

  local entrySize = ldrCtrlMap.ByteEntrySize;
  if( entrySize <= 0 ) then
    warn("[ERROR]: <%s:%s>: Internal Error:. Negative Entry Size", mod, meth);
    return -1; -- General Badness
  end

  local entryCount = 0;
  if( ldrCtrlMap.ByteEntryCount ~= nil and ldrCtrlMap.ByteEntryCount ~= 0 ) then
    entryCount = ldrCtrlMap.ByteEntryCount;
  end
  GP=F and trace("[DEBUG]: <%s:%s> Using EntryCount(%d)", mod, meth, entryCount );

  -- Note: Since the index of Lua arrays start with 1, that makes our
  -- math for lengths and space off by 1. So, we're often adding or
  -- subtracting 1 to adjust.
  -- Calculate how much space we have for items.  We could do this in bytes
  -- or items.  Let's do it in items.
  local totalItemsToWrite = list.size( insertList ) + 1 - listIndex;
  local maxEntries = math.floor(ldrCtrlMap.ByteCountMax / entrySize );
  local itemSlotsAvailable = maxEntries - entryCount;
  GP=F and trace("[DEBUG]: <%s:%s>:MaxEntries(%d) SlotsAvail(%d) #Total ToWrite(%d)",
   mod, meth, maxEntries, itemSlotsAvailable, totalItemsToWrite );

  -- In the unfortunate case where our accounting is bad and we accidently
  -- opened up this page -- and there's no room -- then just return ZERO
  -- items written, and hope that the caller can deal with that.
  if itemSlotsAvailable <= 0 then
    warn("[DEBUG]: <%s:%s> INTERNAL ERROR: No space available on chunk(%s)",
    mod, meth, tostring( ldrCtrlMap ));
    return 0; -- nothing written
  end

  -- If we EXACTLY fill up the chunk, then we flag that so the next Warm
  -- List Insert will know in advance to create a new chunk.
  if totalItemsToWrite == itemSlotsAvailable then
    lsoMap.WarmTopFull = 1; -- Remember to reset on next update.
    GP=F and trace("[DEBUG]: <%s:%s> TotalItems(%d) == SpaceAvail(%d): Top FULL!!",
      mod, meth, totalItemsToWrite, itemSlotsAvailable );
  end

  -- Write only as much as we have space for
  local newItemsStored = totalItemsToWrite;
  if totalItemsToWrite > itemSlotsAvailable then
    newItemsStored = itemSlotsAvailable;
  end

  -- Compute the new space we need in Bytes and either extend existing or
  -- allocate it fresh.
  local totalSpaceNeeded = (entryCount + newItemsStored) * entrySize;
  if ldrChunkRec['LdrBinaryBin'] == nil then
    ldrChunkRec['LdrBinaryBin'] = bytes( totalSpaceNeeded );
    GP=F and trace("[DEBUG]: <%s:%s> Allocated NEW BYTES: Size(%d) ByteArray(%s)",
      mod, meth, totalSpaceNeeded, tostring(ldrChunkRec['LdrBinaryBin']));
  else
    GP=F and trace("[DEBUG]:<%s:%s>Before: Extending BYTES: New Size(%d) ByteArray(%s)",
      mod, meth, totalSpaceNeeded, tostring(ldrChunkRec['LdrBinaryBin']));

    bytes.set_len(ldrChunkRec['LdrBinaryBin'], totalSpaceNeeded );

    GP=F and trace("[DEBUG]:<%s:%s>AFTER: Extending BYTES: New Size(%d) ByteArray(%s)",
      mod, meth, totalSpaceNeeded, tostring(ldrChunkRec['LdrBinaryBin']));
  end
  local chunkByteArray = ldrChunkRec['LdrBinaryBin'];

  -- We're packing bytes into a byte array. Put each one in at a time,
  -- incrementing by "entrySize" for each insert value.
  -- Special case of starting at ZERO -- since we're adding, not
  -- directly indexing the array at zero (Lua arrays start at 1).
  -- Compute where we should start inserting in the Byte Array.
  -- WARNING!!! Unlike a C Buffer, This BYTE BUFFER starts at address 1,
  -- not zero.
  local chunkByteStart = 1 + (entryCount * entrySize);

  GP=F and trace("[DEBUG]: <%s:%s> TotalItems(%d) SpaceAvail(%d) ByteStart(%d)",
    mod, meth, totalItemsToWrite, itemSlotsAvailable, chunkByteStart );

  local byteIndex;
  local insertItem;
  for i = 0, (newItemsStored - 1), 1 do
    byteIndex = chunkByteStart + (i * entrySize);
    insertItem = insertList[i+listIndex];

    GP=F and trace("[DEBUG]:<%s:%s>ByteAppend:Array(%s) Entry(%d) Val(%s) Index(%d)",
      mod, meth, tostring( chunkByteArray), i, tostring( insertItem ),
      byteIndex );

    bytes.put_bytes( chunkByteArray, byteIndex, insertItem );

    GP=F and trace("[DEBUG]: <%s:%s> Post Append: ByteArray(%s)",
      mod, meth, tostring(chunkByteArray));

  end -- for each remaining entry

  -- Update the ctrl map with the new count
  ldrCtrlMap.ByteEntryCount = entryCount + newItemsStored;

  GP=F and trace("[DEBUG]: <%s:%s>: Post Chunk Copy: Ctrl(%s) List(%s)",
    mod, meth, tostring(ldrCtrlMap), tostring( chunkByteArray ));

  -- Store our modifications back into the Chunk Record Bins
  ldrChunkRec['LdrControlBin'] = ldrCtrlMap;
  ldrChunkRec['LdrBinaryBin'] = chunkByteArray;

  GP=F and trace("[EXIT]: <%s:%s> newItemsStored(%d) List(%s) ",
    mod, meth, newItemsStored, tostring( chunkByteArray ));
  return newItemsStored;
end -- ldrChunkInsertBytes()

-- ======================================================================
-- ldrChunkInsert( topWarmChunk, lsoMap, listIndex,  insertList )
-- ======================================================================
-- Insert (append) the LIST of values (overflow from the HotCache) 
-- Call the appropriate method "InsertList()" or "InsertBinary()" to
-- do the storage, based on whether this page is in "List" mode or
-- "Binary" mode.
--
-- Parms:
-- (*) ldrChunkRec: Hotest of the Warm Chunk Records
-- (*) lsoMap: the LSO control information
-- (*) listIndex: Index into <insertList> from where we start copying.
-- (*) insertList: The list of elements to be copied in
-- Return: Number of items written
-- ======================================================================
local function ldrChunkInsert(ldrChunkRec,lsoMap,listIndex,insertList )
  local mod = "LsoStoneman";
  local meth = "ldrChunkInsert()";
  GP=F and trace("[ENTER]: <%s:%s> Index(%d) List(%s)",
    mod, meth, listIndex, tostring( insertList ) );

  if lsoMap.PageMode == "List" then
    return ldrChunkInsertList(ldrChunkRec,lsoMap,listIndex,insertList );
  else
    return ldrChunkInsertBytes(ldrChunkRec,lsoMap,listIndex,insertList );
  end
end -- ldrChunkInsert()

-- ======================================================================
-- ldrHasRoom: Check that there's enough space for an insert in an
-- LSO Data Record.
-- Return: 1=There is room.   0=Not enough room.
-- ======================================================================
-- Parms:
-- (*) ldr: LSO Data Record
-- (*) newValue
-- Return: 1 (ONE) if there's room, otherwise 0 (ZERO)
-- ======================================================================
local function ldrHasRoom( ldr, newValue )
  local mod = "LsoStoneman";
  local meth = "ldrHasRoom()";
  GP=F and trace("[ENTER]: <%s:%s> ldr(%s) newValue(%s) ",
    mod, meth, tostring(ldr), tostring(newValue) );

  local result = 1;  -- Be optimistic 

  -- TODO: ldrHashRoom() This needs to look at SIZES in the case of
  -- BINARY mode.  For LIST MODE, this will work.
  if list.size( ldr.EntryList ) >= ldr.ListEntryMax then
    result = 0;
  end

  GP=F and trace("[EXIT]: <%s:%s> result(%d) ", mod, meth, result );
  return result;
end -- chunkSpaceCheck()


-- ======================================================================
-- readEntryList()
-- ======================================================================
-- This method reads the entry list from Hot, Warm and Cold Caches.
-- It examines each entry, applies the inner UDF function (if applicable)
-- and appends viable candidates to the result list.
-- As always, since we are doing a stack, everything is in LIFO order, 
-- which means we always read back to front.
-- Parms:
--   (*) resultList:
--   (*) entryList:
--   (*) count:
--   (*) func:
--   (*) fargs:
--   (*) all:
-- Return:
--   Implicit: entries are added to the result list
--   Explicit: Number of Elements Read.
-- ======================================================================
local function readEntryList( resultList, entryList, count, func, fargs, all)
  local mod = "LsoStoneman";
  local meth = "readEntryList()";
  GP=F and trace("[ENTER]: <%s:%s> count(%d) resultList(%s) entryList(%s)",
    mod, meth, count, tostring(resultList), tostring(entryList));

  local doTheFunk = 0; -- Welcome to Funky Town

  if (func ~= nil and fargs ~= nil ) then
    doTheFunk = 1;
    GP=F and trace("[ENTER1]: <%s:%s> Count(%d) func(%s) fargs(%s)",
      mod, meth, count, func, tostring(fargs) );
  else
    GP=F and trace("[ENTER2]: <%s:%s> PeekCount(%d)", mod, meth, count );
  end

  -- Get addressability to the Function Table
  local functionTable = require('UdfFunctionTable');

  -- Iterate thru the entryList, gathering up items in the result list.
  -- There are two modes:
  -- (*) ALL Mode: Read the entire list, return all that qualify
  -- (*) Count Mode: Read <count> or <entryListSize>, whichever is smaller
  local numRead = 0;
  local numToRead = 0;
  local listSize = list.size( entryList );
  if all == 1 or count >= listSize then
    numToRead = listSize;
  else
    numToRead = count;
  end

  -- Read back to front (LIFO order), up to "numToRead" entries
  local readValue;
  for i = listSize, 1, -1 do

    -- Apply the UDF to the item, if present, and if result NOT NULL, then
    if doTheFunk == 1 then -- get down, get Funky
      readValue = functionTable[func]( entryList[i], fargs );
    else
      readValue = entryList[i];
    end

    list.append( resultList, readValue );
--    GP=F and trace("[DEBUG]:<%s:%s>Appended Val(%s) to ResultList(%s)",
--      mod, meth, tostring( readValue ), tostring(resultList) );
    
    numRead = numRead + 1;
    if numRead >= numToRead and all == 0 then
      GP=F and trace("[Early EXIT]: <%s:%s> NumRead(%d) resultListSummary(%s)",
        mod, meth, numRead, summarizeList( resultList ));
      return numRead;
    end
  end -- for each entry in the list

  GP=F and trace("[EXIT]: <%s:%s> NumRead(%d) resultListSummary(%s) ",
    mod, meth, numRead, summarizeList( resultList ));
  return numRead;
end -- readEntryList()

-- ======================================================================
-- readByteArray()
-- ======================================================================
-- This method reads the entry list from Warm and Cold List Pages.
-- In each LSO Data Record (LDR), there are three Bins:  A Control Bin,
-- a List Bin (a List() of entries), and a Binary Bin (Compacted Bytes).
-- Similar to its sibling method (readEntryList), readByteArray() pulls a Byte
-- entry from the compact Byte array, applies the (assumed) UDF, and then
-- passes the resulting value back to the caller via the resultList.
--
-- As always, since we are doing a stack, everything is in LIFO order, 
-- which means we always read back to front.
-- Parms:
--   (*) resultList:
--   (*) LDR Chunk Page:
--   (*) count:
--   (*) func:
--   (*) fargs:
--   (*) all:
-- Return:
--   Implicit: entries are added to the result list
--   Explicit: Number of Elements Read.
-- ======================================================================
local function readByteArray( resultList, ldrChunk, count, func, fargs, all)
  local mod = "LsoStoneman";
  local meth = "readByteArray()";
  GP=F and trace("[ENTER]: <%s:%s> Count(%d) ResultList(%s) ",
    mod, meth, count, tostring(resultList) );

  local doTheFunk = 0; -- Welcome to Funky Town

  if (func ~= nil and fargs ~= nil ) then
    doTheFunk = 1;
    GP=F and trace("[ENTER1]: <%s:%s> Count(%d) func(%s) fargs(%s)",
      mod, meth, count, func, tostring(fargs) );
  else
    GP=F and trace("[ENTER2]: <%s:%s> PeekCount(%d)", mod, meth, count );
  end

  -- Get addressability to the Function Table
  local functionTable = require('UdfFunctionTable');

  -- Iterate thru the BYTE structure, gathering up items in the result list.
  -- There are two modes:
  -- (*) ALL Mode: Read the entire list, return all that qualify
  -- (*) Count Mode: Read <count> or <entryListSize>, whichever is smaller
  local ldrCtrlMap = ldrChunk['LdrControlBin'];
  local byteArray = ldrChunk['LdrBinaryBin'];
  local numRead = 0;
  local numToRead = 0;
  local listSize = ldrCtrlMap.ByteEntryCount; -- Number of Entries
  local entrySize = ldrCtrlMap.ByteEntrySize; -- Entry Size in Bytes
  -- When in binary mode, we rely on the LDR page control structure to track
  -- the ENTRY COUNT and the ENTRY SIZE.  Just like walking a list, we
  -- move thru the BYTE value by "EntrySize" amounts.  We will try as much
  -- as possible to treat this as a list, even though we access it directly
  -- as an array.
  --
  if all == 1 or count >= listSize then
    numToRead = listSize;
  else
    numToRead = count;
  end

  -- Read back to front (LIFO order), up to "numToRead" entries
  -- The BINARY information is held in the page's control info
  -- Current Item Count
  -- Current Size (items must be a fixed size)
  -- Max bytes allowed in the ByteBlock.
  -- Example: EntrySize = 10
  -- Address of Entry 1: 0
  -- Address of Entry 2: 10
  -- Address of Entry N: (N - 1) * EntrySize
  -- WARNING!!!  Unlike C Buffers, which start at ZERO, this byte type
  -- starts at ONE!!!!!!
  --
  -- 12345678901234567890 ...  01234567890
  -- +---------+---------+------+---------+
  -- | Entry 1 | Entry 2 | .... | Entry N | 
  -- +---------+---------+------+---------+
  --                            A
  -- To Read:  Start Here ------+
  --           and move BACK towards the front.
  local readValue;
  local byteValue;
  local byteIndex = 0; -- our direct position in the byte array.
  GP=F and trace("[DEBUG]:<%s:%s>Starting loop Byte Array(%s) ListSize(%d)",
      mod, meth, tostring(byteArray), listSize );
  for i = (listSize - 1), 0, -1 do

    byteIndex = 1 + (i * entrySize);
    byteValue = bytes.get_bytes( byteArray, byteIndex, entrySize );

    GP=F and trace("[DEBUG]:<%s:%s>: In Loop: i(%d) BI(%d) BV(%s)",
      mod, meth, i, byteIndex, tostring( byteValue ));

    -- Apply the UDF to the item, if present, and if result NOT NULL, then
    if doTheFunk == 1 then -- get down, get Funky
      readValue = functionTable[func]( byteValue, fargs );
    else
      readValue = byteValue;
    end

    list.append( resultList, readValue );
    GP=F and trace("[DEBUG]:<%s:%s>Appended Val(%s) to ResultList(%s)",
      mod, meth, tostring( readValue ), tostring(resultList) );
    
    numRead = numRead + 1;
    if numRead >= numToRead and all == 0 then
      GP=F and trace("[Early EXIT]: <%s:%s> NumRead(%d) resultList(%s)",
        mod, meth, numRead, tostring( resultList ));
      return numRead;
    end
  end -- for each entry in the list (packed byte array)

  GP=F and trace("[EXIT]: <%s:%s> NumRead(%d) resultListSummary(%s) ",
    mod, meth, numRead, summarizeList( resultList ));
  return numRead;
end -- readByteArray()

-- ======================================================================
-- transferWarmCacheList()
-- ======================================================================
-- Transfer some amount of the WarmCacheList contents (the list of LSO Data
-- Record digests) into the Cold List, which is a linked list of Cold List
-- Directory pages that each point to a list of LDRs.
--
-- There is a configuration parameter (kept in the LSO Control Bin) that 
-- tells us how much of the warm list to migrate to the cold list. That
-- value is set at LSO Create time.
--
-- There is a lot of complexity at this level, as a single Warm List
-- transfer can trigger several operations in the cold list (see the
-- function makeRoomInColdList( lso, digestCount )
-- Parms:
-- (*) lsoMap
-- Return: Success (0) or Failure (-1)
-- ======================================================================
local function transferWarmCacheList( lsoMap )
  local mod = "LsoStoneman";
  local meth = "transferWarmCacheList()";
  local rc = 0;
  GP=F and trace("[ENTER]: <%s:%s> lsoMap(%s)", mod, meth, tostring(lsoMap) );

  -- We are called ONLY when the Warm Dir List is full -- so we should
  -- not have to check that in production, but during development, we're
  -- going to check that the warm list size is >= warm list max, and
  -- return an error if not.  This test can be removed in production.
  local transferAmount = lsoMap.WarmChunkTransfer;


  GP=F and trace("[DEBUG]: <%s:%s> NOT YET READY TO TRANSFER WARM TO COLD: Map(%s)",
    mod, meth, tostring(lsoMap) );

    -- TODO : Finish transferWarmCacheList() ASAP.

  GP=F and trace("[EXIT]: <%s:%s> lsoMap(%s) ", mod, meth, tostring(lsoMap) );
  return rc;
end -- transferWarmCacheList()
  
-- ======================================================================
-- warmCacheListHasRoom( lsoMap )
-- ======================================================================
-- Look at the Warm list and return 1 if there's room, otherwise return 0.
-- Parms:
-- (*) lsoMap: the map for the LSO Bin
-- Return: Decision: 1=Yes, there is room.   0=No, not enough room.
local function warmCacheListHasRoom( lsoMap )
  local mod = "LsoStoneman";
  local meth = "warmCacheListHasRoom()";
  local decision = 1; -- Start Optimistic (most times answer will be YES)
  GP=F and trace("[ENTER]: <%s:%s> LSO BIN(%s) Bin Map(%s)", 
    mod, meth, lsoMap.BinName, tostring( lsoMap ));

--  if lsoMap.WarmChunkCount >= lsoMap.WarmCacheDirMax then
--    decision = 0;
--  end
    -- NOTE:  (TODO: FIXME: Right now we're disabling the Cold Cache,
    -- so for now we claim that there is ALWAYS room in the Warm Cache).
    decision = 1;

  GP=F and trace("[EXIT]: <%s:%s> Decision(%d)", mod, meth, decision );
  return decision;
end -- warmCacheListHasRoom()

-- ======================================================================
-- hotCacheRead( cacheList, peekCount, func, fargs );
-- ======================================================================
-- Parms:
-- Return 'count' items from the Hot Cache
local function hotCacheRead( cacheList, count, func, fargs, all)
  local mod = "LsoStoneman";
  local meth = "hotCacheRead()";
  GP=F and trace("[ENTER]: <%s:%s> Count(%d) ", mod, meth, count );

  local resultList = list();
  local numRead = readEntryList(resultList,cacheList, count, func, fargs, all);

  GP=F and trace("[EXIT]:<%s:%s>resultListSummary(%s)",
    mod, meth, summarizeList(resultList) );
  return resultList;
end -- hotCacheRead()
-- ======================================================================
-- ======================================================================
-- warmCacheListGetTop( topRec, lsoMap )
-- ======================================================================
-- Find the digest of the top of the Warm Dir List, Open that record and
-- return that opened record.
-- ======================================================================
local function warmCacheListGetTop( topRec, lsoMap )
  local mod = "LsoStoneman";
  local meth = "warmCacheListGetTop()";
  GP=F and trace("[ENTER]: <%s:%s> ", mod, meth );

  local warmCacheList = lsoMap.WarmCacheList;
  local stringDigest = tostring( warmCacheList[ list.size(warmCacheList) ]);

  GP=F and trace("[DEBUG]: <%s:%s> Warm Digest(%s) item#(%d)", 
      mod, meth, stringDigest, list.size( warmCacheList ));

  local topWarmChunk = aerospike:crec_open( topRec, stringDigest );

  GP=F and trace("[EXIT]: <%s:%s> result(%s) ",
    mod, meth, ldrChunkSummary( topWarmChunk ) );
  return topWarmChunk;
end -- warmCacheListGetTop()
-- ======================================================================


-- ======================================================================
-- ldrChunkRead( ldrChunk, resultList, count, func, fargs, all );
-- ======================================================================
-- Read ALL, or up to 'count' items from this chunk, process the inner UDF 
-- function (if present) and, for those elements that qualify, add them
-- to the result list.  Read the chunk in FIFO order.
-- Parms:
-- (*) ldrChunk: Record object for the warm or cold LSO Data Record
-- (*) resultList: What's been accumulated so far -- add to this
-- (*) count: Only used when "all" flag is 0.  Return this many items
-- (*) func: Optional Inner UDF function to apply to read items
-- (*) fargs: Function Argument list for inner UDF
-- Return: the NUMBER of items read from this chunk.
-- ======================================================================
local function ldrChunkRead( ldrChunk, resultList, count, func, fargs, all )
-- local function hotCacheRead( cacheList, count, func, fargs, all)
  local mod = "LsoStoneman";
  local meth = "ldrChunkRead()";
  GP=F and trace("[ENTER]: <%s:%s> Count(%d)", mod, meth, count);

  -- If the page is "Binary" mode, then we're using the "Binary" Bin
  -- 'LdrBinaryBin', otherwise we're using the "List" Bin 'LdrListBin'.
  local chunkMap = ldrChunk['LdrControlBin'];
  local numRead = 0;
  if chunkMap.PageMode == "List" then
    local chunkList = ldrChunk['LdrListBin'];
    numRead = readEntryList(resultList,chunkList, count, func, fargs, all);
  else
    numRead = readByteArray(resultList, ldrChunk, count, func, fargs, all);
  end

  GP=F and trace("[EXIT]: <%s:%s> NumberRead(%d) ResultListSummary(%s) ",
    mod, meth, numRead, summarizeList( resultList ));
  return numRead;
end -- ldrChunkRead()
-- ======================================================================


-- ======================================================================
-- warmCacheRead(topRec, resultList, lsoMap, Count, func, fargs, all);
-- ======================================================================
-- Synopsis:
-- Parms:
-- Return: Return the amount read from the Warm Dir List.
-- ======================================================================
local function warmCacheRead(topRec, resultList, lsoMap, count,
                           func, fargs, all)
  local mod = "LsoStoneman";
  local meth = "warmCacheRead()";
  GP=F and trace("[ENTER]: <%s:%s> Count(%d) ", mod, meth, count );

  -- Process the WarmCacheList bottom to top, pulling in each digest in
  -- turn, opening the chunk and reading records (as necessary), until
  -- we've read "count" items.  If the 'all' flag is 1, then read 
  -- everything.
  local warmCacheList = lsoMap.WarmCacheList;

  -- If we're using the "all" flag, then count just doesn't work.  Try to
  -- ignore counts entirely when the ALL flag is set.
  if all == 1 or count < 0 then count = 0; end
  local remaining = count;
  local totalWarmAmountRead = 0;
  local chunkItemsRead = 0;
  local dirCount = list.size( warmCacheList );
  local ldrChunk;
  local stringDigest;
  local status = 0;

  GP=F and trace("[DEBUG]:<%s:%s>:DirCount(%d),Top(%s) Reading warmCacheList(%s)",
    mod, meth, dirCount, validateTopRec( topRec, lsoMap ),
    tostring( warmCacheList) );

  -- Read each Warm Chunk, adding to the resultList, until we either bypass
  -- the readCount, or we hit the end (either readCount is large, or the ALL
  -- flag is set).
  for dirIndex = dirCount, 1, -1 do
    -- Record Digest MUST be in string form
    stringDigest = tostring(warmCacheList[ dirIndex ]);
    GP=F and trace("[DEBUG]: <%s:%s>: Opening Warm Chunk:Index(%d)Digest(%s):",
    mod, meth, dirIndex, stringDigest );
    ldrChunk = aerospike:crec_open( topRec, stringDigest );
    --
    -- resultList is passed by reference and we can just add to it.
    chunkItemsRead =
    ldrChunkRead( ldrChunk, resultList, remaining, func, fargs, all );
    totalWarmAmountRead = totalWarmAmountRead + chunkItemsRead;

    GP=F and trace("[DEBUG]:<%s:%s>:after ChunkRead:NumRead(%d)DirIndex(%d)ResList(%s)", 
      mod, meth, chunkItemsRead, dirIndex, tostring( resultList ));
    -- Early exit ONLY when ALL flag is not set.
    if( all == 0 and
      ( chunkItemsRead >= remaining or totalWarmAmountRead >= count ) )
    then
      GP=F and trace("[Early EXIT]: <%s:%s> totalWarmAmountRead(%d) ResultList(%s) ",
        mod, meth, totalWarmAmountRead, tostring(resultList));
      status = aerospike:crec_close( topRec, ldrChunk );
      return totalWarmAmountRead;
    end

    status = aerospike:crec_close( topRec, ldrChunk );
    GP=F and trace("[DEBUG]: <%s:%s> as:close() status(%s) ",
    mod, meth, tostring( status ) );

    -- Get ready for the next iteration.  Adjust our numbers for the
    -- next round
    remaining = remaining - chunkItemsRead;
  end -- for each warm Chunk

  GP=F and trace("[EXIT]: <%s:%s> totalWarmAmountRead(%d) ResultListSummary(%s) ",
  mod, meth, totalWarmAmountRead, summarizeList(resultList));
  return totalWarmAmountRead;
end -- warmCacheRead()

-- ======================================================================
-- warmCacheInsert()
-- ======================================================================
-- Insert "insertList", which is a list of data entries, into the warm
-- dir list -- a directory of warm Lso Data Records that will contain 
-- the data entries.
-- Parms:
-- (*) topRec: the top record -- needed if we create a new LDR
-- (*) lsoMap: the control map of the top record
-- (*) insertList: the list of entries to be inserted (as_val or binary)
-- Return: 0 for success, -1 if problems.
-- ======================================================================
local function warmCacheInsert( topRec, lsoMap, insertList )
  local mod = "LsoStoneman";
  local meth = "warmCacheInsert()";
  local rc = 0;
  GP=F and trace("[ENTER]: <%s:%s> ", mod, meth );
--GP=F and trace("[ENTER]: <%s:%s> LSO Summary(%s) ", mod, meth, lsoSummary(lsoMap) );

  GP=F and trace("[DEBUG 0]:WDL(%s)", tostring( lsoMap.WarmCacheList ));

  local warmCacheList = lsoMap.WarmCacheList;
  local topWarmChunk;
  -- Whether we create a new one or open an existing one, we save the current
  -- count and close the record.
  -- Note that the last write may have filled up the warmTopChunk, in which
  -- case it set a flag so that we will go ahead and allocate a new one now,
  -- rather than after we read the old top and see that it's already full.
  if list.size( warmCacheList ) == 0 or lsoMap.WarmTopFull == 1 then
    GP=F and trace("[DEBUG]: <%s:%s> Calling Chunk Create ", mod, meth );
    topWarmChunk = warmCacheListChunkCreate( topRec, lsoMap ); -- create new
    lsoMap.WarmTopFull = 0; -- reset for next time.
  else
    GP=F and trace("[DEBUG]: <%s:%s> Calling Get TOP ", mod, meth );
    topWarmChunk = warmCacheListGetTop( topRec, lsoMap ); -- open existing
  end
  GP=F and trace("[DEBUG]: <%s:%s> Post 'GetTop': LsoMap(%s) ", 
    mod, meth, tostring( lsoMap ));

  -- We have a warm Chunk -- write as much as we can into it.  If it didn't
  -- all fit -- then we allocate a new chunk and write the rest.
  local totalCount = list.size( insertList );
  GP=F and trace("[DEBUG]: <%s:%s> Calling Chunk Insert: List(%s)",
    mod, meth, tostring( insertList ));
  local countWritten = ldrChunkInsert( topWarmChunk, lsoMap, 1, insertList );
  if( countWritten == -1 ) then
    warn("[ERROR]: <%s:%s>: Internal Error in Chunk Insert", mod, meth);
    return -1;  -- General badness
  end
  local itemsLeft = totalCount - countWritten;
  if itemsLeft > 0 then
    aerospike:crec_update( topRec, topWarmChunk );
    aerospike:crec_close( topRec, topWarmChunk );
    GP=F and trace("[DEBUG]: <%s:%s> Calling Chunk Create: AGAIN!!", mod, meth );
    topWarmChunk = warmCacheListChunkCreate( topRec, lsoMap ); -- create new
    -- Unless we've screwed up our parameters -- we should never have to do
    -- this more than once.  This could be a while loop if it had to be, but
    -- that doesn't make sense that we'd need to create multiple new LDRs to
    -- hold just PART of the hot cache.
  GP=F and trace("[DEBUG]: <%s:%s> Calling Chunk Insert: List(%s) AGAIN(%d)",
    mod, meth, tostring( insertList ), countWritten + 1);
    countWritten = ldrChunkInsert( topWarmChunk, lsoMap, countWritten+1, insertList );
    if( countWritten == -1 ) then
      warn("[ERROR]: <%s:%s>: Internal Error in Chunk Insert", mod, meth);
      return -1;  -- General badness
    end
    if countWritten ~= itemsLeft then
      warn("[ERROR!!]: <%s:%s> Second Warm Chunk Write: CW(%d) IL(%d) ",
        mod, meth, countWritten, itemsLeft );
    end
  end

  -- Update the Warm Count
  if lsoMap.WarmCacheItemCount == nil then
    lsoMap.WarmCacheItemCount = 0;
  end
  local currentWarmCount = lsoMap.WarmCacheItemCount;
  lsoMap.WarmCacheItemCount = (currentWarmCount + totalCount);

  -- All done -- Save the info of how much room we have in the top Warm
  -- chunk (entry count or byte count)
  GP=F and trace("[DEBUG]: <%s:%s> Saving LsoMap (%s) Before Update ",
    mod, meth, tostring( lsoMap ));
  topRec[lsoMap.BinName] = lsoMap;
  updateWarmCountStatistics( lsoMap, topWarmChunk );

  GP=F and trace("[DEBUG]: <%s:%s> Chunk Summary before storage(%s)",
    mod, meth, ldrChunkSummary( topWarmChunk ));

  GP=F and trace("[DEBUG]: <%s:%s> Calling CREC Update ", mod, meth );
  local status = aerospike:crec_update( topRec, topWarmChunk );
  GP=F and trace("[DEBUG]: <%s:%s> CREC Update Status(%s) ",mod,meth, tostring(status));
  GP=F and trace("[DEBUG]: <%s:%s> Calling CREC Close ", mod, meth );

  status = aerospike:crec_close( topRec, topWarmChunk );
  GP=F and trace("[DEBUG]: <%s:%s> CREC Close Status(%s) ",mod,meth, tostring(status));

  -- Update the total Item Count in the topRec.  The caller will 
  -- "re-store" the map in the record before updating.
  local itemCount = lsoMap.ItemCount + list.size( insertList );
  lsoMap.ItemCount = itemCount;

  return rc;
end -- warmCacheInsert

-- ======================================================================
-- local function hotCacheTransfer( lsoMap, insertValue )
-- ======================================================================
-- The job of hotCacheTransfer() is to move part of the HotCache, as
-- specified by HotCacheTransferAmount, to LDRs in the warm Dir List.
-- Here's the logic:
-- (1) If there's room in the WarmCacheList, then do the transfer there.
-- (2) If there's insufficient room in the WarmDir List, then make room
--     by transferring some stuff from Warm to Cold, then insert into warm.
local function hotCacheTransfer( topRec, lsoMap )
  local mod = "LsoStoneman";
  local meth = "hotCacheTransfer()";
  local rc = 0;
  GP=F and trace("[ENTER]: <%s:%s> LSO Summary() ", mod, meth );
--GP=F and trace("[ENTER]: <%s:%s> LSO Summary(%s) ", mod, meth, lsoSummary(lsoMap) );

  -- if no room in the WarmList, then make room (transfer some of the warm
  -- list to the cold list)
  if warmCacheListHasRoom( lsoMap ) == 0 then
    transferWarmCacheList( lsoMap );
  end

  -- TODO: hotCacheTransfer(): Make this more efficient
  -- Assume "LIST MODE" for now for the Warm Dir digests.
  -- Later, we can pack the digests into the BinaryBin if that is appropriate.
  -- Transfer N items from the hotCache 
  local digestList = list();

  -- Do this the simple (more expensive) way for now:  Build a list of
  -- the items we're moving from the hot cache to the warm dir, then
  -- call insertWarmDir() to find a place for it.
  local transferList = extractHotCacheTransferList( lsoMap );
  rc = warmCacheInsert( topRec, lsoMap, transferList );

  GP=F and trace("[EXIT]: <%s:%s> result(%d) ", mod, meth, rc );
  return rc;
end -- hotCacheTransfer()
-- ======================================================================

-- ======================================================================
-- coldPeek( cacheList, peekCount, func, fargs );
-- ======================================================================
-- Return 'count' items from the Cold Dir List -- adding to the result
-- list passed in.
-- ======================================================================
local function coldCacheRead(topRec, resultList, cacheList, count, func, fargs )
  local mod = "LsoStoneman";
  local meth = "coldCacheRead()";
  GP=F and trace("[ENTER]: <%s:%s> ", mod, meth );

  GP=F and trace("[DEBUG]: <%s:%s> COLD STORAGE NOT YET IMPLEMENTED!! ", mod, meth );

  GP=F and trace("[EXIT]: <%s:%s> ", mod, meth );
  return resultList;
end -- coldCacheRead()

-- ======================================================================
-- lsoMapRead(): Get "peekCount" items from the LSO structure.
-- ======================================================================
-- Read each part of the LSO Map: Hot Cache, Warm Cache, Cold Cache
-- Parms:
-- (*) lsoMap: The main LSO structure (stored in the LSO Bin)
-- (*) peekCount: The total count to read (0 means all)
-- (*) Optional inner UDF Function (from the UdfFunctionTable)
-- (*) fargs: Function arguments (list) fed to the inner UDF
-- Return: The Peek resultList -- in LIFO order
-- ======================================================================
local function lsoMapRead( topRec, lsoMap, peekCount, func, fargs )
  local mod = "LsoStoneman";
  local meth = "lsoMapRead()";
  GP=F and trace("[ENTER]: <%s:%s> ReadCount(%s)", mod, meth, tostring(peekCount));

  if (func ~= nil and fargs ~= nil ) then
    GP=F and trace("[ENTER1]: <%s:%s> Count(%s) func(%s) fargs(%s)",
      mod, meth, tostring(peekCount), tostring(func), tostring(fargs) );
  else
    GP=F and trace("[ENTER2]: <%s:%s> PeekCount(%s)", mod, meth, tostring(peekCount));
  end

  local all = 0;
  if peekCount == 0 then all = 1; end

  -- Fetch from the Cache, then the Warm List, then the Cold List.
  -- Each time we decrement the count and add to the resultlist.
  local cacheList = lsoMap.HotCacheList;
  local resultList = hotCacheRead( cacheList, peekCount, func, fargs, all);
  local numRead = list.size( resultList );
  GP=F and trace("[DEBUG]: <%s:%s> HotListResult(%s)", mod, meth,tostring(resultList));

  local warmCount = 0;
  local warmList;

  -- If the cache had all that we need, then done.  Return list.
  if( numRead >= peekCount and all == 0) then
    return resultList;
  end

  -- We need more -- get more out of the Warm List.  If ALL flag is set,
  -- keep going until we're done.  Otherwise, compute the correct READ count
  -- given that we've already read from the Hot List.
  local remainingCount = 0; -- Default, when ALL flag is on.
  if( all == 0 ) then
    remainingCount = peekCount - numRead;
  end
  GP=F and trace("[DEBUG]: <%s:%s> Checking WarmList Count(%d) All(%d)",
    mod, meth, remainingCount, all);
  -- If no Warm List, then we're done (assume no cold list if no warm)
  if list.size(lsoMap.WarmCacheList) > 0 then
    warmCount =
      warmCacheRead(topRec, resultList, lsoMap, remainingCount, func, fargs, all);
  end

  return resultList; -- No Cold List processing for now. This is everything.

  --  numRead = numRead + warmCount;
  --  remainingCount = peekCount - numRead;
  --  if((all == 1 or remainingCount > 0 ) and ( lsoMap.ColdListHead  ~= 0 )) then
  --    local coldList =
  --      coldPeek(topRecw, armList, lsoMap, remainingCount, func, fargs, all);
  --    return coldList; -- this includes all read so far
  --  else
  --    return warmList; -- something weird happened here.
  --  end -- if cold data

end -- function lsoMapRead

-- ======================================================================
-- valueStorage()
-- ======================================================================
-- This is function is more structure than function -- but we need a way
-- to create a TRANSFORMED VALUE -- and then mark it with the instructions
-- for dealing with it.  Therefore, the WRITE TRANSFORM Inner UDFs 
-- will actually return a VALUE STORAGE Map, which contains the value and
-- other control information that will help us figure out how to deal with
-- it.  Binary Data will LIKELY need to be packed (compressed form, in the
-- BINARY BIN), but not necessarily.
local function valueStorage( valueType, luaValue )
  local valueMap = map();
  valueMap.ValueType = valueType;
  valueMap.Value = luaValue; -- this can be nil
  valueMap.StorageMode = 1; -- 0 is compact, 1 is normal Lua
  valueMap.size = 0; -- if compact, this is how many bytes to copy

  -- Add More things here as we figure out what we want.

  return valueMap;
end -- valueStorage()
-- ======================================================================


-- ======================================================================
-- hotCacheHasRoom( lsoMap, insertValue )
-- ======================================================================
-- return 1 if there's room, otherwise return 0
-- Later on we might consider just doing the insert here when there's room.
-- Parms:
-- (*) lsoMap: the map for the LSO Bin
-- (*) insertValue: the new value to be pushed on the stack
local function hotCacheHasRoom( lsoMap, insertValue )
  local mod = "LsoStoneman";
  local meth = "hotCacheHasRoom()";
  GP=F and trace("[ENTER]: <%s:%s> : ", mod, meth );
  local result = 1;  -- This is the usual case

  local cacheLimit = lsoMap.HotCacheMax;
  local cacheList = lsoMap.HotCacheList;
  if list.size( cacheList ) >= cacheLimit then
    return 0
  end

  GP=F and trace("[EXIT]: <%s:%s> Result(%d) : ", mod, meth, result);
  return result;
end -- hotCacheHasRoom()
-- ======================================================================

--
-- ======================================================================
-- hotCacheInsert( lsoMap, newStorageValue  )
-- ======================================================================
-- Insert a value at the end of the Top Cache List.  The caller has 
-- already verified that space exists, so we can blindly do the insert.
--
-- The MODE of storage depends on what we see in the valueMap.  If the
-- valueMap holds a BINARY type, then we are going to store it in a special
-- binary bin.  Here are the cases:
-- (1) Cache: Special binary bin in the user record (Details TBD)
-- (2) Warm List: The Chunk Record employs a List Bin and Binary Bin, where
--    the individual entries are packed.  In the Chunk Record, there is a
--    Map (control information) showing the status of the packed Binary bin.
-- (3) Cold List: Same Chunk format as the Warm List Chunk Record.
--
-- Change in plan -- All items go on the HotList, regardless of type.
-- Only when we transfer to Warm/Cold do we employ the COMPACT STORAGE
-- trick of packing bytes contiguously in the Binary Bin.
--
-- The Top LSO page (and the individual LDR chunk pages) have the control
-- data about the byte entries (entry size, entry count).
-- Parms:
-- (*) lsoMap: the map for the LSO Bin
-- (*) newStorageValue: the new value to be pushed on the stack
local function hotCacheInsert( lsoMap, newStorageValue  )
  local mod = "LsoStoneman";
  local meth = "hotCacheInsert()";
  GP=F and trace("[ENTER]: <%s:%s> : Insert Value(%s)",
    mod, meth, tostring(newStorageValue) );

  local hotCacheList = lsoMap.HotCacheList;
  list.append( hotCacheList, newStorageValue.Value );
  local itemCount = lsoMap.ItemCount;
  lsoMap.ItemCount = (itemCount + 1);
  local hotCount = lsoMap.HotCacheItemCount;
  lsoMap.HotCacheItemCount = (hotCount + 1);
  return 0;  -- all is well

end -- hotCacheInsert()
-- ======================================================================
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- LSO Main Functions
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
--
-- ======================================================================
-- || stackCreate (Stoneman)    ||
-- ======================================================================
-- Create/Initialize a Stack structure in a bin, using a single LSO
-- bin, using User's name, but Aerospike TYPE (AS_LSO)
--
-- For this version (Stoneman), we will be using a SINGLE MAP object,
-- which contains lots of metadata, plus one list:
-- (*) Namespace Name (just one Namespace -- for now)
-- (*) Set Name
-- (*) Chunk Size (same for both namespaces)
-- (*) Warm Chunk Count: Number of Warm Chunk Data Records
-- (*) Cold Chunk Count: Number of Cold Chunk Data Records
-- (*) Item Count (will NOT be tracked in Stoneman)
-- (*) The List of Warm Chunks of data (each Chunk is a list)
-- (*) The Head of the Cold Data Directory
-- (*) Storage Mode (Compact or Regular) (0 for compact, 1 for regular)
-- (*) Compact Item List
--
-- The LSO starts out in "Compact" mode, which allows the first 100 (or so)
-- entries to be held directly in the record -- in the Hot Cache.  Once the
-- Hot Cache overflows, the entries flow into the warm list, which is a
-- list of LSO Data Records (each 2k record holds N values, where N is
-- approximately (2k/rec size) ).
-- Once the data overflows the warm list, it flows into the cold list,
-- which is a linked list of directory pages -- where each directory page
-- points to a list of LSO Data Record pages.  Each directory page holds
-- roughly 100 page pointers (assuming a 2k page).
-- Parms (inside argList)
-- (1) topRec: the user-level record holding the LSO Bin
-- (2) argList: the list of create parameters
--  (2.1) LsoBinName
--  (2.2) Namespace (just one, for now)
--  (2.3) Set
--  (2.4) LdrByteCountMax
--  (2.5) Design Version
--
--  !!!! More parms needed here to appropriately configure the LSO
--  -> Hot Cache Size
--  -> Hot Cache Transfer amount
--  -> Warm List Size
--  -> Warm List Transfer amount
--
function stackCreate( topRec, lsoBinName, argList )
  local mod = "LsoStoneman";
  local meth = "stackCreate()";

  if argList == nil then
    GP=F and trace("[ENTER1]: <%s:%s> lsoBinName(%s) NULL argList",
      mod, meth, tostring(lsoBinName));
  else
    GP=F and trace("[ENTER2]: <%s:%s> lsoBinName(%s) argList(%s) ",
    mod, meth, tostring( lsoBinName), tostring( argList ));
  end

  -- Some simple protection if things are weird
  if lsoBinName == nil  or type(lsoBinName) ~= "string" then
    warn("[WARNING]: <%s:%s> Bad LSO BIN Name: Using default", mod, meth );
    lsoBinName = "LsoBin";
  end

  -- Check to see if LSO Structure (or anything) is already there,
  -- and if so, error
  if topRec[lsoBinName] ~= nil  then
    warn("[ERROR EXIT]: <%s:%s> LSO BIN(%s) Already Exists",
      mod, meth, tostring(lsoBinName) );
    return('LSO_BIN already exists');
  end

  -- Create and initialize the LSO MAP -- the main LSO structure
  -- initializeLsoMap() also assigns the map to the record bin.
  local lsoMap = initializeLsoMap( topRec, lsoBinName );

  -- If the user has passed in settings that override the defaults
  -- (the argList), then process that now.
  if argList ~= nil then
    adjustLsoMap( lsoMap, argList )
  end

  GP=F and trace("[DEBUG]:<%s:%s>:Dir Map after Init(%s)", mod,meth,tostring(lsoMap));

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
end -- function stackCreate( topRec, namespace, set )

-- ======================================================================
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- || local stackPush (Stoneman V2)
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- This function does the work of both calls -- with and without inner UDF.
--
-- Push a value onto the stack. There are different cases, with different
-- levels of complexity:
-- (*) HotCacheInsert: Instant: Easy
-- (*) WarmCacheInsert: Result of HotCache Overflow:  Medium
-- (*) ColdCacheInsert: Result of WarmCache Overflow:  Complex
-- Parms:
-- (*) topRec:
-- (*) lsoBinName:
-- (*) newValue:
-- (*) func:
-- (*) fargs:
-- =======================================================================
local function localStackPush( topRec, lsoBinName, newValue, func, fargs )
  local mod = "LsoStoneman";
  local meth = "localStackPush()";

  local doTheFunk = 0; -- when == 1, call the func(fargs) on the Push item
  local functionTable = require('UdfFunctionTable');

  if (func ~= nil and fargs ~= nil ) then
    doTheFunk = 1;
    GP=F and trace("[ENTER1]: <%s:%s> LSO BIN(%s) NewValue(%s) func(%s) fargs(%s)",
      mod, meth, tostring(lsoBinName), tostring( newValue ),
      tostring(func), tostring(fargs) );
  else
    GP=F and trace("[ENTER2]: <%s:%s> LSO BIN(%s) NewValue(%s)",
      mod, meth, tostring(lsoBinName), tostring( newValue ));
  end

  -- Some simple protection if things are weird
  if lsoBinName == nil  or type(lsoBinName) ~= "string" then
    warn("[WARNING]: <%s:%s> Bad LSO BIN Name: Using default", mod, meth );
    lsoBinName = "LsoBin";
  end

  local lsoMap;
  if( not aerospike:exists( topRec ) ) then
    GP=F and trace("[WARNING]:<%s:%s>:Record Does Not exist. Creating", mod, meth );
    lsoMap = initializeLsoMap( topRec, lsoBinName );
    aerospike:create( topRec );
  elseif ( topRec[lsoBinName] == nil ) then
    GP=F and trace("[WARNING]: <%s:%s> LSO BIN (%s) DOES NOT Exist: Creating",
                   mod, meth, tostring(lsoBinName) );
    lsoMap = initializeLsoMap( topRec, lsoBinName );
    aerospike:create( topRec );
  end
  
  -- check that our bin is (relatively intact
  local lsoMap = topRec[lsoBinName]; -- The main LSO map
  if lsoMap.Magic ~= "MAGIC" then
    warn("[ERROR EXIT]: <%s:%s> LSO_BIN (%s) Is Corrupted (no magic)",
      mod, meth, lsoBinName );
    return('LSO_BIN Is Corrupted');
  end

  -- Now, it looks like we're ready to insert.  If there is an inner UDF
  -- to apply, do it now.
  local newStorageValue;
  if doTheFunk == 1 then 
    GP=F and trace("[DEBUG]: <%s:%s> Applying UDF (%s) with args(%s)",
      mod, meth, tostring(func), tostring( fargs ));
    newValue = functionTable[func]( newValue, fargs );
  end

  newStorageValue = valueStorage( type(newValue), newValue );
  GP=F and trace("[DEBUG]: <%s:%s> AFTER UDF (%s) with ValueStorage(%s)",
      mod, meth, tostring(func), tostring( newStorageValue ));

  -- If we have room, do the simple cache insert.  If we don't have
  -- room, then make room -- transfer half the cache out to the warm list.
  -- That may, in turn, have to make room by moving some items to the
  -- cold list.
  if hotCacheHasRoom( lsoMap, newStorageValue ) == 0 then
    GP=F and trace("[DEBUG]:<%s:%s>:>>> CALLING TRANSFER HOT CACHE!!<<<", mod, meth );
    hotCacheTransfer( topRec, lsoMap );
  end
  hotCacheInsert( lsoMap, newStorageValue );
  -- Must always assign the object BACK into the record bin.
  topRec[lsoBinName] = lsoMap;

  -- All done, store the topRec
  local rc = -99; -- Use Odd starting Num: so that we know it got changed
  GP=F and trace("[DEBUG]:<%s:%s>:Update Record", mod, meth );
  rc = aerospike:update( topRec );

  GP=F and trace("[EXIT]: <%s:%s> : Done.  RC(%d)", mod, meth, rc );
  return rc
end -- function localStackPush()

-- =======================================================================
-- Stack Push -- with and without inner UDFs
-- These are the globally visible calls -- that call the local UDF to do
-- all of the work.
-- NOTE: All parameters must be protected with "tostring()" so that we
-- do not encounter a format error if the user passes in nil or any
-- other incorrect value/type
-- =======================================================================
function stackPush( topRec, lsoBinName, newValue )
  return localStackPush( topRec, lsoBinName, newValue, nil, nil )
end -- end stackPush()

function stackPushWithUDF( topRec, lsoBinName, newValue, func, fargs )
  return localStackPush( topRec, lsoBinName, newValue, func, fargs );
end -- stackPushWithUDF()

-- ======================================================================
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- || Local StackPeek: Stoneman V2 (the new and improved structure)
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- Return "peekCount" values from the stack, in Stack (LIFO) order.
-- For Each Bin (in LIFO Order), read each Bin in reverse append order.
-- If "peekCount" is zero, then return all.
-- Depending on "peekcount", we may find the elements in:
-- -> Just the HotCache
-- -> The HotCache and the Warm List
-- -> The HotCache, Warm list and Cold list
-- Since our pieces are basically in Stack order, we start at the top
-- (the HotCache), then the WarmList, then the Cold List.  We just
-- keep going until we've seen "PeekCount" entries.  The only trick is that
-- we have to read our blocks backwards.  Our blocks/lists are in stack 
-- order, but the data inside the blocks are in append order.
--
-- Parms:
-- (*) topRec:
-- (*) lsoBinName:
-- (*) peekCount:
-- (*) func:
-- (*) fargs:
-- ======================================================================
local function localStackPeek( topRec, lsoBinName, peekCount, func, fargs )
  local mod = "LsoStoneman";
  local meth = "localStackPeek()";
  GP=F and trace("[ENTER]: <%s:%s> PeekCount(%s) ", mod, meth,tostring(peekCount) );

  if (func ~= nil and fargs ~= nil ) then
    GP=F and trace("[ENTER1]: <%s:%s> LSO BIN(%s) PeekCount(%s) func(%s) fargs(%s)",
      mod, meth, tostring(lsoBinName), tostring(peekCount),
      tostring(func), tostring(fargs) );
  else
    GP=F and trace("[ENTER2]: <%s:%s> LSO BIN(%s) PeekCount(%s)", 
      mod, meth, tostring(lsoBinName), tostring(peekCount) );
  end

  if( not aerospike:exists( topRec ) ) then
    warn("[ERROR EXIT]:<%s:%s>:Missing Record. Exit", mod, meth );
    return('Base Record Does NOT exist');
  end

  -- Verify that the LSO Structure is there: otherwise, error.
  if lsoBinName == nil  or type(lsoBinName) ~= "string" then
    warn("[ERROR EXIT]: <%s:%s> Bad LSO BIN Parameter", mod, meth );
    return('Bad LSO Bin Parameter');
  end
  if( topRec[lsoBinName] == nil ) then
    warn("[ERROR EXIT]: <%s:%s> LSO_BIN (%s) DOES NOT Exists",
      mod, meth, tostring(lsoBinName) );
    return('LSO_BIN Does NOT exist');
  end
  
  -- check that our bin is (mostly) there
  local lsoMap = topRec[lsoBinName]; -- The main LSO map
  if lsoMap.Magic ~= "MAGIC" then
    GP=F and trace("[ERROR EXIT]: <%s:%s> LSO_BIN (%s) Is Corrupted (no magic)",
      mod, meth, lsoBinName );
    return('LSO_BIN Is Corrupted');
  end

  -- Build the user's "resultList" from the items we find that qualify.
  -- They must pass the "transformFunction()" filter.
  -- Also, Notice that we go in reverse order -- to get the "stack function",
  -- which is Last In, First Out.
  GP=F and trace("[DEBUG]: <%s:%s>: Calling Map Peek", mod, meth );
  local resultList = lsoMapRead( topRec, lsoMap, peekCount, func, fargs );

  GP=F and trace("[EXIT]: <%s:%s>: PeekCount(%d) ResultListSummary(%s)",
    mod, meth, peekCount, summarizeList(resultList));

  return resultList;
end -- function localStackPeek() 

-- =======================================================================
-- Stack Peek -- with and without inner UDFs
-- These are the globally visible calls -- that call the local UDF to do
-- all of the work.
-- NOTE: All parameters must be protected with "tostring()" so that we
-- do not encounter a format error if the user passes in nil or any
-- other incorrect value/type.
-- =======================================================================
function stackPeek( topRec, lsoBinName, peekCount )
  local mod = "LsoStoneman";
  local meth = "stackPeek()";
  GP=F and trace("[ENTER]: <%s:%s> LSO BIN(%s) peekCount(%s)",
    mod, meth, tostring(lsoBinName), tostring(peekCount) )
  return localStackPeek( topRec, lsoBinName, peekCount, nil, nil )
end -- end stackPeek()

function stackPeekWithUDF( topRec, lsoBinName, peekCount, func, fargs )
  local mod = "LsoStoneman";
  local meth = "stackPeekWithUDF()";
  GP=F and
  trace("[ENTER]: <%s:%s> LSO BIN(%s) peekCount(%s) func(%s) fargs(%s)",
    mod, meth, tostring(lsoBinName), tostring(peekCount),
    tostring(func), tostring(fargs));
  return localStackPeek( topRec, lsoBinName, peekCount, func, fargs );
end -- stackPeekWithUDF()

-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
