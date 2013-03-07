-- ======================================================================
-- Large Stack Object (LSO) Operations
-- LsoStoneman V1.1 -- (March 5, 2013)
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

-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- LSO Utility Functions
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- These are all local functions to this module and serve various
-- utility and assistance functions.

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
  resultMap.LdrByteMax         = lsoMap.LdrByteMax;
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
-- ldrChunkSummary( ldrChunk )
-- ======================================================================
-- Print out interesting stats about this LDR Chunk Record
-- ======================================================================
local function  ldrChunkSummary( ldrChunkRecord ) 
  local resultMap = map();
  local lsoMap = ldrChunkRecord['LdrControlBin'];
  resultMap.PageType = lsoMap.PageType;
  resultMap.PageMode = lsoMap.PageMode;
  resultMap.Digest   = lsoMap.Digest;
  resultMap.ListSize = list.size( ldrChunkRecord['LdrListBin'] );
  resultMap.WarmList = ldrChunkRecord['LdrListBin'];

  return tostring( resultMap );
end -- ldrChunkSummary()

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
  info("[ENTER]: <%s:%s> \n", mod, meth );

  -- Get the first N (transfer amount) list elements
  local transAmount = lsoMap.HotCacheTransfer;
  local oldHotCacheList = lsoMap.HotCacheList;
  local newHotCacheList = list();
  local resultList = list.take( oldHotCacheList, transAmount );

  -- Move the top elements (Max - N) up to the top of the HotCache List.
  for i = 1, transAmount, 1 do 
    -- newHotCacheList[i] = oldHotCacheList[i+transAmount];
    list.append( newHotCacheList, oldHotCacheList[i+transAmount] );
  end

  info("[DEBUG]: <%s:%s>OldHotCache(%s) NewHotCache(%s)  ResultList(%s) \n",
    mod, meth, tostring(oldHotCacheList), tostring(newHotCacheList),
    tostring(resultList));

  -- Point to the new Cache List and update the Hot Count.
  lsoMap.HotCacheList = newHotCacheList;
  oldHotCacheList = nil;
  lsoMap.HotItemCount = lsoMap.HotItemCount - transAmount;

  info("[EXIT]: <%s:%s> ResultList(%s) \n", mod, meth, tostring(resultList));
  return resultList;
end -- extractHotCacheTransferList()

-- ======================================================================
-- updateWarmCountStatistics( lsoMap, topWarmChunk );
-- ======================================================================
-- TODO: FInish this method
-- ======================================================================
local function updateWarmCountStatistics( lsoMap, topWarmChunk ) 
  return 0;
end

-- ======================================================================
-- ldrChunkInsert( topWarmChunk, lsoMap, listIndex,  insertList )
-- ======================================================================
-- Insert (append) the LIST of values (overflow from the HotCache) 
-- to this chunk's value list.  We start at the position "listIndex"
-- in "insertList".  Note that this call may be a second (or Nth) call,
-- so we are starting our insert in "insertList" from "listIndex", and
-- not implicitly from "1".
-- Parms:
-- (*) topWarmChunkRecord: Hotest of the Warm Chunk Records
-- (*) lsoMap: the LSO control information
-- (*) listIndex: Index into <insertList>
-- (*) insertList
-- Return: Number of items written
-- ======================================================================
local function ldrChunkInsert(topWarmChunkRecord,lsoMap,listIndex,insertList )
  local mod = "LsoStoneman";
  local meth = "ldrChunkInsert()";
  info("[ENTER]: <%s:%s> Index(%d) List(%s)\n",
    mod, meth, listIndex, tostring( insertList ) );

  -- TODO: ldrChunkInsert(): Make this work for BINARY mode as well as LIST.
  
  local ldrCtrlMap = topWarmChunkRecord['LdrControlBin'];
  local ldrValueList = topWarmChunkRecord['LdrListBin'];
  local chunkIndexStart = list.size( ldrValueList ) + 1;

  info("[DEBUG]: <%s:%s> Chunk: CTRL(%s) List(%s)\n",
    mod, meth, tostring( ldrCtrlMap ), tostring( ldrValueList ));

-- Note: Since the index of Lua arrays start with 1, that makes our
-- math for lengths and space off by 1. So, we're often adding or
-- subtracting 1 to adjust.
  local totalItemsToWrite = list.size( insertList ) + 1 - listIndex;
  local itemSpaceAvailable = (ldrCtrlMap.EntryMax - chunkIndexStart) + 1;

    -- In the unfortunate case where our accounting is bad and we accidently
    -- opened up this page -- and there's no room -- then just return ZERO
    -- items written, and hope that the caller can deal with that.
    if itemSpaceAvailable <= 0 then
      warn("[DEBUG]: <%s:%s> INTERNAL ERROR: No space available on chunk(%s)",
      mod, meth, tostring( ldrCtrlMap ));
      return 0; -- nothing written
    end

  -- If we EXACTLY fill up the chunk, then we flag that so the next Warm
  -- List Insert will know in advance to create a new chunk.
  if totalItemsToWrite == itemSpaceAvailable then
    lsoMap.WarmTopFull = 1; -- Now, remember to reset on next update.
    info("[DEBUG]: <%s:%s> TotalItems(%d) == SpaceAvail(%d): Top FULL!!",
      mod, meth, totalItemsToWrite, itemSpaceAvailable );
  end

  info("[DEBUG]: <%s:%s> TotalItems(%d) SpaceAvail(%d)\n",
    mod, meth, totalItemsToWrite, itemSpaceAvailable );

  -- Write only as much as we have space for
  local newItemsStored = totalItemsToWrite;
  if totalItemsToWrite > itemSpaceAvailable then
    newItemsStored = itemSpaceAvailable;
  end

  info("[DEBUG]: <%s:%s>: Copying From(%d) to (%d) Amount(%d)\n",
    mod, meth, listIndex, chunkIndexStart, newItemsStored );

  -- Special case of starting at ZERO -- since we're adding, not
  -- directly indexing the array at zero (Lua arrays start at 1).
  for i = 0, (newItemsStored - 1), 1 do
    list.append( ldrValueList, insertList[i+listIndex] );
  end -- for each remaining entry

  info("[DEBUG]: <%s:%s>: Post Chunk Copy: Ctrl(%s) List(%s)\n",
    mod, meth, tostring(ldrCtrlMap), tostring(ldrValueList));

  -- Store our modifications back into the Chunk Record Bins
  topWarmChunkRecord['LdrControlBin'] = ldrCtrlMap;
  topWarmChunkRecord['LdrListBin'] = ldrValueList;

  info("[EXIT]: <%s:%s> newItemsStored(%d) List(%s) \n",
    mod, meth, newItemsStored, tostring( ldrValueList) );
  return newItemsStored;
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
  info("[ENTER]: <%s:%s> ldr(%s) newValue(%s) \n",
    mod, meth, tostring(ldr), tostring(newValue) );

  local result = 1;  -- Be optimistic 

  -- TODO: ldrHashRoom() This needs to look at SIZES in the case of
  -- BINARY mode.  For LIST MODE, this will work.
  if list.size( ldr.EntryList ) >= ldr.EntryMax then
    result = 0;
  end

  info("[EXIT]: <%s:%s> result(%d) \n", mod, meth, result );
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
  info("[ENTER]: <%s:%s> Count(%d) \n", mod, meth, count );
  local doTheFunk = 0; -- Welcome to Funky Town

  if (func ~= nil and fargs ~= nil ) then
    doTheFunk = 1;
    info("[ENTER1]: <%s:%s> Count(%d) func(%s) fargs(%s)\n",
      mod, meth, count, func, tostring(fargs) );
  else
    info("[ENTER2]: <%s:%s> PeekCount(%d)\n", 
      mod, meth, count );
  end

  -- Iterate thru the cache, gathering up items in the result list.
  local resultList = list();
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
    
    numRead = numRead + 1;
    if numRead >= numToRead then
      info("[Early EXIT]: <%s:%s> NumRead(%d) \n", mod, meth, numRead );
      return numRead;
    end
  end -- for each entry in the list

  info("[EXIT]: <%s:%s> NumRead(%d) \n", mod, meth, numRead );
  return numRead;
end -- readEntryList()

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
  info("[ENTER]: <%s:%s> lsoMap(%s)\n", mod, meth, tostring(lsoMap) );

  -- We are called ONLY when the Warm Dir List is full -- so we should
  -- not have to check that in production, but during development, we're
  -- going to check that the warm list size is >= warm list max, and
  -- return an error if not.  This test can be removed in production.
  local transferAmount = lsoMap.WarmChunkTransfer;


  info("[DEBUG]: <%s:%s> NOT YET READY TO TRANSFER WARM TO COLD: Map(%s)\n",
    mod, meth, tostring(lsoMap) );

    -- TODO : Finish transferWarmCacheList() ASAP.

  info("[EXIT]: <%s:%s> lsoMap(%s) \n", mod, meth, tostring(lsoMap) );
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
  info("[ENTER]: <%s:%s> LSO BIN(%s) Bin Map(%s)\n", 
    mod, meth, lsoMap.BinName, tostring( lsoMap ));

--  if lsoMap.WarmChunkCount >= lsoMap.WarmCacheDirMax then
--    decision = 0;
--  end
    -- NOTE:  (TODO: FIXME: Right now we're disabling the Cold Cache,
    -- so for now we claim that there is ALWAYS room in the Warm Cache).
    decision = 1;

  info("[EXIT]: <%s:%s> Decision(%d)\n", mod, meth, decision );
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
  info("[ENTER]: <%s:%s> Count(%d) \n", mod, meth, count );

  local resultList = list();
  local numRead = readEntryList(resultList,cacheList, count, func, fargs, all);

  info("[EXIT]: <%s:%s> result(%s) \n", mod, meth, tostring(resultList) );
  return resultList;
end -- hotCacheRead()
-- ======================================================================

-- ======================================================================
-- WarmCacheChunkCreate( topRec, lsoMap )
-- ======================================================================
-- Create and initialise a new LDR "chunk", load the new digest for that
-- new chunk into the lsoMap (the warm dir list), and return it.
local function   WarmCacheChunkCreate( topRec, lsoMap )
  local mod = "LsoStoneman";
  local meth = "WarmCacheChunkCreate()";
  info("[ENTER]: <%s:%s> \n", mod, meth );

  -- Create the Aerospike Record, initialize the bins: Ctrl, List
  local newLdrChunkRecord = aerospike:crec_create( topRec );
  local ctrlMap = map();
  ctrlMap.ParentDigest = record.digest( topRec );
  ctrlMap.PageType = "Warm";
  ctrlMap.PageMode = "List";
  local newChunkDigest = record.digest( newLdrChunkRecord );
  ctrlMap.Digest = newChunkDigest;
  ctrlMap.BytesUsed = 0; -- We don't count control info
  ctrlMap.EntryMax = 100; -- Move up to TopRec -- when Stable
  ctrlMap.DesignVersion = 1;
  ctrlMap.LogInfo = 0;
  -- Assign Control info and List info to the LDR bins
  newLdrChunkRecord['LdrControlBin'] = ctrlMap;
  newLdrChunkRecord['LdrListBin'] = list();

  aerospike:crec_update( topRec, newLdrChunkRecord );

  -- Add our new chunk (the digest) to the WarmCacheList
  info("[DEBUG]: <%s:%s> Appending NewChunk(%s) to WarmList(%s)\n",
    mod, meth, tostring(newChunkDigest), tostring(lsoMap.WarmCacheList));
  list.append( lsoMap.WarmCacheList, newChunkDigest );
  info("[DEBUG]: <%s:%s> Post CHunkAppend:NewChunk(%s): LsoMap(%s)\n",
    mod, meth, tostring(newChunkDigest), tostring(lsoMap));
   
  -- Increment the Warm Count
  local warmChunkCount = lsoMap.WarmChunkCount;
  lsoMap.WarmChunkCount = (warmChunkCount + 1);

  -- Update the top (LSO) record with the newly updated lsoMap.
  topRec[ lsoMap.BinName ] = lsoMap;

  info("[EXIT]: <%s:%s> Return(%s) \n",
    mod, meth, ldrChunkSummary(newLdrChunkRecord));
  return newLdrChunkRecord;
end --  WarmCacheChunkCreate()
-- ======================================================================

-- ======================================================================
-- WarmCacheGetTop( topRec, lsoMap )
-- ======================================================================
-- Find the digest of the top of the Warm Dir List, Open that record and
-- return that opened record.
-- ======================================================================
local function   WarmCacheGetTop( topRec, lsoMap )
  local mod = "LsoStoneman";
  local meth = "WarmCacheGetTop()";
  info("[ENTER]: <%s:%s> \n", mod, meth );

  local warmCacheList = lsoMap.WarmCacheList;
  local stringDigest = tostring( warmCacheList[ list.size(warmCacheList) ]);

  info("[DEBUG]: <%s:%s> Warm Digest(%s) item#(%d)\n", 
      mod, meth, stringDigest, list.size( warmDirList ));

  local topWarmChunk = aerospike:crec_open( topRec, stringDigest );

  info("[EXIT]: <%s:%s> result(%s) \n",
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

  local chunkList = ldrChunk['LdrListBin'];
  local numRead = readEntryList(resultList,chunkList, count, func, fargs, all);

  info("[EXIT]: <%s:%s> NumberRead(%d) ResultList(%s) \n",
    mod, meth, numRead, tostring( resultList ));
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
  info("[ENTER]: <%s:%s> Count(%d) \n", mod, meth, count );

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
  local dirCount = list.size( warmDirList );
  local ldrChunk;
  local stringDigest;
  local status = 0;

  info("[DEBUG]: <%s:%s>: DirCount(%d),Top(%s) Reading WarmDirList(%s)(%s):\n",
  mod, meth, dirCount, validateTopRec( topRec, lsoMap ),
  tostring( warmDirList), tostring(warmDirList[1]));

  -- Read each Warm Chunk, adding to the resultList, until we either bypass
  -- the readCount, or we hit the end (either readCount is large, or the ALL
  -- flag is set).
  for dirIndex = dirCount, 1, -1 do
    -- Record Digest MUST be in string form
    stringDigest = tostring(warmDirList[ dirIndex ]);
    info("[DEBUG]: <%s:%s>: Opening Warm Chunk:Index(%d)Digest(%s):\n",
    mod, meth, dirIndex, stringDigest );
    ldrChunk = aerospike:crec_open( topRec, stringDigest );
    --
    -- resultList is passed by reference and we can just add to it.
    chunkItemsRead =
    ldrChunkRead( ldrChunk, resultList, remaining, func, fargs, all );
    totalWarmAmountRead = totalWarmAmountRead + chunkItemsRead;
    -- Early exit ONLY when ALL flag is not set.
    if( all == 0 and
      ( chunkItemsRead >= remaining or totalWarmAmountRead >= count ) )
    then
      info("[Early EXIT]: <%s:%s> totalWarmAmountRead(%d) ResultList(%s) \n",
        mod, meth, totalWarmAmountRead, tostring(resultList));
      status = aerospike:crec_close( topRec, ldrChunk );
      return totalWarmAmountRead;
    end

    status = aerospike:crec_close( topRec, ldrChunk );
    info("[DEBUG]: <%s:%s> as:close() status(%s) \n",
    mod, meth, tostring( status ) );
  end -- for each warm Chunk

  info("[EXIT]: <%s:%s> totalWarmAmountRead(%d) ResultList(%s) \n",
  mod, meth, totalWarmAmountRead, tostring(resultList));
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
  info("[ENTER]: <%s:%s> \n", mod, meth );
--info("[ENTER]: <%s:%s> LSO Summary(%s) \n", mod, meth, lsoSummary(lsoMap) );

  info("[DEBUG 0]:WDL(%s)", tostring( lsoMap.WarmCacheList ));

  local warmCacheList = lsoMap.WarmCacheList;
  local topWarmChunk;
  -- Whether we create a new one or open an existing one, we save the current
  -- count and close the record.
  -- Note that the last write may have filled up the warmTopChunk, in which
  -- case it set a flag so that we will go ahead and allocate a new one now,
  -- rather than after we read the old top and see that it's already full.
  if list.size( warmCacheList ) == 0 or lsoMap.WarmTopFull == 1 then
    info("[DEBUG]: <%s:%s> Calling Chunk Create \n", mod, meth );
    topWarmChunk = warmCacheListChunkCreate( topRec, lsoMap ); -- create new
    lsoMap.WarmTopFull = 0; -- reset for next time.
  else
    info("[DEBUG]: <%s:%s> Calling Get TOP \n", mod, meth );
    topWarmChunk = warmCacheListGetTop( topRec, lsoMap ); -- open existing
  end
  info("[DEBUG]: <%s:%s> Post 'GetTop': LsoMap(%s) \n", 
    mod, meth, tostring( lsoMap ));

  -- We have a warm Chunk -- write as much as we can into it.  If it didn't
  -- all fit -- then we allocate a new chunk and write the rest.
  local totalCount = list.size( insertList );
  info("[DEBUG]: <%s:%s> Calling Chunk Insert: List(%s)\n",
    mod, meth, tostring( insertList ));
  local countWritten = ldrChunkInsert( topWarmChunk, lsoMap, 1, insertList );
  local itemsLeft = totalCount - countWritten;
  if itemsLeft > 0 then
    aerospike:crec_update( topRec, topWarmChunk );
    aerospike:crec_close( topRec, topWarmChunk );
    info("[DEBUG]: <%s:%s> Calling Chunk Create: AGAIN!!\n", mod, meth );
    topWarmChunk = warmCacheListChunkCreate( topRec, lsoMap ); -- create new
    -- Unless we've screwed up our parameters -- we should never have to do
    -- this more than once.  This could be a while loop if it had to be, but
    -- that doesn't make sense that we'd need to create multiple new LDRs to
    -- hold just PART of the hot cache.
  info("[DEBUG]: <%s:%s> Calling Chunk Insert: List(%s) AGAIN(%d)\n",
    mod, meth, tostring( insertList ), countWritten + 1);
    countWritten = ldrChunkInsert( topWarmChunk, lsoMap, countWritten+1, insertList );
    if countWritten ~= itemsLeft then
      warn("[ERROR!!]: <%s:%s> Second Warm Chunk Write: CW(%d) IL(%d) \n",
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
  info("[DEBUG]: <%s:%s> Saving LsoMap (%s) Before Update \n",
    mod, meth, tostring( lsoMap ));
  topRec[lsoMap.BinName] = lsoMap;
  updateWarmCountStatistics( lsoMap, topWarmChunk );

  info("[DEBUG]: <%s:%s> Chunk Summary before storage(%s)\n",
    mod, meth, ldrChunkSummary( topWarmChunk ));

  info("[DEBUG]: <%s:%s> Calling CREC Update \n", mod, meth );
  local status = aerospike:crec_update( topRec, topWarmChunk );
  info("[DEBUG]: <%s:%s> CREC Update Status(%s) \n",mod,meth, tostring(status));
  info("[DEBUG]: <%s:%s> Calling CREC Close \n", mod, meth );

  status = aerospike:crec_close( topRec, topWarmChunk );
  info("[DEBUG]: <%s:%s> CREC Close Status(%s) \n",mod,meth, tostring(status));

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
  info("[ENTER]: <%s:%s> LSO Summary() \n", mod, meth );
--info("[ENTER]: <%s:%s> LSO Summary(%s) \n", mod, meth, lsoSummary(lsoMap) );

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

  info("[EXIT]: <%s:%s> result(%d) \n", mod, meth, rc );
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
  info("[ENTER]: <%s:%s> \n", mod, meth );

  info("[DEBUG]: <%s:%s> COLD STORAGE NOT YET IMPLEMENTED!! \n", mod, meth );

  info("[EXIT]: <%s:%s> \n", mod, meth );
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
  info("[ENTER]: <%s:%s> ReadCount(%s)\n", tostring(peekCount), mod, meth );

  if (func ~= nil and fargs ~= nil ) then
    info("[ENTER1]: <%s:%s> Count(%d) func(%s) fargs(%s)\n",
      mod, meth, peekCount, func, tostring(fargs) );
  else
    info("[ENTER2]: <%s:%s> PeekCount(%d)\n", mod, meth, peekCount );
  end

  local all = 0;
  if peekCount == 0 then all = 1; end

  -- Fetch from the Cache, then the Warm List, then the Cold List.
  -- Each time we decrement the count and add to the resultlist.
  local cacheList = lsoMap.HotCacheList;
  local resultList = hotCacheRead( cacheList, peekCount, func, fargs, all);
  local cacheCount = list.size( resultList );
  info("[DEBUG]: <%s:%s> HotListResult(%s)\n", mod, meth,tostring(resultList));

  local warmCount = 0;
  local warmList;

  -- If the cache had all that we need, then done.  Return list.
  if( cacheCount >= peekCount and all == 0) then
    return resultList;
  end

  -- We need more -- get more out of the Warm List.  If ALL flag is set,
  -- keep going until we're done.  Otherwise, compute the correct READ count
  -- given that we've already read from the Hot List.
  local remainingCount = 0; -- Default, when ALL flag is on.
  if( all == 0 ) then
    remainingCount = peekCount - numRead;
  end
  info("[DEBUG]: <%s:%s> Checking WarmList Count(%d) All(%d)\n",
    mod, meth, remainingCount, all);
  -- If no Warm List, then we're done (assume no cold list if no warm)
  if list.size(lsoMap.WarmDirList) > 0 then
    warmCount =
      warmDirRead(topRec, resultList, lsoMap, remainingCount, func, fargs, all);
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
  info("[ENTER]: <%s:%s> : \n", mod, meth );
  local result = 1;  -- This is the usual case

  local cacheLimit = lsoMap.HotCacheMax;
  local cacheList = lsoMap.HotCacheList;
  if list.size( cacheList ) >= cacheLimit then
    return 0
  end

  info("[EXIT]: <%s:%s> Result(%d) : \n", mod, meth, result);
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
-- Parms:
-- (*) lsoMap: the map for the LSO Bin
-- (*) newStorageValue: the new value to be pushed on the stack
local function hotCacheInsert( lsoMap, newStorageValue  )
  local mod = "LsoStoneman";
  local meth = "hotCacheInsert()";
  info("[ENTER]: <%s:%s> : Insert Value(%s)\n",
    mod, meth, tostring(newStorageValue) );

  local hotCacheList = lsoMap.HotCacheList;
  if newStorageValue.StorageMode == 0 then
    info("[DEBUG]: <%s:%s> : (NOT READY!!) Perform COMPACT STORAGE\n",
      mod, meth );
    -- TODO: hotCacheInsert(): Must finish Compact Storage
    -- Depending on our current state of implementation, we are either
    -- going to copy our compact storage into a special bin, or we are
    -- going to assign the BYTES value into the cache list
  else
    list.append( hotCacheList, newStorageValue.Value );
  end
  local itemCount = lsoMap.ItemCount;
  lsoMap.ItemCount = (itemCount + 1);
  local hotCount = lsoMap.HotCacheItemCount;
  lsoMap.HotCacheItemCount = (hotCount + 1);
  return 0;  -- all is well

end -- hotCacheInsert()
-- ======================================================================

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
  info("[ENTER]: <%s:%s>:: LsoBinName(%s)\n", mod, meth, tostring(lsoBinName));

  -- Create the map, and fill it in.
  local lsoMap = map();
  -- General Parms:
  lsoMap.ItemCount = 0;     -- A count of all items in the stack
  lsoMap.DesignVersion = 1; -- Current version of the code
  lsoMap.Magic = "MAGIC"; -- we will use this to verify we have a valid map
  lsoMap.BinName = lsoBinName; -- Defines the LSO Bin
  lsoMap.NameSpace = "test"; -- Default NS Name -- to be overridden by user
  lsoMap.Set = "set";       -- Default Set Name -- to be overridden by user
  -- LSO Data Record Chunk Settings
  lsoMap.LdrEntryMax = 100;  -- Max # of items in a Data Chunk (List Mode)
  lsoMap.LdrEntrySize = 20;  -- Must be a setable parm
  lsoMap.LdrByteMax = 2000;  -- Max # of BYTES in a Data Chunk (binary mode)
  -- Hot Cache Settings
  lsoMap.HotCacheList = list();
  lsoMap.HotCacheItemCount = 0; -- Number of elements in the Top Cache
  lsoMap.HotCacheMax = 100; -- Max Number for the cache -- when we transfer
  lsoMap.HotCacheTransfer = 50; -- How much to Transfer at a time.
  -- Warm Cache Settings
  lsoMap.WarmTopFull = 0; -- 1  when the top chunk is full (for the next write)
  lsoMap.WarmCacheList = list();   -- Define a new list for the Warm Stuff
  lsoMap.WarmChunkCount = 0; -- Number of Warm Data Record Chunks
  lsoMap.WarmCacheDirMax = 1000; -- Number of Warm Data Record Chunks
  lsoMap.WarmChunkTransfer = 10; -- Number of Warm Data Record Chunks
  lsoMap.WarmTopChunkEntryCount = 0; -- Count of entries in top warm chunk
  lsoMap.WarmTopChunkByteCount = 0; -- Count of bytes used in top warm Chunk
  -- Cold Cache Settings
  lsoMap.ColdChunkCount = 0; -- Number of Cold Data Record Chunks
  lsoMap.ColdDirCount = 0; -- Number of Cold Data Record DIRECTORY Chunks
  lsoMap.ColdListHead  = 0;   -- Nothing here yet
  lsoMap.ColdCacheDirMax = 100;  -- Should be a setable parm

  info("[DEBUG]: <%s:%s> : CTRL Map after Init(%s)\n",
    mod, meth , tostring(lsoMap));

  -- Put our new map in the record, then store the record.
  topRec[lsoBinName] = lsoMap;

  info("[EXIT]:<%s:%s>:Dir Map after Init(%s)\n", mod,meth,tostring(lsoMap));
  return lsoMap
end -- initializeLsoMap


-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- LSO Main Functions
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
--
-- ======================================================================
-- || stackCreate (Stickman)    ||
-- ======================================================================
-- Create/Initialize a Stack structure in a bin, using a single LSO
-- bin, using User's name, but Aerospike TYPE (AS_LSO)
--
-- For this version (Stickman), we will be using a SINGLE MAP object,
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
-- Parms (inside arglist)
-- (1) topRec: the user-level record holding the LSO Bin
-- (2) arglist: the list of create parameters
--  (2.1) LsoBinName
--  (2.2) Namespace (just one, for now)
--  (2.3) Set
--  (2.4) LdrByteMax
--  (2.5) Design Version
--
--  !!!! More parms needed here to appropriately configure the LSO
--  -> Hot Cache Size
--  -> Hot Cache Transfer amount
--  -> Warm List Size
--  -> Warm List Transfer amount
--
function stackCreate( topRec, lsoBinName, arglist )
  local mod = "LsoStoneman";
  local meth = "stackCreate()";

  if arglist == nil then
    info("[ENTER]: <%s:%s> lsoBinName(%s) NULL argList\n",
      mod, meth, tostring(lsoBinName));
  else
    info("[ENTER]: <%s:%s> lsoBinName(%s) argList(%s) \n",
    mod, meth, tostring( lsoBinName), tostring( arglist ));
  end

  -- Some simple protection if things are weird
  if lsoBinName == nil then
    warn("[WARNING]: <%s:%s> Bad LSO BIN Name: Using default\n", mod, meth );
    lsoBinName = "LsoBin";
  end

  -- Check to see if LSO Structure (or anything) is already there,
  -- and if so, error
  if( topRec[lsoBinName] ~= nil ) then
    warn("[ERROR EXIT]: <%s:%s> LSO BIN(%s) Already Exists\n",
      mod, meth, tostring(lsoBinName) );
    return('LSO_BIN already exists');
  else
    -- Create and initialize the LSO MAP -- the main LSO structure
    local lsoMap = initializeLsoMap( topRec, lsoBinName );
  end

  -- Put our new map in the record, then store the record.
  topRec[lsoBinName] = lsoMap;

  info("[DEBUG]:<%s:%s>:Dir Map after Init(%s)\n", mod,meth,tostring(lsoMap));

  -- All done, store the record
  local rc = -99; -- Use Odd starting Num: so that we know it got changed
  if( not aerospike:exists( topRec ) ) then
    info("[DEBUG]:<%s:%s>:Create Record()\n", mod, meth );
    rc = aerospike:create( topRec );
  else
    info("[DEBUG]:<%s:%s>:Update Record()\n", mod, meth );
    rc = aerospike:update( topRec );
  end

  info("[EXIT]: <%s:%s> : Done.  RC(%d)\n", mod, meth, rc );
  return rc;
end -- function stackCreate( topRec, namespace, set )

-- ======================================================================
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- || local stackPush (Stickman V2)
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
    info("[ENTER1]: <%s:%s> LSO BIN(%s) NewValue(%s) func(%s) fargs(%s)\n",
      mod, meth, lsoBinName, tostring( newValue ), func, tostring(fargs) );
  else
    info("[ENTER2]: <%s:%s> LSO BIN(%s) NewValue(%s)\n",
      mod, meth, lsoBinName, tostring( newValue ));
  end

  -- Some simple protection if things are weird
  if lsoBinName == nil then
    warn("[WARNING]: <%s:%s> Bad LSO BIN Name: Using default\n", mod, meth );
    lsoBinName = "LsoBin";
  end

  local lsoMap;
  if( not aerospike:exists( topRec ) ) then
    warn("[WARNING]:<%s:%s>:Record Does Not exist. Creating\n", mod, meth );
    lsoMap = initializeLsoMap( topRec, lsoBinName );
    aerospike:create( topRec );
  end

  if( topRec[lsoBinName] == nil ) then
    warn("[WARNING]: <%s:%s> LSO BIN (%s) DOES NOT Exist: Creating\n",
      mod, meth, tostring(lsoBinName) );
    lsoMap = initializeLsoMap( topRec, lsoBinName );
    aerospike:create( topRec );
  end
  
  -- check that our bin is (relatively intact
  local lsoMap = topRec[lsoBinName]; -- The main LSO map
  if lsoMap.Magic ~= "MAGIC" then
    warn("[ERROR EXIT]: <%s:%s> LSO_BIN (%s) Is Corrupted (no magic)\n",
      mod, meth, lsoBinName );
    return('LSO_BIN Is Corrupted');
  end

  -- Now, it looks like we're ready to insert.  If there is an inner UDF
  -- to apply, do it now.
  local newStorageValue;
  if doTheFunk == 1 then 
    info("[DEBUG]: <%s:%s> Applying UDF (%s) with args(%s)\n",
      mod, meth, func, tostring( fargs ));
    newValue = functionTable[func]( newValue, fargs );
  end

  newStorageValue = valueStorage( type(newValue), newValue );
  info("[DEBUG]: <%s:%s> AFTER UDF (%s) with ValueStorage(%s)\n",
      mod, meth, func, tostring( newStorageValue ));

  -- If we have room, do the simple cache insert.  If we don't have
  -- room, then make room -- transfer half the cache out to the warm list.
  -- That may, in turn, have to make room by moving some items to the
  -- cold list.
  if hotCacheHasRoom( lsoMap, newStorageValue ) == 0 then
    info("[DEBUG]:<%s:%s>:>>> CALLING TRANSFER HOT CACHE!!<<<\n", mod, meth );
    hotCacheTransfer( topRec, lsoMap );
  end
  hotCacheInsert( lsoMap, newStorageValue );
  -- Must always assign the object BACK into the record bin.
  topRec[lsoBinName] = lsoMap;

  -- All done, store the topRec
  local rc = -99; -- Use Odd starting Num: so that we know it got changed
  info("[DEBUG]:<%s:%s>:Update Record\n", mod, meth );
  rc = aerospike:update( topRec );

  info("[EXIT]: <%s:%s> : Done.  RC(%d)\n", mod, meth, rc );
  return rc
end -- function stackPush()

-- =======================================================================
-- Stack Push -- with and without inner UDFs
-- These are the globally visible calls -- that call the local UDF to do
-- all of the work.
-- NOTE: All parameters must be protected with "tostring()" so that we
-- do not encounter a format error if the user passes in nil or any
-- other incorrect value/type
-- =======================================================================
function stackPush( topRec, lsoBinName, newValue )
  local mod = "LsoStoneman";
  local meth = "stackPush()";
  info("[ENTER]: <%s:%s> LSO BIN(%s) NewValue(%s)\n",
    mod, meth, tostring(lsoBinName), tostring( newValue ));
  return localStackPush( topRec, lsoBinName, newValue, nil, nil )
end -- end stackPush()

function stackPushWithUDF( topRec, lsoBinName, newValue, func, fargs )
  local mod = "LsoStoneman";
  local meth = "stackPushWithUDF()";
  info("[ENTER]: <%s:%s> LSO BIN(%s) NewValue(%s) Func(%s) Fargs(%s)\n",
    mod, meth, tostring(lsoBinName), tostring( newValue ),
    tostring(func), tostring(fargs));
  return localStackPush( topRec, lsoBinName, newValue, func, fargs );
end -- stackPushWithUDF()

-- ======================================================================
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- || Local StackPeek: Stickman V2 (the new and improved structure)
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
  info("[ENTER]: <%s:%s> PeekCount(%d) \n", mod, meth, peekCount );

  if (func ~= nil and fargs ~= nil ) then
    info("[ENTER1]: <%s:%s> LSO BIN(%s) PeekCount(%d) func(%s) fargs(%s)\n",
      mod, meth, lsoBinName, peekCount,  func, tostring(fargs) );
  else
    info("[ENTER2]: <%s:%s> LSO BIN(%s) PeekCount(%d)\n", 
      mod, meth, lsoBinName, peekCount );
  end

  if( not aerospike:exists( topRec ) ) then
    warn("[ERROR EXIT]:<%s:%s>:Missing Record. Exit\n", mod, meth );
    return('Base Record Does NOT exist');
  end

  -- Verify that the LSO Structure is there: otherwise, error.
  if( lsoBinName == nil ) then
    warn("[ERROR EXIT]: <%s:%s> Bad LSO BIN Parameter\n", mod, meth );
    return('Bad LSO Bin Parameter');
  end
  if( topRec[lsoBinName] == nil ) then
    warn("[ERROR EXIT]: <%s:%s> LSO_BIN (%s) DOES NOT Exists\n",
      mod, meth, lsoBinName );
    return('LSO_BIN Does NOT exist');
  end
  
  -- check that our bin is (mostly) there
  local lsoMap = topRec[lsoBinName]; -- The main LSO map
  if lsoMap.Magic ~= "MAGIC" then
    info("[ERROR EXIT]: <%s:%s> LSO_BIN (%s) Is Corrupted (no magic)\n",
      mod, meth, lsoBinName );
    return('LSO_BIN Is Corrupted');
  end

  -- Build the user's "resultList" from the items we find that qualify.
  -- They must pass the "transformFunction()" filter.
  -- Also, Notice that we go in reverse order -- to get the "stack function",
  -- which is Last In, First Out.
  info("[DEBUG]: <%s:%s>: Calling Map Peek\n", mod, meth );
  local resultList = lsoMapRead( topRec, lsoMap, peekCount, func, fargs );

  info("[EXIT]: <%s:%s>: PeekCount(%d) ResultList(%s)\n",
    mod, meth, peekCount, tostring(resultList));

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
  info("[ENTER]: <%s:%s> LSO BIN(%s) peekCount(%s)\n",
    mod, meth, tostring(lsoBinName), tostring(peekCount) )
  return localStackPeek( topRec, lsoBinName, peekCount, nil, nil )
end -- end stackPeek()

function stackPeekWithUDF( topRec, lsoBinName, peekCount, func, fargs )
  local mod = "LsoStoneman";
  local meth = "stackPeekWithUDF()";
  info("[ENTER]: <%s:%s> LSO BIN(%s) peekCount(%s) func(%s) fargs(%s)\n",
    mod, meth, tostring(lsoBinName), tostring(peekCount),
    tostring(func), tostring(fargs));
  return localStackPeek( topRec, lsoBinName, peekCount, func, fargs );
end -- stackPeekWithUDF()

--
-- ======================================================================
-- ||| UNIT TESTS |||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- Test Individual pieces to verify functionality
-- ======================================================================
--
-- ======================================================================
-- ===================== <<<<<  S I M P L E  >>>> =======================
-- ======================================================================
function simpleStackCreate(topRec) 
   local binname = "dirlist" 
   local dirlist = list();
   topRec[binname] = dirlist; 
   if( not aerospike:exists( topRec ) ) then
      info("Create Record()\n");
      rc = aerospike:create( topRec );
   else
      info("Update Record()\n");
      rc = aerospike:update( topRec );
   end
   info("SimpleStackCreate Result(%d)\n", rc );
   return "Create Success";
end

-- ======================================================================
-- ======================================================================
function simpleStackPush ( topRec, newValue ) 
   if( not aerospike:exists( topRec ) ) then
      info("stackPush Failed Record not found\n");
      rc = aerospike:create( topRec );
   end 
   local binname    = "dirlist"; 
   local dirlist    = topRec["dirlist"];
   info("Create new Record ");
   newRec           = aerospike:crec_create( topRec );
   newRec["valbin"] = newValue;
   info("Put value in new Record ", tostring( newValue ) );
   info("Update New Record ");
   aerospike:crec_update( topRec, newRec );

   local newdigest  = record.digest( newRec );
   info("Prepend to Top Record ");
   list.prepend (dirlist, tostring( newdigest ));
   info("Put value in Top Record %s", tostring( newdigest ) );
   topRec[binname]  = dirlist;
   info("Update Top Record |%s|", tostring(dirlist));
   rc = aerospike:update( topRec );
   info("SimpleStackPush Result(%d)\n", rc );
   return "Push Result:" .. tostring(newValue) .. " RC: " .. tostring(rc);
end

-- ======================================================================
-- ======================================================================
function simpleStackPeek ( topRec, count ) 
   if( not aerospike:exists( topRec ) ) then
      info("stackPeek Failed Record not found\n");
      rc = aerospike:create( topRec );
   end 
   local binname  = "dirlist"; 
   local dirlist  = topRec[binname];
   info("Dir list state at peek |%s| ", tostring(dirlist));
   local peeklist = list.take(dirlist, count);
   info("Peek size requested %d, peeked %d", count, list.size(peeklist));
   resultlist   = list();
   for index = 1, list.size(peeklist) do
      local valdig = tostring ( dirlist[index] );
      newRec       = aerospike:crec_open( topRec, valdig );
      newValue     = newRec["valbin"];
      list.append(resultlist, tostring( newValue ));
      info("stackPeek: found %s --> %s", valdig, tostring( newValue ) );
      aerospike:crec_close( topRec, newRec );
   end
   return resultlist;
end

-- ======================================================================
-- ===================== <<<<<  M E D I U M  >>>> =======================
-- ======================================================================
function mediumStackCreate(topRec) 
   local binname = "MediumDirList" 
   local lsoMap = map();
   lsoMap.DirList = list();
   topRec[binname] = lsoMap;
   if( not aerospike:exists( topRec ) ) then
      info("Create Record()\n");
      rc = aerospike:create( topRec );
   else
      info("Update Record()\n");
      rc = aerospike:update( topRec );
   end
   info("SimpleStackCreate Result(%d)\n", rc );
   return "Create Success";
end

-- ======================================================================
-- ======================================================================
function mediumStackPush ( topRec, newValue ) 
   if( not aerospike:exists( topRec ) ) then
      info("stackPush Failed Record not found\n");
      rc = aerospike:create( topRec );
   end 
   local binname    = "MediumDirList"; 
   local lsoMap     = topRec[binname];
   local dirlist    = lsoMap.DirList;
   info("Create new Record ");
   newRec           = aerospike:crec_create( topRec );
   newRec["valbin"] = newValue;
   info("Put value in new Record(%s)", tostring( newValue ) );
   info("Update New Record ");
   aerospike:crec_update( topRec, newRec );

   local newdigest  = record.digest( newRec );
   info("Prepend to Top Record ");
   list.prepend (dirlist, tostring( newdigest ));
   lsoMap.DirList = dirlist;
   info("Put value in Top Record (%s)", tostring( newdigest ) );
   topRec[binname]  = lsoMap;
   info("Update Top Record (%s)", tostring( lsoMap ));
   rc = aerospike:update( topRec );
   info("SimpleStackPush Result(%s)\n", tostring(rc) );
   return "Push Result:" .. tostring(newValue) .. " RC: " .. tostring(rc);
end

-- ======================================================================
-- ======================================================================
function mediumStackPeek ( topRec, count ) 
   if( not aerospike:exists( topRec ) ) then
      info("stackPeek Failed Record not found:  Creating\n");
      rc = aerospike:create( topRec );
      info("stackPeek: Create Result(%s) \n", tostring(rc));
   end 
   local binname  = "MediumDirList"; 
   local lsoMap     = topRec[binname];
   local dirlist    = lsoMap.DirList;
   info("Dir list state at peek (%s) ", tostring(dirlist));
   local peeklist = list.take(dirlist, count);
   info("Peek size requested %d, peeked %d", count, list.size(peeklist));
   resultlist   = list();
   for index = 1, list.size(peeklist) do
      local digest = tostring ( dirlist[index] );
      newRec       = aerospike:crec_open( topRec, digest );
      newValue     = newRec["valbin"];
      list.append(resultlist, tostring( newValue ));
      info("stackPeek: found Digest(%s) --> (%s)",
        tostring( digest), tostring( newValue ) );
      rc = aerospike:crec_close( topRec, newRec );
      info("stackPeek: CREC CLOSE Result(%s) \n", tostring(rc));
   end
   return resultlist;
end
-- ======================================================================


-- ======================================================================
-- ===================== <<<<<   L A R G E   >>>> =======================
-- ======================================================================
-- Large Unit Test:
-- Create a large Top Rec, with relatively large chunks whose digests are
-- stored in the WarmCacheList.
-- Store values BOTH in the HotCacheList (direct) and the WarmCacheList,
-- which allocates a record and stores the value there (20 times) in
-- a value list.
--
-- ======================================================================
function largeStackCreate(topRec) 
  local binname = "Large LSO BIN" 
  
  info("[ENTER]: LargeStackCreate \n");

  local lsoMap = initializeLsoMap( topRec, binName );

  topRec[binname] = lsoMap;
  if( not aerospike:exists( topRec ) ) then
     info("Create Record()\n");
     rc = aerospike:create( topRec );
  else
     info("Update Record()\n");
     rc = aerospike:update( topRec );
  end
  info("LargeStackCreate Result(%d) lsoMap(%s)\n", rc, tostring(lsoMap));
  return "Create Success";
end

-- ======================================================================
-- Fill this Chunk up with stuff.  Put stuff in the map bin, and put 20
-- copies of the value in the EntryListBin.
--
-- Create and initialise a new LDR "chunk", load the new digest for that
-- new chunk into the lsoMap (the warm dir list), and return it.
--
-- Record Structure: Chunk Rec[ ldrControlBin, ldrListBin ]
-- ======================================================================
local function testCreateNewComplexRecord( topRec, lsoMap, newValue )
  local mod = "LsoStoneman";
  local meth = "testCreateNewRecord()";
  info("[ENTER]: <%s:%s> \n", mod, meth );

  -- Create the Aerospike Record, initialize the bins: Ctrl, List
  info("[DEBUG]: <%s:%s>: Calling CREC Create\n", mod, meth);
  local newLdrChunkRecord = aerospike:crec_create( topRec );
  local ctrlMap = map();
  ctrlMap.ParentDigest = record.digest( topRec );
  ctrlMap.PageType = "Warm";
  ctrlMap.PageMode = "List";
  info("[DEBUG]: <%s:%s>: Calling Rec.Digest\n", mod, meth);
  local newChunkDigest = record.digest( newLdrChunkRecord );
  ctrlMap.Digest = newChunkDigest;
  ctrlMap.BytesUsed = 0; -- We don't count control info
  ctrlMap.EntryMax = 100; -- Move up to TopRec -- when Stable
  ctrlMap.DesignVersion = 1;
  ctrlMap.LogInfo = 0;
  -- ctrlMap.WarmCacheItemCount = 0;
  -- Assign Control info and List info to the LDR bins
  newLdrChunkRecord['LdrControlBin'] = ctrlMap;
  local valueList = list();
  for i = 1, 20, 1 do
    list.append( valueList, newValue );
  end
  newLdrChunkRecord['LdrListBin'] = valueList;

  info("Update New Record ");
  aerospike:crec_update( topRec, newLdrChunkRecord );

  -- Add our new chunk (the digest) to the WarmCacheList
  list.append( lsoMap.WarmCacheList, newChunkDigest );
  local warmChunkCount = lsoMap.WarmChunkCount;
  lsoMap.WarmChunkCount = (warmChunkCount + 1);

  -- Update the top (LSO) record with the newly updated lsoMap.
  topRec[ lsoMap.BinName ] = lsoMap;

  info("[EXIT]: <%s:%s> ctrlMap(%s) valueList(%s) lsoMap(%s)\n",
    mod, meth, tostring(ctrlMap), tostring(valueList), tostring(lsoMap));
--  info("[EXIT]: <%s:%s> Return(%s) \n",
--    mod, meth, ldrChunkSummary(newLdrChunkRecord));
  return newLdrChunkRecord;
end --  testCreateNewRecord()()
-- ======================================================================
-- ======================================================================
-- SIMPLE VERSION
-- Fill this Chunk up with stuff.  Put stuff in the map bin, and put 20
-- copies of the value in the EntryListBin.
--
-- Create and initialise a new LDR "chunk", load the new digest for that
-- new chunk into the lsoMap (the warm dir list), and return it.
--
-- Record Structure: Chunk Rec[ ldrControlBin, ldrListBin ]
-- ======================================================================
local function testCreateNewSimpleRecord( topRec, lsoMap, newValue )
  local mod = "LsoStoneman";
  local meth = "testCreateNewRecord()";
  info("[ENTER]: <%s:%s> \n", mod, meth );

  -- Create the Aerospike Record, initialize the bins: Ctrl, List
  local newLdrChunkRecord = aerospike:crec_create( topRec );
  local ctrlMap = map();
  ctrlMap.ParentDigest = record.digest( topRec );
  ctrlMap.PageType = "Warm";
  ctrlMap.PageMode = "List";
  local newChunkDigest = record.digest( newLdrChunkRecord );
  ctrlMap.Digest = newChunkDigest;
  ctrlMap.BytesUsed = 0; -- We don't count control info
  ctrlMap.EntryMax = 100; -- Move up to TopRec -- when Stable
  ctrlMap.DesignVersion = 1;
  ctrlMap.LogInfo = 0;
  -- ctrlMap.WarmCacheItemCount = 0;
  -- Assign Control info and List info to the LDR bins
  newLdrChunkRecord['LdrControlBin'] = ctrlMap;
  local valueList = list();
  for i = 1, 20, 1 do
    list.append( valueList, newValue );
  end
  newLdrChunkRecord['LdrListBin'] = valueList;

  info("Update New Record ");
  aerospike:crec_update( topRec, newLdrChunkRecord );

  -- Add our new chunk (the digest) to the WarmCacheList
--  list.append( lsoMap.WarmCacheList, newChunkDigest );
--  local warmChunkCount = lsoMap.WarmChunkCount;
--  lsoMap.WarmChunkCount = (warmChunkCount + 1);

  -- Update the top (LSO) record with the newly updated lsoMap.
--  topRec[ lsoMap.BinName ] = lsoMap;

  info("[EXIT]: <%s:%s> ctrlMap(%s) valueList(%s) lsoMap(%s)\n",
    mod, meth, tostring(ctrlMap), tostring(valueList), tostring(lsoMap));
--  info("[EXIT]: <%s:%s> Return(%s) \n",
--    mod, meth, ldrChunkSummary(newLdrChunkRecord));
  return newLdrChunkRecord;
end --  testCreateNewRecord()()
-- ======================================================================

-- ======================================================================
-- For this value, stuff it in the HotCache AND create a Chunk page for it
-- and stuff 20 copies of that value in the Chunk Page, along with some
-- other stuff.
-- ======================================================================
function largeStackPush ( topRec, newValue ) 
   if( not aerospike:exists( topRec ) ) then
      info("stackPush Failed Record not found\n");
      rc = aerospike:create( topRec );
   end 
   local binname     = "Large LSO BIN" 
   local lsoMap      = topRec[binname];
   local warmCache   = lsoMap.WarmCache;
   local hotCache    = lsoMap.HotCache;
   info("Create new Record ");
   -- newRec = testCreateNewRecord( topRec, lsoMap, newValue )
   newRec = testCreateNewSimpleRecord( topRec, lsoMap, newValue )
   
   local newdigest  = record.digest( newRec );
   info("Append to Top Record ");
   list.append(warmCache, tostring( newdigest ));
   lsoMap.WarmCache = dirlist;
   list.append( hotCache, newValue );
   info("Put value in Top Record (%s)", tostring( newdigest ) );
   topRec[binname]  = lsoMap;
   info("Update Top Record (%s)", tostring( lsoMap ));
   rc = aerospike:update( topRec );
   info("LargeStackPush Result(%s)\n", tostring(rc) );
   return "Push Result:" .. tostring(newValue) .. " RC: " .. tostring(rc);
end

-- ======================================================================
-- Read values from the Hot List and the Warm List
-- Record Structure: Chunk Rec[ ldrControlBin, ldrListBin ]
-- ======================================================================
function largeStackPeek ( topRec, count ) 
   if( not aerospike:exists( topRec ) ) then
      info("stackPeek Failed Record not found:  Exit\n");
      return -1;
   end 

   local binname = "Large LSO BIN" 
   local lsoMap     = topRec[binname];
   local warmCache   = lsoMap.WarmCache;
   local hotCache    = lsoMap.HotCache;
   local dirlist    = warmCache;


   info("Dir list state at peek (%s) ", tostring(dirlist));
   local peeklist = list.take(dirlist, count);
   info("Peek size requested %d, peeked %d", count, list.size(peeklist));
   info("HotPeek:  %s \n", tostring( hotCache ));
   resultlist   = list();
   for index = 1, list.size(peeklist) do
      local digest = tostring ( dirlist[index] );
      chunkRec       = aerospike:crec_open( topRec, digest );
      ctrlMap = chunkRec['LdrControlBin'];
      dirList = chunkRec['LdrListBin'];
      info("ChunkPeek(%d): Digest(%s) CMap(%s) DList(%s)\n",
        index, tostring(digest), tostring(ctrlMap), tostring(dirList));

      newValue     = dirList[1];
      list.append(resultlist, tostring( newValue ));
      list.append(resultlist, tostring( digest ));
      info("stackPeek: found Digest(%s) --> (%s)",
        tostring( digest), tostring( newValue ) );
      rc = aerospike:crec_close( topRec, chunkRec );
      info("stackPeek: CREC CLOSE Result(%s) \n", tostring(rc));
   end
   return resultlist;
end
-- ======================================================================
-- ======================================================================
-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
