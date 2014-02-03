-- Large Stack Object (LSO) Operations
-- Stickman V2.1 -- (Feb 28, 2013)
--
-- TO DO List:
-- TODO: Finish transferWarmDirList() method ASAP.
-- TODO: Make this work for both REGULAR and BINARY Mode
-- TODO: hotCacheTransfer(): Make this more efficient
-- TODO: hotCacheInsert(): Must finish Compact Storage
--
-- ======================================================================
--
-- Functions Supported
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
-- Aerospike Calls:
-- newRec = aerospike:crec_create( topRec )
-- newRec = aerospike:crec_open( topRec, digest)
-- status = aerospike:crec_update( topRec, newRec )
-- status = aerospike:crec_close( topRec, newRec )
-- digest = record.digest( newRec )
-- ======================================================================
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
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- Visual Depiction
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- In a user record, the bin holding the Large Stack Object (LSO) is
-- referred to as an "LSO" bin. The overhead of the LSO value is 
-- (*) LSO Control Info (~70 bytes)
-- (*) LSO Hot Cache: List of data entries (on the order of 100)
-- (*) LSO Warm Directory: List of digests: 10 digests(250 bytes)
-- (*) LSO Cold Directory Head (digest of Head plus count) (30 bytes)
-- (*) Total LSO Record overhead is on the order of 350 bytes
-- NOTES:
-- (*) Record types used in this design:
-- (1) There is the main record that contains the LSO bin (LSO Head)
-- (2) There are Data Chunk Records (both Warm and Cold)
--     ==> Warm and Cold Data Chunk Records have the same format:
--         They both hold User Stack Data.
-- (3) There are Chunk Directory Records (used in the cold list)
-- (*) What connects to what
-- (+) The main record points to:
--     - Warm Data Chunk Records (these records hold stack data)
--     - Chunk Directory Records (these records hold ptrs to Cold Chunks)
-- (*) We may have to add some auxilliary information that will help
-- pick up the pieces in the event of a network/replica problem, where
-- some things have fallen on the floor.
-- +-----+-----+-----+-----+----------------------------------------+
-- |User |User |o o o|LSO  |                                        |
-- |Bin 1|Bin 2|o o o|Bin 1|                                        |
-- +-----+-----+-----+-----+----------------------------------------+
--                  /       \                                       
--   ================================================================
--     LSO Map                                              
--     +-------------------+                                 
--     | LSO Control Info  |
--     +----------------++++                                 
--     | Hot Entry Cache||||   <> Each Chunk dir holds about 100 Chunk Ptrs
--     +----------------++++   <> Each Chunk holds about 100 entries
--   +-| LSO Warm Dir List |   <> Each Chunk dir pts to about 10,000 entries
--   | +-------------------+   +-----+->+-----+->+-----+ ->+-----+
--   | | LSO Cold Dir Head |-> |Rec  |  |Rec  |  |Rec  | o |Rec  |
--   | +-------------------+   |Chunk|  |Chunk|  |Chunk| o |Chunk|
--   |   The Cold Dir is a     |Dir  |  |Dir  |  |Rec  | o |Dir  |
--   |   Linked List of Dirs   +-----+  +-----+  +-----+   +-----+
--   |   that point to records ||...|   ||...|   ||...|    ||...|
--   |   (cold chunks) that    ||   V   ||   V   ||   V    ||   V
--   |   hold the cold data.   ||   +--+||   +--+||   +--+ ||   +--+
--   |                         ||   |Cn|||   |Cn|||   |Cn| ||   |Cn|
--   |                         |V   +--+|V   +--+|V   +--+ |V   +--+
--   |                         |+--+    |+--+    |+--+     |+--+
--   |                         ||C2|    ||C2|    ||C2|     ||C2|
--   |   Warm data ages out     V+--+    V+--+    V+--+     V+--+
--   |   of the "in record"    +--+     +--+     +--+      +--+
--   |   "warm chunks" and     |C1|     |C1|     |C1|      |C1|
--   |   into the cold chunks. +--+     +--+     +--+      +--+
--   |                          A        A        A         A    
--   +-----+   Cold Data Chunks-+--------+--------+---------+
--         |                                                
--         V (Top of Stack List)             Warm Data(HD)
--     +---------+                          Chunk Rec 1
--     |Digest 1 |+----------------------->+--------+
--     |---------|              HD Chunk 2 |Entry 1 |
--     |Digest 2 |+------------>+--------+ |Entry 2 |
--     +---------+              |Entry 1 | |   o    |
--     | o o o   |              |Entry 2 | |   o    |
--     |---------|  HD Chunk N  |   o    | |   o    |
--     |Digest N |+->+--------+ |   o    | |Entry n |
--     +---------+   |Entry 1 | |   o    | +--------+
--                   |Entry 2 | |Entry n |
--                   |   o    | +--------+
--                   |   o    |
--                   |   o    |
--                   |Entry n |
--                   +--------+
-- The "Hot Entry Cache" is the true "Top of Stack", holding roughly the
-- top 50 to 100 values.  The next level of storage is found in the first
-- Warm Data Chunk, Record #1 (top position in the directory).  And, since
-- we process stack operations in LIFO order, but manage them physically as
-- a list (append to the end), we basically read the pieces in top down order,
-- but we read the CONTENTS of those pieces backwards.  It is too expensive
-- to "prepend" to a list -- and we are smart enough to figure out how to
-- read an individual page list bottom up (in reverse append order).
--
-- We don't "age" the individual entries out one at a time as the Hot Cache
-- overflows -- we instead take a group at a time (specified by the
-- HotCacheTransferAmount), which opens up a block empty spots. Notice that
-- the transfer amount is a tuneable parameter -- for heavy reads, we would
-- want MORE data in the cache, and for heavy writes we would want less.
--
-- If we generally pick half (100 entries total, and then transfer 50 at
-- a time when the cache fills up), then half the time the insers will affect
-- ONLY the Top (LSO) record -- so we'll have only one Read, One Write 
-- operation for a stack push.  1 out of 50 will have the double read,
-- double write, and 1 out of a thousand (or so) will have additional
-- IO's depending on the state of the Warm/Cold lists.
--
-- NOTE: Design, V3.  For really cold data -- things out beyond 50,000
-- elements, it might make sense to just push those out to a real disk
-- based file.  If we ever need to read the whole stack, we can afford
-- the time and effort to read the file (it is an unlikely event).  The
-- issue here is that we probably have to teach Aerospike how to transfer
-- (and replicate) files as well as records.
--
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- || CHUNKS ||
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- Chunks hold the actual entries. Each entry holds a small amount of 
-- control information and a list 
-- Chunk Design:
-- (*) Map holding
--    -- Control Info
--       -- Digest (the digest that we would use to find this chunk)
--       -- Status (Warm or Cold)
--       -- Entry Max
--       -- Byte Max
--       -- Bytes Used
--       -- Design Version
--       -- Log Info (Log Sequence Number, for when we log updates)
--    -- Entry List (Holds entry and, implicitly, Entry Count)
--
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- || FUNCTION TABLE ||
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- Table of Functions: Used for Transformation and Filter Functions.
-- This is held in UdfFunctionTable.lua.  Look there for details.
-- ======================================================================

-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- LSO Utility Functions
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||

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
end

-- ======================================================================
-- local function lsoSummary( lsoMap ) (DEBUG/Trace Function)
-- ======================================================================
-- For easier debugging and tracing, we will summarize the lsoMap
-- contents -- without printing out the entire thing -- and return it
-- as a string that can be printed.
-- ======================================================================
local function lsoSummary( lsoMap )
  local mod = "LsoStickman";
  local meth = "lsoSummary()";
  info("[ENTER]: <%s:%s>  \n", mod, meth );

  local resultMap             = map();
  resultMap.SUMMARY           = "LSO Summary String";
  resultMap.BinName           = lsoMap.BinName;
  resultMap.NameSpace         = lsoMap.NameSpace;
  resultMap.Set               = lsoMap.Set;
  resultMap.LdrByteMax         = lsoMap.LdrByteMax;
  resultMap.HotItemCount      = lsoMap.HotItemCount;
  resultMap.HotCacheMax       = lsoMap.HotCacheMax;
  resultMap.HotCacheTransfer  = lsoMap.HotCacheTransfer;
  resultMap.WarmItemCount     = lsoMap.WarmItemCount;
  resultMap.WarmChunkCount    = lsoMap.WarmChunkCount;
  resultMap.WarmCacheDirMax      = lsoMap.WarmCacheDirMax;
  resultMap.ColdItemCount     = lsoMap.ColdItemCount;
  resultMap.ColdChunkCount    = lsoMap.ColdChunkCount;
  resultMap.ColdDirCount      = lsoMap.ColdDirCount;
  resultMap.ItemCount         = lsoMap.ItemCount;

  local resultString = tostring( resultMap );
  info("[EXIT]: <%s:%s> resultMap(%s) \n", mod, meth, resultString );
  return resultString;
end -- lsoSummary()
-- ======================================================================

-- ======================================================================
-- ldrChunkSummary( ldrChunk )
-- ======================================================================
-- Print out interesting stats about this LDR Chunk Record
-- ======================================================================
local function  ldrChunkSummary( ldrChunkRecord ) 
  local mod = "LsoStickman";
  local meth = "ldrChunkSummary()";
  info("[ENTER]: <%s:%s>  \n", mod, meth );

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

-- ======================================================================
-- extractTransferList( lsoMap );
-- ======================================================================
-- Extract the oldest N elements (as defined in lsoMap) and create a
-- list that we return.  Also, reset the HotCache to exclude these elements.
-- list.drop( mylist, firstN ).
-- Recall that the oldest element in the list is at index 1, and the
-- newest element is at index N (max).
-- ======================================================================
local function extractTransferList( lsoMap )
  local mod = "LsoStickman";
  local meth = "extractTransferList()";
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

  lsoMap.HotCacheList = newHotCacheList;
  oldHotCacheList = nil;

  info("[EXIT]: <%s:%s> ResultList(%s) \n", mod, meth, tostring(resultList));
  return resultList;
end -- extractTransferList()
-- ======================================================================

-- ======================================================================
-- updateWarmCountStatistics( lsoMap, topWarmChunk );
-- ======================================================================
-- TODO: FInish this method
-- ======================================================================
local function updateWarmCountStatistics( lsoMap, topWarmChunk ) 
  return 0;
end

-- ======================================================================
-- ldrChunkInsert( topWarmChunk, listIndex,  insertList )
-- ======================================================================
-- Insert (append) the LIST of values (overflow from the HotCache) 
-- to this chunk's value list.  We start at the position "listIndex"
-- in "insertList".  Note that this call may be a second (or Nth) call,
-- so we are starting our insert in "insertList" from "listIndex", and
-- not implicitly from "1".
-- Parms:
-- (*) topWarmChunk
-- (*) listIndex
-- (*) insertList
-- Return: Number of items written
-- ======================================================================
local function ldrChunkInsert( topWarmChunkRecord, listIndex, insertList )
  local mod = "LsoStickman";
  local meth = "ldrChunkInsert()";
  info("[ENTER]: <%s:%s> Index(%d) List(%s)\n",
    mod, meth, listIndex, tostring( insertList ) );

  -- TODO: ldrChunkInsert(): Make this work for BINARY mode as well as LIST.
  
  local ldrCtrlMap = topWarmChunkRecord['LdrControlBin'];
  local ldrValueList = topWarmChunkRecord['LdrListBin'];
  local chunkIndexStart = list.size( ldrValueList ) + 1;

  info("[DEBUG]: <%s:%s> Chunk: CTRL(%s) List(%s)\n",
    mod, meth, tostring( ldrCtrlMap ), tostring( ldrValueList ));

  local totalItemsToWrite = list.size( insertList ) + 1 - listIndex;
  local itemSpaceAvailable = ldrCtrlMap.EntryMax - chunkIndexStart;

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
  for i = 0, newItemsStored, 1 do
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
  local mod = "LsoStickman";
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

-- ======================================================================
-- transferWarmDirList()
-- ======================================================================
-- Transfer some amount of the WarmDirList contents (the list of LSO Data
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
local function transferWarmDirList( lsoMap )
  local mod = "LsoStickman";
  local meth = "transferWarmDirList()";
  local rc = 0;
  info("[ENTER]: <%s:%s> lsoMap(%s)\n", mod, meth, tostring(lsoMap) );

  -- We are called ONLY when the Warm Dir List is full -- so we should
  -- not have to check that in production, but during development, we're
  -- going to check that the warm list size is >= warm list max, and
  -- return an error if not.  This test can be removed in production.
  local transferAmount = lsoMap.WarmChunkTransfer;


  info("[DEBUG]: <%s:%s> NOT YET READY TO TRANSFER WARM TO COLD: Map(%s)\n",
    mod, meth, tostring(lsoMap) );

    -- TODO : Finish transferWarmDirList() ASAP.

  info("[EXIT]: <%s:%s> lsoMap(%s) \n", mod, meth, tostring(lsoMap) );
  return rc;
end -- transferWarmDirList()
-- ======================================================================
  
-- ======================================================================
-- warmDirListHasRoom( lsoMap )
-- ======================================================================
-- Look at the Warm list and return 1 if there's room, otherwise return 0.
-- Parms:
-- (*) lsoMap: the map for the LSO Bin
-- Return: Decision: 1=Yes, there is room.   0=No, not enough room.
local function warmDirListHasRoom( lsoMap )
  local mod = "LsoStickman";
  local meth = "warmDirListHasRoom()";
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
end -- warmDirListHasRoom()
-- ======================================================================

-- ======================================================================
-- hotCacheRead( cacheList, peekCount, func, fargs );
-- ======================================================================
-- Return 'count' items from the Hot Cache
local function hotCacheRead( cacheList, count, func, fargs, all)
  local mod = "LsoStickman";
  local meth = "hotCacheRead()";
  local doTheFunk = 0; -- when == 1, call the func(fargs) on the peek item

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
  local countDown = count;
  local cacheListSize = list.size( cacheList );
  -- Get addressability to the Function Table
  local functionTable = require('UdfFunctionTable');

  -- NOTE: By convention, if the initial count is ZERO, then we get them all.
  -- The "all" flag says -- get them all
  local peekValue;
  for c = cacheListSize, 1, -1 do
    -- Apply the UDF to the item, if present, and if result NOT NULL, then
    if doTheFunk == 1 then -- get down, get Funky
      peekValue = functionTable[func]( cacheList[c], fargs );
    else
      peekValue = cacheList[c];
    end
    list.append( resultList, peekValue );
    info("[DEBUG]:<%s:%s>: ResultList Append (%d)[%s]\n",
        mod, meth, c, tostring( peekValue ) );
    countDown = countDown - 1;
    if( countDown <= 0 and all == 0 ) then
      info("[EARLY EXIT]: <%s:%s> result(%s) \n",
        mod, meth, tostring(resultList) );
      return resultList;
    end
  end -- for each item in the cacheList

  info("[EXIT]: <%s:%s> result(%s) \n", mod, meth, tostring(resultList) );
  return resultList;
end -- hotCacheRead()
-- ======================================================================

-- ======================================================================
-- warmDirListChunkCreate( topRec, lsoMap )
-- ======================================================================
-- Create and initialise a new LDR "chunk", load the new digest for that
-- new chunk into the lsoMap (the warm dir list), and return it.
local function   warmDirListChunkCreate( topRec, lsoMap )
  local mod = "LsoStickman";
  local meth = "warmDirListChunkCreate()";
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
  ctrlMap.WarmItemCount = 0;
  -- Assign Control info and List info to the LDR bins
  newLdrChunkRecord['LdrControlBin'] = ctrlMap;
  newLdrChunkRecord['LdrListBin'] = list();

  aerospike:crec_update( topRec, newLdrChunkRecord );

  -- Add our new chunk (the digest) to the WarmDirList
  info("[DEBUG]: <%s:%s> Appending NewChunk(%s) to WarmList(%s)\n",
    mod, meth, tostring(newChunkDigest), tostring(lsoMap.WarmDirList));
  list.append( lsoMap.WarmDirList, newChunkDigest );
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
end --  warmDirListChunkCreate()
-- ======================================================================

-- ======================================================================
-- warmDirListGetTop( topRec, lsoMap )
-- ======================================================================
-- Find the digest of the top of the Warm Dir List, Open that record and
-- return that opened record.
-- ======================================================================
local function   warmDirListGetTop( topRec, lsoMap )
  local mod = "LsoStickman";
  local meth = "warmDirListGetTop()";
  info("[ENTER]: <%s:%s> \n", mod, meth );

  local warmDirList = lsoMap.WarmDirList;
  local stringDigest = tostring( warmDirList[ list.size(warmDirList) ]);
  info("[DEBUG]: <%s:%s> Warm Digest(%s) \n", mod, meth, stringDigest );
  local topWarmChunk = aerospike:crec_open( topRec, stringDigest );

  info("[EXIT]: <%s:%s> result(%s) \n",
    mod, meth, ldrChunkSummary( topWarmChunk ) );
  return topWarmChunk;
end -- warmDirListGetTop()
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
  local mod = "LsoStickman";
  local meth = "ldrChunkRead()";
  local doTheFunk = 0; -- when == 1, call the func(fargs) on the read item

  if (func ~= nil and fargs ~= nil ) then
    doTheFunk = 1;
    info("[ENTER1]: <%s:%s> ReadCount(%d) func(%s) fargs(%s)\n",
      mod, meth, count, func, tostring(fargs) );
  else
    info("[ENTER2]: <%s:%s> ReadCount(%d)\n", 
      mod, meth, count );
  end

  -- Iterate thru the entry list, gathering up items in the result list.
  local countDown = count;
  local chunkMap = ldrChunk['LdrControlBin'];
  local chunkList = ldrChunk['LdrListBin'];
  -- TODO: Make this work for both REGULAR and BINARY Mode
  -- NOTE: We are assuming "LIST MODE" at the moment, not Binary Mode
  local chunkListSize = list.size( chunkList );
  -- Get addressability to the Function Table
  local functionTable = require('UdfFunctionTable');

  -- NOTE: By convention, if the initial count is ZERO, then we get them all.
  -- The "all" flag says -- get them all
  local readResult; -- the item to be read (if it passes the innerUDF)
  local numberRead = 0;
  info("[DEBUG]:<%s:%s>:  warmCacheList [%s]\n",
        mod, meth, tostring( chunkList ) );
  for c = chunkListSize, 1, -1 do
    -- Apply the UDF to the item, if present, and if result NOT NULL, then
    if doTheFunk == 1 then -- get down, get Funky
      readResult = functionTable[func]( chunkList[c], fargs );
    else
      readResult = chunkList[c];
    end
    if( readResult ~= nil ) then
      list.append( resultList, readResult );
      info("[DEBUG]:<%s:%s>: ResultList Append (%d)[%s]\n",
          mod, meth, c, tostring( readResult ) );
      countDown = countDown - 1;
    end
    if( countDown <= 0 and all == 0 ) then
      info("[EARLY EXIT]: <%s:%s> result(%s) \n",
        mod, meth, tostring(resultList) );
      return numberRead;
    end
    numberRead = numberRead + 1;
  end -- for each item in the chunkList

  info("[EXIT]: <%s:%s> NumberRead(%d) ResultList(%s) \n",
    mod, meth, numberRead, tostring( resultList ));
  return numberRead;
end -- ldrChunkRead()
-- ======================================================================


-- ======================================================================
-- warmDirRead(topRec, List, lsoMap, Count, func, fargs, all);
-- ======================================================================
-- Synopsis:
-- Parms:
-- Return: Return the amount read from the Warm Dir List.
-- ======================================================================
local function warmDirRead(topRec, resultList, lsoMap, count,
                           func, fargs, all)
  local mod = "LsoStickman";
  local meth = "warmDirRead()";
  info("[ENTER]: <%s:%s> Count(%d) \n", mod, meth, count );

  -- Process the WarmDirList bottom to top, pulling in each digest in
  -- turn, opening the chunk and reading records (as necessary), until
  -- we've read "count" items.  If the 'all' flag is 1, then read 
  -- everything.
  local warmDirList = lsoMap.WarmDirList;
  -- If we're using the "all" flag, then count just doesn't work.
  if count < 0 then count = 0; end
  local remaining = count;
  local amountRead = 0;
  local itemsRead = 0;
  local dirCount = list.size( warmDirList );
  local ldrChunk;
  local stringDigest;

  info("[DEBUG]: <%s:%s>: DirCount(%d),Top(%s) Reading WarmDirList(%s)(%s):\n",
    mod, meth, dirCount, validateTopRec( topRec, lsoMap ),
    tostring( warmDirList), tostring(warmDirList[1]));

  for dirIndex = dirCount, 0, -1 do
    -- Record Digest MUST be in string form
    stringDigest = tostring(warmDirList[ dirIndex ]);
    ldrChunk = aerospike:crec_open( topRec, stringDigest );
    -- I ASSUME that resultList is passed by reference and we can just
    -- all add to it.
    itemsRead =
      ldrChunkRead( ldrChunk, resultList, remaining, func, fargs, all );
    amountRead = amountRead + itemsRead;
    if itemsRead >= remaining or amountRead >= count then
      info("[Early EXIT]: <%s:%s> amountRead(%d) ResultList(%s) \n",
        mod, meth, amountRead, tostring(resultList));
      return resultList;
    end

    local status = aerospike:crec_close( topRec, ldrChunk );
    info("[DEBUG]: <%s:%s> as:close() status(%s) \n",
      mod, meth, tostring( status ) );

  end -- for each warm Chunk

  info("[EXIT]: <%s:%s> amountRead(%d) ResultList(%s) \n",
      mod, meth, amountRead, tostring(resultList));
  return resultList;
end -- warmDirRead()

-- ======================================================================
-- warmDirInsert()
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
local function warmDirInsert( topRec, lsoMap, insertList )
  local mod = "LsoStickman";
  local meth = "warmDirInsert()";
  local rc = 0;
  info("[ENTER]: <%s:%s> \n", mod, meth );
--info("[ENTER]: <%s:%s> LSO Summary(%s) \n", mod, meth, lsoSummary(lsoMap) );

  info("[DEBUG 0]:WDL(%s)", tostring( lsoMap.WarmDirList ));

  local warmDirList = lsoMap.WarmDirList;
  local topWarmChunk;
  -- Whether we create a new one or open an existing one, we save the current
  -- count and close the record.
  if list.size( warmDirList ) == 0 then
    info("[DEBUG]: <%s:%s> Calling Chunk Create \n", mod, meth );
    topWarmChunk = warmDirListChunkCreate( topRec, lsoMap ); -- create new
  else
    info("[DEBUG]: <%s:%s> Calling Get TOP \n", mod, meth );
    topWarmChunk = warmDirListGetTop( topRec, lsoMap ); -- open existing
  end
  info("[DEBUG]: <%s:%s> Post 'GetTop': LsoMap(%s) \n", 
    mod, meth, tostring( lsoMap ));

  -- We have a warm Chunk -- write as much as we can into it.  If it didn't
  -- all fit -- then we allocate a new chunk and write the rest.
  local totalCount = list.size( insertList );
  info("[DEBUG]: <%s:%s> Calling Chunk Insert: List(%s)\n",
    mod, meth, tostring( insertList ));
  local countWritten = ldrChunkInsert( topWarmChunk, 1, insertList );
  local itemsLeft = totalCount - countWritten;
  if itemsLeft > 0 then
    aerospike:crec_update( topRec, topWarmChunk );
    aerospike:crec_close( topRec, topWarmChunk );
    info("[DEBUG]: <%s:%s> Calling Chunk Create: AGAIN!!\n", mod, meth );
    topWarmChunk = warmDirListChunkCreate( topRec, lsoMap ); -- create new
    -- Unless we've screwed up our parameters -- we should never have to do
    -- this more than once.  This could be a while loop if it had to be, but
    -- that doesn't make sense that we'd need to create multiple new LDRs to
    -- hold just PART of the hot cache.
  info("[DEBUG]: <%s:%s> Calling Chunk Insert: List(%s) AGAIN(%d)\n",
    mod, meth, tostring( insertList ), countWritten + 1);
    countWritten = ldrChunkInsert( topWarmChunk, countWritten+1, insertList );
    if countWritten ~= itemsLeft then
      info("[ERROR!!]: <%s:%s> Second Warm Chunk Write: CW(%d) IL(%d) \n",
        mod, meth, countWritten, itemsLeft );
    end
  end

  -- Update the Warm Count
  if lsoMap.WarmItemCount == nil then
    lsoMap.WarmItemCount = 0;
  end
  local currentWarmCount = lsoMap.WarmItemCount;
  lsoMap.WarmItemCount = (currentWarmCount + totalCount);

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
end -- warmDirInsert

-- ======================================================================
-- local function hotCacheTransfer( lsoMap, insertValue )
-- ======================================================================
-- The job of hotCacheTransfer() is to move part of the HotCache, as
-- specified by HotCacheTransferAmount, to LDRs in the warm Dir List.
-- Here's the logic:
-- (1) If there's room in the WarmDirList, then do the transfer there.
-- (2) If there's insufficient room in the WarmDir List, then make room
--     by transferring some stuff from Warm to Cold, then insert into warm.
local function hotCacheTransfer( topRec, lsoMap )
  local mod = "LsoStickman";
  local meth = "hotCacheTransfer()";
  local rc = 0;
  info("[ENTER]: <%s:%s> LSO Summary() \n", mod, meth );
--info("[ENTER]: <%s:%s> LSO Summary(%s) \n", mod, meth, lsoSummary(lsoMap) );

  -- if no room in the WarmList, then make room (transfer some of the warm
  -- list to the cold list)
  if warmDirListHasRoom( lsoMap ) == 0 then
    transferWarmDirList( lsoMap );
  end

  -- TODO: hotCacheTransfer(): Make this more efficient
  -- Assume "LIST MODE" for now for the Warm Dir digests.
  -- Later, we can pack the digests into the BinaryBin if that is appropriate.
  -- Transfer N items from the hotCache 
  local digestList = list();

  -- Do this the simple (more expensive) way for now:  Build a list of
  -- the items we're moving from the hot cache to the warm dir, then
  -- call insertWarmDir() to find a place for it.
  local transferList = extractTransferList( lsoMap );
  rc = warmDirInsert( topRec, lsoMap, transferList );

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
local function coldPeek(topRec, resultList, cacheList, count, func, fargs )
  local mod = "LsoStickman";
  local meth = "coldPeek()";
  info("[ENTER]: <%s:%s> \n", mod, meth );

  info("[DEBUG]: <%s:%s> COLD STORAGE NOT YET IMPLEMENTED!! \n", mod, meth );

  info("[EXIT]: <%s:%s> \n", mod, meth );
  return resultList;
end -- coldPeek()

-- ======================================================================
-- lsoMapRead(): Get "peekCount" items from the LSO structure.
-- Read each part of the LSO Map: Hot Cache, Warm Cache, Cold Cache
-- Parms:
-- (*) lsoMap: The main LSO structure (stored in the LSO Bin)
-- (*) peekCount: The total count to read (0 means all)
-- (*) Optional inner UDF Function (from the UdfFunctionTable)
-- (*) fargs: Function arguments (list) fed to the inner UDF
-- Return: The Peek resultList -- in LIFO order
-- ======================================================================
local function lsoMapRead( topRec, lsoMap, peekCount, func, fargs )
  local mod = "LsoStickman";
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
  -- We need more -- get more out of the Warm List
  local remainingCount = peekCount - cacheCount;
  info("[DEBUG]: <%s:%s> Calling WarmList Read: Count(%d)\n",
    mod, meth, remainingCount );
  if((all == 1 or remainingCount > 0) and list.size(lsoMap.WarmDirList) > 0 )
    then
    warmList =
      warmDirRead(topRec, resultList, lsoMap, remainingCount, func, fargs, all);
    info("[DEBUG]: <%s:%s> WarmList Read:Cnt(%d) WarmList(%s) resultList(%s)\n",
        mod, meth, remainingCount, tostring(warmList), tostring(resultList));

    warmCount = list.size( warmList );
    if( warmCount >= peekCount and all == 0) then
      return warmList; -- this includes all read so far
    end
  else
    return warmList; -- this includes all read so far
  end -- end if/else more to read

  remainingCount = peekCount - warmCount;
  if((all == 1 or remainingCount > 0 ) and ( lsoMap.ColdListHead  ~= 0 )) then
    local coldList =
      coldPeek(topRecw, armList, lsoMap, remainingCount, func, fargs, all);
    return coldList; -- this includes all read so far
  else
    return warmList; -- something weird happened here.
  end -- if cold data

end -- function lsoMapRead
-- ======================================================================

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
  local mod = "LsoStickman";
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
  local mod = "LsoStickman";
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
  local hotCount = lsoMap.HotItemCount;
  lsoMap.HotItemCount = (hotCount + 1);
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
  local mod = "LsoStickman";
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
  lsoMap.HotItemCount = 0; -- Number of elements in the Top Cache
  lsoMap.HotCacheMax = 100; -- Max Number for the cache -- when we transfer
  lsoMap.HotCacheTransfer = 50; -- How much to Transfer at a time.
  -- Warm Cache Settings
  lsoMap.WarmDirList = list();   -- Define a new list for the Warm Stuff
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

-- ======================================================================

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
  local mod = "LsoStickman";
  local meth = "stackCreate()";

  info("[ENTER]: <<<<<%s:%s>>>>>> \n", mod, meth );

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
    info("[ERROR EXIT]: <%s:%s> LSO BIN(%s) Already Exists\n",
      mod, meth, tostring(lsoBinName) );
    return('LSO_BIN already exists');
  else
    -- Create and initialize the LSO MAP -- the main LSO structure
    local lsoMap = initializeLsoMap( topRec );
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
  local mod = "LsoStickman";
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
      mod, meth, lsoBinName );
    lsoMap = initializeLsoMap( topRec, lsoBinName );
    aerospike:create( topRec );
  end
  
  -- check that our bin is (relatively intact
  local lsoMap = topRec[lsoBinName]; -- The main LSO map
  if lsoMap.Magic ~= "MAGIC" then
    print("MAGIC ERROR \n");
    info("[ERROR EXIT]: <%s:%s> LSO_BIN (%s) Is Corrupted (no magic)\n",
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
-- =======================================================================
function stackPush( topRec, lsoBinName, newValue )
  local mod = "LsoStickman";
  local meth = "stackPush()";
  info("[ENTER]: <%s:%s> LSO BIN(%s) NewValue(%s)\n",
    mod, meth, lsoBinName, tostring( newValue ));
  return localStackPush( topRec, lsoBinName, newValue, nil, nil )
end -- end stackPush()

function stackPushWithUDF( topRec, lsoBinName, newValue, func, fargs )
  local mod = "LsoStickman";
  local meth = "stackPushWithUDF()";
  info("[ENTER]: <%s:%s> LSO BIN(%s) NewValue(%s) Func(%s) Fargs(%s)\n",
    mod, meth, lsoBinName, tostring( newValue ), func, tostring(fargs));
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
-- =======================================================================
-- ======================================================================
local function localStackPeek( topRec, lsoBinName, peekCount, func, fargs )
  local mod = "LsoStickman";
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
    info("[ERROR EXIT]:<%s:%s>:Missing Record. Exit\n", mod, meth );
    return('Base Record Does NOT exist');
  end

  -- Verify that the LSO Structure is there: otherwise, error.
  if( lsoBinName == nil ) then
    info("[ERROR EXIT]: <%s:%s> Bad LSO BIN Parameter\n", mod, meth );
    return('Bad LSO Bin Parameter');
  end
  if( topRec[lsoBinName] == nil ) then
    info("[ERROR EXIT]: <%s:%s> LSO_BIN (%s) DOES NOT Exists\n",
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
-- =======================================================================
function stackPeek( topRec, lsoBinName, peekCount )
  local mod = "LsoStickman";
  local meth = "stackPeek()";
  info("[ENTER]: <%s:%s> LSO BIN(%s) peekCount(%d)\n",
    mod, meth, lsoBinName, peekCount )
  return localStackPeek( topRec, lsoBinName, peekCount, nil, nil )
end -- end stackPeek()

function stackPeekWithUDF( topRec, lsoBinName, peekCount, func, fargs )
  local mod = "LsoStickman";
  local meth = "stackPeekWithUDF()";
  info("[ENTER]: <%s:%s> LSO BIN(%s) peekCount(%d) func(%s) fargs(%s)\n",
    mod, meth, lsoBinName, peekCount, func, tostring(fargs));
  return localStackPeek( topRec, lsoBinName, peekCount, func, fargs );
end -- stackPeekWithUDF()

--
-- ======================================================================
-- ||| UNIT TESTS |||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- Test Individual pieces to verify functionality
-- ======================================================================
-- ======================================================================
-- ======================================================================
-- ======================================================================

-- ======================================================================
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
-- ======================================================================

-- ======================================================================
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
-- ====================== <<<<<   L A R G E >>>> ========================
-- ======================================================================
-- Large Unit Test:
-- Create a large Top Rec, with relatively large chunks whose digests are
-- stored in the WarmDirList.
-- Store values BOTH in the HotCacheList (direct) and the WarmCacheList,
-- which allocates a record and stores the value there (20 times) in
-- a value list.
--
-- ======================================================================
function largeStackCreate(topRec) 
  local binname = "Large LSO BIN" 
  
  info("[ENTER]: LargeStackCreate \n");

  local lsoMap = map();

  local designVersion = 1; -- Not exactly sure how to deal with code versions
  local entryMax = 100; -- items per chunk (TODO: should be a property)
  local bytesMax = 2000; -- bytes per chunk (TODO: should be a property)

  lsoMap.Magic = "MAGIC"; -- we will use this to verify we have a valid map
  lsoMap.BinName = binname;
  lsoMap.NameSpace = "test"; -- arglist[2];
  lsoMap.Set = "set"; -- arglist[3];
  lsoMap.LdrByteMax = 2000; -- arglist[4];
  lsoMap.HotCache = list(); -- List of data entries
  lsoMap.HotItemCount = 0; -- Number of elements in the Top Cache
  lsoMap.HotCacheMax = 8; -- Max Number for the cache -- when we transfer
  lsoMap.HotCacheTransfer = 4; -- How much to Transfer at a time.
  lsoMap.WarmChunkCount = 0; -- Number of Warm Data Record Chunks
  lsoMap.WarmCacheDirMax = 100; -- Number of Warm Data Record Chunks
  lsoMap.WarmChunkTransfer = 10; -- Number of Warm Data Record Chunks
  lsoMap.WarmTopChunkEntryCount = 0; -- Count of entries in top warm chunk
  lsoMap.WarmTopChunkByteCount = 0; -- Count of bytes used in top warm Chunk
  lsoMap.ColdChunkCount = 0; -- Number of Cold Data Record Chunks
  lsoMap.ColdDirCount = 0; -- Number of Cold Data Record DIRECTORY Chunks
  lsoMap.ItemCount = 0;     -- A count of all items in the stack
  lsoMap.WarmCache = list();   -- List of Page Digests
  lsoMap.ColdListHead  = 0;   -- Digest of Directory Page of Digests
  lsoMap.LdrEntryMax = 100;  -- Should be a setable parm
  lsoMap.LdrEntrySize = 20;  -- Must be a setable parm
  lsoMap.LdrByteMax = 2000;  -- Should be a setable parm
  lsoMap.ColdCacheDirMax = 100;  -- Should be a setable parm
  lsoMap.DesignVersion = designVersion;

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
  local mod = "LsoStickman";
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
  ctrlMap.WarmItemCount = 0;
  -- Assign Control info and List info to the LDR bins
  newLdrChunkRecord['LdrControlBin'] = ctrlMap;
  local valueList = list();
  for i = 1, 20, 1 do
    list.append( valueList, newValue );
  end
  newLdrChunkRecord['LdrListBin'] = valueList;

  info("Update New Record ");
  aerospike:crec_update( topRec, newLdrChunkRecord );

  -- Add our new chunk (the digest) to the WarmDirList
  list.append( lsoMap.WarmDirList, newChunkDigest );
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
  local mod = "LsoStickman";
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
  ctrlMap.WarmItemCount = 0;
  -- Assign Control info and List info to the LDR bins
  newLdrChunkRecord['LdrControlBin'] = ctrlMap;
  local valueList = list();
  for i = 1, 20, 1 do
    list.append( valueList, newValue );
  end
  newLdrChunkRecord['LdrListBin'] = valueList;

  info("Update New Record ");
  aerospike:crec_update( topRec, newLdrChunkRecord );

  -- Add our new chunk (the digest) to the WarmDirList
--  list.append( lsoMap.WarmDirList, newChunkDigest );
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
