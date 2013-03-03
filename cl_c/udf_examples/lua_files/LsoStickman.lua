-- Large Stack Object (LSO) Operations
-- Stickman V2.1 -- (Feb 28, 2013)
--
-- ======================================================================
-- Functions Supported
-- (*) stackCreate: Create the LSO structure in the chosen record bin
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
-- LSO Design and Type Comments:
--
-- The LSO value is a new "particle type" that exists ONLY on the server.
-- It is a complex type (it includes infrastructure that is used by
-- server storage), so it can only be viewed or manipulated by Lua and C
-- functions on the server.  It is represented by a Lua MAP object that
-- comprises control information, a directory of records (for "hot data")
-- and a "Cold List Head" ptr to a linked list of directory structures
-- that each point to the records that hold the actual data values.
--
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- Visual Depiction
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- In a user record, the bin holding the Large Stack Object (LSO) is
-- referred to as an "LSO" bin. The overhead of the LSO value is 
-- (*) LSO Control Info (~70 bytes)
-- (*) LSO Hot Directory: List of digests: 10 digests(250 bytes)
-- (*) LSO Cold Directory Head (digest of Head plus count) (30 bytes)
-- (*) Total LSO Record overhead is on the order of 350 bytes
-- NOTES:
-- (*) Record types used in this design:
-- (1) There is the main record that contains the LSO bin (LSO Head)
-- (2) There are Data Chunk Records (both Hot and Cold)
--     ==> Hot and Cold Data Chunk Records have the same format:
--         They both hold User Stack Data.
-- (3) There are Chunk Directory Records (used in the cold list)
-- (*) What connects to what
-- (+) The main record points to:
--     - Hot Data Chunk Records (these records hold stack data)
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
--     | Top Entry Cache||||   <> Each Chunk dir holds about 100 Chunk Ptrs
--     +----------------++++   <> Each Chunk holds about 100 entries
--   +-| LSO Hot Dir List  |   <> Each Chunk dir pts to about 10,000 entries
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
--   |   Hot data ages out     V+--+    V+--+    V+--+     V+--+
--   |   of the "in record"    +--+     +--+     +--+      +--+
--   |   "hot chunks" and      |C1|     |C1|     |C1|      |C1|
--   |   into the cold chunks. +--+     +--+     +--+      +--+
--   |                          A        A        A         A    
--   +-----+   Cold Data Chunks-+--------+--------+---------+
--         |                                                
--         V (Top of Stack List)             Hot Data(HD)
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
-- Except for the "Top Entry Cache", which is the true "Top of Stack",
-- the next level of storage is found in the first Hot Data Chunk,
-- Record #1 (top position in the directory).  And, since we process
-- stack operations in LIFO order, but manage them physically as a list
-- (append to the end), we basically always read backwards when we
-- do Peek operations.
--
-- This is now outdated -- since we're now doing the Top Cache List
-- in place of a "compact mode".
-- NOTE: Design, V2.  We will cache all data in a VALUES LIST until
-- we -- reach a certain number N (e.g. 100), and then at N+1 we will create
-- all of the remaining bins in the record and redistribute the numbers, 
-- then insert the 101th value.  That way we save the initial storage
-- cost of the chunks (and such) for small (or dead) users.
--
-- NOTE: Design, V3.  For really cold data -- things out beyond 50,000
-- elements, it might make sense to just push those out to a real disk
-- based file.  If we ever need to read the whole stack, we can afford
-- the time and effort to read the file (it is an unlikely event).  The
-- issue here is that we probably have to teach Aerospike how to migrate
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
--       -- Status (Hot or Cold)
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
--

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
  local resultMap = map();
  info("[ENTER]: <%s:%s>  \n", mod, meth );

  resultMap.BinName = lsoMap.BinName;
  resultMap.NameSpace = lsoMap.NameSpace;
  resultMap.Set = lsoMap.Set;
  resultMap.ChunkSize = lsoMap.ChunkSize;
  resultMap.TopCacheCount = lsoMap.TopCacheCount;
  resultMap.TopCacheMax = lsoMap.TopCacheMax;
  resultMap.TopCacheMigrate = lsoMap.TopCacheMigrate;
  resultMap.HotChunkCount = lsoMap.HotChunkCount;
  resultMap.ColdChunkCount = lsoMap.ColdChunkCount;
  resultMap.ColdDirCount = lsoMap.ColdDirCount;
  resultMap.ItemCount = lsoMap.ItemCount;

  local resultString = tostring( resultMap );
  info("[EXIT]: <%s:%s> resultMap(%s) \n", mod, meth, resultString );
  return resultString;
end -- lsoSummary()
-- ======================================================================

-- ======================================================================
-- Get (create) a unique bin name given the current counter
-- Probably not needed in Stickman
-- ======================================================================
-- local function getBinName( count )
--   return "DirBin_" .. tostring( count );
-- end
-- ======================================================================
-- 
-- ======================================================================
-- createChunk:
-- ======================================================================
-- Create a new "chunk" (the thing that holds a list
-- of user values), add it to the top of the "Hot List" and increment
-- the control map's total Chunk Count. If necessary, we'll age out the
-- oldest chunk from the hot list and move it to the top of the Cold list.
-- Parms:
-- (*) lsoMap
-- Return: New Bin Number
-- Chunk Design
-- (*) Map holding
--    -- Control Info
--       -- Status (Hot or Cold)
--       -- Entry Max
--       -- Byte Max
--       -- Bytes Used
--       -- Design Version
--       -- Log Info (Log Sequence Number, for when we log updates)
--    -- Entry List (Holds entry and, implicitly, Entry Count)
--
-- ======================================================================
-- local function createChunk( record, lsoMap, digest, dv, status, entryMax)
--   local mod = "LsoStickman";
--   local meth = "createHotChunk()";
--   info("[ENTER]: <%s:%s>LSO(%s) Dig(%s) DV(%s) Stat(%s) \n",
--     mod, meth, tostring(lsoMap), tostring(digest), tostring(dv), status );
-- 
--   local newChunk = map();
--   newChunk.Status = status;
--   newChunk.EntryMax = entryMax;
--   newChunk.BytesMax = 2000;   -- bytesMax;
--   newChunk.BytesUsed = 0;
--   newChunk.DesignVersion = dv;
--   newChunk.Digest = digest;
--   newChunk.LogInfo = 0;
--   newChunk.EntryList = list();
-- 
--   info("[EXIT]: <%s:%s> New Chunk(%s) \n", mod, meth, tostring(newChunk) );
-- 
--   return newChunk;
-- end -- createChunk()

-- ======================================================================
-- chunkSpaceCheck: Check that there's enough space for an insert.
-- ======================================================================
-- Return: True if there's room, otherwise false
-- (*) Map holding
--    -- Control Info
--       -- Status (Hot or Cold)
--       -- Entry Max
--       -- Byte Max
--       -- Bytes Used
--       -- Design Version
--       -- Log Info (Log Sequence Number, for when we log updates)
--    -- Entry List (Holds entry and, implicitly, Entry Count)
--
-- ======================================================================
-- local function chunkSpaceCheck( chunk, newValue )
--   local mod = "LsoStickman";
--   local meth = "createHotChunk()";
--   info("[ENTER]: <%s:%s> chunk(%s) newValue(%s) \n",
--     mod, meth, tostring(chunk), tostring(newValue) );
-- 
--   local result =   list.size( chunk.EntryList ) < chunk.EntryMax;
-- 
--   info("[EXIT]: <%s:%s> result(%s) \n", mod, meth, tostring(result) );
-- 
-- end -- chunkSpaceCheck()
-- ======================================================================

-- ======================================================================
-- lsoInsertNewValue( lsoMap, newValue );
-- ======================================================================
-- Insert a new value (i.e. append) into the top Chunk of an LSO Map.
-- The Chunk has already been tested for sufficient space.
-- Parms:
-- (*) lsoMap
-- (*) The Top Chunk
-- (*) New Value
-- Return: The count of entries in the list
-- ======================================================================
-- local function lsoInsertNewValue( lsoMap, newValue )
--   local mod = "LsoStickman";
--   local meth = "lsoInsertNewValue()";
--   info("[ENTER]: <%s:%s>lsoBin(%s) chunk(%s) newValue(%s)\n",
--     mod, meth, tostring(lsoMap), tostring(chunk), tostring(newValue));
-- 
--   local lsoMap = record[lsoBinName]; -- The main LSO map
--   local hotDir = lsoMap.DirList; -- The HotList Directory
--   local chunkDigest = hotDir[1]; -- The Top of Stack Chunk
--   local chunkRecord = aerospike:record_get( chunk_digest );
--   chunkInsertNewValue( chunkRecord, newValue )
--   aerospike:record_update( chunkRecord )
-- 
--   info("[EXIT]: <%s:%s> lsoMap(%s) Chunk(%s) \n",
--     mod, meth, tostring(lsoMap), tostring(chunk));
-- 
--   return list.size( chunk.EntryList );
-- end -- lsoInsertNewValue
-- ======================================================================
  

-- ======================================================================
-- chunkInsertNewValue( lsoMap, newValue );
-- ======================================================================
-- Given a Chunk Record (now in memory), insert a new value int it.
-- This is a simple operation of appending a value to the end of the
-- chunk's list.
-- The Chunk has already been tested for sufficient space.
-- Parms:
-- (*) lsoMap
-- (*) The Top Chunk
-- (*) New Value
-- Return: The count of entries in the list
-- ======================================================================
-- local function chunkInsertNewValue( chunkRecord, newValue )
--   local mod = "LsoStickman";
--   local meth = "chunkInsertNewValue()";
--   info("[ENTER]: <%s:%s>lsoBin(%s) chunk(%s) newValue(%s)\n",
--     mod, meth, tostring(lsoMap), tostring(chunk), tostring(newValue));
-- 
--   local chunkLsoMap = chunk_record.lsoBin;
--   local chunkEntryList = chunkLsoMap.
--   list.append( chunk.EntryList, newValue );
-- 
--   info("[EXIT]: <%s:%s> lsoMap(%s) Chunk(%s) \n",
--     mod, meth, tostring(lsoMap), tostring(chunk));
-- 
--   return list.size( chunk.EntryList );
-- end -- chunkInsertNewValue()
  
-- ======================================================================
-- isRoomForNewEntry:
-- Parms:
-- (*) lsoMap: the map for the LSO Bin
-- (*) newValue: the new value to be pushed on the stack
-- ======================================================================
-- local function isRoomForNewEntry( lsoMap, newValue )
--   return 0
-- end -- isRoomForNewEntry()
-- ======================================================================

-- ======================================================================
-- insertNewValue():
-- ======================================================================
-- Parms:
-- (*) lsoMap: the map for the LSO Bin
-- (*) newValue: the new value to be pushed on the stack
-- local function insertNewValue( lsoMap, newValue )
--   return 0
-- end -- insertNewValue()
-- ======================================================================
--
-- ======================================================================
-- createNewChunkInsertValue( lsoMap, newValue );
-- ======================================================================
-- Parms:
-- (*) lsoMap: the map for the LSO Bin
-- (*) newValue: the new value to be pushed on the stack
-- local function createNewChunkInsertValue( lsoMap, newValue )
--   return 0
-- end -- createNewChunkInsertValue()
-- ======================================================================


-- ======================================================================
-- isRoomForNewHotChunk( lsoMap, newChunk )
-- ======================================================================
-- Parms:
-- (*) lsoMap: the map for the LSO Bin
-- (*) newValue: the new value to be pushed on the stack
-- local function isRoomForNewHotChunk( lsoMap, newChunk )
--   return 0
-- end -- isRoomForNewHotChunk( lsoMap, newChunk )
-- ======================================================================

-- ======================================================================
-- hotDirInsert( lsoMap, newChunk );
-- ======================================================================
-- Parms:
-- (*) lsoMap: the map for the LSO Bin
-- (*) newValue: the new value to be pushed on the stack
-- local function hotDirInsert( lsoMap, newChunk )
--   return 0;
-- end -- hotDirInsert( lsoMap, newChunk )
-- ======================================================================
--
-- ======================================================================
-- migrateWarmChunk( lsoMap )
-- ======================================================================
-- Parms:
-- (*) lsoMap: the map for the LSO Bin
-- (*) newValue: the new value to be pushed on the stack
-- local function migrateWarmChunk( lsoMap )
--   return 0;
-- end -- migrateWarmChunk()
-- ======================================================================


-- ======================================================================
-- cachePeek( cacheList, peekCount, func, fargs );
-- ======================================================================
-- Return 'count' items from the Top Cache
-- ======================================================================
local function cachePeek( cacheList, count, func, fargs, all)
  local mod = "LsoStickman";
  local meth = "cachePeek()";
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
end -- cachePeek()
-- ======================================================================


-- ======================================================================
-- hotPeek( cacheList, peekCount, func, fargs );
-- ======================================================================
-- Return 'count' items from the Hot Dir List -- adding to the result
-- list passed in.
-- ======================================================================
local function hotPeek(resultList, cacheList, count, func, fargs )
  return resultList;
end -- hotPeek()

-- ======================================================================
-- local function migrateTopCache( lsoMap, insertValue )
-- ======================================================================
-- The job if migrateTopCache() is to move half of the TopCache to warm
-- storage (i.e. the Hot Directory List).
-- Here are the cases:
-- (1) There is no HotDirList (yet) or there is room for a new data chunk
--     - Make room, if needed
--     - Allocate a data chunk ==> aerospike:create_record()
--     - Copy the data into the new aerospike record
--     - Store the record digest into the HotDirList
-- (2) There is no room in HotDirList, so part of the HotDirList must
--     be migrated to the ColdDirList.  Call migrateHotDirList().
-- ======================================================================
-- Design Notes:
--
--
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- || CHUNKS (LSO Data Records)
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- Chunks hold the actual entries. Each LSO Data Record (LDR) holds a small
-- amount of control information and a list.  A LDR will have three bins:
-- (1) The Control Bin (a Map with the various control data)
-- (2) The List bin -- where we hold "list entries"
-- (3) The Binary Bin -- where we hold compacted binary entries (just the
--     as bytes values)
-- (*) Note that ONLY ONE of the two content bins will be used.  We will be
--     in either LIST MODE (bin 2) or BINARY MODE (bin 3)
-- ==> Control Bin Contents (a Map)
--  -- Page Type (Hot Data, Code Data or Dir)
--  -- Page Mode (List data or Binary Data)
--  -- Digest (the digest that we would use to find this chunk)
--  -- Entry Max
--  -- Byte Max
--  -- Bytes Used
--  -- Design Version
--  -- Log Info (Log Sequence Number, for when we log updates)
--  ==> List Bin Contents
--  ==> Binary Bin Contents
--
--    -- Entry List (Holds entry and, implicitly, Entry Count)
--
-- ======================================================================
local function migrateTopCache( lso_head_record, lsoMap )
  local mod = "LsoStickman";
  local meth = "migrateTopCache()";
  local rc = 0;
  info("[ENTER]: <%s:%s> LSO Summary(%s) \n", mod, meth, lsoSummary(lsoMap) );

  -- if no room in the HotList, then make room
  if not hotDirListHasRoom( lsoMap ) then
    migrateHotDirList( lsoMap );
  end

  -- Create a new Data Chunk, copy the older half (or, more specifically copy
  -- "migrate amount") of the Hot List into it, and then insert it into
  -- the Hot List Directory.
  -- Aerospike Calls:
  -- newChunkRec = aerospike:crec_create( rec )
  -- digest = newChunkRec.digest
  -- chkrec = aerospike:crec_open( rec )
  -- status = aerospike:crec_close( rec, chkrec )
  -- status = aerospike:crec_update( rec, chkrec )
  local lsoDataRecord = aerospike:crec_create( lso_head_record );
  ldrCtrl = map();
  ldrCtrl.PageType = "HotData";
  ldrCtrl.PageMode = "List"; -- this will change
  ldrCtrl.Digest = lsoDataRecord.digest;
  lsoDataRecord.ControlBin = ldrCtrl;
  local digest = lsoDataRecord.digest;

  info("[DEBUG]: <%s:%s> New LSO ldrCtrl (%s) \n", mod, meth, ldrCtrl );


  info("[EXIT]: <%s:%s> result(%d) \n", mod, meth, rc );
  return 0;
end -- migrateTopCache()
-- ======================================================================

-- ======================================================================
-- coldPeek( cacheList, peekCount, func, fargs );
-- ======================================================================
-- Return 'count' items from the Cold Dir List -- adding to the result
-- list passed in.
-- ======================================================================
local function coldPeek(resultList, cacheList, count, func, fargs )
  return resultList;
end -- coldPeek()

-- ======================================================================
-- mapPeek(): Get "peekCount" items from the LSO structure.
-- mapPeek( lsoMap, peekCount, func, fargs );
-- Return resultList -- in LIFO order
-- ======================================================================
local function mapPeek( lsoMap, peekCount, func, fargs )
  local mod = "LsoStickman";
  local meth = "mapPeek()";
  info("[ENTER0]: <%s:%s> \n", mod, meth );

  if (func ~= nil and fargs ~= nil ) then
    info("[ENTER1]: <%s:%s> Count(%d) func(%s) fargs(%s)\n",
      mod, meth, peekCount, func, tostring(fargs) );
  else
    info("[ENTER2]: <%s:%s> PeekCount(%d)\n", 
      mod, meth, peekCount );
  end

  local all;
  if peekCount == 0 then all = 1 else all = 0 end

  -- Fetch from the Cache, then the Hot List, then the Cold List.
  -- Each time we decrement the count and add to the resultlist.
  local cacheList = lsoMap.TopCache;
  local resultList = cachePeek( cacheList, peekCount, func, fargs, all);
  local cacheCount = list.size( resultList );

  -- If the cache had all that we need, then done.  Return list.
  if( cacheCount >= peekCount ) then
    return resultList;
  end
  -- We need more -- get more out of the Hot List
  local remainingCount = peekCount - cacheCount;
  local hotList = hotPeek(resultList, lsoMap, remainingCount, func, fargs, all);
  local hotCount = list.size( hotList );
  if( hotCount >= peekCount ) then
    return hotList;
  end

  remainingCount = peekCount - hotCount;
  local coldList = coldPeek(hotList, lsoMap, remainingCount, func, fargs, all);

  return coldList;

end -- function mapPeek
-- ======================================================================

-- ======================================================================
-- hasRoomInCacheForNewEntry( lsoMap, insertValue )
-- ======================================================================
-- return 1 if there's room, otherwise return 0
-- Later on we might consider just doing the insert here when there's room.
-- Parms:
-- (*) lsoMap: the map for the LSO Bin
-- (*) insertValue: the new value to be pushed on the stack
local function hasRoomInCacheForNewEntry( lsoMap, insertValue )
  local cacheLimit = lsoMap.TopCacheMax;
  local cacheList = lsoMap.TopCache;
  if list.size( cacheList ) >= cacheLimit then
    return 0
  else
    return 1
  end
end -- hasRoomInCacheForNewEntry()
-- ======================================================================

--
-- ======================================================================
-- topCacheInsert( lsoMap, newValue )
-- ======================================================================
-- Insert a value at the end of the Top Cache List.  The caller has 
-- already verified that space exists, so we can blindly do the 
-- insert.
-- Parms:
-- (*) lsoMap: the map for the LSO Bin
-- (*) newValue: the new value to be pushed on the stack
local function topCacheInsert( lsoMap, insertValue )
  local mod = "LsoStickman";
  local meth = "topCacheInsert()";
  info("[ENTER]: <%s:%s> : Insert Value(%s)\n",
    mod, meth, tostring(insertValue) );

  local topCacheList = lsoMap.TopCache;
  list.append( topCacheList, insertValue );
  local itemCount = lsoMap.ItemCount;
  lsoMap.ItemCount = (itemCount + 1);
  return 0;  -- all is well

end -- topCacheInsert()
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
-- (*) Hot Chunk Count: Number of Hot Chunk Data Records
-- (*) Cold Chunk Count: Number of Cold Chunk Data Records
-- (*) Item Count (will NOT be tracked in Stoneman)
-- (*) The List of Hot Chunks of data (each Chunk is a list)
-- (*) The Head of the Cold Data Directory
-- (*) Storage Mode (Compact or Regular) (0 for compact, 1 for regular)
-- (*) Compact Item List
--
-- The LSO starts out in "Compact" mode, which allows the first 100 (or so)
-- entries to be held directly in the record (in the compact item list).
-- Once the count exceeds the short list, it overflows into the regular
-- mechanism (shown above).  In fact, we may want to keep the top of stack
-- in the record itself -- and flow the records thru that.  If we 
-- accumulate up to 100 records in the "record cache" and then when the
-- cache is full we flow out 1/2 (50) to the Data Chunk Records, then we
-- save a double read for every 49/50 operations.
--
-- +=====================================================================+
-- | Usr Bin 1 | Usr Bin 2 | o o o | Usr Bin N | LSO Bin 1 | User Bin M  |
-- +=====================================================================+
-- Parms (inside arglist)
-- (1) LsoBinName
-- (2) Namespace (just one, for now)
-- (3) Set
-- (4) ChunkSize
-- (5) Design Version
function stackCreate( record, arglist )
  local mod = "LsoStickman";
  local meth = "stackCreate()";
  info("[ENTER]: <%s:%s> \n", mod, meth );

  local lsoBinName = arglist[1];
  if( lsoBinName == nil ) then
    info("[ERROR EXIT]: <%s:%s> Bad LSO BIN Parameter\n", mod, meth );
    return('Bad LSO Bin Parameter');
  end

  info("[DEBUG]: <%s:%s> bin(%s) argList(%s)\n",
    mod, meth, lsoBinName, tostring(arglist) );

  -- Check to see if LSO Structure (or anything) is already there,
  -- and if so, error
  if( record[lsoBinName] ~= nil ) then
    info("[ERROR EXIT]: <%s:%s> LSO BIN(%s) Already Exists\n",
      mod, meth, lsoBinName );
    return('LSO_BIN already exists');
  end

  info("[DEBUG]: <%s:%s> : Initialize LSO Map\n", mod, meth );

  -- Define our control information and put it in the record's control bin
  -- Notice that in the next version, Top of Stack (TOS) will not be at the
  -- end, but will instead move and will have a TOS ptr var in the ctrl map.
  local lsoMap = map();
  -- local digest = aerospike:create_digest( record );
  local designVersion = 1; -- Not exactly sure how to deal with code versions
  local entryMax = 100; -- items per chunk (TODO: should be a property)
  local bytesMax = 2000; -- bytes per chunk (TODO: should be a property)

  lsoMap.Magic = "MAGIC"; -- we will use this to verify we have a valid map
  lsoMap.BinName = arglist[1];
  lsoMap.NameSpace = arglist[2];
  lsoMap.Set = arglist[3];
  lsoMap.ChunkSize = arglist[4];
  lsoMap.TopCache = list();
  lsoMap.TopCacheCount = 0; -- Number of elements in the Top Cache
  lsoMap.TopCacheMax = 10; -- Max Number for the cache -- when we migrate
  lsoMap.TopCacheMigrate = 5; -- How much to migrate at a time.
  lsoMap.HotChunkCount = 0; -- Number of Hot Data Record Chunks
  lsoMap.ColdChunkCount = 0; -- Number of Cold Data Record Chunks
  lsoMap.ColdDirCount = 0; -- Number of Cold Data Record DIRECTORY Chunks
  lsoMap.ItemCount = 0;     -- A count of all items in the stack
  lsoMap.DirList = list();   -- Define a new list for the Hot Stuff
  lsoMap.ColdListHead  = 0;   -- Nothing here yet
  lsoMap.DesignVersion = designVersion;

  info("[DEBUG]: <%s:%s> : CTRL Map after Init(%s)\n",
    mod, meth , tostring(lsoMap));

  -- Note:  We no longer create chunks initially -- we wait until the
  -- LSO has accumulated some number of entries, and after that we
  -- migrate a block of entries into chunk storage
  -- local newChunk =
    -- createChunk( record, lsoMap, digest, designVersion, "Hot", entryMax)

  -- Put our new map in the record, then store the record.
  record[lsoBinName] = lsoMap;

  info("[DEBUG]:<%s:%s>:Dir Map after Init(%s)\n", mod,meth,tostring(dirMap));

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
end -- function stackCreate( record, namespace, set )

-- ======================================================================
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- || local stackPush (Stickman V2)
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- This function does the work of both calls -- with and without inner UDF.
--
-- Push a value onto the stack. There are different cases, with different
-- levels of complexity:
-- If there's room in the record itself (Top Cache), then do that.
--   Case 0: If room in top cache, then insert there. Done.
-- If there's room to insert into the top chunk (a Hot Chunk)
--   Case 1: Call chunkInsertNewValue()
-- else
--   Case 2: Allocate a new Chunk
--   If there's room in the hot directory:
--     Case 2.1: Add new chunk to Hot Directory
--     Case 1: Call chunkInsertNewValue()
--   else
--     Case 2.2: Migrate Coldest Hot Chunk to warmest Cold Chunk
--     If There's room in the Cold Stack
--       Case 3.1: Enter Digest in top of Cold Stack
--       Append Warm Chunk digest to end of Cold Stack Directory (new top)
--     else
--       Case 3.2: Make Room at top of Cold Stack
--       Cold stack dir is full, allocate a new Cold Stack Dir
--         --> New Record for Cold Dir, with new digest
--       Point "Next Dir" link to old cold head
--       Point main Record Cold Head (digest) to this new cold stack dir
--       Enter new chunk in cold head dir list.
--
-- Parms:
-- (*) record:
-- (*) lsoBinName:
-- (*) newValue:
-- (*) func:
-- (*) fargs:
-- =======================================================================
local function localStackPush( record, lsoBinName, newValue, func, fargs )
  local mod = "LsoStickman";
  local meth = "localStackPush()";

  if (func ~= nil and fargs ~= nil ) then
    info("[ENTER1]: <%s:%s> LSO BIN(%s) NewValue(%s) func(%s) fargs(%s)\n",
      mod, meth, lsoBinName, tostring( newValue ), func, tostring(fargs) );
  else
    info("[ENTER2]: <%s:%s> LSO BIN(%s) NewValue(%s)\n",
      mod, meth, lsoBinName, tostring( newValue ));
  end

  if( not aerospike:exists( record ) ) then
    print("ERROR ON RECORD EXISTS\n");
    info("[ERROR EXIT]:<%s:%s>:Missing Record. Exit\n", mod, meth );
    return('Base Record Does NOT exist');
  end

  -- Verify that the LSO Structure is there: otherwise, error.
  if( lsoBinName == nil ) then
    info("[ERROR EXIT]: <%s:%s> Bad LSO BIN Parameter\n", mod, meth );
    return('Bad LSO Bin Parameter');
  end
  if( record[lsoBinName] == nil ) then
    info("[ERROR EXIT]: <%s:%s> LSO BIN (%s) DOES NOT Exists\n",
      mod, meth, lsoBinName );
    return('LSO BIN Does NOT exist');
  end
  
  -- check that our bin is (relatively intact
  local lsoMap = record[lsoBinName]; -- The main LSO map
  if lsoMap.Magic ~= "MAGIC" then
    print("MAGIC ERROR \n");
    info("[ERROR EXIT]: <%s:%s> LSO_BIN (%s) Is Corrupted (no magic)\n",
      mod, meth, lsoBinName );
    return('LSO_BIN Is Corrupted');
  end

  -- Now, it looks like we're ready to insert.  If there is an inner UDF
  -- to apply, do it now.
  local insertValue = newValue;
  if( func ~= nil and fargs ~= nil) then
    info("[DEBUG]: <%s:%s> Applying UDF (%s) \n", mod, meth, func );
    --
    -- insertValue = UDF TRANSFORMATION Function call ....
    --
  end

  -- If we have room, do the simple cache insert.  If we don't have
  -- room, then make room -- migrate half the cache out to the hot list.
  -- That may, in turn, have to make room by moving some items to the
  -- cold list.
  if not hasRoomInCacheForNewEntry( lsoMap, insertValue ) then
    migrateTopCache( record, lsoMap );
  end
  topCacheInsert( lsoMap, insertValue );

--  elseif hasRoomForNewEntry( lsoMap, newValue ) then
--    -- Case 1:  Just do the insert: Top Chunk, simple append
--    hotDirInsertNewValue( lsoMap, newValue );
--  else
--    -- Case 2: Allocate new Chunk, enter value, find a place for new chunk
--    newChunk = newChunkInsert( lsoMap, newValue );
--    if  isRoomForNewHotChunk( lsoMap, newChunk ) then
--      hotDirInsert( lsoMap, newChunk );
--    else
--      -- Case 3: Dealing with the Cold Directory
--      -- Make room in the Hot Dir (Make coldest hot the new hotest cold)
--      migrateWarmChunk( lsoMap )
--      hotDirInsert( lsoMap, newChunk );
--    end
--  end

  -- Note that we will (likely) NOT track the exact item count in the
  -- record in the FINAL VERSION, as that would trigger a new record
  -- update for EACH Value insert, in addition to the record update for
  -- the record actually holding the new value.  We want to keep this to
  -- just one record insert rather than two.
--  local itemCount = lsoMap['ItemCount'];
--  itemCount = itemCount + 1;
--  lsoMap['ItemCount'] = itemCount;

  -- Not sure if this is needed -- but it seems to be.
  record[lsoBinName] = lsoMap;

  -- All done, store the record
  local rc = -99; -- Use Odd starting Num: so that we know it got changed
  info("[DEBUG]:<%s:%s>:Update Record\n", mod, meth );
  rc = aerospike:update( record );

  info("[EXIT]: <%s:%s> : Done.  RC(%d)\n", mod, meth, rc );
  return rc
end -- function stackPush()

-- =======================================================================
-- Stack Push -- with and without inner UDFs
-- These are the globally visible calls -- that call the local UDF to do
-- all of the work.
-- =======================================================================
function stackPush( record, lsoBinName, newValue )
  local mod = "LsoStickman";
  local meth = "stackPush()";
  info("[ENTER]: <%s:%s> LSO BIN(%s) NewValue(%s)\n",
    mod, meth, lsoBinName, tostring( newValue ));
  return localStackPush( record, lsoBinName, newValue, nil, nil )
end -- end stackPush()

function stackPushWithUDF( record, lsoBinName, newValue, func, fargs )
  local mod = "LsoStickman";
  local meth = "stackPushWithUDF()";
  info("[ENTER]: <%s:%s> LSO BIN(%s) NewValue(%s) Func(%s) Fargs(%s)\n",
    mod, meth, lsoBinName, tostring( newValue ), func, tostring(fargs));
  return localStackPush( record, lsoBinName, newValue, func, fargs );
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
-- -> Just the TopCache
-- -> The TopCache and the Hot List
-- -> The TopCache, Hot list and Cold list
-- Since our pieces are basically in Stack order, we start at the top
-- (the TopCache), then the HotList, then the Cold List.  We just
-- keep going until we've seen "PeekCount" entries.  The only trick is that
-- we have to read our blocks backwards.  Our blocks/lists are in stack 
-- order, but the data inside the blocks are in append order.
--
-- Parms:
-- (*) record:
-- (*) lsoBinName:
-- (*) peekCount:
-- (*) func:
-- (*) fargs:
-- =======================================================================
-- ======================================================================
local function localStackPeek( record, lsoBinName, peekCount, func, fargs )
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

  if( not aerospike:exists( record ) ) then
    info("[ERROR EXIT]:<%s:%s>:Missing Record. Exit\n", mod, meth );
    return('Base Record Does NOT exist');
  end

  -- Verify that the LSO Structure is there: otherwise, error.
  if( lsoBinName == nil ) then
    info("[ERROR EXIT]: <%s:%s> Bad LSO BIN Parameter\n", mod, meth );
    return('Bad LSO Bin Parameter');
  end
  if( record[lsoBinName] == nil ) then
    info("[ERROR EXIT]: <%s:%s> LSO_BIN (%s) DOES NOT Exists\n",
      mod, meth, lsoBinName );
    return('LSO_BIN Does NOT exist');
  end
  
  -- check that our bin is (mostly) there
  local lsoMap = record[lsoBinName]; -- The main LSO map
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
  local resultList = mapPeek( lsoMap, peekCount, func, fargs );

  info("[EXIT]: <%s:%s>: PeekCount(%d) ResultList(%s)\n",
    mod, meth, peekCount, tostring(resultList));

  return resultList;
end -- function stackPeek() 

-- =======================================================================
-- Stack Peek -- with and without inner UDFs
-- These are the globally visible calls -- that call the local UDF to do
-- all of the work.
-- =======================================================================
function stackPeek( record, lsoBinName, peekCount )
  local mod = "LsoStickman";
  local meth = "stackPeek()";
  info("[ENTER]: <%s:%s> LSO BIN(%s) peekCount(%d)\n",
    mod, meth, lsoBinName, peekCount )
  return localStackPeek( record, lsoBinName, peekCount, nil, nil )
end -- end StackPush()

function stackPeekWithUDF( record, lsoBinName, peekCount, func, fargs )
  local mod = "LsoStickman";
  local meth = "stackPeekWithUDF()";
  info("[ENTER]: <%s:%s> LSO BIN(%s) peekCount(%d) func(%s) fargs(%s)\n",
    mod, meth, lsoBinName, peekCount, func, tostring(fargs));
  return localStackPeek( record, lsoBinName, peekCount, func, fargs );
end -- StackPushWithUDF()

-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
