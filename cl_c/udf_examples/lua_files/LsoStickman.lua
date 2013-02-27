-- Large Stack Object (LSO) Operations
-- Stickman V1.5 -- (Feb 25, 2013)
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
-- +-----+-----+-----+-----+----------------------------------------+
-- |User |User |o o o|LSO  |                                        |
-- |Bin 1|Bin 2|o o o|Bin 1|                                        |
-- +-----+-----+-----+-----+----------------------------------------+
--                  /       \                                       
--     LSO Map     /                                        
--     +-------------------+                                 
--     | LSO Control Info  |   <> Each Chunk dir holds about 100 Chunk Ptrs
--     +-------------------+   <> Each Chunk holds about 100 entries
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
--   |                                                           
--   +-----+                                                
--         |                                                
--         V (Top of Stack List)                                  
--     +---------+                            Chunk 1
--     |Digest 1 |+------------------------> +---------+
--     |---------|                Chunk 2    |Entry 1  |
--     |Digest 2 |+------------> +---------+ |Entry 2  |
--     +---------+               |Entry 1  | |         |
--     | o o o   |               |Entry 2  | |   o     |
--     |---------|    Chunk N    |         | |   o     |
--     |Digest N |+->+---------+ |   o     | |   o     |
--     +---------+   |Entry 1  | |   o     | |         |
--                   |Entry 2  | |   o     | |Entry n  |
--                   |         | |         | +---------+
--                   |   o     | |Entry n  |
--                   |   o     | +---------+
--                   |   o     |
--                   |         |
--                   |Entry n  |
--                   +---------+
--
-- NOTE: Design, V2.  We will cache all data in a VALUES LIST until
-- we -- reach a certain number N (e.g. 100), and then at N+1 we will create
-- all of the remaining bins in the record and redistribute the numbers, 
-- then insert the 101th value.  That way we save the initial storage
-- cost of the chunks (and such) for small (or dead) users.
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
--
-- In order to pass functions as parameters in Lua (from C), we don't have
-- the ability to officially pass a function, but we can pass a name from
-- the client that maps to a function here on the server.
--
-- The functions have to be created by DB administrators -- they are not
-- something that any user can create an upload.
--
-- From the main function table "functionTable", we can call any of the
-- functions defined here by passing its name and the associated arglist
-- that is supplied by the user.  For example, in stackPeekWithUDF, we
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
functionTable = {}

-- ======================================================================
-- Sample Filter function to test user entry 
-- Parms (encased in arglist)
-- (1) Entry List
-- ======================================================================
local function functionTable.transformFilter1( argList )
  local mod = "LsoStickman";
  local meth = "transformFilter1()";
  local resultList = list();
  local entryList = arglist[1]; 
  local entry = 0;
  info("[ENTER]: <%s:%s> EntryList(%s) \n", mod, meth, tostring(entryList));

  -- change EVERY entry that is > 200 to 0.
  for i = 1, list.size( entryList ) do
      info("[DEBUG]: <%s:%s> EntryList[%d](%s) \n",
        mod, meth, i, tostring(entryList[i]));
    if entryList[i] > 200 then 
      info("[DEBUG]: <%s:%s> Setting Entry to ZERO \n", mod, meth );
      entry = 0;
    else 
      info("[DEBUG]: <%s:%s> Setting Entry to entryList(%s) \n", mod, meth, tostring(entryList[i]));
      entry = entryList[i];
    end
    list.append( resultList, entry );
    info("[DEBUG]: <%s:%s> List Append: Result:(%s) Entry(%s)\n",
      mod, meth, tostring(resultList[i]), tostring( entry));
  end

  info("[EXIT]: <%s:%s> Return with ResultList(%s) \n",
    mod, meth, tostring(resultList));
  return resultList;
end

-- ======================================================================
-- Function Range Filter: Performs a range query on one or more of the
-- entries in the list.
-- Parms (encased in arglist)
-- (1) Entry List
-- (2) Relation List {{op, val}, {op, val} ... }
-- ======================================================================
local function functionTable.rangeFilter( arglist )
  local mod = "LsoStickman";
  local meth = "rangeFilter()";
end

-- ======================================================================
-- Function compressTransform1: Compress the multi-part list into a single
-- as_bytes value.  There are various tables stored in this module that
-- describe various structures, and the table index picks the compression
-- structure that we'll use.
-- Parms (encased in arglist)
-- (1) Entry List
-- (2) Compression Field Parameters Table Index
-- ======================================================================
local function functionTable.compressTransform1( arglist )
  local mod = "LsoStickman";
  local meth = "compress()";
end

-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- LSO Utility Functions
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
--
-- ======================================================================
-- Get (create) a unique bin name given the current counter
-- ======================================================================
local function getBinName( count )
return "DirBin_" .. tostring( count );
end

-- ======================================================================
-- createChunk: Create a new "chunk" (the thing that holds a list
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
local function createChunk( record, lsoMap, digest, dv, status, entryMax)
  local mod = "LsoStickman";
  local meth = "createHotChunk()";
  info("[ENTER]: <%s:%s>LSO(%s) Dig(%s) DV(%s) Stat(%s) \n",
    mod, meth, tostring(lsoMap), tostring(digest), dv, status );

  local newChunk = map();
  newChunk.Status = status;
  newChunk.EntryMax = entryMax;
  newChunk.BytesMax = bytesMax;
  newChunk.BytesUsed = 0;
  newChunk.DesignVersion = dv;
  newChunk.Digest = digest;
  newChunk.LogInfo = 0;
  newChunk.EntryList = list();

  info("[EXIT]: <%s:%s> New Chunk(%s) \n", mod, meth, tostring(newChunk) );

  return newChunk;
end -- end createChunk

-- ======================================================================
-- chunkSpaceCheck: Check that there's enough space for an insert.
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
local function chunkSpaceCheck( chunk, newValue )
  local mod = "LsoStickman";
  local meth = "createHotChunk()";
  info("[ENTER]: <%s:%s> chunk(%s) newValue(%s) \n",
    mod, meth, tostring(chunk), tostring(newValue) );

  local result =   list.size( chunk.EntryList ) < chunk.EntryMax;

  info("[EXIT]: <%s:%s> result(%s) \n", mod, meth, tostring(result) );

end -- chunkSpaceCheck()

-- ======================================================================
-- lsoInsertNewValue( lsoMap, newValue );
-- Insert a new value (i.e. append) into the top Chunk of an LSO Map.
-- The Chunk has already been tested for sufficient space.
-- Parms:
-- (*) lsoMap
-- (*) The Top Chunk
-- (*) New Value
-- Return: The count of entries in the list
-- ======================================================================
local function lsoInsertNewValue( lsoMap, newValue )
  local mod = "LsoStickman";
  local meth = "lsoInsertNewValue()";
  info("[ENTER]: <%s:%s>lsoBin(%s) chunk(%s) newValue(%s)\n",
    mod, meth, tostring(lsoMap), tostring(chunk), tostring(newValue));

  local lsoMap = record[lsoBinName]; -- The main LSO map
  local hotDir = lsoMap.DirList; -- The HotList Directory
  local chunkDigest = hotDir[1]; -- The Top of Stack Chunk
  local chunkRecord = aerospike:record_get( chunk_digest );
  chunkInsertNewValue( chunkRecord, newValue )
  aerospike:record_update( chunkRecord )

  info("[EXIT]: <%s:%s> lsoMap(%s) Chunk(%s) \n",
    mod, meth, tostring(lsoMap), tostring(chunk));

  return list.size( chunk.EntryList );
end -- lsoInsertNewValue
  

-- ======================================================================
-- chunkInsertNewValue( lsoMap, newValue );
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
local function chunkInsertNewValue( chunkRecord, newValue )
  local mod = "LsoStickman";
  local meth = "chunkInsertNewValue()";
  info("[ENTER]: <%s:%s>lsoBin(%s) chunk(%s) newValue(%s)\n",
    mod, meth, tostring(lsoMap), tostring(chunk), tostring(newValue));

  local chunkLsoMap = chunk_record.LsoBin;
  local chunkEntryList = chunkLsoMap.
  list.append( chunk.EntryList, newValue );

  info("[EXIT]: <%s:%s> lsoMap(%s) Chunk(%s) \n",
    mod, meth, tostring(lsoMap), tostring(chunk));

  return list.size( chunk.EntryList );
end -- chunkInsertNewValue()
  
-- ======================================================================
-- isRoomForNewEntry:
-- Parms:
-- (*) lsoMap: the map for the LSO Bin
-- (*) newValue: the new value to be pushed on the stack
-- 
-- ======================================================================
local function isRoomForNewEntry( lsoMap, newValue )
end -- isRoomForNewEntry()

-- ======================================================================
-- ======================================================================
-- ======================================================================
-- isRoomForNewEntry:
-- Parms:
-- (*) lsoMap: the map for the LSO Bin
-- (*) newValue: the new value to be pushed on the stack
insertNewValue( lsoMap, newValue );
-- ======================================================================
-- ======================================================================
-- ======================================================================
-- isRoomForNewEntry:
-- Parms:
-- (*) lsoMap: the map for the LSO Bin
-- (*) newValue: the new value to be pushed on the stack
newChunk = createNewChunkInsertValue( lsoMap, newValue );
-- ======================================================================
-- ======================================================================
isRoomForNewHotChunk( lsoMap, newChunk )
-- ======================================================================
-- Parms:
-- (*) lsoMap: the map for the LSO Bin
-- (*) newValue: the new value to be pushed on the stack
isRoomForNewHotChunk( lsoMap, newChunk )
isRoomForNewHotChunk( lsoMap, newChunk )
-- ======================================================================
-- ======================================================================
hotDirInsert( lsoMap, newChunk );
-- ======================================================================
-- Parms:
-- (*) lsoMap: the map for the LSO Bin
-- (*) newValue: the new value to be pushed on the stack
hotDirInsert( lsoMap, newChunk );
end -- hotDirInsert( lsoMap, newChunk )

-- ======================================================================
-- migrateWarmChunk( lsoMap )
-- ======================================================================
-- Parms:
-- (*) lsoMap: the map for the LSO Bin
-- (*) newValue: the new value to be pushed on the stack
local function migrateWarmChunk( lsoMap )
end -- migrateWarmChunk()
-- ======================================================================

-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- LSO Main Functions
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
--
-- ======================================================================
-- || stackCreate (Stickman)    ||
-- ======================================================================
-- Create/Initialize a Stack structure in a bin, using a single LSO
-- bin, using User's name, but Aerospike TYPE (CL_LSO)
--
-- For this version (Stickman), we will be using a SINGLE MAP object,
-- which contains lots of metadata, plus one lists:
-- (*) Namespace Name (just one Namespace -- for now)
-- (*) Set Name
-- (*) Chunk Size (same for both namespaces)
-- (*) Hot Chunk Count
-- (*) Cold Chunk Count
-- (*) Item Count (will NOT be tracked in Stoneman)
-- (*) The List of Hot Chunks of data (each Chunk is a list)
-- (*) The Head of the Cold Data Directory
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
  info("[ENTER]: <%s:%s> argList(%s)\n", mod, meth, arglist );

  -- Check to see if LSO Structure (or anything) is already there,
  -- and if so, error
  if( record[lsoBinMame] ~= nil ) then
    info("[ERROR EXIT]: <%s:%s> LSO BIN(%s) Already Exists\n",
      mod, meth, lsoBinMame );
    return('LSO_BIN already exists');
  end

  info("[DEBUG]: <%s:%s> : Initialize LSO Map\n", mod, meth );

  -- Define our control information and put it in the record's control bin
  -- Notice that in the next version, Top of Stack (TOS) will not be at the
  -- end, but will instead move and will have a TOS ptr var in the ctrl map.
  local lsoMap = map();
  local digest = aerospike:create_digest( record );
  local designVersion = 1;
  local entryMax = 100; -- items per chunk
  local bytesMax = 2000; -- bytes per chunk

  lsoMap.BinName = arglist[1];
  lsoMap.NameSpace = arglist[2];
  -- lsoMap['ColdNameSpace'] = coldNamespace;
  lsoMap.Set = arglist[3];
  lsoMap.ChunkSize = arglist[4];
  lsoMap.HotChunkCount = 0;
  lsoMap.ColdChunkCount = 0;
  lsoMap.ItemCount = 0;
  lsoMap.DirList = list();   -- Define a new list for the Hot Stuff
  lsoMap.ColdListHead  = 0;   -- Nothing here yet
  lsoMap.DesignVersion = designVersion;

  info("[DEBUG]: <%s:%s> : CTRL Map after Init(%s)\n",
    mod, meth , tostring(lsoMap));

  local newChunk =
    createChunk( record, lsoMap, digest, designVersion, "Hot", entryMax)

  -- Put our new map in the record, then store the record.
  record[lsoBinName] = lsoMap;

  info("[DEBUG]:<%s:%s>:Dir Map after Init(%s)\n", mod,meth,tostring(dirMap));

  -- All done, store the record
  local rc = -99; -- Use Odd starting Num: so that we know it got changed
  if( not aerospike:exists( record ) ) then
    info("[DEBUG]:<%s:%s>:Create Record(%s)\n", mod, meth, tostring(record));
    rc = aerospike:create( record );
  else
    info("[DEBUG]:<%s:%s>:Update Record(%s)\n", mod, meth, tostring(record));
    rc = aerospike:update( record );
  end

  info("[EXIT]: <%s:%s> : Done.  RC(%d)\n", mod, meth, rc );
  return rc;
end -- function stackCreate( record, namespace, set )

-- ======================================================================
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- || stackPush (Stickman V1)
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- Push a value onto the stack. There are different cases, with different
-- levels of complexity:
-- If there's room to insert into the top chunk (a Hot Chunk)
--   Case 1: Call insertNewValue()
-- else
--   Case 2: Allocate a new Chunk
--   If there's room in the hot directory:
--     Case 2.1: Add new chunk to Hot Directory
--     Case 1: Call insertNewValue()
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
-- +-----+-----+-----+-----+----------------------------------------+
-- |User |User |o o o|LSO  |                                        |
-- |Bin 1|Bin 2|o o o|Bin 1|                                        |
-- +-----+-----+-----+-----+----------------------------------------+
--                  /       \                                       
--     LSO Map     /                                        
--     +-------------------+                                 
--     | LSO Control Info  |   <> Each Chunk dir holds about 100 Chunk Ptrs
--     +-------------------+   <> Each Chunk holds about 100 entries
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
--   |                                                           
--   +-----+                                                
--         |                                                
--         V (Top of Stack List)                                  
--     +---------+                            Chunk 1
--     |Digest 1 |+------------------------> +---------+
--     |---------|                Chunk 2    |Entry 1  |
--     |Digest 2 |+------------> +---------+ |Entry 2  |
--     +---------+               |Entry 1  | |         |
--     | o o o   |               |Entry 2  | |   o     |
--     |---------|    Chunk N    |         | |   o     |
--     |Digest N |+->+---------+ |   o     | |   o     |
--     +---------+   |Entry 1  | |   o     | |         |
--                   |Entry 2  | |   o     | |Entry n  |
--                   |         | |         | +---------+
--                   |   o     | |Entry n  |
--                   |   o     | +---------+
--                   |   o     |
--                   |         |
--                   |Entry n  |
--                   +---------+
--
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
local function localStackPush( record, lsoBinName, newValue )
  local mod = "LsoStickman";
  local meth = "localStackPush()";
  info("[ENTER]: <%s:%s>  NewValue(%s)\n", mod, meth, tostring( newValue ));

  if( not aerospike:exists( record ) ) then
    info("[ERROR EXIT]:<%s:%s>:Missing Record. Exit\n", mod, meth );
    return('Base Record Does NOT exist');
  end

  -- Verify that the LSO Structure is there: otherwise, error.
  if( record[lsoBinName] == nil ) then
    info("[ERROR EXIT]: <%s:%s> LSO_BIN (%s)DOES NOT Exists\n",
      mod, meth, lsoBinName );
    return('LSO_BIN Does NOT exist');
  end
  
  -- The largest bin number (i.e. ChunkCount) marks the insert position,
  -- provided that there's room. If there's not room, create a new Chunk
  -- and do the insert.
  local lsoMap = record[lsoBinName]; -- The main LSO map
  if isRoomForNewEntry( lsoMap, newValue ) then
    -- Case 1:  Just do the insert: Top Chunk, simple append
    insertNewValue( lsoMap, newValue );
  else
    -- Case 2: Allocate new Chunk, enter value, find a place for new chunk
    newChunk = newChunkInsert( lsoMap, newValue );
    if  isRoomForNewHotChunk( lsoMap, newChunk ) then
      hotDirInsert( lsoMap, newChunk );
    else
      -- Case 3: Dealing with the Cold Directory
      -- Make room in the Hot Dir (Make coldest hot the new hotest cold)
      migrateWarmChunk( lsoMap )
      hotDirInsert( lsoMap, newChunk )
    end
  end

  -- Note that we will (likely) NOT track the exact item count in the
  -- record in the FINAL VERSION, as that would trigger a new record
  -- update for EACH Value insert, in addition to the record update for
  -- the record actually holding the new value.  We want to keep this to
  -- just one record insert rather than two.
  local itemCount = lsoMap['ItemCount'];
  itemCount = itemCount + 1;
  lsoMap['ItemCount'] = itemCount;

  -- Not sure if this is needed -- but it seems to be.
  record[lsoBinName] = lsoMap;

  info("[ENTER]: <%s:%s>Storing Record: LSO(%s) New Value(%s)\n",
    mod, meth, tostring(lsoMap), tostring( newValue ));

  -- All done, store the record
  local rc = -99; -- Use Odd starting Num: so that we know it got changed
  info("[DEBUG]:<%s:%s>:Update Record\n", mod, meth );
  rc = aerospike:update( record );

  info("[EXIT]: <%s:%s> : Done.  RC(%d)\n", mod, meth, rc );
  return rc
end -- function stackPush( record, newValue )

function stackPush( record, newValue )
  local mod = "LsoStickman";
  local meth = "stackPush()";
  info("[ENTER]: <%s:%s>  NewValue(%s)\n", mod, meth, tostring( newValue ));

  return localStackPush( record, newValue )
end


-- ======================================================================
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- || stackPeek (Straw V2)
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- Return "peekCount" values from the stack, in Stack (LIFO) order.
-- For Each Bin (in LIFO Order), read each Bin in reverse append order.
-- If "peekCount" is zero, then return all.
--
-- We will use predetermined BIN names for this initial prototype
-- 'LSO_CTRL_BIN' will be the name of the bin containing the control info
-- 'LSO_DIR_BIN' will be the name of the bin containing the BIN Dir List
-- 'BIN_XX' will be the individual bins that hold lists of data
-- +========================================================================+
-- | Usr Bin 1 | Usr Bin 2 | o o o | Usr Bin N | LSO CTRL BIN | LSO_DIR_BIN |
-- +========================================================================+
-- LSO Bin will contain a MAP of Control Info
-- LSO Dir will contain a MAP of Bins, each bin holding a List of data
-- ======================================================================
function stackPeek( record, peekCount, transformFunc )
  local mod = "LsoStickman";
  local meth = "stackPeek()";
  info("[ENTER]: <%s:%s> PeekCount(%d) \n", mod, meth, peekCount );

  -- Verify that the LSO Structure is there: otherwise, error.
  if( record.LSO_CTRL_BIN == nil ) then
    return('LSO_CTRL_BIN Does NOT exist');
  end
  if( record.LSO_DIR_BIN == nil ) then
    return('LSO_DIR_BIN Does NOT exist');
  end

  -- Build the user's "resultList" from the items we find that qualify.
  -- They must pass the "transformFunction()" filter.
  -- Also, Notice that we go in reverse order -- to get the "stack function",
  -- which is Last In, First Out.
  local resultCount = 0;
  local resultList = list();
  local lsoMap = record.LSO_CTRL_BIN;
  local binNum = lsoMap['ChunkCount']; -- Start with the end (TOS)
  local binName;
  local dirMap = record.LSO_DIR_BIN;
  info("[DEBUG]: <%s:%s> BinNum(%d) Validating Record:CTRL(%s) DIR(%s)\n",
    mod, meth, binNum, tostring(lsoMap), tostring(dirMap));

  for b = binNum, 1, -1 do
    binName = getBinName( b );
    dirMap = record.LSO_DIR_BIN;
    dirList = dirMap[binName]; 
    for i = list.size(dirList),  1, -1 do
      info("[DEBUG]:<%s:%s>: DirList Apply(%d)[%s]\n",
        mod, meth, i, tostring( dirList[i] ) );
      local transformed;

        transformed = transformFilter1 ( dirList[i] );

--      if (transformFunc ~= nil) then
--        info("[DEBUG]:<%s:%s>: Function NOT nil \n", mod, meth );
--        -- transformed = transformFunc(dirList[i]);
--        transformed = dirList[i];
--        transformed = transformFilter1 ( dirList[i] );
--      else
--        transformed = dirList[i];
--      end -- end if valid transformFunc

      if  transformed ~= nil   then
        list.append( resultList, transformed );
        resultCount = resultCount + 1;
        -- If PeekCount is ZERO, it means "all", so PC must be > 0 to
        -- have an early exit.
        if( resultCount >= peekCount and peekCount > 0 ) then
          info("[EARLY EXIT]: <%s:%s>: PeekCount(%d) Returning (%d) \n",
            mod, meth, peekCount, resultCount );
          return resultList;
        end -- end if early exist
      end -- end if transformed
    end -- end for each list item (reverse order)
  end -- end for each bin (reverse order)

  info("[EXIT]: <%s:%s>: PeekCount(%d) Returning (%d) ResultList(%s)\n",
    mod, meth, peekCount, resultCount, tostring(resultList));

  return resultList;

end -- function stackPeek( record, peekCount )


-- ======================================================================
-- Generate Entry
-- ======================================================================
local function generateEntry( seed )
  local mod = "LsoStickman";
  local meth = "generateEntry()";
  local resultList = list();
  local upper = 500;
  local listMax = 4;
  local newElement;
  for i = 1, listMax do
    newElement = math.random( 500 );
    list.append( resultList, newElement );
  end
  info("[EXIT]:<%s:%s> Result(%s)\n", mod, meth, tostring(resultList) );

  return resultList;
end

-- ======================================================================
-- Generate 'recCount' number of quadruplets and store them
-- ======================================================================
function stackPopulate( record, recCount )
  local mod = "LsoStickman";
  local meth = "stackPopulate()";
  info("[ENTER]: <%s:%s> recCount(%d) \n", mod, meth, recCount );

  -- Loop thru 'recCount' times, generate a new entry and push it.
  local newEntry;
  local result = 0;
  for e = 1, recCount do
    newEntry = generateEntry( e );
    localStackPush( record, newEntry );
  end -- end for


  info("[EXIT]: <%s:%s>: Returning (%d) \n", mod, meth, result );
  return result;

end -- function stackPopulate( record, recCount )

-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
