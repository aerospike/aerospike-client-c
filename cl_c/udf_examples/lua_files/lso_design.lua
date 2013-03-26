-- Large Stack Object (LSO) Design
-- (March 15, 2013)(V2.1)
--
-- ======================================================================
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
-- (*) LSO Hot List: List of data entries (on the order of 100)
-- (*) LSO Warm Directory: List of Aerospike Record digests:
--     100 digests(250 bytes)
-- (*) LSO Cold Directory Head (digest of Head plus count) (30 bytes)
-- (*) Total LSO Record overhead is on the order of 350 bytes
-- NOTES:
-- (*) In the Hot List, the data items are stored directly in the
--     hot list (regardless of whether they are bytes or other as_val types)
-- (*) In the Warm Dir List, the list contains aerospike digests of the
--     LSO Data Records (LDRs) that hold the Warm Data.  The LDRs are
--     opened (using the digest), then read/written, then closed/updated.
-- (*) The Cold Dir Head holds the Aerospike Record digest of a record that
--     holds a linked list of cold directories.  Each cold directory holds
--     a list of digests that are the cold LSO Data Records.
-- (*) The Warm and Cold LSO Data Records use the same format -- so they
--     simply transfer from the warm list to the cold list by moving the
--     corresponding digest from the warm list to the cold list.
-- (*) Record types used in this design:
-- (1) There is the main record that contains the LSO bin (LSO Head)
-- (2) There are LSO Data "Chunk" Records (both Warm and Cold)
--     ==> Warm and Cold LSO Data Records have the same format:
--         They both hold User Stack Data.
-- (3) There are Chunk Directory Records (used in the cold list)
--
-- (*) How it all connects together....
-- (+) The main record points to:
--     - Warm Data Chunk Records (these records hold stack data)
--     - Cold Data Directory Records (these records hold ptrs to Cold Chunks)
--
-- (*) We may have to add some auxilliary information that will help
--     pick up the pieces in the event of a network/replica problem, where
--     some things have fallen on the floor.  There might be some "shadow
--     values" in there that show old/new values -- like when we install
--     a new cold dir head, and other things.  TBD
--
-- (*)
--
--
-- +-----+-----+-----+-----+----------------------------------------+
-- |User |User |o o o|LSO  |                                        |
-- |Bin 1|Bin 2|o o o|Bin 1|                                        |
-- +-----+-----+-----+-----+----------------------------------------+
--                  /       \                                       
--   ================================================================
--     LSO Map                                              
--     +-------------------+                                 
--     | LSO Control Info  |  About 20 different values kept in Ctrl Info
--     +----------------++++++++++
--     | Hot Entry List |||||||||| Entries are stored directly in the Record
--     +----------------++++++++++
--     | LSO Warm Dir List |-+                                     
--     +-------------------+ |                                   
--   +-| LSO Cold Dir Head | |                                   
--   | +-------------------+ |                                   
--   |          +------------+
--   |          |                                                
--   |          V (Warm Directory)              Warm Data(WD)
--   |      +---------+                          Chunk Rec 1
--   |      |Digest 1 |+----------------------->+--------+
--   |      |---------|              WD Chunk 2 |Entry 1 |
--   |      |Digest 2 |+------------>+--------+ |Entry 2 |
--   |      +---------+              |Entry 1 | |   o    |
--   |      | o o o   |              |Entry 2 | |   o    |
--   |      |---------|  WD Chunk N  |   o    | |   o    |
--   |      |Digest N |+->+--------+ |   o    | |Entry n |
--   |      +---------+   |Entry 1 | |   o    | +--------+
--   |                    |Entry 2 | |Entry n |
--   |                    |   o    | +--------+
--   |                    |   o    |
--   |                    |   o    |
--   |                    |Entry n |
--   |                    +--------+
--   |                          +-----+->+-----+->+-----+ ->+-----+
--   +----------------------->  |Rec  |  |Rec  |  |Rec  | o |Rec  |
--    The cold dir is a linked  |Chunk|  |Chunk|  |Chunk| o |Chunk|
--    list of dir pages that    |Dir  |  |Dir  |  |Rec  | o |Dir  |
--    point to LSO Data Records +-----+  +-----+  +-----+   +-----+
--    that hold the actual cold  ||...|   ||...|   ||...|    ||...|
--    data (cold chunks).        ||   V   ||   V   ||   V    ||   V
--                               ||   +--+||   +--+||   +--+ ||   +--+
--    As "Warm Data" ages out    ||   |Cn|||   |Cn|||   |Cn| ||   |Cn|
--    of the Warm Dir List, the  |V   +--+|V   +--+|V   +--+ |V   +--+
--    LDRs transfer out of the   |+--+    |+--+    |+--+     |+--+
--    Warm Directory and into    ||C2|    ||C2|    ||C2|     ||C2|
--    the cold directory.         V+--+    V+--+    V+--+     V+--+
--                               +--+     +--+     +--+      +--+
--    The Warm and Cold LDRs     |C1|     |C1|     |C1|      |C1|
--    have identical structure.  +--+     +--+     +--+      +--+
--                                A        A        A         A    
--                                |        |        |         |
--           [Cold Data Chunks]---+--------+--------+---------+
--
--
-- The "Hot Entry List" is the true "Top of Stack", holding roughly the
-- top 50 to 100 values.  The next level of storage is found in the first
-- Warm dir list (the last Chunk in the list).  Since we process stack
-- operations in LIFO order, but manage them physically as a list
-- (append to the end), we basically read the pieces in top down order,
-- but we read the CONTENTS of those pieces backwards.  It is too expensive
-- to "prepend" to a list (which would require moving the entire list each
-- time we tried to plug an entry in the front).  Instead, just read the
-- list segment back to front (in reverse append order).
--
-- We do NOT "age" the individual entries out one at a time as the Hot List
-- overflows -- we instead take a group at a time (specified by the
-- HotListTransferAmount), which opens up a block empty spots. Notice that
-- the transfer amount is a tuneable parameter -- for heavy reads, we would
-- want MORE data in the list, and for heavy writes we would want less.
--
-- If we generally pick half (100 entries total, and then transfer 50 at
-- a time when the list fills up), then half the time the insers will affect
-- ONLY the Top (LSO) record -- so we'll have only one Read, One Write 
-- operation for a stack push.  1 out of 50 will have the double read,
-- double write, and 1 out of a thousand (or so) will have additional
-- IO's depending on the state of the Warm/Cold lists.
--
-- NOTE: Design, V3.  For really cold data -- things out beyond 50,000
-- elements, it might make sense to just push those out to a real disk
-- based file (to which we could just append -- and read in reverse order).
-- If we ever need to read the whole stack, we can afford
-- the time and effort to read the file (it is an unlikely event).  The
-- issue here is that we probably have to teach Aerospike how to transfer
-- (and replicate) files as well as records.
--
-- ======================================================================
-- Aerospike Calls:
-- newRec = aerospike:crec_create( topRec )
-- newRec = aerospike:crec_open( record, digest)
-- status = aerospike:crec_update( record, newRec )
-- status = aerospike:crec_close( record, newRec )
-- digest = record.digest( newRec )
-- ======================================================================

-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- || LSO Bin CONTENTS  ||||||||||||||||||||||||||||||||||||||||||||||||||
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
--

-- ======================================================================
-- Large Stack Object (LSO) Operations
-- LsoIceMan V2.1 -- (March 15, 2013)
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

-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- <><><><> <Initialize Control Maps> <Initialize Control Maps> <><><><>
-- There are three main Record Types used in the LSO Package, and their
-- initialization functions follow.  The initialization functions
-- define the "type" of the control structure:
--
-- (*) TopRec: the top level user record that contains the LSO bin
-- (*) LdrRec: the LSO Data Record that holds user Data
-- (*) ColdDirRec: The Record that holds a list of AS Record Digests
--     (i.e. record pointers) to the LDR Data Records.  The Cold list is
--     a linked list of Directory pages, each of contains a list of
--     digests (record pointers) to the LDR data pages.
-- <+> Naming Conventions:
--   + All Field names (e.g. lsoMap.PageMode) begin with Upper Case
--   + All variable names (e.g. lsoMap.PageMode) begin with lower Case
--   + All Record Field access is done using brackets, with either a
--     variable or a constant (in single quotes).
--     (e.g. topRec[binName] or ldrRec['LdrControlBin']);
--
-- <><><><> <Initialize Control Maps> <Initialize Control Maps> <><><><>
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- Notes on Configuration:
-- (*) The HotEntryList should be the same size as the LDR Page that
--     holds the Data entries.
-- (*) The HotListTransfer should be half or one quarter the size of the
--     HotList -- so that even amounts can be transfered to the warm list.
-- (*) The WarmDigestList should be the same size as the DigestList that
--     is in the ColdDirectory Page
-- (*) The WarmListTransfer should be half or one quarter the size of the
--     list -- so that even amounts can be transfered to the cold list.
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||

-- ======================================================================
-- initializeLsoMap:
-- ======================================================================
-- Set up the LSO Map with the standard (default) values.
-- These values may later be overridden by the user.
-- The structure held in the Record's "LSO BIN" is this map.  This single
-- structure contains ALL of the settings/parameters that drive the LSO
-- behavior.  Thus this function represents the "type" LSO MAP -- all
-- LSO control fields are defined here.
-- The LsoMap is obtained using the user's LSO Bin Name:
-- lsoMap = 
-- ======================================================================
local function initializeLsoMap( topRec, lsoBinName )
  local mod = "LsoIceMan";
  local meth = "initializeLsoMap()";
  GP=F and trace("[ENTER]: <%s:%s>:: LsoBinName(%s)",
    mod, meth, tostring(lsoBinName));

  -- Create the map, and fill it in.
  -- Note: All Field Names start with UPPER CASE.
  local lsoMap = map();
  -- General LSO Parms:
  lsoMap.ItemCount = 0;        -- A count of all items in the stack
  lsoMap.Version = 1.1 ; -- Current version of the code
  lsoMap.Magic = "MAGIC"; -- we will use this to verify we have a valid map
  lsoMap.BinName = lsoBinName; -- Defines the LSO Bin
  lsoMap.NameSpace = "test"; -- Default NS Name -- to be overridden by user
  lsoMap.Set = "set";       -- Default Set Name -- to be overridden by user
  lsoMap.PageMode = "List"; -- "List" or "Binary":
  -- LSO Data Record Chunk Settings: Passed into "Chunk Create"
  lsoMap.LdrEntryCountMax = 200;  -- Max # of items in a Data Chunk (List Mode)
  lsoMap.LdrByteEntrySize = 18;  -- Byte size of a fixed size Byte Entry
  lsoMap.LdrByteCountMax = 2000; -- Max # of BYTES in a Data Chunk (binary mode)
  -- Hot Entry List Settings: List of User Entries
  lsoMap.HotEntryList = list();
  lsoMap.HotEntryListItemCount = 0; -- Number of elements in the Top List
  lsoMap.HotListMax = 200; -- Max Number for the list -- when we transfer
  lsoMap.HotListTransfer = 100; -- How much to Transfer at a time.
  -- Warm Digest List Settings: List of Digests of LSO Data Records
  lsoMap.WarmTopFull = 0; -- 1  when the top chunk is full (for the next write)
  lsoMap.WarmDigestList = list();   -- Define a new list for the Warm Stuff
  lsoMap.WarmListDigestCount = 0; -- Number of Warm Data Record Chunks
  lsoMap.WarmListMax = 4; -- Number of Warm Data Record Chunks
  lsoMap.WarmListTransfer = 2; -- Number of Warm Data Record Chunks
  lsoMap.WarmTopChunkEntryCount = 0; -- Count of entries in top warm chunk
  lsoMap.WarmTopChunkByteCount = 0; -- Count of bytes used in top warm Chunk
  -- Cold Directory List Settings: List of Directory Pages
  lsoMap.ColdTopFull = 0; -- 1 when the cold head is full (for the next write)
  lsoMap.ColdDataRecCount = 0; -- Number of Cold DATA Records
  lsoMap.ColdDirRecCount = 0; -- Number of Cold DIRECTORY Records
  lsoMap.ColdDirListHead  = 0; -- Head (rec Digest) of the Cold List Dir Chain
  lsoMap.ColdListMax = 100;  -- Number of list entries in a Cold list dir node

  GP=F and trace("[DEBUG]: <%s:%s> : CTRL Map after Init(%s)",
      mod, meth , tostring(lsoMap));

  -- Put our new map in the record, then store the record.
  topRec[lsoBinName] = lsoMap;

  GP=F and trace("[EXIT]:<%s:%s>:", mod, meth );
  return lsoMap
end -- initializeLsoMap

-- ======================================================================
-- initializeLdrMap( ldrMap )
-- ======================================================================
-- Set the values in a LSO Data Record (LDR) Control Bin map. LDR Records
-- hold the actual data for both the WarmList and ColdList.
-- This function represents the "type" LDR MAP -- all fields are
-- defined here.
-- There are potentially three bins in an LDR Record:
-- (1) ldrRec['LdrControlBin']: The control Map (defined here)
-- (2) ldrRec['LdrListBin']: The Data Entry List (when in list mode)
-- (3) ldrRec['LdrBinaryBin']: The Packed Data Bytes (when in Binary mode)
-- ======================================================================
local function initializeLdrMap( topRec, ldrRec, ldrMap, lsoMap )
  local mod = "LsoIceMan";
  local meth = "initializeLdrMap()";
  GP=F and trace("[ENTER]: <%s:%s>", mod, meth );

  ldrMap.ParentDigest = record.digest( topRec );
  ldrMap.PageMode = lsoMap.PageMode;
  ldrMap.Digest = record.digest( ldrRec );
  ldrMap.ListEntryMax = lsoMap.LdrEntryCountMax; -- Max entries in value list
  ldrMap.ByteEntrySize = lsoMap.LdrByteEntrySize; -- ByteSize of Fixed Entries
  ldrMap.ByteEntryCount = 0;  -- A count of Byte Entries
  ldrMap.ByteCountMax = lsoMap.LdrByteCountMax; -- Max # of bytes in ByteArray
  ldrMap.Version = lsoMap.Version;
  ldrMap.LogInfo = 0;
end -- initializeLdrMap()


-- ======================================================================
-- initializeColdDirMap( coldDirMap )
-- ======================================================================
-- Set the default values in a Cold Directory Record. ColdDir records
-- contain a list of digests that reference LDRs (above).
-- This function represents the "type" ColdDir MAP -- all fields are
-- defined here.
-- There are two bins in a ColdDir Record:
-- (1) ldrRec['ColdDirCtrlBin']: The control Map (defined here)
-- (2) ldrRec['ColdDirListBin']: The Digest List
-- ======================================================================
local function initializeColdDirMap( topRec, coldDirRec, coldDirMap, lsoMap )
  local mod = "LsoIceMan";
  local meth = "initializeColdDirMap()";
  GP=F and trace("[ENTER]: <%s:%s>", mod, meth );
  
  coldDirMap.ParentDigest = record.digest( topRec );
  coldDirMap.Digest = record.digest( coldDirRec );
  coldDirMap.NextDirRec = 0; -- no other Dir Records (yet).
  coldDirMap.DigestCount = 0; -- no digests in the list -- yet.
  coldDirMap.Version = lsoMap.Version;

  -- This next item should be found in the lsoMap.
  coldDirMap.ColdListMax = lsoMap.ColdListMax; -- Max digs the cold dir list
  coldDirMap.LogInfo = 0;  -- Not used (yet)

end -- initializeColdDirMap()
