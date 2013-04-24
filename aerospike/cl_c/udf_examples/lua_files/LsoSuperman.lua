-- Large Stack Object (LSO or LSTACK) Operations
-- LSTACK.lua:  Superman V4.22 -- (April 22, 2013)
--
-- Module Marker: Keep this in sync with the stated version
local MOD="LsoSuperman_4.22.6"; -- the module name used for tracing

-- ======================================================================
-- Please refer to lstack_design.lua for architecture and design notes.
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
-- LSTACK Functions Supported (Note switch to lower case)
-- (*) lstack_create: Create the LSO structure in the chosen topRec bin
-- (*) lstack_push: Push a user value (AS_VAL) onto the stack
-- (*) lstack_create_and_push: Push a user value (AS_VAL) onto the stack
-- (*) lstack_peek: Read N values from the stack, in LIFO order
-- (*) lstack_peek_then_filter: Read N values from the stack, in LIFO order
-- (*) lstack_trim: Release all but the top N values.
-- (*) lstack_config: retrieve all current config settings in map format
-- (*) lstack_size: Report the NUMBER OF ITEMS in the stack.
--
-- REMEMBER THAT ALL INSERTS ARE INTO HOT LIST -- and transforms are done
-- there.  All UNTRANSFORMS are done reading from the List (Hot List or
-- warm/cold Data Page List).  Notice that even though the values may be
-- transformed (compacted into) bytes, they are still just inserted into
-- the hot list, we don't try to pack them into an array;
-- that is done only in the warm/cold pages (where the benefit is greater).
-- 
-- Read Filters are applied AFTER the UnTransform (bytes and list).
--
-- NOTE: New changes with V4.3 to Push and Peek.
-- (*) Stack Push has an IMPLICIT transform function -- which is defined
--     in the create spec.  So, the two flavors of Stack Push are now
--     + lstack_push(): with implicit transform when defined
--     + lstack_create_and_push(): with the ability to create as
--       needed -- and with the supplied create_spec parameter.
-- (*) Stack Peek has an IMPLICIT UnTransform function -- which is defined
--     in the create spec.  So, the two flavors of Stack Peek are now
--     + lstack_peek(): with implicit untransform, when defined in create.
--     + lstack_peek_then_filter(): with implicit untransform and a filter
--       to act as an additional query mechanism.
--
-- On Create, a Large Stack Object can be configured with a Transform function,
-- to be used on storage (push) and an UnTransform function, to be used on
-- retrieval (peek).
-- (*) stack_push(): Push a user value (AS_VAL) onto the stack, 
--     calling the Transform on the value FIRST to transform it before
--     storing it on the stack.
-- (*) stack_peek_then_filter: Retrieve N values from the stack, and for each
--     value, apply the transformation/filter UDF to the value before
--     adding it to the result list.  If the value doesn't pass the
--     filter, the filter returns nil, and thus it would not be added
--     to the result list.
-- ======================================================================
-- TO DO List: for Future (once delete_subrec() is available)
-- TODO: Implement stack_trim(): Must release storage before record delete.
-- TODO: Implement LStackSubRecordDestructor():
-- ----------------------------------------------------------------------
-- TO DO List: for 4.20
-- TODO: Change to standard package names (and in Function Table)
-- TODO: Change standard return mechanism
-- TODO: Add Exists Subrec Digest in LsoMap.
--
-- TO DO List: for 4.9.
-- DONE: + Move values to constants (local vars  in LSTACK)
--
-- TO DO List: for 4.3.
-- DONE: Change to new API:
--       + Implicit Transform on Push() and Peek()
--       + Implicit Create on Push()
--       + Explicit Filter on Peek()
-- DONE: Implement stack_config(): Return a MAP that includes all of the
--                                 stack configuration values.
-- DONE: FINISH THE TRANSFORM AND FILTER: To both the readByteArray
--       and the List.
-- ----------------------------------------------------------------------
-- Done in 4.2
-- DONE: Implement stack_size(): Return size of the stack.
-- DONE: Check all external calls for valid parameter values
--       e.g. All bin names must be UNDER 14 chars.
--
-- DONE: Verify correct recovery order for operations.
-- DONE: Call lua error('error msg') in the event of critical errors.
-- ----------------------------------------------------------------------
-- ======================================================================
-- Aerospike SubRecord Calls:
-- newRec = aerospike:create_subrec( topRec )
-- newRec = aerospike:open_subrec( topRec, childRecDigest)
-- status = aerospike:update_subrec( topRec, childRec )
-- status = aerospike:close_subrec( topRec, childRec )
-- status = aerospike:delete_subrec( topRec, childRec ) (not yet ready)
-- digest = record.digest( childRec )
-- ======================================================================
-- For additional Documentation, please see lso_design.lua, which should
-- be co-located in the main Development tree with LSTACK.lua
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
local GP=true; -- Leave this ALWAYS true (but value seems not to matter)
local F=true; -- Set F (flag) to true to turn ON global print

-- ======================
-- || GLOBAL CONSTANTS || -- Local, but global to this module
-- ======================
local MAGIC="MAGIC";     -- the magic value for Testing LSO integrity

-- Get addressability to the Function Table: Used for compress and filter
local functionTable = require('UdfFunctionTable');

-- StoreMode (SM) values (which storage Mode are we using?)
local SM_BINARY='B'; -- Using a Transform function to compact values
local SM_LIST='L'; -- Using regular "list" mode for storing values.

-- Package Names for "pre-packaged" settings:
local PackageStandardList=   "StandardList";
local PackageTestModeList=   "TestModeList";
local PackageTestModeBinary= "TestModeBinary";
-- Specific production use: 
-- (*) A List Value (a 5 part tuple)
-- (*) Special, packed (compressed) Binary storage
local PackageProdListValBinStore=    "ProdListValBinStore";
local PackageDebugModeList=  "DebugModeList";
local PackageDebugModeBinary="DebugModeBinary";

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
--   + All Field names (e.g. lsoMap.StoreMode) begin with Upper Case
--   + All variable names (e.g. lsoMap.StoreMode) begin with lower Case
--   + All Record Field access is done using brackets, with either a
--     variable or a constant (in single quotes).
--     (e.g. topRec[binName] or ldrRec['LdrControlBin']);
--
-- <><><><> <Initialize Control Maps> <Initialize Control Maps> <><><><>
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- Notes on Configuration:
-- (*) In order to make the LSO code as efficient as possible, we want
--     to pick the best combination of configuration values for the Hot,
--     Warm and Cold Lists -- so that data transfers from one list to
--     the next with minimal storage upset and runtime management.
--     Similarly, we want the transfer from the LISTS to the Data pages
--     and Data Directories to be as efficient as possible.
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
  local meth = "initializeLsoMap()";
  GP=F and trace("[ENTER]: <%s:%s>:: LsoBinName(%s)",
    MOD, meth, tostring(lsoBinName));

  -- Create the map, and fill it in.
  -- Note: All Field Names start with UPPER CASE.
  local lsoMap = map();
  -- General LSO Parms:
  lsoMap.ItemCount = 0;        -- A count of all items in the stack
  lsoMap.Version = 1 ; -- Current version of the code
  lsoMap.LdtType = "LSTACK"; -- we will use this to verify we have a valid map
  lsoMap.Magic = MAGIC; -- we will use this to verify we have a valid map
  lsoMap.BinName = lsoBinName; -- Defines the LSO Bin
  lsoMap.NameSpace = "test"; -- Default NS Name -- to be overridden by user
  lsoMap.Set = "set";       -- Default Set Name -- to be overridden by user
  lsoMap.StoreMode = SM_LIST; -- SM_LIST or SM_BINARY:
  lsoMap.ExistSubRecDig = 0; -- Pt to the LDT "Exists" subrecord (digest)

  -- LSO Data Record Chunk Settings: Passed into "Chunk Create"
  lsoMap.LdrEntryCountMax = 100;  -- Max # of items in a Data Chunk (List Mode)
  lsoMap.LdrByteEntrySize =  0;  -- Byte size of a fixed size Byte Entry
  lsoMap.LdrByteCountMax =   0; -- Max # of BYTES in a Data Chunk (binary mode)

  -- Hot Entry List Settings: List of User Entries
  lsoMap.HotEntryList = list(); -- the list of data entries
  lsoMap.HotEntryListItemCount = 0; -- Number of elements in the Top List
  lsoMap.HotListMax = 100; -- Max Number for the List -- when we transfer
  lsoMap.HotListTransfer =  50; -- How much to Transfer at a time.

  -- Warm Digest List Settings: List of Digests of LSO Data Records
  lsoMap.WarmDigestList = list(); -- the list of digests for LDRs
  lsoMap.WarmTopFull = 0; -- 1  when the top chunk is full (for the next write)
  lsoMap.WarmListDigestCount = 0; -- Number of Warm Data Record Chunks
  lsoMap.WarmListMax = 100; -- Number of Warm Data Record Chunks
  lsoMap.WarmListTransfer = 2; -- Number of Warm Data Record Chunks
  lsoMap.WarmTopChunkEntryCount = 0; -- Count of entries in top warm chunk
  lsoMap.WarmTopChunkByteCount = 0; -- Count of bytes used in top warm Chunk

  -- Cold Directory List Settings: List of Directory Pages
  lsoMap.ColdDirListHead  = 0; -- Head (Rec Digest) of the Cold List Dir Chain
  lsoMap.ColdTopFull = 0; -- 1 when the cold head is full (for the next write)
  lsoMap.ColdDataRecCount = 0; -- Number of Cold DATA Records (data chunks)
  lsoMap.ColdDirRecCount = 0; -- Number of Cold DIRECTORY Records
  lsoMap.ColdListMax = 100;  -- Number of list entries in a Cold list dir node

  GP=F and trace("[DEBUG]: <%s:%s> : CTRL Map after Init(%s)",
      MOD, meth , tostring(lsoMap));

  -- Put our new map in the record, then store the record.
  topRec[lsoBinName] = lsoMap;

  GP=F and trace("[EXIT]:<%s:%s>:", MOD, meth );
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
  local meth = "initializeLdrMap()";
  GP=F and trace("[ENTER]: <%s:%s>", MOD, meth );

  ldrMap.ParentDigest = record.digest( topRec );
  ldrMap.StoreMode = lsoMap.StoreMode;
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
  local meth = "initializeColdDirMap()";
  GP=F and trace("[ENTER]: <%s:%s>", MOD, meth );
  
  coldDirMap.ParentDigest = record.digest( topRec );
  -- coldDirMap.StoreMode = lsoMap.StoreMode; -- Don't Use, not needed
  coldDirMap.Digest = record.digest( coldDirRec );
  coldDirMap.NextDirRec = 0; -- no other Dir Records (yet).
  coldDirMap.DigestCount = 0; -- no digests in the list -- yet.
  coldDirMap.Version = lsoMap.Version;

  -- This next item should be found in the lsoMap.
  coldDirMap.ColdListMax = lsoMap.ColdListMax; -- Max digs the cold dir list
  coldDirMap.LogInfo = 0;  -- Not used (yet)

end -- initializeColdDirMap()


-- ======================================================================
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- LSO Utility Functions
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- ======================================================================
-- These are all local functions to this module and serve various
-- utility and assistance functions.
-- ======================================================================
--
--
-- ++======================++
-- || Prepackaged Settings ||
-- ++======================++
--
-- ======================================================================
-- This is the standard (default) configuration
-- Package = "StandardList"
-- ======================================================================
local function packageStandardList( lsoMap )
  -- General LSO Parms:
  lsoMap.StoreMode = SM_LIST;
  lsoMap.Transform = nil;
  lsoMap.UnTransform = nil;
  -- LSO Data Record Chunk Settings: Passed into "Chunk Create"
  lsoMap.LdrEntryCountMax = 100;  -- Max # of items in a Data Chunk (List Mode)
  lsoMap.LdrByteEntrySize = 0;  -- Byte size of a fixed size Byte Entry
  lsoMap.LdrByteCountMax = 2000; -- Max # of BYTES in a Data Chunk (binary mode)
  -- Hot Entry List Settings: List of User Entries
  lsoMap.HotListMax = 100; -- Max Number for the List -- when we transfer
  lsoMap.HotListTransfer = 50; -- How much to Transfer at a time.
  -- Warm Digest List Settings: List of Digests of LSO Data Records
  lsoMap.WarmListMax = 100; -- Number of Warm Data Record Chunks
  lsoMap.WarmListTransfer = 50; -- Number of Warm Data Record Chunks
  -- Cold Directory List Settings: List of Directory Pages
  lsoMap.ColdListMax = 100;  -- Number of list entries in a Cold list dir node

end

-- ======================================================================
-- Package = "TestModeList"
-- ======================================================================
local function packageTestModeList( lsoMap )
  -- General LSO Parms:
  lsoMap.StoreMode = SM_LIST;
  lsoMap.Transform = nil;
  lsoMap.UnTransform = nil;
  -- LSO Data Record Chunk Settings: Passed into "Chunk Create"
  lsoMap.LdrEntryCountMax = 100;  -- Max # of items in a Data Chunk (List Mode)
  lsoMap.LdrByteEntrySize = 0;  -- Byte size of a fixed size Byte Entry
  lsoMap.LdrByteCountMax = 2000; -- Max # of BYTES in a Data Chunk (binary mode)
  -- Hot Entry List Settings: List of User Entries
  lsoMap.HotListMax = 100; -- Max Number for the List -- when we transfer
  lsoMap.HotListTransfer = 50; -- How much to Transfer at a time.
  -- Warm Digest List Settings: List of Digests of LSO Data Records
  lsoMap.WarmListMax = 100; -- Number of Warm Data Record Chunks
  lsoMap.WarmListTransfer = 50; -- Number of Warm Data Record Chunks
  -- Cold Directory List Settings: List of Directory Pages
  lsoMap.ColdListMax = 100;  -- Number of list entries in a Cold list dir node
end

-- ======================================================================
-- Package = "TestModeBinary"
-- ======================================================================
local function packageTestModeBinary( lsoMap )
  -- General LSO Parms:
  lsoMap.StoreMode = SM_BINARY;
  lsoMap.Transform = compressTest4;
  lsoMap.UnTransform = unCompressTest4;
  -- LSO Data Record Chunk Settings: Passed into "Chunk Create"
  lsoMap.LdrEntryCountMax = 100;  -- Max # of items in a Data Chunk (List Mode)
  lsoMap.LdrByteEntrySize = 0;  -- Byte size of a fixed size Byte Entry
  lsoMap.LdrByteCountMax = 2000; -- Max # of BYTES in a Data Chunk (binary mode)
  -- Hot Entry List Settings: List of User Entries
  lsoMap.HotListMax = 100; -- Max Number for the List -- when we transfer
  lsoMap.HotListTransfer = 50; -- How much to Transfer at a time.
  -- Warm Digest List Settings: List of Digests of LSO Data Records
  lsoMap.WarmListMax = 100; -- Number of Warm Data Record Chunks
  lsoMap.WarmListTransfer = 50; -- Number of Warm Data Record Chunks
  -- Cold Directory List Settings: List of Directory Pages
  lsoMap.ColdListMax = 100;  -- Number of list entries in a Cold list dir node
end

-- ======================================================================
-- Package = "ProdListValBinStore";
-- Specific Production Use: 
-- (*) Tuple value (5 fields of integers)
-- (*) Transforms
-- (*) Binary Storage (uses a compacted representation)
-- ======================================================================
local function packageProdListValBinStore( lsoMap )
  -- General LSO Parms:
  lsoMap.StoreMode = SM_BINARY;
  lsoMap.Transform = "listCompress_5_18";
  lsoMap.UnTransform = "listUnCompress_5_18";
  -- LSO Data Record Chunk Settings: Passed into "Chunk Create"
  lsoMap.LdrEntryCountMax = 200;  -- Max # of items in a Data Chunk (List Mode)
  lsoMap.LdrByteEntrySize = 18;  -- Byte size of a fixed size Byte Entry
  lsoMap.LdrByteCountMax = 2000; -- Max # of BYTES in a Data Chunk (binary mode)
  -- Hot Entry List Settings: List of User Entries
  lsoMap.HotListMax = 100; -- Max Number for the List -- when we transfer
  lsoMap.HotListTransfer = 50; -- How much to Transfer at a time.
  -- Warm Digest List Settings: List of Digests of LSO Data Records
  lsoMap.WarmListMax = 100; -- Number of Warm Data Record Chunks
  lsoMap.WarmListTransfer = 50; -- Number of Warm Data Record Chunks
  -- Cold Directory List Settings: List of Directory Pages
  lsoMap.ColdListMax = 100;  -- Number of list entries in a Cold list dir node
end

-- ======================================================================
-- Package = "DebugModeList"
-- Test the LSTACK in DEBUG MODE (using very small numbers to force it to
-- make LOTS of warm and close objects with very few inserted items), and
-- use LIST MODE.
-- ======================================================================
local function packageDebugModeList( lsoMap )
  -- General LSO Parms:
  lsoMap.StoreMode = SM_LIST;
  lsoMap.Transform = nil;
  lsoMap.UnTransform = nil;
  -- LSO Data Record Chunk Settings: Passed into "Chunk Create"
  lsoMap.LdrEntryCountMax = 4;  -- Max # of items in a Data Chunk (List Mode)
  lsoMap.LdrByteEntrySize = 0;  -- Byte size of a fixed size Byte Entry
  lsoMap.LdrByteCountMax  = 0; -- Max # of BYTES in a Data Chunk (binary mode)
  -- Hot Entry List Settings: List of User Entries
  lsoMap.HotListMax      =  4; -- Max Number for the List -- when we transfer
  lsoMap.HotListTransfer =  2; -- How much to Transfer at a time.
  -- Warm Digest List Settings: List of Digests of LSO Data Records
  lsoMap.WarmListMax =      4; -- Number of Warm Data Record Chunks
  lsoMap.WarmListTransfer = 2; -- Number of Warm Data Record Chunks
  -- Cold Directory List Settings: List of Directory Pages
  lsoMap.ColdListMax      = 2; -- Number of list entries in a Cold list dir nd
end


-- ======================================================================
-- Package = "DebugModeBinary"
-- Test the LSTACK in DEBUG MODE (using very small numbers to force it to
-- make LOTS of warm and close objects with very few inserted items), and
-- use BINARY MODE.
-- ======================================================================
local function packageDebugModeBinary( lsoMap )
  -- General LSO Parms:
  lsoMap.StoreMode = SM_BINARY;
  lsoMap.Transform = "compressTest4"; -- compress list of 4 ints into B16
  lsoMap.UnTransform = "unCompressTest4"; -- uncompress B16 into 4 ints
  -- LSO Data Record Chunk Settings: Passed into "Chunk Create"
  lsoMap.LdrEntryCountMax = 4;  -- Max # of items in a Data Chunk (List Mode)
  lsoMap.LdrByteEntrySize = 16;  -- Byte size of a fixed size Byte Entry
  lsoMap.LdrByteCountMax  = 65; -- Max # of BYTES in a Data Chunk (binary mode)
  -- Hot Entry List Settings: List of User Entries
  lsoMap.HotListMax      =  4; -- Max Number for the List -- when we transfer
  lsoMap.HotListTransfer =  2; -- How much to Transfer at a time.
  -- Warm Digest List Settings: List of Digests of LSO Data Records
  lsoMap.WarmListMax =      4; -- Number of Warm Data Record Chunks
  lsoMap.WarmListTransfer = 2; -- Number of Warm Data Record Chunks
  -- Cold Directory List Settings: List of Directory Pages
  lsoMap.ColdListMax      = 2; -- Number of list entries in a Cold list dir nd
end

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
  local meth = "adjustLsoMap()";
  GP=F and trace("[ENTER]: <%s:%s>:: LsoMap(%s)::\n ArgListMap(%s)",
    MOD, meth, tostring(lsoMap), tostring( argListMap ));

  -- Iterate thru the argListMap and adjust (override) the map settings 
  -- based on the settings passed in during the stackCreate() call.
  GP=F and trace("[DEBUG]: <%s:%s> : Processing Arguments:(%s)",
    MOD, meth, tostring(argListMap));

  for name, value in map.pairs( argListMap ) do
      GP=F and trace("[DEBUG]: <%s:%s> : Processing Arg: Name(%s) Val(%s)",
          MOD, meth, tostring( name ), tostring( value ));

      -- Process our "prepackaged" settings first:
      -- (*) StandardList: Generic starting mode.
      -- (*) TestMode List Mode: Small sizes to exercise the structure.
      -- (*) TestMode Binary Mode: Employ UDF transform and use small sizes
      --     to exercise the structure.
      -- (*) Production Customer Settings:
      --     - Binary Mode, Compress UDF, High Performance Settings.
      -- NOTE: Eventually, these "packages" will be installed in either
      -- a separate "package" lua file, or possibly in the UdfFunctionTable.
      -- Regardless though -- they will move out of this main file, except
      -- maybe for the "standard" packages.
      if name == "Package" and type( value ) == "string" then
        -- Figure out WHICH package we're going to deploy:
        if value == PackageStandardList then
            packageStandardList( lsoMap );
        elseif value == PackageTestModeList then
            packageTestModeList( lsoMap );
        elseif value == PackageTestModeBinary then
            packageTestModeBinary( lsoMap );
        elseif value == PackageProdListValBinStore then
            packageProdListValBinStore( lsoMap );
        elseif value == PackageDebugModeList then
            packageDebugModeList( lsoMap );
        elseif value == PackageDebugModeBinary then
            packageDebugModeBinary( lsoMap );
        end
      elseif name == "StoreMode" and type( value )  == "string" then
        -- Verify it's a valid value
        if value == SM_LIST or value == SM_BINARY then
          lsoMap.StoreMode = value;
        end
      elseif name == "HotListSize"  and type( value )  == "number" then
        if value >= 10 and value <= 500 then
          lsoMap.HotListMax = value;
        end
      elseif name == "HotListTransfer" and type( value ) == "number" then
        if value >= 2 and value <= ( lsoMap.HotListMax - 2 ) then
          argListMap.HotListTransfer = value;
        end
      elseif name == "ByteEntrySize" and type( value ) == "number" then
        if value > 0 and value <= 4000 then
          lsoMap.LdrByteEntrySize = value;
        end
      end
  end -- for each argument
      
  GP=F and trace("[DEBUG]: <%s:%s> : CTRL Map after Adjust(%s)",
    MOD, meth , tostring(lsoMap));

  GP=F and trace("[EXIT]:<%s:%s>:Dir Map after Init(%s)",
    MOD,meth,tostring(lsoMap));
  return lsoMap
end -- adjustLsoMap

-- ======================================================================
-- validateTopRec( topRec, lsoMap )
-- ======================================================================
-- Validate that the top record looks valid:
-- Get the LSO bin from the rec and check for "MAGIC" (constant MAGIC)
-- Return: true or false
-- ======================================================================
local function  validateTopRec( topRec, lsoMap )
  local thisMap = topRec[lsoMap.BinName];
  if thisMap.Magic == MAGIC then
    return true
  else
    return false
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
  if lsoMap == nil or lsoMap.Magic ~= MAGIC then return "BROKEN MAP"; end;

  local resultMap                = map();
  resultMap.SUMMARY              = "LSO Summary String";
  resultMap.BinName              = lsoMap.BinName;
  resultMap.ItemCount            = lsoMap.ItemCount;
  -- We're not currently tracking NameSpace and Set Correctly
  -- Let's not report this (for now).
  -- resultMap.NameSpace         = lsoMap.NameSpace;
  -- resultMap.Set               = lsoMap.Set;

  -- General LSO Parms:
  resultMap.StoreMode            = lsoMap.StoreMode;
  resultMap.Transform            = lsoMap.Transform;
  resultMap.UnTransform          = lsoMap.UnTransform;

  -- LSO Data Record Chunk Settings:
  resultMap.LdrEntryCountMax     = lsoMap.LdrEntryCountMax;
  resultMap.LdrByteEntrySize     = lsoMap.LdrByteEntrySize;
  resultMap.LdrByteCountMax      = lsoMap.LdrByteCountMax;
  --
  -- Hot Entry List Settings: List of User Entries
  resultMap.HotListMax            = lsoMap.HotListMax;
  resultMap.HotListTransfer       = lsoMap.HotListTransfer;
  resultMap.HotEntryListItemCount = lsoMap.HotEntryListItemCount;

  -- Warm Digest List Settings: List of Digests of LSO Data Records
  resultMap.WarmListMax           = lsoMap.WarmListMax;
  resultMap.WarmListTransfer      = lsoMap.WarmListTransfer;
  resultMap.WarmListDigestCount   = lsoMap.WarmListDigestCount;

  -- Cold Directory List Settings: List of Directory Pages
  resultMap.ColdListMax           = lsoMap.ColdListMax;
  resultMap.ColdListDirRecCount   = lsoMap.ColdListDirRecCount;
  resultMap.ColdListDataRecCount  = lsoMap.ColdListDataRecCount;

  return tostring( resultMap );
end -- lsoSummary()

-- ======================================================================
-- Summarize the List (usually ResultList) so that we don't create
-- huge amounts of crap in the console.
-- Show Size, First Element, Last Element
-- ======================================================================
local function summarizeList( myList )
  if( myList == nil ) then return "NULL LIST"; end;

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
  if( ldrChunkRecord  == nil ) then return "NULL CHUNK RECORD"; end;

  local resultMap = map();
  local ldrCtrlMap = ldrChunkRecord['LdrControlBin'];
  resultMap.StoreMode = ldrCtrlMap.StoreMode;
  resultMap.Digest   = ldrCtrlMap.Digest;
  resultMap.ListSize = list.size( ldrChunkRecord['LdrListBin'] );
  resultMap.WarmList = ldrChunkRecord['LdrListBin'];
  resultMap.ByteCountMax   = ldrCtrlMap.ByteCountMax;

  return tostring( resultMap );
end -- ldrChunkSummary()


-- ======================================================================
-- coldDirSummary( coldDirPage )
-- ======================================================================
-- Print out interesting stats about this Cold Directory Page
-- ======================================================================
local function  coldDirSummary( coldDirPage )
  if( coldDirPage  == nil ) then return "NULL DIR PAGE"; end;

  local resultMap = map();
  local coldDirMap = coldDirPage['ColdDirCtrlBin'];

  return tostring( coldDirMap );
end -- coldDirSummary()

-- ======================================================================
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- General LIST Read/Write(entry list, digest list) and LDR FUNCTIONS
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- The same mechanisms are used in different contexts.  The HotList
-- Entrylist -- is similar to the EntryList in the Warm List.  The 
-- DigestList in the WarmList is similar to the ColdDir digest list in
-- the Cold List.  LDRs pointed to in the Warmlist are the same as the
-- LDRs pointed to in the cold list.

-- ======================================================================
-- readEntryList()
-- ======================================================================
-- This method reads the entry list from Hot, Warm and Cold Lists.
-- It examines each entry, applies the inner UDF function (if applicable)
-- and appends viable candidates to the result list.
-- As always, since we are doing a stack, everything is in LIFO order, 
-- which means we always read back to front.
-- Parms:
--   (*) resultList:
--   (*) lsoMap: 
--   (*) entryList:
--   (*) count:
--   (*) func:
--   (*) fargs:
--   (*) all:
-- Return:
--   Implicit: entries are added to the result list
--   Explicit: Number of Elements Read.
-- ======================================================================
local function readEntryList( resultList, lsoMap, entryList, count,
    func, fargs, all)

  local meth = "readEntryList()";
  GP=F and trace("[ENTER]: <%s:%s> Count(%s) func(%s) fargs(%s) all(%s)",
      MOD,meth,tostring(count), tostring(func), tostring(fargs),tostring(all));

  local doUnTransform = false; 
  if( lsoMap.UnTransform ~= nil ) then
    doUnTransform = true; 
  end

  local applyFilter = false;
  if func ~= nil and fargs ~= nil then
    applyFilter = true;
  end


  -- Iterate thru the entryList, gathering up items in the result list.
  -- There are two modes:
  -- (*) ALL Mode: Read the entire list, return all that qualify
  -- (*) Count Mode: Read <count> or <entryListSize>, whichever is smaller
  local numRead = 0;
  local numToRead = 0;
  local listSize = list.size( entryList );
  if all == true or count >= listSize then
    numToRead = listSize;
  else
    numToRead = count;
  end

  -- Read back to front (LIFO order), up to "numToRead" entries
  local readValue;
  for i = listSize, 1, -1 do

    -- Apply the transform to the item, if present
    if doUnTransform == true then -- apply the transform
      readValue = functionTable[lsoMap.UnTransform]( entryList[i] );
    else
      readValue = entryList[i];
    end

    -- After the transform, we can apply the filter, if it is present.  If
    -- the value passes the filter (or if there is no filter), then add it
    -- to the resultList.
    local resultValue;
    if applyFilter == true then
      resultValue = functionTable[func]( readValue, fargs );
    else
      resultValue = readValue;
    end

    list.append( resultList, readValue );
--    GP=F and trace("[DEBUG]:<%s:%s>Appended Val(%s) to ResultList(%s)",
--      MOD, meth, tostring( readValue ), tostring(resultList) );
    
    numRead = numRead + 1;
    if numRead >= numToRead and all == false then
      GP=F and trace("[Early EXIT]: <%s:%s> NumRead(%d) resultListSummary(%s)",
        MOD, meth, numRead, summarizeList( resultList ));
      return numRead;
    end
  end -- for each entry in the list

  GP=F and trace("[EXIT]: <%s:%s> NumRead(%d) resultListSummary(%s) ",
    MOD, meth, numRead, summarizeList( resultList ));
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
local function readByteArray( resultList, lsoMap, ldrChunk, count,
                              func, fargs, all)
  local meth = "readByteArray()";
  GP=F and trace("[ENTER]: <%s:%s> Count(%s) func(%s) fargs(%s) all(%s)",
    MOD,meth,tostring(count), tostring(func), tostring(fargs), tostring(all));
            
  local doUnTransform = false;
  if( lsoMap.UnTransform ~= nil ) then
    doUnTransform = true;
  end

  local applyFilter = false;
  if func ~= nil and fargs ~= nil then
    applyFilter = true;
  end

  -- Note: functionTable is "global" to this module, defined at top of file.

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
  if all == true or count >= listSize then
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
  -- To Read:  Start Here ------+  (at the beginning of the LAST entry)
  --           and move BACK towards the front.
  local readValue;
  local byteValue;
  local byteIndex = 0; -- our direct position in the byte array.
  GP=F and trace("[DEBUG]:<%s:%s>Starting loop Byte Array(%s) ListSize(%d)",
      MOD, meth, tostring(byteArray), listSize );
  for i = (listSize - 1), 0, -1 do

    byteIndex = 1 + (i * entrySize);
    byteValue = bytes.get_bytes( byteArray, byteIndex, entrySize );

    GP=F and trace("[DEBUG]:<%s:%s>: In Loop: i(%d) BI(%d) BV(%s)",
      MOD, meth, i, byteIndex, tostring( byteValue ));

    -- Apply the UDF to the item, if present, and if result NOT NULL, then
    if doUnTransform == true then -- apply the "UnTransform" function
      readValue = functionTable[lsoMap.UnTransform]( byteValue );
    else
      readValue = byteValue;
    end

    -- After the transform, we can apply the filter, if it is present.  If
    -- the value passes the filter (or if there is no filter), then add it
    -- to the resultList.
    local resultValue;
    if applyFilter == true then
      resultValue = functionTable[func]( readValue, fargs );
    else
      resultValue = readValue;
    end

    -- If the value passes the filter (or if there is no filter), then add
    -- it to the result list.
    if( resultValue ~= nil ) then
      list.append( resultList, resultValue );
    end

    GP=F and trace("[DEBUG]:<%s:%s>Appended Val(%s) to ResultList(%s)",
      MOD, meth, tostring( readValue ), tostring(resultList) );
    
    numRead = numRead + 1;
    if numRead >= numToRead and all == false then
      GP=F and trace("[Early EXIT]: <%s:%s> NumRead(%d) resultList(%s)",
        MOD, meth, numRead, tostring( resultList ));
      return numRead;
    end
  end -- for each entry in the list (packed byte array)

  GP=F and trace("[EXIT]: <%s:%s> NumRead(%d) resultListSummary(%s) ",
    MOD, meth, numRead, summarizeList( resultList ));
  return numRead;
end -- readByteArray()

-- ======================================================================
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- LSO Data Record (LDR) "Chunk" FUNCTIONS
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- LDR routines act specifically on the LDR "Data Chunk" records.

-- ======================================================================
-- ldrChunkInsertList( topWarmChunk, lsoMap, listIndex,  insertList )
-- ======================================================================
-- Insert (append) the LIST of values (overflow from the HotList) 
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
  local meth = "ldrChunkInsertList()";
  GP=F and trace("[ENTER]: <%s:%s> Index(%d) List(%s)",
    MOD, meth, listIndex, tostring( insertList ) );

  local ldrCtrlMap = ldrChunkRec['LdrControlBin'];
  local ldrValueList = ldrChunkRec['LdrListBin'];
  local chunkIndexStart = list.size( ldrValueList ) + 1;
  local ldrByteArray = ldrChunkRec['LdrBinaryBin']; -- might be nil

  GP=F and trace("[DEBUG]: <%s:%s> Chunk: CTRL(%s) List(%s)",
    MOD, meth, tostring( ldrCtrlMap ), tostring( ldrValueList ));

  -- Note: Since the index of Lua arrays start with 1, that makes our
  -- math for lengths and space off by 1. So, we're often adding or
  -- subtracting 1 to adjust.
  local totalItemsToWrite = list.size( insertList ) + 1 - listIndex;
  local itemSlotsAvailable = (ldrCtrlMap.ListEntryMax - chunkIndexStart) + 1;

  -- In the unfortunate case where our accounting is bad and we accidently
  -- opened up this page -- and there's no room -- then just return ZERO
  -- items written, and hope that the caller can deal with that.
  if itemSlotsAvailable <= 0 then
    warn("[ERROR]: <%s:%s> INTERNAL ERROR: No space available on chunk(%s)",
      MOD, meth, tostring( ldrCtrlMap ));
    return 0; -- nothing written
  end

  -- If we EXACTLY fill up the chunk, then we flag that so the next Warm
  -- List Insert will know in advance to create a new chunk.
  if totalItemsToWrite == itemSlotsAvailable then
    lsoMap.WarmTopFull = 1; -- Now, remember to reset on next update.
    GP=F and trace("[DEBUG]:<%s:%s>TotalItems(%d) == SpaceAvail(%d):Top FULL!!",
      MOD, meth, totalItemsToWrite, itemSlotsAvailable );
  end

  GP=F and trace("[DEBUG]: <%s:%s> TotalItems(%d) SpaceAvail(%d)",
    MOD, meth, totalItemsToWrite, itemSlotsAvailable );

  -- Write only as much as we have space for
  local newItemsStored = totalItemsToWrite;
  if totalItemsToWrite > itemSlotsAvailable then
    newItemsStored = itemSlotsAvailable;
  end

  -- This is List Mode.  Easy.  Just append to the list.
  GP=F and trace("[DEBUG]:<%s:%s>:ListMode:Copying From(%d) to (%d) Amount(%d)",
    MOD, meth, listIndex, chunkIndexStart, newItemsStored );

  -- Special case of starting at ZERO -- since we're adding, not
  -- directly indexing the array at zero (Lua arrays start at 1).
  for i = 0, (newItemsStored - 1), 1 do
    list.append( ldrValueList, insertList[i+listIndex] );
  end -- for each remaining entry

  GP=F and trace("[DEBUG]: <%s:%s>: Post Chunk Copy: Ctrl(%s) List(%s)",
    MOD, meth, tostring(ldrCtrlMap), tostring(ldrValueList));

  -- Store our modifications back into the Chunk Record Bins
  ldrChunkRec['LdrControlBin'] = ldrCtrlMap;
  ldrChunkRec['LdrListBin'] = ldrValueList;

  GP=F and trace("[EXIT]: <%s:%s> newItemsStored(%d) List(%s) ",
    MOD, meth, newItemsStored, tostring( ldrValueList) );
  return newItemsStored;
end -- ldrChunkInsertList()


-- ======================================================================
-- ldrChunkInsertBytes( topWarmChunk, lsoMap, listIndex,  insertList )
-- ======================================================================
-- Insert (append) the LIST of values (overflow from the HotList) 
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
  local meth = "ldrChunkInsertBytes()";
  GP=F and trace("[ENTER]: <%s:%s> Index(%d) List(%s)",
    MOD, meth, listIndex, tostring( insertList ) );

  local ldrCtrlMap = ldrChunkRec['LdrControlBin'];
  GP=F and trace("[DEBUG]: <%s:%s> Check LDR CTRL MAP(%s)",
    MOD, meth, tostring( ldrCtrlMap ) );

  local entrySize = ldrCtrlMap.ByteEntrySize;
  if( entrySize <= 0 ) then
    warn("[ERROR]: <%s:%s>: Internal Error:. Negative Entry Size", MOD, meth);
    -- Let the caller handle the error.
    return -1; -- General Badness
  end

  local entryCount = 0;
  if( ldrCtrlMap.ByteEntryCount ~= nil and ldrCtrlMap.ByteEntryCount ~= 0 ) then
    entryCount = ldrCtrlMap.ByteEntryCount;
  end
  GP=F and trace("[DEBUG]:<%s:%s>Using EntryCount(%d)", MOD, meth, entryCount );

  -- Note: Since the index of Lua arrays start with 1, that makes our
  -- math for lengths and space off by 1. So, we're often adding or
  -- subtracting 1 to adjust.
  -- Calculate how much space we have for items.  We could do this in bytes
  -- or items.  Let's do it in items.
  local totalItemsToWrite = list.size( insertList ) + 1 - listIndex;
  local maxEntries = math.floor(ldrCtrlMap.ByteCountMax / entrySize );
  local itemSlotsAvailable = maxEntries - entryCount;
  GP=F and
    trace("[DEBUG]: <%s:%s>:MaxEntries(%d) SlotsAvail(%d) #Total ToWrite(%d)",
    MOD, meth, maxEntries, itemSlotsAvailable, totalItemsToWrite );

  -- In the unfortunate case where our accounting is bad and we accidently
  -- opened up this page -- and there's no room -- then just return ZERO
  -- items written, and hope that the caller can deal with that.
  if itemSlotsAvailable <= 0 then
    warn("[DEBUG]: <%s:%s> INTERNAL ERROR: No space available on chunk(%s)",
    MOD, meth, tostring( ldrCtrlMap ));
    return 0; -- nothing written
  end

  -- If we EXACTLY fill up the chunk, then we flag that so the next Warm
  -- List Insert will know in advance to create a new chunk.
  if totalItemsToWrite == itemSlotsAvailable then
    lsoMap.WarmTopFull = 1; -- Remember to reset on next update.
    GP=F and trace("[DEBUG]:<%s:%s>TotalItems(%d) == SpaceAvail(%d):Top FULL!!",
      MOD, meth, totalItemsToWrite, itemSlotsAvailable );
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
    GP=F and trace("[DEBUG]:<%s:%s>Allocated NEW BYTES: Size(%d) ByteArray(%s)",
      MOD, meth, totalSpaceNeeded, tostring(ldrChunkRec['LdrBinaryBin']));
  else
    GP=F and
    trace("[DEBUG]:<%s:%s>Before: Extending BYTES: New Size(%d) ByteArray(%s)",
      MOD, meth, totalSpaceNeeded, tostring(ldrChunkRec['LdrBinaryBin']));

    bytes.set_len(ldrChunkRec['LdrBinaryBin'], totalSpaceNeeded );

    GP=F and
    trace("[DEBUG]:<%s:%s>AFTER: Extending BYTES: New Size(%d) ByteArray(%s)",
      MOD, meth, totalSpaceNeeded, tostring(ldrChunkRec['LdrBinaryBin']));
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
    MOD, meth, totalItemsToWrite, itemSlotsAvailable, chunkByteStart );

  local byteIndex;
  local insertItem;
  for i = 0, (newItemsStored - 1), 1 do
    byteIndex = chunkByteStart + (i * entrySize);
    insertItem = insertList[i+listIndex];

    GP=F and
    trace("[DEBUG]:<%s:%s>ByteAppend:Array(%s) Entry(%d) Val(%s) Index(%d)",
      MOD, meth, tostring( chunkByteArray), i, tostring( insertItem ),
      byteIndex );

    bytes.put_bytes( chunkByteArray, byteIndex, insertItem );

    GP=F and trace("[DEBUG]: <%s:%s> Post Append: ByteArray(%s)",
      MOD, meth, tostring(chunkByteArray));
  end -- for each remaining entry

  -- Update the ctrl map with the new count
  ldrCtrlMap.ByteEntryCount = entryCount + newItemsStored;

  GP=F and trace("[DEBUG]: <%s:%s>: Post Chunk Copy: Ctrl(%s) List(%s)",
    MOD, meth, tostring(ldrCtrlMap), tostring( chunkByteArray ));

  -- Store our modifications back into the Chunk Record Bins
  ldrChunkRec['LdrControlBin'] = ldrCtrlMap;
  ldrChunkRec['LdrBinaryBin'] = chunkByteArray;

  GP=F and trace("[EXIT]: <%s:%s> newItemsStored(%d) List(%s) ",
    MOD, meth, newItemsStored, tostring( chunkByteArray ));
  return newItemsStored;
end -- ldrChunkInsertBytes()

-- ======================================================================
-- ldrChunkInsert( topWarmChunk, lsoMap, listIndex,  insertList )
-- ======================================================================
-- Insert (append) the LIST of values (overflow from the HotList) 
-- Call the appropriate method "InsertList()" or "InsertBinary()" to
-- do the storage, based on whether this page is in SM_LIST mode or
-- SM_BINARY mode.
--
-- Parms:
-- (*) ldrChunkRec: Hotest of the Warm Chunk Records
-- (*) lsoMap: the LSO control information
-- (*) listIndex: Index into <insertList> from where we start copying.
-- (*) insertList: The list of elements to be copied in
-- Return: Number of items written
-- ======================================================================
local function ldrChunkInsert(ldrChunkRec,lsoMap,listIndex,insertList )
  local meth = "ldrChunkInsert()";
  GP=F and trace("[ENTER]: <%s:%s> Index(%d) List(%s)",
    MOD, meth, listIndex, tostring( insertList ) );

  if lsoMap.StoreMode == SM_LIST then
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
  local meth = "ldrHasRoom()";
  GP=F and trace("[ENTER]: <%s:%s> ldr(%s) newValue(%s) ",
    MOD, meth, tostring(ldr), tostring(newValue) );

  local result = 1;  -- Be optimistic 

  -- TODO: ldrHashRoom() This needs to look at SIZES in the case of
  -- BINARY mode.  For LIST MODE, this will work.
  if list.size( ldr.EntryList ) >= ldr.ListEntryMax then
    result = 0;
  end

  GP=F and trace("[EXIT]: <%s:%s> result(%d) ", MOD, meth, result );
  return result;
end -- ldrHasRoom()


-- ======================================================================
-- ldrChunkRead( ldrChunk, resultList, lsoMap, count, func, fargs, all );
-- ======================================================================
-- Read ALL, or up to 'count' items from this chunk, process the inner UDF 
-- function (if present) and, for those elements that qualify, add them
-- to the result list.  Read the chunk in FIFO order.
-- Parms:
-- (*) ldrChunk: Record object for the warm or cold LSO Data Record
-- (*) resultList: What's been accumulated so far -- add to this
-- (*) lsoMap: Main LSO Control info
-- (*) count: Only used when "all" flag is false.  Return this many items
-- (*) func: Optional Inner UDF function to filter read items
-- (*) fargs: Function Argument list for inner UDF
-- Return: the NUMBER of items read from this chunk.
-- ======================================================================
local function ldrChunkRead( ldrChunk, resultList, lsoMap, count,
                             func, fargs, all )
  local meth = "ldrChunkRead()";
  GP=F and trace("[ENTER]: <%s:%s> Count(%d) All(%s)",
      MOD, meth, count, tostring(all));

  -- If the page is SM_BINARY mode, then we're using the "Binary" Bin
  -- 'LdrBinaryBin', otherwise we're using the "List" Bin 'LdrListBin'.
  local chunkMap = ldrChunk['LdrControlBin'];
  local numRead = 0;
  if chunkMap.StoreMode == SM_LIST then
    local chunkList = ldrChunk['LdrListBin'];
    numRead = readEntryList(resultList, lsoMap, chunkList, count,
                            func, fargs, all);
  else
    numRead = readByteArray(resultList, lsoMap, ldrChunk, count,
                            func, fargs, all);
  end

  GP=F and trace("[EXIT]: <%s:%s> NumberRead(%d) ResultListSummary(%s) ",
    MOD, meth, numRead, summarizeList( resultList ));
  return numRead;
end -- ldrChunkRead()
-- ======================================================================

-- ======================================================================
-- digestListRead(topRec, resultList, lsoMap, Count, func, fargs, all);
-- ======================================================================
-- Synopsis:
-- Parms:
-- (*) topRec: User-level Record holding the LSO Bin
-- (*) resultList: What's been accumulated so far -- add to this
-- (*) lsoMap: Main LSO Control info
-- (*) digestList: The List of Digests (Data Record Ptrs) we will Process
-- (*) count: Only used when "all" flag is 0.  Return this many items
-- (*) func: Optional Inner UDF function to apply to read items
-- (*) fargs: Function Argument list for inner UDF
-- (*) all: When == true, read all items, regardless of "count".
-- Return: Return the amount read from the Digest List.
-- ======================================================================
local function digestListRead(topRec, resultList, lsoMap, digestList, count,
                           func, fargs, all)
  local meth = "digestListRead()";
  GP=F and trace("[ENTER]: <%s:%s> Count(%d) all(%s)",
      MOD, meth, count, tostring(all) );

  GP=F and trace("[DEBUG]: <%s:%s> Count(%d) DigList(%s) ResList(%s)",
      MOD, meth, count, tostring( digestList), tostring( resultList ));

  -- Process the DigestList bottom to top, pulling in each digest in
  -- turn, opening the chunk and reading records (as necessary), until
  -- we've read "count" items.  If the 'all' flag is true, then read 
  -- everything.
  -- NOTE: This method works for both the Warm and Cold lists.

  -- If we're using the "all" flag, then count just doesn't work.  Try to
  -- ignore counts entirely when the ALL flag is set.
  if all == true or count < 0 then count = 0; end
  local remaining = count;
  local totalAmountRead = 0;
  local chunkItemsRead = 0;
  local dirCount = list.size( digestList );
  local ldrChunk;
  local stringDigest;
  local status = 0;

  GP=F and trace("[DEBUG]:<%s:%s>:DirCount(%d)  Reading DigestList(%s)",
    MOD, meth, dirCount, tostring( digestList) );

  -- Read each Data Chunk, adding to the resultList, until we either bypass
  -- the readCount, or we hit the end (either readCount is large, or the ALL
  -- flag is set).
  for dirIndex = dirCount, 1, -1 do
    -- Record Digest MUST be in string form
    stringDigest = tostring(digestList[ dirIndex ]);
    GP=F and trace("[DEBUG]: <%s:%s>: Opening Data Chunk:Index(%d)Digest(%s):",
    MOD, meth, dirIndex, stringDigest );
    ldrChunk = aerospike:open_subrec( topRec, stringDigest );
    
    -- resultList is passed by reference and we can just add to it.
    chunkItemsRead =
    ldrChunkRead( ldrChunk, resultList, lsoMap, remaining, func, fargs, all );
    totalAmountRead = totalAmountRead + chunkItemsRead;

    GP=F and
    trace("[DEBUG]:<%s:%s>:after ChunkRead:NumRead(%d)DirIndex(%d)ResList(%s)", 
      MOD, meth, chunkItemsRead, dirIndex, tostring( resultList ));
    -- Early exit ONLY when ALL flag is not set.
    if( all == false and
      ( chunkItemsRead >= remaining or totalAmountRead >= count ) )
    then
      GP=F and trace("[Early EXIT]:<%s:%s>totalAmountRead(%d) ResultList(%s) ",
        MOD, meth, totalAmountRead, tostring(resultList));
      status = aerospike:close_subrec( topRec, ldrChunk );
      return totalAmountRead;
    end

    status = aerospike:close_subrec( topRec, ldrChunk );
    GP=F and trace("[DEBUG]: <%s:%s> as:close() status(%s) ",
    MOD, meth, tostring( status ) );

    -- Get ready for the next iteration.  Adjust our numbers for the
    -- next round
    remaining = remaining - chunkItemsRead;
  end -- for each Data Chunk Record

  GP=F and trace("[EXIT]: <%s:%s> totalAmountRead(%d) ResultListSummary(%s) ",
  MOD, meth, totalAmountRead, summarizeList(resultList));
  return totalAmountRead;
end -- digestListRead()


-- ======================================================================
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- HOT LIST FUNCTIONS
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- The Hot List is an USER DATA ENTRY list that is managed IN THE RECORD.
-- The top N (most recent) values are held in the record, and then they
-- are aged out into the Warm List (a list of data pages) as they are
-- replaced by newer (more recent) data entries.  Hot List functions
-- directly manage the user data - and always in LIST form (not in
-- compact binary form).

-- ======================================================================
-- hotListRead( resultList, lsoMap, count, func, fargs );
-- ======================================================================
-- Parms:
-- (*) ldrChunk: Record object for the warm or cold LSO Data Record
-- (*) resultList: What's been accumulated so far -- add to this
-- (*) count: Only used when "all" flag is false.  Return this many items
-- (*) func: Optional Inner UDF function to apply to read items
-- (*) fargs: Function Argument list for inner UDF
-- Return 'count' items from the Hot List
local function hotListRead( resultList, lsoMap, count, func, fargs, all)
  local meth = "hotListRead()";
  GP=F and trace("[ENTER]:<%s:%s>Count(%d) All(%s)",
      MOD, meth, count, tostring( all ) );

  local hotList = lsoMap.HotEntryList;

  local numRead =
    readEntryList(resultList, lsoMap, hotList, count, func, fargs, all);

  GP=F and trace("[EXIT]:<%s:%s>resultListSummary(%s)",
    MOD, meth, summarizeList(resultList) );
  return resultList;
end -- hotListRead()
-- ======================================================================

-- ======================================================================
-- extractHotListTransferList( lsoMap );
-- ======================================================================
-- Extract the oldest N elements (as defined in lsoMap) and create a
-- list that we return.  Also, reset the HotList to exclude these elements.
-- list.drop( mylist, firstN ).
-- Recall that the oldest element in the list is at index 1, and the
-- newest element is at index N (max).
-- NOTE: We may need to wait to collapse this list until AFTER we know
-- that the underlying SUB_RECORD operations have succeeded.
-- ======================================================================
local function extractHotListTransferList( lsoMap )
  local meth = "extractHotListTransferList()";
  GP=F and trace("[ENTER]: <%s:%s> ", MOD, meth );

  -- Get the first N (transfer amount) list elements
  local transAmount = lsoMap.HotListTransfer;
  local oldHotEntryList = lsoMap.HotEntryList;
  local newHotEntryList = list();
  local resultList = list.take( oldHotEntryList, transAmount );

  -- Now that the front "transAmount" elements are gone, move the remaining
  -- elements to the front of the array (OldListSize - trans).
  for i = 1, list.size(oldHotEntryList) - transAmount, 1 do 
    list.append( newHotEntryList, oldHotEntryList[i+transAmount] );
  end

  GP=F and trace("[DEBUG]:<%s:%s>OldHotList(%s) NewHotList(%s) ResultList(%s)",
    MOD, meth, tostring(oldHotEntryList), tostring(newHotEntryList),
    tostring(resultList));

  -- Point to the new Hot List and update the Hot Count.
  lsoMap.HotEntryList = newHotEntryList;
  oldHotEntryList = nil;
  lsoMap.HotEntryListItemCount = lsoMap.HotEntryListItemCount - transAmount;

  GP=F and trace("[EXIT]: <%s:%s> ResultList(%s)",
    MOD, meth, summarizeList(resultList));
  return resultList;
end -- extractHotListTransferList()


-- ======================================================================
-- hotListHasRoom( lsoMap, insertValue )
-- ======================================================================
-- Return true if there's room, otherwise return false.
-- (*) lsoMap: the map for the LSO Bin
-- (*) insertValue: the new value to be pushed on the stack
local function hotListHasRoom( lsoMap, insertValue )
  local meth = "hotListHasRoom()";
  GP=F and trace("[ENTER]: <%s:%s> : ", MOD, meth );
  local result = true;  -- This is the usual case

  local hotListLimit = lsoMap.HotListMax;
  local hotList = lsoMap.HotEntryList;
  if list.size( hotList ) >= hotListLimit then
    return false;
  end

  GP=F and trace("[EXIT]: <%s:%s> Result(%s) : ", MOD, meth, tostring(result));
  return result;
end -- hotListHasRoom()
-- ======================================================================

--
-- ======================================================================
-- hotListInsert( lsoMap, newStorageValue  )
-- ======================================================================
-- Insert a value at the end of the Hot Entry List.  The caller has 
-- already verified that space exists, so we can blindly do the insert.
--
-- The MODE of storage depends on what we see in the valueMap.  If the
-- valueMap holds a BINARY type, then we are going to store it in a special
-- binary bin.  Here are the cases:
-- (1) Warm List: The Chunk Record employs a List Bin and Binary Bin, where
--    the individual entries are packed.  In the Chunk Record, there is a
--    Map (control information) showing the status of the packed Binary bin.
-- (2) Cold List: Same Chunk format as the Warm List Chunk Record.
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
local function hotListInsert( lsoMap, newStorageValue  )
  local meth = "hotListInsert()";
  GP=F and trace("[ENTER]: <%s:%s> : Insert Value(%s)",
    MOD, meth, tostring(newStorageValue) );

  -- Update the hot list with a new element (and update the map)
  local hotList = lsoMap.HotEntryList;
  list.append( hotList, newStorageValue );
  lsoMap.HotEntryList = hotList;
  -- Update the count (overall count and hot list count)
  local itemCount = lsoMap.ItemCount;
  lsoMap.ItemCount = (itemCount + 1);
  local hotCount = lsoMap.HotEntryListItemCount;
  lsoMap.HotEntryListItemCount = (hotCount + 1);

  GP=F and trace("[EXIT]: <%s:%s> : Map Result(%s)",
    MOD, meth, tostring( lsoMap ) );

  return 0;  -- all is well
end -- hotListInsert()

-- ======================================================================
-- ======================================================================
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- WARM LIST FUNCTIONS
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
--
--
-- ======================================================================
-- warmListChunkCreate( topRec, lsoMap )
-- ======================================================================
-- Create and initialise a new LDR "chunk", load the new digest for that
-- new chunk into the lsoMap (the warm dir list), and return it.
local function   warmListChunkCreate( topRec, lsoMap )
  local meth = "warmListChunkCreate()";
  GP=F and trace("[ENTER]: <%s:%s> ", MOD, meth );

  -- Create the Aerospike Record, initialize the bins: Ctrl, List
  -- Note: All Field Names start with UPPER CASE.
  local newLdrChunkRecord = aerospike:create_subrec( topRec );
  local ldrMap = map();
  local newChunkDigest = record.digest( newLdrChunkRecord );

  initializeLdrMap( topRec, newLdrChunkRecord, ldrMap, lsoMap );

  -- Assign Control info and List info to the LDR bins
  newLdrChunkRecord['LdrControlBin'] = ldrMap;
  newLdrChunkRecord['LdrListBin'] = list();

  GP=F and trace("[DEBUG]: <%s:%s> Chunk Create: CTRL Contents(%s)",
    MOD, meth, tostring(ldrMap) );

  aerospike:update_subrec( topRec, newLdrChunkRecord );

  -- Add our new chunk (the digest) to the WarmDigestList
  GP=F and trace("[DEBUG]: <%s:%s> Appending NewChunk(%s) to WarmList(%s)",
    MOD, meth, tostring(newChunkDigest), tostring(lsoMap.WarmDigestList));
  list.append( lsoMap.WarmDigestList, newChunkDigest );
  GP=F and trace("[DEBUG]: <%s:%s> Post CHunkAppend:NewChunk(%s): LsoMap(%s)",
    MOD, meth, tostring(newChunkDigest), tostring(lsoMap));
   
  -- Increment the Warm Count
  local warmChunkCount = lsoMap.WarmListDigestCount;
  lsoMap.WarmListDigestCount = (warmChunkCount + 1);

  -- NOTE: This may not be needed -- we may wish to update the topRec ONLY
  -- after all of the underlying SUB-REC  operations have been done.
  -- Update the top (LSO) record with the newly updated lsoMap.
  topRec[ lsoMap.BinName ] = lsoMap;

  GP=F and trace("[EXIT]: <%s:%s> Return(%s) ",
    MOD, meth, ldrChunkSummary(newLdrChunkRecord));
  return newLdrChunkRecord;
end --  warmListChunkCreate()
-- ======================================================================

-- ======================================================================
-- extractWarmListTransferList( lsoMap );
-- ======================================================================
-- Extract the oldest N digests from the WarmList (as defined in lsoMap)
-- and create a list that we return.  Also, reset the WarmList to exclude
-- these elements.  -- list.drop( mylist, firstN ).
-- Recall that the oldest element in the list is at index 1, and the
-- newest element is at index N (max).
-- NOTE: We may need to wait to collapse this list until AFTER we know
-- that the underlying SUB-REC  operations have succeeded.
-- ======================================================================
local function extractWarmListTransferList( lsoMap )
  local meth = "extractWarmListTransferList()";
  GP=F and trace("[ENTER]: <%s:%s> ", MOD, meth );

  -- Get the first N (transfer amount) list elements
  local transAmount = lsoMap.WarmListTransfer;
  local oldWarmDigestList = lsoMap.WarmDigestList;
  local newWarmDigestList = list();
  local resultList = list.take( oldWarmDigestList, transAmount );

  -- Now that the front "transAmount" elements are gone, move the remaining
  -- elements to the front of the array (OldListSize - trans).
  for i = 1, list.size(oldWarmDigestList) - transAmount, 1 do 
    list.append( newWarmDigestList, oldWarmDigestList[i+transAmount] );
  end

  GP=F and trace("[DEBUG]:<%s:%s>OldWarmList(%s) NewWarmList(%s)ResList(%s) ",
    MOD, meth, tostring(oldWarmDigestList), tostring(newWarmDigestList),
    tostring(resultList));

  -- Point to the new Warm List and update the Hot Count.
  lsoMap.WarmDigestList = newWarmDigestList;
  oldWarmDigestList = nil;
  lsoMap.WarmListDigestCount = lsoMap.WarmListDigestCount - transAmount;

  GP=F and trace("[EXIT]: <%s:%s> ResultList(%s) LsoMap(%s)",
      MOD, meth, summarizeList(resultList), tostring(lsoMap));

  return resultList;
end -- extractWarmListTransferList()

  
-- ======================================================================
-- warmListHasRoom( lsoMap )
-- ======================================================================
-- Look at the Warm list and return 1 if there's room, otherwise return 0.
-- Parms:
-- (*) lsoMap: the map for the LSO Bin
-- Return: Decision: 1=Yes, there is room.   0=No, not enough room.
local function warmListHasRoom( lsoMap )
  local meth = "warmListHasRoom()";
  local decision = 1; -- Start Optimistic (most times answer will be YES)
  GP=F and trace("[ENTER]: <%s:%s> LSO BIN(%s) Bin Map(%s)", 
    MOD, meth, lsoMap.BinName, tostring( lsoMap ));

  if lsoMap.WarmListDigestCount >= lsoMap.WarmListMax then
    decision = 0;
  end

  GP=F and trace("[EXIT]: <%s:%s> Decision(%d)", MOD, meth, decision );
  return decision;
end -- warmListHasRoom()


-- ======================================================================
-- warmListRead(topRec, resultList, lsoMap, Count, func, fargs, all);
-- ======================================================================
-- Synopsis: Pass the Warm list on to "digestListRead()" and let it do
-- all of the work.
-- Parms:
-- (*) topRec: User-level Record holding the LSO Bin
-- (*) resultList: What's been accumulated so far -- add to this
-- (*) lsoMap: The main structure of the LSO Bin.
-- (*) count: Only used when "all" flag is false.  Return this many items
-- (*) func: Optional Inner UDF function to apply to read items
-- (*) fargs: Function Argument list for inner UDF
-- (*) all: When == 1, read all items, regardless of "count".
-- Return: Return the amount read from the Warm Dir List.
-- ======================================================================
local function warmListRead(topRec, resultList, lsoMap, count, func, fargs, all)
  local digestList = lsoMap.WarmDigestList;
  return digestListRead(topRec, resultList, lsoMap,
                          digestList, count, func, fargs, all);
end


-- ======================================================================
-- warmListGetTop( topRec, lsoMap )
-- ======================================================================
-- Find the digest of the top of the Warm Dir List, Open that record and
-- return that opened record.
-- ======================================================================
local function warmListGetTop( topRec, lsoMap )
  local meth = "warmListGetTop()";
  GP=F and trace("[ENTER]: <%s:%s> ", MOD, meth );

  local warmDigestList = lsoMap.WarmDigestList;
  local stringDigest = tostring( warmDigestList[ list.size(warmDigestList) ]);

  GP=F and trace("[DEBUG]: <%s:%s> Warm Digest(%s) item#(%d)", 
      MOD, meth, stringDigest, list.size( warmDigestList ));

  local topWarmChunk = aerospike:open_subrec( topRec, stringDigest );

  GP=F and trace("[EXIT]: <%s:%s> result(%s) ",
    MOD, meth, ldrChunkSummary( topWarmChunk ) );
  return topWarmChunk;
end -- warmListGetTop()
-- ======================================================================


-- ======================================================================
-- warmListInsert()
-- ======================================================================
-- Insert "entryList", which is a list of data entries, into the warm
-- dir list -- a directory of warm Lso Data Records that will contain 
-- the data entries.
-- Parms:
-- (*) topRec: the top record -- needed if we create a new LDR
-- (*) lsoMap: the control map of the top record
-- (*) entryList: the list of entries to be inserted (as_val or binary)
-- Return: 0 for success, -1 if problems.
-- ======================================================================
local function warmListInsert( topRec, lsoMap, entryList )
  local meth = "warmListInsert()";
  local rc = 0;
  GP=F and trace("[ENTER]: <%s:%s> ", MOD, meth );
--GP=F and trace("[ENTER]: <%s:%s> LSO Summary(%s) ", MOD, meth, lsoSummary(lsoMap) );

  GP=F and trace("[DEBUG 0]:WDL(%s)", tostring( lsoMap.WarmDigestList ));

  local warmDigestList = lsoMap.WarmDigestList;
  local topWarmChunk;
  -- Whether we create a new one or open an existing one, we save the current
  -- count and close the record.
  -- Note that the last write may have filled up the warmTopChunk, in which
  -- case it set a flag so that we will go ahead and allocate a new one now,
  -- rather than after we read the old top and see that it's already full.
  if list.size( warmDigestList ) == 0 or lsoMap.WarmTopFull == 1 then
    GP=F and trace("[DEBUG]: <%s:%s> Calling Chunk Create ", MOD, meth );
    topWarmChunk = warmListChunkCreate( topRec, lsoMap ); -- create new
    lsoMap.WarmTopFull = 0; -- reset for next time.
  else
    GP=F and trace("[DEBUG]: <%s:%s> Calling Get TOP ", MOD, meth );
    topWarmChunk = warmListGetTop( topRec, lsoMap ); -- open existing
  end
  GP=F and trace("[DEBUG]: <%s:%s> Post 'GetTop': LsoMap(%s) ", 
    MOD, meth, tostring( lsoMap ));

  -- We have a warm Chunk -- write as much as we can into it.  If it didn't
  -- all fit -- then we allocate a new chunk and write the rest.
  local totalEntryCount = list.size( entryList );
  GP=F and trace("[DEBUG]: <%s:%s> Calling Chunk Insert: List(%s)",
    MOD, meth, tostring( entryList ));
  local countWritten = ldrChunkInsert( topWarmChunk, lsoMap, 1, entryList );
  if( countWritten == -1 ) then
    warn("[ERROR]: <%s:%s>: Internal Error in Chunk Insert", MOD, meth);
    error('Internal Error on insert(1)');
  end
  local itemsLeft = totalEntryCount - countWritten;
  if itemsLeft > 0 then
    aerospike:update_subrec( topRec, topWarmChunk );
    aerospike:close_subrec( topRec, topWarmChunk );
    GP=F and trace("[DEBUG]:<%s:%s>Calling Chunk Create: AGAIN!!", MOD, meth );
    topWarmChunk = warmListChunkCreate( topRec, lsoMap ); -- create new
    -- Unless we've screwed up our parameters -- we should never have to do
    -- this more than once.  This could be a while loop if it had to be, but
    -- that doesn't make sense that we'd need to create multiple new LDRs to
    -- hold just PART of the hot list.
  GP=F and trace("[DEBUG]: <%s:%s> Calling Chunk Insert: List(%s) AGAIN(%d)",
    MOD, meth, tostring( entryList ), countWritten + 1);
    countWritten =
        ldrChunkInsert( topWarmChunk, lsoMap, countWritten+1, entryList );
    if( countWritten == -1 ) then
      warn("[ERROR]: <%s:%s>: Internal Error in Chunk Insert", MOD, meth);
      error('Internal Error on insert(2)');
    end
    if countWritten ~= itemsLeft then
      warn("[ERROR!!]: <%s:%s> Second Warm Chunk Write: CW(%d) IL(%d) ",
        MOD, meth, countWritten, itemsLeft );
      error('Internal Error on insert(3)');
    end
  end

  -- NOTE: We do NOT have to update the WarmDigest Count here; that is done
  -- in the warmListChunkCreate() call.

  -- All done -- Save the info of how much room we have in the top Warm
  -- chunk (entry count or byte count)
  GP=F and trace("[DEBUG]: <%s:%s> Saving LsoMap (%s) Before Update ",
    MOD, meth, tostring( lsoMap ));
  topRec[lsoMap.BinName] = lsoMap;

  GP=F and trace("[DEBUG]: <%s:%s> Chunk Summary before storage(%s)",
    MOD, meth, ldrChunkSummary( topWarmChunk ));

  GP=F and trace("[DEBUG]: <%s:%s> Calling SUB-REC  Update ", MOD, meth );
  local status = aerospike:update_subrec( topRec, topWarmChunk );
  GP=F and trace("[DEBUG]: <%s:%s> SUB-REC  Update Status(%s) ",MOD,meth, tostring(status));
  GP=F and trace("[DEBUG]: <%s:%s> Calling SUB-REC  Close ", MOD, meth );

  status = aerospike:close_subrec( topRec, topWarmChunk );
  GP=F and trace("[DEBUG]: <%s:%s> SUB-REC  Close Status(%s) ",
    MOD,meth, tostring(status));

  -- Notice that the TOTAL ITEM COUNT of the LSO doesn't change.  We've only
  -- moved entries from the hot list to the warm list.

  return rc;
end -- warmListInsert


-- ======================================================================
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- COLD LIST FUNCTIONS
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
--
-- ======================================================================
-- coldDirHeadCreate()
-- ======================================================================
-- Set up a new Head Directory page for the cold list.  The Cold List Dir
-- pages each hold a list of digests to data pages.  Note that
-- the data pages (LDR pages) are already built from the warm list, so
-- the cold list just holds those LDR digests after the record agest out
-- of the warm list. 
-- Parms:
-- (*) topRec: the top record -- needed when we create a new dir and LDR
-- (*) lsoMap: the control map of the top record
local function coldDirHeadCreate( topRec, lsoMap )
  local meth = "coldDirHeadCreate()";
  GP=F and trace("[ENTER]: <%s:%s>: lsoMap(%s)", MOD, meth, tostring(lsoMap));

  -- Create the Aerospike Record, initialize the bins: Ctrl, List
  -- Note: All Field Names start with UPPER CASE.
  local newColdHead = aerospike:create_subrec( topRec );
  local coldDirMap = map();
  initializeColdDirMap( topRec, newColdHead, coldDirMap, lsoMap );

  -- Update our global counts ==> One more Cold Dir Record.
  local coldDirRecCount = lsoMap.ColdDirRecCount;
  lsoMap.ColdDirRecCount = coldDirRecCount + 1;

  -- Plug this directory into the chain of Dir Records (starting at HEAD).
  newColdHead.NextDirRec = lsoMap.ColdDirListHead;
  lsoMap.ColdDirListHead = coldDirMap.Digest;

  -- Save our updates in the records
  newColdHead['ColdDirListBin'] = list(); -- allocate a new digest list
  newColdHead['ColdDirCtrlBin'] = coldDirMap;

  -- NOTE: We don't want to update the TOP RECORD until we know that
  -- the  underlying children record operations are complete.
  -- We MUST REMEMBER to verify that the top rec gets updated at the end.
  -- TODO: Verify Top Record updated and Saved.
  -- Update the top (LSO) record with the newly updated lsoMap.
  -- topRec[ lsoMap.BinName ] = lsoMap;

  GP=F and trace("[EXIT]: <%s:%s> Return(%s) ",
    MOD, meth, coldDirSummary( newColdHead ));
  return newColdHead;
end --  coldDirHeadCreate()()
-- ======================================================================

-- ======================================================================
-- coldDirRecInsert(lsoMap, coldHeadRec,digestListIndex,digestList);
-- ======================================================================
-- Insert as much as we can of "digestList", which is a list of digests
-- to LDRs, into a -- Cold Directory Page.  Return num written.
-- It is the caller's job to allocate a NEW Dir Rec page if not all of
-- digestList( digestListIndex to end) fits.
-- Parms:
-- (*) lsoMap: the main control structure
-- (*) coldHeadRec: The Cold List Directory Record
-- (*) digestListIndex: The starting Read position in the list
-- (*) digestList: the list of digests to be inserted
-- Return: Number of digests written, -1 for error.
-- ======================================================================
local function coldDirRecInsert(lsoMap,coldHeadRec,digestListIndex,digestList)
  local meth = "coldDirRecInsert()";
  local rc = 0;
  GP=F and trace("[ENTER]:<%s:%s> ColdHead(%s) ColdDigestList(%s)",
      MOD, meth, tostring(coldHeadRec), tostring( digestList ));

  local coldDirMap = coldHeadRec['ColdDirCtrlBin'];
  local coldDirList = coldHeadRec['ColdDirListBin'];
  local coldDirMax = coldDirMap.ColdListMax;

  -- Write as much as we can into this Cold Dir Page.  If this is not the
  -- first time around the startIndex (digestListIndex) may be a value
  -- other than 1 (first position).
  -- Note: Since the index of Lua arrays start with 1, that makes our
  -- math for lengths and space off by 1. So, we're often adding or
  -- subtracting 1 to adjust.
  local totalItemsToWrite = list.size( digestList ) + 1 - digestListIndex;
  local itemSlotsAvailable = (coldDirMax - digestListIndex) + 1;

  -- In the unfortunate case where our accounting is bad and we accidently
  -- opened up this page -- and there's no room -- then just return ZERO
  -- items written, and hope that the caller can deal with that.
  if itemSlotsAvailable <= 0 then
    warn("[ERROR]: <%s:%s> INTERNAL ERROR: No space available on chunk(%s)",
    MOD, meth, tostring( coldDirMap ));
    -- Deal with this at a higher level.
    return -1; -- nothing written, Error.  Bubble up to caller
  end

  -- If we EXACTLY fill up the ColdDirRec, then we flag that so the next Cold
  -- List Insert will know in advance to create a new ColdDirHEAD.
  if totalItemsToWrite == itemSlotsAvailable then
    lsoMap.ColdTopFull = 1; -- Now, remember to reset on next update.
    GP=F and trace("[DEBUG]:<%s:%s>TotalItems(%d) == SpaceAvail(%d):Top FULL!!",
      MOD, meth, totalItemsToWrite, itemSlotsAvailable );
  end

  GP=F and trace("[DEBUG]: <%s:%s> TotalItems(%d) SpaceAvail(%d)",
    MOD, meth, totalItemsToWrite, itemSlotsAvailable );

  -- Write only as much as we have space for
  local newItemsStored = totalItemsToWrite;
  if totalItemsToWrite > itemSlotsAvailable then
    newItemsStored = itemSlotsAvailable;
  end

  -- This is List Mode.  Easy.  Just append to the list.  We don't expect
  -- to have a "binary mode" for just the digest list.  We could, but that
  -- would be extra complexity for very little gain.
  GP=F and trace("[DEBUG]:<%s:%s>:ListMode:Copying From(%d) to (%d) Amount(%d)",
    MOD, meth, digestListIndex, list.size(digestList), newItemsStored );

  -- Special case of starting at ZERO -- since we're adding, not
  -- directly indexing the array at zero (Lua arrays start at 1).
  for i = 0, (newItemsStored - 1), 1 do
    list.append( coldDirList, digestList[i + digestListIndex] );
  end -- for each remaining entry

  -- Update the Count of Digests on the page (should match list size).
  local digestCount = coldDirMap.DigestCount;
  coldDirMap.DigestCount = digestCount + newItemsStored;

  GP=F and trace("[DEBUG]: <%s:%s>: Post digest Copy: Ctrl(%s) List(%s)",
    MOD, meth, tostring(coldDirMap), tostring(coldDirList));

  -- Store our modifications back into the Chunk Record Bins
  coldHeadRec['ColdDirCtrlBin'] = coldDirMap;
  coldHeadRec['ColdDirListBin'] = coldDirList;

  GP=F and trace("[EXIT]: <%s:%s> newItemsStored(%d) Digest List(%s) map(%s)",
    MOD, meth, newItemsStored, tostring( coldDirList), tostring(coldDirMap));

  return newItemsStored;
end -- coldDirRecInsert()


-- ======================================================================
-- coldListInsert()
-- ======================================================================
-- Insert "insertList", which is a list of digest entries, into the cold
-- dir page -- a directory of cold Lso Data Record digests that contain 
-- the actual data entries. Note that the data pages were built when the
-- warm list was created, so all we're doing now is moving the LDR page
-- DIGESTS -- not the data itself.
-- Parms:
-- (*) topRec: the top record -- needed if we create a new LDR
-- (*) lsoMap: the control map of the top record
-- (*) digestList: the list of digests to be inserted (as_val or binary)
-- Return: 0 for success, -1 if problems.
-- ======================================================================
local function coldListInsert( topRec, lsoMap, digestList )
  local meth = "coldListInsert()";
  local rc = 0;

  GP=F and trace("[ENTER]: <%s:%s> LSO Map Contents(%s) ",
      MOD, meth, tostring(lsoMap) );

  GP=F and trace("[DEBUG 0]:WDL(%s)", tostring( lsoMap.WarmDigestList ));

  local transferAmount = list.size( digestList );

  -- If we don't have a cold list, then we have to build one.  Also, if
  -- the current cold Head is completely full, then we also need to add
  -- a new one.
  local stringDigest;
  local coldHeadRec;

  local coldHeadDigest = lsoMap.ColdDirListHead;
  if coldHeadDigest == nil or coldHeadDigest == 0 or lsoMap.ColdTopFull == 1
  then
    -- Create a new Cold Directory Head and link it in the Dir Chain.
    coldHeadRec = coldDirHeadCreate( topRec, lsoMap );
    coldHeadDigest = record.digest( coldHeadRec );
    stringDigest = tostring( coldHeadDigest );
  else
    stringDigest = tostring( coldHeadDigest );
    coldHeadRec = aerospike:open_subrec( topRec, stringDigest );
  end

  local coldHeadCtrlMap = coldHeadRec['ColdDirCtrlBin'];
  local coldHeadList = coldHeadRec['ColdDirListlBin'];

  GP=F and trace("[DEBUG]:<%s:%s>:ColdHeadCtrl(%s) ColdHeadList(%s)",
    MOD, meth, tostring( coldHeadCtrlMap ), tostring( coldHeadList ));

  -- Iterate thru and transfer the "digestList" (which is a list of
  -- LDR data chunk record digests) into the coldDirHead.  If it doesn't all
  -- fit, then create a new coldDirHead and keep going.
  local digestsWritten = 0;
  local digestsLeft = transferAmount;
  local digestListIndex = 1; -- where in the insert list we copy from.
  while digestsLeft > 0 do
    digestsWritten =
      coldDirRecInsert(lsoMap, coldHeadRec, digestListIndex, digestList);
    if( digestsWritten == -1 ) then
      warn("[ERROR]: <%s:%s>: Internal Error in Cold Dir Insert", MOD, meth);
      error('ERROR in Cold List Insert(1)');
    end
    digestsLeft = digestsLeft - digestsWritten;
    digestListIndex = digestListIndex + digestsWritten;
    -- If we have more to do -- then write/close the current coldHeadRec and
    -- allocate ANOTHER one (woo hoo).
    if digestsLeft > 0 then
      aerospike:update_subrec( topRec, coldHeadRec );
      aerospike:close_subrec( topRec, coldHeadRec );
      GP=F and trace("[DEBUG]: <%s:%s> Calling Cold DirHead Create: AGAIN!!",
          MOD, meth );
      coldHeadRec = coldDirHeadCreate( topRec, lsoMap );
    end
  end -- while digests left to write.
  
  -- Update the Cold List Digest Count (add to cold, subtract from warm)
  local coldDataRecCount = lsoMap.ColdDataRecCount;
  lsoMap.ColdDataRecCount = coldDataRecCount + transferAmount;

  local warmListCount = lsoMap.WarmListDigestCount;
  lsoMap.WarmListDigestCount = warmListCount - transferAmount;

  -- All done -- Save the info of how much room we have in the top Warm
  -- chunk (entry count or byte count)
  GP=F and trace("[DEBUG]: <%s:%s> Saving LsoMap (%s) Before Update ",
    MOD, meth, tostring( lsoMap ));
  topRec[lsoMap.BinName] = lsoMap;

  local status = aerospike:update_subrec( topRec, coldHeadRec );
  GP=F and trace("[DEBUG]: <%s:%s> SUB-REC  Update Status(%s) ",
    MOD,meth, tostring(status));

  status = aerospike:close_subrec( topRec, coldHeadRec );
  GP=F and trace("[DEBUG]: <%s:%s> SUB-REC  Close Status(%s) ",
    MOD,meth, tostring(status));

  -- Note: warm->cold transfer only.  No new data added here.
  -- So, no new counts to upate (just warm/cold adjustments).

  return rc;
end -- coldListInsert


-- ======================================================================
-- coldListRead(topRec, resultList, lsoMap, Count, func, fargs, all);
-- ======================================================================
-- Synopsis: March down the Cold List Directory Pages (a linked list of
-- directory pages -- that each point to Lso Data Record "chunks") and
-- read "count" data entries.  Use the same ReadDigestList method as the
-- warm list.
-- Parms:
-- (*) topRec: User-level Record holding the LSO Bin
-- (*) resultList: What's been accumulated so far -- add to this
-- (*) lsoMap: The main structure of the LSO Bin.
-- (*) count: Only used when "all" flag is 0.  Return this many items
-- (*) func: Optional Inner UDF function to apply to read items
-- (*) fargs: Function Argument list for inner UDF
-- (*) all: When == 1, read all items, regardless of "count".
-- Return: Return the amount read from the Cold Dir List.
-- ======================================================================
local function coldListRead(topRec, resultList, lsoMap, count,
                           func, fargs, all)
  local meth = "coldListRead()";
  GP=F and trace("[ENTER]: <%s:%s> Count(%d) All(%s)",
      MOD, meth, count, tostring( all ));

  -- Process the coldDirList (a linked list) head to tail.
  -- For each dir, read in the LDR Records (in reverse list order), and
  -- then each page (in reverse list order), until
  -- we've read "count" items.  If the 'all' flag is true, then read 
  -- everything.
  local dirPageDigest = lsoMap.ColdDirListHead;

  -- Outer loop -- Process each Cold Directory Page.  Each Cold Dir page
  -- holds a list of digests -- just like our WarmDigestList in the
  -- record, so the processing of that will be the same.
  -- Process the Linked List of Dir pages, head to tail
  local numRead = 0;
  local totalNumRead = 0;
  local countRemaining =  count;

  trace("[DEBUG]:<%s:%s>:Starting DirPage Loop: DPDigest(%s)",
      MOD, meth, tostring(dirPageDigest) );

  while dirPageDigest ~= nil and dirPageDigest ~= 0 do
    trace("[DEBUG]:<%s:%s>:Top of DirPage Loop: DPDigest(%s)",
      MOD, meth, tostring(dirPageDigest) );
    -- Open the Directory Page
    local stringDigest = tostring( dirPageDigest ); -- must be a string
    local dirPageRec = aerospike:open_subrec( topRec, stringDigest );
    local digestList = dirPageRec['ColdDirListBin'];
    local dirPageCtrlMap = dirPageRec['ColdDirCtrlBin'];

    GP=F and trace("[DEBUG]:<%s:%s>Looking at subrec digest(%s) Map(%s) L(%s)",
      MOD, meth, stringDigest, tostring(dirPageCtrlMap),tostring(digestList));

    numRead = digestListRead(topRec, resultList, lsoMap, digestList,
                            countRemaining, func, fargs, all)
    if numRead <= 0 then
        warn("[ERROR]:<%s:%s>:Cold List Read Error: Digest(%s)",
          MOD, meth, stringDigest );
          return numRead;
    end

    totalNumRead = totalNumRead + numRead;
    countRemaining = countRemaining - numRead;

    GP=F and trace("[DEBUG]:<%s:%s>:After Read: TotalRead(%d) NumRead(%d)",
          MOD, meth, totalNumRead, numRead );
    GP=F and trace("[DEBUG]:<%s:%s>:CountRemain(%d) NextDir(%s)",
          MOD, meth, countRemaining, tostring(dirPageCtrlMap.NextDirRec));

    if countRemaining <= 0 or dirPageCtrlMap.NextDirRec == 0 then
        GP=F and trace("[EARLY EXIT]:<%s:%s>:Cold Read: (%d) Items",
          MOD, meth, totalNumRead );
        aerospike:close_subrec( topRec, dirPageRec );
        return totalNumRead;
    end

    GP=F and trace("[DEBUG]:<%s:%s>Reading NEXT DIR:", MOD, meth );
    
    -- Ok, so now we've read ALL of the contents of a Directory Record
    -- and we're still not done.  Close the old dir, open the next and
    -- keep going.
    local dirPageCtrlMap = dirPageRec['ColdDirCtrlBin'];

    GP=F and trace("[DEBUG]:<%s:%s>Looking at subrec digest(%s) Map(%s) L(%s)",
      MOD, meth, stringDigest, tostring(dirPageCtrlMap),tostring(digestList));

    dirPageDigest = dirPageCtrlMap.NextDirRec; -- Next in Linked List.
    aerospike:close_subrec( topRec, dirPageRec );

  end -- while Dir Page not empty.

  GP=F and trace("[EXIT]:<%s:%s>totalAmountRead(%d) ResultListSummary(%s) ",
      MOD, meth, totalNumRead, summarizeList(resultList));
  return totalNumRead;
end -- coldListRead()

-- ======================================================================
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- LSO General Functions
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- General Functions that require use of many of the above functions, so
-- they cannot be shoved into any one single category.
-- ======================================================================


-- ======================================================================
-- warmListTransfer()
-- ======================================================================
-- Transfer some amount of the WarmDigestList contents (the list of LSO Data
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
-- (*) topRec: The top level user record (needed for create_subrec)
-- (*) lsoMap
-- Return: Success (0) or Failure (-1)
-- ======================================================================
local function warmListTransfer( topRec, lsoMap )
  local meth = "warmListTransfer()";
  local rc = 0;
  GP=F and trace("[ENTER]: <%s:%s><>>> TRANSFER TO COLD LIST<<<<> lsoMap(%s)",
    MOD, meth, tostring(lsoMap) );

  -- if we haven't yet initialized the cold list, then set up the
  -- first Directory Head (a page of digests to data pages).  Note that
  -- the data pages are already built from the warm list, so all we're doing
  -- here is moving the reference (the digest value) from the warm list
  -- to the cold directory page.

  -- Build the list of items (digests) that we'll be moving from the warm
  -- list to the cold list. Use coldListInsert() to insert them.
  local transferList = extractWarmListTransferList( lsoMap );
  rc = coldListInsert( topRec, lsoMap, transferList );
  GP=F and trace("[EXIT]: <%s:%s> lsoMap(%s) ", MOD, meth, tostring(lsoMap) );
  return rc;
end -- warmListTransfer()


-- ======================================================================
-- local function hotListTransfer( lsoMap, insertValue )
-- ======================================================================
-- The job of hotListTransfer() is to move part of the HotList, as
-- specified by HotListTransferAmount, to LDRs in the warm Dir List.
-- Here's the logic:
-- (1) If there's room in the WarmDigestList, then do the transfer there.
-- (2) If there's insufficient room in the WarmDir List, then make room
--     by transferring some stuff from Warm to Cold, then insert into warm.
local function hotListTransfer( topRec, lsoMap )
  local meth = "hotListTransfer()";
  local rc = 0;
  GP=F and trace("[ENTER]: <%s:%s> LSO Summary(%s) ",
      MOD, meth, lsoSummary(lsoMap) );

  -- if no room in the WarmList, then make room (transfer some of the warm
  -- list to the cold list)
  if warmListHasRoom( lsoMap ) == 0 then
    warmListTransfer( topRec, lsoMap );
  end

  -- Do this the simple (more expensive) way for now:  Build a list of the
  -- items (data entries) that we're moving from the hot list to the warm dir,
  -- then call insertWarmDir() to find a place for it.
  local transferList = extractHotListTransferList( lsoMap );
  rc = warmListInsert( topRec, lsoMap, transferList );

  GP=F and trace("[EXIT]: <%s:%s> result(%d) ", MOD, meth, rc );
  return rc;
end -- hotListTransfer()
-- ======================================================================
--
-- ======================================================================
-- validateBinName(): Validate that the user's bin name for this large
-- object complies with the rules of Aerospike. Currently, a bin name
-- cannot be larger than 14 characters (a seemingly low limit).
-- ======================================================================
local function validateBinName( binName )
  local meth = "validateBinName()";
  GP=F and trace("[ENTER]: <%s:%s> validate Bin Name(%s)",
      MOD, meth, tostring(binName));

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
-- Check that the topRec, the BinName and CrtlMap are valid, otherwise
-- jump out with an error() call. Notice that we look at different things
-- depending on whether or not "mustExist" is true.
-- Parms:
-- (*) topRec:
-- ======================================================================
local function validateRecBinAndMap( topRec, lsoBinName, mustExist )
  local meth = "validateRecBinAndMap()";
  GP=F and trace("[ENTER]:<%s:%s> BinName(%s) ME(%s)",
    MOD, meth, tostring( lsoBinName ), tostring( mustExist ));

  -- Start off with validating the bin name -- because we might as well
  -- flag that error first if the user has given us a bad name.
  validateBinName( lsoBinName );

  -- If "mustExist" is true, then several things must be true or we will
  -- throw an error.
  -- (*) Must have a record.
  -- (*) Must have a valid Bin
  -- (*) Must have a valid Map in the bin.
  --
  -- Otherwise, If "mustExist" is false, then basically we're just going
  -- to check that our bin includes MAGIC, if it is non-nil.
  if mustExist == true then
    -- Check Top Record Existence.
    if( not aerospike:exists( topRec ) and mustExist == true ) then
      warn("[ERROR EXIT]:<%s:%s>:Missing Record. Exit", MOD, meth );
      error('Base Record Does NOT exist');
    end

    -- Control Bin Must Exist
    if( topRec[lsoBinName] == nil ) then
      warn("[ERROR EXIT]: <%s:%s> LSO BIN (%s) DOES NOT Exists",
            MOD, meth, tostring(lsoBinName) );
      error('LSO BIN Does NOT exist');
    end

    -- check that our bin is (mostly) there
    local lsoMap = topRec[lsoBinName]; -- The main lsoMap map
    if lsoMap.Magic ~= MAGIC then
      GP=F and warn("[ERROR EXIT]:<%s:%s>LSO BIN(%s) Corrupted (no magic)",
            MOD, meth, tostring( lsoBinName ) );
      error('LSO BIN Is Corrupted (No Magic::1)');
    end
    -- Ok -- all done for the Must Exist case.
  else
    -- OTHERWISE, we're just checking that nothing looks bad, but nothing
    -- is REQUIRED to be there.  Basically, if a control bin DOES exist
    -- then it MUST have magic.
    if topRec ~= nil and topRec[lsoBinName] ~= nil then
      local lsoMap = topRec[lsoBinName];
      if lsoMap.Magic ~= MAGIC then
        GP=F and warn("[ERROR EXIT]:<%s:%s> LSO BIN(%s) Corrupted (no magic)",
              MOD, meth, tostring( lsoBinName ) );
        error('LSO BIN Is Corrupted (No Magic::2)');
      end
    end -- if worth checking
  end -- else for must exist

end -- validateRecBinAndMap()


-- ======================================================================
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- LSTACK Main Functions
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
--
-- ======================================================================
-- || lstack_create ||
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
-- entries to be held directly in the record -- in the Hot List.  Once the
-- Hot List overflows, the entries flow into the warm list, which is a
-- list of LSO Data Records (each 2k record holds N values, where N is
-- approximately (2k/rec size) ).
-- Once the data overflows the warm list, it flows into the cold list,
-- which is a linked list of directory pages -- where each directory page
-- points to a list of LSO Data Record pages.  Each directory page holds
-- roughly 100 page pointers (assuming a 2k page).
-- Parms (inside argList)
-- (1) topRec: the user-level record holding the LSO Bin
-- (2) lsoBinName: The name of the LSO Bin
-- (3) createSpec: The map (not list) of create parameters
-- Result:
--   rc = 0: ok
--   rc < 0: Aerospike Errors
--
--  NOTE: 
--  !!!! More parms needed here to appropriately configure the LSO
--  -> Package (one of the pre-named packages that hold all the info)
--  OR
--  Individual entries (this is now less attractive)
--  -> Hot List Size
--  -> Hot List Transfer amount
--  -> Warm List Size
--  -> Warm List Transfer amount
-- ========================================================================
function lstack_create( topRec, lsoBinName, createSpec )
  local meth = "stackCreate()";

  if createSpec == nil then
    GP=F and trace("[ENTER1]: <%s:%s> lsoBinName(%s) NULL createSpec",
      MOD, meth, tostring(lsoBinName));
  else
    GP=F and trace("[ENTER2]: <%s:%s> lsoBinName(%s) createSpec(%s) ",
    MOD, meth, tostring( lsoBinName), tostring( createSpec ));
  end

  -- Some simple protection of faulty records or bad bin names
    validateRecBinAndMap( topRec, lsoBinName, false );

  -- Create and initialize the LSO MAP -- the main LSO structure
  -- initializeLsoMap() also assigns the map to the record bin.
  local lsoMap = initializeLsoMap( topRec, lsoBinName );

  -- If the user has passed in settings that override the defaults
  -- (the createSpec), then process that now.
  if createSpec ~= nil then
    adjustLsoMap( lsoMap, createSpec )
  end

  GP=F and trace("[DEBUG]:<%s:%s>:Dir Map after Init(%s)",
    MOD, meth, tostring(lsoMap));

  -- All done, store the record
  local rc = -99; -- Use Odd starting Num: so that we know it got changed
  if( not aerospike:exists( topRec ) ) then
    GP=F and trace("[DEBUG]:<%s:%s>:Create Record()", MOD, meth );
    rc = aerospike:create( topRec );
  else
    GP=F and trace("[DEBUG]:<%s:%s>:Update Record()", MOD, meth );
    rc = aerospike:update( topRec );
  end

  GP=F and trace("[EXIT]: <%s:%s> : Done.  RC(%d)", MOD, meth, rc );
  return rc;
end -- function lstack_create()
-- ======================================================================

-- ======================================================================
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- || local stackPush (with implicit create)
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- Push a value onto the stack.
-- Also, if the LSO Bin does not yet exist, it will be created with the
-- values defined by the "package" name, if specified in the create_spec,
-- otherwise it will be created with the default values.
--
-- Regarding push(). There are different cases, with different
-- levels of complexity:
-- (*) HotListInsert: Instant: Easy
-- (*) WarmListInsert: Result of HotList Overflow:  Medium
-- (*) ColdListInsert: Result of WarmList Overflow:  Complex
-- Parms:
-- (1) topRec: the user-level record holding the LSO Bin
-- (2) lsoBinName: The name of the LSO Bin
-- (3) newValue: The value to be inserted (pushed on the stack)
-- (4) createSpec: The map of create parameters
-- Result:
--   rc = 0: ok
--   rc < 0: Aerospike Errors
-- NOTE: When using info/trace calls, ALL parameters must be protected
-- with "tostring()" so that we do not encounter a format error if the user
-- passes in nil or any other incorrect value/type.
-- ======================================================================
-- =======================================================================
local function localStackPush( topRec, lsoBinName, newValue, createSpec )
  local meth = "localStackPush()";

  -- Note: functionTable is "global" to this module, defined at top of file.

  GP=F and trace("[ENTER1]:<%s:%s>LSO BIN(%s) NewVal(%s) createSpec(%s)",
      MOD, meth, tostring(lsoBinName), tostring( newValue ),
      tostring( createSpec ) );

  -- Some simple protection of faulty records or bad bin names
  validateRecBinAndMap( topRec, lsoBinName, false );

  -- Check for existence, and create if not there.  If we create AND there
  -- is a "createSpec", then configure this LSO appropriately.
  local lsoMap;
  if( not aerospike:exists( topRec ) ) then
    GP=F and trace("[WARNING]:<%s:%s>:Record Does Not exist. Creating",
      MOD, meth );
    lsoMap = initializeLsoMap( topRec, lsoBinName );
    if( createSpec ~= nil ) then
      adjustLsoMap( lsoMap, createSpec );
    end
    aerospike:create( topRec );
  elseif ( topRec[lsoBinName] == nil ) then
    GP=F and trace("[WARNING]: <%s:%s> LSO BIN (%s) DOES NOT Exist: Creating",
                   MOD, meth, tostring(lsoBinName) );
    lsoMap = initializeLsoMap( topRec, lsoBinName );
    if( createSpec ~= nil ) then
      adjustLsoMap( lsoMap, createSpec );
    end
    aerospike:create( topRec );
  else
    -- if the map already exists, then we don't adjust with createSpec.
    lsoMap = topRec[lsoBinName];
  end
  
  -- Now, it looks like we're ready to insert.  If there is a transform
  -- function present, then apply it now.
  -- Note: functionTable is "global" to this module, defined at top of file.
  local newStoreValue;
  if lsoMap.Transform ~= nil  then 
    GP=F and trace("[DEBUG]: <%s:%s> Applying Transform (%s)",
      MOD, meth, tostring(lsoMap.Transform ) );
    newStoreValue = functionTable[lsoMap.Transform]( newValue );
  else
    newStoreValue = newValue;
  end

  -- If we have room, do the simple list insert.  If we don't have
  -- room, then make room -- transfer half the list out to the warm list.
  -- That may, in turn, have to make room by moving some items to the
  -- cold list.
  if hotListHasRoom( lsoMap, newStoreValue ) == false then
    GP=F and trace("[DEBUG]:<%s:%s>: CALLING TRANSFER HOT LIST!!",MOD, meth );
    hotListTransfer( topRec, lsoMap );
  end
  hotListInsert( lsoMap, newStoreValue );
  -- Must always assign the object BACK into the record bin.
  topRec[lsoBinName] = lsoMap;

  -- All done, store the topRec.  Note that this is the ONLY place where
  -- we should be updating the TOP RECORD.  If something fails before here,
  -- we would prefer that the top record remains unchanged.
  local rc = -99; -- Use Odd starting Num: so that we know it got changed
  GP=F and trace("[DEBUG]:<%s:%s>:Update Record", MOD, meth );
  rc = aerospike:update( topRec );

  GP=F and trace("[EXIT]: <%s:%s> : Done.  RC(%d)", MOD, meth, rc );
  return rc
end -- function localStackPush()

-- =======================================================================
-- Stack Push -- with and without implicit create spec.
-- These are the globally visible calls -- that call the local UDF to do
-- all of the work.
-- NOTE: All parameters must be protected with "tostring()" so that we
-- do not encounter a format error if the user passes in nil or any
-- other incorrect value/type
-- =======================================================================
function lstack_push( topRec, lsoBinName, newValue )
  return localStackPush( topRec, lsoBinName, newValue, nil )
end -- end lstack_push()

function lstack_create_and_push( topRec, lsoBinName, newValue, createSpec )
  return localStackPush( topRec, lsoBinName, newValue, createSpec );
end -- lstack_create_and_push()

-- ======================================================================
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- || Local StackPeek: 
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- Return "peekCount" values from the stack, in Stack (LIFO) order.
-- For Each Bin (in LIFO Order), read each Bin in reverse append order.
-- If "peekCount" is zero, then return all.
-- Depending on "peekcount", we may find the elements in:
-- -> Just the HotList
-- -> The HotList and the Warm List
-- -> The HotList, Warm list and Cold list
-- Since our pieces are basically in Stack order, we start at the top
-- (the HotList), then the WarmList, then the Cold List.  We just
-- keep going until we've seen "PeekCount" entries.  The only trick is that
-- we have to read our blocks backwards.  Our blocks/lists are in stack 
-- order, but the data inside the blocks are in append order.
--
-- Parms:
-- (1) topRec: the user-level record holding the LSO Bin
-- (2) lsoBinName: The name of the LSO Bin
-- (3) newValue: The value to be inserted (pushed on the stack)
-- (4) func: The "Inner UDF" that will filter Peek output
-- (5) fargs: Arg List to the filter function (i.e. func(val, fargs)).
-- Result:
--   res = (when successful) List (empty or populated) 
--   res = (when error) nil
-- Note 1: We need to switch to a two-part return, with the first value
-- being the status return code, and the second being the content (or
-- error message).
--
-- NOTE: When using info/trace calls, ALL parameters must be protected
-- with "tostring()" so that we do not encounter a format error if the user
-- passes in nil or any other incorrect value/type.
-- ======================================================================
local function localStackPeek( topRec, lsoBinName, peekCount, func, fargs )
  local meth = "localStackPeek()";

  GP=F and trace("[ENTER1]: <%s:%s> LSO BIN(%s) Count(%s) func(%s) fargs(%s)",
    MOD, meth, tostring(lsoBinName), tostring(peekCount),
    tostring(func), tostring(fargs) );

  -- Some simple protection of faulty records or bad bin names
  validateRecBinAndMap( topRec, lsoBinName, true );
  local lsoMap = topRec[ lsoBinName ];

  -- Build the user's "resultList" from the items we find that qualify.
  -- They must pass the "transformFunction()" filter.
  -- Also, Notice that we go in reverse order -- to get the "stack function",
  -- which is Last In, First Out.
  
  -- When the user passes in a "peekCount" of ZERO, then we read ALL.
  -- Actually -- we will also read ALL if count is negative.
  local all = false;
  local count = 0;
  if peekCount <= 0 then
      all = true;
  else
      count = peekCount;
  end

  -- Set up our answer list.
  local resultList = list(); -- everyone will fill this in

  -- Fetch from the Hot List, then the Warm List, then the Cold List.
  -- Each time we decrement the count and add to the resultlist.
  local resultList = hotListRead(resultList, lsoMap, count, func, fargs, all);
  local numRead = list.size( resultList );
  GP=F and trace("[DEBUG]: <%s:%s> HotListResult:Summary(%s)",
      MOD, meth, summarizeList(resultList));

  local warmCount = 0;

  -- If the list had all that we need, then done.  Return list.
  if(( numRead >= count and all == false) or numRead >= lsoMap.ItemCount) then
    return resultList;
  end

  -- We need more -- get more out of the Warm List.  If ALL flag is set,
  -- keep going until we're done.  Otherwise, compute the correct READ count
  -- given that we've already read from the Hot List.
  local remainingCount = 0; -- Default, when ALL flag is on.
  if( all == false ) then
    remainingCount = count - numRead;
  end
  GP=F and trace("[DEBUG]: <%s:%s> Checking WarmList Count(%d) All(%s)",
    MOD, meth, remainingCount, tostring(all));
  -- If no Warm List, then we're done (assume no cold list if no warm)
  if list.size(lsoMap.WarmDigestList) > 0 then
    warmCount =
      warmListRead(topRec, resultList, lsoMap, remainingCount, func, fargs,all);
  end

  -- As Agent Smith would say... "MORE!!!".
  -- We need more, so get more out of the COLD List.  If ALL flag is set,
  -- keep going until we're done.  Otherwise, compute the correct READ count
  -- given that we've already read from the Hot and Warm Lists.
  local coldCount = 0;
  if( all == false ) then
    remainingCount = count - numRead - warmCount;
      GP=F and trace("[DEBUG]:<%s:%s>After WmRd:A(%s)RC(%d)PC(%d)NR(%d)WC(%d)",
        MOD, meth, tostring(all), remainingCount, count, numRead, warmCount );
  end

  numRead = list.size( resultList );
  -- If we've read enough, then return.
  if((remainingCount <= 0 and all == false) or numRead >= lsoMap.ItemCount)then
      return resultList; -- We have all we need.  Return.
  end

  -- Otherwise, go look for more in the Cold List.
  local coldCount = 
      coldListRead(topRec,resultList,lsoMap,remainingCount,func,fargs,all);

  GP=F and trace("[EXIT]: <%s:%s>: PeekCount(%d) ResultListSummary(%s)",
    MOD, meth, peekCount, summarizeList(resultList));

  return resultList;
end -- function localStackPeek() 

-- =======================================================================
-- lstack_peek() -- with and without inner UDFs
-- These are the globally visible calls -- that call the local UDF to do
-- all of the work.
-- =======================================================================
function lstack_peek( topRec, lsoBinName, peekCount )
  return localStackPeek( topRec, lsoBinName, peekCount, nil, nil )
end -- end lstack_peek()

function lstack_peek_then_filter( topRec, lsoBinName, peekCount, func, fargs )
  return localStackPeek( topRec, lsoBinName, peekCount, func, fargs );
end -- lstack_peek_then_filter()


-- ========================================================================
-- lstack_trim() -- Remove all but the top N elements
-- Parms:
-- (1) topRec: the user-level record holding the LSO Bin
-- (2) lsoBinName: The name of the LSO Bin
-- (3) trimCount: Leave this many elements on the stack
-- Result:
--   rc = 0: ok
--   rc < 0: Aerospike Errors
-- ========================================================================
function lstack_trim( topRec, lsoBinName, trimCount )
  local meth = "lstack_trim()";

  GP=F and trace("[ENTER1]: <%s:%s> lsoBinName(%s) trimCount(%s)",
    MOD, meth, tostring(lsoBinName), tostring( trimCount ));

  -- validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  validateRecBinAndMap( topRec, lsoBinName, true );

  error('Trim() is not yet implemented (Sorry)');

  GP=F and trace("[EXIT]: <%s:%s>", MOD, meth );

  return config;
end -- function lstack_trim()

-- ========================================================================
-- lstack_size() -- return the number of elements (item count) in the stack.
-- Parms:
-- (1) topRec: the user-level record holding the LSO Bin
-- (2) lsoBinName: The name of the LSO Bin
-- Result:
--   rc >= 0  (the size)
--   rc < 0: Aerospike Errors
-- ========================================================================
function lstack_size( topRec, lsoBinName )
  local meth = "lstack_size()";

  GP=F and trace("[ENTER1]: <%s:%s> lsoBinName(%s)",
    MOD, meth, tostring(lsoBinName));

  -- validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  validateRecBinAndMap( topRec, lsoBinName, true );

  local lsoMap = topRec[ lsoBinName ];
  local itemCount = lsoMap.ItemCount;

  GP=F and trace("[EXIT]: <%s:%s> : size(%d)", MOD, meth, itemCount );

  return itemCount;
end -- function lstack_size()

-- ========================================================================
-- lstack_config() -- return the config settings
-- Parms:
-- (1) topRec: the user-level record holding the LSO Bin
-- (2) lsoBinName: The name of the LSO Bin
-- Result:
--   res = (when successful) config Map 
--   res = (when error) nil
-- ========================================================================
function lstack_config( topRec, lsoBinName )
  local meth = "lstack_config()";

  GP=F and trace("[ENTER1]: <%s:%s> lsoBinName(%s)",
    MOD, meth, tostring(lsoBinName));

  -- validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  validateRecBinAndMap( topRec, lsoBinName, true );

  local lsoMap = topRec[ lsoBinName ];
  local config = lsoSummary( lsoMap );

  GP=F and trace("[EXIT]: <%s:%s> : config(%s)", MOD, meth, tostring(config));

  return config;
end -- function lstack_config()

-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
