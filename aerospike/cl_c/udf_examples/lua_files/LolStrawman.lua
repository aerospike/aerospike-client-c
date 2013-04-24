-- Large Ordered List (LLIST) Design
-- LListStrawman_4.22.0:  Last Update April 22, 2013: tjl
--
-- Keep this MOD value in sync with version above
local MOD = "LlistStrawman4.22.0"; -- module name used for tracing.

-- ======================================================================
-- The Large Ordered List is a sorted list, organized according to a Key
-- value.  It is assumed that the stored object is more complex than just an
-- atomic key value -- otherwise one of the other Large Object mechanisms
-- (e.g. Large Stack, Large Set) would be used.  The cannonical form of a
-- LLIST element is a map, which includes a KEY field and other data fields.
--
-- In this first version, we may choose to use a FUNCTION to derrive the 
-- key value from the complex object (e.g. Map).
-- In the first iteration, we will use atomic values and the fixed KEY field
-- for comparisons.
--
-- Compared to Large Stack and Large Set, the Large Ordered List is managed
-- continuously (i.e. it is kept sorted), so there is some additional
-- overhead in the storage operation (to do the insertion sort), but there
-- is reduced overhead for the retieval operation, since it is doing a
-- binary search (order log(N)) rather than scan (order N).
-- ======================================================================
-- Functions Supported
-- (*) llist_create: Create the LLIST structure in the chosen topRec bin
-- (*) llist_insert: Insert a user value (AS_VAL) into the list
-- (*) llist_search: Search the ordered list, using tree search
-- (*) llist_delete: Remove an element from the list
-- ==> The Insert, Search and Delete functions have a "Multi" option,
--     which allows the caller to pass in multiple list keys that will
--     result in multiple operations.  Multi-operations provide higher
--     performance since there can be many operations performed with
--     a single "client-server crossing".
-- (*) llist_multi_insert():
-- (*) llist_multi_search():
-- (*) llist_multi_delete():
-- ==> The Insert and Search functions have the option of passing in a
--     Transformation/Filter UDF that modifies values before storage or
--     modify and filter values during retrieval.
-- (*) llist_insert_with_udf() llist_multi_insert_with_udf():
--     Insert a user value (AS_VAL) in the ordered list, 
--     calling the supplied UDF on the value FIRST to transform it before
--     storing it.
-- (*) llist_search_with_udf, llist_multi_search_with_udf:
--     Retrieve a value from the list. Prior to fetching the
--     item, apply the transformation/filter UDF to the value before
--     adding it to the result list.  If the value doesn't pass the
--     filter, the filter returns nil, and thus it would not be added
--     to the result list.
-- ======================================================================
-- LLIST Design and Type Comments:
--
-- The LLIST value is a new "particle type" that exists ONLY on the server.
-- It is a complex type (it includes infrastructure that is used by
-- server storage), so it can only be viewed or manipulated by Lua and C
-- functions on the server.  It is represented by a Lua MAP object that
-- comprises control information, a directory of records that serve as
-- B+Tree Nodes (either inner nodes or data nodes).
--
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- Here is a sample B+ tree:  There are N keys and (N+1) pointers (digests)
-- in an inner node (including the root).  All of the data resides in the
-- leaves, and the inner nodes are just keys and pointers.
--
--                                   _________
--                                  |_30_|_60_|
--                               _/      |      \_
--                             _/        |        \_
--                           _/          |          \_
--                         _/            |            \_
--                       _/              |              \_
--                     _/                |                \_
--          ________ _/          ________|              ____\_________
--         |_5_|_20_|           |_40_|_50_|            |_70_|_80_|_90_|
--       _/   /    /          _/     |   /            /     |    |     \
--      /    /    /          /       |   |           /      |    |      \
--     /    /    /          /      _/    |          /       |    |       \
--    /    /    |          /      /      |          |       |    |        \
--+---+ +-----+ +-----+ +-----++-----++-----+ +-----+ +-----++-----+ +-------+
--|1|3| |6|7|8| |22|26| |30|39||40|46||51|55| |61|64| |70|75||83|86| |90|95|99|
--+---+ +-----+ +-----+ +-----++-----++-----+ +-----+ +-----++-----+ +-------+

-- B-tree nodes have a variable number of keys and children, subject to some
-- constraints.
-- A B-tree is a tree with root root[T] with the following properties:
-- Every node has the following fields:
-- 
-- (*)  n[x], the number of keys currently in node x.
-- For example, n[|40|50|] in the above example B-tree is 2.
-- n[|70|80|90|] is 3.
-- The n[x] keys themselves, stored in nondecreasing order:
-- key1[x] <= key2[x] <= ... <= keyn[x][x]
-- For example, the keys in |70|80|90| are ordered.
--   leaf[x], a boolean value that is:
--   True if x is a leaf and False if x is an internal node. 
--     If x is an internal node, it contains:
--       n[x]+1 pointers c1, c2, ... , cn[x], cn[x]+1 to its children.
--       For example, in the above B-tree, the root node has two keys,
--       thus three children. Leaf nodes have no children so their ci fields
--       are undefined.
--     The keys keyi[x] separate the ranges of keys stored in each subtree:
--     if ki is any key stored in the subtree with root ci[x], then
-- 
--         k1 <= key1[x] <= k2 <= key2[x] <= ... <= keyn[x][x] <= kn[x]+1. 
-- 
--     For example, everything in the far left subtree of the root is numbered
--     less than 30. Everything in the middle subtree is between 30 and 60,
--     while everything in the far right subtree is greater than 60. The same
--     property can be seen at each level for all keys in non-leaf nodes.
--     Every leaf has the same depth, which is the tree's height h. In the
--     above example, h=2.
--     There are lower and upper bounds on the number of keys a node can
--     contain. These bounds can be expressed in terms of a fixed integer
--     t >= 2 called the minimum degree of the B-tree:
--         Every node other than the root must have at least t-1 keys. Every
--     internal node other than the root thus has at least t children. If the
--     tree is nonempty, the root must have at least one key.
--         Every node can contain at most 2t-1 keys. Therefore, an internal
--     node can have at most 2t children. We say that a node is full if it
--     contains exactly 2t-1 keys. 
-- 
-- Searching a B-tree Searching a B-tree is much like searching a binary
-- search tree, only the decision whether to go "left" or "right" is replaced
-- by the decision whether to go to child 1, child 2, ..., child n[x]. The
-- following procedure, B-Tree-Search, should be called with the root node as
-- its first parameter. It returns the block where the key k was found along
-- with the index of the key in the block, or "null" if the key was not found:
-- 
-- ++=============================================================++
-- || B-Tree-Search (x, k) // search starting at node x for key k ||
-- ++=============================================================++
--     i = 1
--     // search for the correct child
--     while i <= n[x] and k > keyi[x] do
--         i++
--     end while
-- 
--     // now i is the least index in the key array such that k <= keyi[x],
--     // so k will be found here or in the i'th child
-- 
--     if i <= n[x] and k = keyi[x] then 
--         // we found k at this node
--         return (x, i)
--     
--     if leaf[x] then return null
-- 
--     // we must read the block before we can work with it
--     Disk-Read (ci[x])
--     return B-Tree-Search (ci[x], k)
-- 
-- ++===========================++
-- || Creating an empty B+ Tree ||
-- ++===========================++
-- 
-- To initialize a B+ Tree, we build an empty root node, which means
-- we initialize the LListMap in topRec[LolBinName]
-- 
-- B+ Tree-Create (T)
--     x = allocate-node ();
--     leaf[x] = True
--     n[x] = 0
--     Disk-Write (x)
--     root[T] = x
-- 
-- This assumes there is an allocate-node function that returns a node with
-- key, c, leaf fields, etc., and that each node has a unique "address",
-- which, in our case, is an Aerospike record digest.
-- 
-- ++===============================++
-- || Inserting a key into a B-tree ||
-- ++===============================++
-- 
-- (*) Traverse the Tree, locating the Leaf Node that would contain the
-- new entry, remembering the path from root to leaf.
-- (*) If room in leaf, insert node.
-- (*) Else, split node, propagate dividing key up to parent.
-- (*) If parent full, split parent, propogate up. Iterate
-- (*) If root full, Create new level, move root contents to new level
--     NOTE: It might be better to divide root into 3 or 4 pages, rather
--     than 2.  This will take a little more thinking.

-- ======================================================================
-- TO DO List:
-- TODO:
-- (1) Initialize Maps for Root, Nodes, Leaves
-- (2) Create Search Function
-- (3) Simple Insert (Root plus Leaf Insert)
-- (4) Node Split Insert
-- (5) Simple Delete
-- (6) Complex Insert
-- ======================================================================
-- Aerospike Calls:
-- newRec = aerospike:crec_create( topRec )
-- newRec = aerospike:crec_open( topRec, childRecDigest)
-- status = aerospike:crec_update( topRec, childRec )
-- status = aerospike:crec_close( topRec, childRec )
-- digest = record.digest( childRec )
-- ======================================================================
-- For additional Documentation, please see LolDesign.lua
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
local F=true; -- Set F (flag) to true to turn ON global print

-- ===========================================
-- || GLOBAL VALUES -- Local to this module ||
-- ===========================================
-- Flag values
local FV_INSERT  = 'I'; -- flag to scanList to Insert the value (if not found)
local FV_SCAN    = 'S'; -- Regular Scan (do nothing else)
local FV_DELETE  = 'D'; -- flag to show scanList to Delete the value, if found

local FV_EMPTY = "__empty__"; -- the value is NO MORE

-- Switch from a single list to B+ Tree after this amount
local DEFAULT_THRESHHOLD = 100;

-- Use this to test for CtrlMap Integrity.  Every map should have one.
local MAGIC="MAGIC";     -- the magic value for Testing LLIST integrity

-- StoreMode (SM) values (which storage Mode are we using?)
local SM_BINARY  ='B'; -- Using a Transform function to compact values
local SM_LIST    ='L'; -- Using regular "list" mode for storing values.

-- StoreState (SS) values (which "state" is the set in?)
local SS_COMPACT ='C'; -- Using "single bin" (compact) mode
local SS_REGULAR ='R'; -- Using "Regular Storage" (regular) mode

-- KeyType (KT) values
local KT_ATOMIC  ='A'; -- the set value is just atomic (number or string)
local KT_COMPLEX ='C'; -- the set value is complex. Use Function to get key.

-- Key Compare Function for Complex Objects
-- By default, a complex object will have a "KEY" field, which the
-- key_compare() function will use to compare.  If the user passes in
-- something else, then we'll use THAT to perform the compare, which
-- MUST return -1, 0 or 1 for A < B, A == B, A > B.
-- UNLESS we are using a simple true/false equals compare.
-- ========================================================================
-- Actually -- the default will be EQUALS.  The >=< functions will be used
-- in the Ordered LIST implementation, not in the simple list implementation.
-- ========================================================================
local KC_DEFAULT="keyCompareEqual"; -- Key Compare used only in complex mode
local KH_DEFAULT="keyHash";         -- Key Hash used only in complex mode

-- Package Names
-- Standard, Test and Debug Packages
local PackageStandardList    = "StandardList";
local PackageTestModeList    = "TestModeList";
local PackageTestModeBinary  = "TestModeBinary";
local PackageTestModeNumber  = "TestModeNumber";
local PackageDebugModeList   = "DebugModeList";
local PackageDebugModeBinary = "DebugModeBinary";
local PackageDebugModeNumber = "DebugModeNumber";
local PackageProdListValBinStore = "ProdListValBinStore";

-- set up our "outside" links
local  CRC32 = require('CRC32');
local functionTable = require('UdfFunctionTable');

-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- <><><><> <Initialize Control Maps> <Initialize Control Maps> <><><><>
-- There are three main Record Types used in the LLIST Package, and their
-- initialization functions follow.  The initialization functions
-- define the "type" of the control structure:
--
-- (*) TopRec: the top level user record that contains the LLIST bin,
--     including the Root Directory.
-- (*) InnerNodeRec: Interior B+ Tree nodes
-- (*) DataNodeRec: The Data Leaves
--
-- <+> Naming Conventions:
--   + All Field names (e.g. ldtMap.PageMode) begin with Upper Case
--   + All variable names (e.g. ldtMap.PageMode) begin with lower Case
--   + All Record Field access is done using brackets, with either a
--     variable or a constant (in single quotes).
--     (e.g. topRec[binName] or ldrRec['LdrControlBin']);
--
-- <><><><> <Initialize Control Maps> <Initialize Control Maps> <><><><>

-- ======================================================================
-- initializeLListMap:
-- ======================================================================
-- Set up the LLIST Map with the standard (default) values.
-- These values may later be overridden by the user.
-- The structure held in the Record's "LLIST BIN" is this map.  This single
-- structure contains ALL of the settings/parameters that drive the LLIST
-- behavior.  Thus this function represents the "type" LLIST MAP -- all
-- LLIST control fields are defined here.
-- The LListMap is obtained using the user's LLIST Bin Name:
-- ldtMap = topRec[ldtBinName]
-- ======================================================================
local function initializeLolRoot( topRec, ldtBinName, transFunc, untransFunc,
                                  funcArgs )
  local meth = "initializeLListMap()";
  GP=F and trace("[ENTER]: <%s:%s>:: LolBinName(%s)",
    MOD, meth, tostring(ldtBinName));

  -- The LLIST Map -- with Default Values
  -- General Tree Settings
  ldtMap.LdtType="LLIST";   -- Mark this as a Large Ordered List
  ldtMap.ItemCount = 0;     -- A count of all items in the LLIST
  ldtMap.DesignVersion = 1; -- Current version of the code
  ldtMap.Magic = "MAGIC";   -- Used to verify we have a valid map
  ldtMap.ExistSubRecDig = 0; -- Pt to the LDT "Exists" subrecord (digest)
  ldtMap.BinName = ldtBinName; -- Name of the Bin for this LLIST in TopRec
  ldtMap.NameSpace = "test"; -- Default NS Name -- to be overridden by user
  ldtMap.Set = "set";       -- Default Set Name -- to be overridden by user
  ldtMap.PageMode = "List"; -- "List" or "Binary" (applies to all nodes)
  ldtMap.TreeLevel = 1;     -- Start off Lvl 1: Root plus leaves
  ldtMap.DataLeafCount = 0;
  ldtMap.InnerNodeCount = 0;
  ldtMap.KeyType = 0; -- 0 is atomic, 1 is map (with a KEY field).
  ldtMap.TransFunc = 0; -- Name of the transform (from user to storage)
  ldtMap.UnTransFunc = 0; -- Reverse transform (from storage to user)
  --
  -- Top Node Tree Root Directory
  ldtMap.RootDirMax = 100;
  ldtMap.KeyByteArray = 0; -- Byte Array, when in compressed mode
  ldtMap.DigestByteArray = 0; -- DigestArray, when in compressed mode
  ldtMap.KeyList = 0; -- Key List, when in List Mode
  ldtMap.DigestList = 0; -- Digest List, when in List Mode
  ldtMap.CompactList = list();
  
  -- LLIST Inner Node Settings
  ldtMap.InnerNodeEntryCountMax = 50;  -- Max # of items (key+digest)
  ldtMap.InnerNodeByteEntrySize = 11;  -- Size (in bytes) of Key obj
  ldtMap.InnerNodeByteCountMax = 2000; -- Max # of BYTES

  -- LLIST Tree Leaves (Data Pages)
  ldtMap.DataPageEntryCountMax = 100;  -- Max # of items
  ldtMap.DataPageByteEntrySize = 44;  -- Size (in bytes) of data obj
  ldtMap.DataPageByteCountMax = 2000; -- Max # of BYTES per data page

  GP=F and trace("[DEBUG]: <%s:%s> : CTRL Map after Init(%s)",
      MOD, meth , tostring(ldtMap));

  -- Put our new map in the record, then store the record.
  topRec[ldtBinName] = ldtMap;

  GP=F and trace("[EXIT]:<%s:%s>:", MOD, meth );
  return ldtMap
end -- initializeLListMap

-- ++======================++
-- || Prepackaged Settings ||
-- ++======================++
--
-- ======================================================================
-- This is the standard (default) configuration
-- Package = "StandardList"
-- ======================================================================
local function packageStandardList( ldtMap )
  
  -- General Parameters
  ldtMap.Transform = nil;
  ldtMap.UnTransform = nil;
  ldtMap.StoreState = SS_COMPACT; -- start in "compact mode"
  ldtMap.StoreMode = SM_LIST; -- Use List Mode
  ldtMap.BinaryStoreSize = nil; -- Don't waste room if we're not using it
  ldtMap.KeyType = KT_ATOMIC; -- Atomic Keys
  ldtMap.BinName = ldtBinName;
  ldtMap.Modulo = DEFAULT_DISTRIB;
  ldtMap.ThreshHold = DEFAULT_THRESHHOLD; -- Rehash after this many inserts

end -- packageStandardList()

-- ======================================================================
-- Package = "TestModeNumber"
-- ======================================================================
local function packageTestModeNumber( ldtMap )
  
  -- General Parameters
  ldtMap.Transform = nil;
  ldtMap.UnTransform = nil;
  ldtMap.StoreState = SS_COMPACT; -- start in "compact mode"
  ldtMap.StoreMode = SM_LIST; -- Use List Mode
  ldtMap.BinaryStoreSize = nil; -- Don't waste room if we're not using it
  ldtMap.KeyType = KT_ATOMIC; -- Atomic Keys
  ldtMap.BinName = ldtBinName;
  ldtMap.Modulo = DEFAULT_DISTRIB;
  ldtMap.ThreshHold = DEFAULT_THRESHHOLD; -- Rehash after this many have been inserted
 
end -- packageTestModeList()


-- ======================================================================
-- Package = "TestModeList"
-- ======================================================================
local function packageTestModeList( ldtMap )
  
  -- General Parameters
  ldtMap.Transform = nil;
  ldtMap.UnTransform = nil;
  ldtMap.StoreState = SS_COMPACT; -- start in "compact mode"
  ldtMap.StoreMode = SM_LIST; -- Use List Mode
  ldtMap.BinaryStoreSize = nil; -- Don't waste room if we're not using it
  ldtMap.KeyType = KT_COMPLEX; -- Complex Object (need key function)
  ldtMap.BinName = ldtBinName;
  ldtMap.Modulo = DEFAULT_DISTRIB;
  ldtMap.ThreshHold = DEFAULT_THRESHHOLD; -- Rehash after this many have been inserted
 
end -- packageTestModeList()

-- ======================================================================
-- Package = "TestModeBinary"
-- ======================================================================
local function packageTestModeBinary( ldtMap )
  
  -- General Parameters
  ldtMap.Transform = "compressTest4";
  ldtMap.UnTransform = "unCompressTest4";
  ldtMap.StoreState = SS_COMPACT; -- start in "compact mode"
  ldtMap.StoreMode = SM_LIST; -- Use List Mode
  ldtMap.BinaryStoreSize = nil; -- Don't waste room if we're not using it
  ldtMap.KeyType = KT_COMPLEX; -- Complex Object (need key function)
  ldtMap.BinName = ldtBinName;
  ldtMap.Modulo = DEFAULT_DISTRIB;
  ldtMap.ThreshHold = DEFAULT_THRESHHOLD; -- Rehash after this many have been inserted

end -- packageTestModeBinary( ldtMap )

-- ======================================================================
-- Package = "ProdListValBinStore"
-- This Production App uses a compacted (transformed) representation.
-- ======================================================================
local function packageProdListValBinStore( ldtMap )
  
  -- General Parameters
  ldtMap.Transform = "listCompress_5_18";
  ldtMap.UnTransform = "listUnCompress_5_18";
  ldtMap.StoreState = SS_COMPACT; -- start in "compact mode"
  ldtMap.StoreMode = SM_BINARY; -- Use a Byte Array
  ldtMap.BinaryStoreSize = 4; -- Storing a single 4 byte integer
  ldtMap.KeyType = KT_ATOMIC; -- Atomic Keys (a number)
  ldtMap.BinName = ldtBinName;
  ldtMap.Modulo = DEFAULT_DISTRIB;
  ldtMap.ThreshHold = 100; -- Rehash after this many have been inserted
  
end -- packageProdListValBinStore()

-- ======================================================================
-- Package = "DebugModeList"
-- Test the LLIST with very small numbers to force it to make LOTS of
-- warm and close objects with very few inserted items.
-- ======================================================================
local function packageDebugModeList( ldtMap )
  
  -- General Parameters
  ldtMap.Transform = nil;
  ldtMap.UnTransform = nil;
  ldtMap.StoreState = SS_COMPACT; -- start in "compact mode"
  ldtMap.StoreMode = SM_LIST; -- Use List Mode
  ldtMap.BinaryStoreSize = nil; -- Don't waste room if we're not using it
  ldtMap.KeyType = KT_ATOMIC; -- Atomic Keys
  ldtMap.BinName = ldtBinName;
  ldtMap.Modulo = DEFAULT_DISTRIB;
  ldtMap.ThreshHold = 4; -- Rehash after this many have been inserted

end -- packageDebugModeList()

-- ======================================================================
-- Package = "DebugModeBinary"
-- Perform the Debugging style test with compression.
-- ======================================================================
local function packageDebugModeBinary( ldtMap )
  
  -- General Parameters
  ldtMap.Transform = "compressTest4";
  ldtMap.UnTransform = "unCompressTest4";
  ldtMap.KeyCompare = "debugListCompareEqual"; -- "Simple" list comp
  ldtMap.StoreState = SS_COMPACT; -- start in "compact mode"
  ldtMap.StoreMode = SM_LIST; -- Use List Mode
  ldtMap.BinaryStoreSize = 16; -- Don't waste room if we're not using it
  ldtMap.KeyType = KT_COMPLEX; -- special function for list compare.
  ldtMap.BinName = ldtBinName;
  ldtMap.Modulo = DEFAULT_DISTRIB;
  ldtMap.ThreshHold = 4; -- Rehash after this many have been inserted

end -- packageDebugModeBinary( ldtMap )

-- ======================================================================
-- Package = "DebugModeNumber"
-- Perform the Debugging style test with a number
-- ======================================================================
local function packageDebugModeNumber( ldtMap )
  local meth = "packageDebugModeNumber()";
  GP=F and trace("[ENTER]: <%s:%s>:: CtrlMap(%s)",
    MOD, meth, tostring(ldtMap) );
  
  -- General Parameters
  ldtMap.Transform = nil;
  ldtMap.UnTransform = nil;
  ldtMap.KeyCompare = nil;
  ldtMap.StoreState = SS_COMPACT; -- start in "compact mode"
  ldtMap.StoreMode = SM_LIST; -- Use List Mode
  ldtMap.BinaryStoreSize = 0; -- Don't waste room if we're not using it
  ldtMap.KeyType = KT_ATOMIC; -- Simple Number (atomic) compare
  ldtMap.BinName = ldtBinName;
  ldtMap.Modulo = DEFAULT_DISTRIB;
  ldtMap.ThreshHold = 4; -- Rehash after this many have been inserted

  GP=F and trace("[EXIT]: <%s:%s>:: CtrlMap(%s)",
    MOD, meth, tostring(ldtMap) );
end -- packageDebugModeNumber( ldtMap )

-- ======================================================================
-- adjustLListMap:
-- ======================================================================
-- Using the settings supplied by the caller in the stackCreate call,
-- we adjust the values in the LListMap.
-- Parms:
-- (*) ldtMap: the main LList Bin value
-- (*) argListMap: Map of LList Settings 
-- ======================================================================
local function adjustLListMap( ldtMap, argListMap )
  local meth = "adjustLListMap()";
  GP=F and trace("[ENTER]: <%s:%s>:: LListMap(%s)::\n ArgListMap(%s)",
    MOD, meth, tostring(ldtMap), tostring( argListMap ));

  -- Iterate thru the argListMap and adjust (override) the map settings 
  -- based on the settings passed in during the create() call.
  GP=F and trace("[DEBUG]: <%s:%s> : Processing Arguments:(%s)",
    MOD, meth, tostring(argListMap));

  for name, value in map.pairs( argListMap ) do
    GP=F and trace("[DEBUG]: <%s:%s> : Processing Arg: Name(%s) Val(%s)",
        MOD, meth, tostring( name ), tostring( value ));

    -- Process our "prepackaged" settings first:
    -- NOTE: Eventually, these "packages" will be installed in either
    -- a separate "package" lua file, or possibly in the UdfFunctionTable.
    -- Regardless though -- they will move out of this main file, except
    -- maybe for the "standard" packages.
    if name == "Package" and type( value ) == "string" then
      -- Figure out WHICH package we're going to deploy:
      if value == PackageStandardList then
          packageStandardList( ldtMap );
      elseif value == PackageTestModeList then
          packageTestModeList( ldtMap );
      elseif value == PackageTestModeBinary then
          packageTestModeBinary( ldtMap );
      elseif value == PackageTestModeNumber then
          packageTestModeNumber( ldtMap );
      elseif value == PackageProdListValBinStore then
          packageProdListValBinStore( ldtMap );
      elseif value == PackageDebugModeList then
          packageDebugModeList( ldtMap );
      elseif value == PackageDebugModeBinary then
          packageDebugModeBinary( ldtMap );
      elseif value == PackageDebugModeNumber then
          packageDebugModeNumber( ldtMap );
      end
    elseif name == "KeyType" and type( value ) == "string" then
      -- Use only valid values (default to ATOMIC if not specifically complex)
      if value == KT_COMPLEX or value == "complex" then
        ldtMap.KeyType = KT_COMPLEX;
      else
        ldtMap.KeyType = KT_ATOMIC;
      end
    elseif name == "StoreMode"  and type( value ) == "string" then
      -- Verify it's a valid value
      if value == SM_BINARY or value == SM_LIST then
        ldtMap.StoreMode = value;
      end
    elseif name == "Modulo"  and type( value ) == "number" then
      -- Verify it's a valid value
      if value > 0 and value < MODULO_MAX then
        ldtMap.Modulo = value;
      end
    end
  end -- for each argument

  GP=F and trace("[EXIT]: <%s:%s> : CTRL Map after Adjust(%s)",
    MOD, meth , tostring(ldtMap));
      
  return ldtMap
end -- adjustLListMap


-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- || B+ Tree Data Page Record |||||||||||||||||||||||||||||||||||||||||||
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- Records used for B+ Tree nodes have three bins:
-- Chunks hold the actual entries. Each LDT Data Record (LDR) holds a small
-- amount of control information and a list.  A LDR will have three bins:
-- (1) The Control Bin (a Map with the various control data)
-- (2) The Data List Bin ('DataListBin') -- where we hold "list entries"
-- (3) The Binary Bin -- where we hold compacted binary entries (just the
--     as bytes values)
-- (*) Although logically the Directory is a list of pairs (Key, Digest),
--     in fact it is two lists: Key List, Digest List, where the paired
--     Key/Digest have the same index entry in the two lists.
-- (*) Note that ONLY ONE of the two content bins will be used.  We will be
--     in either LIST MODE (bin 2) or BINARY MODE (bin 3)
-- ==> 'LolControlBin' Contents (a Map)
--    + 'TopRecDigest': to track the parent (root node) record.
--    + 'Digest' (the digest that we would use to find this chunk)
--    + 'ItemCount': Number of valid items on the page:
--    + 'TotalCount': Total number of items (valid + deleted) used.
--    + 'Bytes Used': Number of bytes used, but ONLY when in "byte mode"
--    + 'Design Version': Decided by the code:  DV starts at 1.0
--    + 'Log Info':(Log Sequence Number, for when we log updates)
--
--  ==> 'LolListBin' Contents (A List holding entries)
--  ==> 'LolBinaryBin' Contents (A single BYTE value, holding packed entries)
--    + Note that the Size and Count fields are needed for BINARY and are
--      kept in the control bin (EntrySize, ItemCount)
--
--    -- Entry List (Holds entry and, implicitly, Entry Count)
  
-- ======================================================================
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- || Initialize Interior B+ Tree Nodes  (Records) |||||||||||||||||||||||
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- ======================================================================
-- initializeNodeMap( Interior Tree Nodes )
-- ======================================================================
-- Set the values in an Inner Tree Node Control Map and Key/Digest Lists.
-- There are potentially FIVE bins in an Interior Tree Node Record:
-- (1) nodeRec['NodeCtrlBin']: The control Map (defined here)
-- (2) nodeRec['KeyListBin']: The Data Entry List (when in list mode)
-- (3) nodeRec['KeyBnryBin']: The Packed Data Bytes (when in Binary mode)
-- (4) nodeRec['DgstListBin']: The Data Entry List (when in list mode)
-- (5) nodeRec['DgstBnryBin']: The Packed Data Bytes (when in Binary mode)
-- Pages are either in "List" mode or "Binary" mode (the whole tree is in
-- one mode or the other), so the record will employ only three fields.
-- Either Bins 1,2,4 or Bins 1,3,5.
--
-- NOTE: For the Digests, we could potentially NOT store the Lock bits
-- and the Partition Bits -- since we force all of those to be the same,
-- we know they are all identical to the top record.  So, that would save
-- us 4 bytes PER DIGEST -- which adds up for 50 to 100 entries.
-- We would use a transformation method to transform a 20 byte value into
-- and out of a 16 byte value.
--
-- ======================================================================
local function initializeNodeMap(topRec, parentRec, nodeRec, nodeMap, ldtMap)
  local meth = "initializeNodeMap()";
  GP=F and trace("[ENTER]: <%s:%s>", MOD, meth );

  nodeMap.RootDigest = record.digest( topRec );
  nodeMap.ParentDigest = record.digest( parentRec );
  nodeMap.PageMode = ldtMap.PageMode;
  nodeMap.Digest = record.digest( nodeRec );
  -- Note: Item Count is implicitly the KeyList size
  nodeMap.KeyListMax = 100; -- Digest List is ONE MORE than Key List
  nodeMap.ByteEntrySize = ldtMap.LdrByteEntrySize; -- ByteSize of Fixed Entries
  nodeMap.ByteEntryCount = 0;  -- A count of Byte Entries
  nodeMap.ByteCountMax = ldtMap.LdrByteCountMax; -- Max # of bytes in ByteArray
  nodeMap.Version = ldtMap.Version;
  nodeMap.LogInfo = 0;
end -- initializeNodeMap()

-- ======================================================================
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- || B+ Tree Data Page Record |||||||||||||||||||||||||||||||||||||||||||
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- Records used for B+ Tree modes have three bins:
-- Chunks hold the actual entries. Each LDT Data Record (LDR) holds a small
-- amount of control information and a list.  A LDR will have three bins:
-- (1) The Control Bin (a Map with the various control data)
-- (2) The Data List Bin ('DataListBin') -- where we hold "list entries"
-- (3) The Binary Bin -- where we hold compacted binary entries (just the
--     as bytes values)
-- (*) Although logically the Directory is a list of pairs (Key, Digest),
--     in fact it is two lists: Key List, Digest List, where the paired
--     Key/Digest have the same index entry in the two lists.
-- (*) Note that ONLY ONE of the two content bins will be used.  We will be
--     in either LIST MODE (bin 2) or BINARY MODE (bin 3)
-- ==> 'LolControlBin' Contents (a Map)
--    + 'TopRecDigest': to track the parent (root node) record.
--    + 'Digest' (the digest that we would use to find this chunk)
--    + 'ItemCount': Number of valid items on the page:
--    + 'TotalCount': Total number of items (valid + deleted) used.
--    + 'Bytes Used': Number of bytes used, but ONLY when in "byte mode"
--    + 'Design Version': Decided by the code:  DV starts at 1.0
--    + 'Log Info':(Log Sequence Number, for when we log updates)
--
--  ==> 'LolListBin' Contents (A List holding entries)
--  ==> 'LolBinaryBin' Contents (A single BYTE value, holding packed entries)
--    + Note that the Size and Count fields are needed for BINARY and are
--      kept in the control bin (EntrySize, ItemCount)
--
--    -- Entry List (Holds entry and, implicitly, Entry Count)
-- ======================================================================
-- Set the values in an Inner Tree Node Control Map and Key/Digest Lists.
-- There are potentially FIVE bins in an Interior Tree Node Record:
-- (1) nodeRec['NodeCtrlBin']: The control Map (defined here)
-- (2) nodeRec['KeyListBin']: The Data Entry List (when in list mode)
-- (3) nodeRec['KeyBnryBin']: The Packed Data Bytes (when in Binary mode)
-- (4) nodeRec['DgstListBin']: The Data Entry List (when in list mode)
-- (5) nodeRec['DgstBnryBin']: The Packed Data Bytes (when in Binary mode)
-- Pages are either in "List" mode or "Binary" mode (the whole tree is in
-- one mode or the other), so the record will employ only three fields.
-- Either Bins 1,2,4 or Bins 1,3,5.
-- initializeLeafMap(): Data Leaf Nodes
-- ======================================================================
local function initializeLeafMap(topRec, parentRec, leafRec, leafMap, ldtMap)
  local meth = "initializeLeafMap()";
  GP=F and trace("[ENTER]: <%s:%s>", MOD, meth );

  leafMap.RootDigest = record.digest( topRec );
  leafMap.ParentDigest = record.digest( parentRec );
  leafMap.PageMode = ldtMap.PageMode;
  leafMap.Digest = record.digest( leafRec );
  -- Note: Item Count is implicitly the KeyList size
  leafMap.DataListMax = 100; -- Max Number of items in List of data items
  leafMap.ByteEntrySize = ldtMap.LdrByteEntrySize; -- ByteSize of Fixed Entries
  leafMap.ByteEntryCount = 0;  -- A count of Byte Entries
  leafMap.ByteCountMax = ldtMap.LdrByteCountMax; -- Max # of bytes in ByteArray
  leafMap.Version = ldtMap.Version;
  leafMap.LogInfo = 0;
end -- initializeLeafMap()

-- ======================================================================
-- ======================================================================
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- Large Ordered List (LLIST) Utility Functions
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- These are all local functions to this module and serve various
-- utility and assistance functions.

-- ======================================================================
-- adjustLListMap:
-- ======================================================================
-- Using the settings supplied by the caller in the listCreate call,
-- we adjust the values in the LListMap.
-- Parms:
-- (*) ldtMap: the main List Bin value
-- (*) argListMap: Map of List Settings 
-- ======================================================================
local function adjustLListMap( ldtMap, argListMap )
  local meth = "adjustLListMap()";
  GP=F and trace("[ENTER]: <%s:%s>:: LListMap(%s)::\n ArgListMap(%s)",
    MOD, meth, tostring(ldtMap), tostring( argListMap ));

  -- Iterate thru the argListMap and adjust (override) the map settings 
  -- based on the settings passed in during the listCreate() call.
  GP=F and trace("[DEBUG]: <%s:%s> : Processing Arguments:(%s)",
    MOD, meth, tostring(argListMap));

-- Fill in when we have a better idea of the settings.

  GP=F and trace("[DEBUG]: <%s:%s> : CTRL Map after Adjust(%s)",
    MOD, meth , tostring(ldtMap));

  GP=F and trace("[EXIT]:<%s:%s>:Dir Map after Init(%s)",
      MOD,meth,tostring(ldtMap));

  return ldtMap
end -- adjustLListMap

-- ======================================================================
-- validateTopRec( topRec, ldtMap )
-- ======================================================================
-- Validate that the top record looks valid:
-- Get the LDT bin from the rec and check for magic
-- Return: "good" or "bad"
-- ======================================================================
local function  validateTopRec( topRec, ldtMap )
  local thisMap = topRec[ldtMap.BinName];
  if thisMap.Magic == "MAGIC" then
    return "good"
  else
    return "bad"
  end
end -- validateTopRec()


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
-- local function Tree Summary( ldtMap ) (DEBUG/Trace Function)
-- ======================================================================
-- For easier debugging and tracing, we will summarize the Tree Map
-- contents -- without printing out the entire thing -- and return it
-- as a string that can be printed.
-- ======================================================================
local function ldtSummary( ldtMap )
  local resultMap             = map();
  resultMap.SUMMARY           = "List Summary String";

  return tostring( resultMap );
end -- ldtSummary()

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
-- rootNodeSummary( topRec, ldtMap )
-- ======================================================================
-- Print out interesting stats about this B+ Tree Root
-- ======================================================================
local function  rootNodeSummary( topRec, ldtMap )
  local resultMap = ldtMap;

  -- Add to this -- move selected fields into resultMap and return it.

  return tostring( resultMap  );
end -- rootNodeSummary


-- ======================================================================
-- interiorNodeSummary( intNode )
-- ======================================================================
-- Print out interesting stats about this Interior B+ Tree Node
-- ======================================================================
local function  interiorNodeSummary( intNode )
  local resultMap = intNode['NodeCtrlBin'];

  -- Add to this -- move selected fields into resultMap and return it.

  return tostring( resultMap  );
end -- interiorNodeSummary()


-- ======================================================================
-- leafNodeSummary( leafNode )
-- ======================================================================
-- Print out interesting stats about this B+ Tree Leaf (Data) node
-- ======================================================================
local function  leafNodeSummary( leafNode )
  local resultMap = map();
  local nodeMap = nodeRecord['NodeCtrlBin'];

  return tostring( resultMap );
end -- leafNodeSummary()

-- ======================================================================
-- nodeHasRoom: Check that there's enough space for an insert in the 
-- node list.
-- 
-- Return: 1=There is room.   0=Not enough room.
-- ======================================================================
-- Parms:
-- (*) keyList
-- (*) listMax
-- Return: 1 (ONE) if there's room, otherwise 0 (ZERO)
-- ======================================================================
local function nodeHasRoom( keyList, listMax )
  local meth = "nodeHasRoom()";
  GP=F and trace("[ENTER]: <%s:%s> keyList(%s) ListMax(%s)",
    MOD, meth, tostring(keyList), tostring(listMax) );

  local result = 1;  -- Be optimistic 

  -- TODO: Finish Method
  print("[!!! FINISH THIS METHOD !!! (%s) ", meth );


  GP=F and trace("[EXIT]: <%s:%s> result(%d) ", MOD, meth, result );
  return result;
end -- nodeHasRoom()


-- ======================================================================
-- keyListInsert( keyList, newKey, digestList, newDigest ))
-- ======================================================================
-- Insert a new keyValue into a keyList, and the associated digest into
-- the digestList.
-- The caller has already verified that there's room in this list for
-- one more entry (pair).
--
local function keyListInsert( ldtMap, keyList, newKey, digestList, newDigest )
  local meth = "keyListInsert()";
  GP=F and trace("[ENTER]: <%s:%s> : Insert Value(%s), keyList(%s)",
    MOD, meth, tostring(newKey), tostring( keyList ));

  local rc = 0;

  local position = searchKeyList( keyList, newKey );

  -- Assuming there's room, Move items to the right to make room for

  -- TODO: Finish this method
  print("[!!! FINISH THIS METHOD !!! (%s) ", meth );

  
  return rc;
end -- keyListInsert()


-- ======================================================================
-- compare:
-- ======================================================================
-- Compare Search Value with data, following the protocol for data
-- compare types.
-- Return -1 for SV < data, 0 for SV == data, 1 for SV > data
-- Return -2 if either of the values is null
-- ======================================================================
local function compare( keyType, sv, data )
  local result = 0;
  -- For atomic types (keyType == 0), compare objects directly
  if sv == nil or data == nil then return -2 end;
  if keyType == 0 then
    if sv == data then
      return 0;
    elseif sv < data then
      return -1;
    else
      return 1;
    end
  else
    -- For complex types, we have to be more careful about using the
    -- 'KEY' field -- we must check that it exists first.
    if sv.KEY == nil or data.KEY == nil then return -2 end;
    if sv.KEY == data.KEY then
      return 0;
    elseif sv.KEY < data.KEY then
      return -1;
    else
      return 1;
    end
  end
end -- compare()

-- ======================================================================
-- NOTE: Can we make Root, Inner and Leaf look the same?
-- Each node must have similar fields (including NodeType)
-- structure: LevelCount
-- TopPtr = InnerNodeList[1]
-- LeafPtr = InnerNodeList[ list.size(InnerNodeList) ]
-- InnerNodeList, PositionList
--
-- node = root
-- While node not leaf
--   position = search keyList( node )
--   if savePath then addToPath( node, position ) end
--   node = getNextNode( node, position )
-- end
-- searchLeaf( node )
-- ======================================================================
-- B-Tree-Search (x, k) // search starting at node x for key k
--     i = 1
--     // search for the correct child
--     while i <= n[x] and k > keyi[x] do
--         i++
--     end while
-- 
--     // now i is the least index in the key array such that k <= keyi[x],
--     // so k will be found here or in the i'th child
-- 
--     if i <= n[x] and k = keyi[x] then 
--         // we found k at this node
--         return (x, i)
--     
--     if leaf[x] then return null
-- 
--     // we must read the block before we can work with it
--     Disk-Read (ci[x])
--     return B-Tree-Search (ci[x], k)

-- ======================================================================
-- searchKeyList(): 
-- ======================================================================
-- Given a Key List, return the index of the digest to take next.
local function searchKeyList( keyList, searchValue )
end -- searcKeyhList()

-- ======================================================================
-- searchLeaf(): Locate the matching value(s) in the leaf node(s).
-- ======================================================================
-- Leaf Node:
-- (*) TopRec digest
-- (*) Parent rec digest
-- (*) This Rec digest
-- (*) NEXT Leaf
-- (*) PREV Leaf
-- (*) Min value is implicitly index 1,
-- (*) Max value is implicitly at index (size of list)
-- (*) Beginning of last value
-- Parms:
-- (*) topRec: 
-- (*) leafNode:
-- (*) searchPath:
-- (*) ldtMap:
-- (*) resultList:
-- (*) searchValue:
-- (*) func:
-- (*) fargs:
-- (*) flag:
-- ======================================================================
local function searchLeaf(topRec, leafNode, searchPath, ldtMap, resultList,
                          searchValue, func, fargs, flag)
  -- Linear scan of the Leaf Node (binary search will come later), for each
  -- match, add to the resultList.
  local compareResult = 0;
  if ldtMap.PageMode == 0 then
    -- Do the BINARY page mode search here
    GP=F and trace("[WARNING]: <%s:%s> :BINARY MODE NOT IMPLEMENTED",
        MOD, meth, tostring(newStorageValue), tostring( resultList));
    return nil; -- TODO: Build this mode.
  else
    -- Do the List page mode search here
    -- Later: Split the loop search into two -- atomic and map objects
    local leafDataList = leafNode['DataListBin'];
    local keyType = ldtMap.KeyType;
    for i = 1, list.size( leafDataList ), 1 do
      compareResult = compare( keyType, searchValue, leafDataList[i] );
      if compareResult == -2 then
        return nil -- error result.
      end
      if compareResult == 0 then
        -- Start gathering up values
        gatherLeafListData( topRec, leafNode, ldtMap, resultList, searchValue,
          func, fargs, flag );
        GP=F and trace("[FOUND VALUES]: <%s:%s> : Value(%s) Result(%s)",
          MOD, meth, tostring(newStorageValue), tostring( resultList));
          return resultList;
      elseif compareResult  == 1 then
        GP=F and trace("[NotFound]: <%s:%s> : Value(%s)",
          MOD, meth, tostring(newStorageValue) );
          return resultList;
      end
      -- otherwise, keep looking.  We haven't passed the spot yet.
    end -- for each list item
  end -- end else list mode

end -- searchLeaf()


-- ======================================================================
-- locateLeaf(): Find the leaf node that would contain the search value
-- ======================================================================
-- Traverse the path from the root to the leaf node.  Return the path
-- information from the search.
-- Root node:
-- + Unique (list or set)
-- + Total Count:
-- + Tree Level (1, 2, ... N)
-- + Min value
-- + Max value
--
-- Inner node:
-- + TopRec digest
-- + Parent rec digest
-- + This Rec digest
-- + 
-- ======================================================================
local function locateLeaf( topRec, ldtMap, searchValue, func, fargs )
    -- Start at the root, find the appropriate record pointer and 
    -- traverse the tree down to the data page.  Then hand off searching
    -- to the searchLeaf() function.
  local meth = "hotListInsert()";
  GP=F and trace("[ENTER]: <%s:%s> : Insert Value(%s)",
    MOD, meth, tostring(newStorageValue) );

  local keyList = ldtMap.KeyList;
  local digestList = ldtMap.DigestList;



end -- locateLeaf()

-- ======================================================================
-- ldtMapSearch(): Read all elements matching SearchValue
-- ======================================================================
-- Parms:
-- (*) ldtMap: The main LDT structure (stored in the LDT Bin)
-- (*) peekCount: The total count to read (0 means all)
-- (*) Optional inner UDF Function (from the UdfFunctionTable)
-- (*) fargs: Function arguments (list) fed to the inner UDF
-- Return: The Peek resultList -- in LIFO order
-- ======================================================================
local function ldtMapSearch( topRec, ldtMap, searchValue, func, fargs )
  local meth = "ldtMapSearch()";
  GP=F and trace("[ENTER]: <%s:%s> searchValue(%s)",
      MOD, meth, tostring(searchValue));

  if (func ~= nil and fargs ~= nil ) then
    GP=F and trace("[ENTER1]: <%s:%s> SearchValue(%s) func(%s) fargs(%s)",
      MOD, meth, tostring(searchValue), tostring(func), tostring(fargs) );
  else
    GP=F and trace("[ENTER2]: <%s:%s> SearchValue(%s)",
    MOD, meth, tostring(searchValue));
  end

  -- Set up our resultList list. Add to the resultList from every node that
  -- contains keys that match the searchValue
  local resultList = list();

  local leafNode = locateLeaf( topRec, ldtMap, searchValue, func, fargs );
  -- a tree search returns a STRUCTURE that shows the search path and the
  -- location of the item (or the insert position).
  local flag = 'S'; -- S=Search, I=Insert, D=Delete
  -- leafSearch will scan the linked list of Tree Leaf Pages.
  local searchPath  =
    searchLeaf(topRec,leafNode,ldtMap,resultList,searchValue,func,fargs,flag);

  GP=F and trace("[EXIT]: <%s:%s> searchValue(%s) resultList(%s)",
      MOD, meth, tostring(searchValue), tostring(resultList));
  
  return resultList; 
end -- function ldtMapSearch()

-- =======================================================================
-- Apply Transform Function
-- Take the Transform defined in the ldtMap, if present, and apply
-- it to the value, returning the transformed value.  If no transform
-- is present, then return the original value (as is).
-- NOTE: This can be made more efficient.
-- =======================================================================
local function applyTransform( transformFunc, newValue )
  local meth = "applyTransform()";
  GP=F and trace("[ENTER]: <%s:%s> transform(%s) type(%s) Value(%s)",
 MOD, meth, tostring(transformFunc), type(transformFunc), tostring(newValue));

  local storeValue = newValue;
  if transformFunc ~= nil then 
    storeValue = transformFunc( newValue );
  end
  return storeValue;
end -- applyTransform()

-- =======================================================================
-- Apply UnTransform Function
-- Take the UnTransform defined in the ldtMap, if present, and apply
-- it to the dbValue, returning the unTransformed value.  If no unTransform
-- is present, then return the original value (as is).
-- NOTE: This can be made more efficient.
-- =======================================================================
local function applyUnTransform( ldtMap, storeValue )
  local returnValue = storeValue;
  if ldtMap.UnTransform ~= nil and
    functionTable[ldtMap.UnTransform] ~= nil then
    returnValue = functionTable[ldtMap.UnTransform]( storeValue );
  end
  return returnValue;
end -- applyUnTransform( value )

-- =======================================================================
-- unTransformSimpleCompare()
-- Apply the unTransform function and compare the values.
-- Return the unTransformed search value if the values match.
-- =======================================================================
local function unTransformSimpleCompare(unTransform, dbValue, searchValue)
  local modSearchValue = searchValue;
  local resultValue = nil;

  if unTransform ~= nil then
    modSearchValue = unTransform( searchValue );
  end

  if dbValue == modSearchValue then
    resultValue = modSearchValue;
  end

  return resultValue;
end -- unTransformSimpleCompare()


-- =======================================================================
-- unTransformComplexCompare()
-- Apply the unTransform function and compare the values, using the
-- compare function (it's a complex compare).
-- Return the unTransformed search value if the values match.
-- parms:
-- (*) trans: The transformation function: Perform if not null
-- (*) comp: The Compare Function (must not be null)
-- (*) dbValue: The value pulled from the DB
-- (*) searchValue: The value we're looking for.
-- =======================================================================
local function unTransformComplexCompare(trans, comp, dbValue, searchValue)
  local modSearchValue = searchValue;
  local resultValue = nil;

  if unTransform ~= nil then
    modSearchValue = unTransform( searchValue );
  end

  if dbValue == modSearchValue then
    resultValue = modSearchValue;
  end

  return resultValue;
end -- unTransformComplexCompare()

-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||

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
--     ==> if ==  FV_INSERT: insert the element IF NOT FOUND
--     ==> if ==  FV_SCAN: then return element if found, else return nil
--     ==> if ==  FV_DELETE:  then replace the found element with nil
-- Return:
-- For FV_SCAN and FV_DELETE:
--    nil if not found, Value if found.
--   (NOTE: Can't return 0 -- because that might be a valid value)
-- For insert (FV_INSERT):
-- Return 0 if found (and not inserted), otherwise 1 if inserted.
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
local function complexScanList(ldtCtrlMap, binList, value, flag ) 
  local meth = "complexScanList()";
  local result = nil;

  local transform = nil;
  local unTransform = nil;
  if ldtCtrlMap.Transform ~= nil then
    transform = functionTable[ldtCtrlMap.Transform];
  end

  if ldtCtrlMap.UnTransform ~= nil then
    unTransform = functionTable[ldtCtrlMap.UnTransform];
  end

  -- Scan the list for the item, return true if found,
  -- Later, we may return a set of things 
  local resultValue = nil;
  for i = 1, list.size( binList ), 1 do
    GP=F and trace("[DEBUG]: <%s:%s> It(%d) Comparing SV(%s) with BinV(%s)",
                   MOD, meth, i, tostring(value), tostring(binList[i]));
    if binList[i] ~= nil and binList[i] ~= FV_EMPTY then
      resultValue = unTransformComplexCompare(unTransform, binList[i], value);
      if resultValue ~= nil then
        GP=F and trace("[EARLY EXIT]: <%s:%s> Found(%s)",
          MOD, meth, tostring(resultValue));
        if( flag == FV_DELETE ) then
          binList[i] = FV_EMPTY; -- the value is NO MORE
          -- Decrement ItemCount (valid entries) but TotalCount stays the same
          local itemCount = ldtCtrlMap.ItemCount;
          ldtCtrlMap.ItemCount = itemCount - 1;
        elseif flag == FV_INSERT then
          return 0 -- show caller nothing got inserted (don't count it)
        end
        -- Found it -- return result
        return resultValue;
      end -- end if found it
    end -- end if value not nil or empty
  end -- for each list entry in this binList

  -- Didn't find it.  If FV_INSERT, then append the value to the list
  if flag == FV_INSERT then
    GP=F and trace("[DEBUG]: <%s:%s> INSERTING(%s)",
                   MOD, meth, tostring(value));

    -- apply the transform (if needed)
    local storeValue = applyTransform( transform, value );
    list.append( binList, storeValue );
    return 1 -- show caller we did an insert
  end

  GP=F and trace("[LATE EXIT]: <%s:%s> Did NOT Find(%s)",
    MOD, meth, tostring(value));
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
--     ==> if ==  FV_INSERT: insert the element IF NOT FOUND
--     ==> if ==  FV_SCAN: then return element if found, else return nil
--     ==> if ==  FV_DELETE:  then replace the found element with nil
-- Return:
-- For FV_SCAN and FV_DELETE:
--    nil if not found, Value if found.
--   (NOTE: Can't return 0 -- because that might be a valid value)
-- For FV_INSERT:
-- Return 0 if found (and not inserted), otherwise 1 if inserted.
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
local function simpleScanList(resultList, ldtCtrlMap, binList, value, flag,
  filter, fargs ) 
  local meth = "simpleScanList()";
  GP=F and trace("[ENTER]: <%s:%s> Looking for V(%s), ListSize(%d) List(%s)",
                 MOD, meth, tostring(value), list.size(binList),
                 tostring(binList))

  local rc = 0;
  -- Check once for the transform/untransform functions -- so we don't need
  -- to do it inside the loop.
  local transform = nil;
  local unTransform = nil;
  if ldtCtrlMap.Transform ~= nil then
    transform = functionTable[ldtCtrlMap.Transform];
  end

  if ldtCtrlMap.UnTransform ~= nil then
    unTransform = functionTable[ldtCtrlMap.UnTransform];
  end

  -- Scan the list for the item, return true if found,
  -- Later, we may return a set of things 
  local resultValue = nil;
  for i = 1, list.size( binList ), 1 do
    GP=F and trace("[DEBUG]: <%s:%s> It(%d) Comparing SV(%s) with BinV(%s)",
                   MOD, meth, i, tostring(value), tostring(binList[i]));
    if binList[i] ~= nil and binList[i] ~= FV_EMPTY then
      resultValue = unTransformSimpleCompare(unTransform, binList[i], value);
      if resultValue ~= nil then
        GP=F and trace("[EARLY EXIT]: <%s:%s> Found(%s)",
          MOD, meth, tostring(resultValue));
        if( flag == FV_DELETE ) then
          binList[i] = FV_EMPTY; -- the value is NO MORE
          -- Decrement ItemCount (valid entries) but TotalCount stays the same
          local itemCount = ldtCtrlMap.ItemCount;
          ldtCtrlMap.ItemCount = itemCount - 1;
        elseif flag == FV_INSERT then
          return 0 -- show caller nothing got inserted (don't count it)
        end
        -- Found it -- return result (only for scan and delete, not insert)
        list.append( resultList, resultValue );
        return 0; -- Found it. Return with success.
      end -- end if found it
    end -- end if not null and not empty
  end -- end for each item in the list

  -- Didn't find it.  If FV_INSERT, then append the value to the list
  -- Ideally, if we noticed a hole, we should use THAT for insert and not
  -- make the list longer.
  -- TODO: Fill in holes if we notice a lot of gas in the lists.
  if flag == FV_INSERT then
    GP=F and trace("[EXIT]: <%s:%s> Inserting(%s)",
                   MOD, meth, tostring(value));
    local storeValue = applyTransform( transform, value );
    list.append( binList, storeValue );
    return 1 -- show caller we did an insert
  end
  GP=F and trace("[LATE EXIT]: <%s:%s> Did NOT Find(%s)",
                 MOD, meth, tostring(value));
  return 0; -- All is well.
end -- simpleScanList


-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- Scan a List for an item.  Return the item if found.
-- Since there are two types of scans (simple, complex), we do the test
-- up front and call the appropriate scan type (rather than do the test
-- of which compare to do -- for EACH value.
-- Parms:
-- (*) ldtCtrlMap: the control map -- so we can see the type of key
-- (*) binList: the list of values from the record
-- (*) searchValue: the value we're searching for
-- (*) flag:
--     ==> if ==  FV_DELETE:  then replace the found element with nil
--     ==> if ==  FV_SCAN: then return element if found, else return nil
--     ==> if ==  FV_INSERT: insert the element IF NOT FOUND
-- Return: nil if not found, Value if found.
-- (NOTE: Can't return 0 -- because that might be a valid value)
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
local function scanList( resultList, ldtCtrlMap, binList, searchValue, flag,
    filter, fargs ) 
  local meth = "scanList()";

  GP=F and trace("[ENTER]:<%s:%s>Res(%s)Mp(%s)BL(%s)SV(%s)Fg(%s)F(%s)Frgs(%s)",
      MOD, meth, tostring( resultList), tostring(ldtCtrlMap),
      tostring(binList), tostring(searchValue), tostring(flag),
      tostring( filter ), tostring( fargs ));

  GP=F and trace("[DEBUG]:<%s:%s> KeyType(%s) A(%s) C(%s)",
      MOD, meth, tostring(ldtCtrlMap.KeyType), tostring(KT_ATOMIC),
      tostring(KT_COMPLEX) );

  -- Choices for KeyType are KT_ATOMIC or KT_COMPLEX
  if ldtCtrlMap.KeyType == KT_ATOMIC then
    return simpleScanList(resultList, ldtCtrlMap, binList, searchValue, flag ) 
  else
    return complexScanList(resultList, ldtCtrlMap, binList, searchValue, flag ) 
  end
end -- scanList()

-- ======================================================================
-- localInsert( ldtMap, newValue, stats )
-- ======================================================================
-- Perform the main work of insert (used by both convertList() and the
-- regular insert().
-- Parms:
-- (*) topRec: The top DB Record:
-- (*) ldtMap: The LDT control map
-- (*) newValue: Value to be inserted
-- (*) stats: 1=Please update Counts, 0=Do NOT update counts (rehash)
-- ======================================================================
local function localInsert( topRec, ldtMap, newValue, stats )
  local meth = "localInsert()";
  GP=F and trace("[ENTER]:<%s:%s>Insert(%s)", MOD, meth, tostring(newValue));

  -- If our state is "compact", do a simple list insert, otherwise do a
  -- real tree insert.
  local insertResult = 0;
  if( ldtMap.StoreState == SS_COMPACT ) then 
    insertResult = listInsert( topRec, ldtMap, newValue );
  else
    insertResult = treeInsert( topRec, ldtMap, newValue );
  end

  -- update stats if appropriate.
  if stats == 1 and insertResult == 1 then -- Update Stats if success
    local itemCount = ldtMap.ItemCount;
    local totalCount = ldtMap.TotalCount;
    ldtMap.ItemCount = itemCount + 1; -- number of valid items goes up
    ldtMap.TotalCount = totalCount + 1; -- Total number of items goes up
    GP=F and trace("[DEBUG]: <%s:%s> itemCount(%d)", MOD, meth, itemCount );
  end
  topRec[ldtMap] = ldtMap;

  GP=F and trace("[EXIT]: <%s:%s>Storing Record() with New Value(%s): Map(%s)",
                 MOD, meth, tostring( newValue ), tostring( ldtMap ) );
    -- No need to return anything
end -- localInsert


-- ======================================================================
-- convertList( topRec, ldtBinName, ldtMap )
-- ======================================================================
-- When we start in "compact" StoreState (SS_COMPACT), we eventually have
-- to switch to "regular" state when we get enough values.  So, at some
-- point (StoreThreshHold), we take our simple list and then insert into
-- the B+ Tree.
-- So -- copy out all of the items from bin 1, null out the bin, and
-- then resinsert them using "regular" mode.
-- Parms:
-- (*) topRec
-- (*) ldtBinName
-- (*) ldtMap
-- ======================================================================
local function convertList( topRec, ldtBinName, ldtMap )
  local meth = "rehashSet()";
  GP=F and trace("[ENTER]:<%s:%s> !!!! REHASH !!!! ", MOD, meth );
  GP=F and trace("[ENTER]:<%s:%s> !!!! REHASH !!!! ", MOD, meth );

  -- Get the list, make a copy, then iterate thru it, re-inserting each one.
  local singleBinName = getBinName( 0 );
  local singleBinList = topRec[singleBinName];
  if singleBinList == nil then
  warn("[INTERNAL ERROR]:<%s:%s> Rehash can't use Empty Bin (%s) list",
  MOD, meth, tostring(singleBinName));
  error('BAD BIN 0 LIST for Rehash');
  end
  local listCopy = list.take( singleBinList, list.size( singleBinList ));
  topRec[singleBinName] = nil; -- this will be reset shortly.
  ldtMap.StoreState = SS_REGULAR; -- now in "regular" (modulo) mode

  -- Rebuild. Allocate new lists for all of the bins, then re-insert.
  -- Create ALL of the new bins, each with an empty list
  -- Our "indexing" starts with ZERO, to match the modulo arithmetic.
  local distrib = ldtMap.Modulo;
  for i = 0, (distrib - 1), 1 do
  setupNewBin( topRec, i );
  end -- for each new bin

  for i = 1, list.size(listCopy), 1 do
    localInsert( topRec, ldtMap, listCopy[i], 0 ); -- do NOT update counts.
  end

  GP=F and trace("[EXIT]: <%s:%s>", MOD, meth );
end -- convertList()

-- ======================================================================
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- Large Ordered List (LLIST) Main Functions
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
--
-- ======================================================================
-- || listCreate ||
-- ======================================================================
-- Create/Initialize a Large Ordered List  structure in a bin, using a
-- single LLIST -- bin, using User's name, but Aerospike TYPE (AS_LOL)
--
-- We will use a SINGLE MAP object, which contains control information and
-- two lists (the root note Key and pointer lists).
-- (*) Namespace Name
-- (*) Set Name
-- (*) Tree Node Size
-- (*) Inner Node Count
-- (*) Data Leaf Node Count
-- (*) Total Item Count
-- (*) Storage Mode (Binary or List Mode): 0 for Binary, 1 for List
-- (*) Key Storage
-- (*) Value Storage
--
-- Parms (inside argList)
-- (1) topRec: the user-level record holding the LDT Bin
-- (2) argList: the list of create parameters
--  (2.1) LolBinName
--  (2.2) Namespace (just one, for now)
--  (2.3) Set
--  (2.4) LdrByteCountMax
--  (2.5) Design Version
--
function listCreate( topRec, ldtBinName, argList )
  local meth = "listCreate()";

  if argList == nil then
    GP=F and trace("[ENTER1]: <%s:%s> ldtBinName(%s) NULL argList",
      MOD, meth, tostring(ldtBinName));
  else
    GP=F and trace("[ENTER2]: <%s:%s> ldtBinName(%s) argList(%s) ",
    MOD, meth, tostring( ldtBinName), tostring( argList ));
  end

  -- Some simple protection if things are weird
  if ldtBinName == nil  or type(ldtBinName) ~= "string" then
    warn("[WARNING]: <%s:%s> Bad LDT BIN Name: Using default", MOD, meth );
    ldtBinName = "LolBin";
  end

  -- Check to see if LDT Structure (or anything) is already there,
  -- and if so, error
  if topRec[ldtBinName] ~= nil  then
    warn("[ERROR EXIT]: <%s:%s> LDT BIN(%s) Already Exists",
      MOD, meth, tostring(ldtBinName) );
    return('LDT_BIN already exists');
  end

  -- Create and initialize the LDT MAP -- the main LDT structure
  -- initializeLListMap() also assigns the map to the record bin.
  local ldtMap = initializeLListMap( topRec, ldtBinName );

  -- If the user has passed in settings that override the defaults
  -- (the argList), then process that now.
  if argList ~= nil then
    adjustLListMap( ldtMap, argList )
  end

  GP=F and trace("[DEBUG]:<%s:%s>:Dir Map after Init(%s)",
  MOD,meth,tostring(ldtMap));

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
end -- function listCreate( topRec, namespace, set )

-- ======================================================================
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- || local listInsert
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- This function does the work of both calls -- with and without inner UDF.
--
-- Insert a value into the list (into the B+ Tree).  We will have both a
-- COMPACT storage mode and a TREE storage mode.  When in COMPACT mode,
-- the root node holds the list directly (linear search and append).
-- When in Tree mode, the root node holds the top level of the tree.
-- Parms:
-- (*) topRec:
-- (*) ldtBinName:
-- (*) newValue:
-- (*) createSpec:
-- =======================================================================
local function localLListInsert( topRec, ldtBinName, newValue, createSpec )
  local meth = "localLListInsert()";
  GP=F and trace("[ENTER]:<%s:%s>LLIST BIN(%s) NwVal(%s) createSpec(%s)",
    MOD, meth, tostring(ldtBinName), tostring( newValue ),tostring(createSpec));

  local ldtMap;

  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  -- This function does not build, save or update.  It only checks.
  -- Check to see if LDT Structure (or anything) is already there.  If there
  -- is an LDT BIN present, then it MUST be valid.
  validateRecBinAndMap( topRec, ldtBinName, false );

  -- If the record does not exist, or the BIN does not exist, then we must
  -- create it and initialize the LDT map. Otherwise, use it.
  if( topRec[ldtBinName] == nil ) then
    GP=F and trace("[DEBUG]<%s:%s>LIST CONTROL BIN does not Exist:Creating",
         MOD, meth );
    ldtMap =
      initializeLListMap( topRec, ldtBinName );
    -- If the user has passed in some settings that override our defaults
    -- (createSpce) then apply them now.
    if createSpec ~= nil then 
      adjustLListMap( ldtMap, createSpec );
    end
    topRec[ldtBinName] = ldtMap;
  else
    -- all there, just use it
    ldtMap = topRec[ ldtBinName ];
  end
  -- Note: We'll do the aerospike:create() at the end of this function,
  -- if needed.

  -- When we're in "Compact" mode, before each insert, look to see if 
  -- it's time to turn our single list into a tree.
  local totalCount = ldtMap.TotalCount;
  if ldtMap.StoreState == SS_COMPACT and
    totalCount >= ldtMap.ThreshHold
  then
    convertList( topRec, ldtBinName, ldtMap );
  end
 
  -- Call our local multi-purpose insert() to do the job.(Update Stats)
  localInsert( topRec, ldtMap, newValue, 1 );

  -- All done, store the record (either CREATE or UPDATE)
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
end -- function localLListInsert()

-- =======================================================================
-- List Insert -- with and without inner UDFs
-- These are the globally visible calls -- that call the local UDF to do
-- all of the work.
-- =======================================================================
function llist_insert( topRec, ldtBinName, newValue )
  return localLListInsert( topRec, ldtBinName, newValue, nil )
end -- end llist_insert()

function llist_create_then_insert( topRec, ldtBinName, newValue, createSpec )
  return localLListInsert( topRec, ldtBinName, newValue, createSpec );
end -- llist_create_then_insert()

-- ======================================================================
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- || Local listSearch:
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- Return all of the objects that match "SearchValue".
--
-- Parms:
-- (*) topRec:
-- (*) ldtBinName:
-- (*) searchValue
-- (*) func:
-- (*) fargs:
-- ======================================================================
local function localListSearch( topRec, ldtBinName, searchValue, func, fargs )
  local meth = "localListSearch()";
  GP=F and trace("[ENTER]: <%s:%s> searchValue(%s) ",
      MOD, meth,tostring(searchValue) );

  -- Define our return list
  local resultList = list()
  
  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  validateRecBinAndMap( topRec, ldtBinName, true );
  
  -- Search the tree -- keeping track of the path from the root to the leaf
  --
  --
  local ldtMap = topRec[ldtBinName];
  local binNumber = computeSetBin( searchValue, ldtMap );
  local binName = getBinName( binNumber );
  local binList = topRec[binName];
  rc =
  scanList(resultList,ldtMap,binList,searchValue,FV_SCAN,filter,fargs);
  
  GP=F and trace("[EXIT]: <%s:%s>: Search Returns (%s)",
  MOD, meth, tostring(result));
  
  return rc;
  

  if (func ~= nil and fargs ~= nil ) then
GP=F and trace("[ENTER1]: <%s:%s> BIN(%s) srchVal(%s) func(%s) fargs(%s)",
      MOD, meth, tostring(ldtBinName), tostring(searchValue),
      tostring(func), tostring(fargs) );
  else
    GP=F and trace("[ENTER2]: <%s:%s> LLIST BIN(%s) searchValue(%s)", 
      MOD, meth, tostring(ldtBinName), tostring(searchValue) );
  end

  if( not aerospike:exists( topRec ) ) then
    warn("[ERROR EXIT]:<%s:%s>:Missing Record. Exit", MOD, meth );
    return('Base Record Does NOT exist');
  end

  -- Verify that the LLIST Structure is there: otherwise, error.
  if ldtBinName == nil  or type(ldtBinName) ~= "string" then
    warn("[ERROR EXIT]: <%s:%s> Bad LLIST BIN Parameter", MOD, meth );
    return('Bad LLIST Bin Parameter');
  end
  if( topRec[ldtBinName] == nil ) then
    warn("[ERROR EXIT]: <%s:%s> LLIST_BIN (%s) DOES NOT Exists",
      MOD, meth, tostring(ldtBinName) );
    return('LLIST_BIN Does NOT exist');
  end
  
  -- check that our bin is (mostly) there
  local ldtMap = topRec[ldtBinName]; -- The main lol map
  if ldtMap.Magic ~= "MAGIC" then
    GP=F and trace("[ERROR EXIT]: <%s:%s> LLIST_BIN (%s) Is Corrupted (no magic)",
      MOD, meth, ldtBinName );
    return('LLIST_BIN Is Corrupted');
  end

  -- Build the user's "resultList" from the items we find that qualify.
  -- They must pass the "transformFunction()" filter.
  -- Call map search to do the real work.
  GP=F and trace("[DEBUG]: <%s:%s>: Calling Map Peek", MOD, meth );
  local resultList = ldtMapSearch( topRec, ldtMap, peekCount, func, fargs );

  GP=F and trace("[EXIT]: <%s:%s>: SearchValue(%d) ResultListSummary(%s)",
    MOD, meth, tostring(searchValue), summarizeList(resultList));

  return resultList;
end -- function localListSearch() 

-- =======================================================================
-- listSearch -- with and without inner UDFs
-- These are the globally visible calls -- that call the local UDF to do
-- all of the work.
-- NOTE: All parameters must be protected with "tostring()" so that we
-- do not encounter a format error if the user passes in nil or any
-- other incorrect value/type.
-- =======================================================================
function list_search( topRec, ldtBinName, searchValue )
  local meth = "listSearch()";
  GP=F and trace("[ENTER]: <%s:%s> LLIST BIN(%s) searchValue(%s)",
    MOD, meth, tostring(ldtBinName), tostring(searchValue) )
  return localListSearch( topRec, ldtBinName, searchValue, nil, nil )
end -- end list_search()

function list_search_with_filter( topRec, ldtBinName, searchValue, func, fargs )
  local meth = "listSearch()";
  GP=F and trace("[ENTER]: <%s:%s> BIN(%s) searchValue(%s) func(%s) fargs(%s)",
    MOD, meth, tostring(ldtBinName), tostring(searchValue),
    tostring(func), tostring(fargs));

  return localListSearch( topRec, ldtBinName, searchValue, func, fargs )
end -- end list_search_with_filter()


-- ======================================================================
-- || llistDelete ||
-- ======================================================================
-- Delete the specified item(s).
--
-- Parms 
-- (1) topRec: the user-level record holding the LDT Bin
-- (2) LolBinName
-- (3) deleteValue: Search Structure
--
function llist_delete( topRec, ldtBinName, deleteValue )
  local meth = "listDelete()";

  if argList == nil then
    GP=F and trace("[ENTER1]: <%s:%s> ldtBinName(%s) NULL argList",
      MOD, meth, tostring(ldtBinName));
  else
    GP=F and trace("[ENTER2]: <%s:%s> ldtBinName(%s) argList(%s) ",
    MOD, meth, tostring( ldtBinName), tostring( argList ));
  end

  -- Some simple protection if things are weird
  if ldtBinName == nil  or type(ldtBinName) ~= "string" then
    warn("[WARNING]: <%s:%s> Bad LDT BIN Name: Using default", MOD, meth );
    ldtBinName = "LolBin";
  end

  -- Check to see if LDT Structure (or anything) is already there,
  -- and if so, error
  if topRec[ldtBinName] == nil  then
    warn("[ERROR EXIT]: <%s:%s> LLIST BIN(%s) Does Not Exist!",
      MOD, meth, tostring(ldtBinName) );
    return('LLIST_BIN does not exist');
  end

  -- Call map delete to do the real work.
  local result = ldtMapDelete( topRec, ldtBinName, deleteValue );

  GP=F and trace("[DEBUG]:<%s:%s>:Dir Map after Init(%s)",
  MOD,meth,tostring(ldtMap));

  -- All done, store the record
  local rc = -99; -- Use Odd starting Num: so that we know it got changed
  GP=F and trace("[DEBUG]:<%s:%s>:Update Record()", MOD, meth );
  rc = aerospike:update( topRec );

  GP=F and trace("[EXIT]: <%s:%s> : Done.  RC(%d)", MOD, meth, rc );
  return rc;
end -- function llist_delete()


-- ========================================================================
-- llist_size() -- return the number of elements (item count) in the set.
-- ========================================================================
function llist_size( topRec, ldtBinName )
  local meth = "llist_size()";

  GP=F and trace("[ENTER1]: <%s:%s> ldtBinName(%s)",
  MOD, meth, tostring(ldtBinName));

  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  validateRecBinAndMap( topRec, ldtBinName, true );

  local ldtMap = topRec[ ldtBinName ];
  local itemCount = ldtMap.ItemCount;

  GP=F and trace("[EXIT]: <%s:%s> : size(%d)", MOD, meth, itemCount );

  return itemCount;
end -- function llist_size()

-- ========================================================================
-- llist_config() -- return the config settings
-- ========================================================================
function llist_config( topRec, ldtBinName )
  local meth = "LList_config()";

  GP=F and trace("[ENTER1]: <%s:%s> ldtBinName(%s)",
  MOD, meth, tostring(ldtBinName));

  -- Validate the topRec, the bin and the map.  If anything is weird, then
  -- this will kick out with a long jump error() call.
  validateRecBinAndMap( topRec, ldtBinName, true );

  local config = LListSummary( topRec[ ldtBinName ] );

  GP=F and trace("[EXIT]: <%s:%s> : config(%s)", MOD, meth, config );

  return config;
end -- function llist_config()

-- ========================================================================
-- ========================================================================
-- ========================================================================

-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
