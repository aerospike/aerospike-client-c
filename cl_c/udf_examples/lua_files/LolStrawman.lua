-- Large Ordered List (LOL) Design
-- (March 21, 2013)(V1.1)
--
-- ======================================================================
-- The Large Ordered List is a sorted list, organized according to a Key
-- value.  It is assumed that the stored object is more complex than just an
-- atomic key value -- otherwise one of the other Large Object mechanisms
-- (e.g. Large Stack, Large Set) would be used.  The cannonical form of a
-- LOL element is a map, which includes a KEY field and other data fields.
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
-- (*) llist_create: Create the LOL structure in the chosen topRec bin
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
-- LOL Design and Type Comments:
--
-- The LOL value is a new "particle type" that exists ONLY on the server.
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
--|1|3| |6|7|8| |22|26| |32|39||42|48||51|55| |61|64| |71|75||83|86| |91|95|99|
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
-- we initialize the LolMap in topRec[LolBinName]
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

-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- <><><><> <Initialize Control Maps> <Initialize Control Maps> <><><><>
-- There are three main Record Types used in the LOL Package, and their
-- initialization functions follow.  The initialization functions
-- define the "type" of the control structure:
--
-- (*) TopRec: the top level user record that contains the LOL bin,
--     including the Root Directory.
-- (*) InnerNodeRec: Interior B+ Tree nodes
-- (*) DataNodeRec: The Data Leaves
--
-- <+> Naming Conventions:
--   + All Field names (e.g. lolMap.PageMode) begin with Upper Case
--   + All variable names (e.g. lolMap.PageMode) begin with lower Case
--   + All Record Field access is done using brackets, with either a
--     variable or a constant (in single quotes).
--     (e.g. topRec[binName] or ldrRec['LdrControlBin']);
--
-- <><><><> <Initialize Control Maps> <Initialize Control Maps> <><><><>

-- ======================================================================
-- initializeLolMap:
-- ======================================================================
-- Set up the LOL Map with the standard (default) values.
-- These values may later be overridden by the user.
-- The structure held in the Record's "LOL BIN" is this map.  This single
-- structure contains ALL of the settings/parameters that drive the LOL
-- behavior.  Thus this function represents the "type" LOL MAP -- all
-- LOL control fields are defined here.
-- The LolMap is obtained using the user's LOL Bin Name:
-- lolMap = topRec[lolBinName]
-- ======================================================================
local function initializeLolRoot( topRec, lolBinName, transFunc, untransFunc,
                                  funcArgs )
  local mod = "LolStrawman";
  local meth = "initializeLolMap()";
  GP=F and trace("[ENTER]: <%s:%s>:: LolBinName(%s)",
    mod, meth, tostring(lolBinName));

  -- The LOL Map -- with Default Values
  -- General Tree Settings
  lolCtrlMap.ItemCount = 0;     -- A count of all items in the LOL
  lolCtrlMap.DesignVersion = 1; -- Current version of the code
  lolCtrlMap.Magic = "MAGIC";   -- Used to verify we have a valid map
  lolCtrlMap.BinName = lolBinName; -- Name of the Bin for this LOL in TopRec
  lolCtrlMap.NameSpace = "test"; -- Default NS Name -- to be overridden by user
  lolCtrlMap.Set = "set";       -- Default Set Name -- to be overridden by user
  lolCtrlMap.PageMode = "List"; -- "List" or "Binary" (applies to all nodes)
  lolCtrlMap.TreeLevel = 1;     -- Start off Lvl 1: Root plus leaves
  lolCtrlMap.DataLeafCount = 0;
  lolCtrlMap.InnerNodeCount = 0;
  lolCtrlMap.KeyType = 0; -- 0 is atomic, 1 is map (with a KEY field).
  lolCtrlMap.TransFunc = 0; -- Name of the transform (from user to storage)
  lolCtrlMap.UnTransFunc = 0; -- Reverse transform (from storage to user)
  --
  -- Top Node Tree Root Directory
  lolCtrlMap.RootDirMax = 100;
  lolCtrlMap.KeyByteArray = 0; -- Byte Array, when in compressed mode
  lolCtrlMap.DigestByteArray = 0; -- DigestArray, when in compressed mode
  lolCtrlMap.KeyList = 0; -- Key List, when in List Mode
  lolCtrlMap.DigestList = 0; -- Digest List, when in List Mode
  
  -- LOL Inner Node Settings
  lolCtrlMap.InnerNodeEntryCountMax = 50;  -- Max # of items (key+digest)
  lolCtrlMap.InnerNodeByteEntrySize = 11;  -- Size (in bytes) of Key obj
  lolCtrlMap.InnerNodeByteCountMax = 2000; -- Max # of BYTES

  -- LOL Tree Leaves (Data Pages)
  lolCtrlMap.DataPageEntryCountMax = 100;  -- Max # of items
  lolCtrlMap.DataPageByteEntrySize = 44;  -- Size (in bytes) of data obj
  lolCtrlMap.DataPageByteCountMax = 2000; -- Max # of BYTES per data page

  GP=F and trace("[DEBUG]: <%s:%s> : CTRL Map after Init(%s)",
      mod, meth , tostring(lolCtrlMap));

  -- Put our new map in the record, then store the record.
  topRec[lolBinName] = lolMap;

  GP=F and trace("[EXIT]:<%s:%s>:", mod, meth );
  return lolMap
end -- initializeLolMap


-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- || B+ Tree Data Page Record |||||||||||||||||||||||||||||||||||||||||||
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- Records used for B+ Tree modes have three bins:
-- Chunks hold the actual entries. Each LSO Data Record (LDR) holds a small
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
local function initializeNodeMap(topRec, parentRec, nodeRec, nodeMap, lolMap)
  local mod = "LolStrawman";
  local meth = "initializeNodeMap()";
  GP=F and trace("[ENTER]: <%s:%s>", mod, meth );

  nodeMap.RootDigest = record.digest( topRec );
  nodeMap.ParentDigest = record.digest( parentRec );
  nodeMap.PageMode = lolMap.PageMode;
  nodeMap.Digest = record.digest( nodeRec );
  -- Note: Item Count is implicitly the KeyList size
  nodeMap.KeyListMax = 100; -- Digest List is ONE MORE than Key List
  nodeMap.ByteEntrySize = lolMap.LdrByteEntrySize; -- ByteSize of Fixed Entries
  nodeMap.ByteEntryCount = 0;  -- A count of Byte Entries
  nodeMap.ByteCountMax = lolMap.LdrByteCountMax; -- Max # of bytes in ByteArray
  nodeMap.Version = lolMap.Version;
  nodeMap.LogInfo = 0;
end -- initializeNodeMap()

-- ======================================================================
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- || B+ Tree Data Page Record |||||||||||||||||||||||||||||||||||||||||||
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- Records used for B+ Tree modes have three bins:
-- Chunks hold the actual entries. Each LSO Data Record (LDR) holds a small
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
local function initializeLeafMap(topRec, parentRec, leafRec, leafMap, lolMap)
  local mod = "LolStrawman";
  local meth = "initializeLeafMap()";
  GP=F and trace("[ENTER]: <%s:%s>", mod, meth );

  leafMap.RootDigest = record.digest( topRec );
  leafMap.ParentDigest = record.digest( parentRec );
  leafMap.PageMode = lolMap.PageMode;
  leafMap.Digest = record.digest( leafRec );
  -- Note: Item Count is implicitly the KeyList size
  leafMap.DataListMax = 100; -- Max Number of items in List of data items
  leafMap.ByteEntrySize = lolMap.LdrByteEntrySize; -- ByteSize of Fixed Entries
  leafMap.ByteEntryCount = 0;  -- A count of Byte Entries
  leafMap.ByteCountMax = lolMap.LdrByteCountMax; -- Max # of bytes in ByteArray
  leafMap.Version = lolMap.Version;
  leafMap.LogInfo = 0;
end -- initializeLeafMap()

-- ======================================================================
-- ======================================================================
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- Large Ordered List (LOL) Utility Functions
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- These are all local functions to this module and serve various
-- utility and assistance functions.

-- ======================================================================
-- adjustLolMap:
-- ======================================================================
-- Using the settings supplied by the caller in the listCreate call,
-- we adjust the values in the LolMap.
-- Parms:
-- (*) lolMap: the main List Bin value
-- (*) argListMap: Map of List Settings 
-- ======================================================================
local function adjustLolMap( lolMap, argListMap )
  local mod = "LolStrawman";
  local meth = "adjustLolMap()";
  GP=F and trace("[ENTER]: <%s:%s>:: LolMap(%s)::\n ArgListMap(%s)",
    mod, meth, tostring(lolMap), tostring( argListMap ));

  -- Iterate thru the argListMap and adjust (override) the map settings 
  -- based on the settings passed in during the listCreate() call.
  GP=F and trace("[DEBUG]: <%s:%s> : Processing Arguments:(%s)",
    mod, meth, tostring(argListMap));

-- Fill in when we have a better idea of the settings.

  GP=F and trace("[DEBUG]: <%s:%s> : CTRL Map after Adjust(%s)",
    mod, meth , tostring(lolMap));

  GP=F and trace("[EXIT]:<%s:%s>:Dir Map after Init(%s)",
      mod,meth,tostring(lolMap));

  return lolMap
end -- adjustLolMap

-- ======================================================================
-- validateTopRec( topRec, lolMap )
-- ======================================================================
-- Validate that the top record looks valid:
-- Get the LSO bin from the rec and check for magic
-- Return: "good" or "bad"
-- ======================================================================
local function  validateTopRec( topRec, lolMap )
  local thisMap = topRec[lolMap.BinName];
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
  local mod = "LsoSuperMan";
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
--
-- ======================================================================
-- local function Tree Summary( lolMap ) (DEBUG/Trace Function)
-- ======================================================================
-- For easier debugging and tracing, we will summarize the Tree Map
-- contents -- without printing out the entire thing -- and return it
-- as a string that can be printed.
-- ======================================================================
local function lolSummary( lolMap )
  local resultMap             = map();
  resultMap.SUMMARY           = "List Summary String";

  return tostring( resultMap );
end -- lolSummary()

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
-- rootNodeSummary( topRec, lolMap )
-- ======================================================================
-- Print out interesting stats about this B+ Tree Root
-- ======================================================================
local function  rootNodeSummary( topRec, lolMap )
  local resultMap = lolMap;

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
  local mod = "LolStrawman";
  local meth = "nodeHasRoom()";
  GP=F and trace("[ENTER]: <%s:%s> keyList(%s) ListMax(%s)",
    mod, meth, tostring(keyList), tostring(listMax) );

  local result = 1;  -- Be optimistic 

  -- TODO: Finish Method
  print("[!!! FINISH THIS METHOD !!! (%s) ", meth );


  GP=F and trace("[EXIT]: <%s:%s> result(%d) ", mod, meth, result );
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
local function keyListInsert( keyList, newKey, digestList, newDigest )
  local mod = "LolStrawman";
  local meth = "keyListInsert()";
  GP=F and trace("[ENTER]: <%s:%s> : Insert Value(%s), keyList(%s)",
    mod, meth, tostring(newKey), tostring( keyList ));

  local rc = 0;

  local position = searchKeyList( keyList, newKey );

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
-- (*) lolMap:
-- (*) resultList:
-- (*) searchValue:
-- (*) func:
-- (*) fargs:
-- (*) flag:
-- ======================================================================
local function searchLeaf(topRec, leafNode, searchPath, lolMap, resultList,
                          searchValue, func, fargs, flag)
  -- Linear scan of the Leaf Node (binary search will come later), for each
  -- match, add to the resultList.
  local compareResult = 0;
  if lolMap.PageMode == 0 then
    -- Do the BINARY page mode search here
    GP=F and trace("[WARNING]: <%s:%s> :BINARY MODE NOT IMPLEMENTED",
        mod, meth, tostring(newStorageValue), tostring( resultList));
    return nil; -- TODO: Build this mode.
  else
    -- Do the List page mode search here
    -- Later: Split the loop search into two -- atomic and map objects
    local leafDataList = leafNode['DataListBin'];
    local keyType = lolMap.KeyType;
    for i = 1, list.size( leafDataList ), 1 do
      compareResult = compare( keyType, searchValue, leafDataList[i] );
      if compareResult == -2 then
        return nil -- error result.
      end
      if compareResult == 0 then
        -- Start gathering up values
        gatherLeafListData( topRec, leafNode, lolMap, resultList, searchValue,
          func, fargs, flag );
        GP=F and trace("[FOUND VALUES]: <%s:%s> : Value(%s) Result(%s)",
          mod, meth, tostring(newStorageValue), tostring( resultList));
          return resultList;
      elseif compareResult  == 1 then
        GP=F and trace("[NotFound]: <%s:%s> : Value(%s)",
          mod, meth, tostring(newStorageValue) );
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
local function locateLeaf( topRec, lolMap, searchValue, func, fargs )
    -- Start at the root, find the appropriate record pointer and 
    -- traverse the tree down to the data page.  Then hand off searching
    -- to the searchLeaf() function.
  local mod = "LolStrawman";
  local meth = "hotListInsert()";
  GP=F and trace("[ENTER]: <%s:%s> : Insert Value(%s)",
    mod, meth, tostring(newStorageValue) );

  local rootKeyList = lolMap.
end

-- ======================================================================
-- lolMapSearch(): Read all elements matching SearchValue
-- ======================================================================
-- Parms:
-- (*) lolMap: The main LSO structure (stored in the LSO Bin)
-- (*) peekCount: The total count to read (0 means all)
-- (*) Optional inner UDF Function (from the UdfFunctionTable)
-- (*) fargs: Function arguments (list) fed to the inner UDF
-- Return: The Peek resultList -- in LIFO order
-- ======================================================================
local function lolMapSearch( topRec, lolMap, searchValue, func, fargs )
  local mod = "LolStrawman";
  local meth = "lolMapSearch()";
  GP=F and trace("[ENTER]: <%s:%s> searchValue(%s)",
      mod, meth, tostring(searchValue));

  if (func ~= nil and fargs ~= nil ) then
    GP=F and trace("[ENTER1]: <%s:%s> SearchValue(%s) func(%s) fargs(%s)",
      mod, meth, tostring(searchValue), tostring(func), tostring(fargs) );
  else
    GP=F and trace("[ENTER2]: <%s:%s> SearchValue(%s)",
    mod, meth, tostring(searchValue));
  end

  -- Set up our resultList list. Add to the resultList from every node that
  -- contains keys that match the searchValue
  local resultList = list();

  local leafNode = locateLeaf( topRec, lolMap, searchValue, func, fargs );
  -- a tree search returns a STRUCTURE that shows the search path and the
  -- location of the item (or the insert position).
  local flag = 'S'; -- S=Search, I=Insert, D=Delete
  -- leafSearch will scan the linked list of Tree Leaf Pages.
  local searchPath  =
    searchLeaf(topRec,leafNode,lolMap,resultList,searchValue,func,fargs,flag);

  GP=F and trace("[EXIT]: <%s:%s> searchValue(%s) resultList(%s)",
      mod, meth, tostring(searchValue), tostring(resultList));
  
  return resultList; 
end -- function lolMapSearch()

-- ======================================================================
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- Large Ordered List (LOL) Main Functions
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
--
-- ======================================================================
-- || listCreate ||
-- ======================================================================
-- Create/Initialize a Large Ordered List  structure in a bin, using a
-- single LOL -- bin, using User's name, but Aerospike TYPE (AS_LOL)
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
-- (1) topRec: the user-level record holding the LSO Bin
-- (2) argList: the list of create parameters
--  (2.1) LolBinName
--  (2.2) Namespace (just one, for now)
--  (2.3) Set
--  (2.4) LdrByteCountMax
--  (2.5) Design Version
--
function listCreate( topRec, lolBinName, argList )
  local mod = "LolStrawman";
  local meth = "listCreate()";

  if argList == nil then
    GP=F and trace("[ENTER1]: <%s:%s> lolBinName(%s) NULL argList",
      mod, meth, tostring(lolBinName));
  else
    GP=F and trace("[ENTER2]: <%s:%s> lolBinName(%s) argList(%s) ",
    mod, meth, tostring( lolBinName), tostring( argList ));
  end

  -- Some simple protection if things are weird
  if lolBinName == nil  or type(lolBinName) ~= "string" then
    warn("[WARNING]: <%s:%s> Bad LSO BIN Name: Using default", mod, meth );
    lolBinName = "LolBin";
  end

  -- Check to see if LSO Structure (or anything) is already there,
  -- and if so, error
  if topRec[lolBinName] ~= nil  then
    warn("[ERROR EXIT]: <%s:%s> LSO BIN(%s) Already Exists",
      mod, meth, tostring(lolBinName) );
    return('LSO_BIN already exists');
  end

  -- Create and initialize the LSO MAP -- the main LSO structure
  -- initializeLolMap() also assigns the map to the record bin.
  local lolMap = initializeLolMap( topRec, lolBinName );

  -- If the user has passed in settings that override the defaults
  -- (the argList), then process that now.
  if argList ~= nil then
    adjustLolMap( lolMap, argList )
  end

  GP=F and trace("[DEBUG]:<%s:%s>:Dir Map after Init(%s)",
  mod,meth,tostring(lolMap));

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
end -- function listCreate( topRec, namespace, set )

-- ======================================================================
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- || local listInsert
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- This function does the work of both calls -- with and without inner UDF.
--
-- Insert a value into the list (into the B+ Tree)
-- Parms:
-- (*) topRec:
-- (*) lolBinName:
-- (*) newValue:
-- (*) func:
-- (*) fargs:
-- =======================================================================
local function localStackPush( topRec, lolBinName, newValue, func, fargs )
  local mod = "LolStrawman";
  local meth = "localStackPush()";

  local doTheFunk = 0; -- when == 1, call the func(fargs) on the Push item
  local functionTable = require('UdfFunctionTable');

  if (func ~= nil and fargs ~= nil ) then
    doTheFunk = 1;
    GP=F and trace("[ENTER1]:<%s:%s>LOL BIN(%s) NewVal(%s) func(%s) fargs(%s)",
      mod, meth, tostring(lolBinName), tostring( newValue ),
      tostring(func), tostring(fargs) );
  else
    GP=F and trace("[ENTER2]: <%s:%s> LOL BIN(%s) NewValue(%s)",
      mod, meth, tostring(lolBinName), tostring( newValue ));
  end

  -- Some simple protection if things are weird
  if lolBinName == nil  or type(lolBinName) ~= "string" then
    warn("[WARNING]: <%s:%s> Bad LSO BIN Name: Using default", mod, meth );
    lolBinName = "LolBin";
  end

  local lolMap;
  if( not aerospike:exists( topRec ) ) then
    GP=F and trace("[WARNING]:<%s:%s>:Record Does Not exist. Creating", mod, meth );
    lolMap = initializeLolMap( topRec, lolBinName );
    aerospike:create( topRec );
  elseif ( topRec[lolBinName] == nil ) then
    GP=F and trace("[WARNING]: <%s:%s> LSO BIN (%s) DOES NOT Exist: Creating",
                   mod, meth, tostring(lolBinName) );
    lolMap = initializeLolMap( topRec, lolBinName );
    aerospike:create( topRec );
  end
  
  -- check that our bin is (relatively intact
  local lolMap = topRec[lolBinName]; -- The main LSO map
  if lolMap.Magic ~= "MAGIC" then
    warn("[ERROR EXIT]: <%s:%s> LSO_BIN (%s) Is Corrupted (no magic)",
      mod, meth, lolBinName );
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
  if hotListHasRoom( lolMap, newStorageValue ) == 0 then
    GP=F and trace("[DEBUG]:<%s:%s>: CALLING TRANSFER HOT CACHE!!",mod, meth );
    hotListTransfer( topRec, lolMap );
  end
  hotListInsert( lolMap, newStorageValue );
  -- Must always assign the object BACK into the record bin.
  topRec[lolBinName] = lolMap;

  -- All done, store the topRec.  Note that this is the ONLY place where
  -- we should be updating the TOP RECORD.  If something fails before here,
  -- we would prefer that the top record remains unchanged.
  local rc = -99; -- Use Odd starting Num: so that we know it got changed
  GP=F and trace("[DEBUG]:<%s:%s>:Update Record", mod, meth );
  rc = aerospike:update( topRec );

  GP=F and trace("[EXIT]: <%s:%s> : Done.  RC(%d)", mod, meth, rc );
  return rc
end -- function localStackPush()

-- =======================================================================
-- List Insert -- with and without inner UDFs
-- These are the globally visible calls -- that call the local UDF to do
-- all of the work.
-- =======================================================================
function listInsert( topRec, lolBinName, newValue )
  return localListInsert( topRec, lolBinName, newValue, nil, nil )
end -- end listInsert()

function listInsert( topRec, lolBinName, newValue, func, fargs )
  return localListInsert( topRec, lolBinName, newValue, func, fargs );
end -- listInsert()

-- ======================================================================
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- || Local listSearch:
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- Return all of the objects that match "SearchValue".
--
-- Parms:
-- (*) topRec:
-- (*) lolBinName:
-- (*) searchValue
-- (*) func:
-- (*) fargs:
-- ======================================================================
local function localListSearch( topRec, lolBinName, searchValue, func, fargs )
  local mod = "LolStrawman";
  local meth = "localListSearch()";
  GP=F and trace("[ENTER]: <%s:%s> searchValue(%s) ",
      mod, meth,tostring(searchValue) );

  if (func ~= nil and fargs ~= nil ) then
    GP=F and trace("[ENTER1]: <%s:%s> BIN(%s) srchVal(%s) func(%s) fargs(%s)",
      mod, meth, tostring(lolBinName), tostring(searchValue),
      tostring(func), tostring(fargs) );
  else
    GP=F and trace("[ENTER2]: <%s:%s> LOL BIN(%s) searchValue(%s)", 
      mod, meth, tostring(lolBinName), tostring(searchValue) );
  end

  if( not aerospike:exists( topRec ) ) then
    warn("[ERROR EXIT]:<%s:%s>:Missing Record. Exit", mod, meth );
    return('Base Record Does NOT exist');
  end

  -- Verify that the LOL Structure is there: otherwise, error.
  if lolBinName == nil  or type(lolBinName) ~= "string" then
    warn("[ERROR EXIT]: <%s:%s> Bad LOL BIN Parameter", mod, meth );
    return('Bad LOL Bin Parameter');
  end
  if( topRec[lolBinName] == nil ) then
    warn("[ERROR EXIT]: <%s:%s> LOL_BIN (%s) DOES NOT Exists",
      mod, meth, tostring(lolBinName) );
    return('LOL_BIN Does NOT exist');
  end
  
  -- check that our bin is (mostly) there
  local lolMap = topRec[lolBinName]; -- The main lol map
  if lolMap.Magic ~= "MAGIC" then
    GP=F and trace("[ERROR EXIT]: <%s:%s> LOL_BIN (%s) Is Corrupted (no magic)",
      mod, meth, lolBinName );
    return('LOL_BIN Is Corrupted');
  end

  -- Build the user's "resultList" from the items we find that qualify.
  -- They must pass the "transformFunction()" filter.
  -- Call map search to do the real work.
  GP=F and trace("[DEBUG]: <%s:%s>: Calling Map Peek", mod, meth );
  local resultList = lolMapSearch( topRec, lolMap, peekCount, func, fargs );

  GP=F and trace("[EXIT]: <%s:%s>: SearchValue(%d) ResultListSummary(%s)",
    mod, meth, tostring(searchValue), summarizeList(resultList));

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
function listSearch( topRec, lolBinName, searchValue )
  local mod = "LolStrawman";
  local meth = "listSearch()";
  GP=F and trace("[ENTER]: <%s:%s> LOL BIN(%s) searchValue(%s)",
    mod, meth, tostring(lolBinName), tostring(searchValue) )
  return localListSearch( topRec, lolBinName, searchValue, nil, nil )
end -- end listSearch()

function listSearch( topRec, lolBinName, searchValue, func, fargs )
  local mod = "LolStrawman";
  local meth = "listSearch()";
  GP=F and trace("[ENTER]: <%s:%s> BIN(%s) searchValue(%s) func(%s) fargs(%s)",
    mod, meth, tostring(lolBinName), tostring(searchValue),
    tostring(func), tostring(fargs));

  return localListSearch( topRec, lolBinName, searchValue, func, fargs )
end -- end listSearch()


-- ======================================================================
-- || listDelete ||
-- ======================================================================
-- Delete the specified item(s).
--
-- Parms 
-- (1) topRec: the user-level record holding the LSO Bin
-- (2) LolBinName
-- (3) deleteValue: Search Structure
--
function listDelete( topRec, lolBinName, deleteValue )
  local mod = "LolStrawman";
  local meth = "listDelete()";

  if argList == nil then
    GP=F and trace("[ENTER1]: <%s:%s> lolBinName(%s) NULL argList",
      mod, meth, tostring(lolBinName));
  else
    GP=F and trace("[ENTER2]: <%s:%s> lolBinName(%s) argList(%s) ",
    mod, meth, tostring( lolBinName), tostring( argList ));
  end

  -- Some simple protection if things are weird
  if lolBinName == nil  or type(lolBinName) ~= "string" then
    warn("[WARNING]: <%s:%s> Bad LSO BIN Name: Using default", mod, meth );
    lolBinName = "LolBin";
  end

  -- Check to see if LSO Structure (or anything) is already there,
  -- and if so, error
  if topRec[lolBinName] == nil  then
    warn("[ERROR EXIT]: <%s:%s> LOL BIN(%s) Does Not Exist!",
      mod, meth, tostring(lolBinName) );
    return('LOL_BIN does not exist');
  end

  -- Call map delete to do the real work.
  local result = lolMapDelete( topRec, lolBinName, deleteValue );

  GP=F and trace("[DEBUG]:<%s:%s>:Dir Map after Init(%s)",
  mod,meth,tostring(lolMap));

  -- All done, store the record
  local rc = -99; -- Use Odd starting Num: so that we know it got changed
  GP=F and trace("[DEBUG]:<%s:%s>:Update Record()", mod, meth );
  rc = aerospike:update( topRec );

  GP=F and trace("[EXIT]: <%s:%s> : Done.  RC(%d)", mod, meth, rc );
  return rc;
end -- function listDelete()

-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
