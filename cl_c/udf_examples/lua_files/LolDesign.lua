-- Large Ordered List (LOL) Design
-- (March 15, 2013)(V1.1)
--
-- ======================================================================
-- The Large Ordered List is a sorted list, organized according to a Key
-- value.  It is assumed that the stored object is more complex than just an
-- atomic key value -- otherwise one of the other Large Object mechanisms
-- (e.g. Large Stack, Large Set) would be used.  The cannonical form of a
-- LOL element is a map, which includes a KEY field and other data fields.
-- Compared to Large Stack and Large Set, the Large Ordered List is managed
-- continuously (i.e. it is kept sorted), so there is some additional
-- overhead in the storage operation (to do the insertion sort), but there
-- is reduced overhead for the retieval operation, since it is doing a
-- binary search (order log(N)) rather than scan (order N).
-- ======================================================================
-- Functions Supported
-- (*) ListCreate: Create the LLO structure in the chosen topRec bin
-- (*) ListInsert: Push a user value (AS_VAL) onto the stack
-- (*) ListSearch: Search the ordered list, using binary search
-- (*) ListDelete: Remove an element from the list
-- ==> The Insert, Search and Delete functions have a "Multi" option,
--     which allows the caller to pass in multiple list keys that will
--     result in multiple operations.  Multi-operations provide higher
--     performance since there can be many operations performed with
--     a single "client-server crossing".
-- (*) ListMultiInsert():
-- (*) ListMultiSearch():
-- (*) ListMultiDelete():
-- ==> The Insert and Search functions have the option of passing in a
--     Transformation/Filter UDF that modifies values before storage or
--     modify and filter values during retrieval.
-- (*) ListInsert, ListMultiInsert:
--     Insert a user value (AS_VAL) in the ordered list, 
--     calling the supplied UDF on the value FIRST to transform it before
--     storing it.
-- (*) ListSearch, ListMultiSearch:
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
-- LOL Visual Depiction
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- In a user record, the bin holding the Large Ordered List (LOL) is
-- referred to as an "LOL" bin. The overhead of the LOL value is 
-- (*) LOL Control Info (~?? bytes)
-- (*) LOL Node Directory:
-- NOTES:
-- (*) The LOL Root Node directory may point directly to data pages (i.e.
--     record digests of records holding data) or inner tree nodes (i.e.
--     record digests of records holding search keys and digests of records
--     holding user data).
-- (*) The B+Tree holding the ordered list can be multiple levels deep.
--     It will start at level 1 (the root directory in the record and a
--     set of data pages)
-- (*) Record types used in this design:
-- (1) There is the main record that contains the LOL control bin
--     and the LOL Root Directory bin (The LOL Root). The Root directory
--     may be in either LIST mode or BINARY mode.  In BINARY mode, the
--     user keys must be fixed length comparable in "byte" form, meaning
--     they will be treated as large numbers.
-- (2) There is the B+Tree inner node, which is a list of keys and digests
--     that point to either (1) another level of inner nodes, or (2) B+Tree
--     data pages.
-- (3) B+Tree Data pages: a list of key/data objects. The list will have two
--     forms: Unique and non-Unique.  In the Unique case, the key will be
--     present with the object in every case -> [<key><Object>].  In the
--     non-unique case, a count field will show how many objects are present
--     for a particular key [<key><count>(<object>,<object>...<object>)].
-- ==> Multiple leaf pages of THE SAME KEY will be chained together.  The
--     scan of pages will use the NEXT/PREVIOUS pointers once the search
--     finds the start.
--
-- (*) How it all connects together....
-- (+) The Top Record contains two bins: Control and Root Directory
-- (+) The Root Directory contains keys and digests that point to Tree nodes,
--     either inner nodes or data nodes.
--
-- (*) We wish to keep the Root directory in a separate bin so that we can
--     (potentially) avoid any de-serialization of the key/digest values.
--     When we are in "binary mode", the bytes will be packed in a BYTES
--     value that is not serialized/deserialized.  In "list mode", we will
--     use regular lists.
-- (*) We may have to add some auxilliary information that will help
--     pick up the pieces in the event of a network/replica problem, where
--     some things have fallen on the floor.  There might be some "shadow
--     values" in there that show old/new values -- like when we install
--     a new cold dir head, and other things.  TBD
--
-- +-----+-----+-----+-----+----+-----------------------------------+
-- |User |User |     |LOL  |LOL |                                   |
-- |Bin 1|Bin 2|o o o|Ctrl |Root|                                   |
-- |     |     |     |Bin  |Dir |                                   |
-- +-----+-----+-----+--+--+--+-+-----------------------------------+
--                      V     |  +=+=+=+=+=+=+=+=+=+=+=+=+          
--                   +=====+  +->| | | | | | | | | | | | | Root Dir
--                   |CTRL |     +=+=+=+=+=+=+=+=+=+=+=+=+          
--                   | MAP |     (One Root Dir Entry: [Key, Digest])                                  
--                   +=====+                                        
--
                                  B+Tree of Order 4, level 2
                                  +-------+
                                  | Root  |
                                  | Node  |     Root Node (Order 4)
  Inner Nodes (Order 4)           ++-+-+-++
                                   | | | | 
         +------------------------ + | | +--------------------------+
         |                  +--------+ +---------+                  |
         |                  |                    |                  |
         V                  V                    V                  V
     +---+---+          +---+---+            +---+---+          +---+---+
     | Inner |          | Inner |            | Inner |          | Inner |  
     | Node  |          | Node  |            | Node  |          | Node  |  
     +++---+++          +++---+++            +++---+++          +++---+++
      ||   ||            ||   ||              ||   ||            ||   ||
  +---+|   |+---+    +---+|   |+---+      +---+|   |+---+    +---+|   |+---+
  |    |   |    |    |    |   |    |      |    |   |    |    |    |   |    |
  V    V   V    V    V    V   V    V      V    V   V    V    V    V   V    V
+-++ +-++ ++-+ ++-++-++ +-++ ++-+ ++-+  +-++ +-++ ++-+ ++-++-++ +-++ ++-+ ++-+
|D | |D | |D | |D ||D | |D | |D | |D |  |D | |D | |D | |D ||D | |D | |D | |D |
|A | |A | |A | |A ||A | |A | |A | |A |  |A | |A | |A | |A ||A | |A | |A | |A |
|T | |T | |T | |T ||T | |T | |T | |T |  |T | |T | |T | |T ||T | |T | |T | |T |
|A | |A | |A | |A ||A | |A | |A | |A |  |A | |A | |A | |A ||A | |A | |A | |A |
|  | |  | |  | |  ||  | |  | |  | |  |  |  | |  | |  | |  ||  | |  | |  | |  |
|PG| |PG| |PG| |PG||PG| |PG| |PG| |PG|  |PG| |PG| |PG| |PG||PG| |PG| |PG| |PG|
+--+ +--+ +--+ +--++--+ +--+ +--+ +--+  +--+ +--+ +--+ +--++--+ +--+ +--+ +--+
--   ================================================================
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- || LOL Control Bin CONTENTS  ||||||||||||||||||||||||||||||||||||||||||
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- In the LOL bin (named by the user), there is a Map object:
-- accessed, as follows:
--  local lolCtrlMap = topRec[lolCtrlBinName]; -- The main LOL map
-- The Contents of the map (with values, is as follows:
-- Note: The arglist values show which values are configured by the user:
--
-- The LOL Map -- with Default Values
-- General Parms:
lolCtrlMap.ItemCount = 0;     -- A count of all items in the LOL
lolCtrlMap.DesignVersion = 1; -- Current version of the code
lolCtrlMap.Magic = "MAGIC";   -- Used to verify we have a valid map
lolCtrlMap.BinName = lsoBinName; -- The name of the Bin for this LOL in TopRec
lolCtrlMap.NameSpace = "test"; -- Default NS Name -- to be overridden by user
lolCtrlMap.Set = "set";       -- Default Set Name -- to be overridden by user
lolCtrlMap.PageMode = "List"; -- "List" or "Binary" (applies to all nodes)
lolCtrlMap.RootDirMax = 0;
lolCtrlMap.DataLeafCount = 0;
lolCtrlMap.InnerNodeCount = 0;
lolCtrlMap.KeyType = 0; -- 0 is atomic, 1 is map (with a KEY field).
-- LOL Tree Leaves (Data Pages)
lolCtrlMap.DataPageEntryCountMax = 200;  -- Max # of items
lolCtrlMap.DataPageByteEntrySize = 20;  -- Byte size of a fixed size Byte Entry
lolCtrlMap.DataPageByteCountMax = 2000; -- Max # of BYTES
-- LOL Inner Nodes
lolCtrlMap.InnerNodeEntryCountMax = 200;  -- Max # of items
lolCtrlMap.InnerNodeByteEntrySize = 20;  -- Byte size of a fixed size Byte Entry
lolCtrlMap.InnerNodeByteCountMax = 2000; -- Max # of BYTES

-- ======================================================================
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- || Top Record Root Directory             ||||||||||||||||||||||||||||||
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- Two Modes: "List", and "Binary"
-- List Mode:
--
-- Binary Modes:
--
-- ======================================================================
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- || Interior B+ Tree Nodes  (Records) ||||||||||||||||||||||||||||||||||
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- Records used for Interior B+ Tree modes have three bins:
-- (1) Control Bin  ('NodeCtrlBin')
-- (2) Key List Bin ('KeyListBin')
-- (3) Key Binary Bin ('KeyBinaryBin')
-- (4) Digest List Bin ('DigestListBin')
-- (5) Digest Binary Bin ('DigestBnryBin')
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
-- The Control map Structure:
-- lolInteriorCtrlMap.ItemCount
-- Two Data Modes: "List" and "Binary"
-- List Mode:
--
-- Binary Mode:
--
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
--
-- ======================================================================
-- Aerospike Calls:
-- newRec = aerospike:crec_create( topRec )
-- newRec = aerospike:crec_open( record, digest)
-- status = aerospike:crec_update( record, newRec )
-- status = aerospike:crec_close( record, newRec )
-- digest = record.digest( newRec )
-- ======================================================================


                                  +-------+
                                  | Root  |
                                  | Node  |     Root Node (Order 4)
  Inner Nodes (Order 4)           ++-+-+-++
                                   | | | | 
         +------------------------ + | | +--------------------------+
         |                  +--------+ +---------+                  |
         |                  |                    |                  |
         V                  V                    V                  V
     +---+---+          +---+---+            +---+---+          +---+---+
     | Inner |          | Inner |            | Inner |          | Inner |  
     | Node  |          | Node  |            | Node  |          | Node  |  
     +++---+++          +++---+++            +++---+++          +++---+++
      ||   ||            ||   ||              ||   ||            ||   ||
  +---+|   |+---+    +---+|   |+---+      +---+|   |+---+    +---+|   |+---+
  |    |   |    |    |    |   |    |      |    |   |    |    |    |   |    |
  V    V   V    V    V    V   V    V      V    V   V    V    V    V   V    V
+-++ +-++ ++-+ ++-++-++ +-++ ++-+ ++-+  +-++ +-++ ++-+ ++-++-++ +-++ ++-+ ++-+
|D | |D | |D | |D ||D | |D | |D | |D |  |D | |D | |D | |D ||D | |D | |D | |D |
|A | |A | |A | |A ||A | |A | |A | |A |  |A | |A | |A | |A ||A | |A | |A | |A |
|T | |T | |T | |T ||T | |T | |T | |T |  |T | |T | |T | |T ||T | |T | |T | |T |
|A | |A | |A | |A ||A | |A | |A | |A |  |A | |A | |A | |A ||A | |A | |A | |A |
|  | |  | |  | |  ||  | |  | |  | |  |  |  | |  | |  | |  ||  | |  | |  | |  |
|PG| |PG| |PG| |PG||PG| |PG| |PG| |PG|  |PG| |PG| |PG| |PG||PG| |PG| |PG| |PG|
+--+ +--+ +--+ +--++--+ +--+ +--+ +--+  +--+ +--+ +--+ +--++--+ +--+ +--+ +--+


B-Trees
B-Trees are a variation on binary search trees that allow quick searching in files on disk. Instead of storing one key and having two children, B-tree nodes have n keys and n+1 children, where n can be large. This shortens the tree (in terms of height) and requires much less disk access than a binary search tree would. The algorithms are a bit more complicate, requiring more computation than a binary search tree, but this extra complication is worth it because computation is much cheaper than disk access.
Disk Access
Secondary storage usually refers to the fixed disks found in modern computers. These devices contain several platters of magnetically sensitive material rotating rapidly. Data is stored as changes in the magnetic properties on different portions of the platters. Data is separated into tracks, concentric circles on the platters. Each track is further divided into sectors which form the unit of a transaction between the disk and the CPU. A typical sector size is 512 bytes. The data is read and written by arms that go over the platters, accessing different sectors as they are requested. The disk is spinning at a constant rate (7200 RPM is typical for 1998 mid-range systems).

The time it takes to access data on secondary storage is a function of three variables:

    The time it takes for the arm to move to the track where the requested sector lies. Usually around 10 milliseconds.
    The time it takes for the right sector to spin under the arm. For a 7200 RPM drive, this is 4.1 milliseconds.
    The time it takes to read or write the data. Depending on the density of the data, this time is negligible compared to the other two. 

So an arbitrary 512-byte sector can be accessed (read or written) in roughly 15 milliseconds. Subsequent reads to an adjacent area of the disk will be much faster, since the head is already in exactly the right place. Data can be arranged into "blocks" that are these adjacent multi-sector aggregates.

Contrast this to access times to RAM. From the last lecture, a typical non-sequential RAM access took about 5 microseconds. This is 3000 times faster; we can do at least 3000 memory accesses in the time it takes to do one disk access, and probably more since the algorithm doing the memory accesses is typically following the principal of locality.

So, we had better make each disk access count as much as possible. This is what B-trees do.

For the purposes of discussion, records we might want to search through (bank records, student records, etc.) are stored on disk along with their keys (account number, social security number, etc.), and many are all stored on the same disk "block." The size of a block and the amount of data can be tuned with experimentation or analysis beyond the scope of this lecture. In practice, sometimes only "pointers" to other disk blocks are stored in internal nodes of a B-tree, with leaf nodes containing the real data; this allows storing many more keys and/or having smaller (and thus faster) blocks.
B-Tree Definition
Here is a sample B-tree:

                                     _________
                                    |_30_|_60_|
                                   _/    |    \_
                                 _/      |      \_
                               _/        |        \_
                             _/          |          \_
                           _/            |            \_
                         _/              |              \_
                       _/                |                \_
            ________ _/          ________|              ____\_________
           |_5_|_20_|           |_40_|_50_|            |_70_|_80_|_90_|
          /    |     \          /    |    \           /     |    |     \
         /     |      \        /     |     \         /      |    |      \
        /      |       |      |      |      |       |       |    |       \
       /       |       |      |      |      |       |       |    |        \
|1|3| |6|7|8| |12|16|  |32|39||42|48||51|55|  |61|64| |71|75||83|86| |91|95|99|

B-tree nodes have a variable number of keys and children, subject to some constraints. In many respects, they work just like binary search trees, but are considerably "fatter." The following definition is from the book, with some references to the above example:

A B-tree is a tree with root root[T] with the following properties:

    Every node has the following fields:
        n[x], the number of keys currently in node x. For example, n[|40|50|] in the above example B-tree is 2. n[|70|80|90|] is 3.
        The n[x] keys themselves, stored in nondecreasing order: key1[x] <= key2[x] <= ... <= keyn[x][x] For example, the keys in |70|80|90| are ordered.
        leaf[x], a boolean value that is True if x is a leaf and False if x is an internal node. 
    If x is an internal node, it contains n[x]+1 pointers c1, c2, ... , cn[x], cn[x]+1 to its children. For example, in the above B-tree, the root node has two keys, thus three children. Leaf nodes have no children so their ci fields are undefined.
    The keys keyi[x] separate the ranges of keys stored in each subtree: if ki is any key stored in the subtree with root ci[x], then

        k1 <= key1[x] <= k2 <= key2[x] <= ... <= keyn[x][x] <= kn[x]+1. 

    For example, everything in the far left subtree of the root is numbered less than 30. Everything in the middle subtree is between 30 and 60, while everything in the far right subtree is greater than 60. The same property can be seen at each level for all keys in non-leaf nodes.
    Every leaf has the same depth, which is the tree's height h. In the above example, h=2.
    There are lower and upper bounds on the number of keys a node can contain. These bounds can be expressed in terms of a fixed integer t >= 2 called the minimum degree of the B-tree:
        Every node other than the root must have at least t-1 keys. Every internal node other than the root thus has at least t children. If the tree is nonempty, the root must have at least one key.
        Every node can contain at most 2t-1 keys. Therefore, an internal node can have at most 2t children. We say that a node is full if it contains exactly 2t-1 keys. 

Some Analysis
Theorem 19.1 in the book states that any n-key B-tree with n > 1 of height h and minimum degree t satisfies the following property:

    h <= logt(n+1)/2 

That of course gives us that the height of a B-tree is always O(log n), but that log hides an impressive performance gain over regular binary search trees (since performance of algorithms will be proportional to the height of the tree in many cases).

Consider a binary search tree arranged on a disk, with pointers being the byte offset in the file where a child occurs. A typical situation will have maybe 50 bytes of information, 4 bytes of key, and 8 bytes (two 32-bit integers) for left and right pointers. That makes 62 bytes that will comfortably fit in a 512-byte sector. In fact, we can put many such nodes in the same sector; however, when our n (= number of nodes) grows large, it is unlikely that the same two nodes will be accessed sequentially, so access to each node will cost roughly one disk access. In the best possible case, the a binary tree with n nodes is of height about floor(log2n). So searching for an arbitrary node will take about log2n disk accesses. In a file with one million nodes, for instance, the phone book for a medium-sized city, this is about 20 disk accesses. Assuming the 15 millisecond access time. a single access will take 0.3 seconds.

Contrast this with a B-tree with records that fit into one 512-byte sector. Let t=4. Then each node can have up to 8 children, 7 keys. With 50*7 bytes of information, 4*7 bytes of keys, 4*8 bytes of children pointers, and 4 bytes to store n[x], we have 414 bytes of information fitting comfortably into a 512 byte sector. With one million records, we would have to do log41,000,000 = 10 disk accesses, taking 0.15 seconds, reducing by a half the time it takes. If we choose to keep all the information in the leaves as suggested above and only keep pointer and key information, we can fit up to 64 keys and let t=32. Now the number of disk accesses in our example is less than or equal to log32 1,000,000 = 4. In practice, up to a few thousand keys can be supported with blocks spanning many sectors; such blocks take only a tiny bit longer to access than a single arbitrary access, so performance is still improved.

Of course, asymptotically, the number of accesses is "the same," but for real-world numbers, B-trees are a lot better. The key is the fact that disk access times are much slower than memory and computation time. If we were to implement B-trees using real memory and pointers, there would probably be no performance improvement whatsoever because of the algorithmic overhead; indeed, there might be a performance decrease.
Operations on B-trees
Let's look at the operations on a B-tree. We assume that the root node is always kept in memory; it makes no sense to retrieve it from the disk every time since we will always need it. (In fact, it might be wise to store a "cache" of frequently used and/or low depth nodes in memory to further reduce disk accesses...)

Searching a B-tree Searching a B-tree is much like searching a binary search tree, only the decision whether to go "left" or "right" is replaced by the decision whether to go to child 1, child 2, ..., child n[x]. The following procedure, B-Tree-Search, should be called with the root node as its first parameter. It returns the block where the key k was found along with the index of the key in the block, or "null" if the key was not found:

B-Tree-Search (x, k) // search starting at node x for key k
    i = 1

    // search for the correct child

    while i <= n[x] and k > keyi[x] do
        i++
    end while

    // now i is the least index in the key array such that
    // k <= keyi[x], so k will be found here or
    // in the i'th child

    if i <= n[x] and k = keyi[x] then 
        // we found k at this node
        return (x, i)
    
    if leaf[x] then return null

    // we must read the block before we can work with it

    Disk-Read (ci[x])
    return B-Tree-Search (ci[x], k)

The time in this algorithm is dominated by the time to do disk reads. Clearly, we trace a path from root down possibly to a leaf, doing one disk read each time, so the number of disk reads for B-Tree-Search is O(h) = O(log n) where h is the height of the B-tree and n is the number of keys.

We do a linear search for the correct key. There are (t) keys (at least t-1 and at most 2t-1), and this search is done for each disk access, so the computation time is O(t log n). Of course, this time is very small compared to the time for disk accesses. If we have some spare time one day, in between reading Netscape and playing DOOM, we might consider using a binary search (remember, the keys are nondecreasing) and get this down to O(log t log n). 


B-Trees continued.
More B-tree operations
Creating an empty B-tree

To initialize a B-tree, we need simply to build an empty root node:

B-Tree-Create (T)
    x = allocate-node ();
    leaf[x] = True
    n[x] = 0
    Disk-Write (x)
    root[T] = x

This assumes there is an allocate-node function that returns a node with key, c, leaf fields, etc., and that each node has a unique "address" on the disk.

Clearly, the running time of B-Tree-Create is O(1), dominated by the time it takes to write the node to disk.

Inserting a key into a B-tree

Inserting into a B-tree is a bit more complicated than inserting into an ordinary binary search tree. We have to find a place to put the new key. We would prefer to put it in the root, since that is kept in RAM and so we don't have to do any disk accesses. If that node is not full (i.e., n[x] for that node is not 2t-1), then we can just stick the new key in, shift around some pointers and keys, write the results back to disk, and we're done. Otherwise, we will have to split the root and do something with the resulting pair of nodes, maintaining the properties of the definition of a B-tree.

Here is the general algorithm for insertinging a key k into a B-tree T. It calls two other procedures, B-Tree-Split-Child, that splits a node, and B-Tree-Insert-Nonfull, that handles inserting into a node that isn't full.

B-Tree-Insert (T, k)
    r = root[T]
    if n[r] = 2t - 1 then
        // uh-oh, the root is full, we have to split it
        s = allocate-node ()
        root[T] = s     // new root node
        leaf[s] = False // will have some children
        n[s] = 0    // for now
        c1[s] = r // child is the old root node
        B-Tree-Split-Child (s, 1, r) // r is split
        B-Tree-Insert-Nonfull (s, k) // s is clearly not full
    else
        B-Tree-Insert-Nonfull (r, k)
    endif

Let's look at the non-full case first: this procedure is called by B-Tree-Insert to insert a key into a node that isn't full. In a B-tree with a large minimum degree, this is the common case. Before looking at the pseudocode, let's look at a more English explanation of what's going to happen:

To insert the key k into the node x, there are two cases:

    x is a leaf node. Then we find where k belongs in the array of keys, shift everything over to the left, and stick k in there.
    x is not a leaf node. We can't just stick k in because it doesn't have any children; children are really only created when we split a node, so we don't get an unbalanced tree. We find a child of x where we can (recursively) insert k. We read that child in from disk. If that child is full, we split it and figure out which one k belongs in. Then we recursively insert k into this child (which we know is non-full, because if it were, we would have split it). 

Here's the algorithm:

B-Tree-Insert-Nonfull (x, k)
    i = n[x]

    if leaf[x] then

        // shift everything over to the "right" up to the
        // point where the new key k should go

        while i >= 1 and k < keyi[x] do
            keyi+1[x] = keyi[x]
            i--
        end while

        // stick k in its right place and bump up n[x]

        keyi+1[x] = k
        n[x]++
    else

        // find child where new key belongs:

        while i >= 1 and k < keyi[x] do
            i--
        end while

        // if k is in ci[x], then k <= keyi[x] (from the definition)
        // we'll go back to the last key (least i) where we found this
        // to be true, then read in that child node

        i++
        Disk-Read (ci[x])
        if n[ci[x]] = 2t - 1 then

            // uh-oh, this child node is full, we'll have to split it

            B-Tree-Split-Child (x, i, ci[x])

            // now ci[x] and ci+1[x] are the new children, 
            // and keyi[x] may have been changed. 
            // we'll see if k belongs in the first or the second

            if k > keyi[x] then i++
        end if

        // call ourself recursively to do the insertion

        B-Tree-Insert-Nonfull (ci[x], k)
    end if

Now let's see how to split a node. When we split a node, we always do it with respect to its parent; two new nodes appear and the parent has one more child than it did before. Again, let's see some English before we have to look at the pseudocode:

We will split a node y that is the ith child of its parent x. Node x will end up having one more child we'll call z, and we'll make room for it in the ci[x] array right next to y.

We know y is full, so it has 2t-1 keys. We'll "cut" y in half, copying keyt+1[y] through key2t-1[y] into the first t-1 keys of this new node z.

If the node isn't a leaf, we'll also have to copy over the child pointers ct+1[y] through c2t[y] (one more child than keys) into the first t children of z.

Then we have to shift the keys and children of x over one starting at index i+1 to accomodate the new node z, and then update the n[] counts on x, y and z, finally writing them to disk.

Here's the pseudocode:

B-Tree-Split-Child (x, i, y)
    z = allocate-node ()

    // new node is a leaf if old node was 

    leaf[z] = leaf[y]

    // we since y is full, the new node must have t-1 keys

    n[z] = t - 1

    // copy over the "right half" of y into z

    for j in 1..t-1 do
        keyj[z] = keyj+t[y]
    end for

    // copy over the child pointers if y isn't a leaf

    if not leaf[y] then
        for j in 1..t do
            cj[z] = cj+t[y]
        end for
    end if

    // having "chopped off" the right half of y, it now has t-1 keys

    n[y] = t - 1

    // shift everything in x over from i+1, then stick the new child in x;
    // y will half its former self as ci[x] and z will 
    // be the other half as ci+1[x]

    for j in n[x]+1 downto i+1 do
        cj+1[x] = cj[x]
    end for
    ci+1 = z

    // the keys have to be shifted over as well...

    for j in n[x] downto i do
        keyj+1[x] = keyj[x]
    end for

    // ...to accomodate the new key we're bringing in from the middle 
    // of y (if you're wondering, since (t-1) + (t-1) = 2t-2, where 
    // the other key went, its coming into x)
    
    keyi[x] = keyt[y]
    n[x]++

    // write everything out to disk

    Disk-Write (y)
    Disk-Write (z)
    Disk-Write (x)

Note that this is the only time we ever create a child. Doing a split doesn't increase the height of a tree, because we only add a sibling to existing keys at the same level. Thus, the only time the height of the tree ever increases is when we split the root. So we satisfy the part of the definition that says "each leaf must occur at the same depth."

Example of Insertion

Let's look at an example of inserting into a B-tree. For preservation of sanity, let t = 2. So a node is full if it has 2(2)-1 = 3 keys in it, and each node can have up to 4 children. We'll insert the sequence 5 9 3 7 1 2 8 6 0 4 into the tree:

Step 1: Insert 5
                                  ___
                                 |_5_|

Step 2: Insert 9
B-Tree-Insert simply calls B-Tree-Insert-Nonfull, putting 9 to the
right of 5:
                                 _______
                                |_5_|_9_|

Step 3: Insert 3
Again, B-Tree-Insert-Nonfull is called
                               ___ _______
                              |_3_|_5_|_9_|

Step 4: Insert 7
Tree is full.  We allocate a new (empty) node, make it the root, split
the former root, then pull 5 into the new root:
                                 ___
                                |_5_|
                             __ /   \__
                            |_3_|  |_9_|

Then insert we insert 7; it goes in with 9
                                 ___
                                |_5_|
                             __ /   \______
                            |_3_|  |_7_|_9_|

Step 5: Insert 1
It goes in with 3
                                 ___
                                |_5_|
                         ___ __ /   \______
                        |_1_|_3_|  |_7_|_9_|

Step 6: Insert 2
It goes in with 3
                                 ___
                                |_5_|
                               /     \
                       ___ __ /___    \______
                      |_1_|_2_|_3_|  |_7_|_9_|

Step 7: Insert 8
It goes in with 9
 
                                 ___
                                |_5_|
                               /     \
                       ___ __ /___    \__________
                      |_1_|_2_|_3_|  |_7_|_8_|_9_|

Step 8: Insert 6
It would go in with |7|8|9|, but that node is full.  So we split it,
bringing its middle child into the root:

                                _______
                               |_5_|_8_|
                              /    |   \
                     ___ ____/__  _|_   \__
                    |_1_|_2_|_3_||_7_| |_9_|

Then insert 6, which goes in with 7:
                                _______
                            ___|_5_|_8_|__
                           /       |      \
                  ___ ____/__    __|____   \__
                 |_1_|_2_|_3_|  |_6_|_7_|  |_9_|

Step 9: Insert 0

0 would go in with |1|2|3|, which is full, so we split it, sending the middle
child up to the root:
                             ___________
                            |_2_|_5_|_8_|
                          _/    |   |    \_
                        _/      |   |      \_
                      _/_     __|   |______  \___
                     |_1_|   |_3_| |_6_|_7_| |_9_| 

Now we can put 0 in with 1
                             ___________
                            |_2_|_5_|_8_|
                          _/    |   |    \_
                        _/      |   |      \_
                  ___ _/_     __|   |______  \___
                 |_0_|_1_|   |_3_| |_6_|_7_| |_9_| 


Step 10: Insert 4
It would be nice to just stick 4 in with 3, but the B-Tree algorithm
requires us to split the full root.  Note that, if we don't do this and
one of the leaves becomes full, there would be nowhere to put the middle
key of that split since the root would be full, thus, this split of the
root is necessary:
                                 ___
                                |_5_|
                            ___/     \___
                           |_2_|     |_8_|
                         _/    |     |    \_
                       _/      |     |      \_
                 ___ _/_     __|     |______  \___
                |_0_|_1_|   |_3_|   |_6_|_7_| |_9_| 

Now we can insert 4, assured that future insertions will work:

                                 ___
                                |_5_|
                            ___/     \___
                           |_2_|     |_8_|
                         _/    |     |    \_
                       _/      |     |      \_
                 ___ _/_    ___|___  |_______ \____
                |_0_|_1_|  |_3_|_4_| |_6_|_7_| |_9_| 

                          


-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --


