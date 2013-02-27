-- Large Stack Object Operations
-- Strawman V2 -- (Feb 15, 2013)
-- (*) stackCreate
-- (*) stackPush
-- (*) stackPeek

-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- LSO Utility Functions
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- Get (create) a unique bin name given the current counter
-- ======================================================================
local function getBinName( count )
  return "DirBin_" .. tostring( count );
end

-- ======================================================================
-- Sample Filter function to test user entry 
-- ======================================================================
local function transformFilter1( entryList )
  local mod = "LuaStackOps_V2";
  local meth = "transformFilter1()";
  local resultList = list();
  local entry = 0;
  info("[ENTER]: <%s:%s> EntryList(%s) \n", mod, meth, tostring(entryList));

  -- change EVERY entry that is > 200 to 0.
  for i = 1, list.size( entryList ) do
      info("[DEBUG]: <%s:%s> EntryList[%d](%s) \n", mod, meth, i, tostring(entryList[i]));
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
-- createNewChunk: Create a new "chunk" (the thing that holds a list
-- of user values), add it to the Dir Map and Increment the control
-- map's ChunkCount. We are blindly allocating a new chunk and bumping
-- the chuck count -- assuming (eek) that the caller knows the correct
-- state of the system.
-- Parms:
-- (*) lastBinNum: The last bin Num:
-- Return: New Bin Number
-- ======================================================================
local function createNewChunk( ctrlMap, dirMap )
  local mod = "LuaStackOps_V2";
  local meth = "createNewChunk()";
  info("[ENTER]: <%s:%s>CTRL(%s) DIR(%s)\n",
    mod, meth, tostring(ctrlMap), tostring(dirMap));

  local chunkCount = ctrlMap['ChunkCount'];
  chunkCount = chunkCount + 1;
  ctrlMap['ChunkCount'] = chunkCount;
  local binNum = chunkCount;
  local binName = getBinName( binNum );
  dirMap[binName] = list(); -- Create a new list in the new bin

  info("[EXIT]: <%s:%s> BinNum(%d) BinName(%s) ctrlMap(%s) dirMap(%s)\n",
    mod, meth, binNum, binName, tostring(ctrlMap), tostring(dirMap));

  return binNum;
end

-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- LSO Main Functions
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
--
-- ======================================================================
-- || stackCreate (Strawman V2) ||
-- ======================================================================
-- Create/Initialize a Stack structure in a record, using multiple bins
--
-- We will use predetermined BIN names for this initial prototype
-- 'LSO_CTRL_BIN' will be the name of the bin containing the control info
-- 'LSO_DIR_BIN' will be the name of the bin containing the BIN Dir List
-- 'BIN_XX' will be the individual bins that hold lists of data
-- +========================================================================+
-- | Usr Bin 1 | Usr Bin 2 | o o o | Usr Bin N | LSO CTRL BIN | LSO_DIR_BIN |
-- +========================================================================+
-- LSO Bin contains a MAP of Control Info
-- LSO Dir contains a MAP of Bins, each bin holding a List of data
function stackCreate( record, namespace, set )
  local mod = "LuaStackOps_V2";
  local meth = "stackCreate()";
  info("[ENTER]: <%s:%s> NS(%s) Set(%s)\n", mod, meth, namespace, set );

  -- Check to see if LSO Structure (or anything) is already there,
  -- and if so, error
  if( record.LSO_CTRL_BIN ~= nil ) then
    info("[ERROR EXIT]: <%s:%s> LSO_CRTL_BIN Already Exists\n", mod, meth );
    return('LSO_CTRL_BIN already exists');
  end

  if( record.LSO_DIR_BIN ~= nil ) then
    info("[ERROR EXIT]: <%s:%s> LSO_DIR_BIN Already Exists\n", mod, meth );
    return('LSO_DIR_BIN already exists');
  end

  info("[DEBUG]: <%s:%s> : Initialize CTRL Map\n", mod, meth );

  -- Define our control information and put it in the record's control bin
  -- Notice that in the next version, Top of Stack (TOS) will not be at the
  -- end, but will instead move and will have a TOS ptr var in the ctrl map.
  local ctrlMap = map();
  ctrlMap['NameSpace'] = namespace;
  ctrlMap['Set'] = set;
  ctrlMap['ChunkSize'] = 5;
  ctrlMap['ChunkCount'] = 0;
  ctrlMap['ItemCount'] = 0;

  info("[DEBUG]: <%s:%s> : CTRL Map after Init(%s)\n",
    mod, meth , tostring(ctrlMap));

  -- Build our Dir Map and Create our first chunk
  local dirMap = map();
  local binNum = createNewChunk( ctrlMap, dirMap );

  -- Put our new maps in the record, then store the record.
  record.LSO_DIR_BIN = dirMap;
  record.LSO_CTRL_BIN = ctrlMap;

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
-- || stackPush (Strawman V2)
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- Push a value onto the stack.
-- Find the "Current Bin" (Top), and append to that bin's list.
-- Currently, each List (in a bin) has an IMPLICIT entry count, which is
-- just the list size, so we get that automatically.
-- In this version -- it's an ENTRY COUNT, but eventually, it will have
-- to be an overall SIZE (in bytes).
-- Also -- we will keep those counts in our own control structures
-- (Separate Records, with control structures in each one).
--
-- We will use predetermined BIN names for this initial prototype
-- 'LSO_CTRL_BIN' will be the name of the bin containing the control info
-- 'LSO_DIR_BIN' will be the name of the bin containing the BIN Dir List
-- 'BIN_XX' will be the individual bins that hold lists of data
-- Notice that this means that THERE CAN BE ONLY ONE LSO object per record.
-- In the final version, this will change -- there will be multiple 
-- LSO's per record because they will occupy only a single bin, and that
-- bin will be of type LSO.
-- +========================================================================+=~
-- | Usr Bin 1 | Usr Bin 2 | o o o | Usr Bin N | LSO CTRL BIN | LSO_DIR_BIN | ~
-- +========================================================================+=~
--    ~=+===========================================+
--    ~ | Dir Bin 1 | Dir Bin 2 | o o o | Dir Bin N |
--    ~=+===========================================+
-- LSO Bin will contain a MAP of Control Info
-- LSO Dir will contain a MAP of Bins, each bin holding a List of data
local function localStackPush( record, newValue )
  local mod = "LuaStackOps_V2";
  local meth = "localStackPush()";
  info("[ENTER]: <%s:%s>  NewValue(%s)\n", mod, meth, tostring( newValue ));

  -- Verify that the LSO Structure is there: otherwise, error.
  if( record.LSO_CTRL_BIN == nil ) then
    info("[ERROR EXIT]: <%s:%s> LSO_CTRL_BIN DOES NOT Exists\n", mod, meth );
    return('LSO_CTRL_BIN Does NOT exist');
  end
  if( record.LSO_DIR_BIN == nil ) then
    info("[ERROR EXIT]: <%s:%s> LSO_DIR_BIN DOES NOT Exists\n", mod, meth );
    return('LSO_DIR_BIN Does NOT exist');
  end

  -- The largest bin number (i.e. ChunkCount) marks the insert position,
  -- provided that there's room. If there's not room, create a new Chunk
  -- and do the insert.
  local ctrlMap = record.LSO_CTRL_BIN; -- Control Info
  local binNum = ctrlMap['ChunkCount'];
  local binName = getBinName( binNum );
  local dirMap = record.LSO_DIR_BIN;  -- Directory Info
  local dirList = dirMap[binName];

  info("[DEBUG]: <%s:%s>:Pulled from the Record: CTRL Map(%s) DIR Map(%s)\n",
    mod, meth, tostring(ctrlMap), tostring(dirMap));
  info("[DEBUG]: <%s:%s>: Bin(%d)(%s) gets LIST(%s) \n",
    mod, meth, binNum, binName, tostring(dirList) );

  -- Look to see if we can add in the current (top) chunk, or if we have to
  -- create a new chunk to hold the new value.
  local maxChunkSize = ctrlMap['ChunkSize'];
  if( list.size(dirList) >= maxChunkSize ) then
    info("[DEBUG]: <%s:%s>: Special New Chunk Insert\n", mod,meth );
    -- Create a new chunk, plug it in and increment the chunk count
    binNum = createNewChunk( ctrlMap, dirMap );
    binName = getBinName( binNum );
    dirList = dirMap[binName]; -- get the new (empty) list

    info("[DEBUG]: <%s:%s> Created New Bin(%d)\n", mod, meth, binNum);
    info("[DEBUG]: <%s:%s> Validate Create: ctrlMap(%s) dirMap(%s) \n",
      mod, meth, tostring(ctrlMap), tostring(dirMap) );

    list.append( dirList, newValue );

    info("[DEBUG]: <%s:%s>: Appended NewValue(%s) to DirList(%s)\n",
      mod, meth, tostring( newValue ), tostring( dirList ));

    dirMap[binName] = dirList;
    record.LSO_DIR_BIN = dirMap; -- Store the udpated Dir List
  else
    info("[DEBUG]: <%s:%s>: Regular Insert\n", mod,meth );

    -- Insert the new value at TOS and bump the item count.
    list.append( dirList, newValue );

    info("[DEBUG]: <%s:%s>: Appended NewValue(%s) to DirList(%s)\n",
      mod, meth, tostring( newValue ), tostring( dirList ));

    dirMap[binName] = dirList;
    record.LSO_DIR_BIN = dirMap; -- Store the udpated Dir List
  end

  info("[DEBUG]: <%s:%s>: Bin(%d)(%s) has LIST(%s) Map(%s)\n",
    mod, meth, binNum, binName, tostring(dirList), tostring(dirMap));

  -- Note that we will (likely) NOT track the exact item count in the
  -- record in the FINAL VERSION, as that would trigger a new record
  -- update for EACH Value insert, in addition to the record update for
  -- the record actually holding the new value.  We want to keep this to
  -- just one record insert rather than two.
  local itemCount = ctrlMap['ItemCount'];
  itemCount = itemCount + 1;
  ctrlMap['ItemCount'] = itemCount;
  record.LSO_CTRL_BIN = ctrlMap;

  info("[ENTER]: <%s:%s>Storing Record(%s) New Value(%s)\n",
    mod, meth, tostring(record), tostring( newValue ));

  -- All done, store the record
  local rc = -99; -- Use Odd starting Num: so that we know it got changed
  if( not aerospike:exists( record ) ) then
    info("[DEBUG]:<%s:%s>:Create Record\n", mod, meth );
    rc = aerospike:create( record );
  else
    info("[DEBUG]:<%s:%s>:Update Record\n", mod, meth );
    rc = aerospike:update( record );
  end

  info("[EXIT]: <%s:%s> : Done.  RC(%d)\n", mod, meth, rc );
  return rc
end -- function stackPush( record, newValue )

function stackPush( record, newValue )
  local mod = "LuaStackOps_V2";
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
  local mod = "LuaStackOps_V2";
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
  local ctrlMap = record.LSO_CTRL_BIN;
  local binNum = ctrlMap['ChunkCount']; -- Start with the end (TOS)
  local binName;
  local dirMap = record.LSO_DIR_BIN;
  info("[DEBUG]: <%s:%s> BinNum(%d) Validating Record:CTRL(%s) DIR(%s)\n",
    mod, meth, binNum, tostring(ctrlMap), tostring(dirMap));

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
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- || stackTrim (Straw V2)
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- Trim all but the top N values from the stack
-- We're going to do this "dumb" style -- and just remove everything
-- that is AFTER "trim count".
-- ======================================================================
function stackTrim( record, trimCount )
  local mod = "LuaStackOps_V2";
  local meth = "stackTrim()";
  info("[ENTER]: <%s:%s> TrimCount(%d) \n", mod, meth, trimCount );

  local result = 0;

  -- Verify that the LSO Structure is there: otherwise, error.
  if( record.LSO_CTRL_BIN == nil ) then
    return('LSO_CTRL_BIN Does NOT exist');
  end
  if( record.LSO_DIR_BIN == nil ) then
    return('LSO_DIR_BIN Does NOT exist');
  end

  -- Compute where we need to start deleting.  For each WHOLE CHUNK,
  -- we will just delete the chunk (null out the entry).  Leave a partial
  -- chunk at the end.
  --  <<< Code to be completed >>>

  info("[EXIT]: <%s:%s>: TrimCount(%d) Result(%d)\n", 
    mod, meth, trimCount, result );

  return result;

end -- function stackTrim()


-- ======================================================================
-- Generate Entry
-- ======================================================================
local function generateEntry( seed )
  local mod = "LuaStackOps_V2";
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
  local mod = "LuaStackOps_V2";
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
