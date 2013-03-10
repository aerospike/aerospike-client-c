-- ======================================================================
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- || UDF FUNCTION TABLE ||
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- Last Update: (Feb 28, 2013) tjl
--
-- Table of Functions: Used for Transformation and Filter Functions in
-- conjunction with Large Stack Objects (LSO) and Large Sets (LSET).
--
-- There is a new family of Aerospike Types and Functions that are
-- implemented with UDFs: Large Stack Objects (LSO) and Large Sets (LSET).
-- Some of these new functions take a UDF as a parameter, which is then
-- executed on the server side.  We pass those "inner" UDFs by name, and
-- and those names reference a function that is stored in a table. This
-- module defines those "inner" UDFs.
--
-- This table (currently) defines
-- (*) LSO Transform functions: Used for peek() and push()
-- (*) LSO Filter functions: Used for peek()
-- (*) LSET Transform functions: Used for insert() and select()
-- (*) LSET Filter functions: Used for select()
-- 
-- In order to pass functions as parameters in Lua (from C), we don't have
-- the ability to officially pass a true Lua function as a parameter to
-- the top level Lua function, so we instead pass the "inner" Lua function
-- by name, as a simple string.  That string corresponds to the names of
-- functions that are stored in this file, and the parameters to be fed
-- to the inner UDFs are passed in a list (arglist) to the outer UDF.
--
-- NOTE: These functions are not meant to be written by regular users.
-- It is the job of knowledgeable DB Administrators to write, review and
-- install both the top level UDFs and these "inner" UDFs on the Aerospike
-- server.  As a result, there are few protections against misuse or
-- just plain bad coding.  So -- Users and Administrators Beware!!
--
--
-- ======================================================================
-- || GLOBAL PRINT ||
-- ======================================================================
-- Use this flag to enable/disable global printing (the "detail" level
-- in the server).
-- Usage: GP=F and trace()
-- When "F" is true, the trace() call is executed.  When it is false,
-- the trace() call is NOT executed (regardless of the value of GP)
-- ======================================================================
local GP=true; -- Doesn't matter what this is set to.
local F=false; -- Set F (flag) to true to turn ON global print

--
-- ======================================================================
-- Usage:
--
-- From the main function table "functionTable", we can call any of the
-- functions defined here by passing its name and the associated arglist
-- that is supplied by the user.  For example, in stackPeekWithUDF, we
-- 
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
local UdfFunctionTable = {}

-- ======================================================================
-- Sample Filter function to test user entry 
-- Parms (encased in arglist)
-- (1) Entry List
-- ======================================================================
function UdfFunctionTable.transformFilter1( argList )
  local mod = "UdfFunctionTable";
  local meth = "transformFilter1()";
  local resultList = list();
  local entryList = arglist[1]; 
  local entry = 0;
  GP=F and info("[ENTER]: <%s:%s> EntryList(%s) \n",
                 mod, meth, tostring(entryList));

  -- change EVERY entry that is > 200 to 0.
  for i = 1, list.size( entryList ) do
      GP=F and info("[DEBUG]: <%s:%s> EntryList[%d](%s) \n",
        mod, meth, i, tostring(entryList[i]));
    if entryList[i] > 200 then 
      GP=F and info("[DEBUG]: <%s:%s> Setting Entry to ZERO \n", mod, meth );
      entry = 0;
    else 
      GP=F and info("[DEBUG]: <%s:%s> Setting Entry to entryList(%s) \n",
        mod, meth, tostring(entryList[i]));
      entry = entryList[i];
    end
    list.append( resultList, entry );
    GP=F and info("[DEBUG]: <%s:%s> List Append: Result:(%s) Entry(%s)\n",
                  mod, meth, tostring(resultList[i]), tostring( entry));
  end

  GP=F and info("[EXIT]: <%s:%s> Return with ResultList(%s) \n",
                mod, meth, tostring(resultList));
  return resultList;
end
-- ======================================================================

-- ======================================================================
-- Function Range Filter: Performs a range query on one or more of the
-- entries in the list.
-- Parms (encased in arglist)
-- (1) Entry List
-- (2) Relation List {{op, val}, {op, val} ... }
-- ======================================================================
function UdfFunctionTable.rangeFilter( arglist )
  local mod = "UdfFunctionTable";
  local meth = "rangeFilter()";
  local rc = 0;
  GP=F and info("[ENTER]: <%s:%s> ArgList(%s) \n",
                mod, meth, tostring(arglist));

  GP=F and info("[DEBUG]: <%s:%s> >>>>>>>> HELLO!!! <<<<<<< \n", mod, meth );

  GP=F and info("[EXIT]: <%s:%s> Result(%d) \n", mod, meth, rc );

  return rc;
end
-- ======================================================================

-- ======================================================================
-- Function compressTransform1: Compress the multi-part list into a single
-- as_bytes value.  There are various tables stored in this module that
-- describe various structures, and the table index picks the compression
-- structure that we'll use.
-- Parms (encased in arglist)
-- (1) Entry List
-- (2) Compression Field Parameters Table Index
-- ======================================================================
function UdfFunctionTable.compressTransform1( arglist )
  local mod = "UdfFunctionTable";
  local meth = "compress()";
  local rc = 0;
  GP=F and info("[ENTER]: <%s:%s> ArgList(%s) \n",
                mod, meth, tostring(arglist));

  GP=F and info("[DEBUG]: <%s:%s> >>>>>>>> HELLO!!! <<<<<<< \n", mod, meth );

  GP=F and info("[EXIT]: <%s:%s> Result(%d) \n", mod, meth, rc );
  return rc;
end
-- ======================================================================

-- ======================================================================
-- Function unCompressTransform1: uncompress the single byte string into
-- multiple map fields -- as defined by the compression field table.
-- Parms (encased in arglist)
-- (1) Entry List
-- (2) Compression Field Parameters Table Index
-- ======================================================================
function UdfFunctionTable.unCompressTransform1( arglist )
  local mod = "UdfFunctionTable";
  local meth = "unCompress()";
  local rc = 0;
  GP=F and info("[ENTER]: <%s:%s> ArgList(%s) \n",
                mod, meth, tostring(arglist));

  GP=F and info("[DEBUG]: <%s:%s> >>>>>>>> HELLO!!! <<<<<<< \n", mod, meth );

  GP=F and info("[EXIT]: <%s:%s> Result(%d) \n", mod, meth, rc );
  return rc;
end
-- ======================================================================

-- ======================================================================
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ||                     Add New Functions Here.                      ||
-- ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================

-- ======================================================================
-- Function testFilter1: 
-- ======================================================================
-- Test:  Print arguments and HELLO.
-- Parms (encased in arglist)
-- (1) Entry List
-- (2) Compression Field Parameters Table Index
-- ======================================================================
function UdfFunctionTable.testFilter1( arglist )
  local mod = "UdfFunctionTable";
  local meth = "testFilter1()";
  GP=F and info("[ENTER]: <%s:%s> ArgList(%s) \n",
                mod, meth, tostring(arglist));

  local result = "Test Filter1 Hello";
  GP=F and info("[DEBUG]: <%s:%s> Msg (%s) ArgList(%s) \n",
                mod, meth, result, tostring(arglist));

  GP=F and info("[EXIT]: <%s:%s> Result(%s) \n", mod, meth, result );

  return result
end
-- ======================================================================


-- ======================================================================
-- Function stumbleCompress4: Compress a 4 part tuple into a single 18 byte
-- value that we'll pack into storage.
-- The StumbleUpon application creates a 4 part tuple, each part with
-- the following sizes: 4 bytes, 4 bytes, 8 bytes and 2 bytes.
-- (1) stumbleTuple
-- (2) arglist
-- Return:
-- The newly created Byte object, 18 bytes long
-- ======================================================================
function UdfFunctionTable.stumbleCompress4( stumbleTuple, arglist )
  local mod = "UdfFunctionTable";
  local meth = "stumbleCompress()";
  local rc = 0;
  GP=F and info("[ENTER]: <%s:%s> tuple(%s) ArgList(%s) \n",
                mod, meth, tostring(stumbleTuple), tostring(arglist));

  local b18 = bytes(18);
  bytes.put_int32(b18, 1,  stumbleTuple[1] ); -- 4 byte int
  bytes.put_int32(b18, 5,  stumbleTuple[2] ); -- 4 byte int
  bytes.put_int64(b18, 9,  stumbleTuple[3] ); -- 8 byte int
  bytes.put_int16(b18, 17, stumbleTuple[4] ); -- 2 byte int

  GP=F and info("[EXIT]: <%s:%s> Result(%s) \n", mod, meth, tostring(b18));
  return b18
end -- stumbleCompress4( stumbleTuple, arglist )

-- ======================================================================
-- Function stumbleUnCompress4: Uncompress a single 18 byte packed binary
-- object into 4 integer fields.
-- The StumbleUpon application uses a 4 part tuple, each part with
-- the following sizes: 4 bytes, 4 bytes, 8 bytes and 2 bytes.
-- (1) b18: the byteObject
-- (2) arglist
-- Return:
-- the stumbleTuple
-- ======================================================================
function UdfFunctionTable.stumbleUnCompress4( b18, arglist )
  local mod = "UdfFunctionTable";
  local meth = "stumbleUnCompress()";
  local rc = 0;
  -- protect against bad prints
  if arglist == nil then arglist = 0; end
  GP=F and info("[ENTER]: <%s:%s> tuple(%s) Tuple Type(%s) ArgList(%s) \n",
                mod, meth, tostring(b18), type(b18), tostring(arglist));

  local stumbleTuple = list();
  stumbleTuple[1] = bytes.get_int32(b18, 1 ); -- 4 byte int
  stumbleTuple[2] = bytes.get_int32(b18, 5 ); -- 4 byte int
  stumbleTuple[3] = bytes.get_int64(b18, 9 ); -- 8 byte int
  stumbleTuple[4] = bytes.get_int16(b18, 17); -- 2 byte int

  GP=F and info("[EXIT]: <%s:%s> Result(%s) type(%s)\n",
                mod, meth, tostring(stumbleTuple), type(stumbleTuple ));
  return stumbleTuple;
end
-- ======================================================================

-- ======================================================================
-- Function stumbleCompress5: Compress a 5 part tuple into a single 18 byte
-- value that we'll pack into storage.
-- The StumbleUpon application creates a 5 part tuple, each part with
-- the following sizes: 4 bytes, 4 bytes, 4 bytes, 4 bytes and 2 bytes.
-- (1) stumbleTuple
-- (2) arglist
-- Return:
-- The newly created Byte object, 18 bytes long
-- ====================================================================== 
function UdfFunctionTable.stumbleCompress5( stumbleTuple, arglist )
  local mod = "UdfFunctionTable";
  local meth = "stumbleCompress5()";
  local rc = 0;
  GP=F and info("[ENTER]: <%s:%s> tuple(%s) ArgList(%s) ",
                mod, meth, tostring(stumbleTuple), tostring(arglist));

  local b18 = bytes(18);
  bytes.put_int32(b18, 1,  stumbleTuple[1] ); -- 4 byte int
  bytes.put_int32(b18, 5,  stumbleTuple[2] ); -- 4 byte int
  bytes.put_int32(b18, 9,  stumbleTuple[3] ); -- 4 byte int
  bytes.put_int32(b18, 13, stumbleTuple[4] ); -- 4 byte int
  bytes.put_int16(b18, 17, stumbleTuple[5] ); -- 2 byte int

  GP=F and info("[EXIT]: <%s:%s> BinaryResult(%s)", mod, meth, tostring(b18));
  return b18
end -- stumbleCompress5( stumbleTuple, arglist )

-- ======================================================================
-- Function stumbleUnCompress5: Uncompress a single 18 byte packed binary
-- object into 5 integer fields.
-- The StumbleUpon application uses a 4 part tuple, each part with
-- the following sizes: 4 bytes, 4 bytes, 4 bytes, 4 bytes and 2 bytes.
-- (1) b18: the byteObject
-- (2) arglist
-- Return:
-- the stumbleTuple
-- ======================================================================
function UdfFunctionTable.stumbleUnCompress5( b18, arglist )
  local mod = "UdfFunctionTable";
  local meth = "stumbleUnCompress5()";
  local rc = 0;
  -- protect against bad prints
  if arglist == nil then arglist = 0; end
  GP=F and info("[ENTER]: <%s:%s> BinaryTuple(%s) TupleType(%s) ArgList(%s) \n",
                mod, meth, tostring(b18), type(b18), tostring(arglist));

  local stumbleTuple = list(5);
  -- NOTE: Must append.  Can't index directly into it.
  list.append( stumbleTuple, bytes.get_int32(b18, 1 ));  -- 4 byte int
  list.append( stumbleTuple, bytes.get_int32(b18, 5 ));  -- 4 byte int
  list.append( stumbleTuple, bytes.get_int32(b18, 9 ));  -- 4 byte int
  list.append( stumbleTuple, bytes.get_int32(b18, 13)); -- 4 byte int
  list.append( stumbleTuple, bytes.get_int16(b18, 17));  -- 2 byte int

--  stumbleTuple[1] = bytes.get_int32(b18, 1 );  -- 4 byte int
--  stumbleTuple[2] = bytes.get_int32(b18, 5 );  -- 4 byte int
--  stumbleTuple[3] = bytes.get_int32(b18, 9 );  -- 4 byte int
--  stumbleTuple[4] = bytes.get_int32(b18, 13 ); -- 4 byte int
--  stumbleTuple[5] = bytes.get_int16(b18, 17);  -- 2 byte int

  GP=F and info("[EXIT]: <%s:%s> TupleResult(%s) type(%s)\n",
                mod, meth, tostring(stumbleTuple), type(stumbleTuple ));
  return stumbleTuple;
end -- stumbleUnCompress5()
-- ======================================================================


-- ======================================================================
-- Function stumbleCompress20: Compress a 5 part tuple into a single 20 byte
-- value that we'll pack into storage.
-- The StumbleUpon application creates a 5 part tuple, each part with
-- the following sizes: 4 bytes, 4 bytes, 4 bytes, 4 bytes and 4 bytes.
-- (1) stumbleTuple
-- (2) arglist
-- Return:
-- The newly created Byte object, 20 bytes long
-- ====================================================================== 
function UdfFunctionTable.stumbleCompress20( stumbleTuple, arglist )
  local mod = "UdfFunctionTable";
  local meth = "stumbleCompress20()";
  local rc = 0;
  GP=F and info("[ENTER]: <%s:%s> tuple(%s) ArgList(%s) ",
                mod, meth, tostring(stumbleTuple), tostring(arglist));

  local b20 = bytes(20);
  bytes.put_int32(b20, 1,  stumbleTuple[1] ); -- 4 byte int
  GP=F and info("[D]:<%s:%s>Bytes after 1(%s) ", mod, meth, tostring(b20));
  bytes.put_int32(b20, 5,  stumbleTuple[2] ); -- 4 byte int
  GP=F and info("[D]:<%s:%s>Bytes after 2(%s) ", mod, meth, tostring(b20));
  bytes.put_int32(b20, 9,  stumbleTuple[3] ); -- 4 byte int
  GP=F and info("[D]:<%s:%s>Bytes after 3(%s) ", mod, meth, tostring(b20));
  bytes.put_int32(b20, 13, stumbleTuple[4] ); -- 4 byte int
  GP=F and info("[D]:<%s:%s>Bytes after 4(%s) ", mod, meth, tostring(b20));
  bytes.put_int32(b20, 17, stumbleTuple[5] ); -- 4 byte int
  GP=F and info("[D]:<%s:%s>Bytes after 5(%s) ", mod, meth, tostring(b20));

  GP=F and info("[EXIT]: <%s:%s> BinaryResult(%s)", mod, meth, tostring(b20));
  return b20
end -- stumbleCompress20( stumbleTuple, arglist )

-- ======================================================================
-- Function stumbleUnCompress20: Uncompress a single 20 byte packed binary
-- object into 5 integer fields.
-- The StumbleUpon application uses a 4 part tuple, each part with
-- the following sizes: 4 bytes, 4 bytes, 4 bytes, 4 bytes and 4 bytes.
-- (1) b20: the byteObject
-- (2) arglist
-- Return:
-- the stumbleTuple
-- ======================================================================
function UdfFunctionTable.stumbleUnCompress20( b20, arglist )
  local mod = "UdfFunctionTable";
  local meth = "stumbleUnCompress20()";
  local rc = 0;
  -- protect against bad prints
  if arglist == nil then arglist = 0; end
  GP=F and info("[ENTER]: <%s:%s> BinaryTuple(%s) TupleType(%s) ArgList(%s) \n",
                mod, meth, tostring(b20), type(b20), tostring(arglist));

  local stumbleTuple = list(5);
  -- NOTE: Must append.  Can't index directly into it.
  list.append( stumbleTuple, bytes.get_int32(b20, 1 ));  -- 4 byte int
  GP=F and info("[D]:<%s:%s>Tuple(%s) after 1",
                 mod, meth, tostring(stumbleTuple));
  list.append( stumbleTuple, bytes.get_int32(b20, 5 ));  -- 4 byte int
  GP=F and info("[D]:<%s:%s>Tuple(%s) after 2",
                 mod, meth, tostring(stumbleTuple));
  list.append( stumbleTuple, bytes.get_int32(b20, 9 ));  -- 4 byte int
  GP=F and info("[D]:<%s:%s>Tuple(%s) after 3",
                mod, meth, tostring(stumbleTuple));
  list.append( stumbleTuple, bytes.get_int32(b20, 13)); -- 4 byte int
  GP=F and info("[D]:<%s:%s>Tuple(%s) after 4",
                mod, meth, tostring(stumbleTuple));
  list.append( stumbleTuple, bytes.get_int32(b20, 17));  -- 4 byte int
  GP=F and info("[D]:<%s:%s>Tuple(%s) after 5",
                mod, meth, tostring(stumbleTuple));

--  stumbleTuple[1] = bytes.get_int32(b20, 1 );  -- 4 byte int
--  stumbleTuple[2] = bytes.get_int32(b20, 5 );  -- 4 byte int
--  stumbleTuple[3] = bytes.get_int32(b20, 9 );  -- 4 byte int
--  stumbleTuple[4] = bytes.get_int32(b20, 13 ); -- 4 byte int
--  stumbleTuple[5] = bytes.get_int16(b20, 17);  -- 2 byte int

  GP=F and info("[EXIT]: <%s:%s> TupleResult(%s) type(%s)\n",
                mod, meth, tostring(stumbleTuple), type(stumbleTuple ));
  return stumbleTuple;
end -- stumbleUnCompress20()
-- ======================================================================

-- ======================================================================
-- This is needed to export the function table for this module
-- Leave this statement at the end of the module.
-- ==> Define all functions before this end section.
-- ======================================================================
return UdfFunctionTable;
-- ======================================================================

--
-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
