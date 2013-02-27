-- Define the module so it can be used by others.
module(..., package.UDF_Table);
--
-- There is a new family of Aerospike Types and Functions that are
-- implemented with UDFs: Large Stack Objects (LSO) and Large Sets (LSET).
-- Some of these new functions take a UDF as a parameter, which is then
-- executed on the server side.  We pass those "inner" UDFs by name, and
-- and those names reference a function that is stored in a table. This
-- module defines those "inner" UDFs.
--
-- This table defines
-- (*) LSO Transform functions: Used for peek() and push()
-- (*) LSO Filter functions: Used for peek()
-- (*) LSET Transform functions: Used for insert() and select()
-- (*) LSET Filter functions: Used for select()
-- 
-- Last Update: (Feb 27, 2013) tjl
--
-- ======================================================================
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- || FUNCTION TABLE ||
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
-- Table of Functions: Used for Transformation and Filter Functions.
--
-- In order to pass functions as parameters in Lua (from C), we don't have
-- the ability to officially pass a true Lua function as a parameter to
-- the top level Lua function, so we instead pass the "inner" Lua function
-- by name, as a simple string.  That string corresponds to the names of
-- functions that are stored in this file, and the parameters to be fed
-- to the inner UDFs are passed in a list (arglist).
--
-- NOTE: These functions are not meant to be written by regular users.
-- It is the job of knowledgeable DB Administrators to write, review and
-- install both the top level UDFs and these "inner" UDFs on the Aerospike
-- server.  As a result, there are few protections against misuse or
-- just plain bad coding.  So -- Users and Administrators Beware!!
-- ======================================================================
-- Usage:
--
-- From the main function table "functionTable", we can call any of the
-- functions defined here by passing its name and the associated arglist
-- that is supplied by the user.  For example, in stackPeekWithUDF, we
-- 
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
UdfFunctionTable = {}

-- ======================================================================
-- Sample Filter function to test user entry 
-- Parms (encased in arglist)
-- (1) Entry List
-- ======================================================================
local function lsoFunctionTable.transformFilter1( argList )
  local mod = "LsoFilters";
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
  local mod = "LsoFilters";
  local meth = "rangeFilter()";
  info("[ENTER]: <%s:%s> ArgList(%s) \n", mod, meth, tostring(arglist));

  info("[EXIT]: <%s:%s> Result(%d) \n", mod, meth, 0 );

  return 0
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
  local mod = "LsoFilters";
  local meth = "compress()";

  return 0
end

-- ======================================================================
-- Function unCompressTransform1: uncompress the single byte string into
-- multiple map fields -- as defined by the compression field table.
-- Parms (encased in arglist)
-- (1) Entry List
-- (2) Compression Field Parameters Table Index
-- ======================================================================
local function functionTable.unCompressTransform1( arglist )
  local mod = "LsoFilters";
  local meth = "unCompress()";

  return 0
end

return UdfFunctionTable;

-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
