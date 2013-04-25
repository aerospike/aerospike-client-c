-- Define the module so it can be used by others.
module(..., package.lsoFilters);

-- Large Stack Object (LSO) Operations
-- Filter and Transformation Functions to be used with Lua LSO functions.
-- Last Update: (Feb 25, 2013)
--
-- ======================================================================
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- || FUNCTION TABLE ||
-- |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
-- ======================================================================
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
lsoFunctionTable = {}

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

return functionTable;

-- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> -- <EOF> --
