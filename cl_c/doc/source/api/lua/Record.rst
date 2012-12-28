..  lua:module:: record

*******************
record (Lua Module)
*******************

The :lua:mod:`record` module exposes the :c:type:`as_rec` to the Lua environment. 

The fields of an :type:`as_rec` are accessed like a Lua Table::
    
    r["city"]   = "San Francisco"
    r["state"]  = "CA"

The :c:type:`as_rec` only provides the ability to access and modify the data of a database record, but does not persist changes to the data. To persist changes, you should use the :lua:data:`aerospike` object.