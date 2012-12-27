.. _apiref:

*************
Record
*************

A :type:`Record` represents a record, which is a mapping of field names to values. The :type:`Record` is a Lua representation of a :type:`as_rec`.

A :type:`Record` contains fields, which are accessed like a Lua Table::
    
    r["city"]   = "San Francisco"
    r["state"]  = "CA"

A :type:`Record` only represents the data in a database record, but does not provide any database operations, instead, you should utilize :type:`Aerospike` object::

    if aerospike:exists(r) then
        aerospike:update(r)
    else
        aerospike:create(r)
    end

Types
-----

..  type:: Record

    A :type:`Record` represents a record, which is a mapping of field names to values. The :type:`Record` is a Lua representation of a :type:`as_rec`.
