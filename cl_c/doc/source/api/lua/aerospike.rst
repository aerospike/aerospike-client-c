..  lua:module:: aerospike

**********************
aerospike (Lua Object)
**********************

The :lua:mod:`aerospike` object is a global object that exposes database operations to the Lua environment.

..  lua:function:: aerospike:create(as_rec r)

    Create a new record in the database.::

        aerospike:create(rec)

..  lua:function:: aerospike:update(as_rec r)

    Update an existing record in the database.::

        aerospike:update(rec)

..  lua:function:: aerospike:exists(as_rec r)

    Checks for the existance of a record in the database.::

        aerospike:exists(rec)

..  lua:function:: aerospike:remove(as_rec r)

    Remove an existing record from the database.::

        aerospike:remove(rec)

