********************
Working with Records
********************

The first argument to user-defined functions (UDF) is a :type:`Record`. When a UDF is called, the client will specify the namespace, set and key of the :type:`Record` which the UDF should be applied to. The :type:`Record` itself does not need to exist when the UDF is called. The UDF will receive a :type:`Record` as the first argument, regardless of whether it exists or not in the database. The UDF can then choose to access or modify fields of a :type:`Record` and persist or remove the :type:`Record` from the database.

A :type:`Record` will be provided as the first argument to a UDF. A :type:`Record` cannot be created programmatically from within a UDF.


Accessing Records
===============================

A :type:`Record` can be accessed like a Lua Table::

    rec.field1
    rec["field2"]

The values for the fields of a :type:`Record` will be either a ``Integer``, ``String``, :type:`List`, or :type:`Map`. 

The first accessed to a field of a :type:`Record` will cause the data for the :type:`Record` to be lazy loaded from the database. This means that the data is not read from the database until it is actually needed. 

Modifying Records
=================

A :type:`Record` can be modified like a Lua Table::

    rec.field1 = 1
    rec["field2"] = 2

The values for the fields of a :type:`Record` can only be either a ``Integer``, ``String``, :type:`List`, or :type:`Map`. You can not set fields to values of other types such as ``Boolean``, ``Table``, etc.

If you set the value of a field to ``nil``, then the field itself will be removed from the :type:`Record`::

    rec.field1 = nil

Modifications to a field of a :type:`Record` is not persisted until either :func:`aerospike:create` or :func:`aerospike:update` is called. 

Persisting Records
==================

Aerospike provides the following persistence functions:

- :func:`aerospike:exists` – tests for the existence of the :type:`Record` in the database.
- :func:`aerospike:create` – creates a new :type:`Record` in the database.
- :func:`aerospike:update` – updates an existing :type:`Record` in the database.
- :func:`aerospike:remove` – removes an existing :type:`Record` from the database.

Modifications to a :type:`Record` are stored only when :func:`aerospike:create` or :func:`aerospike:update` are called. 

Currently, the :func:`aerospike:exists` should be tested before invoking either :func:`aerospike:create`, :func:`aerospike:update` or :func:`aerospike:remove`::

    if aerospike:exists(rec) then
        aerospike:update(rec)
    else
        aerospike:create(rec)
    end

**Note:** *This flow will be simplified in a future release be a single function call.*

Similarly, you should check the existence of a :type:`Record` before removing it::

    if aerospike:exists(rec) then
        aerospike:remove(rec)
    end

**Note:** *The remove flow will be simplified in a future release.*