.. _apiref:

*************
Map
*************

A :type:`Map` is a mapping of keys to values. The :type:`Map` is a Lua representation of a :type:`as_map`.

The following is a quick example::

    local m = map()
    m["a"] = 1
    m["b"] = 2
    info(m["a"])

A :type:`Map` can be initialized with a Lua Table::

    local m = map {a=1, b=2}
    info(m["a"])


Types
---------

..  type:: Map

    A :type:`Map` is a mapping of key to values. The :type:`Map` is a Lua representation of a :type:`as_map`.


Functions
---------

..  function:: map.size(m)

    Get the number of key-value pairs in a :type:`Map` *m*.
