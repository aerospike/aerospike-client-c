****************
map (Lua Module)
****************

The :lua:mod:`map` module exposes the :type:`as_map` to the Lua environment.

The following is a quick example::

    local m = map()
    m["a"] = 1
    m["b"] = 2
    info(m["a"])

A :type:`Map` can be initialized with a Lua Table::

    local m = map {a=1, b=2}
    info(m["a"])

Functions
---------

..  lua:function:: map(Table t)

    Creates a new :type:`as_map`. It can be optionally initialized via an associative Lua Table. ::

        local m = map {a=1, b=2, c=3}

.. lua:module:: map

..  lua:function:: map.size(Map m)

    Get the number of key-value pairs in a :type:`Map` *m*.::

        local m = map {a=1, b=2, c=3}
        map.size(m)
        -- 3
