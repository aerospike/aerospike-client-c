
*****************
list (Lua Module)
*****************

The :lua:mod:`list` module exposes the :type:`as_list` to the Lua environment.

The following is a quick example::

    local l = list()
    m[1] = "a"
    m[2] = "b"
    info(l[1])

A :type:`List` can be initialize with a Lua Table::

    local l = list {"a","b"}
    info(l[1])

Functions
---------

..  lua:function:: list(Table t)

    Creates a new :type:`as_map`. It can be optionally initialized via an indexed Lua Table. 
    ::

        local l = list {1,2,3}

..  lua:module:: list


..  lua:function:: list.size(List l)

    Get the number of key-value pairs in a :type:`List` *l*.
    ::

        local l = list {1,2,3}
        list.size(l) 
        -- 3

..  lua:function:: list.iterator(List l)

    Get an iterator for the elements of the :type:`List` *l*.

..  lua:function:: list.append(List l, as_val v)

    Append a value to the end of the :type:`List` *l*.
    ::

        local l = list {1,2,3}
        list.append(l, 4)
        list.append(l, 5)
        -- [1, 2, 3, 4, 5]

..  lua:function:: list.prepend(List l, as_val v)

    Prepend a value to the beginning of the :type:`List` *l*.
    ::
    
        local l = list {1,2,3}
        list.prepend(l, 4)
        list.prepend(l, 5)
        -- [5, 4, 1, 2, 3]
    
..  lua:function:: list.take(List l)

    Select the first *n* elements of the :type:`List` *l*
    ::
    
        local l = list {1,2,3}
        list.take(l, 2)
        -- [1, 2]

..  lua:function:: list.drop(List l)

    Select all elements except the first *n* elements of the :type:`List` *l*.
    ::
    
        local l = list {1,2,3}
        list.drop(l, 2)
        -- [3]
