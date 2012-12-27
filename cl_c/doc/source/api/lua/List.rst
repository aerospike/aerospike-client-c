*************
List
*************

A :type:`List` is a sequence of values. The :type:`List` is a Lua representation of a :type:`as_list`.

The following is a quick example::

    local l = list()
    m[1] = "a"
    m[2] = "b"
    info(l[1])

A :type:`List` can be initialize with a Lua Table::

    local l = list {"a","b"}
    info(l[1])

Types
-----

..  type:: List

    A :type:`List` is a sequence of values. The :type:`List` is a Lua representation of a :type:`as_list`.


Functions
---------

..  function:: list.size(as_list l)

    Get the number of key-value pairs in a :type:`List` *l*.

..  function:: list.iterator(as_list l)

    Get an iterator for the elements of the :type:`List` *l*.

..  function:: list.append(as_list l, as_val v)

    Append a value to the end of the :type:`List` *l*.

..  function:: list.prepend(as_list l, as_val v)

    Prepend a value to the beginning of the :type:`List` *l*.
    
..  function:: list.take(as_list l)

    Select the first *n* elements of the :type:`List` *l*

..  function:: list.drop(as_list l)

    Select all elements except the first *n* elements of the :type:`List` *l*.
