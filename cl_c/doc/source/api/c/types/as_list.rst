*******
as_list
*******

A collection of values. 

To begin using an :type:`as_list`, you will need to first have it allocated and initialized via one of the implementations::

    as_list * states = as_arraylist_new(50);

To add elements to the list, use :func:`as_list_append`::

    as_list_append(states, as_string_new("Arizona"));
    as_list_append(states, as_string_new("Askansas"));

To prepend elements to the list, use :func:`as_list_prepend`::

    as_list_prepend(states, as_string_new("Alaska"));
    as_list_prepend(states, as_string_new("Alabama"));

To get a sublist, use either :func:`as_list_take` and :func:`as_list_drop`::

    as_list * sublist1 = as_list_take(state, 5);
    as_list * sublist2 = as_list_drop(sublist1, 3);

To iterate over the values, you can get an as_iterator via :func:`as_list_iterator`::

    as_iterator * i = as_map_iterator(states);
    while ( as_iterator_has_next(i) ) {
        as_string * state = (as_string *) as_iterator_next(i);
        printf("%s\n", as_string_tostring(string));
    }
    as_iterator_free(i);

You should also make sure to use :func:`as_list_free` to free the list when you are done::

    as_list_free(states);

You will need to remember that values added to the list will be freed by the list. 

Types
=====

..  type:: struct as_list
    
    A handle to the specific implementation of List. It contains a pointer to the actual
    data structure containing the list and the functions that operate on that list.

Implementations
===============

..  function:: as_list * as_arraylist_new(uint32_t n, uint32_t z)

    .. refcounting:: new

    Returns a new as_map that is backed by a dynamic array. 

    The initial size of the array is specified by *n* and  *z* specifies number of 
    new entries to allocate when the array reaches capacity.

..  function:: as_list * as_linkedlist_new(as_val * head, as_list * tail)

    .. refcounting:: new

    Returns a new as_list that is backed by a linked list.


Functions
=========

..  function:: inline int as_list_free(as_list * l)

    Free the :type:`as_list` and its contents.

..  function:: inline void * as_list_source(const as_list * l)

    Get the backend source for the :type:`as_list`.

..  function:: inline uint32_t as_list_hash(as_list * l)

    Get the hash value for the :type:`as_list`.

..  function:: inline uint32_t as_list_size(as_list * l)

    Get the number of elements.

..  function:: inline int as_list_append(as_list * l, as_val * v)

    Append a value.

..  function:: inline int as_list_prepend(as_list * l, as_val * v)

    Prepend a value.

..  function:: inline as_val * as_list_get(const as_list * l, const uint32_t i)

    Get a value at specified index.

..  function:: inline int as_list_set(as_list * l, const uint32_t i, as_val * v)

    Set a value at specified index.

..  function:: inline as_val * as_list_head(const as_list * l)

    Selects the first element.

..  function:: inline as_list * as_list_tail(const as_list * l)

    Selects all elements except the first.

..  function:: inline as_list * as_list_drop(const as_list * l, uint32_t n)

    .. refcounting:: new

    Selects all elements except first *n* elements.

..  function:: inline as_list * as_list_take(const as_list * l, uint32_t n)

    .. refcounting:: new
    
    Selects first *n* elements.

..  function:: inline as_iterator * as_list_iterator(const as_list * l)

    .. refcounting:: new
    
    Create a new iterator over the elements of the list.

    Example::

        as_iterator * i = as_map_iterator(list);
        while ( as_iterator_has_next(i) ) {
            as_val * element = as_iterator_next(i);
        }

..  function:: inline as_val * as_list_toval(const as_list * l)

    Convert to an as_val. If conversion fails, then NULL is return. You can alternative do a type cast::

        as_val * v = (as_val *) x;

..  function:: inline as_list * as_list_fromval(const as_val * v)

    Convert from an as_val. If conversion fails, then NULL is return. 

