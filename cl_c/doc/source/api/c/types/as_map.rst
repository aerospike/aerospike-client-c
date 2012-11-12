******
as_map
******

A collection of key-value pairs. The key and value can be any :type:`as_val` type.

To being using an :type:`as_map`, you will need to first have it allocated and initialized via one of the implementations::

    as_map * turtles = as_hashmap_new(32);

To set key-value mappings to the map, use :func:`as_map_set`::

    as_map_set(turtles, as_string_new("a"), as_string_new("Aldabra Tortoise"));
    as_map_set(turtles, as_string_new("b"), as_string_new("Bog Turtle"));
    as_map_set(turtles, as_string_new("c"), as_string_new("Common Snapping Turtle"));
    as_map_set(turtles, as_string_new("d"), as_string_new("Diamondback Terrapin"));
    as_map_set(turtles, as_string_new("e"), as_string_new("Eastern Box Turtle"));
    as_map_set(turtles, as_string_new("f"), as_string_new("False Map Turtle"));

To get a value mapped to a key, use :func:`as_map_get`::

    as_val * a = as_map_get(turtles, "a");
    as_string * c = (as_string *) as_map_get(turtles, "c");

To iterate over the entries of a map, use an :type:`as_iterator` which you can get via :func:`as_map_iterator`. You will need
to remember that the valuess returns in the key-value pair, represented as an :type:`as_pair`::

    as_iterator * i = as_map_iterator(turtles);
    while ( as_iterator_has_next(i) ) {
        as_pair * p = (as_pair *) as_iterator_next(i);
        as_string * k = (as_string *) as_pair_1(p);
        as_string * v = (as_string *) as_pair_2(p);
        printf("%s -> %s\n", as_string_tostring(k), as_string_tostring(v));
    }
    as_iterator_free(i);


You should also make sure to use :func:`as_map_free` to free the map when you are done::

    as_map_free(turtles);

You will need to remember that keys and values added to the map will be freed by the map. 

Types
=====

..  type:: struct as_map
    
    A handle to the specific implementation of List. It contains a pointer to the actual
    data structure containing the list and the functions that operate on that list.

Implementations
===============

.. function:: as_map * as_hashmap_new(uint32_t n)

   .. refcounting:: new

   Returns a new as_map that is backed by a hash table. The parameter ``n`` specifies the number
   of buckets to allocate for the table.

Functions
=========

..  function:: inline int as_map_free(as_map * m)

    Free the :type:`as_map` and its contents.

..  function:: inline void * as_map_source(const as_map * m)

    Get the source backing this :type:`as_map`.

..  function:: inline int as_map_hash(as_map * m)

    Get the hash value for this :type:`as_map`. 

..  function:: inline uint32_t as_map_size(const as_map * m)

    Get the number of key-value entries.

..  function:: inline as_val * as_map_get(const as_map * m, const as_val * k)

    Get the value associated with the specified key. If no value is mapped, then return NULL.

..  function:: inline int as_map_set(as_map * m, const as_val * k, const as_val * v)

    Associate a value with the specified key.

..  function:: inline int as_map_clear(as_map * m)

    Removes all entries from the map.

..  function:: inline as_iterator * as_map_iterator(const as_map * m)

    .. refcounting:: new

    Returns a new iterator over the entries of this map. Each entry is an ``as_pair *``. 

    Example::

        as_iterator * i = as_map_iterator(turtles);
        while ( as_iterator_has_next(i) ) {
            as_pair * p = (as_pair *) as_iterator_next(i);
            as_val * k = as_pair_1(p);
            as_val * v = as_pair_2(p);
        }


..  function:: inline as_val * as_map_toval(const as_map * m)

    Convert to an as_val. If conversion fails, then NULL is return. You can alternative do a type cast::

        as_val * v = (as_val *) x;

..  function:: inline as_map * as_map_fromval(const as_val * v)

    Convert from an as_val. If conversion fails, then NULL is return. 

