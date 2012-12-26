.. _apiref:

*************
as_pair
*************

Types
=====

..  type:: struct as_pair

    A tuple, pair, two things, no more, no less.

Macros
======

..  macro:: #define pair(a,b)

    A wrapper for :func:`as_pair_new`, that converts the values to an :type:`as_val`.

Functions
=========

..  function:: int as_pair_init(as_pair *, as_val *, as_val *)

    Initializes an :type:`as_pair` allocated on the stack.

    Use this to initialize a pair that was created on the stack::

        as_pair p;
        as_pair_init(&pair, as_string_new("a"), as_string_new("b"));

..  function:: int as_pair_destroy(as_pair *)

    Free the values contained by the :type:`as_pair`. 

..  function:: inline as_pair * as_pair_new(as_val * _1, as_val * _2)

    Allocates and initializes an :type:`as_pair` on the heap.

..  function:: inline int as_pair_free(as_pair * p)

    Free the :type:`as_pair` and it's values.

..  function:: inline as_val * as_pair_1(as_pair * p)

    Get the first value.

..  function:: inline as_val * as_pair_2(as_pair * p)

    Get the second value.

..  function:: inline as_val * as_pair_toval(const as_pair * p)

    Convert to an as_val. If conversion fails, then NULL is return. You can alternative do a type cast::

        as_val * v = (as_val *) x;

..  function:: inline as_pair * as_pair_fromval(const as_val * v)

    Convert from an as_val. If conversion fails, then NULL is return. 

