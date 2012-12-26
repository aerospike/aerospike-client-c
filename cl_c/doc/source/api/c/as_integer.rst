.. _apiref:

*************
as_integer
*************

Types
=====

..  type:: struct as_integer

    A 64-bit signed integer.

Functions
=========

..  function:: int as_integer_init(as_integer * i, int64_t v)

    Initialize a stack allocated :type:`as_integer`::

        as_integer i;
        as_integer_init(&i, 1234);

..  function:: inline int as_integer_destroy(as_integer * i)

    Frees the contents of the :type:`as_integer`, not the :type:`as_integer` itself.

    Use this to clean up a stack allocated :type:`as_integer`::

        as_integer_destroy(&i);

..  function:: inline as_integer * as_integer_new(int64_t i)

    Allocates a new as_integer on the heap::

        as_integer * i = as_integer_new(1234);

..  function:: inline int as_integer_free(as_integer * i)

    Free the :type:`as_integer` and its contents.

..  function:: inline int64_t as_integer_toint(const as_integer * i)

    Get the raw value of the :type:`as_integer`.

..  function:: inline as_val * as_integer_toval(const as_integer * i)

    Convert to an as_val. If conversion fails, then NULL is return. You can alternative do a type cast::

        as_val * v = (as_val *) x;

..  function:: inline as_integer * as_integer_fromval(const as_val * v)

    Convert from an as_val. If conversion fails, then NULL is return. 