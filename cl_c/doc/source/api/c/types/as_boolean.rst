.. module:: as
**********
as_boolean
**********

Types
=====

..  type:: struct as_boolean

    A boolean value, either true or false, nothing more, nothing less.

Functions
=========

..  function:: int as_boolean_init(as_boolean * b, bool v)

    Initialize a stack allocated :type:`as_boolean`::

        as_boolean b;
        as_boolean_init(&b, true);

..  function:: inline int as_boolean_destroy(as_boolean * b)

    Frees the contents of the :type:`as_boolean`, not the :type:`as_boolean` itself.

    Use this to clean up a stack allocated :type:`as_boolean`::

        as_boolean_destroy(&b);

..  function:: inline as_boolean * as_boolean_new(bool b)

    Allocates a new as_boolean on the heap::

        as_boolean * b = as_boolean_new(true);

..  function:: inline int as_boolean_free(as_boolean * b)

    Free the :type:`as_boolean` and its contents.

..  function:: inline bool as_boolean_tobool(const as_boolean * b)

    Get the raw value of the :type:`as_boolean`.

..  function:: inline as_val * as_boolean_toval(const as_boolean * b)

    Convert to an as_val. If conversion fails, then NULL is return. You can alternative do a type cast::

        as_val * v = (as_val *) x;

..  function:: inline as_boolean * as_boolean_fromval(const as_val * v)

    Convert from an as_val. If conversion fails, then NULL is return. 