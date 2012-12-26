.. _apiref:

*************
as_string
*************

Types
=====

..  type:: struct as_string

    A string.

Functions
=========

..  function:: int as_string_init(as_string * s, char * cs)

    Initialize a stack allocated :type:`as_string`::

        as_string s;
        as_string_init(&s, "abc");

..  function:: inline int as_string_destroy(as_string * s) 

    Frees the contents of the :type:`as_string`, not the :type:`as_string` itself.

..  function:: inline as_string * as_string_new(char * s)

    Allocates a new :type:`as_string` on the heap::

        as_string * s = as_string_new("abc");

..  function:: inline int as_string_free(as_string * s)

    Free the :type:`as_string` and its contents.

..  function:: inline char * as_string_tostring(const as_string * s)

    Get the raw value of the :type:`as_string`.

..  function:: inline size_t as_string_len(as_string * s)

    Get the length of the string.

..  function:: inline as_val * as_string_toval(const as_string * s)

    Convert to an as_val. If conversion fails, then NULL is return. You can alternative do a type cast::

        as_val * v = (as_val *) x;

..  function:: inline as_string * as_string_fromval(const as_val * v)

    Convert from an as_val. If conversion fails, then NULL is return. 