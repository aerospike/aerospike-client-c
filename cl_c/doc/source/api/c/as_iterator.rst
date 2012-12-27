***********
as_iterator
***********

Iterators are data structures that provide the ability to iterate over a sequence of elements.

Types
=====

..  type:: struct as_iterator

    A handle to the iterator.

Functions
=========

..  function:: inline int as_iterator_free(as_iterator * i)

    Free the :type:`as_iterator` and its contents.

..  function:: inline const void * as_iterator_source(const as_iterator * i)

    Get the backend source for the :type:`as_iterator`.

..  function:: inline const bool as_iterator_has_next(const as_iterator * i)

    Test whether the :type:`as_iterator` can provide another element.

..  function:: inline const as_val * as_iterator_next(as_iterator * i)

    Produces the next element of this :type:`as_iterator`.

