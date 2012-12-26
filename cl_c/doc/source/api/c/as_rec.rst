.. _apiref:

*************
as_rec
*************

Represents a record in the database. 

Types
=====

..  type:: struct as_rec
    
    A handle pointing to data representing a record.

Functions
=========

..  function:: inline int as_rec_free(as_rec * r)

    Free the :type:`as_rec` and its contents.

..  function:: inline void * as_rec_source(const as_rec * r)

    Get the backend source of the :type:`as_rec`.

..  function:: inline uint32_t as_rec_hash(as_rec * r)

    Get the hash value of the :type:`as_rec`.

..  function:: inline as_val * as_rec_get(const as_rec * r, const char * name)

    Get the value for the field with specified name.

..  function:: inline int as_rec_set(const as_rec * r, const char * name, const as_val * value)

    Set the value for the field with the specified name.

..  function:: inline int as_rec_remove(const as_rec * r, const char * name)

    Remove the field with the specified name.

..  function:: inline as_val * as_rec_toval(const as_rec * r)
    
    Convert to an as_val. If conversion fails, then NULL is return. You can alternative do a type cast::

        as_val * v = (as_val *) x;

..  function:: inline as_rec * as_rec_fromval(const as_val * v)

    Convert from an as_val. If conversion fails, then NULL is return. 