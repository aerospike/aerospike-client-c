*************
as_val
*************

How the hell do I explain this? There is no sub-typing and such in C, but this effectively provides it.

Types
=====

..  type:: struct as_val
    
    Represents a value.

..  type:: enum as_val_t

    The type of value. The following members are defined:

    +--------------------+
    | ``AS_UNKNOWN``     |
    +--------------------+
    | ``AS_NIL``         |
    +--------------------+
    | ``AS_BOOLEAN``     |
    +--------------------+
    | ``AS_INTEGER``     |
    +--------------------+
    | ``AS_LIST``        |
    +--------------------+
    | ``AS_MAP``         |
    +--------------------+
    | ``AS_REC``         |
    +--------------------+
    | ``AS_PAIR``        |
    +--------------------+

Macros
=========

..  macro:: #define as_val_free(v)

    Free the as_val.

..  macro:: #define as_val_type(v)

    Get the type of the as_val.

..  macro:: #define as_val_hash(v)

    Get the hash value of the as_val.

..  macro:: #define as_val_tostring(v)

    Get the string representation of the as_val

..  macro:: #define as_val_size(v)

    Get the size of the as_val.

