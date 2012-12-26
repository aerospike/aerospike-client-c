.. _apiref:

*************
as_stream
*************

An  :type:`as_stream` produces a continuous stream of elements.

Types
=====

..  type:: struct as_stream

    A handle to the stream. 

Macros
=======

..  macro:: #define AS_STREAM_END

    Indicates the end of a stream.

Functions
=========

..  function:: inline int as_stream_free(as_stream * s)
    
    Free the :type:`as_stream` and its contents.

..  function:: inline void * as_stream_source(const as_stream * s)
    
    Get the backend source for the :type:`as_stream`.

..  function:: inline const as_val * as_stream_read(const as_stream * s)

    Read the next element from the stream. An AS_STREAM_END is returned when the end of the
    stream has been reached.
    

