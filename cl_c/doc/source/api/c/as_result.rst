.. _apiref:

*************
as_result
*************

Types
=====

..  type:: struct as_result

    Represents either a success or a failure.

Functions
=========

..  function:: inline int as_result_init(as_result * r, bool is_success, as_val * v)

    

..  function:: inline int as_result_destroy(as_result * r)

    

..  function:: inline int as_result_free(as_result * r)

    

..  function:: inline as_result * as_success(as_val * v)

    

..  function:: inline as_result * as_failure(as_val * e)

    

..  function:: inline int as_result_tosuccess(as_result * r, as_val * v)

    

..  function:: inline int as_result_tofailure(as_result * r, as_val * e)

    
