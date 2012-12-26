.. _apiref:

*******************
Key-Value Store
*******************

Functions
=========

..  function:: cl_rv citrusleaf_get_all(cl_cluster *asc, const char *ns, const char *set, const cl_object *key, cl_bin **bins, int *n_bins, int timeout_ms, uint32_t *cl_gen)

    

..  function:: cl_rv citrusleaf_get_all_digest(cl_cluster *asc, const char *ns, const cf_digest *d, cl_bin **bins, int *n_bins, int timeout_ms, uint32_t *cl_gen)

    

..  function:: cl_rv citrusleaf_get_all_digest_getsetname(cl_cluster *asc, const char *ns, const cf_digest *d, cl_bin **bins, int *n_bins, int timeout_ms, uint32_t *cl_gen, char **setname)

    

..  function:: cl_rv citrusleaf_put(cl_cluster *asc, const char *ns, const char *set, const cl_object *key, const cl_bin *bins, int n_bins, const cl_write_parameters *cl_w_p)

    

..  function:: cl_rv citrusleaf_put_digest(cl_cluster *asc, const char *ns, const cf_digest *d, const cl_bin *bins, int n_bins, const cl_write_parameters *cl_w_p)

    

..  function:: cl_rv citrusleaf_put_replace(cl_cluster *asc, const char *ns, const char *set, const cl_object *key, const cl_bin *values, int n_values, const cl_write_parameters *cl_w_p)

    

..  function:: cl_rv citrusleaf_restore(cl_cluster *asc, const char *ns, const cf_digest *digest, const char *set, const cl_bin *values, int n_values, const cl_write_parameters *cl_w_p)

    

..  function:: cl_rv citrusleaf_async_put(cl_cluster *asc, const char *ns, const char *set, const cl_object *key, const cl_bin *bins, int n_bins, const cl_write_parameters *cl_w_p, uint64_t trid, void *udata)

    

..  function:: cl_rv citrusleaf_async_put_digest(cl_cluster *asc, const char *ns, const cf_digest *d, char *setname, const cl_bin *bins, int n_bins, const cl_write_parameters *cl_w_p, uint64_t trid, void *udata)

    

..  function:: cl_rvclient citrusleaf_check_cluster_health(cl_cluster *asc)

    

..  function:: void citrusleaf_sleep_for_tender(cl_cluster *asc)

    

..  function:: cl_rv citrusleaf_get(cl_cluster *asc, const char *ns, const char *set, const cl_object *key, cl_bin *bins, int n_bins, int timeout_ms, uint32_t *cl_gen)

    

..  function:: cl_rv citrusleaf_get_digest(cl_cluster *asc, const char *ns, const cf_digest *d, cl_bin *bins, int n_bins, int timeout_ms, uint32_t *cl_gen)

    

..  function:: cl_rv citrusleaf_delete(cl_cluster *asc, const char *ns, const char *set, const cl_object *key, const cl_write_parameters *cl_w_p)

    

..  function:: cl_rv citrusleaf_delete_digest(cl_cluster *asc, const char *ns,  const cf_digest *d, const cl_write_parameters *cl_w_p)

    

..  function:: cl_rv citrusleaf_exists_key(cl_cluster *asc, const char *ns, const char *set, const cl_object *key, cl_bin *bins, int n_bins, int timeout_ms, uint32_t *cl_gen)

    

..  function:: cl_rv citrusleaf_exists_digest(cl_cluster *asc, const char *ns, const cf_digest *d, cl_bin *bins, int n_bins, int timeout_ms, uint32_t *cl_gen)

    

