%module citrusleaf
%include <typemaps.i>
%include "carrays.i"
%include "cpointer.i"

%{
#include "citrusleaf/citrusleaf.h"
char * get_name(cl_bin * bin, int in) {
        return (bin[in].bin_name);
}
cl_object get_object(cl_bin * bin,int in) {
        return bin[in].object;
}
%}


/*Declaring an array*/
%array_class(cl_bin,cl_bin_arr);

/*Declaring pointers*/
%pointer_functions(int,intp);
%pointer_functions(cl_bin*,cl_bin_p);
%pointer_functions(char,charp);
%pointer_functions(cf_digest,cf_digest_p);

/*Exposing C structures to python applications*/
typedef struct { char digest[CF_DIGEST_KEY_SZ]; } cf_digest;

typedef struct cl_bin_s {
        char            bin_name[32];
        cl_object       object;
} cl_bin;
typedef struct cl_object_s {
        enum cl_type     type;
        size_t                  sz;
        union {
                char            *str;  
                void            *blob;
                long long            i64;  
       } u;

        void *free;       

} cl_object;

enum cl_type { CL_NULL = 0x00, CL_INT = 0x01, CL_FLOAT = 2, CL_STR = 0x03, CL_BLOB = 0x04,
        CL_TIMESTAMP = 5, CL_DIGEST = 6, CL_JAVA_BLOB = 7, CL_CSHARP_BLOB = 8, CL_PYTHON_BLOB = 9,
        CL_RUBY_BLOB = 10, CL_PHP_BLOB = 11, CL_UNKNOWN = 666666};
typedef enum cl_rv {
    CITRUSLEAF_FAIL_ASYNCQ_FULL = -3,
    CITRUSLEAF_FAIL_TIMEOUT = -2,
        CITRUSLEAF_FAIL_CLIENT = -1,    
        CITRUSLEAF_OK = 0,
        CITRUSLEAF_FAIL_UNKNOWN = 1,    
        CITRUSLEAF_FAIL_NOTFOUND = 2,   
        CITRUSLEAF_FAIL_GENERATION = 3, 
        CITRUSLEAF_FAIL_PARAMETER = 4,  
        CITRUSLEAF_FAIL_KEY_EXISTS = 5,
        CITRUSLEAF_FAIL_BIN_EXISTS = 6,
        CITRUSLEAF_FAIL_CLUSTER_KEY_MISMATCH = 7,
        CITRUSLEAF_FAIL_PARTITION_OUT_OF_SPACE = 8,
        CITRUSLEAF_FAIL_SERVERSIDE_TIMEOUT = 9,
        CITRUSLEAF_FAIL_NOXDS = 10
} cl_rv;

typedef struct {
        bool unique;  
        bool unique_bin; 
        bool use_generation;     
        bool use_generation_gt;  
        bool use_generation_dup;    
        unsigned int generation;
        int timeout_ms;
        unsigned int record_ttl;    
        cl_write_policy w_pol;
} cl_write_parameters;

/*Exposed functions to the python application*/
int citrusleaf_init(void);
void citrusleaf_shutdown(void);
cl_cluster * citrusleaf_cluster_create(void);
int citrusleaf_cluster_add_host(cl_cluster *asc, char const *host, short port, int timeout_ms);
void citrusleaf_cluster_destroy(cl_cluster * asc);
static inline void cl_write_parameters_set_default(cl_write_parameters *cl_w_p);
void citrusleaf_object_init(cl_object *o);
void citrusleaf_object_init_str(cl_object *o, char const *str);
void citrusleaf_object_init_str2(cl_object *o, char const *str, size_t str_len);
void citrusleaf_object_init_blob(cl_object *o, void const *buf, size_t buf_len);
void citrusleaf_object_init_blob2(cl_object *o, void const *buf, size_t buf_len, cl_type type); // several blob types
void citrusleaf_object_init_int(cl_object *o, long long i);
void citrusleaf_object_init_null(cl_object *o);
int citrusleaf_put(cl_cluster * asc, const char * ns, const char * set, const cl_object *key, cl_bin_arr * values, int n_bins,const cl_write_parameters *cl_w_p);
int citrusleaf_get(cl_cluster * asc, const char * ns, const char * set, const cl_object * key, cl_bin_arr * bins, int n_bins, int timeout_ms, int * cl_gen);
int citrusleaf_get_all(cl_cluster * asc, const char *ns , const char * set, const cl_object * key,cl_bin ** bins, int * sz, int timeout_ms, int * cl_gen);
int citrusleaf_get_digest(cl_cluster *asc, const char *ns, const cf_digest * d, cl_bin_arr *bins, int n_bins, int timeout_ms, int *cl_gen);
int citrusleaf_put_digest(cl_cluster *asc, const char *ns, const cf_digest *d, cl_bin_arr *bins, int n_bins, cl_write_parameters * cl_wp);
int citrusleaf_delete_digest(cl_cluster *asc, const char *ns,  const cf_digest *d, const cl_write_parameters *cl_w_p);
int citrusleaf_delete(cl_cluster *asc, const char *ns, const char *set, const cl_object *key, const cl_write_parameters *cl_w_p);
char * get_name(cl_bin * bin, int in);
cl_object get_object(cl_bin * bin, int in);
void citrusleaf_bins_free(cl_bin_arr *bins, int n_bins);
