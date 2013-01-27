%module citrusleaf
%include <typemaps.i>
%include "carrays.i"
%include "cpointer.i"
%include "cdata.i"

%{
#include "citrusleaf/citrusleaf.h"

/*Structure Definitions*/
typedef struct cl_record {
        int gen;
        cl_bin **bin;
        cf_digest * digest;
        char * ns;
        char * set;
        uint32_t record_ttl;
        int n_bins;
}cl_record;

typedef struct batchresult {
        cl_record * records;
        int index;
        cl_rv rv;
}BatchResult;

/*Wrapper functions*/
char * get_name(cl_bin * bin, int in) {
        return (bin[in].bin_name);
}
cl_object get_object(cl_bin * bin,int in) {
        return bin[in].object;
}

int cb(char *ns, cf_digest *keyd, char *set, uint32_t generation, uint32_t record_ttl, cl_bin *bin, int n_bins, bool is_last, void *udata) {
        BatchResult * p = (BatchResult*) udata;
        cl_record * re = p->records;
        int ind = p->index;
        re[ind].gen = generation;
        re[ind].digest = keyd;
        
        // copy bin array into new memory
        cl_bin* bins = (cl_bin*)malloc(n_bins * sizeof(cl_bin));
        memcpy(bins, bin, n_bins*sizeof(cl_bin));
	
        re[ind].bin = (cl_bin**)malloc(sizeof(cl_bin*));
        *(re[ind].bin) = bins;
	
        re[ind].n_bins = n_bins;
        re[ind].record_ttl = record_ttl; 
        (p->index)++;
}

BatchResult  citrusleaf_batch_get(cl_cluster *asc, char *ns, const cf_digest *digests, int n_digests, cl_bin *bins, int n_bins, bool get_key) {
        cl_record * ret = (cl_record *)malloc(sizeof(cl_record)*n_digests);
        BatchResult val;
        val.records = ret;
        val.index = 0;
        int rv = citrusleaf_get_many_digest(asc,ns,digests,n_digests,bins,n_bins,get_key,cb,&val);      
        val.rv = rv;
        return val;
}
void citrusleaf_free_bins(cl_bin * bin, int n, cl_bin**binp) {
        if(bin) {
                citrusleaf_bins_free(bin,n);
        }
        if (binp && *binp) {
                free(*binp);
                *binp = NULL;
        }
        return;
}

%}


/*Declaring an array*/
%array_class(cl_bin,cl_bin_arr);
%array_class(cf_digest,cf_digest_arr);
%array_class(cl_record,cl_record_arr);
%array_class(cl_operation,cl_op_arr);

/*Declaring pointers*/
%pointer_functions(int,intp);
%pointer_functions(cl_bin*,cl_bin_p);
%pointer_functions(char,charp);
%pointer_functions(cf_digest,cf_digest_p);

/* Exposing Structures to python applications*/
typedef struct cl_record {
        int gen;
        cl_bin* * bin;
         char * ns;
        char * set;
        unsigned int  record_ttl;
        int n_bins;
        cf_digest * digest;
}cl_record;

typedef struct batchresult {
        cl_record_arr * records;
        int index;
        int rv;
}BatchResult;

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
???LINES MISSING
int citrusleaf_operate(cl_cluster *asc, const char *ns, const char *set, const cl_object *key, cl_op_arr *operations, int n_operations, const cl_write_parameters *cl_w_p, int replace, int *generation);
