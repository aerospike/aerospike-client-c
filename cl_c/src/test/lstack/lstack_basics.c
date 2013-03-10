
#include "../test.h"
#include "../util/udf.h"
#include <citrusleaf/citrusleaf.h>
#include <citrusleaf/as_types.h>
#include <citrusleaf/as_lso.h>

/******************************************************************************
 * UTILITY FUNCTION
 *****************************************************************************/
void __log_append(FILE * f, const char * prefix, const char * fmt, ...) {
    char msg[128] = {0};
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(msg, 128, fmt, ap);
    va_end(ap);
    fprintf(f, "%s%s\n",prefix,msg);
}

/******************************************************************************
 * TEST CASES
 *****************************************************************************/
static int lso_push_number(char * ns, char * set,
                           char *package_name, char * key,
                           char * lso_bin_name, int val) {
    extern cl_cluster * cluster;

    cl_write_parameters wp;
    cl_write_parameters_set_default(&wp);
    wp.timeout_ms = 1000;
    wp.record_ttl = 864000;

    char * compress_func   = "stumbleCompress5";
    as_list *compress_args = as_arraylist_new( 1, 1 );
    as_list_add_integer( compress_args, 1 ); // dummy argument

    as_list * listp = as_arraylist_new( 5, 5 );
    int64_t urlid   = val + 1;
    as_list_add_integer( listp, urlid );
    int64_t created = val + 2;
    as_list_add_integer( listp, created );
    int64_t meth_a  = val + 3;
    as_list_add_integer( listp, meth_a );
    int64_t meth_b  = val + 4;
    as_list_add_integer( listp, meth_b );
    int64_t status  = val + 5;
    as_list_add_integer( listp, status );

    return as_lso_push_with_transform( cluster, ns, set, key, lso_bin_name,
                                       (as_val *)listp, package_name,
                                       compress_func, compress_args,
                                       wp.timeout_ms);
}

TEST( as_lso_push_with_transform_1, "as_lso_push_with_transform" ) {
    char * package_name = "LSTACK";
    char * user_key     = "User_111";
    char * lso_bin_name = "number_stack";

    int rc = lso_push_number("test", "unit", package_name,
                             user_key, lso_bin_name, 5);
    assert_int_eq( rc, 0 );
}

/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

static bool before(atf_suite * suite) {
    return true;
}

static bool after(atf_suite * suite) {
    return true;
}

SUITE( lstack_basics, "test basics.lua" ) {
    suite_before( before );
    suite_after( after );

    suite_add( as_lso_push_with_transform_1 );
}
