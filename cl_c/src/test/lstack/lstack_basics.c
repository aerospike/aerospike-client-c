
#include "../test.h"
#include "../util/udf.h"
#include <citrusleaf/citrusleaf.h>
#include <citrusleaf/as_types.h>
#include <citrusleaf/aerospike_lstack.h>

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
static int lso_push_quintuplet(char * ns, char * set, char * key,
                               char * lso_bin_name, int val) {
    extern cl_cluster * cluster;

    cl_write_parameters wp;
    cl_write_parameters_set_default(&wp);
    wp.timeout_ms = 1000;
    wp.record_ttl = 864000;

    // Set the Create Spec to use the StumbleUpon Package.
    // char * compress_func   = "stumbleCompress5";
    as_map *create_args = as_hashmap_new( 2, 2 );
    as_map_set( create_args,
            (as_val *) as_string_new("Package", false ),
            (as_val *) as_string_new("StumbleUpon", false ));


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

    return aerospike_lstack_push_with_create(
            cluster, ns, set, key, lso_bin_name, (as_val *)listp,
            create_args, wp.timeout_ms);
    as_list_destroy(listp);
    as_list_destroy(create_args);
}

// ==================================================================
TEST( aerospike_lstack_push_1, "aerospike_lstack_push_with_create" ) {
    char * user_key     = "User_111";
    char * lso_bin_name = "number_stack";

    int rc = lso_push_quintuplet("test", "unit", user_key, lso_bin_name, 5);
    assert_int_eq( rc, 0 );
}

// ==================================================================
static int lso_peek_quintuplet(char * ns, char * set,
                               char * key, char * lso_bin_name) {
    extern cl_cluster * cluster;

    cl_write_parameters wp;
    cl_write_parameters_set_default(&wp);
    wp.timeout_ms = 1000;
    wp.record_ttl = 864000;

    // char * uncompress_func = "stumbleUnCompress5";
    // as_list *uncompress_args = as_arraylist_new( 1, 1 );
    // as_list_add_integer( uncompress_args, 1 ); // dummy argument

    int peek_count = 1;
    as_result * resultp = aerospike_lstack_peek(
            cluster, ns, set, key, lso_bin_name, peek_count, wp.timeout_ms);
    if ( resultp ) {
        int issuccess = resultp->is_success;
        return resultp->is_success;
    }
    else {
        return false;
    }
}

TEST( aerospike_lstack_peek_1, "aerospike_lstack_peek" ) {
    char * user_key     = "User_111";
    char * lso_bin_name = "number_stack";

    int rc = lso_peek_quintuplet("test", "unit", user_key, lso_bin_name);
    assert_int_eq( rc, 1 );
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

    suite_add( aerospike_lstack_push_1 );
    suite_add( aerospike_lstack_peek_1 );
}
