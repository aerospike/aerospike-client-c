
#include "../test.h"
#include <citrusleaf/citrusleaf.h>

/******************************************************************************
 * MACROS
 *****************************************************************************/

#define KB 1024
#define MB 1024 * 1024
#define GB 1024 * 1024 * 1024
#define STRING_MAX 1024 * 1024

/******************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/

static int put_string(const char * ns, const char * set, const char * key, const char * name, const char * value) {

    extern cl_cluster * cluster;

    cl_write_parameters wp;
    cl_write_parameters_set_default(&wp);
    wp.timeout_ms = 1000;
    wp.record_ttl = 864000;

    cl_object okey;
    citrusleaf_object_init_str(&okey, key);

    cl_bin bins[1];
    strcpy(bins[0].bin_name, name);
    citrusleaf_object_init_str(&bins[0].object, value);

    return citrusleaf_put(cluster, ns, set, &okey, bins, 1, &wp);
}

/******************************************************************************
 * TEST CASES
 *****************************************************************************/

TEST( kv_string_lengths, "test string lengths" ) {

    char value[STRING_MAX] = { [0 ... STRING_MAX-2] = 'a', [STRING_MAX-1] = '\0' };

    for ( int i = 0; i < STRING_MAX; i += 1024*10 ) {
        char name[16] = { [0 ... 15] = '\0' };
        sprintf(name,"%d",i);
        value[i] = '\0';
        int rc = put_string("test", "test", "string_lengths", name, value);
        assert_int_eq( rc, 0 );
        value[i] = 'a';
    }
}

TEST( kv_string_put, "put a string in a bin" ) {
    char name[16] = "string";
    char value[1024] = {[0 ... 1022] = 'a', [1023] = '\0'};
    int rc = put_string("test", "test", "string_bin", name, value);
    assert_int_eq( rc, 0 );
}

TEST( kv_string_get, "get a string from a bin" ) {

    char name[16] = "string";
    char value[1024] = {[0 ... 1022] = 'a', [1023] = '\0'};

    extern cl_cluster * cluster;

    cl_object key;
    citrusleaf_object_init_str(&key, "string_bin");

    cl_bin *    bins = NULL;
    int         nbins = 0;
    uint32_t    gen = 0;

    int rc = citrusleaf_get_all(cluster, "test", "test", &key, &bins, &nbins, 1000, &gen);

    assert_int_eq(rc, 0);
    assert_int_eq(nbins, 1);
    assert( strcmp(bins[0].bin_name, name) == 0 );
    assert_int_eq( bins[0].object.type, CL_STR );
    assert( strcmp(bins[0].object.u.str, value) == 0 );
    citrusleaf_object_free(&bins[0].object);
    free(bins);
}

/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

SUITE( kv_string, "test client handling of string" ) {
    suite_add( kv_string_put );
    suite_add( kv_string_get );
    suite_add( kv_string_lengths );
}
