
#include <aerospike/aerospike.h>
#include <aerospike/aerospike_index.h>

#include <aerospike/as_error.h>
#include <aerospike/as_status.h>

#include <aerospike/as_record.h>
#include <aerospike/as_integer.h>
#include <aerospike/as_string.h>
#include <aerospike/as_list.h>
#include <aerospike/as_arraylist.h>
#include <aerospike/as_map.h>
#include <aerospike/as_hashmap.h>
#include <aerospike/as_val.h>

#include "../test.h"
#include "../unittest.h"


/******************************************************************************
 * GLOBAL VARS
 *****************************************************************************/

extern aerospike * as;

/******************************************************************************
 * TYPES
 *****************************************************************************/


/******************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/


/******************************************************************************
 * TEST CASES
 *****************************************************************************/

TEST( index_basics_create , "Create index on bin" ) {

    as_error err;
    as_error_reset(&err);

    aerospike_index_string_create(as, &err, NULL, TEST_NAMESPACE, SET, "new_bin", "idx_test_new_bin");
    if ( err.code != AEROSPIKE_OK && err.code != AEROSPIKE_ERR_INDEX_FOUND ) {
        info("error(%d): %s", err.code, err.message);
    }
    assert_int_eq( err.code , AEROSPIKE_OK );

}

TEST( index_basics_create_numeric , "Create index on numeric bin" )
{

    as_error err;
    as_error_reset(&err);

    aerospike_index_integer_create(as, &err, NULL, TEST_NAMESPACE, SET, "new_number__bin", "idx_test_new_number_bin");

    if ( err.code != AEROSPIKE_OK && err.code != AEROSPIKE_ERR_INDEX_FOUND )
    {
        info("error(%d): %s", err.code, err.message);
    }

    assert_int_eq( err.code , AEROSPIKE_OK );

    aerospike_index_remove(as, &err, NULL, TEST_NAMESPACE, "idx_test_new_number_bin");

    if ( err.code != AEROSPIKE_OK )
    {
        info("error(%d): %s", err.code, err.message);
    }

    assert_int_eq( err.code, AEROSPIKE_OK );

}

TEST( index_basics_create_bad_params , "Create index on bad parameters" )
{

    as_error err;
    as_error_reset(&err);

    aerospike_index_integer_create(as, &err, NULL, TEST_NAMESPACE, SET, 999, 999);

    if ( err.code == AEROSPIKE_OK && err.code == AEROSPIKE_ERR_INDEX_FOUND )
    {
        info("Error: No Index should be returned for bad parameters");
    }

    assert_int_ne( err.code , AEROSPIKE_OK );
}

TEST( index_basics_create_null , "Create index using null values" )
{

    as_error err;
    as_error_reset(&err);

    aerospike_index_string_create(as, &err, NULL, NULL, NULL, NULL, NULL);

    if ( err.code == AEROSPIKE_OK && err.code == AEROSPIKE_ERR_INDEX_FOUND )
    {
        info("Error: No Index should be returned for null index name");
    }

    assert_int_ne( err.code , AEROSPIKE_OK );

}

TEST( index_basics_drop , "Drop index" ) {

    as_error err;
    as_error_reset(&err);

    aerospike_index_remove(as, &err, NULL, TEST_NAMESPACE, "idx_test_new_bin");
    if ( err.code != AEROSPIKE_OK ) {
        info("error(%d): %s", err.code, err.message);
    }
    assert_int_eq( err.code, AEROSPIKE_OK );

}


/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

SUITE( index_basics, "aerospike_sindex basic tests" ) {
    suite_add( index_basics_create );
    suite_add( index_basics_drop );

    suite_add( index_basics_create_numeric );
    suite_add( index_basics_create_null );
//    suite_add( index_basics_create_bad_params );
}

