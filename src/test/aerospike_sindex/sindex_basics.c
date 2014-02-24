
#include <aerospike/aerospike.h>
#include <aerospike/aerospike_scan.h>

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

/******************************************************************************
 * GLOBAL VARS
 *****************************************************************************/

extern aerospike * as;

#define NAMESPACE "test"
#define SET "test"


/******************************************************************************
 * TYPES
 *****************************************************************************/


/******************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/


/******************************************************************************
 * TEST CASES
 *****************************************************************************/

TEST( sindex_basics_create , "Create index on bin" ) {

    as_error err;
    as_error_reset(&err);
    
    aerospike_index_create(as, &err, NULL, NAMESPACE, SET, "new_bin", AS_TYPE_STR, "idx_test_new_bin");
    if ( err.code != AEROSPIKE_OK && err.code != AEROSPIKE_ERR_INDEX_EXISTS ) {
        info("error(%d): %s", err.code, err.message);
    }
    assert_int_eq( err.code , AEROSPIKE_OK );

}

TEST( sindex_basics_drop , "Drop index" ) {

    as_error err;
    as_error_reset(&err);

    aerospike_index_remove(as, &err, NULL, NAMESPACE, "idx_test_new_bin");
    if ( err.code != AEROSPIKE_OK ) {
        info("error(%d): %s", err.code, err.message);
    }
    assert_int_eq( err.code, AEROSPIKE_OK );

}

/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

SUITE( sindex_basics, "aerospike_sindex basic tests" ) {
	suite_add( sindex_basics_create );
	suite_add( sindex_basics_drop );
}
