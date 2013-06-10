
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

/******************************************************************************
 * TYPES
 *****************************************************************************/


/******************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/


/******************************************************************************
 * TEST CASES
 *****************************************************************************/

TEST( digest_apply_1 , "apply 1" ) {
	assert_not_null(NULL);
}

/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

SUITE( digest_apply, "aerospike_digest apply tests" ) {
  suite_add( digest_apply_1 );
}
