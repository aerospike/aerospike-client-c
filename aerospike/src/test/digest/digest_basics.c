
#include "../test.h"

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_digest.h>

/******************************************************************************
 * TEST CASES
 *****************************************************************************/

TEST( digest_basics_put, "put a record" ) {
}

TEST( digest_basics_get, "get a record" ) {
}

/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

SUITE( digest_basics, "aerospike_digest tests" ) {
    suite_add( digest_basics_put );
    suite_add( digest_basics_get );
}
