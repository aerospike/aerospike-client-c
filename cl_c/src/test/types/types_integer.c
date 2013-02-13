
#include "../test.h"
#include <citrusleaf/as_integer.h>
#include <citrusleaf/as_string.h>
#include <limits.h>

/******************************************************************************
 * TEST CASES
 *****************************************************************************/

TEST( types_integer_0, "as_integer containing 0" ) {
    as_integer i;
    as_integer_init(&i,0);
    assert( as_integer_toint(&i) == 0 );
    as_val_destroy(&i);
}

TEST( types_integer_pos_1, "as_integer containing 1" ) {
    as_integer i;
    as_integer_init(&i,1);
    assert( as_integer_toint(&i) == 1 );
    as_val_destroy(&i);
}

TEST( types_integer_neg_1, "as_integer containing -1" ) {
    as_integer i;
    as_integer_init(&i,1);
    assert( as_integer_toint(&i) == 1 );
}

TEST( types_integer_ulong_max, "as_string containing ULONG_MAX" ) {
    as_integer i;
    as_integer_init(&i,ULONG_MAX);
    assert( as_integer_toint(&i) == ULONG_MAX );
}

TEST( types_integer_long_max, "as_string containing LONG_MAX" ) {
    as_integer i;
    as_integer_init(&i,LONG_MAX);
    assert( as_integer_toint(&i) == LONG_MAX );
}

TEST( types_integer_long_min, "as_string containing LONG_MIN" ) {
    as_integer i;
    as_integer_init(&i,LONG_MIN);
    assert( as_integer_toint(&i) == LONG_MIN );
}

/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

SUITE( types_integer, "as_integer" ) {
    suite_add( types_integer_0 );
    suite_add( types_integer_pos_1 );
    suite_add( types_integer_neg_1 );
    suite_add( types_integer_ulong_max );
    suite_add( types_integer_long_max );
    suite_add( types_integer_long_min );
}