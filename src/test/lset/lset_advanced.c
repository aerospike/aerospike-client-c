
// #include "../test.h"
// #include "../util/udf.h"
// #include <citrusleaf/citrusleaf.h>
// #include <citrusleaf/as_types.h>
// #include <citrusleaf/aerospike_lset.h>

#include "lset_test.h"

// LSET ADVANCED
// In the LSET ATF, define the tests for the very basic operations:
// (*) advanced insert_compact()
// (*) advanced insert_objects()
// (*) advanced insert_objects_compact()
// (*) advanced search_compact()
// (*) advanced search_objects()
// (*) advanced search_objects_and_filter()
// (*) advanced search_objects_compact_and_filter()

static char * MOD = "lset_advanced.c::13_04_26";
/******************************************************************************
 * TEST CASES
 *****************************************************************************/

// ++=====================++
// || ADVANCED OPERATIONS ||
// ++=====================++

TEST( lset_advanced_insert_compact, "lset Adv insert compact" ) {
    static char * meth = "lset_advanced_insert_compact";
    int rc = 0;
    printf("Test(%s) called\n", meth );
    char * user_key   = "User_111";
    char * ldt_bin   = "num_adv";

        int    iterations = 100;

    // TODO: Fill in Function
//    rc =  lset_insert_with_transform_test(user_key, ldt_bin, iterations); 
    assert_int_eq( rc, 0 );
} // end lset_advanced_insert_compact

TEST( lset_advanced_insert_objects, "lset Adv insert objects" ) {
    static char * meth = "lset_advanced_insert_objects()";
    int rc = 0;
    printf("Test(%s) called\n", meth );

    // TODO: Fill in Function
    
    assert_int_eq( rc, 0 );
} // end   lset_advanced_insert_objects()

TEST( lset_advanced_insert_objects_compact, "lset adv insert Obj Compact" ) {
    static char * meth = "lset_advanced_insert_objects_compact()";
    int rc = 0;
    printf("Test(%s) called\n", meth );

    // TODO: Fill in Function
    
    assert_int_eq( rc, 0 );
} // end   lset_advanced_insert_objects_compact()


TEST( lset_advanced_search_compact, "lset Adv search compact" ) {
    static char * meth = "lset_advanced_search_compact()";
    int rc = 0;
    printf("Test(%s) called\n", meth );
    char * user_key   = "User_111";
    char * ldt_bin   = "num_adv";

    // TODO: Fill in Function
//    rc = lset_search_with_transform_test(user_key, ldt_bin, NULL, NULL, 10 );    
    assert_int_eq( rc, 0 );
} // end   lset_advanced_search_compact()

TEST( lset_advanced_search_objects, "lset Adv search obj" ) {
    static char * meth = "lset_advanced_search_objects()";
    int rc = 0;
    printf("Test(%s) called\n", meth );

    // TODO: Fill in Function
    
    assert_int_eq( rc, 0 );
} // end   lset_advanced_search_objects()

TEST( lset_advanced_search_objects_and_filter, "lset Adv search Obj & Filter" ) {
    static char * meth = "lset_advanced_search_objects_and_filter()";
    int rc = 0;
    printf("Test(%s) called\n", meth );

    // TODO: Fill in Function
    
    assert_int_eq( rc, 0 );
} // end   lset_advanced_search_objects_and_filter()

TEST( lset_advanced_search_objects_compact_and_filter,
        "lset Adv search Obj Compact & Filter" )
{
    static char * meth = "lset_advanced_search_objects_compact_and_filter()";
    int rc = 0;
    printf("Test(%s) called\n", meth );

    // TODO: Fill in Function
    
    assert_int_eq( rc, 0 );
} // end   lset_advanced_search_objects_compact_and_filter()

