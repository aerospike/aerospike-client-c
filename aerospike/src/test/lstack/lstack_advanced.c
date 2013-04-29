
// #include "../test.h"
// #include "../util/udf.h"
// #include <citrusleaf/citrusleaf.h>
// #include <citrusleaf/as_types.h>
// #include <citrusleaf/aerospike_lstack.h>

#include "lstack_test.h"

// LSTACK ADVANCED
// In the LSTACK ATF, define the tests for the very basic operations:
// (*) advanced push_compact()
// (*) advanced push_objects()
// (*) advanced push_objects_compact()
// (*) advanced peek_compact()
// (*) advanced peek_objects()
// (*) advanced peek_objects_and_filter()
// (*) advanced peek_objects_compact_and_filter()

static char * MOD = "lstack_advanced.c::13_04_26";
/******************************************************************************
 * TEST CASES
 *****************************************************************************/

TEST( lstack_advanced_push_compact, "lstack Adv push compact" ) {
    static char * meth = "lstack_advanced_push_compact";
    printf("Test(%s) called\n", meth );

    // TODO: Fill in Function
    
    return true;
} // end lstack_advanced_push_compact

TEST( lstack_advanced_push_objects, "lstack Adv push objects" ) {
    static char * meth = "lstack_advanced_push_objects()";
    printf("Test(%s) called\n", meth );

    // TODO: Fill in Function
    
    return true;
} // end   lstack_advanced_push_objects()

TEST( lstack_advanced_push_objects_compact, "lstack adv push Obj Compact" ) {
    static char * meth = "lstack_advanced_push_objects_compact()";
    printf("Test(%s) called\n", meth );

    // TODO: Fill in Function
    
    return true;
} // end   lstack_advanced_push_objects_compact()


TEST( lstack_advanced_peek_compact, "lstack Adv peek compact" ) {
    static char * meth = "lstack_advanced_peek_compact()";
    printf("Test(%s) called\n", meth );

    // TODO: Fill in Function
    
    return true;
} // end   lstack_advanced_peek_compact()

TEST( lstack_advanced_peek_objects, "lstack Adv peek obj" ) {
    static char * meth = "lstack_advanced_peek_objects()";
    printf("Test(%s) called\n", meth );

    // TODO: Fill in Function
    
    return true;
} // end   lstack_advanced_peek_objects()

TEST( lstack_advanced_peek_objects_and_filter, "lstack Adv peek Obj & Filter" ) {
    static char * meth = "lstack_advanced_peek_objects_and_filter()";
    printf("Test(%s) called\n", meth );

    // TODO: Fill in Function
    
    return true;
} // end   lstack_advanced_peek_objects_and_filter()

TEST( lstack_advanced_peek_objects_compact_and_filter,
        "lstack Adv peek Obj Compact & Filter" )
{
    static char * meth = "lstack_advanced_peek_objects_compact_and_filter()";
    printf("Test(%s) called\n", meth );

    // TODO: Fill in Function
    
    return true;
} // end   lstack_advanced_peek_objects_compact_and_filter()

