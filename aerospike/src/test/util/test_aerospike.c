/**
 * An as_aerospike for tests
 */

#include "../test.h"
#include "test_aerospike.h"

/*****************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/

static int test_aerospike_log(const as_aerospike * as, const char * file, const int line, const int level, const char * msg);

/*****************************************************************************
 * CONSTANTS
 *****************************************************************************/

static const as_aerospike_hooks test_aerospike_hooks = {
    .destroy = NULL,
    .rec_create = NULL,
    .rec_update = NULL,
    .rec_remove = NULL,
    .rec_exists = NULL,
    .log = test_aerospike_log,
};

/*****************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/

as_aerospike * test_aerospike_new() {
    return as_aerospike_new(NULL, &test_aerospike_hooks);
}

as_aerospike * test_aerospike_init(as_aerospike * a) {
    return as_aerospike_init(a, NULL, &test_aerospike_hooks);
}

static int test_aerospike_log(const as_aerospike * as, const char * file, const int line, const int level, const char * msg) {
    char l[10] = {'\0'};
    switch(level) {
        case 1:
            strncpy(l,"WARN",10);
            break;
        case 2:
            strncpy(l,"INFO",10);
            break;
        case 3:
            strncpy(l,"DEBUG",10);
            break;
        default:
            strncpy(l,"TRACE",10);
            break;
    }
    atf_log_line(stderr, l, ATF_LOG_PREFIX, file, line, msg);
    return 0;
}


