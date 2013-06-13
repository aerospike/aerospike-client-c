#pragma once

/**
 * An as_logger for tests.
 */

#include <aerospike/as_logger.h>

/*****************************************************************************
 * FUNCTIONS
 *****************************************************************************/

typedef struct test_logger_context_s {
    as_logger_level level;
} test_logger_context;

/*****************************************************************************
 * VARIABLES
 *****************************************************************************/

extern test_logger_context test_logger;

/*****************************************************************************
 * FUNCTIONS
 *****************************************************************************/

as_logger * test_logger_new();
as_logger * test_logger_init(as_logger *);