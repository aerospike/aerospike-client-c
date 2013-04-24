#pragma once

/**
 * An as_logger for tests.
 */

#include <as_logger.h>

/*****************************************************************************
 * FUNCTIONS
 *****************************************************************************/

struct test_logger_context_s;
typedef struct test_logger_context_s test_logger_context;

struct test_logger_context_s {
    as_log_level level;
};

/*****************************************************************************
 * VARIABLES
 *****************************************************************************/

extern test_logger_context test_logger;

/*****************************************************************************
 * FUNCTIONS
 *****************************************************************************/

as_logger * test_logger_new();
as_logger * test_logger_init(as_logger *);