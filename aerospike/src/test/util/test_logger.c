/**
 * An as_aerospike for tests
 */

#include "../test.h"
#include "test_logger.h"

/*****************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/

static int test_logger_is_enabled(const as_logger *, const as_logger_level);
static as_logger_level test_logger_get_level(const as_logger *);
static int test_logger_log(const as_logger *, const as_logger_level, const char *, const int, const char *, va_list);

/*****************************************************************************
 * CONSTANTS
 *****************************************************************************/

test_logger_context test_logger = {
    .level = AS_LOGGER_LEVEL_INFO
};

static const as_logger_hooks test_logger_hooks = {
    .destroy    = NULL,
    .enabled    = test_logger_is_enabled,
    .level      = test_logger_get_level,
    .log        = test_logger_log
};

static const char * log_level_string[5] = {
    [AS_LOGGER_LEVEL_ERROR]  = "ERROR",
    [AS_LOGGER_LEVEL_WARN]   = "WARN",
    [AS_LOGGER_LEVEL_INFO]   = "INFO",
    [AS_LOGGER_LEVEL_DEBUG]  = "DEBUG",
    [AS_LOGGER_LEVEL_TRACE]  = "TRACE",
};

/*****************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/

as_logger * test_logger_new() {
    return as_logger_new(&test_logger, &test_logger_hooks);
}

as_logger * test_logger_init(as_logger * l) {
    return as_logger_init(l, &test_logger, &test_logger_hooks);
}

static int test_logger_is_enabled(const as_logger * logger, const as_logger_level level) {
    return test_logger.level <= level;
}

static as_logger_level test_logger_get_level(const as_logger * logger) {
    return test_logger.level;
}

static int test_logger_log(const as_logger * logger, const as_logger_level level, const char * file, const int line, const char * format, va_list args) {
    if ( test_logger.level > level ) return 0;
    
    char message[1024] = { '\0' };
    vsnprintf(message, 1024, format, args);
    atf_log_line(stderr, log_level_string[level], ATF_LOG_PREFIX, file, line, message);
    return 0;
}


