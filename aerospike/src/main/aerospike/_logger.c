#include <aerospike/aerospike.h>
#include <aerospike/as_logger.h>
#include <aerospike/as_log.h>

#include "_log.h"

static const as_logger_level aerospike_log_level_tologger[] = {
	[AS_LOG_LEVEL_ERROR]	= AS_LOGGER_LEVEL_ERROR,
	[AS_LOG_LEVEL_WARN]		= AS_LOGGER_LEVEL_WARN,
	[AS_LOG_LEVEL_INFO]		= AS_LOGGER_LEVEL_INFO,
	[AS_LOG_LEVEL_DEBUG]	= AS_LOGGER_LEVEL_DEBUG,
	[AS_LOG_LEVEL_TRACE]	= AS_LOGGER_LEVEL_TRACE
};

static const as_log_level aerospike_logger_level_tolog[] = {
	[AS_LOGGER_LEVEL_TRACE]	= AS_LOG_LEVEL_TRACE,
	[AS_LOGGER_LEVEL_DEBUG]	= AS_LOG_LEVEL_DEBUG,
	[AS_LOGGER_LEVEL_INFO]	= AS_LOG_LEVEL_INFO,
	[AS_LOGGER_LEVEL_WARN]	= AS_LOG_LEVEL_WARN,
	[AS_LOGGER_LEVEL_ERROR]	= AS_LOG_LEVEL_ERROR
};

static int aerospike_logger_destroy(as_logger * logger)
{
	return 0;
}

/**
 * Test if the log level is enabled for the logger.
 */
static int aerospike_logger_enabled(const as_logger * logger, const as_logger_level level)
{
	aerospike * as = (aerospike * ) logger->source;
	return aerospike_logger_level_tolog[level] <= as->log.level;
}

/**
 * Get the current log level of the logger.
 */
static as_logger_level aerospike_logger_level(const as_logger * logger)
{
	aerospike * as = (aerospike * ) logger->source;
	return aerospike_log_level_tologger[as->log.level];
}

/**
 * Log a message using the logger.
 */
static int aerospike_logger_log(const as_logger * logger, const as_logger_level level, const char * file, const int line, const char * fmt, va_list ap)
{
	aerospike * as = (aerospike * ) logger->source;

	if ( aerospike_logger_level_tolog[level] <= as->log.level ) {
		char msg[1024] = {0};
		vsnprintf(msg, 1024 - 1, fmt, ap);
		msg[1024 - 1] = '\0';

		as_log_level l = aerospike_logger_level_tolog[level];
		as_log_callback callback = (as_log_callback) as->log.callback;
		callback(l, NULL, file, line, msg);
	}
	return 0;
}

static const as_logger_hooks aerospike_logger_hooks = {
	.destroy = aerospike_logger_destroy,
	.enabled = aerospike_logger_enabled,
	.level = aerospike_logger_level,
	.log = aerospike_logger_log
};


as_logger * aerospike_logger(aerospike * as)
{
	return as_logger_new(as, &aerospike_logger_hooks);
}

