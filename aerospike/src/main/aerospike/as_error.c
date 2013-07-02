#include <aerospike/as_error.h>
#include <stdarg.h>

extern inline as_error * as_error_init(as_error * err);

extern inline as_status as_error_reset(as_error * err);

extern inline as_status as_error_setall(as_error * err, as_status code, const char * message, const char * func, const char * file, uint32_t line);

extern inline as_status as_error_setallv(as_error * err, as_status code, const char * func, const char * file, uint32_t line, const char * fmt, ...);

extern inline as_status as_error_set(as_error * err, as_status code, const char * fmt, ...);
