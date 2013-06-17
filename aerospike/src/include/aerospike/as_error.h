/******************************************************************************
 *	Copyright 2008-2013 by Aerospike.
 *
 *	Permission is hereby granted, free of charge, to any person obtaining a copy 
 *	of this software and associated documentation files (the "Software"), to 
 *	deal in the Software without restriction, including without limitation the 
 *	rights to use, copy, modify, merge, publish, distribute, sublicense, and/or 
 *	sell copies of the Software, and to permit persons to whom the Software is 
 *	furnished to do so, subject to the following conditions:
 *	
 *	The above copyright notice and this permission notice shall be included in 
 *	all copies or substantial portions of the Software.
 *	
 *	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING 
 *	FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 *	IN THE SOFTWARE.
 *****************************************************************************/

#pragma once 

#include <aerospike/as_status.h>

#include <inttypes.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>

/******************************************************************************
 *	TYPES
 *****************************************************************************/

/**
 *	Contains information about an error
 */
typedef struct as_error_s {

	/**
	 *	Numeric error code
	 */
	as_status code;

	/**
	 *	NULL-terminated error message
	 */
	char message[1024];

	/**
	 *	Name of the function where the error occurred.
	 */
	const char * func;

	/**
	 *	Name of the file where the error occurred.
	 */
	const char * file;

	/**
	 *	Line in the file where the error occurred.
	 */
	uint32_t line;

} as_error;

/******************************************************************************
 *	MACROS
 *****************************************************************************/

/**
 *	as_error_update(&as->error, AEROSPIKE_OK, "%s %d", "a", 1);
 */
#define as_error_update(__err, __code, __fmt, ...) \
	as_error_setallv( __err, __code, __func__, __FILE__, __LINE__, __fmt, ##__VA_ARGS__ );

/******************************************************************************
 *	FUNCTIONS
 *****************************************************************************/

/**
 *	Initialize the error to default (empty) values, returning the error.
 *
 *	@param err The error to initialize.
 *
 *	@returns The initialized err.
 */
inline as_error * as_error_init(as_error * err) {
	err->code = AEROSPIKE_OK;
	err->message[0] = '\0';
	err->func = NULL;
	err->file = NULL;
	err->line = 0;
	return err;
}

/**
 *	Resets the error to default (empty) values, returning the status code.
 *
 *	@param err The error to reset.
 *
 *	@returns AEROSPIKE_OK.
 */
inline as_status as_error_reset(as_error * err) {
	err->code = AEROSPIKE_OK;
	err->message[0] = '\0';
	err->func = NULL;
	err->file = NULL;
	err->line = 0;
	return err->code;
}

/**
 *	Sets the error.
 *
 *	@return The status code set for the error.
 */
inline as_status as_error_setall(as_error * err, int32_t code, const char * message, const char * func, const char * file, uint32_t line) {
	err->code = code;
	strncpy(err->message, message, sizeof(err->message) - 1);
	err->message[sizeof(err->message) - 1] = '\0';
	err->func = func;
	err->file = file;
	err->line = line;
	return err->code;
}

/**
 *	Sets the error.
 *
 *	@return The status code set for the error.
 */
inline as_status as_error_setallv(as_error * err, int32_t code, const char * func, const char * file, uint32_t line, const char * fmt, ...) {
	if ( fmt != NULL ) {
		va_list ap;
		va_start(ap, fmt);
		vsnprintf(err->message, sizeof(err->message) - 1, fmt, ap);
		err->message[sizeof(err->message) - 1] = '\0';
		va_end(ap);   
	}
	err->code = code;
	err->func = func;
	err->file = file;
	err->line = line;
	return err->code;
}

/**
 *	Sets the error message
 */
inline as_status as_error_set(as_error * err, int32_t code, const char * fmt, ...) {
	if ( fmt != NULL ) {
		va_list ap;
		va_start(ap, fmt);
		vsnprintf(err->message, sizeof(err->message) - 1, fmt, ap);
		err->message[sizeof(err->message) - 1] = '\0';
		va_end(ap);   
	}
	err->code = code;
	return err->code;
}
