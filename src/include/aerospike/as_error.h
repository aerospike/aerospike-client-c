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

#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

/******************************************************************************
 *	MACROS
 *****************************************************************************/

/**
 * The size of as_error.message
 *
 *	@ingroup as_error_object
 */
#define AS_ERROR_MESSAGE_MAX_SIZE 	1024

/**
 * The maximum string length of as_error.message
 *
 *	@ingroup as_error_object
 */
#define AS_ERROR_MESSAGE_MAX_LEN 	(AS_ERROR_MESSAGE_MAX_SIZE - 1)

/******************************************************************************
 *	TYPES
 *****************************************************************************/

/**
 *	All operations that interact with the Aerospike cluster accept an as_error
 *	argument and return an as_status value. The as_error argument is populated
 *	with information about the error that occurred. The as_status return value
 *	is the as_error.code value.
 *
 *	When an operation succeeds, the as_error.code value is usually set to 
 *	`AEROSPIKE_OK`. There are some operations which may have other success 
 *	status codes, so please review each operation for information on status 
 *	codes.
 *
 *	When as_error.code is not a success value (`AEROSPIKE_OK`), then you can 
 *	expect the other fields of as_error.code to be populated.
 *
 *	Example usage:
 *	~~~~~~~~~~{.c}
 *	as_error err;
 *
 *	if ( aerospike_key_get(&as, &err, NULL, &key, &rec) != AEROSPIKE_OK ) {
 *		fprintf(stderr, "(%d) %s at %s[%s:%d]\n", error.code, err.message, err.func, err.file. err.line);
 *	}
 *	~~~~~~~~~~
 *
 *	You can reuse an as_error with multiple operations. Each operation 
 *	internally resets the error. So, if an error occurred in one operation,
 *	and you did not check it, then the error will be lost with subsequent 
 *	operations.
 *
 *	Example usage:
 *
 *	~~~~~~~~~~{.c}
 *	as_error err;
 *
 *	if ( aerospike_key_put(&as, &err, NULL, &key, rec) != AEROSPIKE_OK ) {
 *		fprintf(stderr, "(%d) %s at %s[%s:%d]\n", error.code, err.message, err.func, err.file. err.line);
 *	}
 *
 *	if ( aerospike_key_get(&as, &err, NULL, &key, &rec) != AEROSPIKE_OK ) {
 *		fprintf(stderr, "(%d) %s at %s[%s:%d]\n", error.code, err.message, err.func, err.file. err.line);
 *	}
 *	~~~~~~~~~~
 *
 *	@ingroup client_objects
 */
typedef struct as_error_s {

	/**
	 *	Numeric error code
	 */
	as_status code;

	/**
	 *	NULL-terminated error message
	 */
	char message[AS_ERROR_MESSAGE_MAX_SIZE];

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
 *
 *	@ingroup as_error_object
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
 *
 *	@relates as_error
 *	@ingroup as_error_object
 */
static inline as_error * as_error_init(as_error * err) {
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
 *
 *	@relates as_error
 *	@ingroup as_error_object
 */
static inline as_status as_error_reset(as_error * err) {
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
 *
 *	@relates as_error
 */
static inline as_status as_error_setall(as_error * err, as_status code, const char * message, const char * func, const char * file, uint32_t line) {
	err->code = code;
	strncpy(err->message, message, AS_ERROR_MESSAGE_MAX_LEN);
	err->message[AS_ERROR_MESSAGE_MAX_LEN] = '\0';
	err->func = func;
	err->file = file;
	err->line = line;
	return err->code;
}

/**
 *	Sets the error.
 *
 *	@return The status code set for the error.
 *
 *	@relates as_error
 */
static inline as_status as_error_setallv(as_error * err, as_status code, const char * func, const char * file, uint32_t line, const char * fmt, ...) {
	if ( fmt != NULL ) {
		va_list ap;
		va_start(ap, fmt);
		vsnprintf(err->message, AS_ERROR_MESSAGE_MAX_LEN, fmt, ap);
		err->message[AS_ERROR_MESSAGE_MAX_LEN] = '\0';
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
 *
 *	@relates as_error
 */
static inline as_status as_error_set(as_error * err, as_status code, const char * fmt, ...) {
	if ( fmt != NULL ) {
		va_list ap;
		va_start(ap, fmt);
		vsnprintf(err->message, AS_ERROR_MESSAGE_MAX_LEN, fmt, ap);
		err->message[AS_ERROR_MESSAGE_MAX_LEN] = '\0';
		va_end(ap);   
	}
	err->code = code;
	return err->code;
}
