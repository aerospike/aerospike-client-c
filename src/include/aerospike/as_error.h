/*
 * Copyright 2008-2024 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
#pragma once 

#include <aerospike/as_std.h>
#include <aerospike/as_status.h>
#include <aerospike/as_string.h>

#include <stdarg.h>
#include <stdio.h>
#include <string.h>

#ifdef __cplusplus
extern "C" {
#endif

//---------------------------------
// Definitions
//---------------------------------

/**
 * The size of as_error.message
 */
#define AS_ERROR_MESSAGE_MAX_SIZE 	1024

/**
 * The maximum string length of as_error.message
 */
#define AS_ERROR_MESSAGE_MAX_LEN 	(AS_ERROR_MESSAGE_MAX_SIZE - 1)

//---------------------------------
// Types
//---------------------------------

/**
 * All operations that interact with the Aerospike cluster accept an as_error
 * argument and return an as_status value. The as_error argument is populated
 * with information about the error that occurred. The as_status return value
 * is the as_error.code value.
 *
 * When an operation succeeds, the as_error.code value is usually set to 
 * `AEROSPIKE_OK`. There are some operations which may have other success 
 * status codes, so please review each operation for information on status 
 * codes.
 *
 * When as_error.code is not a success value (`AEROSPIKE_OK`), then you can 
 * expect the other fields of as_error.code to be populated.
 *
 * Example usage:
 * ~~~~~~~~~~{.c}
 * as_error err;
 *
 * if ( aerospike_key_get(&as, &err, NULL, &key, &rec) != AEROSPIKE_OK ) {
 * 	fprintf(stderr, "(%d) %s at %s[%s:%d]\n", error.code, err.message, err.func, err.file. err.line);
 * }
 * ~~~~~~~~~~
 *
 * You can reuse an as_error with multiple operations. Each operation 
 * internally resets the error. So, if an error occurred in one operation,
 * and you did not check it, then the error will be lost with subsequent 
 * operations.
 *
 * Example usage:
 *
 * ~~~~~~~~~~{.c}
 * as_error err;
 *
 * if ( aerospike_key_put(&as, &err, NULL, &key, rec) != AEROSPIKE_OK ) {
 * 	fprintf(stderr, "(%d) %s at %s[%s:%d]\n", error.code, err.message, err.func, err.file. err.line);
 * }
 *
 * if ( aerospike_key_get(&as, &err, NULL, &key, &rec) != AEROSPIKE_OK ) {
 * 	fprintf(stderr, "(%d) %s at %s[%s:%d]\n", error.code, err.message, err.func, err.file. err.line);
 * }
 * ~~~~~~~~~~
 *
 * @ingroup client_objects
 */
typedef struct as_error_s {

	/**
	 * Numeric error code
	 */
	as_status code;

	/**
	 * NULL-terminated error message
	 */
	char message[AS_ERROR_MESSAGE_MAX_SIZE];

	/**
	 * Name of the function where the error occurred.
	 */
	const char * func;

	/**
	 * Name of the file where the error occurred.
	 */
	const char * file;

	/**
	 * Line in the file where the error occurred.
	 */
	uint32_t line;

	/**
	 * Is it possible that the write command completed even though this error was generated.
	 * This may be the case when a client error occurs (like timeout) after the command was sent
	 * to the server.
	 */
	bool in_doubt;

} as_error;

//---------------------------------
// Macros
//---------------------------------

/**
 * Set all as_error fields and default in_doubt to false. Variable arguments are accepted.
 *
 * @relates as_error
 */
#define as_error_update(__err, __code, __fmt, ...) \
	as_error_setallv( __err, __code, __func__, __FILE__, __LINE__, __fmt, ##__VA_ARGS__ );

/**
 * Set all as_error fields and default in_doubt to false. Variable arguments are not accepted.
 *
 * @relates as_error
 */
#define as_error_set_message(__err, __code, __msg) \
	as_error_setall( __err, __code, __msg, __func__, __FILE__, __LINE__ );

//---------------------------------
// Functions
//---------------------------------

/**
 * Initialize the error to default (empty) values, returning the error.
 *
 * @param err The error to initialize.
 *
 * @returns The initialized err.
 *
 * @relates as_error
 */
static inline as_error*
as_error_init(as_error* err)
{
	err->code = AEROSPIKE_OK;
	err->message[0] = '\0';
	err->func = NULL;
	err->file = NULL;
	err->line = 0;
	err->in_doubt = false;
	return err;
}

/**
 * Resets the error to default (empty) values, returning the status code.
 *
 * @param err The error to reset.
 *
 * @returns AEROSPIKE_OK.
 *
 * @relates as_error
 */
static inline as_status
as_error_reset(as_error* err)
{
	err->code = AEROSPIKE_OK;
	err->message[0] = '\0';
	err->func = NULL;
	err->file = NULL;
	err->line = 0;
	err->in_doubt = false;
	return err->code;
}

/**
 * Sets the error.
 *
 * @return The status code set for the error.
 *
 * @relates as_error
 */
static inline as_status
as_error_setall(as_error* err, as_status code, const char * message, const char * func, const char * file, uint32_t line)
{
	err->code = code;
	as_strncpy(err->message, message, AS_ERROR_MESSAGE_MAX_SIZE);
	err->func = func;
	err->file = file;
	err->line = line;
	err->in_doubt = false;
	return err->code;
}

/**
 * Sets the error.
 *
 * @return The status code set for the error.
 *
 * @relates as_error
 */
static inline as_status
as_error_setallv(as_error* err, as_status code, const char * func, const char * file, uint32_t line, const char * fmt, ...)
{
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
	err->in_doubt = false;
	return err->code;
}

/**
 * Set whether it is possible that the write command may have completed
 * even though this exception was generated.  This may be the case when a
 * client error occurs (like timeout) after the command was sent to the server.
 *
 * @relates as_error
 */
static inline void
as_error_set_in_doubt(as_error* err, bool is_read, uint32_t command_sent_counter)
{
	err->in_doubt = (!is_read && (command_sent_counter > 1 || (command_sent_counter == 1 &&
					(err->code == AEROSPIKE_ERR_TIMEOUT || err->code <= 0))));
}

/**
 * Copy error from source to target.
 *
 * @relates as_error
 */
static inline void
as_error_copy(as_error * trg, const as_error * src)
{
	trg->code = src->code;
	strcpy(trg->message, src->message);
	trg->func = src->func;
	trg->file = src->file;
	trg->line = src->line;
	trg->in_doubt = src->in_doubt;
}

/**
 * Append string to error message.
 *
 * @relates as_error
 */
static inline void
as_error_append(as_error* err, const char* str)
{
	strncat(err->message, str, sizeof(err->message) - strlen(err->message) - 1);
}

/**
 * Return string representation of error code.  Result should not be freed.
 *
 * @relates as_error
 */
AS_EXTERN char*
as_error_string(as_status status);

#ifdef __cplusplus
} // end extern "C"
#endif
