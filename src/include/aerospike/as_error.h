/*
 * Copyright 2008-2026 Aerospike, Inc.
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
#include <aerospike/as_subcode.h>

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

/**
 * Maximum bytes stored for a trace op name, including the trailing null byte.
 *
 * Public compatibility note: the expression-trace structs below are a branch-level
 * compatibility change and intentionally grow public struct sizes on this release line.
 */
#define AS_EXP_TRACE_OP_MAX_SIZE 32

/**
 * Maximum number of path frames stored inline.
 */
#define AS_EXP_TRACE_PATH_MAX_DEPTH 16

/**
 * Maximum bytes stored for the trace snippet, including the trailing null byte.
 */
#define AS_EXP_TRACE_SNIPPET_MAX_SIZE 256

/**
 * Top-level error-detail msgpack keys returned in field 45.
 */
#define AS_ERROR_DETAIL_KEY_SUBCODE 1
#define AS_ERROR_DETAIL_KEY_MESSAGE 2
#define AS_ERROR_DETAIL_KEY_EXP_TRACE 3

/**
 * Expression-trace sub-map keys returned under AS_ERROR_DETAIL_KEY_EXP_TRACE.
 */
#define AS_EXP_TRACE_KEY_PHASE 1
#define AS_EXP_TRACE_KEY_BYTE_OFFSET 2
#define AS_EXP_TRACE_KEY_OP 3
#define AS_EXP_TRACE_KEY_DEPTH 4
#define AS_EXP_TRACE_KEY_PATH 5
#define AS_EXP_TRACE_KEY_SNIPPET 6
#define AS_EXP_TRACE_KEY_OUTCOME 7
#define AS_EXP_TRACE_KEY_LANG 8
#define AS_EXP_TRACE_KEY_AEL_OFFSET 9
#define AS_EXP_TRACE_KEY_AEL_SPAN 10

/**
 * Expression trace phase values returned by the server.
 */
typedef enum as_exp_trace_phase_e {
	AS_EXP_TRACE_PHASE_UNKNOWN = 0,
	AS_EXP_TRACE_PHASE_BUILD = 1,
	AS_EXP_TRACE_PHASE_EVAL = 2
} as_exp_trace_phase;

/**
 * Expression trace outcome values returned by the server for eval traces.
 */
typedef enum as_exp_trace_outcome_e {
	AS_EXP_TRACE_OUTCOME_UNKNOWN = 0,
	AS_EXP_TRACE_OUTCOME_FAULT = 1,
	AS_EXP_TRACE_OUTCOME_FALSE = 2,
	AS_EXP_TRACE_OUTCOME_ABSENT = 3
} as_exp_trace_outcome;

/**
 * Expression trace language values surfaced to clients.
 *
 * When the server omits key 8 (lang), the client defaults lang to
 * AS_EXP_TRACE_LANG_MSGPACK while keeping has_lang false so callers can still
 * distinguish wire presence from the effective language.
 */
typedef enum as_exp_trace_lang_e {
	AS_EXP_TRACE_LANG_UNKNOWN = 0,
	AS_EXP_TRACE_LANG_MSGPACK = 1,
	AS_EXP_TRACE_LANG_AEL = 2
} as_exp_trace_lang;

/**
 * Structured expression trace returned under error-detail key 3.
 *
 * Most fields are optional on the wire. The `has_*` and `*_size` members distinguish
 * "present" from "absent". C intentionally exposes a curated subset of the server's
 * expression-trace map with bounded inline storage. When a field is absent, the client
 * cannot tell whether the field was not applicable or omitted by the server's
 * response-size budget. The exception is lang: when key 8 is absent, has_lang remains
 * false, but lang defaults to AS_EXP_TRACE_LANG_MSGPACK for trace consumers.
 */
typedef struct as_exp_trace_s {
	bool has_phase;
	uint8_t phase;

	bool has_byte_offset;
	uint64_t byte_offset;

	bool has_op;
	char op[AS_EXP_TRACE_OP_MAX_SIZE];

	bool has_depth;
	uint16_t depth;

	bool has_path;
	uint8_t path_size;
	char path[AS_EXP_TRACE_PATH_MAX_DEPTH][AS_EXP_TRACE_OP_MAX_SIZE];

	bool has_snippet;
	char snippet[AS_EXP_TRACE_SNIPPET_MAX_SIZE];

	bool has_outcome;
	uint8_t outcome;

	/**
	 * True if the server explicitly returned key 8 (lang).
	 * When false, lang still defaults to AS_EXP_TRACE_LANG_MSGPACK for trace consumers.
	 */
	bool has_lang;
	uint8_t lang;

	bool has_ael_offset;
	uint32_t ael_offset;

	bool has_ael_span;
	uint32_t ael_span;
} as_exp_trace;

/**
 * Server-authored error detail payload used by batch row results.
 *
 * `message` surfaces the same user-facing text as `as_error.message`. Use `has_message`,
 * `has_subcode`, and `has_exp_trace` to determine which wire fields were actually present.
 */
typedef struct as_error_detail_s {
	bool has_subcode;
	uint32_t subcode;

	bool has_message;
	bool has_exp_trace;
	as_exp_trace exp_trace;

	char message[AS_ERROR_MESSAGE_MAX_SIZE];
} as_error_detail;

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
 * @code
 * as_error err;
 *
 * if ( aerospike_key_get(&as, &err, NULL, &key, &rec) != AEROSPIKE_OK ) {
 * 	fprintf(stderr, "(%d) %s at %s[%s:%d]\n", error.code, err.message, err.func, err.file. err.line);
 * }
 * @endcode
 *
 * You can reuse an as_error with multiple operations. Each operation 
 * internally resets the error. So, if an error occurred in one operation,
 * and you did not check it, then the error will be lost with subsequent 
 * operations.
 *
 * Example usage:
 *
 * @code
 * as_error err;
 *
 * if ( aerospike_key_put(&as, &err, NULL, &key, rec) != AEROSPIKE_OK ) {
 * 	fprintf(stderr, "(%d) %s at %s[%s:%d]\n", error.code, err.message, err.func, err.file. err.line);
 * }
 *
 * if ( aerospike_key_get(&as, &err, NULL, &key, &rec) != AEROSPIKE_OK ) {
 * 	fprintf(stderr, "(%d) %s at %s[%s:%d]\n", error.code, err.message, err.func, err.file. err.line);
 * }
 * @endcode
 *
 * @ingroup client_objects
 */
typedef struct as_error_s {

	/**
	 * Numeric error code
	 */
	as_status code;

	/**
	 * Server error detail subcode. When error_detail_verbosity >= 1 on the request policy
	 * and the server returns structured error details, this field contains the numeric subcode.
	 * Use has_subcode to distinguish "subcode 0" from "subcode absent".
	 */
	uint32_t subcode;

	/**
	 * True if error detail field 45 key 1 (subcode) was present.
	 */
	bool has_subcode;

	/**
	 * True if error detail field 45 key 2 (message) was present.
	 * The surfaced message text still lives in message[] and may later be replaced by
	 * fallback or UDF text without changing this wire-presence bit.
	 */
	bool has_message;

	/**
	 * True if error detail field 45 key 3 contained at least one client-exposed
	 * expression trace field after reserved/unknown trace keys were dropped.
	 */
	bool has_exp_trace;

	/**
	 * Curated structured expression trace returned when error_detail_verbosity >= 3.
	 * Absent optional fields remain zeroed with their corresponding has_* flag unset.
	 */
	as_exp_trace exp_trace;

	/**
	 * NULL-terminated user-facing error message.
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

/**
 * If the error already has server detail, update code/in_doubt/location fields only.
 * Otherwise, set a default message from the error code string.
 *
 * @relates as_error
 */
#define as_error_update_status(__err, __code) \
	do { \
		if (as_error_has_server_detail(__err)) { \
			if ((__err)->message[0] == '\0') { \
				as_error_set_message_preserving_server_detail((__err), (__code), \
					as_error_string(__code), __func__, __FILE__, __LINE__); \
			} \
			else { \
				as_error_update_server_detail((__err), (__code), __func__, __FILE__, __LINE__); \
			} \
		} \
		else { \
			as_error_set_message((__err), (__code), as_error_string(__code)); \
		} \
	} while(0)

/**
 * If the error already has server detail, update code/in_doubt/location fields only.
 * Otherwise, set a message containing the node address and error code string.
 *
 * @relates as_error
 */
#define as_error_update_address(__err, __code, __address) \
	do { \
		if (as_error_has_server_detail(__err)) { \
			if ((__err)->message[0] == '\0') { \
				as_error_set_messagev_preserving_server_detail((__err), (__code), \
					__func__, __FILE__, __LINE__, "%s %s", (__address), as_error_string(__code)); \
			} \
			else { \
				as_error_update_server_detail((__err), (__code), __func__, __FILE__, __LINE__); \
			} \
		} \
		else { \
			as_error_update((__err), (__code), "%s %s", \
				(__address), as_error_string(__code)); \
		} \
	} while(0)

//---------------------------------
// Functions
//---------------------------------

static inline void
as_exp_trace_reset(as_exp_trace* trace)
{
	memset(trace, 0, sizeof(as_exp_trace));
}

static inline void
as_exp_trace_copy(as_exp_trace* trg, const as_exp_trace* src)
{
	memcpy(trg, src, sizeof(as_exp_trace));
}

static inline void
as_error_detail_reset(as_error_detail* detail)
{
	detail->has_subcode = false;
	detail->subcode = 0;
	detail->has_message = false;
	detail->has_exp_trace = false;
	as_exp_trace_reset(&detail->exp_trace);
	detail->message[0] = '\0';
}

static inline void
as_error_detail_copy(as_error_detail* trg, const as_error_detail* src)
{
	trg->has_subcode = src->has_subcode;
	trg->subcode = src->subcode;
	trg->has_message = src->has_message;
	trg->has_exp_trace = src->has_exp_trace;
	as_exp_trace_copy(&trg->exp_trace, &src->exp_trace);
	strcpy(trg->message, src->message);
}

static inline bool
as_error_detail_has_server_detail(const as_error_detail* detail)
{
	return detail->has_subcode || detail->has_message || detail->has_exp_trace;
}

static inline void
as_error_clear_server_detail(as_error* err)
{
	err->subcode = 0;
	err->has_subcode = false;
	err->has_message = false;
	err->has_exp_trace = false;
	as_exp_trace_reset(&err->exp_trace);
	err->message[0] = '\0';
}

static inline bool
as_error_has_server_detail(const as_error* err)
{
	return err->has_subcode || err->has_message || err->has_exp_trace;
}

static inline void
as_error_copy_server_fields(as_error* trg, const as_error* src)
{
	trg->subcode = src->subcode;
	trg->has_subcode = src->has_subcode;
	trg->has_message = src->has_message;
	trg->has_exp_trace = src->has_exp_trace;
	as_exp_trace_copy(&trg->exp_trace, &src->exp_trace);
}

static inline as_status
as_error_update_server_detail(
	as_error* err, as_status code, const char* func, const char* file, uint32_t line
	)
{
	err->code = code;
	err->func = func;
	err->file = file;
	err->line = line;
	err->in_doubt = false;
	return err->code;
}

static inline as_status
as_error_set_message_preserving_server_detail(
	as_error* err, as_status code, const char* message, const char* func, const char* file,
	uint32_t line
	)
{
	as_error_update_server_detail(err, code, func, file, line);
	as_strncpy(err->message, message, AS_ERROR_MESSAGE_MAX_SIZE);
	return err->code;
}

static inline as_status
as_error_set_messagev_preserving_server_detail(
	as_error* err, as_status code, const char* func, const char* file, uint32_t line,
	const char* fmt, ...
	)
{
	as_error_update_server_detail(err, code, func, file, line);

	if (fmt != NULL) {
		va_list ap;
		va_start(ap, fmt);
		vsnprintf(err->message, AS_ERROR_MESSAGE_MAX_LEN, fmt, ap);
		err->message[AS_ERROR_MESSAGE_MAX_LEN] = '\0';
		va_end(ap);
	}
	return err->code;
}

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
	as_error_clear_server_detail(err);
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
	as_error_clear_server_detail(err);
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
	as_error_clear_server_detail(err);
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
	as_error_clear_server_detail(err);
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
	as_error_copy_server_fields(trg, src);
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
