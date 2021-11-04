/*
 * Copyright 2008-2018 Aerospike, Inc.
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

#include <aerospike/as_error.h>
#include <aerospike/as_event.h>
#include <aerospike/as_record.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * User callback when an asynchronous write completes.
 *
 * @param err			This error structure is only populated when the command fails. Null on success.
 * @param udata 		User data that is forwarded from asynchronous command function.
 */
typedef void (*as_async_write_listener) (as_error* err, void* udata);
	
/**
 * User callback when an asynchronous read completes with a record result.
 *
 * @param err			This error structure is only populated when the command fails. Null on success.
 * @param record 		The return value from the asynchronous command.  Null on error.
 * @param udata 		User data that is forwarded from asynchronous command function.
 */
typedef void (*as_async_record_listener) (as_error* err, as_record* record, void* udata);

/**
 * User callback when asynchronous read completes with an as_val result.
 *
 * @param err			This error structure is only populated when the command fails. Null on success.
 * @param val			The return value from the asynchronous command. This value will need to be cast
 * 						to the structure that corresponds to the asynchronous command.  Null on error.
 * @param udata 		User data that is forwarded from asynchronous command function.
 */
typedef void (*as_async_value_listener) (as_error* err, as_val* val, void* udata);

/**
 * User callback when pipelined command has been sent, i.e., when the connection is ready for sending
 * the next command.
 *
 * @param udata 		User data that is forwarded from asynchronous command function.
 */
typedef void (*as_pipe_listener) (void* udata);

/**
 * User callback when asynchronous info command completes with a string result.
 *
 * @param err			This error structure is only populated when the command fails. Null on success.
 * @param response		The return string from the asynchronous info command. Null on error. 
 *						Do not free when finished.  This will be done automatically.
 * @param udata			User data that is forwarded from asynchronous command function.
 */
typedef void (*as_async_info_listener) (as_error* err, char* response, void* udata);

#ifdef __cplusplus
} // end extern "C"
#endif
