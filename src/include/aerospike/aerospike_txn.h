/*
 * Copyright 2008-2025 Aerospike, Inc.
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

#include <aerospike/aerospike.h>
#include <aerospike/as_txn.h>

#ifdef __cplusplus
extern "C" {
#endif

//---------------------------------
// Types
//---------------------------------

struct as_event_loop;

/**
 * Transaction commit status code.
 */
typedef enum {
	/**
	 * Commit succeeded.
	 */
	AS_COMMIT_OK,

	/**
	 * Transaction has already been committed.
	 */
	AS_COMMIT_ALREADY_COMMITTED,

	/**
	 * Transaction has already been aborted.
	 */
	AS_COMMIT_ALREADY_ABORTED,

	/**
	 * Transaction verify failed. Transaction will be aborted.
	 */
	AS_COMMIT_VERIFY_FAILED,

	/**
	 * Transaction mark roll forward abandoned. Transaction will be aborted when error is not in doubt.
	 * If the error is in doubt (usually timeout), the commit is in doubt.
	 */
	AS_COMMIT_MARK_ROLL_FORWARD_ABANDONED,

	/**
	 * Client roll forward abandoned. Server will eventually commit the transaction.
	 */
	AS_COMMIT_ROLL_FORWARD_ABANDONED,

	/**
	 * Transaction has been rolled forward, but client transaction close was abandoned.
	 * Server will eventually close the transaction.
	 */
	AS_COMMIT_CLOSE_ABANDONED
} as_commit_status;

/**
 * Asynchronous commit listener.  This function is called once when aerospike_commit_async() completes or an
 * error has occurred.
 *
 * @param err			Error structure that is populated if an error occurs. NULL on success.
 * @param status 		Status of commit when err is NULL.
 * @param udata 			User data that is forwarded from asynchronous command function.
 * @param event_loop	Event loop that this command was executed on. Use this event loop when
 *						running nested asynchronous commands when single threaded behavior is
 *						desired for the group of commands.
 * @relates aerospike
 */
typedef void (*as_commit_listener)(
	as_error* err, as_commit_status status, void* udata, struct as_event_loop* event_loop
	);

/**
 * Transaction abort status code.
 */
typedef enum {
	/**
	 * Abort succeeded.
	 */
	AS_ABORT_OK,

	/**
	 * Transaction has already been aborted.
	 */
	AS_ABORT_ALREADY_ABORTED,

	/**
	 * Transaction has already been committed.
	 */
	AS_ABORT_ALREADY_COMMITTED,

	/**
	 * Client roll back abandoned. Server will eventually abort the transaction.
	 */
	AS_ABORT_ROLL_BACK_ABANDONED,

	/**
	 * Transaction has been rolled back, but client transaction close was abandoned.
	 * Server will eventually close the transaction.
	 */
	AS_ABORT_CLOSE_ABANDONED
} as_abort_status;

/**
 * Asynchronous commit listener.  This function is called once when aerospike_abort_async() completes or an
 * error has occurred.
 *
 * @param err			Error structure that is populated if an error occurs. NULL on success.
 * @param status 		Status of abort when err is NULL.
 * @param udata 			User data that is forwarded from asynchronous command function.
 * @param event_loop	Event loop that this command was executed on. Use this event loop when
 *						running nested asynchronous commands when single threaded behavior is
 *						desired for the group of commands.
 * @relates aerospike
 */
typedef void (*as_abort_listener)(
	as_error* err, as_abort_status status, void* udata, struct as_event_loop* event_loop
	);

//---------------------------------
// Functions
//---------------------------------

/**
 * Attempt to commit the given transaction. First, the expected record versions are
 * sent to the server nodes for verification. If all nodes return success, the transaction is
 * committed. Otherwise, the transaction is aborted.
 *
 * Requires server version 8.0+
 *
 * @param as 	Aerospike instance.
 * @param err	Error detail structure that is populated if an error occurs.
 * @param txn	Transaction.
 * @param commit_status	Indicates success or the step in the commit process that failed.
 *						Pass in NULL to ignore.
 *
 * @return AEROSPIKE_OK if successful. Otherwise an error.
 * @relates aerospike
 */
AS_EXTERN as_status
aerospike_commit(aerospike* as, as_error* err, as_txn* txn, as_commit_status* commit_status);

/**
 * Abort and rollback the given transaction.
 *
 * Requires server version 8.0+
 *
 * @param as 	Aerospike instance.
 * @param err	Error detail structure that is populated if an error occurs.
 * @param txn	Transaction.
 * @param abort_status	Indicates success or the step in the abort process that failed.
 *						Pass in NULL to ignore.
 *
 * @return AEROSPIKE_OK if successful. Otherwise an error.
 * @relates aerospike
 */
AS_EXTERN as_status
aerospike_abort(aerospike* as, as_error* err, as_txn* txn, as_abort_status* abort_status);

/**
 * Asynchronously attempt to commit the given transaction. First, the expected record
 * versions are sent to the server nodes for verification. If all nodes return success, the transaction
 * is committed. Otherwise, the transaction is aborted.
 *
 * Requires server version 8.0+
 *
 * @param as 			Aerospike instance.
 * @param err			Error detail structure that is populated if an error occurs.
 * @param txn			Transaction.
 * @param listener		User function to be called with command results.
 * @param udata 			User data that is forwarded from asynchronous command function.
 * @param event_loop	Event loop that this command was executed on. Use this event loop when
 *						running nested asynchronous commands when single threaded behavior is
 *						desired for the group of commands.
 *
 * @return AEROSPIKE_OK if async command succesfully queued. Otherwise an error.
 * @relates aerospike
 */
AS_EXTERN as_status
aerospike_commit_async(
	aerospike* as, as_error* err, as_txn* txn, as_commit_listener listener, void* udata,
	struct as_event_loop* event_loop
	);

/**
 * Asynchronously abort and rollback the given transaction.
 *
 * Requires server version 8.0+
 *
 * @param as 			Aerospike instance.
 * @param err			Error detail structure that is populated if an error occurs.
 * @param txn			Transaction.
 * @param listener		User function to be called with command results.
 * @param udata 			User data that is forwarded from asynchronous command function.
 * @param event_loop	Event loop that this command was executed on. Use this event loop when
 *						running nested asynchronous commands when single threaded behavior is
 *						desired for the group of commands.
 *
 * @return AEROSPIKE_OK if async command succesfully queued. Otherwise an error.
 * @relates aerospike
 */
AS_EXTERN as_status
aerospike_abort_async(
	aerospike* as, as_error* err, as_txn* txn, as_abort_listener listener, void* udata,
	struct as_event_loop* event_loop
	);

#ifdef __cplusplus
} // end extern "C"
#endif
