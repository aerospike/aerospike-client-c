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

#include <aerospike/aerospike.h>
#include <aerospike/as_txn.h>

#ifdef __cplusplus
extern "C" {
#endif

//---------------------------------
// Functions
//---------------------------------

/**
 * Attempt to commit the given multi-record transaction. First, the expected record versions are
 * sent to the server nodes for verification. If all nodes return success, the transaction is
 * committed. Otherwise, the transaction is aborted.
 *
 * Requires server version 8.0+
 *
 * @param as 	Aerospike instance.
 * @param err	Error detail structure that is populated if an error occurs.
 * @param txn	Multi-record transaction.
 *
 * @return AEROSPIKE_OK if successful. Otherwise an error.
 * @relates aerospike
 */
AS_EXTERN as_status
aerospike_commit(aerospike* as, as_error* err, as_txn* txn);

#ifdef __cplusplus
} // end extern "C"
#endif
