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
// Types
//---------------------------------


//---------------------------------
// Functions
//---------------------------------

/**
 * Initialize a stack allocated aerospike instance. 
 *
 * The config parameter can be an instance of `as_config` or `NULL`. If `NULL`,
 * then the default configuration will be used.
 *
 * Ownership of the as_config instance fields are transferred to the aerospike instance.
 * The user should never call as_config_destroy() directly.
 *
 * ~~~~~~~~~~{.c}
 * aerospike as;
 * aerospike_init(&as, &config);
 * ~~~~~~~~~~
 *
 * Once you are finished using the instance, then you should destroy it via the 
 * `aerospike_destroy()` function.
 *
 * @param as 		The aerospike instance to initialize.
 * @param config 	The configuration to use for the instance.
 *
 * @returns the initialized aerospike instance
 *
 * @see config for information on configuring the client.
 *
 * @relates aerospike
 */
AS_EXTERN as_status
aerospike_commit(aerospike* as, as_error* err, as_txn* txn);

#ifdef __cplusplus
} // end extern "C"
#endif
