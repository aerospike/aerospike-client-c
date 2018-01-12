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
#include "index_util.h"
#include <aerospike/aerospike_index.h>
#include "../test.h"

bool
index_process_return_code(as_status status, as_error* err, as_index_task* task)
{
	switch (status) {
		case AEROSPIKE_OK:
			aerospike_index_create_wait(err, task, 0);
			break;

		case AEROSPIKE_ERR_INDEX_FOUND:
			info("index already exists");
			break;

		default:
			info("error(%d): %s", err->code, err->message);
			return false;
	}
	return true;
}
