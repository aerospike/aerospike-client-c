/*
 * as_stap.h
 *
 * Copyright (C) 2015 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * This program is free software: you can redistribute it and/or modify it under
 * the terms of the GNU Affero General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more
 * details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses/
 */

#pragma once

#if defined(USE_SYSTEMTAP)
#include <sys/sdt.h>
#include "probes.h"
#else
#define AEROSPIKE_PUT_EXECUTE_STARTING(arg1)
#define AEROSPIKE_PUT_EXECUTE_FINISHED(arg1)
#define AEROSPIKE_QUERY_FOREACH_STARTING(arg1)
#define AEROSPIKE_QUERY_FOREACH_FINISHED(arg1)
#define AEROSPIKE_QUERY_ENQUEUE_TASK(arg1, arg2)
#define AEROSPIKE_QUERY_COMMAND_EXECUTE(arg1, arg2)
#define AEROSPIKE_QUERY_COMMAND_COMPLETE(arg1, arg2)
#define AEROSPIKE_QUERY_COMMAND_COMPLETE(arg1, arg2)
#define AEROSPIKE_QUERY_PARSE_RECORDS_STARTING(arg1, arg2, arg3)
#define AEROSPIKE_QUERY_PARSE_RECORDS_FINISHED(arg1, arg2, arg3, arg4)
#define AEROSPIKE_QUERY_AGGPARSE_STARTING(arg1, arg2)
#define AEROSPIKE_QUERY_AGGPARSE_FINISHED(arg1, arg2)
#define AEROSPIKE_QUERY_AGGCB_STARTING(arg1, arg2)
#define AEROSPIKE_QUERY_AGGCB_FINISHED(arg1, arg2)
#define AEROSPIKE_QUERY_RECPARSE_STARTING(arg1, arg2)
#define AEROSPIKE_QUERY_RECPARSE_BINS(arg1, arg2)
#define AEROSPIKE_QUERY_RECPARSE_FINISHED(arg1, arg2)
#define AEROSPIKE_QUERY_RECCB_STARTING(arg1, arg2)
#define AEROSPIKE_QUERY_RECCB_FINISHED(arg1, arg2)
#endif
