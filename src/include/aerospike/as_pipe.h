/*
 * Copyright 2015 Aerospike, Inc.
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

#include <aerospike/as_async.h>
#include <aerospike/as_log.h>
#include <aerospike/as_log_macros.h>
#include <aerospike/as_node.h>
#include <aerospike/as_socket.h>

#include <citrusleaf/alloc.h>
#include <citrusleaf/cf_ll.h>

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <unistd.h>

#include <netinet/in.h>
#include <netinet/tcp.h>

#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>

typedef struct as_pipe_connection {
	as_async_command* writer;
	cf_ll readers;
	int32_t fd;
	bool canceled;
	bool in_pool;
} as_pipe_connection;

extern bool
as_pipe_connection_setup(int32_t fd, as_error* err);

extern int32_t
as_pipe_get_connection(as_async_command* cmd);

extern void
as_pipe_socket_error(as_async_command* cmd, as_error* err);

extern void
as_pipe_timeout(as_async_command* cmd);

extern void
as_pipe_response_error(as_async_command* cmd, as_error* err);

extern void
as_pipe_response_complete(as_async_command* cmd);

extern void
as_pipe_write_start(as_async_command* cmd);

extern void
as_pipe_read_start(as_async_command* cmd, bool has_event);
