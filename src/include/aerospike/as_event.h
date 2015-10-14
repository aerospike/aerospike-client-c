/*
 * Copyright 2008-2015 Aerospike, Inc.
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

#include <aerospike/as_queue.h>
#include <pthread.h>
#include <stdint.h>
#include <stdbool.h>

/**
 *	@defgroup async_events Asynchronous Event Abstraction
 *
 *  Generic asynchronous events abstraction.  Designed to support multiple event libraries
 *	such as libev and libuv.  Only one library can be supported per build.
 */

#if defined(AS_USE_LIBEV)
#include <ev.h>

#define as_event_context_placeholder \
struct ev_io watcher;\
struct ev_timer timer;

#elif defined(AS_USE_EPOLL)

#include <citrusleaf/cf_shash.h>
#include <sys/epoll.h>
#define as_event_context_placeholder

#else

#define as_event_context_placeholder

#endif

#ifdef __cplusplus
extern "C" {
#endif

/******************************************************************************
 * TYPES
 *****************************************************************************/

/**
 *	Generic asynchronous event loop abstraction.  There is one event loop per thread.
 *	Event loops can be created by the client, or be referenced to externally created event loops.
 *
 *	@ingroup async_events
 */
typedef struct {
#if defined(AS_USE_LIBEV)
	struct ev_loop* loop;
	struct ev_async wakeup;
	pthread_mutex_t lock;
	as_queue queue;
#elif defined(AS_USE_EPOLL)
	int epoll_fd;
	int read_pipe;
	int write_pipe;
	int pad;
#else
	void* loop;
#endif
		
	pthread_t thread;
	uint32_t index;
} as_event_loop;

struct as_async_command;

/******************************************************************************
 * PUBLIC FUNCTIONS
 *****************************************************************************/

/**
 *	Create new event loops. This method should only be called when asynchronous client commands 
 *	will be used and the calling program itself is not asynchronous.  If this method is used,
 *	it must be called before aerospike_connect().
 *
 *	@param capacity	Number of event loops to create.
 *
 *	@ingroup async_events
 */
as_event_loop*
as_event_create_loops(uint32_t capacity);

/**
 *	Set the number of externally created event loops.  This method should be called when the 
 *	calling program wants to share event loops with the client.  This reduces resource usage and
 *	can increase performance.
 *
 *	This method is used in conjunction with as_event_set_external_loop() to fully define the
 *	external loop to the client.
 *
 *	~~~~~~~~~~{.c}
 *	struct {
 *		pthread_t thread;
 *		struct ev_loop* loop;
 *		as_event_loop* as_loop;
 *	} my_loop;
 *
 *	struct my_loop loops[8];  // initialize these loops
 *	as_event_set_external_loop_capacity(8);
 *
 *	for (int i = 0; i < 8; i++) {
 *		struct my_loop* loop = &loops[i];
 *		loop->as_loop = as_event_set_external_loop(loop->loop, loop->thread);
 *	}
 *	~~~~~~~~~~
 *
 *	@param capacity	Number of externally created event loops.
 *
 *	@ingroup async_events
 */
bool
as_event_set_external_loop_capacity(uint32_t capacity);

/**
 *	Define the external loop and it's corresponding thread to the client. This method should be 
 *	called when the calling program wants to share event loops with the client.  This reduces
 *	resource usage and can increase performance.
 *
 *	This method is used in conjunction with as_event_set_external_loop() to fully define the
 *	external loop to the client and obtain a reference the client's event loop abstraction.
 *
 *	~~~~~~~~~~{.c}
 *	struct {
 *		pthread_t thread;
 *		struct ev_loop* loop;
 *		as_event_loop* as_loop;
 *	} my_loop;
 *
 *	struct my_loop loops[8];  // initialize these loops
 *	as_event_set_external_loop_capacity(8);
 *
 *	for (int i = 0; i < 8; i++) {
 *		struct my_loop* loop = &loops[i];
 *		loop->as_loop = as_event_set_external_loop(loop->loop, loop->thread);
 *	}
 *	~~~~~~~~~~
 *
 *	@param loop		External event loop.
 *	@param thread	Thread that the event loop is running on.  Used as a read-only reference to
 *					determine if a new async client command is running in the event loop thread.
 *	@return			Client's generic event_loop abstraction that is used in client async
 *					commands.
 *
 *	@ingroup async_events
 */
as_event_loop*
as_event_set_external_loop(void* loop, pthread_t thread);

/**
 *	Close internally created event loops and release memory for event loop abstraction.
 *	This method should be called once on program shutdown if as_event_create_loops() or
 *	as_event_set_external_loop_capacity() was called.
 *
 *	@ingroup async_events
 */
void
as_event_close_loops();

/******************************************************************************
 * PRIVATE GLOBAL VARIABLES
 *****************************************************************************/

extern as_event_loop* as_event_loops;
extern uint32_t as_event_loop_size;
	
/******************************************************************************
 * PRIVATE FUNCTIONS
 *****************************************************************************/

bool
as_event_create_loop(as_event_loop* event_loop);

bool
as_event_close_loop(as_event_loop* event_loop);
	
void
as_event_register_wakeup(as_event_loop* event_loop);
	
bool
as_event_send(struct as_async_command* cmd);
	
void
as_event_register_write(struct as_async_command* cmd);

void
as_event_register_read(struct as_async_command* cmd);

void
as_event_set_write(struct as_async_command* cmd);

void
as_event_set_read(struct as_async_command* cmd);

void
as_event_unregister(struct as_async_command* cmd);

void
as_event_init_timer(struct as_async_command* cmd);

void
as_event_stop_timer(struct as_async_command* cmd);

static inline as_event_loop*
as_event_assign(uint32_t current)
{
	return &as_event_loops[current % as_event_loop_size];
}

#ifdef __cplusplus
} // end extern "C"
#endif
