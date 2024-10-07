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

#include <aerospike/as_error.h>
#include <aerospike/as_queue.h>
#include <pthread.h>

/**
 * @defgroup async_events Event Framework Abstraction
 *
 * Generic asynchronous events abstraction.  Designed to support multiple event libraries.
 * Only one library is supported per build.
 */
#if defined(AS_USE_LIBEV) || defined(AS_USE_LIBUV) || defined(AS_USE_LIBEVENT)
#define AS_EVENT_LIB_DEFINED 1
#endif

#if defined(AS_USE_LIBEV)
#include <ev.h>
#elif defined(AS_USE_LIBUV)
#include <uv.h>
#elif defined(AS_USE_LIBEVENT)
#include <event2/event_struct.h>
#include <aerospike/as_vector.h>
#else
#endif

#ifdef __cplusplus
extern "C" {
#endif

/******************************************************************************
 * TYPES
 *****************************************************************************/
	
/**
 * Asynchronous event loop configuration.
 *
 * @ingroup async_events
 */
typedef struct as_policy_event {
	/**
	 * Maximum number of async commands that can be processed in each event loop at any point in
	 * time. Each executing non-pipeline async command requires a socket connection.  Consuming too
	 * many sockets can negatively affect application reliability and performance.  If the user does
	 * not limit async command count in their application, this field should be used to enforce a
	 * limit internally in the client.
	 *
	 * If this limit is reached, the next async command will be placed on the event loop's delay
	 * queue for later execution.  If this limit is zero, all async commands will be executed
	 * immediately and the delay queue will not be used.
	 *
	 * If defined, a reasonable value is 40.  The optimal value will depend on cpu count, cpu speed,
	 * network bandwitdh and the number of event loops employed.
	 *
	 * Default: 0 (execute all async commands immediately)
	 */
	int max_commands_in_process;

	/**
	 * Maximum number of async commands that can be stored in each event loop's delay queue for
	 * later execution.  Queued commands consume memory, but they do not consume sockets. This
	 * limit should be defined when it's possible that the application executes so many async
	 * commands that memory could be exhausted.
	 *
	 * If this limit is reached, the next async command will be rejected with error code
	 * AEROSPIKE_ERR_ASYNC_QUEUE_FULL.  If this limit is zero, all async commands will be accepted
	 * into the delay queue.
	 *
	 * The optimal value will depend on your application's magnitude of command bursts and the
	 * amount of memory available to store commands.
	 *
	 * Default: 0 (no delay queue limit)
	 */
	uint32_t max_commands_in_queue;

	/**
	 * Initial capacity of each event loop's delay queue.  The delay queue can resize beyond this
	 * initial capacity.
	 *
	 * Default: 256 (if delay queue is used)
	 */
	uint32_t queue_initial_capacity;
} as_policy_event;

/**
 * Generic asynchronous event loop abstraction.  There is one event loop per thread.
 * Event loops can be created by the client, or be referenced to externally created event loops.
 *
 * @ingroup async_events
 */
typedef struct as_event_loop {
#if defined(AS_USE_LIBEV)
	struct ev_loop* loop;
	struct ev_async wakeup;
#elif defined(AS_USE_LIBUV)
	uv_loop_t* loop;
	uv_async_t* wakeup;
#elif defined(AS_USE_LIBEVENT)
	struct event_base* loop;
	struct event wakeup;
	struct event trim;
	as_vector clusters;
#else
	void* loop;
#endif
		
	struct as_event_loop* next;
	pthread_mutex_t lock;
	as_queue queue;
	as_queue delay_queue;
	as_queue pipe_cb_queue;
	pthread_t thread;
	uint32_t index;
	uint32_t max_commands_in_queue;
	int max_commands_in_process;
	int pending;
	// Count of consecutive errors occurring before event loop registration.
	// Used to prevent deep recursion.
	uint32_t errors;
	bool using_delay_queue;
	bool pipe_cb_calling;
} as_event_loop;

/******************************************************************************
 * GLOBAL VARIABLES
 *****************************************************************************/

AS_EXTERN extern as_event_loop* as_event_loops;
AS_EXTERN extern as_event_loop* as_event_loop_current;
AS_EXTERN extern uint32_t as_event_loop_size;
AS_EXTERN extern bool as_event_single_thread;

/******************************************************************************
 * PUBLIC FUNCTIONS
 *****************************************************************************/

/**
 * Initialize event loop configuration variables.
 *
 * @ingroup async_events
 */
static inline void
as_policy_event_init(as_policy_event* policy)
{
	policy->max_commands_in_process = 0;
	policy->max_commands_in_queue = 0;
	policy->queue_initial_capacity = 256;
}

/**
 * Create new aerospike internal event loops with default event policy. These event loops are used
 * exclusively for aerospike database commands and are not shared with the application for other
 * tasks. If shared event loops are desired, use as_event_set_external_loop_capacity() and
 * as_event_set_external_loop() instead.
 *
 * This function must be called before aerospike_connect().
 *
 * @param capacity	Number of event loops to create.
 * @return			Event loop array.
 *
 * @ingroup async_events
 */
AS_EXTERN as_event_loop*
as_event_create_loops(uint32_t capacity);

/**
 * Create new aerospike internal event loops with specified event policy. These event loops are used
 * exclusively for aerospike database commands and are not shared with the application for other
 * tasks. If shared event loops are desired, use as_event_set_external_loop_capacity() and
 * as_set_external_event_loop() instead.
 *
 * This function must be called before aerospike_connect().
 *
 * @param err			The as_error to be populated if an error occurs.
 * @param policy		Event loop configuration.  Pass in NULL for default configuration.
 * @param capacity		Number of event loops to create.
 * @param event_loops	Created event loops.  Pass in NULL if event loops do not need to be retrieved.
 * @return AEROSPIKE_OK If successful. Otherwise an error.
 *
 * @ingroup async_events
 */
AS_EXTERN as_status
as_create_event_loops(as_error* err, as_policy_event* policy, uint32_t capacity, as_event_loop** event_loops);

/**
 * Set the number of aerospike external event loops.  This method should be called when the
 * application wants to share event loops with the client.  This reduces resource usage and
 * can increase performance.
 *
 * This method is used in conjunction with as_event_set_external_loop() or
 * as_set_external_event_loop() to fully define the the external loop to the client and obtain a
 * reference to the client's event loop abstraction.
 *
 * ~~~~~~~~~~{.c}
 * struct {
 * 	   pthread_t thread;
 * 	   struct ev_loop* loop;
 * 	   as_event_loop* as_loop;
 * } my_loop;
 *
 * static void* my_loop_worker_thread(void* udata)
 * {
 *     struct my_loop* myloop = udata;
 * 	   myloop->loop = ev_loop_new(EVFLAG_AUTO);
 * 	   myloop->as_loop = as_event_set_external_loop(myloop->loop);
 * 	   ev_loop(myloop->loop, 0);
 * 	   ev_loop_destroy(myloop->loop);
 * 	   return NULL;
 * }
 *
 * int capacity = 8;
 * struct my_loop* loops = malloc(sizeof(struct my_loop) * capacity);
 * as_event_set_external_loop_capacity(capacity);
 *
 * for (int i = 0; i < capacity; i++) {
 * 	   struct my_loop* myloop = &loops[i];
 * 	   return pthread_create(&myloop->thread, NULL, my_loop_worker_thread, myloop) == 0;
 * }
 * ~~~~~~~~~~
 *
 * @param capacity	Number of externally created event loops.
 * @return			True if all external loops were initialized.
 *
 * @ingroup async_events
 */
AS_EXTERN bool
as_event_set_external_loop_capacity(uint32_t capacity);

/**
 * Register an aerospike external event loop with the client with default event policy.
 *
 * This method should be called when the calling program wants to share event loops with the client.
 * This reduces resource usage and can increase performance.
 *
 * This method must be called in the same thread as the event loop that is being registered.
 *
 * This method is used in conjunction with as_event_set_external_loop_capacity() to fully define
 * the external loop to the client and obtain a reference to the client's event loop abstraction.
 *
 * ~~~~~~~~~~{.c}
 * struct {
 *     pthread_t thread;
 * 	   struct ev_loop* loop;
 * 	   as_event_loop* as_loop;
 * } my_loop;
 *
 * static void* my_loop_worker_thread(void* udata)
 * {
 * 	    struct my_loop* myloop = udata;
 *      myloop->loop = ev_loop_new(EVFLAG_AUTO);
 * 	    myloop->as_loop = as_event_set_external_loop(myloop->loop);
 * 	    ev_loop(myloop->loop, 0);
 * 	    ev_loop_destroy(myloop->loop);
 * 	    return NULL;
 * }
 *
 * int capacity = 8;
 * struct my_loop* loops = malloc(sizeof(struct my_loop) * capacity);
 * as_event_set_external_loop_capacity(capacity);
 *
 * for (int i = 0; i < capacity; i++) {
 * 	   struct my_loop* myloop = &loops[i];
 * 	   return pthread_create(&myloop->thread, NULL, my_loop_worker_thread, myloop) == 0;
 * }
 * ~~~~~~~~~~
 *
 * @param loop		External event loop.
 * @return			Client's generic event loop abstraction that is used in client async commands.
 * 					Returns NULL if external loop capacity would be exceeded.
 *
 * @ingroup async_events
 */
AS_EXTERN as_event_loop*
as_event_set_external_loop(void* loop);

/**
 * Register an aerospike external event loop with the client with specified event policy.
 *
 * This method should be called when the calling program wants to share event loops with the client.
 * This reduces resource usage and can increase performance.
 *
 * This method must be called in the same thread as the event loop that is being registered.
 *
 * This method is used in conjunction with as_event_set_external_loop_capacity() to fully define
 * the external loop to the client and obtain a reference to the client's event loop abstraction.
 *
 * ~~~~~~~~~~{.c}
 * struct {
 *     pthread_t thread;
 * 	   struct ev_loop* loop;
 * 	   as_event_loop* as_loop;
 * } my_loop;
 *
 * static void* my_loop_worker_thread(void* udata)
 * {
 * 	    struct my_loop* myloop = udata;
 *      myloop->loop = ev_loop_new(EVFLAG_AUTO);
 *
 *      as_policy_event policy;
 *      as_policy_event_init(&policy);
 *      policy.max_commands_in_process = 30;
 *
 *      as_error err;
 *      if (as_set_external_event_loop(&err, &policy, myloop->loop, &myloop->as_loop) != AEROSPIKE_OK) {
 *          printf("Failed to set event loop: %d %s\n, err.code, err.message);
 *          return NULL;
 *      }
 * 	    myloop->as_loop = as_event_set_external_loop(myloop->loop);
 * 	    ev_loop(myloop->loop, 0);
 * 	    ev_loop_destroy(myloop->loop);
 * 	    return NULL;
 * }
 *
 * int capacity = 8;
 * struct my_loop* loops = malloc(sizeof(struct my_loop) * capacity);
 * as_event_set_external_loop_capacity(capacity);
 *
 * for (int i = 0; i < capacity; i++) {
 * 	   struct my_loop* myloop = &loops[i];
 * 	   return pthread_create(&myloop->thread, NULL, my_loop_worker_thread, myloop) == 0;
 * }
 * ~~~~~~~~~~
 *
 * @param err			The as_error to be populated if an error occurs.
 * @param policy		Event loop configuration.  Pass in NULL for default configuration.
 * @param loop			External event loop.
 * @param event_loop	Created event loop.
 * @return AEROSPIKE_OK If successful. Otherwise an error.
 *
 * @ingroup async_events
 */
AS_EXTERN as_status
as_set_external_event_loop(as_error* err, as_policy_event* policy, void* loop, as_event_loop** event_loop);

/**
 * Find client's event loop abstraction given the external event loop.
 *
 * @param loop		External event loop.
 * @return			Client's generic event loop abstraction that is used in client async commands.
 * 					Returns NULL if loop not found.
 *
 * @ingroup async_events
 */
AS_EXTERN as_event_loop*
as_event_loop_find(void* loop);

/**
 * Retrieve event loop by array index.
 *
 * @param index		Event loop array index.
 * @return			Client's generic event loop abstraction that is used in client async commands.
 *
 * @ingroup async_events
 */
static inline as_event_loop*
as_event_loop_get_by_index(uint32_t index)
{
	return index < as_event_loop_size ? &as_event_loops[index] : NULL;
}

/**
 * Retrieve a random event loop using round robin distribution.
 *
 * @return			Client's generic event loop abstraction that is used in client async commands.
 *
 * @ingroup async_events
 */
static inline as_event_loop*
as_event_loop_get(void)
{
	// The last event loop points to the first event loop to create a circular linked list.
	// Not atomic because doesn't need to be exactly accurate.
	as_event_loop* event_loop = as_event_loop_current;
	as_event_loop_current = event_loop->next;
	return event_loop;
}
	
/**
 * Return the approximate number of commands currently being processed on
 * the event loop.  The value is approximate because the call may be from a
 * different thread than the event loop’s thread and there are no locks or
 * atomics used.
 *
 * @ingroup async_events
 */
static inline int
as_event_loop_get_process_size(as_event_loop* event_loop)
{
	return event_loop->pending;
}

/**
 * Return the approximate number of commands stored on this event loop's
 * delay queue that have not been started yet.  The value is approximate
 * because the call may be from a different thread than the event loop’s
 * thread and there are no locks or atomics used.
 *
 * @ingroup async_events
 */
static inline uint32_t
as_event_loop_get_queue_size(as_event_loop* event_loop)
{
	return as_queue_size(&event_loop->delay_queue);
}

/**
 * Close internal event loops and release watchers for internal and external event loops.
 * The global event loop array will also be destroyed for internal event loops.
 *
 * This method should be called once on program shutdown if as_event_create_loops() or
 * as_event_set_external_loop_capacity() was called.
 *
 * The shutdown sequence is slightly different for internal and external event loops.
 *
 * Internal:
 * ~~~~~~~~~~{.c}
 * as_event_close_loops();
 * ~~~~~~~~~~
 *
 * External:
 * ~~~~~~~~~~{.c}
 * as_event_close_loops();
 * Join on external loop threads.
 * as_event_destroy_loops();
 * ~~~~~~~~~~
 *
 * @return		True if event loop close was successful.  If false, as_event_destroy_loops() should
 * 				not be called.
 *
 * @ingroup async_events
 */
AS_EXTERN bool
as_event_close_loops(void);

/**
 * Close internal event loop and release internal/external event loop watchers.
 * This optional method can be used instead of as_event_close_loops().
 * If used, must be called from event loop's thread.
 */
AS_EXTERN void
as_event_close_loop(as_event_loop* event_loop);

/**
 * Destroy global event loop array.  This function only needs to be called for external
 * event loops.
 *
 * @ingroup async_events
 */
AS_EXTERN void
as_event_destroy_loops(void);

/******************************************************************************
 * LIBEVENT SINGLE THREAD MODE FUNCTIONS
 *****************************************************************************/

#if defined(AS_USE_LIBEVENT)
struct aerospike_s;

/**
 * Event loop close aerospike listener
 *
 * @ingroup async_events
 */
typedef void (*as_event_close_listener) (void* udata);

/**
 * Set flag to signify that all async commands will be created in their associated event loop thread.
 * If enabled, the client can remove locks associated with sending async commands to the event loop.
 * This flag is only referenced when running the client with the libevent framework.
 *
 * By default, async single thread mode is false.
 *
 * @ingroup async_events
 */
static inline void
as_event_set_single_thread(bool single_thread)
{
	as_event_single_thread = single_thread;
}

/**
 * Register aerospike instance with event loop.
 * Should only be called in libevent single-thread mode.
 * The call must occur in the event loop's thread.
 *
 * @ingroup async_events
 */
AS_EXTERN void
as_event_loop_register_aerospike(as_event_loop* event_loop, struct aerospike_s* as);

/**
 * Unregister and free aerospike instance resources associated with event loop.
 * Should only be called in libevent single-thread mode.
 * The call must occur in the event loop's thread.
 *
 * Listener is called when all aerospike instance async commands have completed
 * on this event loop. Do not call aerospike_close() until listeners return on all
 * event loops.
 *
 * @ingroup async_events
 */
AS_EXTERN void
as_event_loop_close_aerospike(
	as_event_loop* event_loop, struct aerospike_s* as, as_event_close_listener listener, void* udata
	);

#endif

#ifdef __cplusplus
} // end extern "C"
#endif
