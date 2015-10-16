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
#include <aerospike/as_event.h>
#include <aerospike/as_async.h>
#include <aerospike/as_log_macros.h>
#include <aerospike/as_socket.h>
#include <citrusleaf/alloc.h>
#include <errno.h>

/******************************************************************************
 * COMMON
 *****************************************************************************/

as_event_loop* as_event_loops = 0;
uint32_t as_event_loop_capacity = 0;
uint32_t as_event_loop_size = 0;
static bool as_event_threads_created = false;

as_event_loop*
as_event_create_loops(uint32_t capacity)
{
	as_event_loops = cf_malloc(sizeof(as_event_loop) * capacity);
	
	if (! as_event_loops) {
		return 0;
	}
	
	as_event_loop_capacity = capacity;
	as_event_threads_created = true;
	
	for (uint32_t i = 0; i < capacity; i++) {
		as_event_loop* event_loop = &as_event_loops[i];
		event_loop->index = i;
		
		if (! as_event_create_loop(event_loop)) {
			as_event_close_loops();
			return 0;
		}
		as_event_loop_size++;
	}
	return as_event_loops;
}

bool
as_event_set_external_loop_capacity(uint32_t capacity)
{
	size_t mem_size = sizeof(as_event_loop) * capacity;
	as_event_loops = cf_malloc(mem_size);
	
	if (! as_event_loops) {
		return false;
	}
	
	memset(as_event_loops, 0, mem_size);
	as_event_loop_capacity = capacity;
	as_event_threads_created = false;
	return true;
}

as_event_loop*
as_event_set_external_loop(void* loop, pthread_t thread)
{
	uint32_t current = ck_pr_faa_32(&as_event_loop_size, 1);
	
	if (current >= as_event_loop_capacity) {
		as_log_error("Failed to add external loop. Capacity is %u", as_event_loop_capacity);
		return 0;
	}
	
	as_event_loop* event_loop = &as_event_loops[current];
	event_loop->index = current;
	event_loop->loop = loop;
	event_loop->thread = thread;
	as_event_register_wakeup(event_loop);
	return event_loop;
}

void
as_event_close_loops()
{
	if (as_event_loops) {
		// Send stop signal to loops.
		bool join = true;
		
		for (uint32_t i = 0; i < as_event_loop_size; i++) {
			as_event_loop* event_loop = &as_event_loops[i];
			
			if (! as_event_close_loop(event_loop)) {
				as_log_error("Failed to send stop command to event loop");
				join = false;
			}
		}
		
		if (as_event_threads_created && join) {
			// Join threads.
			for (uint32_t i = 0; i < as_event_loop_size; i++) {
				as_event_loop* event_loop = &as_event_loops[i];
				pthread_join(event_loop->thread, NULL);
			}
		}
		cf_free(as_event_loops);
	}
}

#if defined(AS_USE_LIBEV)

/******************************************************************************
 * LIBEV
 *****************************************************************************/

static void*
as_ev_worker(void* udata)
{
	struct ev_loop* loop = udata;
	ev_loop(loop, 0);
	ev_loop_destroy(loop);
	return NULL;
}

static void
as_ev_wakeup(struct ev_loop* loop, ev_async* watcher, int revents)
{
	// Read command pointers from queue.
	as_event_loop* event_loop = (as_event_loop*)((uint8_t*)watcher - offsetof(as_event_loop, wakeup));
	void* cmd;
	
	pthread_mutex_lock(&event_loop->lock);
	
	while (as_queue_pop(&event_loop->queue, &cmd)) {
		if (cmd) {
			// Process new command.
			as_async_command_execute(cmd);
		}
		else {
			// Received stop signal.
			ev_async_stop(event_loop->loop, &event_loop->wakeup);
			
			// Only stop event loop if client created event loop.
			if (as_event_threads_created) {
				ev_unloop(loop, EVUNLOOP_ALL);
			}
			
			// Cleanup event loop resources.
			as_queue_destroy(&event_loop->queue);
			pthread_mutex_unlock(&event_loop->lock);
			pthread_mutex_destroy(&event_loop->lock);
			return;
		}
	}
	pthread_mutex_unlock(&event_loop->lock);
}

static void
as_ev_callback(struct ev_loop* loop, ev_io* watcher, int revents)
{
	void* cmd = watcher;
	
	if (revents & EV_READ) {
		as_async_command_receive(cmd);
	}
	else if (revents & EV_WRITE) {
		as_async_command_send(cmd);
	}
	else if (revents & EV_ERROR) {
		as_log_error("Async error occurred: %d", revents);
	}
	else {
		as_log_warn("Unknown event received: %d", revents);
	}
}

static void
as_ev_timeout(struct ev_loop* loop, ev_timer* timer, int revents)
{
	// One-off timers are automatically stopped by libev.
	as_async_timeout((void*)((uint8_t*)timer - offsetof(as_event_command, timer)));
}

bool
as_event_create_loop(as_event_loop* event_loop)
{
	event_loop->loop = ev_loop_new(EVFLAG_AUTO);
	
	if (! event_loop->loop) {
		return false;
	}
	
	pthread_mutex_init(&event_loop->lock, 0);
	as_queue_init(&event_loop->queue, sizeof(void*), 256);  // TODO make configurable.
	as_event_register_wakeup(event_loop);
	
	return pthread_create(&event_loop->thread, NULL, as_ev_worker, event_loop->loop) == 0;
}

bool
as_event_close_loop(as_event_loop* event_loop)
{
	// Send stop command through queue so it can be executed in event loop thread.
	void* ptr = 0;
	pthread_mutex_lock(&event_loop->lock);
	bool status = as_queue_push(&event_loop->queue, &ptr);
	pthread_mutex_unlock(&event_loop->lock);
	
	if (status) {
		ev_async_send(event_loop->loop, &event_loop->wakeup);
	}
	return status;
}

void
as_event_register_wakeup(as_event_loop* event_loop)
{
	ev_async_init(&event_loop->wakeup, as_ev_wakeup);
	ev_async_start(event_loop->loop, &event_loop->wakeup);
}

bool
as_event_send(as_event_command* cmd)
{
	// Send command through queue so it can be executed in event loop thread.
	as_event_loop* event_loop = cmd->event_loop;
	
	pthread_mutex_lock(&event_loop->lock);
	bool status = as_queue_push(&event_loop->queue, &cmd);
	pthread_mutex_unlock(&event_loop->lock);

	if (status) {
		ev_async_send(event_loop->loop, &event_loop->wakeup);
	}
	return status;
}

void
as_event_register_write(as_event_command* cmd)
{
	ev_io_init(&cmd->watcher, as_ev_callback, cmd->fd, EV_WRITE);
	ev_io_start(cmd->event_loop->loop, &cmd->watcher);
}

void
as_event_register_read(as_event_command* cmd)
{
	ev_io_init(&cmd->watcher, as_ev_callback, cmd->fd, EV_READ);
	ev_io_start(cmd->event_loop->loop, &cmd->watcher);
}

void
as_event_set_write(as_event_command* cmd)
{
	ev_io_stop(cmd->event_loop->loop, &cmd->watcher);
	ev_io_set(&cmd->watcher, cmd->fd, EV_WRITE);
	ev_io_start(cmd->event_loop->loop, &cmd->watcher);
}

void
as_event_set_read(as_event_command* cmd)
{
	ev_io_stop(cmd->event_loop->loop, &cmd->watcher);
	ev_io_set(&cmd->watcher, cmd->fd, EV_READ);
	ev_io_start(cmd->event_loop->loop, &cmd->watcher);
}

void
as_event_init_timer(as_event_command* cmd)
{
	ev_timer_init(&cmd->timer, as_ev_timeout, cmd->timeout_ms / 1000.0, 0.0);
	ev_timer_start(cmd->event_loop->loop, &cmd->timer);
}

void
as_event_stop_timer(as_event_command* cmd)
{
	ev_timer_stop(cmd->event_loop->loop, &cmd->timer);
}

void
as_event_unregister(as_event_command* cmd)
{
	ev_io_stop(cmd->event_loop->loop, &cmd->watcher);
}

#elif defined(AS_USE_LIBUV)

/******************************************************************************
 * LIBUV - TBD
 *****************************************************************************/

#else

/******************************************************************************
 * NO EVENT LIB DEFINED
 *****************************************************************************/

bool
as_event_create_loop(as_event_loop* event_loop)
{
	return false;
}

bool
as_event_close_loop(as_event_loop* event_loop)
{
	return false;
}

void
as_event_register_wakeup(as_event_loop* event_loop)
{
}

bool
as_event_send(as_event_command* cmd)
{
	return false;
}

void
as_event_register_write(as_event_command* cmd)
{
}

void
as_event_register_read(as_event_command* cmd)
{
}

void
as_event_set_write(as_event_command* cmd)
{
}

void
as_event_set_read(as_event_command* cmd)
{
}

void
as_event_init_timer(as_event_command* cmd)
{
}

void
as_event_stop_timer(as_event_command* cmd)
{
}

void
as_event_unregister(as_event_command* cmd)
{
}

#endif
