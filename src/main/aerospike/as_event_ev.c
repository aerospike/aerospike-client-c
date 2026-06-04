/*
 * Copyright 2008-2026 Aerospike, Inc.
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
#include <aerospike/as_event_internal.h>
#include <aerospike/as_admin.h>
#include <aerospike/as_async.h>
#include <aerospike/as_atomic.h>
#include <aerospike/as_log_macros.h>
#include <aerospike/as_pipe.h>
#include <aerospike/as_proto.h>
#include <aerospike/as_socket.h>
#include <aerospike/as_status.h>
#include <aerospike/as_thread.h>
#include <aerospike/as_tls.h>
#include <citrusleaf/alloc.h>
#include <citrusleaf/cf_byte_order.h>
#include <openssl/bio.h>
#include <openssl/err.h>
#include <openssl/ssl.h>

//---------------------------------
// Globals
//---------------------------------

extern int as_event_send_buffer_size;
extern int as_event_recv_buffer_size;
extern bool as_event_threads_created;

//---------------------------------
// Libev Functions
//---------------------------------

#if defined(AS_USE_LIBEV)

void
as_event_close_loop(as_event_loop* event_loop)
{
	ev_async_stop(event_loop->loop, &event_loop->wakeup);
	
	// Only stop event loop if client created event loop.
	if (as_event_threads_created) {
		ev_unloop(event_loop->loop, EVUNLOOP_ALL);
	}
	
	// Cleanup event loop resources.
	as_event_loop_destroy(event_loop);
}

static void
as_ev_wakeup(struct ev_loop* loop, ev_async* wakeup, int revents)
{
	// Read command pointers from queue.
	as_event_loop* event_loop = wakeup->data;
	as_event_commander cmd;
	uint32_t i = 0;

	// Only process original size of queue.  Recursive pre-registration errors can
	// result in new commands being added while the loop is in process.  If we process
	// them, we could end up in an infinite loop.
	pthread_mutex_lock(&event_loop->lock);
	uint32_t size = as_queue_size(&event_loop->queue);
	bool status = as_queue_pop(&event_loop->queue, &cmd);
	pthread_mutex_unlock(&event_loop->lock);

	while (status) {
		if (! cmd.executable) {
			// Received stop signal.
			as_event_close_loop(event_loop);
			return;
		}
		cmd.executable(event_loop, cmd.udata);

		if (++i < size) {
			pthread_mutex_lock(&event_loop->lock);
			status = as_queue_pop(&event_loop->queue, &cmd);
			pthread_mutex_unlock(&event_loop->lock);
		}
		else {
			break;
		}
	}
}

static void*
as_ev_worker(void* udata)
{
	as_event_loop* event_loop = udata;

	as_thread_set_name_index("ev", event_loop->index);

	struct ev_loop* loop = event_loop->loop;
	ev_loop(loop, 0);
	ev_loop_destroy(loop);
	as_tls_thread_cleanup();
	return NULL;
}

static inline void
as_ev_init_loop(as_event_loop* event_loop)
{
	ev_async_init(&event_loop->wakeup, as_ev_wakeup);
	event_loop->wakeup.data = event_loop;
	ev_async_start(event_loop->loop, &event_loop->wakeup);	
}

bool
as_event_create_loop(as_event_loop* event_loop)
{
	event_loop->loop = ev_loop_new(EVFLAG_AUTO);
	
	if (! event_loop->loop) {
		return false;
	}
	as_ev_init_loop(event_loop);
	
	return pthread_create(&event_loop->thread, NULL, as_ev_worker, event_loop) == 0;
}

void
as_event_register_external_loop(as_event_loop* event_loop)
{
	// This method is only called when user sets an external event loop.
	as_ev_init_loop(event_loop);
}

bool
as_event_execute(as_event_loop* event_loop, as_event_executable executable, void* udata)
{
	// Send command through queue so it can be executed in event loop thread.
	pthread_mutex_lock(&event_loop->lock);
	as_event_commander qcmd = {.executable = executable, .udata = udata};
	bool queued = as_queue_push(&event_loop->queue, &qcmd);
	pthread_mutex_unlock(&event_loop->lock);

	if (queued) {
		ev_async_send(event_loop->loop, &event_loop->wakeup);
	}
	return queued;
}

static inline void
as_ev_watch_write(as_event_command* cmd)
{
	as_event_connection* conn = cmd->conn;
	int watch = cmd->pipe_listener != NULL ? EV_WRITE | EV_READ : EV_WRITE;

	// Skip if we're already watching the right stuff.
	if (watch == conn->watching) {
		return;
	}
	conn->watching = watch;

	ev_io_stop(cmd->event_loop->loop, &conn->watcher);
	ev_io_set(&conn->watcher, conn->socket.fd, watch);
	ev_io_start(cmd->event_loop->loop, &conn->watcher);
}

static inline void
as_ev_watch_read(as_event_command* cmd)
{
	as_event_connection* conn = cmd->conn;
	int watch = EV_READ;

	// Skip if we're already watching the right stuff.
	if (watch == conn->watching) {
		return;
	}
	conn->watching = watch;

	ev_io_stop(cmd->event_loop->loop, &conn->watcher);
	ev_io_set(&conn->watcher, conn->socket.fd, watch);
	ev_io_start(cmd->event_loop->loop, &conn->watcher);
}

#define AS_EVENT_WRITE_COMPLETE 0
#define AS_EVENT_WRITE_INCOMPLETE 1
#define AS_EVENT_WRITE_ERROR 2

#define AS_EVENT_READ_COMPLETE 3
#define AS_EVENT_READ_INCOMPLETE 4
#define AS_EVENT_READ_ERROR 5

#define AS_EVENT_TLS_NEED_READ 6
#define AS_EVENT_TLS_NEED_WRITE 7

#define AS_EVENT_COMMAND_DONE 8

//---------------------------------
// Libev TLS (BIO pair) Functions
//---------------------------------

// For async TLS connections OpenSSL is wired to an in-memory BIO pair instead
// of the socket fd (SSL_set_fd). SSL_read()/SSL_write() then operate purely on
// memory and never block on the kernel socket buffers. All socket I/O is
// performed explicitly here: ciphertext produced by OpenSSL (readable from the
// network BIO) is sent to the socket, and ciphertext from the socket is fed
// into the network BIO for OpenSSL to consume. The connection only ever parks
// on the socket-readiness direction that is actually blocked (send blocked ->
// watch write, recv blocked -> watch read). This avoids the deadlock the old
// SSL_set_fd design hit when a large TLS write filled the kernel send buffer
// over a high-latency link: the watcher could end up waiting on a readiness
// event that the peer would never produce. See issue #208.

typedef struct as_ev_tls {
	BIO* nbio;          // Network side of the BIO pair (SSL owns the other side).
	uint8_t* wbuf;      // Ciphertext pulled from nbio but not yet sent to socket.
	int wbuf_capacity;
	int wbuf_len;       // Total valid bytes in wbuf.
	int wbuf_pos;       // Bytes of wbuf already sent.
} as_ev_tls;

#define AS_EV_TLS_FLUSH_DONE 0        // All pending ciphertext sent to socket.
#define AS_EV_TLS_FLUSH_WOULDBLOCK 1  // Socket send would block; EV_WRITE armed.
#define AS_EV_TLS_FLUSH_ERROR -1

static void as_ev_connect_complete(as_event_command* cmd);

void
as_ev_tls_conn_free(as_event_connection* conn)
{
	as_ev_tls* tls = conn->tls;

	if (! tls) {
		return;
	}

	// The internal BIO is owned by the SSL object and freed by SSL_free()
	// in as_socket_close(). Only the network BIO and buffer are freed here.
	if (tls->nbio) {
		BIO_free(tls->nbio);
	}

	if (tls->wbuf) {
		cf_free(tls->wbuf);
	}

	cf_free(tls);
	conn->tls = NULL;
}

// Returns true if OpenSSL still has inbound data that can be consumed by
// SSL_read() without another socket read event: either decrypted plaintext
// waiting in the SSL object (SSL_pending), or ciphertext already pulled from
// the socket and buffered in the read BIO (BIO_pending on the read side of the
// pair). Used to drive the read-drain loop so coalesced TLS records spanning a
// single socket read are not stranded.
static inline bool
as_ev_tls_pending(as_event_connection* conn)
{
	if (! conn->socket.tls) {
		return false;
	}

	SSL* ssl = conn->socket.ssl;

	if (SSL_pending(ssl) > 0) {
		return true;
	}

	BIO* rbio = SSL_get_rbio(ssl);
	return rbio != NULL && BIO_pending(rbio) > 0;
}

// Send all ciphertext currently produced by OpenSSL (plus any previously
// buffered partial write) to the socket. Returns AS_EV_TLS_FLUSH_*.
static int
as_ev_tls_flush(as_event_command* cmd)
{
	as_event_connection* conn = cmd->conn;
	as_ev_tls* tls = conn->tls;
	int fd = conn->socket.fd;

	// Compact any partially-sent leftover to the front so newly produced
	// ciphertext can be appended after it.
	if (tls->wbuf_pos > 0) {
		if (tls->wbuf_pos < tls->wbuf_len) {
			memmove(tls->wbuf, tls->wbuf + tls->wbuf_pos, tls->wbuf_len - tls->wbuf_pos);
		}
		tls->wbuf_len -= tls->wbuf_pos;
		tls->wbuf_pos = 0;
	}

	// Pull all ciphertext OpenSSL has produced into wbuf.
	int pending = BIO_pending(tls->nbio);

	if (pending > 0) {
		int need = tls->wbuf_len + pending;

		if (need > tls->wbuf_capacity) {
			tls->wbuf = cf_realloc(tls->wbuf, need);
			tls->wbuf_capacity = need;
		}

		int n = BIO_read(tls->nbio, tls->wbuf + tls->wbuf_len, pending);

		if (n > 0) {
			tls->wbuf_len += n;
		}
	}

	// Send as much of wbuf as the socket will accept.
	while (tls->wbuf_pos < tls->wbuf_len) {
#if defined(__linux__)
		ssize_t bytes = send(fd, tls->wbuf + tls->wbuf_pos, tls->wbuf_len - tls->wbuf_pos, MSG_NOSIGNAL);
#else
		ssize_t bytes = write(fd, tls->wbuf + tls->wbuf_pos, tls->wbuf_len - tls->wbuf_pos);
#endif
		if (bytes > 0) {
			tls->wbuf_pos += (int)bytes;
			cmd->bytes_out += (uint32_t)bytes;
			continue;
		}

		if (bytes < 0) {
			if (as_last_error() == AS_WOULDBLOCK) {
				as_ev_watch_write(cmd);
				return AS_EV_TLS_FLUSH_WOULDBLOCK;
			}
			return AS_EV_TLS_FLUSH_ERROR;
		}
		// bytes == 0: connection closed by peer.
		return AS_EV_TLS_FLUSH_ERROR;
	}

	tls->wbuf_pos = 0;
	tls->wbuf_len = 0;
	return AS_EV_TLS_FLUSH_DONE;
}

// Read available ciphertext from the socket and feed it into the BIO pair so
// OpenSSL can decrypt it. Returns:
//   1  socket read would block (no data available right now)
//   0  some data was fed into the BIO (or the BIO is full and SSL can proceed)
//  -1  socket error / closed by peer
static int
as_ev_tls_recv(as_event_command* cmd)
{
	as_event_connection* conn = cmd->conn;
	as_ev_tls* tls = conn->tls;
	int fd = conn->socket.fd;

	// Only read as much as the BIO pair can accept so no socket bytes are lost.
	int space = (int)BIO_ctrl_get_write_guarantee(tls->nbio);

	if (space <= 0) {
		// BIO full: OpenSSL can make progress from what is already buffered.
		return 0;
	}

	uint8_t buf[16 * 1024];

	if (space > (int)sizeof(buf)) {
		space = (int)sizeof(buf);
	}

	ssize_t bytes = read(fd, buf, space);

	if (bytes > 0) {
		int pos = 0;

		while (pos < bytes) {
			int w = BIO_write(tls->nbio, buf + pos, (int)bytes - pos);

			if (w <= 0) {
				// Should not happen: read was sized to the write guarantee.
				return -1;
			}
			pos += w;
		}
		return 0;
	}

	if (bytes < 0) {
		if (as_last_error() == AS_WOULDBLOCK) {
			return 1;
		}
		return -1;
	}
	// bytes == 0: connection closed by peer.
	return -1;
}

static int
as_ev_tls_write_error(as_event_command* cmd, int rv)
{
	if (! as_event_socket_retry(cmd)) {
		as_error err;
		as_socket_error(cmd->conn->socket.fd, cmd->node, &err, AEROSPIKE_ERR_TLS_ERROR, "TLS write failed", rv);
		as_event_socket_error(cmd, &err);
	}
	return AS_EVENT_WRITE_ERROR;
}

static int
as_ev_tls_read_error(as_event_command* cmd, int rv)
{
	if (! as_event_socket_retry(cmd)) {
		as_error err;
		as_socket_error(cmd->conn->socket.fd, cmd->node, &err, AEROSPIKE_ERR_TLS_ERROR, "TLS read failed", rv);
		as_event_socket_error(cmd, &err);
	}
	return AS_EVENT_READ_ERROR;
}

static void
as_ev_tls_connect_error(as_event_command* cmd)
{
	if (! as_event_socket_retry(cmd)) {
		as_error err;
		as_error_set_message(&err, AEROSPIKE_ERR_TLS_ERROR, "TLS connection failed");
		as_event_socket_error(cmd, &err);
	}
}

// Drive the TLS handshake using the BIO pair. On completion calls
// as_ev_connect_complete(); on failure errors/retries the command. After this
// returns the caller must not touch cmd (it may have been freed or re-queued).
static void
as_ev_tls_handshake(as_event_command* cmd, as_event_connection* conn)
{
	SSL* ssl = conn->socket.ssl;

	// Feed any ciphertext waiting on the socket into the BIO pair. "Would
	// block" (rv == 1) just means no new data right now.
	int rr = as_ev_tls_recv(cmd);

	if (rr < 0) {
		as_ev_tls_connect_error(cmd);
		return;
	}

	int rv = SSL_do_handshake(ssl);
	int e = (rv == 1) ? SSL_ERROR_NONE : SSL_get_error(ssl, rv);

	// Always push whatever output the handshake produced to the socket.
	int fr = as_ev_tls_flush(cmd);

	if (fr == AS_EV_TLS_FLUSH_ERROR) {
		as_ev_tls_connect_error(cmd);
		return;
	}

	if (rv == 1) {
		if (fr == AS_EV_TLS_FLUSH_WOULDBLOCK) {
			// Final flight still draining. SSL_is_init_finished() is true now,
			// so the next writable event re-enters here: recv() yields nothing,
			// SSL_do_handshake() returns 1 again, the flush completes, and we
			// fall through to as_ev_connect_complete().
			return;
		}
		as_ev_connect_complete(cmd);
		return;
	}

	if (e == SSL_ERROR_WANT_READ || e == SSL_ERROR_WANT_WRITE) {
		if (fr == AS_EV_TLS_FLUSH_WOULDBLOCK) {
			// Waiting for the socket to accept output (EV_WRITE armed).
			return;
		}
		// Output fully sent; wait for the peer's response.
		as_ev_watch_read(cmd);
		return;
	}

	// Hard handshake failure.
	as_ev_tls_connect_error(cmd);
}

// Replace the fd socket BIO created by as_tls_wrap()/SSL_set_fd() with an
// in-memory BIO pair and put the SSL object into client connect state.
static void
as_ev_tls_init(as_event_command* cmd)
{
	as_event_connection* conn = cmd->conn;
	SSL* ssl = conn->socket.ssl;

	as_ev_tls* tls = cf_malloc(sizeof(as_ev_tls));
	tls->nbio = NULL;
	tls->wbuf = NULL;
	tls->wbuf_capacity = 0;
	tls->wbuf_len = 0;
	tls->wbuf_pos = 0;
	conn->tls = tls;

	BIO* ibio = NULL;

	// Size 0 selects OpenSSL's default buffer (large enough for a full TLS
	// record), matching the libuv backend.
	BIO_new_bio_pair(&ibio, 0, &tls->nbio, 0);

	// SSL_set_bio() frees the previous fd BIO. That BIO was created with
	// BIO_NOCLOSE by SSL_set_fd(), so the socket fd is not affected.
	SSL_set_bio(ssl, ibio, ibio);
	SSL_set_connect_state(ssl);
}

static int
as_ev_write(as_event_command* cmd)
{
	uint8_t* buf = (uint8_t*)cmd + cmd->write_offset;

	if (cmd->conn->socket.tls) {
		SSL* ssl = cmd->conn->socket.ssl;

		// First push out any ciphertext left over from a previous partial
		// socket write.
		int fr = as_ev_tls_flush(cmd);

		if (fr == AS_EV_TLS_FLUSH_WOULDBLOCK) {
			return AS_EVENT_WRITE_INCOMPLETE;
		}
		if (fr == AS_EV_TLS_FLUSH_ERROR) {
			return as_ev_tls_write_error(cmd, 0);
		}

		// Feed plaintext into OpenSSL and flush the resulting ciphertext.
		while (cmd->pos < cmd->len) {
			int rv = SSL_write(ssl, buf + cmd->pos, (int)(cmd->len - cmd->pos));

			if (rv > 0) {
				cmd->pos += rv;

				fr = as_ev_tls_flush(cmd);
				if (fr == AS_EV_TLS_FLUSH_WOULDBLOCK) {
					return AS_EVENT_WRITE_INCOMPLETE;
				}
				if (fr == AS_EV_TLS_FLUSH_ERROR) {
					return as_ev_tls_write_error(cmd, rv);
				}
				continue;
			}

			int e = SSL_get_error(ssl, rv);

			if (e == SSL_ERROR_WANT_WRITE) {
				// The BIO pair output buffer is full. Drain it to the socket
				// to make room, then retry SSL_write.
				fr = as_ev_tls_flush(cmd);
				if (fr == AS_EV_TLS_FLUSH_WOULDBLOCK) {
					return AS_EVENT_WRITE_INCOMPLETE;
				}
				if (fr == AS_EV_TLS_FLUSH_ERROR) {
					return as_ev_tls_write_error(cmd, rv);
				}
				continue;
			}

			if (e == SSL_ERROR_WANT_READ) {
				// OpenSSL needs to read (e.g. TLS 1.3 post-handshake / re-key).
				// Flush any output, then feed it socket data and retry.
				fr = as_ev_tls_flush(cmd);
				if (fr == AS_EV_TLS_FLUSH_WOULDBLOCK) {
					return AS_EVENT_WRITE_INCOMPLETE;
				}
				if (fr == AS_EV_TLS_FLUSH_ERROR) {
					return as_ev_tls_write_error(cmd, rv);
				}

				int rr = as_ev_tls_recv(cmd);
				if (rr == 1) {
					// No socket data yet; wait for it, then resume the write.
					as_ev_watch_read(cmd);
					return AS_EVENT_TLS_NEED_READ;
				}
				if (rr < 0) {
					return as_ev_tls_write_error(cmd, rv);
				}
				continue;
			}

			// Hard TLS error.
			return as_ev_tls_write_error(cmd, rv);
		}
	}
	else {
		int fd = cmd->conn->socket.fd;
		ssize_t bytes;
	
		do {
#if defined(__linux__)
			bytes = send(fd, buf + cmd->pos, cmd->len - cmd->pos, MSG_NOSIGNAL);
#else
			bytes = write(fd, buf + cmd->pos, cmd->len - cmd->pos);
#endif
			if (bytes > 0) {
				cmd->pos += bytes;
				cmd->bytes_out += bytes;
				continue;
			}
		
			if (bytes < 0) {
				int e = as_last_error();

				if (e == AS_WOULDBLOCK) {
					as_ev_watch_write(cmd);
					return AS_EVENT_WRITE_INCOMPLETE;
				}

				if (! as_event_socket_retry(cmd)) {
					as_error err;
					as_socket_error(fd, cmd->node, &err, AEROSPIKE_ERR_ASYNC_CONNECTION, "Socket write failed", e);
					as_event_socket_error(cmd, &err);
				}
				return AS_EVENT_WRITE_ERROR;
			}
			else {
				if (! as_event_socket_retry(cmd)) {
					as_error err;
					as_socket_error(fd, cmd->node, &err, AEROSPIKE_ERR_ASYNC_CONNECTION, "Socket write closed by peer", 0);
					as_event_socket_error(cmd, &err);
				}
				return AS_EVENT_WRITE_ERROR;
			}
		} while (cmd->pos < cmd->len);
	}

	// Socket timeout applies only to read events.
	// Reset event received because we are switching from a write to a read state.
	// This handles case where write succeeds and read event does not occur.  If we didn't reset,
	// the socket timeout would go through two iterations (double the timeout) because a write
	// event occurred in the first timeout period.
	cmd->flags &= ~AS_ASYNC_FLAGS_EVENT_RECEIVED;
	return AS_EVENT_WRITE_COMPLETE;
}

static int
as_ev_read(as_event_command* cmd)
{
	cmd->flags |= AS_ASYNC_FLAGS_EVENT_RECEIVED;

	if (cmd->conn->socket.tls) {
		SSL* ssl = cmd->conn->socket.ssl;

		while (cmd->pos < cmd->len) {
			int rv = SSL_read(ssl, cmd->buf + cmd->pos, (int)(cmd->len - cmd->pos));

			if (rv > 0) {
				cmd->pos += rv;
				continue;
			}

			int e = SSL_get_error(ssl, rv);

			if (e == SSL_ERROR_WANT_READ) {
				// Need more ciphertext from the socket.
				int rr = as_ev_tls_recv(cmd);
				if (rr == 1) {
					as_ev_watch_read(cmd);
					return AS_EVENT_READ_INCOMPLETE;
				}
				if (rr < 0) {
					return as_ev_tls_read_error(cmd, rv);
				}
				continue;
			}

			if (e == SSL_ERROR_WANT_WRITE) {
				// OpenSSL needs to write (e.g. handshake / re-key during read).
				int fr = as_ev_tls_flush(cmd);
				if (fr == AS_EV_TLS_FLUSH_WOULDBLOCK) {
					return AS_EVENT_TLS_NEED_WRITE;
				}
				if (fr == AS_EV_TLS_FLUSH_ERROR) {
					return as_ev_tls_read_error(cmd, rv);
				}
				continue;
			}

			// SSL_ERROR_ZERO_RETURN (peer closed) or a hard error.
			return as_ev_tls_read_error(cmd, rv);
		}
	}
	else {
		int fd = cmd->conn->socket.fd;
		ssize_t bytes;
	
		do {
			bytes = read(fd, cmd->buf + cmd->pos, cmd->len - cmd->pos);
		
			if (bytes > 0) {
				cmd->pos += bytes;
				cmd->bytes_in += bytes;
				continue;
			}
		
			if (bytes < 0) {
				int e = as_last_error();

				if (e == AS_WOULDBLOCK) {
					as_ev_watch_read(cmd);
					return AS_EVENT_READ_INCOMPLETE;
				}

				if (! as_event_socket_retry(cmd)) {
					as_error err;
					as_socket_error(fd, cmd->node, &err, AEROSPIKE_ERR_ASYNC_CONNECTION, "Socket read failed", e);
					as_event_socket_error(cmd, &err);
				}
				return AS_EVENT_READ_ERROR;
			}
			else {
				if (! as_event_socket_retry(cmd)) {
					as_error err;
					as_socket_error(fd, cmd->node, &err, AEROSPIKE_ERR_ASYNC_CONNECTION, "Socket read closed by peer", 0);
					as_event_socket_error(cmd, &err);
				}
				return AS_EVENT_READ_ERROR;
			}
		} while (cmd->pos < cmd->len);
	}
	
	return AS_EVENT_READ_COMPLETE;
}

static inline void
as_ev_command_read_start(as_event_command* cmd)
{
	cmd->command_sent_counter++;
	cmd->len = sizeof(as_proto);
	cmd->pos = 0;
	cmd->state = AS_ASYNC_STATE_COMMAND_READ_HEADER;

	as_ev_watch_read(cmd);
	
	if (cmd->pipe_listener != NULL) {
		as_pipe_read_start(cmd);
	}
}

static inline void
as_ev_command_write(as_event_command* cmd)
{
	as_ev_watch_write(cmd);

	if (as_ev_write(cmd) == AS_EVENT_WRITE_COMPLETE) {
		// Done with write. Register for read.
		as_ev_command_read_start(cmd);
	}
}

void
as_event_command_write_start(as_event_command* cmd)
{
	cmd->state = AS_ASYNC_STATE_COMMAND_WRITE;
	as_event_set_write(cmd);
	as_ev_command_write(cmd);
}

static int
as_ev_command_start(as_event_command* cmd)
{
	if (as_event_connection_complete(cmd)) {
		return AS_EVENT_COMMAND_DONE;
	}

	as_event_command_write_start(cmd);
	return AS_EVENT_READ_COMPLETE;
}

static inline void
as_ev_command_auth_write(as_event_command* cmd)
{
	as_ev_watch_write(cmd);
		
	if (as_ev_write(cmd) == AS_EVENT_WRITE_COMPLETE) {
		// Done with auth write. Register for auth read.
		as_event_set_auth_read_header(cmd);
		as_ev_watch_read(cmd);
	}
}

static void
as_ev_connect_complete(as_event_command* cmd)
{
	if (cmd->cluster->auth_enabled) {
		as_session* session = as_session_load(&cmd->node->session);

		if (session) {
			as_incr_uint32(&session->ref_count);
			as_event_set_auth_write(cmd, session);
			as_session_release(session);

			cmd->state = AS_ASYNC_STATE_AUTH_WRITE;
			as_ev_command_auth_write(cmd);
		}
		else {
			as_ev_command_start(cmd);
		}
	}
	else {
		as_ev_command_start(cmd);
	}
}

static int
as_ev_command_peek_block(as_event_command* cmd)
{
	// Batch, scan, query may be waiting on end block.
	// Prepare for next message block.
	cmd->len = sizeof(as_proto);
	cmd->pos = 0;
	cmd->state = AS_ASYNC_STATE_COMMAND_READ_HEADER;

	int rv = as_ev_read(cmd);
	if (rv != AS_EVENT_READ_COMPLETE) {
		return rv;
	}
	
	as_proto* proto = (as_proto*)cmd->buf;

	if (! as_event_proto_parse(cmd, proto)) {
		return AS_EVENT_READ_ERROR;
	}

	size_t size = proto->sz;
	
	cmd->len = (uint32_t)size;
	cmd->pos = 0;
	cmd->state = AS_ASYNC_STATE_COMMAND_READ_BODY;
	
	// Check for end block size.
	if (cmd->len == sizeof(as_msg) && cmd->proto_type_rcv != AS_COMPRESSED_MESSAGE_TYPE) {
		// Look like we received end block.  Read and parse to make sure.
		rv = as_ev_read(cmd);
		if (rv != AS_EVENT_READ_COMPLETE) {
			return rv;
		}
		cmd->pos = 0;

		if (! cmd->parse_results(cmd)) {
			// We did not finish after all. Prepare to read next header.
			cmd->len = sizeof(as_proto);
			cmd->pos = 0;
			cmd->state = AS_ASYNC_STATE_COMMAND_READ_HEADER;
		}
		else {
			return AS_EVENT_COMMAND_DONE;
		}
	}
	else {
		// Received normal data block.  Stop reading for fairness reasons and wait
		// till next iteration.
		if (cmd->len > cmd->read_capacity) {
			if (cmd->flags & AS_ASYNC_FLAGS_FREE_BUF) {
				cf_free(cmd->buf);
			}
			cmd->buf = cf_malloc(size);
			cmd->read_capacity = cmd->len;
			cmd->flags |= AS_ASYNC_FLAGS_FREE_BUF;
		}
	}

	return AS_EVENT_READ_COMPLETE;
}

static int
as_ev_parse_authentication(as_event_command* cmd)
{
	int rv;
	if (cmd->state == AS_ASYNC_STATE_AUTH_READ_HEADER) {
		// Read response length
		rv = as_ev_read(cmd);
		if (rv != AS_EVENT_READ_COMPLETE) {
			return rv;
		}

		if (! as_event_set_auth_parse_header(cmd)) {
			return AS_EVENT_READ_ERROR;
		}

		if (cmd->len > cmd->read_capacity) {
			as_error err;
			as_error_update(&err, AEROSPIKE_ERR_CLIENT, "Authenticate response size is corrupt: %u", cmd->len);
			as_event_parse_error(cmd, &err);
			return AS_EVENT_READ_ERROR;
		}
	}

	rv = as_ev_read(cmd);
	if (rv != AS_EVENT_READ_COMPLETE) {
		return rv;
	}
	
	// Parse authentication response.
	uint8_t code = cmd->buf[AS_ASYNC_AUTH_RETURN_CODE];
	
	if (code && code != AEROSPIKE_SECURITY_NOT_ENABLED) {
		// Can't authenticate socket, so must close it.
		as_node_signal_login(cmd->node);
		as_error err;
		as_error_update(&err, code, "Authentication failed: %s", as_error_string(code));
		as_event_parse_error(cmd, &err);
		return AS_EVENT_READ_ERROR;
	}

	return as_ev_command_start(cmd);
}

static int
as_ev_command_read(as_event_command* cmd)
{
	int rv;

	if (cmd->state == AS_ASYNC_STATE_COMMAND_READ_HEADER) {
		// Read response length
		rv = as_ev_read(cmd);
		if (rv != AS_EVENT_READ_COMPLETE) {
			return rv;
		}
		
		as_proto* proto = (as_proto*)cmd->buf;

		if (! as_event_proto_parse(cmd, proto)) {
			return AS_EVENT_READ_ERROR;
		}

		size_t size = proto->sz;
		
		cmd->len = (uint32_t)size;
		cmd->pos = 0;
		cmd->state = AS_ASYNC_STATE_COMMAND_READ_BODY;
		
		if (cmd->len > cmd->read_capacity) {
			if (cmd->flags & AS_ASYNC_FLAGS_FREE_BUF) {
				cf_free(cmd->buf);
			}
			cmd->buf = cf_malloc(size);
			cmd->read_capacity = cmd->len;
			cmd->flags |= AS_ASYNC_FLAGS_FREE_BUF;
		}
	}
	
	// Read response body
	rv = as_ev_read(cmd);
	if (rv != AS_EVENT_READ_COMPLETE) {
		return rv;
	}
	cmd->pos = 0;

	if (cmd->proto_type_rcv == AS_COMPRESSED_MESSAGE_TYPE) {
		if (! as_event_decompress(cmd)) {
			return AS_EVENT_READ_ERROR;
		}
	}

	if (! cmd->parse_results(cmd)) {
		// Batch, scan, query is not finished.
		return as_ev_command_peek_block(cmd);
	}

	return AS_EVENT_COMMAND_DONE;		
}

static void
as_ev_callback_common(as_event_command* cmd, as_event_connection* conn) {
	switch (cmd->state) {
	case AS_ASYNC_STATE_CONNECT:
		as_ev_connect_complete(cmd);
		break;

	case AS_ASYNC_STATE_TLS_CONNECT:
		// as_ev_tls_handshake() drives reads/writes against the BIO pair and
		// completes or errors the command. Do not touch cmd afterwards.
		as_ev_tls_handshake(cmd, conn);
		break;

	case AS_ASYNC_STATE_AUTH_WRITE:
		as_ev_command_auth_write(cmd);
		break;

	case AS_ASYNC_STATE_AUTH_READ_HEADER:
	case AS_ASYNC_STATE_AUTH_READ_BODY:
		// If we're using TLS we must loop until there are no bytes
		// left in the encryption buffer because we won't get another
		// read event from libev.
		do {
			switch (as_ev_parse_authentication(cmd)) {
				case AS_EVENT_COMMAND_DONE:
				case AS_EVENT_READ_ERROR:
					// Do not touch cmd again because it's been deallocated.
					return;

				case AS_EVENT_READ_COMPLETE:
					as_ev_watch_read(cmd);
					break;

				default:
					break;
			}
		} while (as_ev_tls_pending(cmd->conn));
		break;

	case AS_ASYNC_STATE_COMMAND_WRITE:
		as_ev_command_write(cmd);
		break;

	case AS_ASYNC_STATE_COMMAND_READ_HEADER:
	case AS_ASYNC_STATE_COMMAND_READ_BODY:
		// If we're using TLS we must loop until there are no bytes
		// left in the encryption buffer because we won't get another
		// read event from libev.
		do {
			switch (as_ev_command_read(cmd)) {
			case AS_EVENT_COMMAND_DONE:
			case AS_EVENT_READ_ERROR:
				// Do not touch cmd again because it's been deallocated.
				return;
			
			case AS_EVENT_READ_COMPLETE:
				as_ev_watch_read(cmd);
				break;
				
			default:
				break;
			}
		} while (as_ev_tls_pending(cmd->conn));
		break;

	default:
		as_log_error("unexpected cmd state %d", cmd->state);
		break;
	}
}

static void
as_ev_callback(struct ev_loop* loop, ev_io* watcher, int revents)
{
	if (revents & EV_READ) {
		as_event_connection* conn = watcher->data;
		as_event_command* cmd;
		
		if (conn->pipeline) {
			as_pipe_connection* pipe = (as_pipe_connection*)conn;
			
			if (pipe->writer && cf_ll_size(&pipe->readers) == 0) {
				// Authentication response will only have a writer.
				cmd = pipe->writer;
			}
			else {
				// Next response is at head of reader linked list.
				cf_ll_element* link = cf_ll_get_head(&pipe->readers);
				
				if (link) {
					cmd = as_pipe_link_to_command(link);
				}
				else {
					as_log_debug("Pipeline read event ignored");
					return;
				}
			}
		}
		else {
			cmd = ((as_async_connection*)conn)->cmd;
		}

		as_ev_callback_common(cmd, conn);
	}
	else if (revents & EV_WRITE) {
		as_event_connection* conn = watcher->data;
		
		as_event_command* cmd = conn->pipeline ?
			((as_pipe_connection*)conn)->writer :
			((as_async_connection*)conn)->cmd;

		as_ev_callback_common(cmd, conn);
	}
	else if (revents & EV_ERROR) {
		as_log_error("Async error occurred: %d", revents);
	}
	else {
		as_log_warn("Unknown event received: %d", revents);
	}
}

static void
as_ev_watcher_init(as_event_command* cmd, as_socket* sock)
{
	as_event_connection* conn = cmd->conn;
	memcpy(&conn->socket, sock, sizeof(as_socket));
	conn->tls = NULL;

	// Change state if using TLS.
	if (as_socket_use_tls(cmd->cluster->tls_ctx)) {
		// Replace the fd socket BIO created by as_socket_wrap() with an
		// in-memory BIO pair so SSL I/O never blocks on the kernel socket.
		as_ev_tls_init(cmd);
		cmd->state = AS_ASYNC_STATE_TLS_CONNECT;
	}

	int watch = cmd->pipe_listener != NULL ? EV_WRITE | EV_READ : EV_WRITE;
	conn->watching = watch;
	
	ev_io_init(&conn->watcher, as_ev_callback, conn->socket.fd, watch);
	conn->watcher.data = conn;
	ev_io_start(cmd->event_loop->loop, &conn->watcher);
}

static int
as_ev_try_connections(int fd, as_address* addresses, socklen_t size, int i, int max)
{
	while (i < max) {
		if (as_socket_connect_fd(fd, (struct sockaddr*)&addresses[i].addr, size)) {
			return i;
		}
		i++;
	}
	return -1;
}

static int
as_ev_try_family_connections(as_event_command* cmd, int family, int begin, int end, int index, as_address* primary, as_socket* sock)
{
	// Create a non-blocking socket.
	as_socket_fd fd;
	int rv = as_socket_create_fd(family, &fd);

	if (rv < 0) {
		return rv;
	}

	if (cmd->pipe_listener && ! as_pipe_modify_fd(fd)) {
		return -1000;
	}

	as_tls_context* ctx = as_socket_get_tls_context(cmd->cluster->tls_ctx);

	if (! as_socket_wrap(sock, family, fd, ctx, cmd->node->tls_name)) {
		return -1001;
	}

	// Try addresses.
	as_address* addresses = cmd->node->addresses;
	socklen_t size = (family == AF_INET)? sizeof(struct sockaddr_in) : sizeof(struct sockaddr_in6);

	if (index >= 0) {
		// Try primary address.
		if (as_socket_connect_fd(fd, (struct sockaddr*)&primary->addr, size)) {
			return index;
		}
		
		// Start from current index + 1 to end.
		rv = as_ev_try_connections(fd, addresses, size, index + 1, end);
		
		if (rv < 0) {
			// Start from begin to index.
			rv = as_ev_try_connections(fd, addresses, size, begin, index);
		}
	}
	else {
		rv = as_ev_try_connections(fd, addresses, size, begin, end);
	}
	
	if (rv < 0) {
		// Couldn't start a connection on any socket address - close the socket.
		as_socket_close(sock);
		return -1002;
	}
	return rv;
}

static void
as_ev_connect_error(as_event_command* cmd, as_address* primary, int rv)
{
	// Socket has already been closed. Release connection.
	cf_free(cmd->conn);
	as_event_decr_conn(cmd);
	cmd->event_loop->errors++;

	if (as_event_command_retry(cmd, false)) {
		return;
	}

	as_error err;
	as_error_update(&err, AEROSPIKE_ERR_ASYNC_CONNECTION, "Connect failed: %d %s %s", rv, cmd->node->name, primary->name);

	// Only timer needs to be released on socket connection failure.
	// Watcher has not been registered yet.
	as_event_timer_stop(cmd);
	as_event_error_callback(cmd, &err);
}

void
as_event_connect(as_event_command* cmd, as_async_conn_pool* pool)
{
	// Initialize TLS state up front so any failure before as_ev_watcher_init()
	// leaves conn->tls in a known state.
	cmd->conn->tls = NULL;

	// Try addresses.
	as_socket sock;
	as_node* node = cmd->node;
	uint32_t index = node->address_index;
	as_address* primary = &node->addresses[index];
	int rv;
	int first_rv;

	if (primary->addr.ss_family == AF_INET) {
		// Try IPv4 addresses first.
		rv = as_ev_try_family_connections(cmd, AF_INET, 0, node->address4_size, index, primary, &sock);
		
		if (rv < 0) {
			// Try IPv6 addresses.
			first_rv = rv;
			rv = as_ev_try_family_connections(cmd, AF_INET6, AS_ADDRESS4_MAX, AS_ADDRESS4_MAX + node->address6_size, -1, NULL, &sock);
		}
	}
	else {
		// Try IPv6 addresses first.
		rv = as_ev_try_family_connections(cmd, AF_INET6, AS_ADDRESS4_MAX, AS_ADDRESS4_MAX + node->address6_size, index, primary, &sock);
		
		if (rv < 0) {
			// Try IPv4 addresses.
			first_rv = rv;
			rv = as_ev_try_family_connections(cmd, AF_INET, 0, node->address4_size, -1, NULL, &sock);
		}
	}
	
	if (rv < 0) {
		as_ev_connect_error(cmd, primary, first_rv);
		return;
	}
	
	if (rv != index) {
		// Replace invalid primary address with valid alias.
		// Other threads may not see this change immediately.
		// It's just a hint, not a requirement to try this new address first.
		as_store_uint32(&node->address_index, rv);
		as_log_debug("Change node address %s %s", node->name, as_node_get_address_string(node));
	}

	pool->opened++;
	as_ev_watcher_init(cmd, &sock);
	cmd->event_loop->errors = 0; // Reset errors on valid connection.
}

void
as_ev_timer_cb(struct ev_loop* loop, ev_timer* timer, int revents)
{
	as_event_process_timer(timer->data);
}

void
as_ev_repeat_cb(struct ev_loop* loop, ev_timer* timer, int revents)
{
	as_event_socket_timeout(timer->data);
}

static void
as_ev_close_connections(as_node* node, as_async_conn_pool* pool)
{
	as_event_connection* conn;
	
	while (as_queue_pop(&pool->queue, &conn)) {
		as_event_release_connection(conn, pool);
	}
	as_queue_destroy(&pool->queue);
}

void
as_event_node_destroy(as_node* node)
{
	// Close connections.
	for (uint32_t i = 0; i < as_event_loop_size; i++) {
		as_ev_close_connections(node, &node->async_conn_pools[i]);
		as_ev_close_connections(node, &node->pipe_conn_pools[i]);
	}
	cf_free(node->async_conn_pools);
	cf_free(node->pipe_conn_pools);
}

#endif
