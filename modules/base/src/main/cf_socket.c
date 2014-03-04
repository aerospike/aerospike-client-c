/******************************************************************************
 * Copyright 2008-2013 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy 
 * of this software and associated documentation files (the "Software"), to 
 * deal in the Software without restriction, including without limitation the 
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or 
 * sell copies of the Software, and to permit persons to whom the Software is 
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in 
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING 
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 *****************************************************************************/

#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include <citrusleaf/cf_byte_order.h>
#include <citrusleaf/cf_clock.h>
#include <citrusleaf/cf_errno.h>
#include <citrusleaf/cf_log_internal.h>
#include <citrusleaf/cf_socket.h>

#if ! defined(CF_WINDOWS)

#include <fcntl.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/event.h>
#include <sys/time.h>

#if defined(__linux__)
#include <unistd.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#endif // __linux__

#if defined(__APPLE__)
#define SOL_TCP IPPROTO_TCP
#endif // __APPLE__


// #define DEBUG_TIME

#ifdef DEBUG_TIME
#include <pthread.h>
static void debug_time_printf(const char* desc, int try, int busy, uint64_t start, uint64_t end, uint64_t deadline)
{
	cf_info("%s|%zu|%d|%d|%lu|%lu|%lu",
		desc, (uint64_t)pthread_self(),
		try,
		busy,
		start,
		end,
		deadline
	);
}
#endif


int
cf_socket_create_nb()
{
	// Create the socket.
	int fd = socket(AF_INET, SOCK_STREAM, 0);

	if (-1 == fd) {
		cf_warn("could not allocate socket, errno %d", errno);
		return -1;
	}

    // Make the socket nonblocking.
	int flags = fcntl(fd, F_GETFL, 0);

    if (flags < 0) {
		cf_warn("could not read socket flags");
		close(fd);
        return -1;
	}

    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
		cf_warn("could not set socket nonblocking");
		close(fd);
        return -1;
	}

	int f = 1;
	setsockopt(fd, SOL_TCP, TCP_NODELAY, &f, sizeof(f));

	return fd;
}

void
cf_print_sockaddr_in(char *prefix, struct sockaddr_in *sa_in)
{
	char str[INET_ADDRSTRLEN];
	inet_ntop(AF_INET, &(sa_in->sin_addr), str, INET_ADDRSTRLEN);
	cf_error("%s %s:%d", prefix, str, (int)cf_swap_from_be16(sa_in->sin_port));
}


#if defined(__linux__)

static char *
my_strerror_r(const int err, char *errbuf, size_t errbuf_len)
{
	if (errbuf) {
		errbuf[0] = '\0';
		strerror_r(errno, errbuf, errbuf_len);
	}
	
	return errbuf;
}


//
// Network socket helpers
// Often, you know the amount you want to read, and you have a timeout.
//
// Note that we now use epoll(7) for effecting the read/write timeout,
// since select(2) is limited by the different (and possibly conflicting)
// definitions (including size limitations) of "fd_set" on different
// compiler tool chains and platforms.
//
// There are two timeouts: the total deadline for the transaction and the
// maximum time this attempt can take without making progress on a connection,
// which we consider a failure so we can flip over to another node that might
// be healthier.
//
// Returns 0 upon success, or else the error number upon failure.
//
int
cf_socket_read_timeout(int fd, uint8_t *buf, size_t buf_len, uint64_t trans_deadline, int attempt_ms)
{
	if (!buf_len) {
		return 0;
	}
    
	char *ctx = "cf_socket_read_timeout()";
	int errbuf_len = 64;
	char errbuf[errbuf_len];
    
	int flags;
	if (-1 == (flags = fcntl(fd, F_GETFL, 0))) {
		flags = 0;
	}
	if (!(flags & O_NONBLOCK)) {
		if (-1 == fcntl(fd, F_SETFL, flags | O_NONBLOCK)) {
			return(ENOENT);
		}
	}
    
	// between the transaction deadline and the attempt_ms, find the lesser
	// and create a deadline for this attempt
	uint64_t deadline = attempt_ms + cf_getms();
	if ((trans_deadline != 0) && (trans_deadline < deadline)) {
		deadline = trans_deadline;
	}
    
	int rv = 0, epoll_fd = -1, busy = 0, try = 0;
	size_t pos = 0;
	uint64_t start = cf_getms();
    
	if (0 > (epoll_fd = epoll_create(1))) {
		rv = errno;
		cf_warn("%s: epoll_create() failed (errno %d: \"%s\") ~~ Failing!", ctx, errno, my_strerror_r(errno, errbuf, errbuf_len));
		goto Fail;
	}
    
	struct epoll_event event;
	memset(&event, 0, sizeof(struct epoll_event));
	event.data.fd = fd;
	event.events = EPOLLIN;
    
	if (0 > epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &event)) {
		rv = errno;
		cf_warn("%s: epoll_ctl(ADD) of socket failed (errno %d: \"%s\") ~~ Failing!", ctx, errno, my_strerror_r(errno, errbuf, errbuf_len));
		goto Fail;
	}
    
	do {
		uint64_t now = cf_getms();
		if (now > deadline) {
#ifdef DEBUG_TIME
			debug_time_printf("socket read timeout 1", try, busy, start, now, deadline);
#endif
			rv = ETIMEDOUT;
			goto Fail;
		}
        
		uint64_t ms_left = deadline - now;
        
		int nevents = 0;
		int max_events = 1;
		int wait_ms = 1;
		struct epoll_event events[max_events];
        
		if (0 > (nevents = epoll_wait(epoll_fd, events, max_events, wait_ms))) {
			if ((rv = errno) == EINTR) {
				cf_debug("%s: epoll_wait() on socket encountered EINTR ~~ Retrying!", ctx);
				busy++;
				goto Retry;
			} else {
				cf_warn("%s: epoll_wait() on socket failed (errno %d: \"%s\") ~~ Failing!", ctx, errno, my_strerror_r(errno, errbuf, errbuf_len));
			}
			goto Fail;
		} else {
			if (nevents == 0) {
				cf_debug("%s: epoll_wait() returned no events ~~ Retrying!", ctx);
				busy++;
				goto Retry;
			}
			if (nevents != 1) {
				cf_warn("%s: epoll_wait() returned %d events ~~ only 1 expected, so ignoring others!", ctx, nevents);
			}
			if (events[0].data.fd == fd) {
				if (events[0].events & EPOLLIN) {
					cf_debug("%s: epoll_wait() on socket ready for read detected ~~ Succeeding!", ctx);
				} else {
					// (Note:  ERR and HUP events are automatically waited for as well.)
					if (events[0].events & (EPOLLERR | EPOLLHUP)) {
						cf_debug("%s: epoll_wait() on socket detected failure event 0x%x ~~ Failing!", ctx, events[0].events);
					} else {
						cf_warn("%s: epoll_wait() on socket detected non-read events 0x%x ~~ Failing!", ctx, events[0].events);
					}
					rv = EBADF;
					goto Fail;
				}
			} else {
				cf_warn("%s: epoll_wait() on socket returned event on unknown socket %d ~~ Retrying!", ctx, events[0].data.fd);
				goto Retry;
			}
            
			int r_bytes = read(fd, buf + pos, buf_len - pos);
            
			if (r_bytes > 0) {
				pos += r_bytes;
				if (pos >= buf_len)	{
					goto Success;
				}
			} else if (r_bytes == 0) {
				// It's likely the socket has been closed on the remote side.
				rv = EBADF;
				goto Fail;
			} else if ((errno != ETIMEDOUT) && (errno != EWOULDBLOCK) && (errno != EINPROGRESS) && (errno != EAGAIN)) {
#ifdef DEBUG_TIME
				debug_time_printf("socket read timeout 2", try, busy, start, now, deadline);
#endif
				rv = errno;
				goto Fail;
			}
		}
        
    Retry:
		try++;
        
	} while (pos < buf_len);
    
Success:
	if (0 > epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, &event)) {
		cf_warn("%s: epoll_ctl(DEL) on socket failed (errno %d: \"%s\")", ctx, errno, my_strerror_r(errno, errbuf, errbuf_len));
	}
	close(epoll_fd);
    
	rv = 0;
	goto Done;
    
Fail:
	cf_debug("%s: socket read timeout fail: %d (%s)", ctx, errno, my_strerror_r(errno, errbuf, errbuf_len));
    
	if (epoll_fd > 0) {
		if (0 > epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, &event)) {
			cf_warn("%s: epoll_ctl(DEL) on socket failed (errno %d: \"%s\")", ctx, errno, my_strerror_r(errno, errbuf, errbuf_len));
		}
		close(epoll_fd);
	}
    
Done:
	return(rv);
}


int
cf_socket_write_timeout(int fd, uint8_t *buf, size_t buf_len, uint64_t trans_deadline, int attempt_ms)
{
	if (!buf_len) {
		return 0;
	}
    
	char *ctx = "cf_socket_write_timeout()";
	int errbuf_len = 64;
	char errbuf[errbuf_len];
    
	int flags;
	if (-1 == (flags = fcntl(fd, F_GETFL, 0))) {
		flags = 0;
	}
	if (!(flags & O_NONBLOCK)) {
		if (-1 == fcntl(fd, F_SETFL, flags | O_NONBLOCK)) {
			return(ENOENT);
		}
	}
    
	// between the transaction deadline and the attempt_ms, find the lesser
	// and create a deadline for this attempt
	uint64_t deadline = attempt_ms + cf_getms();
	if ((trans_deadline != 0) && (trans_deadline < deadline)) {
		deadline = trans_deadline;
	}
    
	int rv = 0, epoll_fd = -1, busy = 0, try = 0;
	size_t pos = 0;
	uint64_t start = cf_getms();
    
	if (0 > (epoll_fd = epoll_create(1))) {
		rv = errno;
		cf_warn("%s: epoll_create() failed (errno %d: \"%s\") ~~ Failing!", ctx, errno, my_strerror_r(errno, errbuf, errbuf_len));
		goto Fail;
	}
    
	struct epoll_event event;
	memset(&event, 0, sizeof(struct epoll_event));
	event.data.fd = fd;
	event.events = EPOLLOUT;
    
	if (0 > epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &event)) {
		rv = errno;
		cf_warn("%s: epoll_ctl(ADD) of socket failed (errno %d: \"%s\") ~~ Failing!", ctx, errno, my_strerror_r(errno, errbuf, errbuf_len));
		goto Fail;
	}
    
	do {
		uint64_t now = cf_getms();
		if (now > deadline) {
#ifdef DEBUG_TIME
			debug_time_printf("socket write timeout 1", try, busy, start, now, deadline);
#endif
			rv = ETIMEDOUT;
			goto Fail;
		}
        
		uint64_t ms_left = deadline - now;
        
		int nevents = 0;
		int max_events = 1;
		int wait_ms = 1;
		struct epoll_event events[max_events];
        
		if (0 > (nevents = epoll_wait(epoll_fd, events, max_events, wait_ms))) {
			if ((rv = errno) == EINTR) {
				cf_debug("%s: epoll_wait() on socket encountered EINTR ~~ Retrying!", ctx);
				busy++;
				goto Retry;
			} else {
				cf_warn("%s: epoll_wait() on socket failed (errno %d: \"%s\") ~~ Failing!", ctx, errno, my_strerror_r(errno, errbuf, errbuf_len));
			}
			goto Fail;
		} else {
			if (nevents == 0) {
				cf_debug("%s: epoll_wait() returned no events ~~ Retrying!", ctx);
				busy++;
				goto Retry;
			}
			if (nevents != 1) {
				cf_warn("%s: epoll_wait() returned %d events ~~ only 1 expected, so ignoring others!", ctx, nevents);
			}
			if (events[0].data.fd == fd) {
				if (events[0].events & EPOLLOUT) {
					cf_debug("%s: epoll_wait() on socket ready for write detected ~~ Succeeding!", ctx);
				} else {
					// (Note:  ERR and HUP events are automatically waited for as well.)
					if (events[0].events & (EPOLLERR | EPOLLHUP)) {
						cf_debug("%s: epoll_wait() on socket detected failure event 0x%x ~~ Failing!", ctx, events[0].events);
					} else {
						cf_warn("%s: epoll_wait() on socket detected non-write events 0x%x ~~ Failing!", ctx, events[0].events);
					}
					rv = EBADF;
					goto Fail;
				}
			} else {
				cf_warn("%s: epoll_wait() on socket returned event on unknown socket %d ~~ Retrying!", ctx, events[0].data.fd);
				goto Retry;
			}
            
			int r_bytes = write(fd, buf + pos, buf_len - pos);
            
			if (r_bytes > 0) {
				pos += r_bytes;
				if (pos >= buf_len)	{
					goto Success;
				}
			} else if (r_bytes == 0) {
				// It's likely the socket has been closed on the remote side.
				rv = EBADF;
				goto Fail;
			} else if ((errno != ETIMEDOUT) && (errno != EWOULDBLOCK) && (errno != EINPROGRESS) && (errno != EAGAIN)) {
#ifdef DEBUG_TIME
				debug_time_printf("socket write timeout 2", try, busy, start, now, deadline);
#endif
				rv = errno;
				goto Fail;
			}
		}
        
    Retry:
		try++;
        
	} while (pos < buf_len);
    
Success:
	if (0 > epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, &event)) {
		cf_warn("%s: epoll_ctl(DEL) on socket failed (errno %d: \"%s\")", ctx, errno, my_strerror_r(errno, errbuf, errbuf_len));
	}
	close(epoll_fd);
    
	rv = 0;
	goto Done;
    
Fail:
	cf_debug("%s: socket write timeout fail: %d (%s)", ctx, errno, my_strerror_r(errno, errbuf, errbuf_len));
    
	if (epoll_fd > 0) {
		if (0 > epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, &event)) {
			cf_warn("%s: epoll_ctl(DEL) on socket failed (errno %d: \"%s\")", ctx, errno, my_strerror_r(errno, errbuf, errbuf_len));
		}
		close(epoll_fd);
	}
    
Done:
	return(rv);
}

#endif // __linux__


#if defined(__APPLE__)

//
// There is a conflict even among various versions of
// linux, because it's common to compile kernels - or set ulimits -
// where the FD_SETSIZE is much greater than a compiled version.
// Thus, we can compute the required size of the fdset and use a reasonable size
// the other option is using epoll, which is a little scary for this kind of "I just want a timeout"
// usage.
//
// The reality is 8 bits per byte, but this calculation is a little more general
static size_t
get_fdset_size( int fd )
{
	if (fd < FD_SETSIZE)	return(sizeof(fd_set));
	int bytes_per_512 = sizeof(fd_set)  / (FD_SETSIZE / 512);
	return (  ((fd / 512)+1) * bytes_per_512 );
}


//
// Network socket helpers
// Often, you know the amount you want to read, and you have a timeout.
// Do the simple of wrapping in a select so we can read or write with timout
//
// There are two timeouts: the total deadline for the transaction,
// and the maximum time before making progress on a connection which
// we consider a failure so we can flip over to another node that might be healthier.
//
// Return the error number, not the number of bytes.
//
// there are two timeouts: the timeout for this attempt, and the deadline
// for the entire transaction.
//
int
cf_socket_read_timeout(int fd, uint8_t *buf, size_t buf_len, uint64_t trans_deadline, int attempt_ms)
{
#ifdef DEBUG_TIME
	uint64_t start = cf_getms();
#endif
	struct timeval tv;
	size_t pos = 0;
    
	int flags;
	if (-1 == (flags = fcntl(fd, F_GETFL, 0)))
		flags = 0;
	if (! (flags & O_NONBLOCK)) {
		if (-1 == fcntl(fd, F_SETFL, flags | O_NONBLOCK)) {
			return(EBADF);
		}
	}
    
	// between the transaction deadline and the attempt_ms, find the lesser
	// and create a deadline for this attempt
	uint64_t deadline = attempt_ms + cf_getms();
    if ((trans_deadline != 0) && (trans_deadline < deadline))
		deadline = trans_deadline;
    
	// Setup fdset. This looks weird, but there's no guarentee
	// that FD_SETSIZE this was compiled on has much to do with the machine
	// we're running on.
 	fd_set *rset = 0;
 	fd_set  stackset;
 	size_t  rset_size;
 	if (fd < FD_SETSIZE) { // common case
 		rset = &stackset;
 		rset_size = sizeof(stackset);
 	}
 	else {
 		rset_size = get_fdset_size(fd);
 		rset = (fd_set*)malloc ( rset_size );
 		if (!rset)	return(-1);
 	}
    
	int rv = 0;
	int try = 0;
#ifdef DEBUG_TIME
	int select_busy = 0;
#endif
    
	do {
		uint64_t now = cf_getms();
		if (now > deadline) {
#ifdef DEBUG_TIME
			debug_time_printf("socket readselect timeout", try, select_busy, start, now, deadline);
#endif
			rv = ETIMEDOUT;
			goto Out;
        }
        
		uint64_t ms_left = deadline - now;
		tv.tv_sec = ms_left / 1000;
		tv.tv_usec = (ms_left % 1000) * 1000;
        
		memset((void*)rset, 0, rset_size);
		FD_SET(fd, rset);
		rv = select(fd +1, rset /*readfd*/, 0 /*writefd*/, 0 /*oobfd*/, &tv);
        
		// we only have one fd, so we know it's ours?
		if ((rv > 0) && FD_ISSET(fd, rset)) {
            
			int r_bytes = (int)read(fd, buf + pos, buf_len - pos );
            
			if (r_bytes > 0) {
				pos += r_bytes;
            }
			else if (r_bytes == 0) {
				// We believe this means that the server has closed this socket.
				rv = EBADF;
				goto Out;
			}
            else if (errno != ETIMEDOUT
                     // It's apparently possible that select() returns successfully yet
                     // the socket is not ready for reading.
                     && errno != EWOULDBLOCK && errno != EINPROGRESS && errno != EAGAIN) {
#ifdef DEBUG_TIME
    			debug_time_printf("socket read timeout", try, select_busy, start, now, deadline);
#endif
				rv = errno;
				goto Out;
            }
		}
		else if (rv == 0) {
#ifdef DEBUG_TIME
			select_busy++;
#endif
        }
        else {
			if (rv == -1)  {
				rv = errno;
				goto Out;
			}
        }
        
        try++;
        
	} while( pos < buf_len );
    
	rv = 0; // success!
    
Out:
	if (rset != &stackset)	free(rset);
	return( rv );
}


int
cf_socket_write_timeout(int fd, uint8_t *buf, size_t buf_len, uint64_t trans_deadline, int attempt_ms)
{
#ifdef DEBUG_TIME
	uint64_t start = cf_getms();
#endif
	struct timeval tv;
	size_t pos = 0;
    
	int flags;
	if (-1 == (flags = fcntl(fd, F_GETFL, 0)))
		flags = 0;
	if (! (flags & O_NONBLOCK)) {
		if (-1 == fcntl(fd, F_SETFL, flags | O_NONBLOCK)) {
			return(ENOENT);
		}
	}
    
	// between the transaction deadline and the attempt_ms, find the lesser
	// and create a deadline for this attempt
	uint64_t deadline = attempt_ms + cf_getms();
    if ((trans_deadline != 0) && (trans_deadline < deadline))
		deadline = trans_deadline;
    
	// Setup fdset. This looks weird, but there's no guarentee
	// that FD_SETSIZE this was compiled on has much to do with the machine
	// we're running on.
 	fd_set *wset = 0;
 	fd_set  stackset;
 	size_t  wset_size;
 	if (fd < FD_SETSIZE) { // common case
 		wset = &stackset;
 		wset_size = sizeof(stackset);
 	}
 	else {
 		wset_size = get_fdset_size(fd);
 		wset = (fd_set*)malloc ( wset_size );
 		if (wset == 0)	return(-1);
 	}
    
	int rv = 0;
	int try = 0;
#ifdef DEBUG_TIME
	int select_busy = 0;
#endif
    
	do {
		uint64_t now = cf_getms();
		if (now > deadline) {
#ifdef DEBUG_TIME
			debug_time_printf("socket writeselect timeout", try, select_busy, start, now, deadline);
#endif
			rv = ETIMEDOUT;
			goto Out;
        }
        
		uint64_t ms_left = deadline - now;
		tv.tv_sec = ms_left / 1000;
		tv.tv_usec = (ms_left % 1000) * 1000;
        
		memset((void*)wset, 0, wset_size);
		FD_SET(fd, wset);
        
		rv = select(fd +1, 0 /*readfd*/, wset /*writefd*/, 0/*oobfd*/, &tv);
        
		// we only have one fd, so we know it's ours, but select seems confused sometimes - do the safest thing
		if ((rv > 0) && FD_ISSET(fd, wset)) {
            
			int r_bytes = (int)write(fd, buf + pos, buf_len - pos );
            
			if (r_bytes > 0) {
				pos += r_bytes;
				if (pos >= buf_len)	{ rv = 0; goto Out; } // done happily
            }
			else if (r_bytes == 0) {
				// We shouldn't see 0 returned unless we try to write 0 bytes, which we don't.
				rv = EBADF;
				goto Out;
			}
            else if (errno != ETIMEDOUT
                     && errno != EWOULDBLOCK && errno != EINPROGRESS && errno != EAGAIN) {
#ifdef DEBUG_TIME
    			debug_time_printf("socket write timeout", try, select_busy, start, now, deadline);
#endif
                rv = errno;
                goto Out;
			}
		}
        else if (rv == 0) {
#ifdef DEBUG_TIME
        	select_busy++;
#endif
    	}
        else {
			if (rv == -1)	{ rv = errno; goto Out; }
        }
        
		try++;
        
	} while( pos < buf_len );
    rv = 0;
    
Out:
	if (wset != &stackset) free(wset);
    
	return( rv );
}

#endif // __APPLE__


//
// These FOREVER calls are only called in the 'getmany' case, which is used
// for application level highly variable queries
//
int
cf_socket_read_forever(int fd, uint8_t *buf, size_t buf_len)
{
	// one way is to make sure the fd is blocking, and block
	int flags;
	if (-1 == (flags = fcntl(fd, F_GETFL, 0)))
		flags = 0;
	if (flags & O_NONBLOCK) {
		if (-1 == fcntl(fd, F_SETFL, flags & ~O_NONBLOCK)) {
			return(ENOENT);
		}
	}
    
	size_t pos = 0;
	do {
		int r_bytes = (int)read(fd, buf + pos, buf_len - pos );
		if (r_bytes < 0) { // don't combine these into one line! think about it!
			if (errno != ETIMEDOUT) {
				return(errno);
			}
		}
		else if (r_bytes == 0) {
			// blocking read returns 0 bytes socket timedout at server side
			// is closed
			return EBADF;
		}
		else {
			pos += r_bytes;
		}
	} while (pos < buf_len);
    
	return(0);
}


int
cf_socket_write_forever(int fd, uint8_t *buf, size_t buf_len)
{
	// one way is to make sure the fd is blocking, and block
	int flags;
	if (-1 == (flags = fcntl(fd, F_GETFL, 0)))
		flags = 0;
	if (flags & O_NONBLOCK) {
		if (-1 == fcntl(fd, F_SETFL, flags & ~O_NONBLOCK)) {
			return(ENOENT);
		}
	}
    
	size_t pos = 0;
	do {
		int r_bytes = (int)write(fd, buf + pos, buf_len - pos );
		if (r_bytes < 0) { // don't combine these into one line! think about it!
			if (errno != ETIMEDOUT) {
				return(errno);
			}
		}
		else {
			pos += r_bytes;
		}
	} while (pos < buf_len);
    
	if (-1 == fcntl(fd, F_SETFL, flags & O_NONBLOCK)) {
		return(ENOENT);
	}
    
	return(0);
}

#else // CF_WINDOWS
//====================================================================
// Windows
//

#include <WinSock2.h>
#include <Ws2tcpip.h>


int
cf_socket_create_nb()
{
	// Create the socket.
	SOCKET fd = socket(AF_INET, SOCK_STREAM, 0);

	if (-1 == fd) {
		cf_warn("could not allocate socket, errno %d", errno);
		return -1;
	}

    // Make the socket nonblocking.
	u_long iMode = 1;

	if (NO_ERROR != ioctlsocket(fd, FIONBIO, &iMode)) {
		cf_info("could not connect nonblocking socket %d, errno %d", fd, errno);
		return -1;
	}

	int f = 1;
	setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, (char*)&f, sizeof(f));

	return (int)fd;
}


#endif // CF_WINDOWS


int
cf_socket_start_connect_nb(int fd, struct sockaddr_in* sa)
{
	if (0 != connect(fd, (struct sockaddr*)sa, sizeof(*sa))) {
        if (! IS_CONNECTING()) {
        	if (errno == ECONNREFUSED) {
        		cf_debug("host refused socket connection");
        	}
        	else {
        		cf_info("could not connect nonblocking socket %d, errno %d", fd, errno);
        	}

        	return -1;
		}
	}

	return 0;
}


int
cf_socket_create_and_connect_nb(struct sockaddr_in* sa)
{
	// Create the socket.
	int fd = cf_socket_create_nb();

	// Initiate non-blocking connect.
	if (0 != cf_socket_start_connect_nb(fd, sa)) {
		cf_close(fd);
		return -1;
	}

	return fd;
}
