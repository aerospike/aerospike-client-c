/*
 * A good, basic C client for the Aerospike protocol
 * Creates a library which is linkable into a variety of systems
 *
 * First attempt is a very simple non-threaded blocking interface
 * currently coded to C99 - in our tree, GCC 4.2 and 4.3 are used
 *
 * Brian Bulkowski, 2009
 * All rights reserved
 */

#include <sys/types.h>
#include <sys/socket.h> // socket calls
#include <stdio.h>
#include <errno.h> //errno
#include <stdlib.h> //fprintf
#include <unistd.h> // close
#include <string.h>
#include <fcntl.h>
#include <arpa/inet.h>  // ntonl
#include <stdbool.h>
#include <inttypes.h> // PRIu64
#include <pthread.h>

#include <asm/byteorder.h> // 64-bit swap macro

#include "citrusleaf/cf_clock.h"

#include <signal.h>
#include <netinet/tcp.h>

// #define DEBUG_TIME
// #define DEBUG
// #define DEBUG_VERBOSE


#ifdef DEBUG_TIME
static void debug_time_printf(const char* desc, int try, int select_busy, long start, long end, long deadline)
{
	fprintf(stderr, "%s|%zu|%d|%d|%"PRIu64"|%"PRIu64"|%"PRIu64"\n",
		desc,
		(uint64_t)pthread_self(),
		try,
		select_busy,
		start,
		end,
		deadline
	);
}
#endif

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
get_fdset_size( int fd ) {
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
#ifdef DEBUG_VERBOSE
	fprintf(stderr, "read timeout %d %p %zd\n",fd, buf, buf_len);
#endif
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
// 		fprintf(stderr, "fd too large for compiled constant: fd %d setsize %d calcsize %d\n",
// 			fd,sizeof(fd_set),rset_size);
 		rset = malloc ( rset_size );
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
#ifdef DEBUG    
            fprintf(stderr, "Citrusleaf: timeout when reading (%d) - deadline was %"PRIu64" now is %"PRIu64"\n",
                fd,deadline, cf_getms() );
#endif
#ifdef DEBUG_TIME
			debug_time_printf("socket readselect timeout", try, select_busy, start, now, deadline);
#endif    
			rv = ETIMEDOUT;
			goto Out;
        }

		uint64_t ms_left = deadline - now;
		tv.tv_sec = ms_left / 1000;
		tv.tv_usec = (ms_left % 1000) * 1000;
		
		memset(rset, 0, rset_size);
		FD_SET(fd, rset);
		rv = select(fd +1, rset /*readfd*/, 0 /*writefd*/, 0 /*oobfd*/, &tv);

		// we only have one fd, so we know it's ours?
		if ((rv > 0) && FD_ISSET(fd, rset)) {
			
			int r_bytes = read(fd, buf + pos, buf_len - pos );

			if (r_bytes > 0) {
				pos += r_bytes;
            }
			else if (r_bytes == 0) {
				// We believe this means that the server has closed this socket.
#ifdef DEBUG
				fprintf(stderr, "Citrusleaf: read returned 0, fd %d\n", fd);
#endif
				rv = EBADF;
				goto Out;
			}
            else if (errno != ETIMEDOUT
					// It's apparently possible that select() returns successfully yet
					// the socket is not ready for reading.
					&& errno != EWOULDBLOCK && errno != EINPROGRESS && errno != EAGAIN) {
#ifdef DEBUG
                fprintf(stderr, "Citrusleaf: error when reading from server - %d fd %d errno %d : %s\n",r_bytes,fd, errno, strerror(errno));
#endif
#ifdef DEBUG_TIME
    			debug_time_printf("socket read timeout", try, select_busy, start, now, deadline);
#endif				
				rv = errno;
				goto Out;
            }
#ifdef DEBUG
			else {
				fprintf(stderr, "Citrusleaf: will retry - error when reading from server - %d fd %d errno %d : %s\n", r_bytes, fd, errno, strerror(errno));
			}
#endif
		}
		else if (rv == 0) {
#ifdef DEBUG_TIME
			select_busy++;
#endif
#ifdef DEBUG			
            fprintf(stderr, "read select: returned 0 likely timeout fd %d errno %d\n",fd, errno);
#endif            
        }
        else {
#ifdef DEBUG
            fprintf(stderr, "read select: (%d) returned odd answer  %d errno %d try %d fdnotset\n",fd,rv, errno, try);
#endif
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
#ifdef DEBUG_VERBOSE
	fprintf(stderr, "write with timeout %d %p %zd\n",fd,buf,buf_len);
#endif
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
 		wset = malloc ( wset_size );
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
#ifdef DEBUG    
            fprintf(stderr, "Citrusleaf: timeout when writing (%d) - deadline was %"PRIu64" now is %"PRIu64" try %d\n",
                fd,deadline, cf_getms(),try );
#endif
#ifdef DEBUG_TIME
			debug_time_printf("socket writeselect timeout", try, select_busy, start, now, deadline);
#endif    
			rv = ETIMEDOUT;
			goto Out;
        }

		uint64_t ms_left = deadline - now;
		tv.tv_sec = ms_left / 1000;
		tv.tv_usec = (ms_left % 1000) * 1000;

		memset(wset, 0, wset_size);
		FD_SET(fd, wset);
		
		rv = select(fd +1, 0 /*readfd*/, wset /*writefd*/, 0/*oobfd*/, &tv);
		
		// we only have one fd, so we know it's ours, but select seems confused sometimes - do the safest thing
		if ((rv > 0) && FD_ISSET(fd, wset)) {
			
			int r_bytes = write(fd, buf + pos, buf_len - pos );

			if (r_bytes > 0) {
				pos += r_bytes;
				if (pos >= buf_len)	{ rv = 0; goto Out; } // done happily
            }
			else if (r_bytes == 0) {
				// We shouldn't see 0 returned unless we try to write 0 bytes, which we don't.
#ifdef DEBUG
				fprintf(stderr, "Citrusleaf: write returned 0, fd %d\n", fd);
#endif
				rv = EBADF;
				goto Out;
			}
            else if (errno != ETIMEDOUT
					&& errno != EWOULDBLOCK && errno != EINPROGRESS && errno != EAGAIN) {
#ifdef DEBUG              
                fprintf(stderr, "Citrusleaf: error when writing to server - %d fd %d errno %d\n",r_bytes,fd, errno);
#endif                    
#ifdef DEBUG_TIME
    			debug_time_printf("socket write timeout", try, select_busy, start, now, deadline);
#endif				
                rv = errno;
                goto Out;
			}
#ifdef DEBUG        
            else {
                fprintf(stderr, "Citrusleaf: will retry - error when writing to server - %d fd %d errno %d\n", r_bytes, fd, errno);
            }
#endif        
		}
        else if (rv == 0) {
#ifdef DEBUG_TIME
        	select_busy++;
#endif
#ifdef DEBUG        	
        	fprintf(stderr, "write: select returned 0, likely timeout fd %d errno %d\n",fd,errno);
#endif        	
    	}
        else {
#ifdef DEBUG
            fprintf(stderr, "write select: (%d) returned odd answer %d errno %d try %d tvsec %d tvusec %d\n",rv, errno,try,tv.tv_sec,tv.tv_usec);
#endif
			if (rv == -1)	{ rv = errno; goto Out; }
        }
			
		try++;

	} while( pos < buf_len );
    rv = 0;
	
Out:
	if (wset != &stackset) free(wset);

	return( rv );
}

//
// These FOREVER calls are only called in the 'getmany' case, which is used
// for application level highly variable queries
//
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

//	fprintf(stderr, "read forever %d %p %zd\n",fd,buf,buf_len);
	
	size_t pos = 0;
	do {
//		fprintf(stderr, "read: fd %d buf %p len %zd\n",fd,buf+pos,buf_len-pos);
		int r_bytes = read(fd, buf + pos, buf_len - pos );
//		fprintf(stderr, "read: fd %d rb %d err %d\n",fd,r_bytes,errno);
		if (r_bytes < 0) { // don't combine these into one line! think about it!
			if (errno != ETIMEDOUT) {
#ifdef DEBUG                
				fprintf(stderr, "Citrusleaf: error when reading from server - %d fd %d errno %d\n",r_bytes,fd, errno);
#endif                
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
	
//	fprintf(stderr, "write forever %d %p %zd\n",fd,buf, buf_len);
	
	size_t pos = 0;
	do {
		int r_bytes = write(fd, buf + pos, buf_len - pos );
		if (r_bytes < 0) { // don't combine these into one line! think about it!
			if (errno != ETIMEDOUT) {
#ifdef DEBUG                
				fprintf(stderr, "Citrusleaf: error when writing to server - %d fd %d errno %d\n",r_bytes,fd, errno);
#endif                
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

int
cf_create_nb_socket(struct sockaddr_in *sa, int timeout)
{
	int flags;

    struct timeval  ts;
    
    ts.tv_sec = timeout;
    ts.tv_usec = 0;

	// Create the socket 
	int fd = -1;
	if (-1 == (fd = socket ( AF_INET, SOCK_STREAM, 0 ))) {
		fprintf(stderr, "could not allocate socket errno %d\n",errno);
		return(-1);
	}

    //set socket nonblocking flag
    if( (flags = fcntl(fd, F_GETFL, 0)) < 0) {
		close(fd);
        return -1;
	}
    
    if(fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
		close(fd);
        return -1;
	}

	int f = 1;
	setsockopt(fd, SOL_TCP, TCP_NODELAY, &f, sizeof(f));

	//initiate non-blocking connect
	if (0 != connect(fd, (struct sockaddr *) sa, sizeof( *sa ) )) {
        if (errno != EINPROGRESS) {
			close(fd);
            return -1;
		}
	}
	return fd;
}

