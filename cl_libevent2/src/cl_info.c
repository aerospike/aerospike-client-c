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

#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <event2/dns.h>
#include <event2/event.h>

#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_errno.h"
#include "citrusleaf/cf_log_internal.h"
#include "citrusleaf/cf_socket.h"
#include "citrusleaf/cf_vector.h"
#include "citrusleaf/proto.h"

#include "citrusleaf_event2/cl_cluster.h"
#include "citrusleaf_event2/ev2citrusleaf.h"
#include "citrusleaf_event2/ev2citrusleaf-internal.h"


//
// Globals to track transaction counts to clean up properly
//

cf_atomic_int g_cl_info_transactions = 0;



// debug
extern  void sockaddr_in_dump(char *prefix, struct sockaddr_in *sa_in);


cl_info_request *
info_request_create()
{
	cl_info_request *cir = (cl_info_request*)malloc( sizeof(cl_info_request) + ( event_get_struct_event_size() ) );
	if (cir) memset((void*)cir, 0, sizeof(*cir));
	return(cir);
}

void
info_request_destroy(cl_info_request *cir)
{
	
	if (cir->rd_buf)	free(cir->rd_buf);
	if (cir->wr_buf) {
		if (cir->wr_buf != cir->wr_tmp)
			free(cir->wr_buf);
	}
	free(cir);
}

struct event *
info_request_get_network_event(cl_info_request *cir)
{
	return( (struct event *) &cir->event_space[0] );	
}

/*
** when you expect a single result back, info result into just that string
*/

int
citrusleaf_info_parse_single(char *values, char **value)
{
	while (*values && (*values != '\t'))
		values++;
	if (*values == 0)	return(-1);
	values++;
	*value = values;
	while (*values && (*values != '\n'))
		values++;
	if (*values == 0)	return(-1);
	*values = 0;
	return(0);
	
}

int
info_make_request(cl_info_request *cir, char *names)
{
	cir->wr_buf_size = sizeof(cl_proto);
	if (names) {
		uint32_t nameslen = strlen(names);
		cir->wr_buf_size += nameslen;
		if (names[nameslen-1] != '\n')
			cir->wr_buf_size++;
	}

	// set up the buffer pointer
	if (cir->wr_buf_size > sizeof(cir->wr_tmp)) {
		cir->wr_buf = (uint8_t*)malloc( cir->wr_buf_size );
		if (!cir->wr_buf)	return(-1);
	} else {
		cir->wr_buf = cir->wr_tmp;
	}

	// do byte-by-byte so we can convert :-(	
	if (names) {
		char *src = names;
		char *dst = (char *) (cir->wr_buf + sizeof(cl_proto));
		while (*src) {
			if ((*src == ';') || (*src == ':') || (*src == ',')) 
				*dst = '\n';
			else 
				*dst = *src;
			src++;
			dst++;
		}
		if ( src[-1] != '\n')	*dst = '\n';
	}

	cl_proto *proto = (cl_proto *) cir->wr_buf;
	proto->sz = cir->wr_buf_size - sizeof(cl_proto); 
	proto->version = CL_PROTO_VERSION;
	proto->type = CL_PROTO_TYPE_INFO;
	cl_proto_swap(proto);
	return(0);
}


void
info_event_fn(evutil_socket_t fd, short event, void *udata)
{
	cl_info_request *cir = (cl_info_request *)udata;
	int rv;
	
	cf_atomic_int_incr(&g_cl_stats.info_events);
	
	uint64_t _s = cf_getms();

	if (event & EV_WRITE) {
		if (cir->wr_buf_pos < cir->wr_buf_size) {
			rv = send(fd, (char*)&cir->wr_buf[cir->wr_buf_pos], cir->wr_buf_size - cir->wr_buf_pos, MSG_NOSIGNAL | MSG_DONTWAIT);
			if (rv > 0) {
				cir->wr_buf_pos += rv;
				if (cir->wr_buf_pos == cir->wr_buf_size) {
					// changing from WRITE to READ requires redoing the set then the add 
					event_assign(info_request_get_network_event(cir),cir->base, fd, EV_READ, info_event_fn, cir);		
				}
			}
			else if (rv == 0) {
				cf_debug("write info failed: illegal send return 0: errno %d", errno);
				goto Fail;
			}
			else if ((errno != EAGAIN) && (errno != EWOULDBLOCK)) {
				cf_debug("write info failed: rv %d errno %d", rv, errno);
				goto Fail;
			}
		}
	}

	if (event & EV_READ) {
		if (cir->rd_header_pos < sizeof(cl_proto) ) {
			rv = recv(fd, (char*)&cir->rd_header_buf[cir->rd_header_pos], sizeof(cl_proto) - cir->rd_header_pos, MSG_NOSIGNAL | MSG_DONTWAIT);
			if (rv > 0) {
				cir->rd_header_pos += rv;
			}				
			else if (rv == 0) {
				cf_info("read info failed: remote close: rv %d errno %d", rv, errno);
				goto Fail;
			}
			else if ((errno != EAGAIN) && (errno != EWOULDBLOCK)) {
				cf_info("read info failed: unknown error: rv %d errno %d", rv, errno);
				goto Fail;
			}
		}
		if (cir->rd_header_pos == sizeof(cl_proto)) {
			if (cir->rd_buf_size == 0) {
				// calculate msg size
				cl_proto *proto = (cl_proto *) cir->rd_header_buf;
				cl_proto_swap(proto);
				
				// set up the read buffer
				cir->rd_buf = (uint8_t*)malloc(proto->sz + 1);
				if (!cir->rd_buf) {
					cf_warn("cl info malloc fail");
					goto Fail;
				}
				cir->rd_buf[proto->sz] = 0;
				cir->rd_buf_pos = 0;
				cir->rd_buf_size = proto->sz;
			}
			if (cir->rd_buf_pos < cir->rd_buf_size) {
				rv = recv(fd, (char*)&cir->rd_buf[cir->rd_buf_pos], cir->rd_buf_size - cir->rd_buf_pos, MSG_NOSIGNAL | MSG_DONTWAIT);
				if (rv > 0) {
					cir->rd_buf_pos += rv;
					if (cir->rd_buf_pos >= cir->rd_buf_size) {
						// caller frees rdbuf
						(*cir->user_cb)(0 /*return value*/, (char*)cir->rd_buf, cir->rd_buf_size, cir->user_data);
						cir->rd_buf = 0;
						event_del(info_request_get_network_event(cir) ); // WARNING: this is not necessary. BOK says it is safe: maybe he's right, maybe wrong.

						cf_close(fd);
						info_request_destroy(cir);
						cir = 0;
						cf_atomic_int_incr(&g_cl_stats.info_complete);
						cf_atomic_int_decr(&g_cl_info_transactions);
						
						uint64_t delta = cf_getms() - _s;
						if (delta > CL_LOG_DELAY_INFO) cf_info("CL_DELAY cl_info event OK fn: %lu", delta);

						return;
					}
				}
				else if (rv == 0) {
					cf_info("info failed: remote termination fd %d cir %p rv %d errno %d", fd, cir, rv, errno);
					goto Fail;
				}
				else if ((errno != EAGAIN) && (errno != EWOULDBLOCK)) {
					cf_info("info failed: connection has unknown error fd %d cir %p rv %d errno %d", fd, cir, rv, errno);
					goto Fail;
				}
			}
		}
	}

	event_add(info_request_get_network_event(cir), 0 /*timeout*/);					
	
	uint64_t delta = cf_getms() - _s;
	if (delta > CL_LOG_DELAY_INFO) cf_info("CL_DELAY cl_info event again fn: %lu", delta);

	return;
	
Fail:
	(*cir->user_cb) ( -1, 0 , 0,cir->user_data );
	event_del(info_request_get_network_event(cir)); // WARNING: this is not necessary. BOK says it is safe: maybe he's right, maybe wrong.
	cf_close(fd);
	info_request_destroy(cir);
	cf_atomic_int_incr(&g_cl_stats.info_complete);
	cf_atomic_int_decr(&g_cl_info_transactions);
	
	delta = cf_getms() - _s;
	if (delta > CL_LOG_DELAY_INFO) cf_info("CL_DELAY: cl_info event fail OK took %lu", delta);
}



//
// Request the info of a particular sockaddr_in,
// used internally for host-crawling as well as supporting the external interface
//

int
ev2citrusleaf_info_host(struct event_base *base, struct sockaddr_in *sa_in, char *names, int timeout_ms,
	ev2citrusleaf_info_callback cb, void *udata) 
{
	
	uint64_t _s = cf_getms();
	
	cf_atomic_int_incr(&g_cl_stats.info_host_requests);
	
	cl_info_request *cir = info_request_create();
	if (!cir)	return(-1);
	
	cir->user_cb = cb;
	cir->user_data = udata;
	cir->base = base;

	// Create the socket a little early, just in case
	int fd = cf_socket_create_and_connect_nb(sa_in);

	if (fd == -1) {
		info_request_destroy(cir);

		uint64_t delta = cf_getms() - _s;
		if (delta > CL_LOG_DELAY_INFO) cf_info("CL_DELAY: info host no socket connect: %lu", delta);

		return -1;
	}
	
	// fill the buffer while I'm waiting
	if (0 != info_make_request(cir, names)) {
		cf_warn("buffer fill failed");
		
		info_request_destroy(cir);
		cf_close(fd);
		
		uint64_t delta = cf_getms() - _s;
		if (delta > CL_LOG_DELAY_INFO) cf_info("CL_DELAY: info host bad request: %lu", delta);
		
		return(-1);
	}
	
	// setup for event
	event_assign(info_request_get_network_event(cir),cir->base, fd, EV_WRITE | EV_READ, info_event_fn, (void *) cir);
	event_add(info_request_get_network_event(cir), 0/*timeout*/);
	
	cf_atomic_int_incr(&g_cl_info_transactions);
	
	uint64_t delta = cf_getms() - _s;
	if (delta > CL_LOG_DELAY_INFO) cf_info("CL_DELAY: info host standard: %lu", delta);

	
	return(0);
}

typedef struct {
	ev2citrusleaf_info_callback cb;
	void *udata;
	char *names;
	uint32_t	timeout_ms;
	struct event_base *base;
} info_resolve_state;

// Got resolution - callback!
// 
// WARNING! It looks like a bug to have the possibilities fo multiple callbacks
// fired from this resolve function.
//

void
info_resolve_cb(int result, cf_vector *sockaddr_in_v, void *udata)
{
	info_resolve_state *irs = (info_resolve_state *)udata;
	if (result != 0) {
		cf_info("info resolution: async fail %d", result);
		(irs->cb) ( -1 /*return value*/, 0, 0 ,irs->udata );
		goto Done;
	}		
	for (uint32_t i=0; i < cf_vector_size(sockaddr_in_v) ; i++)
	{
		struct sockaddr_in  sa_in;
		cf_vector_get(sockaddr_in_v, i, &sa_in);

		if (0 != ev2citrusleaf_info_host(irs->base, &sa_in, irs->names, irs->timeout_ms, irs->cb, irs->udata )) {
			cf_info("info resolution: can't start infohost after resolve just failed");

			(irs->cb) ( -1 /*return value*/, 0, 0 ,irs->udata );
			goto Done;
		}
	}
Done:	
	cf_atomic_int_decr(&g_cl_info_transactions);
	free(irs->names);
	free(irs);
}

//
// External function is helper which goes after a particular hostname.
//
// TODO: timeouts are wrong here. If there are 3 host names, you'll end up with
// 3x timeout_ms
//

int
ev2citrusleaf_info(struct event_base *base, struct evdns_base *dns_base, 
	char *host, short port, char *names, int timeout_ms,
	ev2citrusleaf_info_callback cb, void *udata)
{
	int rv = -1;
	info_resolve_state *irs = 0;
	
	cf_atomic_int_incr(&g_cl_stats.info_host_requests);
	
	struct sockaddr_in sa_in;
	// if we can resolve immediate, jump directly to resolution
	if (0 == cl_lookup_immediate(host, port, &sa_in)) {
		if (0 == ev2citrusleaf_info_host(base, &sa_in, names, timeout_ms, cb, udata )) {
			rv = 0;
			goto Done;
		}
	}
	else {
		irs = (info_resolve_state*)malloc( sizeof(info_resolve_state) );
		if (!irs)	goto Done;
		irs->cb = cb;
		irs->udata = udata;
		if (names) { 
			irs->names = strdup(names);
			if (!irs->names) goto Done;
		}
		else irs->names = 0;
		irs->base = base;
		irs->timeout_ms = timeout_ms;
		if (0 != cl_lookup(dns_base, host, port, info_resolve_cb, irs)) 
			goto Done;
		irs = 0;
		
		cf_atomic_int_incr(&g_cl_info_transactions);
		
	}
	
	
Done:
	if (irs) {
		if (irs->names)	free(irs->names);
		free(irs);
	}
	return(rv);
}

//
// When shutting down the entire module, need to make sure that
// all info requests pending are also shut down
//

// AKG - no base available for this... no-op for now.
void
//ev2citrusleaf_info_shutdown(struct event_base *base)
ev2citrusleaf_info_shutdown()
{
//	while ( ( cf_atomic_int_get(g_cl_info_transactions) > 0 ) &&
//		    (event_base_loop(base, EVLOOP_ONCE) == 0) )
//	    ;
//	return;	
}


