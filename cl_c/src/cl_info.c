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
#include <pthread.h>
#include <fcntl.h>
#include <arpa/inet.h>

#include "citrusleaf/citrusleaf.h"
#include "citrusleaf/cl_cluster.h"
#include "citrusleaf/proto.h"
#include "citrusleaf/cf_socket.h"

// #define DEBUG 1

//
// The interesting note here is that using blocking calls - if you're in a non-event-oriented
// system - is far faster. So that's what we'll do for the moment.
//
// When you have a real event-oriented user, you'll want to fully epoll and all that,
// but so far the clients don't want to do that
//
// Timeouts are handled by the timer system, which will call back if the timer 
// expires, and that will close the file descriptor out from under the blocking call.
// Have to be a little careful about locks, but seems to be fast and effective.
//
// Only problem with this approach is it's bad for the DNS lookup we do through there
// so figure out how to be careful with that
//


static int
info_expire_transaction( void *udata)
{
	int fd = (int) (size_t) udata;
	close(fd);
	return(0);
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


//
// Request the info of a particular sockaddr_in,
// used internally for host-crawling as well as supporting the external interface
//

int
citrusleaf_info_host(struct sockaddr_in *sa_in, char *names, char **values, int timeout_ms, bool send_asis) 
{
	uint bb_size = 16384;
	int rv = -1;
    int io_rv;
	*values = 0;
	
	// Deal with the incoming 'names' parameter
	// Translate interior ';'  in the passed-in names to \n
	uint32_t	slen = 0;
	if (names) {
		if (send_asis) {
			slen = strlen(names);
		} else {
			char *_t = names;
			while (*_t) { 
				slen++; 
				if ((*_t == ';') || (*_t == ':') || (*_t == ',')) *_t = '\n'; 
				_t++; 
			}
		}
	}
	
	// Sometimes people forget/cant add the trailing '\n'. Be nice and add it for them.
	// using a stack allocated variable so we dn't have to clean up, Pain in the ass syntactically
	// but a nice little thing
	if (names) {
		if (names[slen-1] == '\n') {
			slen = 0;
		} else { 
			slen++; if (slen > bb_size) { return(-1); } 
		}
	}
	char names_with_term[slen+1];
	if (slen) { 
		strcpy(names_with_term, names);
		names_with_term[slen-1] = '\n';
		names_with_term[slen] = 0;
		names = names_with_term;
	}
		
	// Actually doing a non-blocking connect
	int fd = cf_create_nb_socket(sa_in, timeout_ms);
	if (fd == -1) {
		return -1;
	}

	cl_proto 	*req;
	uint8_t		buf[bb_size];
	uint		buf_sz;
	
	if (names) {
		uint sz = strlen(names);
		buf_sz = sz + sizeof(cl_proto);
		if (buf_sz < bb_size)
			req = (cl_proto *) buf;
		else
			req = (cl_proto *) malloc(buf_sz);
		if (req == NULL)	goto Done;

		req->sz = sz;
		memcpy(req->data,names,sz);
	}
	else {
		req = (cl_proto *) buf;
		req->sz = 0;
		buf_sz = sizeof(cl_proto);
	}
		
	req->version = CL_PROTO_VERSION;
	req->type = CL_PROTO_TYPE_INFO;
	cl_proto_swap(req);
	
    if (timeout_ms)
        io_rv = cf_socket_write_timeout(fd, (uint8_t *) req, buf_sz, 0, timeout_ms);
    else
        io_rv = cf_socket_write_forever(fd, (uint8_t *) req, buf_sz);
    
	if ((uint8_t *)req != buf)	free(req);
	if (io_rv != 0) {
#ifdef DEBUG        
		cf_debug("info returned error, rv %d errno %d bufsz %d", io_rv, errno, buf_sz);
#endif        
		goto Done;
	}
	
	cl_proto	*rsp = (cl_proto *)buf;
    if (timeout_ms) 
        io_rv = cf_socket_read_timeout(fd, buf, 8, 0, timeout_ms);
    else
        io_rv = cf_socket_read_forever(fd, buf, 8);
    
    if (0 != io_rv) {
#ifdef DEBUG        
		cf_debug("info socket read failed: rv %d errno %d", io_rv, errno);
#endif        
		goto Done;
	}
	cl_proto_swap(rsp);
	
	if (rsp->sz) {
		uint8_t *v_buf = malloc(rsp->sz + 1);
		if (!v_buf) goto Done;
        
        if (timeout_ms)
            io_rv = cf_socket_read_timeout(fd, v_buf, rsp->sz, 0, timeout_ms);
        else
            io_rv = cf_socket_read_forever(fd, v_buf, rsp->sz);
        
        if (io_rv != 0) {
            free(v_buf);
            goto Done;
		}
			
		v_buf[rsp->sz] = 0;
		*values = (char *) v_buf;
	}                                                                                               
	else {
		fprintf(stderr, "rsp size is 0\n");
		*values = 0;
	}
	rv = 0;

Done:	
	shutdown(fd, SHUT_RDWR);
	close(fd);
	
	return(rv);
	
}

//
// External function is helper which goes after a particular hostname.
//
// TODO: timeouts are wrong here. If there are 3 addresses for a host name,
// you'll end up with 3x timeout_ms
//

int
citrusleaf_info(char *hostname, short port, char *names, char **values, int timeout_ms)
{
	int rv = -1;
	cf_vector sockaddr_in_v;
	cf_vector_init(&sockaddr_in_v, sizeof( struct sockaddr_in ), 5, 0);
	if (0 != cl_lookup(NULL, hostname, port, &sockaddr_in_v)) {
		cf_debug("Could not find host %s", hostname);
		goto Done;
	}
	
	for (uint i=0; i < cf_vector_size(&sockaddr_in_v) ; i++) 
	{
		struct sockaddr_in  sa_in;
		cf_vector_get(&sockaddr_in_v, i, &sa_in);

		if (0 == citrusleaf_info_host(&sa_in, names, values, timeout_ms, false)) {
			rv = 0;
			goto Done;
		}
	}
	
Done:
	cf_vector_destroy( &sockaddr_in_v );	
	return(rv);
}

static void
dump_sockaddr_in(char *prefix, struct sockaddr_in *sa_in)
{
	char str[INET_ADDRSTRLEN];
	inet_ntop(AF_INET, &(sa_in->sin_addr), str, INET_ADDRSTRLEN);	
	fprintf(stderr,"%s %s:%d\n",prefix,str,(int)ntohs(sa_in->sin_port));
}


/* gets information back from any of the nodes in the cluster */
int
citrusleaf_info_cluster(cl_cluster *asc, char *names, char **values_r, bool send_asis, int timeout_ms)
{
	if (timeout_ms == 0) timeout_ms = 100; // milliseconds
	uint64_t start = cf_getms();
	uint64_t end = start + timeout_ms;
	char *values = 0;
	
	//
	// not sure yet about the thread safety of this - I have only read-only use
	// of these vectors
	// 
	for (uint i=0;i<cf_vector_size(&asc->node_v);i++) {
		
		cl_cluster_node *cn = cf_vector_pointer_get(&asc->node_v, i);
		if (cn == 0) continue;
		
		for (uint j=0;j<cf_vector_size(&cn->sockaddr_in_v);j++) {
			
			struct sockaddr_in *sa_in = cf_vector_getp(&cn->sockaddr_in_v, j);
			if (sa_in == 0) continue;
			
//			dump_sockaddr_in("info_cluster call to address ",sa_in);
			
			values = 0;
			
			if (0 == citrusleaf_info_host(sa_in, names, &values, end - cf_getms(), send_asis)) {
				// success
				*values_r = values;
				return(0);
			}
			if (cf_getms() >= end) 
				return(-1);
		}
	}
	return(-1);
}
	
/* gets information back from ALL of the nodes in the cluster */
/* @TODO error checking in case a node doens't return the same value as another */
int
citrusleaf_info_cluster_all(cl_cluster *asc, char *names, char **values_r, bool send_asis, int timeout_ms)
{
	if (timeout_ms == 0) timeout_ms = 100; // milliseconds
	uint64_t start = cf_getms();
	uint64_t end = start + timeout_ms;
	char *values = 0;
	
	//
	// not sure yet about the thread safety of this - I have only read-only use
	// of these vectors
	// 
	for (uint i=0;i<cf_vector_size(&asc->node_v);i++) {
		
		cl_cluster_node *cn = cf_vector_pointer_get(&asc->node_v, i);
		if (cn == 0) continue;
		
		for (uint j=0;j<cf_vector_size(&cn->sockaddr_in_v);j++) {
			
			struct sockaddr_in *sa_in = cf_vector_getp(&cn->sockaddr_in_v, j);
			if (sa_in == 0) continue;
			
			// dump_sockaddr_in("info_cluster call to address ",sa_in);
			
			values = 0;
			
			if (0 == citrusleaf_info_host(sa_in, names, &values, end - cf_getms(), send_asis)) {
				// success
				*values_r = values;
				break;
			} 			
			if (cf_getms() >= end) {
				fprintf(stderr, "failing clinfo_cluster_all() timeout %d\n",timeout_ms);
				return(-1);
			}
		}
	}
	return(0);
}	

