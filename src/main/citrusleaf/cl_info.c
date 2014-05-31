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
 
#include <sys/types.h>
#include <sys/socket.h> // socket calls
#include <stdio.h>
#include <errno.h> //errno
#include <unistd.h> // close
#include <string.h>
#include <pthread.h>
#include <fcntl.h>
#include <arpa/inet.h>

#include <citrusleaf/cf_socket.h>
#include <citrusleaf/cf_proto.h>

#include <citrusleaf/citrusleaf.h>
#include <aerospike/as_cluster.h>

#include "internal.h"


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


// static int
// info_expire_transaction( void *udata)
// {
// 	int fd = (int) (size_t) udata;
// 	close(fd);
// 	return(0);
// }

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

// Request the info of a particular sockaddr_in.
// Used internally for host-crawling as well as supporting the external interface.
// Return 0 on success and -1 on error.
int
citrusleaf_info_host(struct sockaddr_in *sa_in, char *names, char **values, int timeout_ms, bool send_asis, bool check_bounds)
{
	return citrusleaf_info_host_limit(sa_in, names, values, timeout_ms, send_asis, 0, check_bounds);
}

// Request the info of a particular sockaddr_in.
// Reject info request if response length is greater than max_response_length.
// Return 0 on success and -1 on error.
int
citrusleaf_info_host_limit(struct sockaddr_in *sa_in, char *names, char **values, int timeout_ms, bool send_asis, uint64_t max_response_length, bool check_bounds)
{
	uint bb_size = 2048;
	int rv = -1;
    int io_rv;
	*values = 0;
	
	// Deal with the incoming 'names' parameter
	// Translate interior ';'  in the passed-in names to \n
	uint32_t	slen = 0;
	if (names) {
		if (send_asis) {
			slen = (uint32_t)strlen(names);
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
			slen++;
			// If check bounds is true, do not allow beyond a certain limit
			if	(check_bounds && (slen > bb_size)) {
				return(-1); 
			} 
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
	int fd = cf_socket_create_and_connect_nb(sa_in);
	if (fd == -1) {
		return -1;
	}

	cl_proto 	*req;
	uint8_t		buf[bb_size];
	uint		buf_sz;
	
	bool        rmalloced = false;
	if (names) {
		uint sz = (uint)strlen(names);
		buf_sz = sz + sizeof(cl_proto);
		if (buf_sz < bb_size)
			req = (cl_proto *) buf;
		else {
			req = (cl_proto *) malloc(buf_sz);
			rmalloced = true;
		}
		if (req == NULL)	goto Done;

		req->sz = sz;
		memcpy((void*)req + sizeof(cl_proto), names, sz);
	}
	else {
		req = (cl_proto *) buf;
		req->sz = 0;
		buf_sz = sizeof(cl_proto);
		names = "";
	}
		
	req->version = CL_PROTO_VERSION;
	req->type = CL_PROTO_TYPE_INFO;
	cl_proto_swap_to_be(req);
	
    if (timeout_ms)
        io_rv = cf_socket_write_timeout(fd, (uint8_t *) req, buf_sz, 0, timeout_ms);
    else
        io_rv = cf_socket_write_forever(fd, (uint8_t *) req, buf_sz);
    
	if (rmalloced) {
		free (req); 
	}
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
	cl_proto_swap_from_be(rsp);
	
	if (rsp->sz) {
		size_t read_length = rsp->sz;
		bool limit_reached = false;

		if (max_response_length > 0 && rsp->sz > max_response_length) {
			// Response buffer is too big.  Read a few bytes just to see what the buffer contains.
			read_length = 100;
			limit_reached = true;
		}

		uint8_t *v_buf = malloc(read_length + 1);
		if (!v_buf) {
			cf_warn("Info request '%s' failed. Failed to malloc %d bytes", names, read_length);
			goto Done;
		}

        if (timeout_ms)
            io_rv = cf_socket_read_timeout(fd, v_buf, read_length, 0, timeout_ms);
        else
            io_rv = cf_socket_read_forever(fd, v_buf, read_length);
        
        if (io_rv != 0) {
            free(v_buf);

            if (io_rv != ETIMEDOUT) {
            	cf_warn("Info request '%s' failed. Failed to read %d bytes. Return code %d", names, read_length, io_rv);
            }
            goto Done;
		}
		v_buf[read_length] = 0;

		if (limit_reached) {
			// Response buffer is too big.  Log warning and reject.
			cf_warn("Info request '%s' failed. Response buffer length %lu is excessive. Buffer: %s", names, rsp->sz, v_buf);
			goto Done;
		}
		*values = (char *) v_buf;
	}                                                                                               
	else {
		cf_warn("rsp size is 0");
		*values = 0;
	}
	rv = 0;

Done:	
	shutdown(fd, SHUT_RDWR);
	cf_close(fd);
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
	as_vector sockaddr_in_v;
	as_vector_inita(&sockaddr_in_v, sizeof(struct sockaddr_in), 5);
	
	if (! as_lookup(NULL, hostname, port, &sockaddr_in_v)) {
		goto Done;
	}
		
	for (uint32_t i = 0; i < sockaddr_in_v.size; i++)
	{
		struct sockaddr_in* sa_in = as_vector_get(&sockaddr_in_v, i);

		if (0 == citrusleaf_info_host(sa_in, names, values, timeout_ms, true, /* check bounds */ true)) {
			rv = 0;
			goto Done;
		}
	}
	
Done:
	as_vector_destroy(&sockaddr_in_v);
	return(rv);
}

/* gets information back from any of the nodes in the cluster */
int
citrusleaf_info_cluster(as_cluster *cluster, char *names, char **values_r, bool send_asis, bool check_bounds, int timeout_ms)
{
	if (timeout_ms == 0) {
		timeout_ms = 100; // milliseconds
	}
	
	uint64_t start = cf_getms();
	uint64_t end = start + timeout_ms;
	int ret = -1;

	as_nodes* nodes = as_nodes_reserve(cluster);
	
	for (uint32_t i = 0; i < nodes->size; i++) {
		as_node* node = nodes->array[i];
		struct sockaddr_in* sa_in = as_node_get_address(node);
		char* values = 0;
		
		if (citrusleaf_info_host(sa_in, names, &values, (int)(end - cf_getms()), send_asis, check_bounds) == 0) {
			*values_r = values;
			ret = 0;
			break;
		}
		
		if (cf_getms() >= end) {
			ret = -2;
			break;
		}
	}
	as_nodes_release(nodes);
	return ret;
}

int
citrusleaf_info_cluster_foreach(
	as_cluster *cluster, const char *command, bool send_asis, bool check_bounds, int timeout_ms, void *udata,
	bool (*callback)(const as_node * node, const struct sockaddr_in * sa_in, const char *command, char *value, void *udata)
)
{
	//Usage Notes:
	//udata = memory allocated by caller, passed back to the caller callback function, ufn()
	//command = command string, memory allocated by caller, caller must free it, passed to server for execution
	//value = memory allocated by c-client for caller, caller must free it after using it.
	
	if (timeout_ms == 0) {
		timeout_ms = 100; // milliseconds
	}
	
	uint64_t start = cf_getms();
	uint64_t end = start + timeout_ms;
	int ret = 0;

	as_nodes* nodes = as_nodes_reserve(cluster);
	
	for (uint32_t i = 0; i < nodes->size; i++) {
		as_node* node = nodes->array[i];
		struct sockaddr_in* sa_in = as_node_get_address(node);
		char* value = 0;

		if (citrusleaf_info_host(sa_in, (char *)command, &value, (int)(end - cf_getms()), send_asis, check_bounds) == 0) {
			bool status = callback(node, sa_in, command, value, udata);
			
			if (value) {
				free(value);
			}
			
			if(! status) {
				ret = -1;
				break;
			}
		}
		
		if (cf_getms() >= end) {
			ret = -2;
			break;
		}
	}
	as_nodes_release(nodes);
	return ret;
}
