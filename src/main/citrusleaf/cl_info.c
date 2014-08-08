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
 
#include <citrusleaf/cl_info.h>
#include <aerospike/as_admin.h>
#include <aerospike/as_cluster.h>
#include <aerospike/as_info.h>
#include <aerospike/as_lookup.h>
#include <citrusleaf/cf_b64.h>
#include <citrusleaf/cf_log_internal.h>
#include <citrusleaf/cf_proto.h>
#include <citrusleaf/cf_socket.h>
#include <sys/types.h>
#include <sys/socket.h> // socket calls
#include <stdio.h>
#include <errno.h> //errno
#include <unistd.h> // close
#include <string.h>
#include <pthread.h>
#include <fcntl.h>
#include <arpa/inet.h>

// #define DEBUG_INFO 1

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
	// Actually doing a non-blocking connect
	int fd = cf_socket_create_and_connect_nb(sa_in);
	
	if (fd == -1) {
		return -1;
	}
	
	int rv = citrusleaf_info_host_limit(fd, names, values, timeout_ms, send_asis, 0, check_bounds);
	
	shutdown(fd, SHUT_RDWR);
	cf_close(fd);
	
	return rv;
}

// Authenticate connection and request the info of a particular sockaddr_in.
// values should be freed on both success and error (where values contains error message).
// Return 0 on success.
int
citrusleaf_info_host_auth(as_cluster* cluster, struct sockaddr_in *sa_in, char *names, char **values, int timeout_ms, bool send_asis, bool check_bounds)
{
	int fd = cf_socket_create_and_connect_nb(sa_in);
	
	if (fd == -1) {
		*values = 0;
		return CITRUSLEAF_FAIL_UNAVAILABLE;
	}
	
	if (cluster->user) {
		int status = as_authenticate(fd, cluster->user, cluster->password, timeout_ms);
		
		if (status) {
			cf_debug("Authentication failed for %s", cluster->user);
			cf_close(fd);
			*values = 0;
			return status;
		}
	}
	
	int rc = citrusleaf_info_host_limit(fd, names, values, timeout_ms, send_asis, 0, check_bounds);
	shutdown(fd, SHUT_RDWR);
	cf_close(fd);
	
	if (rc) {
		*values = 0;
		return rc;
	}
	
	char* error = 0;
	rc = citrusleaf_info_validate(*values, &error);
	
	if (rc) {
		char* rs = strdup(error);
		free(*values);
		*values = rs;
	}
	return rc;
}

// Request the info of a particular sockaddr_in.
// Reject info request if response length is greater than max_response_length.
// Return 0 on success and -1 on error.
int
citrusleaf_info_host_limit(int fd, char *names, char **values, int timeout_ms, bool send_asis, uint64_t max_response_length, bool check_bounds)
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
#ifdef DEBUG_INFO
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
#ifdef DEBUG_INFO
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
		cf_debug("rsp size is 0");
		*values = 0;
	}
	rv = 0;

Done:	
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
	
	if (! as_lookup(NULL, hostname, port, true, &sockaddr_in_v)) {
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

int
citrusleaf_info_auth(as_cluster *cluster, char *hostname, short port, char *names, char **values, int timeout_ms)
{
	as_vector sockaddr_in_v;
	as_vector_inita(&sockaddr_in_v, sizeof(struct sockaddr_in), 5);
	
	if (! as_lookup(NULL, hostname, port, true, &sockaddr_in_v)) {
		as_vector_destroy(&sockaddr_in_v);
		return CITRUSLEAF_FAIL_UNAVAILABLE;
	}
	
	int rc = CITRUSLEAF_FAIL_UNAVAILABLE;
	
	for (uint32_t i = 0; i < sockaddr_in_v.size && rc == CITRUSLEAF_FAIL_UNAVAILABLE; i++)
	{
		if (i > 0) {
			free(*values);
		}
		struct sockaddr_in* sa_in = as_vector_get(&sockaddr_in_v, i);
		rc = citrusleaf_info_host_auth(cluster, sa_in, names, values, timeout_ms, true, /* check bounds */ true);
	}
	as_vector_destroy(&sockaddr_in_v);
	return rc;
}

/* gets information back from any of the nodes in the cluster */
int
citrusleaf_info_cluster(as_cluster *cluster, char *names, char **values, bool send_asis, bool check_bounds, int timeout_ms)
{
	// Try each node until one succeeds.
	if (timeout_ms == 0) {
		timeout_ms = 1000;
	}
	
	int rc = CITRUSLEAF_FAIL_UNAVAILABLE;
	uint64_t start = cf_getms();
	uint64_t end = start + timeout_ms;
	as_nodes* nodes = as_nodes_reserve(cluster);
	
	for (uint32_t i = 0; i < nodes->size; i++) {
		if (i > 0) {
			free(*values);
		}
		
		as_node* node = nodes->array[i];
		struct sockaddr_in* sa_in = as_node_get_address(node);
		rc = citrusleaf_info_host_auth(cluster, sa_in, names, values, (int)(end - cf_getms()), send_asis, check_bounds);
		
		if (rc == CITRUSLEAF_FAIL_UNAVAILABLE) {
			if (cf_getms() >= end) {
				rc = CITRUSLEAF_FAIL_TIMEOUT;
				break;
			}
		}
		else {
			break;
		}
	}
	as_nodes_release(nodes);
	return rc;
}

int
citrusleaf_info_cluster_foreach(
	as_cluster *cluster, const char *command, bool send_asis, bool check_bounds, int timeout_ms, void *udata, char** error,
	bool (*callback)(const as_node * node, const char *command, char *value, void *udata)
)
{
	//Usage Notes:
	//udata = memory allocated by caller, passed back to the caller callback function, ufn()
	//command = command string, memory allocated by caller, caller must free it, passed to server for execution
	//value = memory allocated by c-client for caller, caller must free it after using it.
	if (timeout_ms == 0) {
		timeout_ms = 1000;
	}
	*error = 0;
	
	int rc = CITRUSLEAF_FAIL_UNAVAILABLE;
	uint64_t start = cf_getms();
	uint64_t end = start + timeout_ms;
	as_nodes* nodes = as_nodes_reserve(cluster);
	
	for (uint32_t i = 0; i < nodes->size; i++) {
		as_node* node = nodes->array[i];
		struct sockaddr_in* sa_in = as_node_get_address(node);
		char* response = 0;

		rc = citrusleaf_info_host_auth(cluster, sa_in, (char *)command, &response, (int)(end - cf_getms()), send_asis, check_bounds);
		
		if (rc == 0) {
			bool status = callback(node, command, response, udata);
			free(response);
			
			if (! status) {
				rc = CITRUSLEAF_FAIL_QUERY_ABORTED;
				break;
			}
		}
		else {
			free(response);
			if (rc != CITRUSLEAF_FAIL_UNAVAILABLE) {
				break;
			}
		}
				
		if (cf_getms() >= end) {
			rc = CITRUSLEAF_FAIL_TIMEOUT;
			break;
		}
	}
	as_nodes_release(nodes);
	return rc;
}

static int
citrusleaf_info_parse_error(char* begin, char** message)
{
	// Parse error format: <code>:<message>\n
	char* end = strchr(begin, ':');
	
	if (! end) {
		*message = 0;
		return CITRUSLEAF_FAIL_UNKNOWN;
	}
	*end = 0;
	
	int rc = atoi(begin);
	
	if (rc == 0) {
		*message = 0;
		return CITRUSLEAF_FAIL_UNKNOWN;
	}
	end++;
	
	char* newline = strchr(end, '\n');
	
	if (newline) {
		*newline = 0;
	}
	
	*message = end;
	return rc;
}

static void
citrusleaf_info_decode_error(char* begin)
{
	// Decode base64 message in place.
	// UDF error format: <error message>;file=<file>;line=<line>;message=<base64 message>\n
	char* msg = strstr(begin, "message=");
	
	if (msg) {
		msg += 8;
		
		uint32_t src_len = (uint32_t)strlen(msg) - 1; // Ignore newline '\n' at the end
		uint32_t trg_len = 0;
		
		if (cf_b64_validate_and_decode_in_place((uint8_t*)msg, src_len, &trg_len)) {
			msg[trg_len] = 0;
		}
	}
}

int
citrusleaf_info_validate(char* response, char** message)
{
	char* p = response;
	
	if (p) {
		// Check for errors embedded in the response.
		// ERROR: may appear at beginning of string.
		if (strncmp(p, "ERROR:", 6) == 0) {
			return citrusleaf_info_parse_error(p + 6, message);
		}
		
		// ERROR: or FAIL: may appear after a tab.
		while ((p = strchr(p, '\t'))) {
			p++;
			
			if (strncmp(p, "ERROR:", 6) == 0) {
				return citrusleaf_info_parse_error(p + 6, message);
			}
			
			if (strncmp(p, "FAIL:", 5) == 0) {
				return citrusleaf_info_parse_error(p + 5, message);
			}
			
			if (strncmp(p, "error=", 6) == 0) {
				*message = p;
				citrusleaf_info_decode_error(p + 6);
				return CITRUSLEAF_FAIL_UDF_BAD_RESPONSE;
			}
		}
	}
	return 0;
}
