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

#include <netdb.h> //gethostbyname_r

#include "citrusleaf/citrusleaf.h"
#include "citrusleaf/cl_cluster.h"
#include "citrusleaf/proto.h"
#include "citrusleaf/cf_log_internal.h"

// #define DEBUG

//
// Do a lookup on the given name and port.
// Add the returned values to the passed in vector, which
// must support sockaddr_in sized elements
// The addition to the vector will be done via a 'unique' add,
// just in case
// Return 0 in case of success
//
extern int h_errno;

int
cl_lookup(cl_cluster *asc, char *hostname, short port, cf_vector *sockaddr_in_v)
{
	// do the gethostbyname to find the IP address
	size_t hstbuflen = 1024;
	uint8_t	stack_hstbuf[hstbuflen];
	void *tmphstbuf = stack_hstbuf;
	int rv, herr, addrmapsz;
	struct hostent hostbuf, *hp;
	cl_addrmap *map;
	int retry = 0;
	//Find if there is an alternate address that should be used for this hostname.
	if (asc && (asc->host_addr_map_v.len > 0)) {
		addrmapsz = asc->host_addr_map_v.len;
		for (int i=0; i<addrmapsz; i++) {
			map = cf_vector_pointer_get(&asc->host_addr_map_v, i);
			if (map && strcmp(map->orig, hostname) == 0) {
				//found a mapping for this address. Use the alternate one.
				cf_debug("Using %s instead of %s", map->alt, hostname);
				hostname = map->alt;
				break;
			}
		}
	}

	do {
#ifdef OSX // on OSX, gethostbyname is thread safe and there is no '_r' version 
		hp = gethostbyname2(hostname, AF_INET);
		rv = 0;
		if(hp == NULL){
			herr = h_errno; // I'm hoping this is thread-safe too, in the Mac world...
		}
#else
		rv = gethostbyname2_r(hostname, AF_INET, &hostbuf, tmphstbuf, hstbuflen,
				&hp, &herr);
#endif
		/* TRY_AGAIN for a maximun of 3 times, after which throw an error */
		if(retry > 2) {
			cf_error("gethostbyname of %s - maxmimum retries failed", hostname);
			retry = 0;
			return -1;
		}
		if (hp == NULL) {
			hostname = hostname ? hostname : "NONAME";
			switch(herr) {
				case HOST_NOT_FOUND:
					cf_error("gethostbyname says no host at %s", hostname);
					break;
				case NO_ADDRESS:
					cf_error("gethostbyname of %s says invalid address (errno %d)", hostname, herr);
					break;
				case NO_RECOVERY:
					cf_error("gethostbyname of %s says form error (errno %d)", hostname, herr);
					break;
				case TRY_AGAIN:
					cf_error("gethostbyname of %s returned TRY_AGAIN, try again (rv=%d)", hostname, rv);
					retry++;
					continue;
				default:
					cf_error("gethostbyname of %s returned an unknown error (errno %d)", hostname, herr);
					break;
			}
			if (tmphstbuf != stack_hstbuf)		free(tmphstbuf);
			return(-1);
		}
		else if (rv != 0) {
			if (rv == ERANGE) {
				hstbuflen *= 2;
				if (tmphstbuf == stack_hstbuf)
					tmphstbuf = malloc(hstbuflen);
				else
					tmphstbuf = realloc (tmphstbuf, hstbuflen);
				if (!tmphstbuf) {
					cf_error("malloc fail");
					return(-1);
				}
			}
			else if (rv == EAGAIN || herr == TRY_AGAIN) {
				cf_error("gethostbyname returned EAGAIN, try again");
				retry++;
			}
			else if (rv == ETIMEDOUT) {
				cf_error("gethostbyname for %s timed out", hostname ? (hostname): "NONAME");
				if (tmphstbuf != stack_hstbuf)		free(tmphstbuf);
				return(-1);
			}
			else {
				cf_error("gethostbyname returned an unknown error %d %d (errno %d)",rv,herr, errno);
				if (tmphstbuf != stack_hstbuf)		free(tmphstbuf);
				return(-1);
			}
		}
	} while ((rv != 0) || (hp == NULL));

#ifdef DEBUG
	cf_debug("host lookup: %s canonical: %s addrtype %d length: %d",
		hostname, hp->h_name, hp->h_addrtype, hp->h_length);

	for (int i=0;hp->h_aliases[i];i++) {
		cf_debug("  alias %d: %s",i, hp->h_aliases[i]);
	}
	for (int i=0;hp->h_addr_list[i];i++) {
		// todo: print something about the actual address
		cf_debug("  address %d: %x",i,*(uint32_t *) hp->h_addr_list[i]);
	}
#endif

	if (hp->h_addrtype != AF_INET) {
		cf_error("unknown address type %d", hp->h_addrtype);
		if (tmphstbuf != stack_hstbuf)		free(tmphstbuf);
		return(-1);
	}
	
	// sockaddr_in_v is passed as NULL from caller which needs
	// to only check if lookup succeeds. If reach here it is 
	// a successful lookup. 
	if (sockaddr_in_v == NULL) {
		goto ret_success;
	}
	
	// Move into vector
	for (int i=0;hp->h_addr_list[i];i++) {
		struct sockaddr_in 	addr;
		memset(&addr,0,sizeof(addr));
		addr.sin_family = hp->h_addrtype;
		addr.sin_addr.s_addr = *(uint32_t *) hp->h_addr_list[i];
		addr.sin_port = htons(port);
		
		cf_vector_append_unique(sockaddr_in_v, &addr);
	}

ret_success:
	if (tmphstbuf != stack_hstbuf)		free(tmphstbuf);
	
	return(0);
}	


