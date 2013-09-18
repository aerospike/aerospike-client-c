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
#include <stdlib.h> //fprintf
#include <unistd.h> // close
#include <string.h>
#include <pthread.h>
#include <fcntl.h>
#include <arpa/inet.h> // inet_ntop
#include <signal.h>
#include <ctype.h>
#include <netdb.h> //gethostbyname_r
#include <netinet/tcp.h>

#include <citrusleaf/cf_client_rc.h>
#include <citrusleaf/cf_proto.h>
#include <citrusleaf/cf_queue.h>
#include <citrusleaf/cf_vector.h>

#include <citrusleaf/citrusleaf.h>
#include <citrusleaf/cl_cluster.h>

#include "internal.h"



// #define INFO_TIMEOUT_MS 100
#define INFO_TIMEOUT_MS 300
//#define DEBUG 1
// #define DEBUG_VERBOSE 1

// Forward references
static void cluster_tend( cl_cluster *asc); 

#include <time.h>
static inline void print_ms(char *pre)
{
	cf_debug("%s %"PRIu64, pre, cf_getms());
}

int g_clust_initialized = 0;
static int g_clust_tend_speed = 1;
extern int g_init_pid;

//
// Debug function. Should be elsewhere.
//

static void
dump_sockaddr_in(char *prefix, struct sockaddr_in *sa_in)
{
	if (cf_debug_enabled()) {
		char str[INET_ADDRSTRLEN];
		inet_ntop(AF_INET, &(sa_in->sin_addr), str, INET_ADDRSTRLEN);
		cf_debug("%s %s:%d", prefix, str, (int)ntohs(sa_in->sin_port));
	}
}

#ifdef DEBUG	
static void
dump_cluster(cl_cluster *asc)
{
	if (cf_debug_enabled()) {
		pthread_mutex_lock(&asc->LOCK);

		cf_debug("registered hosts:");
		for (uint i=0;i<cf_vector_size(&asc->host_str_v);i++) {
			char *host_s = cf_vector_pointer_get(&asc->host_str_v,i);
			int   port = cf_vector_integer_get(&asc->host_port_v,i);
			cf_debug(" host %d: %s:%d",i,host_s,port);
		}

		cf_debug("nodes: %u",cf_vector_size(&asc->node_v));
		for (uint i=0;i<cf_vector_size(&asc->node_v);i++) {
			cl_cluster_node *cn = cf_vector_pointer_get(&asc->node_v, i);
			struct sockaddr_in *sa_in = cf_vector_getp(&cn->sockaddr_in_v, 0);
			char str[INET_ADDRSTRLEN];
			inet_ntop(AF_INET, &(sa_in->sin_addr), str, INET_ADDRSTRLEN);
			cf_debug("%d %s : %s:%d (%d conns) (%d async conns)",i,cn->name,str,
				(int)ntohs(sa_in->sin_port),cf_queue_sz(cn->conn_q),
				cf_queue_sz(cn->conn_q_asyncfd));
		}
		cf_debug("partitions: %d",asc->n_partitions);
		pthread_mutex_unlock(&asc->LOCK);
	}
}
#endif

cl_cluster_node *
cl_cluster_node_get_byaddr(cl_cluster *asc, struct sockaddr_in *sa_in)
{
	// No need to lock nodes list because this function is only called by
	// cluster_tend() thread which has exclusive write access.
	for (uint i=0;i<cf_vector_size(&asc->node_v);i++) {
		cl_cluster_node *cn = cf_vector_pointer_get(&asc->node_v, i);
		for (uint j=0;j<cf_vector_size(&cn->sockaddr_in_v);j++) {
			struct sockaddr_in *node_sa_in = cf_vector_getp(&cn->sockaddr_in_v, j);
			if (memcmp(sa_in, node_sa_in, sizeof(struct sockaddr_in) ) == 0 ) {
				return(cn);
			}
		}
	}
	return(0);
}


//
// Useful utility function for splitting into a vector
// fmt is a string of characters to split on
// the string is the string to split
// the vector will have pointers into the strings
// this modifies the input string by inserting nulls
//

static void
str_split(char split_c, char *str, cf_vector *v)
{
	char *prev = str;
	while (*str) {
		if (split_c == *str) {
			*str = 0;
			cf_vector_append(v, &prev);
			prev = str+1;
		}
		str++;
	}
	if (prev != str) {
		cf_vector_append(v, &prev);
	}
}



// List of all current clusters so the tender can maintain them
// 
static pthread_t	tender_thr;
static cf_ll		cluster_ll;
static pthread_mutex_t cluster_ll_LOCK = PTHREAD_MUTEX_INITIALIZER; 

cl_cluster *
citrusleaf_cluster_create(void)
{
    if (! g_clust_initialized )    return(0);
    
	cl_cluster *asc = malloc(sizeof(cl_cluster));
	if (!asc)	return(0);
	
	asc->state = 0;
	asc->follow = true;
	asc->nbconnect = false;
	asc->found_all = false;
	asc->last_node = 0;
	asc->ref_count = 1;
	// Default is 0 so the cluster uses global tend speed.
	// For the cluster user has to specifically set the own
	// value
	asc->tend_speed = 0;
    asc->info_timeout = INFO_TIMEOUT_MS;
	
	pthread_mutex_init(&asc->LOCK, 0);
	
	cf_vector_pointer_init(&asc->host_str_v, 10, 0);
	cf_vector_integer_init(&asc->host_port_v, 10, 0);
	cf_vector_pointer_init(&asc->host_addr_map_v, 10, 0);
	cf_vector_pointer_init(&asc->node_v, 10, 0);
	
	pthread_mutex_lock(&cluster_ll_LOCK);
	cf_ll_append(&cluster_ll, (cf_ll_element *) asc);
	pthread_mutex_unlock(&cluster_ll_LOCK);
	
	asc->n_partitions = 0;
	asc->partition_table_head = 0;
	
	return(asc);
}

// Wrapper over create function and add_host to check if a cluster object for 
// this host is already created. Return the object if it exists or else
// create the cluster object, add the host and return the new object.
cl_cluster *
citrusleaf_cluster_get_or_create(char *host, short port, int timeout_ms)
{
	if (! g_clust_initialized )    return(0);
#ifdef DEBUG
        cf_debug("get or create for host %s:%d",host, (int)port);
#endif
	cl_cluster *asc = NULL;

	// Check if the host and port exists in the linked list of hosts
	pthread_mutex_lock(&cluster_ll_LOCK);
	cf_ll_element *cur_element = cf_ll_get_head(&cluster_ll);
	char *hostp;
	short portp;
	while(cur_element) {
		asc = (cl_cluster *) cur_element;
		pthread_mutex_lock(&asc->LOCK);
		for (uint32_t i=0; i<cf_vector_size(&asc->host_str_v); i++) {
			hostp = (char*) cf_vector_pointer_get(&asc->host_str_v, i);
			portp = (int ) cf_vector_integer_get(&asc->host_port_v, i);
			if ((strncmp(host,hostp,strlen(host)+1) == 0) && (port == portp)) {
				// Found the cluster object.
				// Increment the reference count
				#ifdef DEBUG
				cf_debug("host already added on a cluster object. Increment ref_count (%d) and returning pointer - %p", asc->ref_count, asc);
				#endif
				asc->ref_count++;
				pthread_mutex_unlock(&asc->LOCK);
				pthread_mutex_unlock(&cluster_ll_LOCK);
				return(asc);
			}
		}
		pthread_mutex_unlock(&asc->LOCK);
		cur_element = cf_ll_get_next(cur_element);
	}
	pthread_mutex_unlock(&cluster_ll_LOCK);

	// Cluster object for this host does not exist. Create new.
	asc = citrusleaf_cluster_create();
	if (!asc) {
		cf_error("get_or_create - could not create cluster");
		return(0);
	}

	// Add the host to the created cluster object	
	int ret = citrusleaf_cluster_add_host(asc, host, port, timeout_ms);
	if (0 != ret) {
		cf_error("get_or_create - add_host failed with error %d", ret);
		citrusleaf_cluster_release_or_destroy(&asc);
		return(0);
	}

	return(asc);
}

//
// Major TODO!
// * destroy all the linked hosts
// * remove self from cluster list
//

void
citrusleaf_cluster_destroy(cl_cluster *asc)
{
	// First remove the entry from global linked list of clusters
	// so that the tender function will not look into it.
	// As there is no destruction function specified in cf_ll_init()
	// the element will not freed in cf_ll_delete. It will just be 
	// removed from the linked list. So we need to free it later in
	// this function
	pthread_mutex_lock(&cluster_ll_LOCK);
	cf_ll_delete( &cluster_ll , (cf_ll_element *) asc);
	pthread_mutex_unlock(&cluster_ll_LOCK);

retry:
	pthread_mutex_lock(&asc->LOCK);
	if (asc->state & CLS_TENDER_RUNNING) {
		//If tender process is active, we cannot destroy at the moment
		pthread_mutex_unlock(&asc->LOCK);
		sleep(1);
		goto retry;
	}
	asc->state |= CLS_FREED;
	pthread_mutex_unlock(&asc->LOCK);

	for (uint i=0;i<cf_vector_size(&asc->host_str_v);i++) {
		char * host_str = cf_vector_pointer_get(&asc->host_str_v, i);
		free(host_str);
	}
	cf_vector_destroy(&asc->host_str_v);
	cf_vector_destroy(&asc->host_port_v);
	
	for (uint i=0;i<cf_vector_size(&asc->host_addr_map_v);i++) {
		cl_addrmap * addr_map_str = cf_vector_pointer_get(&asc->host_addr_map_v, i);
		free(addr_map_str->orig);
		free(addr_map_str->alt);
		free(addr_map_str);
	}
	cf_vector_destroy(&asc->host_addr_map_v);

	for (uint i=0;i<cf_vector_size(&asc->node_v);i++) {
		cl_cluster_node *cn = cf_vector_pointer_get(&asc->node_v, i);
		cl_cluster_node_release(cn, "C-");
	}
	cf_vector_destroy(&asc->node_v);
	
	cl_partition_table_destroy_all(asc);
	
	pthread_mutex_destroy(&asc->LOCK);

	free(asc);

}

// Function to release (decrement reference count) and destroy the object if
// the reference count becomes 0
void
citrusleaf_cluster_release_or_destroy(cl_cluster **asc) {
#ifdef DEBUG
	if (asc && *asc) {
		cf_debug("release or destroy for cluster object - %p. ref_count = %d", *asc, (*asc)->ref_count);
	} else {
		cf_debug("release or destroy - asc is  NULL");
	}
#endif

	if (!asc || !(*asc))
		return;

	pthread_mutex_lock(&(*asc)->LOCK);
	if ((*asc)->ref_count > 0) {
		(*asc)->ref_count--;

		if (0 == (*asc)->ref_count) {
			pthread_mutex_unlock(&(*asc)->LOCK);
			// Destroy the object as reference count is 0
			#ifdef DEBUG
			cf_debug("destroying the cluster object as reference count is 0");
			#endif
			citrusleaf_cluster_destroy(*asc);
			*asc = NULL;
			return;
		}
	}
	pthread_mutex_unlock(&(*asc)->LOCK);
}


void
citrusleaf_cluster_shutdown(void)
{
	// this one use has to be threadsafe, because two simultaneous shutdowns???
	cf_ll_element *e;
	// pthread_mutex_lock(&cluster_ll_LOCK);
	while ((e = cf_ll_get_head(&cluster_ll))) {
		cl_cluster *asc = (cl_cluster *)e; 
		citrusleaf_cluster_destroy(asc); // safe?
	}

	/* Cancel tender thread */	
	pthread_cancel(tender_thr);

	/* 
	 * If a process is forked, the threads in it do not get spawned in the child process.
	 * In citrusleaf_init(), we are remembering the processid(g_init_pid) of the process who spawned the 
	 * background threads. If the current process is not the process who spawned the background threads
	 * then it cannot call pthread_join() on the threads which does not exist in this process.
	 */
	if(g_init_pid == getpid()) {
		pthread_join(tender_thr,NULL);
	}

	// pthread_mutex_unlock(&cluster_ll_LOCK);
}

cl_rv
citrusleaf_cluster_add_host(cl_cluster *asc, char const *host_in, short port, int timeout_ms)
{
	int rv = CITRUSLEAF_OK;
#ifdef DEBUG	
	cf_debug("adding host %s:%d timeout %d",host_in, (int)port, timeout_ms);
#endif	
	// Find if the host has already been added on this cluster object
	char *hostp;
	// int portp, found = 0;
	int portp;
	pthread_mutex_lock(&asc->LOCK);
	for (uint32_t i=0; i<cf_vector_size(&asc->host_str_v); i++) {
		hostp = (char*) cf_vector_pointer_get(&asc->host_str_v, i);
		portp = (int ) cf_vector_integer_get(&asc->host_port_v, i);
		if ((strncmp(host_in,hostp,strlen(host_in)+1)==0) && (port == portp)) {
			// Return OK if host is already added in the list
			pthread_mutex_unlock(&asc->LOCK);
			#ifdef DEBUG
			cf_debug("host already added in this cluster object. Return OK");
			#endif
			return(CITRUSLEAF_OK);
		}
	}

	char *host = strdup(host_in);

	pthread_mutex_unlock(&asc->LOCK);
	// Lookup the address before adding to asc. If lookup fails
	// return CITRUSLEAF_FAIL_CLIENT
	// Resolve - error message need to change.
	cf_vector_define(sockaddr_in_v, sizeof( struct sockaddr_in ), 0);
 	if(cl_lookup(asc, host, port, &sockaddr_in_v) != 0) {
		rv = CITRUSLEAF_FAIL_CLIENT;
		goto cleanup;
	}
	
	// Host not found on this cluster object
	// Add the host and port to the lists of hosts to try when maintaining
	pthread_mutex_lock(&asc->LOCK);
	cf_vector_pointer_append(&asc->host_str_v, host);
	cf_vector_integer_append(&asc->host_port_v, (int) port);
	pthread_mutex_unlock(&asc->LOCK);
	asc->found_all = false; // Added a new item in the list. Mark the cluster not fully discovered.

	// Fire the normal tender function to speed up resolution
	cluster_tend(asc);

	if (timeout_ms == 0)	timeout_ms = 100;
	
	if (timeout_ms > 0) {
		int n_tends = 0;
		uint64_t start_ms = cf_getms();
		do {
			n_tends++;
			if (asc->found_all == false) { cluster_tend(asc); }
			if (asc->found_all == false) usleep(1000);
		} while ((asc->found_all == false) && ((cf_getms() - start_ms) < (unsigned int) timeout_ms) );
#ifdef DEBUG		
		cf_debug("add host: required %d tends %"PRIu64"ms to set right", n_tends, cf_getms() - start_ms);
#endif		
	}

	// The cluster may or many not be fully discovered (found_all is true/false). 
	// found_all flag only signifies if the full cluster is discovered or not.
	// It does not signify if the newly added node is reacheable or not.
	// We should check if the newly added node is in the list of reacheable nodes. 
	bool reacheable = false;
	for (uint i=0;i<cf_vector_size(&sockaddr_in_v);i++) {
		struct sockaddr_in *sin = cf_vector_getp(&sockaddr_in_v, i);
		if (0 != cl_cluster_node_get_byaddr(asc, sin)) {
			reacheable = true;
		}
	}
	if (!reacheable) {
		rv = CITRUSLEAF_FAIL_TIMEOUT;
		goto cleanup;
	}

cleanup:
	cf_vector_destroy(&sockaddr_in_v);
	return rv;
}

void
citrusleaf_cluster_add_addr_map(cl_cluster *asc, char *orig, char *alt)
{
	int	i, vsz;
	cl_addrmap *oldmap=NULL;


	//Search if the given mapping already exists
	vsz = asc->host_addr_map_v.len;
	for (i=0; i<vsz; i++)
	{
		oldmap = cf_vector_pointer_get(&asc->host_addr_map_v, i);
		if (oldmap && strcmp(oldmap->orig, orig) == 0) {
			//The original address is already in the map. update its alternative.
			free(oldmap->alt); //free the old string.
			oldmap->alt = strdup(alt);
			return;
		}
	}

	//We need to add the supplied map only if does not already exist
	if (i==vsz) {
		cl_addrmap *newmap = (cl_addrmap *) malloc(sizeof(cl_addrmap));
		if (newmap == NULL) {
			return;
		}

		newmap->orig = strdup(orig);
		newmap->alt = strdup(alt);
		cf_vector_pointer_append(&asc->host_addr_map_v, newmap);
		//cf_debug("Adding the mapping %x:%s->%s", newmap, orig, alt);
	} 
	else {
		//cf_debug("Mapping %s->%s already exists", oldmap->orig, oldmap->alt);
	}

}

bool 
citrusleaf_cluster_settled(cl_cluster *asc)
{
	return( asc->found_all );
}

int
citrusleaf_cluster_get_nodecount(cl_cluster *asc)
{
	return cf_vector_size(&asc->node_v);
}

void
citrusleaf_cluster_follow(cl_cluster *asc, bool flag)
{
	asc->follow = flag;
}

//
// this helper is specific to the PHP implementation, although maybe it's not too bad
// an idea elsewhere
//
cl_cluster *
citrusleaf_cluster_get(char const *url)
{
//	cf_debug(" cluster get: %s",url);

	// make sure it's a citrusleaf url
	char *urlx = strdup(url);
	char *proto = strchr(urlx, ':');
	if (!proto) {
		cf_error("warning: url %s illegal for citrusleaf connect", url);
		free(urlx);
		return(0);
	}
	*proto = 0;
	if (strcmp(proto, "citrusleaf") == 0) {
		cf_error("warning: url %s illegal for citrusleaf connect", url);
		free(urlx);
		return(0);
	}
	char *host = proto+3;
	char *port = strchr(host, ':');
	int port_i = 0;

	if (port) {
		*port = 0; // will terminate the host string properly
		port_i = atoi(port + 1);
	}
	else {
		port = strchr(host, '/');
		if (port) *port = 0;
	}
	if (port_i == 0) port_i = 3000; 

	// cf_debug(" cluster get: host %s port %d",host, port_i);

	// search the cluster list for matching url open names
	cl_cluster *asc = 0;
	pthread_mutex_lock(&cluster_ll_LOCK);
	cf_ll_element *e = cf_ll_get_head(&cluster_ll);
	while (e && asc == 0) {
		cl_cluster *cl_asc = (cl_cluster *) e;

		// cf_debug(" cluster get: comparing against %p",cl_asc);

		uint i;
		pthread_mutex_lock(&cl_asc->LOCK);
		for (i=0;i<cf_vector_size(&cl_asc->host_str_v);i++) {
			char *cl_host_str = cf_vector_pointer_get(&cl_asc->host_str_v, i);
			int   cl_port_i = cf_vector_integer_get(&cl_asc->host_port_v, i);

			// cf_debug(" cluster get: comparing against %s %d",cl_host_str, cl_port_i);

			if (strcmp(cl_host_str, host)!= 0)	continue;
			if (cl_port_i == port_i) {
				// found
				asc = cl_asc;
				break;
			}
			// cf_debug(" cluster get: comparing against %p",cl_asc);
		}
		pthread_mutex_unlock(&cl_asc->LOCK);

		e = cf_ll_get_next(e);
	}
	pthread_mutex_unlock(&cluster_ll_LOCK);

	if (asc) {
		// cf_debug(" cluster get: reusing cluster %p",asc);
		free(urlx);
		return(asc);
	}

	// doesn't exist yet? create a new one
	asc = citrusleaf_cluster_create();
	citrusleaf_cluster_add_host(asc, host, port_i, 0);

    // check to see if we actually got some initial node
	uint node_v_sz = cf_vector_size(&asc->node_v);
    if (node_v_sz==0) {
		cf_error("no node added in initial create");
        citrusleaf_cluster_destroy(asc);
    	free(urlx);
        return NULL;
    }
        
	// cf_debug(" cluster get: new cluster %p",asc);

	free(urlx);
	return(asc);	
}

cl_cluster_node *
cl_cluster_node_create(char *name, struct sockaddr_in *sa_in)
{
	cl_cluster_node *cn = cf_client_rc_alloc( sizeof(cl_cluster_node ) );
	if (!cn)	return(0);

#ifdef DEBUG_NODE_REF_COUNT
	// To balance the ref-count logs, we need this:
	cf_debug("node reserve: %s %s %p : %d", "C+", name, cn, cf_client_rc_count(cn));
#endif

	strcpy(cn->name, name);

	cn->intervals_absent = 0;

	cf_vector_init(&cn->sockaddr_in_v, sizeof( struct sockaddr_in ), 5, 0);
	cf_vector_append(&cn->sockaddr_in_v, sa_in);
	
	cn->conn_q = cf_queue_create( sizeof(int), true );
	cn->conn_q_asyncfd = cf_queue_create( sizeof(int), true );

	cn->asyncfd = -1;
	cn->asyncwork_q = cf_queue_create(sizeof(cl_async_work *), true);
	
	cn->partition_generation = 0xFFFFFFFF;

	return(cn);
}

void
cl_cluster_node_release(cl_cluster_node *cn, const char *tag)
{
	// tag key:
	// C:  original alloc and insertion in cluster node list
	// PM: partition table, master
	// PP: partition table, prole
	// T:  transaction

#ifdef DEBUG_NODE_REF_COUNT
	cf_debug("node release: %s %s %p : %d", msg, cn->name, cn, cf_client_rc_count(cn));
#endif

	cl_async_work *aw;
	if (0 == cf_client_rc_release(cn)) {
		
		cf_vector_destroy(&cn->sockaddr_in_v);
		
		// Drain out the queue and close the FDs
		int rv;
		do {
			int	fd;
			rv = cf_queue_pop(cn->conn_q, &fd, CF_QUEUE_NOWAIT);
			if (rv == CF_QUEUE_OK)
				close(fd);
		} while (rv == CF_QUEUE_OK);
		do {
			int	fd;
			rv = cf_queue_pop(cn->conn_q_asyncfd, &fd, CF_QUEUE_NOWAIT);
			if (rv == CF_QUEUE_OK)
				close(fd);
		} while (rv == CF_QUEUE_OK);

		do {
			//When we reach this point, ideally there should not be any workitems.
			rv = cf_queue_pop(cn->asyncwork_q, &aw, CF_QUEUE_NOWAIT);
			if (rv == CF_QUEUE_OK) {
				free(aw);
			}
		} while (rv == CF_QUEUE_OK);

		//We want to delete all the workitems of this node
		if (g_cl_async_hashtab) {
			shash_reduce_delete(g_cl_async_hashtab, cl_del_node_asyncworkitems, cn);
		}

		//Now that all the workitems are released the FD can be closed
		if (cn->asyncfd != -1) {
			close(cn->asyncfd);
			cn->asyncfd = -1;
		}

		cf_queue_destroy(cn->conn_q);
		cf_queue_destroy(cn->conn_q_asyncfd);
		cf_queue_destroy(cn->asyncwork_q);

		cf_client_rc_free(cn);
	}
}

void
cl_cluster_node_reserve(cl_cluster_node *cn, const char *tag)
{
	// tag key:
	// C:  original alloc and insertion in cluster node list
	// PM: partition table, master
	// PP: partition table, prole
	// T:  transaction

#ifdef DEBUG_NODE_REF_COUNT
	cf_debug("node reserve: %s %s %p : %d", tag, cn->name, cn, cf_client_rc_count(cn));
#endif

	cf_client_rc_reserve(cn);
}

// Just send to a random node when you've tried and failed at a "good" node

cl_cluster_node *
cl_cluster_node_get_random(cl_cluster *asc)
{
	cl_cluster_node* cn = NULL;
	uint32_t i = 0;
	uint32_t node_v_sz = 0;

	do {
		// Get a node from the node list, round-robin.
		pthread_mutex_lock(&asc->LOCK);

		node_v_sz = cf_vector_size(&asc->node_v);

		if (node_v_sz == 0) {
#ifdef DEBUG
			cf_debug("cluster node get random: no nodes in this cluster");
#endif
			pthread_mutex_unlock(&asc->LOCK);
			return NULL;
		}

		if (++asc->last_node >= node_v_sz) {
			asc->last_node = 0;
		}

		cn = (cl_cluster_node*)cf_vector_pointer_get(&asc->node_v, asc->last_node);
		i++;

		// Right now cn should never be null, but leave this check and the loop
		// in case we ever add throttling...
		if (cn) {
			cl_cluster_node_reserve(cn, "T+");
#ifdef DEBUG
			cf_debug("random node chosen: %s", cn->name);
#endif
		}

		pthread_mutex_unlock(&asc->LOCK);

	} while (! cn && i < node_v_sz);

	return cn;
}

//
// Get a likely-healthy node for communication
// The digest is a good hint for an optimal node

cl_cluster_node *
cl_cluster_node_get(cl_cluster *asc, const char *ns, const cf_digest *d, bool write)
{
	// First, try to get one that matches this digest.
	cl_cluster_node* cn = cl_partition_table_get(asc, ns, cl_partition_getid(asc->n_partitions, d), write);

	if (cn) {
#ifdef DEBUG_VERBOSE		
		cf_debug("cluster node get: found match key %"PRIx64" node %s (%s):",
											*(uint64_t*)d, cn->name, write?"write":"read");
#endif		
		return cn;
	}

#ifdef DEBUG_VERBOSE	
	cf_debug("cluster node get: not found, try random key %"PRIx64, *(uint64_t *)d);
#endif	

	return cl_cluster_node_get_random(asc);
}

void 
cl_cluster_get_node_names(cl_cluster *asc, int *n_nodes, char **node_names)
{
	pthread_mutex_lock(&asc->LOCK);
	uint size = cf_vector_size(&asc->node_v);
	*n_nodes = size;

	if (size == 0) {
		*node_names = 0;
		pthread_mutex_unlock(&asc->LOCK);
		return;
	}

	*node_names = malloc(NODE_NAME_SIZE * size);
	if (*node_names == 0) {
		pthread_mutex_unlock(&asc->LOCK);
		return;
	}

	char *nptr = *node_names;
	for (uint i = 0; i < size; i++) {
		cl_cluster_node *cn = cf_vector_pointer_get(&asc->node_v, i);
		memcpy(nptr, cn->name, NODE_NAME_SIZE);
		nptr += NODE_NAME_SIZE;
	}
	pthread_mutex_unlock(&asc->LOCK);
}

cl_cluster_node *
cl_cluster_node_get_byname(cl_cluster *asc, const char *name)
{
	pthread_mutex_lock(&asc->LOCK);

	for (uint i = 0; i < cf_vector_size(&asc->node_v); i++) {
		cl_cluster_node *cn = cf_vector_pointer_get(&asc->node_v, i);
		if (strcmp(name, cn->name) == 0) {
			pthread_mutex_unlock(&asc->LOCK);
			return(cn);
		}
	}
	pthread_mutex_unlock(&asc->LOCK);
	return(0);
}

// Put the node back, whatever that means (release the reference count?)

void
cl_cluster_node_put(cl_cluster_node *cn)
{
	cl_cluster_node_release(cn, "T-");
}

int
cl_cluster_node_fd_create(cl_cluster_node *cn, bool nonblocking)
{
	int fd;
	// uint64_t starttime, endtime;
		
	// allocate a new file descriptor
	if (-1 == (fd = socket ( AF_INET, SOCK_STREAM, 0))) {
#ifdef DEBUG			
		cf_debug("could not allocate a socket, serious problem");
#endif			
		return(-1);
	}
#ifdef DEBUG_VERBOSE		
	else {
		cf_debug("new socket: fd %d node %s",fd, cn->name);
	}
#endif

	if (nonblocking == true) {
		int flags;
		if (-1 == (flags = fcntl(fd, F_GETFL, 0)))
			flags = 0;
		if (-1 == fcntl(fd, F_SETFL, flags | O_NONBLOCK)) {
			close(fd); 
			fd = -1;
			goto Done;
		}
	}
	
	int f = 1;
	setsockopt(fd, SOL_TCP, TCP_NODELAY, &f, sizeof(f));

	// loop over all known IP addresses for the server
	for (uint i=0;i< cf_vector_size(&cn->sockaddr_in_v);i++) {
		struct sockaddr_in *sa_in = cf_vector_getp(&cn->sockaddr_in_v, i);
	
		// uint64_t starttime = cf_getms();
		//dump_sockaddr_in("Connecting to ", sa_in);
		if (0 == connect(fd, (struct sockaddr *) sa_in, sizeof(struct sockaddr_in) ) )
		{
			// set nonblocking 
			int flags;
			if (-1 == (flags = fcntl(fd, F_GETFL, 0)))
				flags = 0;
			if (-1 == fcntl(fd, F_SETFL, flags | O_NONBLOCK)) {
				close(fd); fd = -1; goto Done;
			}
			
			goto Done;
		}
		else {
			//In case of a non-blocking connect, the connection may not be established immediately.
			//in-progress is a valid return value. We can do select later and use the socket.
			if (nonblocking && (errno == EINPROGRESS))
			{
				dump_sockaddr_in("Connecting to ", sa_in);
				cf_debug("Non-blocking connect returned EINPROGRESS as expected");
				goto Done;
			}
			// todo: remove this sockaddr from the list, or dun the node?
			if (errno == ECONNREFUSED) {
				cf_error("a host is refusing connections");
			}
			else {
				cf_error("connect fail: errno %d", errno);
			}
		}
	}
	close(fd);
	fd = -1;
		
Done:
	// endtime = cf_getms();
	//cf_debug("Time taken to open a new connection = %"PRIu64, (endtime - starttime));
	return(fd);
}

int
cl_cluster_node_fd_get(cl_cluster_node *cn, bool asyncfd, bool nbconnect)
{
	int fd;
#if ONEASYNCFD
	if (asyncfd == true) {
		if (cn->asyncfd == -1) {
			cn->asyncfd = cl_cluster_node_fd_create(cn, true);
		} 
		fd = cn->asyncfd;
		return fd;
	}
#endif
	cf_queue *q;
	if (asyncfd == true) {
		q = cn->conn_q_asyncfd;
	} else {
		q = cn->conn_q;
	}
	
	int rv = cf_queue_pop(q, &fd, CF_QUEUE_NOWAIT);
	if (rv == CF_QUEUE_OK)
		;
	else if (rv == CF_QUEUE_EMPTY) {
		if ((asyncfd == true) || (nbconnect == true)) {
			//Use a non-blocking socket for async client
			fd = cl_cluster_node_fd_create(cn, true);
		} else {
			fd = cl_cluster_node_fd_create(cn, false);
		}
	}
	else {
		fd = -1;
	}

	return fd;
}

void
cl_cluster_node_fd_put(cl_cluster_node *cn, int fd, bool asyncfd)
{
#if ONEASYNCFD
	return;	//FD does not get closed. It just lies around.
#endif
	cf_queue *q;
	if (asyncfd == true) {
		q = cn->conn_q_asyncfd;
		// Async queue is used by XDS. It can open lot of connections 
		// depending on batch-size. Dont worry about limiting the pool.
		cf_queue_push(q, &fd);
	} else {
		q = cn->conn_q;
		if (! cf_queue_push_limit(q, &fd, 300)) {
			close(fd);
		}
	}
}


//
// Parse a services string of the form:
// host:port;host:port
// Into the unique cf_vector of sockaddr_t

static void
cluster_services_parse(cl_cluster *asc, char *services, cf_vector *sockaddr_t_v) 
{
	cf_vector_define(host_str_v, sizeof(void *), 0);
	str_split(';',services, &host_str_v);
	// host_str_v is vector of addr:port
	for (uint i=0;i<cf_vector_size(&host_str_v);i++) {
		char *host_str = cf_vector_pointer_get(&host_str_v, i);
		cf_vector_define(host_port_v, sizeof(void *), 0);
		str_split(':', host_str, &host_port_v);
		if (cf_vector_size(&host_port_v) == 2) {
			char *host_s = cf_vector_pointer_get(&host_port_v,0);
			char *port_s = cf_vector_pointer_get(&host_port_v,1);
			int port = atoi(port_s);
			cl_lookup(asc, host_s, port, sockaddr_t_v);
		}
		cf_vector_destroy(&host_port_v);
	}
	cf_vector_destroy(&host_str_v);
}

static char*
trim(char *str)
{
	// Warning: This method walks on input string.
	char *begin = str;

	// Trim leading space
	while (isspace(*begin)) {
		begin++;
	}

	if(*begin == 0) {
		return begin;
	}

	// Trim trailing space.  Go to end first so whitespace is preserved
	// in the middle of the string.
	char *end = begin + strlen(begin) - 1;

	while (end > begin && isspace(*end)) {
		end--;
	}
	*(end + 1) = 0;
	return begin;
}

typedef struct ns_partition_map_s {
	char	ns[32];
	bool	owns[];
} ns_partition_map;

ns_partition_map*
ns_partition_map_get(cf_vector* p_maps_v, const char* ns, int n_partitions)
{
	uint32_t n_maps = cf_vector_size(p_maps_v);
	ns_partition_map* p_map;

	for (uint32_t i = 0; i < n_maps; i++) {
		p_map = (ns_partition_map*)cf_vector_pointer_get(p_maps_v, i);

		if (strcmp(p_map->ns, ns) == 0) {
			return p_map;
		}
	}

	size_t size = sizeof(ns_partition_map) + (n_partitions * sizeof(bool));

	p_map = (ns_partition_map*)malloc(size);

	if (! p_map) {
		cf_error("%s partition map allocation failed", ns);
		return NULL;
	}

	memset((void*)p_map, 0, size);
	strcpy(p_map->ns, ns);
	cf_vector_append(p_maps_v, (void*)&p_map);

	return p_map;
}

void
ns_partition_map_destroy(cf_vector* p_maps_v)
{
	uint32_t n_maps = cf_vector_size(p_maps_v);

	for (uint32_t i = 0; i < n_maps; i++) {
		free(cf_vector_pointer_get(p_maps_v, i));
	}
}

// TODO - should probably move base 64 stuff to cf_base.
const uint8_t CF_BASE64_DECODE_ARRAY[] = {
	    /*00*/ /*01*/ /*02*/ /*03*/ /*04*/ /*05*/ /*06*/ /*07*/   /*08*/ /*09*/ /*0A*/ /*0B*/ /*0C*/ /*0D*/ /*0E*/ /*0F*/
/*00*/	    0,     0,     0,     0,     0,     0,     0,     0,       0,     0,     0,     0,     0,     0,     0,     0,
/*10*/      0,     0,     0,     0,     0,     0,     0,     0,       0,     0,     0,     0,     0,     0,     0,     0,
/*20*/	    0,     0,     0,     0,     0,     0,     0,     0,       0,     0,     0,    62,     0,     0,     0,    63,
/*30*/	   52,    53,    54,    55,    56,    57,    58,    59,      60,    61,     0,     0,     0,     0,     0,     0,
/*40*/	    0,     0,     1,     2,     3,     4,     5,     6,       7,     8,     9,    10,    11,    12,    13,    14,
/*50*/	   15,    16,    17,    18,    19,    20,    21,    22,      23,    24,    25,     0,     0,     0,     0,     0,
/*60*/	    0,    26,    27,    28,    29,    30,    31,    32,      33,    34,    35,    36,    37,    38,    39,    40,
/*70*/	   41,    42,    43,    44,    45,    46,    47,    48,      49,    50,    51,     0,     0,     0,     0,     0,
/*80*/	    0,     0,     0,     0,     0,     0,     0,     0,       0,     0,     0,     0,     0,     0,     0,     0,
/*90*/	    0,     0,     0,     0,     0,     0,     0,     0,       0,     0,     0,     0,     0,     0,     0,     0,
/*A0*/	    0,     0,     0,     0,     0,     0,     0,     0,       0,     0,     0,     0,     0,     0,     0,     0,
/*B0*/	    0,     0,     0,     0,     0,     0,     0,     0,       0,     0,     0,     0,     0,     0,     0,     0,
/*C0*/	    0,     0,     0,     0,     0,     0,     0,     0,       0,     0,     0,     0,     0,     0,     0,     0,
/*D0*/	    0,     0,     0,     0,     0,     0,     0,     0,       0,     0,     0,     0,     0,     0,     0,     0,
/*E0*/	    0,     0,     0,     0,     0,     0,     0,     0,       0,     0,     0,     0,     0,     0,     0,     0,
/*F0*/	    0,     0,     0,     0,     0,     0,     0,     0,       0,     0,     0,     0,     0,     0,     0,     0
};

#define B64DA CF_BASE64_DECODE_ARRAY

void
b64_decode(const uint8_t* in, int len, uint8_t* out)
{
	int i = 0;
	int j = 0;

	while (i < len) {
		out[j + 0] = (B64DA[in[i + 0]] << 2) | (B64DA[in[i + 1]] >> 4);
		out[j + 1] = (B64DA[in[i + 1]] << 4) | (B64DA[in[i + 2]] >> 2);
		out[j + 2] = (B64DA[in[i + 2]] << 6) |  B64DA[in[i + 3]];

		i += 4;
		j += 3;
	}
}

void
ns_partition_map_set(ns_partition_map* p_map, const char* p_encoded_bitmap,
		int encoded_bitmap_len, int n_partitions)
{
	// First decode the base 64.
	// Size allows for padding - is actual size rounded up to multiple of 3.
	uint8_t* bitmap = (uint8_t*)alloca((encoded_bitmap_len / 4) * 3);

	b64_decode((const uint8_t*)p_encoded_bitmap, encoded_bitmap_len, bitmap);

	// Then expand the bitmap into our bool array.
	for (int i = 0; i < n_partitions; i++) {
		if ((bitmap[i >> 3] & (0x80 >> (i & 7))) != 0) {
			p_map->owns[i] = true;
		}
	}
}

void
parse_replicas_map(char* list, int n_partitions, cf_vector* p_maps_v)
{
	// Format: <namespace1>:<base 64 encoded bitmap>;<namespace2>:<base 64 encoded bitmap>; ...
	// Warning: this method walks on partitions string argument.
	char* p = list;

	while (*p) {
		// Store pointer to namespace string.
		char* list_ns = p;

		// Loop until : and set it to null.
		while (*p && *p != ':') {
			p++;
		}

		if (*p == ':') {
			*p++ = 0;
		}
		else {
			cf_warn("ns %s has no encoded bitmap", list_ns);
			break;
		}

		// Store pointer to base 64 encoded bitmap.
		char* p_encoded_bitmap = p;

		// Loop until ; or null-terminator.
		while (*p && *p != ';') {
			p++;
		}

		// Calculate length of encoded bitmap.
		int encoded_bitmap_len = (int)(p - p_encoded_bitmap);

		// If we found ; set it to null and advance read pointer.
		if (*p == ';') {
			*p++ = 0;
		}

		// Sanity check namespace.
		char* ns = trim(list_ns);
		size_t len = strlen(ns);

		if (len == 0 || len > 31) {
			cf_warn("invalid partition namespace %s", ns);
			continue;
		}

		// Sanity check encoded bitmap.
		// TODO - annoying to calculate these every time...
		int bitmap_size = (n_partitions + 7) / 8;
		int expected_encoded_len = ((bitmap_size + 2) / 3) * 4;

		if (expected_encoded_len != encoded_bitmap_len) {
			cf_warn("invalid partition bitmap %s", p_encoded_bitmap);
			continue;
		}

		// Get or create map for specified maps vector and namespace.
		ns_partition_map* p_map =
				ns_partition_map_get(p_maps_v, ns, n_partitions);

		// Fill out the map's partition ownership information.
		if (p_map) {
			ns_partition_map_set(p_map, p_encoded_bitmap, encoded_bitmap_len,
					n_partitions);
		}
	}
}

// Equivalent to node_info_req_parse_replicas() in libevent client.
void
cl_cluster_node_parse_replicas(cl_cluster* asc, cl_cluster_node* cn, char* rbuf)
{
	cf_vector_define(master_maps_v, sizeof(ns_partition_map*), 0);
	cf_vector_define(prole_maps_v, sizeof(ns_partition_map*), 0);

	// Returned list format is name1\tvalue1\nname2\tvalue2\n...
	cf_vector_define(lines_v, sizeof(void*), 0);
	str_split('\n', rbuf, &lines_v);

	for (uint32_t j = 0; j < cf_vector_size(&lines_v); j++) {
		char* line = (char*)cf_vector_pointer_get(&lines_v, j);

		cf_vector_define(pair_v, sizeof(void*), 0);
		str_split('\t', line, &pair_v);

		if (cf_vector_size(&pair_v) != 2) {
			// Will happen if a requested field is returned empty.
			cf_vector_destroy(&pair_v);
			continue;
		}

		char* name = (char*)cf_vector_pointer_get(&pair_v, 0);
		char* value = (char*)cf_vector_pointer_get(&pair_v, 1);

		if (strcmp(name, "partition-generation") == 0) {
			int gen = atoi(value);

			// Update to the new partition generation.
			cf_atomic_int_set(&cn->partition_generation, (cf_atomic_int_t)gen);

			cf_debug("node %s got partition generation %d", cn->name, gen);
		}
		else if (strcmp(name, "replicas-master") == 0) {
			// Parse the master replicas.
			parse_replicas_map(value, asc->n_partitions, &master_maps_v);
		}
		else if (strcmp(name, "replicas-prole") == 0) {
			// Parse the prole replicas.
			parse_replicas_map(value, asc->n_partitions, &prole_maps_v);
		}
		else {
			cf_warn("node %s info replicas did not request %s", cn->name, name);
		}

		cf_vector_destroy(&pair_v);
	}

	cf_vector_destroy(&lines_v);

	// Note - p_prole_map will not be null in the single node case. We also
	// assume it's impossible for a node to have no masters.

	uint32_t n_master_maps = cf_vector_size(&master_maps_v);

	for (uint32_t i = 0; i < n_master_maps; i++) {
		ns_partition_map* p_master_map = (ns_partition_map*)
				cf_vector_pointer_get(&master_maps_v, i);
		ns_partition_map* p_prole_map = ns_partition_map_get(&prole_maps_v,
				p_master_map->ns, asc->n_partitions);

		cl_partition_table_update(asc, cn, p_master_map->ns, p_master_map->owns,
				p_prole_map->owns);
	}

	ns_partition_map_destroy(&master_maps_v);
	ns_partition_map_destroy(&prole_maps_v);

	cf_vector_destroy(&master_maps_v);
	cf_vector_destroy(&prole_maps_v);
}

//
// Ping a given node. Make sure it's node name is still its node name.
// See if there's been a re-vote of the cluster.
// Grab the services and insert them into the services vector.
// Try all known addresses of this node, too

static void
cluster_ping_node(cl_cluster *asc, cl_cluster_node *cn, cf_vector *services_v)
{
	bool update_partitions = false;

#ifdef DEBUG	
	cf_debug("cluster ping node: %s",cn->name);
#endif	
	
	// for all elements in the sockaddr_in list - ping and add hosts, if not
	// already extant
	for (uint i=0;i<cf_vector_size(&cn->sockaddr_in_v);i++) {
		struct sockaddr_in *sa_in = cf_vector_getp(&cn->sockaddr_in_v, i);
		
		char *values = 0;
		if (0 != citrusleaf_info_host_limit(sa_in, "node\npartition-generation\nservices\n",
				&values, asc->info_timeout, false, 10000, /* check bounds */ true)) {
			// todo: this address is no longer right for this node, update the node's list
			// and if there's no addresses left, dun node
			cf_debug("Info request failed for %s", cn->name);
			continue;
		}

		// reminder: returned list is name1\tvalue1\nname2\tvalue2\n
		cf_vector_define(lines_v, sizeof(void *), 0);
		str_split('\n',values,&lines_v);
		for (uint j=0;j<cf_vector_size(&lines_v);j++) {
			char *line = cf_vector_pointer_get(&lines_v, j);
			cf_vector_define(pair_v, sizeof(void *), 0);
			str_split('\t',line, &pair_v);
			
			if (cf_vector_size(&pair_v) == 2) {
				char *name = cf_vector_pointer_get(&pair_v,0);
				char *value = cf_vector_pointer_get(&pair_v,1);
				
				if ( strcmp(name, "node") == 0) {
					
					if (strcmp(value, cn->name) != 0) {
						// node name has changed. Dun is easy, would be better to remove the address
						// from the list of addresses for this node, and only dun if there
						// are no addresses left
						cf_info("node name has changed!");
					}
				}
				else if (strcmp(name, "partition-generation") == 0) {
					if (cn->partition_generation != (uint32_t) atoi(value)) {
						update_partitions = true;				
						cn->partition_generation = atoi(value);
					}
				}
				else if (strcmp(name, "services")==0) {
					cluster_services_parse(asc, value, services_v);
				}
			}
			cf_vector_destroy(&pair_v);
		}
		cf_vector_destroy(&lines_v);
		free(values);
	}

	if (update_partitions) {
		// cf_debug("node %s: partitions have changed, need update", cn->name);

		for (uint i=0;i<cf_vector_size(&cn->sockaddr_in_v);i++) {
			struct sockaddr_in *sa_in = cf_vector_getp(&cn->sockaddr_in_v, i);
			char *values = 0;

			if (0 != citrusleaf_info_host_limit(sa_in, "partition-generation\nreplicas-master\nreplicas-prole\n",
					&values, asc->info_timeout, false, 2000000, /*check bounds */ true)) {
				continue;
			}

			if (values) {
				cl_cluster_node_parse_replicas(asc, cn, values);
				free(values);
			}

			break;
		}
	}
}



//
// Ping this address, get its node name, see if it's unique
// side effect: causes creation of node if new
static void
cluster_ping_address(cl_cluster *asc, struct sockaddr_in *sa_in)
{
		
	char *values = 0;
	if (0 != citrusleaf_info_host(sa_in, "node", &values, asc->info_timeout, false, /* check bounds */ true)){
	 return;
	}
	
	char *value = 0;
	if (0 != citrusleaf_info_parse_single(values, &value)) {
		free(values);
		return;
	}
		
	// if new nodename, add to cluster
	cl_cluster_node *cn = cl_cluster_node_get_byname(asc, value);
	if (!cn) {
		if (cf_debug_enabled()) {
			cf_debug("%s node unknown, creating new", value);
			dump_sockaddr_in("New node is ",sa_in);
		}
		cl_cluster_node *node = cl_cluster_node_create(value, sa_in);

		// Appends must be locked regardless of only being called from tend thread, because
		// other threads reads need to wait on their locks for the append to complete.
		if (node) {
			pthread_mutex_lock(&asc->LOCK);
			cf_vector_pointer_append(&asc->node_v, node);
			pthread_mutex_unlock(&asc->LOCK);
		}
	}
	// if not new, add address to node
	else {
		cf_vector_append_unique(&cn->sockaddr_in_v, sa_in);
	}
	
	free(values);
		
}


// number of partitions for a cluster never changes, but you do have to get it once

void
cluster_get_n_partitions( cl_cluster *asc, cf_vector *sockaddr_in_v )
{
	for (uint i=0;i<cf_vector_size(sockaddr_in_v);i++) {
		
		// check if someone found the value
		if (asc->n_partitions != 0)	return;
		
		struct sockaddr_in *sa_in = cf_vector_getp(sockaddr_in_v, i);

		char *values = 0;
		if (0 != citrusleaf_info_host(sa_in, "partitions", &values, asc->info_timeout, false, /*check bounds*/ true)) {
			continue;
		}
		
		char *value = 0;
		if (0 != citrusleaf_info_parse_single(values, &value)) {
			free(values);
			continue;
		}
	
		asc->n_partitions = atoi(value);
		
		free(values);
	}
}

static void
cluster_tend(cl_cluster *asc)
{
	pthread_mutex_lock(&asc->LOCK);
	// If thre is already a tending process running for this cluster, there is no
	// point in running one more immediately. Moreover, there are assumptions in
	// the code that only one tender function is running at a time. So, abort.
	if ((asc->state & CLS_FREED) || (asc->state & CLS_TENDER_RUNNING)) {
		cf_debug("Not running cluster tend as the state of the cluster is 0x%x", asc->state);
		pthread_mutex_unlock(&asc->LOCK);
		return;
	}
	asc->state |= CLS_TENDER_RUNNING;
	pthread_mutex_unlock(&asc->LOCK);

	// For all registered hosts --- resolve into the cluster's sockaddr_in list
	uint n_hosts = cf_vector_size(&asc->host_str_v);
	cf_vector_define(sockaddr_in_v, sizeof( struct sockaddr_in ), 0);
	for (uint i=0;i<n_hosts;i++) {
		
        if (cf_debug_enabled()) {
		    char *host = cf_vector_pointer_get(&asc->host_str_v, i);
		    int port = cf_vector_integer_get(&asc->host_port_v, i);
    		cf_debug("lookup hosts: %s:%d",host,port);
        }		
		cl_lookup(asc, cf_vector_pointer_get(&asc->host_str_v, i), 
					cf_vector_integer_get(&asc->host_port_v, i),
					&sockaddr_in_v);
	}
	// Compare this list against the current list of addresses of known nodes.
	// anything new, ping and get its info
	for (uint i=0;i<cf_vector_size(&sockaddr_in_v);i++) {
		struct sockaddr_in *sin = cf_vector_getp(&sockaddr_in_v,i);
		if (0 == cl_cluster_node_get_byaddr(asc, sin)) {
			cluster_ping_address(asc, sin);
		}
	}
	
	if (asc->n_partitions == 0)
		cluster_get_n_partitions(asc, &sockaddr_in_v);
	
	// vector will now contain an accumulation of the service addresses
	cf_vector_reset(&sockaddr_in_v);

	// Now, ping known nodes to see if there's an update.
	for (uint i = 0; i < cf_vector_size(&asc->node_v); i++) {
		cl_cluster_node *cn = cf_vector_pointer_get(&asc->node_v, i);

		// Check if this node is in the partition map. (But skip the first time,
		// since the node can't be in the map yet.)
		if (cn->intervals_absent == 0 || cl_partition_table_is_node_present(asc, cn)) {
			cn->intervals_absent = 1;
		}
		else if (cn->intervals_absent++ > MAX_INTERVALS_ABSENT) {
			// This node has been out of the map for MAX_INTERVALS_ABSENT laps.
			cf_debug("DELETE SUPERSEDED NODE %s %p i %d", cn->name, cn, i);
			pthread_mutex_lock(&asc->LOCK);
			cf_vector_delete(&asc->node_v, i);
			pthread_mutex_unlock(&asc->LOCK);
			i--;
			cl_cluster_node_release(cn, "C-");
			continue;
		}

		cluster_ping_node(asc, cn, &sockaddr_in_v);
		for (uint j=0;j<cf_vector_size(&cn->sockaddr_in_v);j++) {
			struct sockaddr_in *sa_in = cf_vector_getp(&cn->sockaddr_in_v,j);
			cf_vector_append_unique(&sockaddr_in_v, sa_in);
		}
	}
	
	// cf_debug("CLUSTER TEND: sockaddr_in_v len %d %s",cf_vector_size(&sockaddr_in_v), asc->found_all?"foundall":"notfound" );
	
	// Compare all services with known nodes - explore if new
	if (asc->follow == true) {
		int n_new = 0;
		for (uint i=0;i<cf_vector_size(&sockaddr_in_v);i++) {
			struct sockaddr_in *sin = cf_vector_getp(&sockaddr_in_v, i);
			if (cf_debug_enabled()) {
				dump_sockaddr_in("testing service address",sin);
			}		
			if (0 == cl_cluster_node_get_byaddr(asc, sin)) {
				if (cf_debug_enabled()) {
					dump_sockaddr_in("pinging",sin);
				}		
				cluster_ping_address(asc, sin);
				n_new++;
			}
		}
		if (n_new == 0)	{
			//cf_debug("CLUSTER TEND: *** FOUND ALL ***");
			asc->found_all = true;
		}
		//cf_debug("CLUSTER TEND: n_new is %d foundall %d",n_new,asc->found_all);
	}

	cf_vector_destroy(&sockaddr_in_v);

#ifdef DEBUG	
	dump_cluster(asc);
#endif	

	pthread_mutex_lock(&asc->LOCK);
	asc->state &= ~CLS_TENDER_RUNNING;
	pthread_mutex_unlock(&asc->LOCK);

	return;
}

void
citrusleaf_cluster_change_info_timeout(cl_cluster *asc, int msecs)
{
    asc->info_timeout = msecs;
}

void 
citrusleaf_cluster_change_tend_speed(cl_cluster *asc, int secs)
{
	asc->tend_speed = secs;
}

void
citrusleaf_change_tend_speed(int secs)
{
	g_clust_tend_speed = secs;
}

void 
citrusleaf_cluster_use_nbconnect(struct cl_cluster_s *asc)
{
	asc->nbconnect = true;
}

void
citrusleaf_sleep_for_tender(cl_cluster *asc)
{
	if (asc->tend_speed  > 0)
		sleep(asc->tend_speed);
	else
		sleep(g_clust_tend_speed);
}

//
// This rolls through every cluster, tries to add and delete nodes that might
// have gone bad
//
static void *
cluster_tender_fn(void *gcc_is_ass)
{
	uint64_t cnt = 1;
	do {
		sleep(1);
		
		// if tend speed is non zero tend at that speed
		// otherwise at default speed
		cf_ll_element *e = cf_ll_get_head(&cluster_ll);
		while (e) {
			int speed = ((cl_cluster *)e)->tend_speed;
			if (speed) {
				if ((cnt % speed) == 0) {
					cluster_tend( (cl_cluster *) e);
				}
			} else {
				if ((cnt % g_clust_tend_speed) == 0) {
					cluster_tend( (cl_cluster *) e);
				}
			}
			e = cf_ll_get_next(e);
		}
		cnt++;
	} while (1);
	return(0);
}


//
// Initialize the thread that keeps track of the cluster
//
int citrusleaf_cluster_init()
{
    if (g_clust_initialized)    return(0);
    
    	// No destruction function is used. 
	// NOTE: A destruction function should not be used because elements
	// in this list are used even after they are removed from the list.
	// Refer to the function citrusleaf_cluster_destroy() for more info.
	cf_ll_init(&cluster_ll, 0, false);
	
    g_clust_initialized = 1;
    
   	g_clust_tend_speed = 1;
	pthread_create( &tender_thr, 0, cluster_tender_fn, 0);
	return(0);	
}


