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
#include <arpa/inet.h> // inet_ntop
#include <signal.h>
#include <ctype.h>

#include <netdb.h> //gethostbyname_r
#include <netinet/tcp.h>

#include "citrusleaf/citrusleaf.h"
#include "citrusleaf/citrusleaf-internal.h"
#include "citrusleaf/cl_cluster.h"
#include "citrusleaf/proto.h"
#include "citrusleaf/cl_request.h"
#include "citrusleaf/cf_socket.h"

//#define DEBUG 1
//#define DEBUG_VERBOSE 1

// Forward references
static void cluster_tend( cl_cluster *asc); 

#include <time.h>
static inline void print_ms(char *pre)
{
	cf_debug("%s %"PRIu64, pre, cf_getms());
}

int g_clust_initialized = 0;
static int g_clust_tend_speed = 1;
extern int g_cl_turn_debug_on;
extern int g_init_pid;

//
// Debug function. Should be elsewhere.
//

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
cf_ll		cluster_ll;
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
	// Default is 0 so the cluster uses global tend period.
	// For the cluster user has to specifically set the own
	// value
	asc->tend_speed = 0;
	
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
		cl_cluster_node_release(cn);
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
	int portp, found = 0;
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
	cl_addrmap *oldmap=NULL, *newmap=NULL;


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
		cl_addrmap *newmap = (cl_addrmap *) malloc(sizeof(struct cl_addrmap));
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
	int i;
	cl_cluster_node *cn = cf_client_rc_alloc( sizeof(cl_cluster_node ) );
	if (!cn)	return(0);
	
	strcpy(cn->name, name);

	cn->dun_score = 0;
	cn->dunned = false;
	
	cf_vector_init(&cn->sockaddr_in_v, sizeof( struct sockaddr_in ), 5, 0);
	cf_vector_append(&cn->sockaddr_in_v, sa_in);
	
	cn->conn_q = cf_queue_create( sizeof(int), true );
	cn->conn_q_asyncfd = cf_queue_create( sizeof(int), true );

	cn->asyncfd = -1;
	cn->asyncwork_q = cf_queue_create(sizeof(cl_async_work *), true);
	
	cn->partition_generation = 0xFFFFFFFF;
	
	pthread_mutex_init(&cn->LOCK, 0);
	
	return(cn);
}

void
cl_cluster_node_release(cl_cluster_node *cn)
{
	int i;
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
		pthread_mutex_destroy(&cn->LOCK);
		cf_client_rc_free(cn);
	}
	
}

// Just send to a random node when you've tried and failed at a "good" node

cl_cluster_node *
cl_cluster_node_get_random(cl_cluster *asc)
{
	cl_cluster_node *cn;
	
	uint node_v_sz = cf_vector_size(&asc->node_v);
	if (node_v_sz == 0) {
#ifdef DEBUG		
		cf_debug("cluster node get: no nodes in this cluster");
#endif		
		return(0);
	}

	uint i=0;
	do {
		asc->last_node ++;
		if (asc->last_node >= node_v_sz)	asc->last_node = 0;
		int node_i = asc->last_node;

#ifdef DEBUG_VERBOSE		
		cf_debug("cluster node get: vsize %d choosing %d",
			cf_vector_size(&asc->node_v), node_i);
#endif		

		cn = cf_vector_pointer_get(&asc->node_v, node_i);
		i++;
        
	} while( (cn->dunned == true) && (i <= node_v_sz) );

#ifdef DEBUG    
    cf_debug("   random node chosen: %s",cn->name);
#endif    
    
	// grab a reservation
	cf_client_rc_reserve(cn);

	return(cn);
}
//
// Get a likely-healthy node for communication
// The digest is a good hint for an optimal node

cl_cluster_node *
cl_cluster_node_get(cl_cluster *asc, const char *ns, const cf_digest *d, bool write)
{
	cl_cluster_node	*cn;	
	pthread_mutex_lock(&asc->LOCK);

	// first, try to get one that matches this digest
	cn = cl_partition_table_get(asc, (char *)ns, cl_partition_getid(asc->n_partitions, (cf_digest *) d) , write);
	if (cn && (cn->dunned == false)) {
#ifdef DEBUG_VERBOSE		
		cf_debug("cluster node get: found match key %"PRIx64" node %s (%s):",
											*(uint64_t*)d, cn->name, write?"write":"read");
#endif		
		cf_client_rc_reserve(cn);
		pthread_mutex_unlock(&asc->LOCK);
		return(cn);
	}
#ifdef DEBUG_VERBOSE	
	cf_debug("cluster node get: not found, try random key %"PRIx64, *(uint64_t *)d);
#endif	
	
	cn = cl_cluster_node_get_random(asc);
	pthread_mutex_unlock(&asc->LOCK);
	return(cn);
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
cl_cluster_node_get_byname(cl_cluster *asc, char *name)
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

int
cl_cluster_get_node_names_byhostportlist(cl_cluster *asc, char *list_nodes, int *n_nodes, char **node_names)
{
	//get vector containing host:port
	cf_vector_define(host_str_v, sizeof(void *), 0);// host_str_v is vector of addr:port
	str_split(';', list_nodes, &host_str_v);

	uint size = cf_vector_size(&host_str_v);
	*n_nodes = size;
	*node_names = malloc(NODE_NAME_SIZE * size);
	if (*node_names == 0) {
		return(0);
	}

	char *nptr = *node_names;
	for (uint i=0; i<size; i++) {
		char *host_str = cf_vector_pointer_get(&host_str_v, i);
		cf_vector_define(host_port_v, sizeof(void *), 0);
		str_split(':', host_str, &host_port_v);
		if (cf_vector_size(&host_port_v) == 2) {
			char *host_s = cf_vector_pointer_get(&host_port_v, 0);
			char *port_s = cf_vector_pointer_get(&host_port_v, 1);
			uint port = atoi(port_s);
			cf_debug("host-port:%s:%d, ", host_s, port);
			char *info_name;
			char *value;
			if (0 == citrusleaf_info(host_s, port, "node", &info_name, 3000)) {
				citrusleaf_info_parse_single(info_name, &value);
				cf_debug("node-name:%s\n", value);
				memcpy(nptr, value, NODE_NAME_SIZE);
				nptr += (NODE_NAME_SIZE);
			} else {
				cf_debug("%s:%u is not accessible or timed out. \n", host_s, port);
				return (-2);
			}
			free(info_name);
		} else {
				cf_debug("Command line input format error for option l\n");
				return (-1);
		}
		cf_vector_destroy(&host_port_v);
	}
	cf_vector_destroy(&host_str_v);
	return(0);
}

// Put the node back, whatever that means (release the reference count?)

void
cl_cluster_node_put(cl_cluster_node *cn)
{
	cl_cluster_node_release(cn);
}

//
// Todo: will dunned hosts be in the host list with a flag, or in a different list?
//

void
cl_cluster_node_dun(cl_cluster_node *cn, int32_t score)
{
	if (cn->dunned) {
		return;
	}

#ifdef DEBUG_VERBOSE
	if (score > 1) {
		cf_debug("node %s health decreased %d", cn->name, score);
	}
#endif

	if (cf_atomic32_add(&cn->dun_score, score) > NODE_DUN_THRESHOLD) {
		cn->dunned = true;

#ifdef DEBUG
		cf_debug("dunning node %s", cn->name);
#endif
	}
}

void
cl_cluster_node_ok(cl_cluster_node *cn)
{
	if (! cn->dunned) {
		cf_atomic32_set(&cn->dun_score, 0);
	}
}

int
cl_cluster_node_fd_create(cl_cluster_node *cn, bool nonblocking)
{
	int fd;
	uint64_t starttime, endtime;
		
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
	
		uint64_t starttime = cf_getms();
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
#if XDS //Hack for XDS
				cf_print_sockaddr_in("Connecting to ", sa_in);
				cf_debug("Non-blocking connect returned EINPROGRESS as expected");
#endif
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
	endtime = cf_getms();
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

//
// Process new partitions information
// namespace:part_id;namespace:part_id
//
// Update the cluster with the new information

static void
cluster_partitions_process(cl_cluster *asc, cl_cluster_node *cn, char *partitions, bool write) 
{
	// Partitions format: <namespace1>:<partition id1>;<namespace2>:<partition id2>; ...
	// Warning: This method walks on partitions string argument.
	char *p = partitions;

	while (*p)
	{
		char *partition_str = p;
		// loop till split and set it to null
		while ((*p) && (*p != ';')) {
			p++;
		}
		if (*p == ';'){
			*p = 0;
			p++;
		}
		cf_vector_define(partition_v, sizeof(void *), 0);
		str_split(':', partition_str, &partition_v);

		unsigned int vsize = cf_vector_size(&partition_v);
		if (vsize == 2) {
			char *namespace_s = cf_vector_pointer_get(&partition_v,0);
			char *partid_s = cf_vector_pointer_get(&partition_v,1);

			// it's coming over the wire, so validate it
			char *ns = trim(namespace_s);
			int len = strlen(ns);

			if (len == 0 || len > 31) {
				cf_warn("Invalid partition namespace %s", ns);
				goto Next;
			}

			int partid = atoi(partid_s);

			if (partid < 0 || partid >= (int)asc->n_partitions) {
				cf_warn("Invalid partition id %s. max=%u", partid_s, asc->n_partitions);
				goto Next;
			}
				
			pthread_mutex_lock(&asc->LOCK);
			cl_partition_table_set(asc, cn, ns, partid, write);
			pthread_mutex_unlock(&asc->LOCK);
		}
		else {
			cf_warn("Invalid partition vector size %u. element=%s", vsize, partition_str);
		}
Next:
		cf_vector_destroy(&partition_v);
	}
}

//
// Ping a given node. Make sure it's node name is still its node name.
// See if there's been a re-vote of the cluster.
// Grab the services and insert them into the services vector.
// Try all known addresses of this node, too
//
static void
cluster_ping_node(cl_cluster *asc, cl_cluster_node *cn, cf_vector *services_v)
{
	// cf_debug("cluster ping node: %s",cn->name);

	bool update_partitions = false;

	// for all elements in the sockaddr_in list - ping and add hosts.
	for (uint i=0;i<cf_vector_size(&cn->sockaddr_in_v);i++) {
		struct sockaddr_in *sa_in = cf_vector_getp(&cn->sockaddr_in_v, i);
		cl_node_info node_info;

		if (cl_get_node_info(cn->name, sa_in, &node_info) != 0) {
			// todo: this address is no longer right for this node, update the node's list
			// and if there's no addresses left, dun node
			cf_debug("Info request failed for %s", cn->name);
			cl_cluster_node_dun(cn, NODE_DUN_INFO_ERR);
			continue;
		}

		if (node_info.dun) {
			cl_cluster_node_dun(cn, NODE_DUN_INFO_ERR);
			cl_node_info_free(&node_info);
			break;
		}

		cl_cluster_node_ok(cn);

		if (strcmp(node_info.node_name, cn->name) != 0) {
			// node name has changed. Dun is easy, would be better to remove the address
			// from the list of addresses for this node, and only dun if there
			// are no addresses left
			cf_info("node name has changed! old='%s' new='%s'", cn->name, node_info.node_name);
			cl_cluster_node_dun(cn, NODE_DUN_NAME_CHG);
		}

		if (cn->partition_generation != node_info.partition_generation) {
			update_partitions = true;
			cn->partition_generation = node_info.partition_generation;
		}

		cluster_services_parse(asc, node_info.services, services_v);
		cl_node_info_free(&node_info);
		break;
	}
	
	if (update_partitions) {
		// cf_debug("node %s: partitions have changed, need update", cn->name);

		// remove all current values, then add up-to-date values
		pthread_mutex_lock(&asc->LOCK);
		cl_partition_table_remove_node(asc, cn);
		pthread_mutex_unlock(&asc->LOCK);

		for (uint i=0;i<cf_vector_size(&cn->sockaddr_in_v);i++) {
			struct sockaddr_in *sa_in = cf_vector_getp(&cn->sockaddr_in_v, i);
			cl_replicas replicas;

			if (cl_get_replicas(cn->name, sa_in, &replicas) != 0) {
				continue;
			}

			if (replicas.write_replicas) {
				cluster_partitions_process(asc, cn, replicas.write_replicas, true);
			}

			if (replicas.read_replicas) {
				cluster_partitions_process(asc, cn, replicas.read_replicas, false);
			}

			cl_replicas_free(&replicas);
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
	char node_name[NODE_NAME_SIZE];

	if (cl_get_node_name(sa_in, node_name) != 0) {
		return;
	}
		
	// if new nodename, add to cluster
	cl_cluster_node *cn = cl_cluster_node_get_byname(asc, node_name);
	if (!cn) {
		/*
		if (cf_debug_enabled()) {
			cf_debug("%s node unknown, creating new", node_name);
			dump_sockaddr_in("New node is ",sa_in);
		}
		*/
		cl_cluster_node *node = cl_cluster_node_create(node_name, sa_in);

		// Appends must be locked regardless of only being called from tend thread, because
		// other threads reads need to wait on their locks for the append to complete.
		pthread_mutex_lock(&asc->LOCK);
		cf_vector_pointer_append(&asc->node_v, node);
		pthread_mutex_unlock(&asc->LOCK);
	}
	// if not new, add address to node
	else {
		cf_vector_append_unique(&cn->sockaddr_in_v, sa_in);
	}
}

//
// number of partitions for a cluster never changes, but you do have to get it once
//
void
cluster_get_n_partitions( cl_cluster *asc, cf_vector *sockaddr_in_v )
{
	// check if someone found the value
	if (asc->n_partitions != 0)
		return;

	for (uint i=0;i<cf_vector_size(sockaddr_in_v);i++) {
		struct sockaddr_in *sa_in = cf_vector_getp(sockaddr_in_v, i);
		int n_partitions = 0;

		if (cl_get_n_partitions(sa_in, &n_partitions) != 0) {
			continue;
		}
		asc->n_partitions = n_partitions;
		break;
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

	// Start off by removing dunned hosts
	for (uint i=0;i<cf_vector_size(&asc->node_v);i++) {
		cl_cluster_node *cn = cf_vector_pointer_get(&asc->node_v, i);
		if (!cn)	continue;
		if (cn->dunned) {
			cf_debug(" DELETE DUNNED NODE %s %p i %d",cn->name,cn,i);
			cf_vector_delete(&asc->node_v, i);
			i--;
			cl_partition_table_remove_node(asc, cn);
			cl_cluster_node_release(cn);
		}
	}
	pthread_mutex_unlock(&asc->LOCK);

	// For all registered hosts --- resolve into the cluster's sockaddr_in list
	uint n_hosts = cf_vector_size(&asc->host_str_v);
	cf_vector_define(sockaddr_in_v, sizeof( struct sockaddr_in ), 0);
	for (uint i=0;i<n_hosts;i++) {
		/*
        if (cf_debug_enabled()) {
		    char *host = cf_vector_pointer_get(&asc->host_str_v, i);
		    int port = cf_vector_integer_get(&asc->host_port_v, i);
    		cf_debug("lookup hosts: %s:%d",host,port);
        }
        */
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

	// Now, ping known nodes to see if there's an update
	// No need to lock node list because nodes are only added/removed in this thread.
	for (uint i = 0; i < cf_vector_size(&asc->node_v); i++) {
		cl_cluster_node *cn = cf_vector_pointer_get(&asc->node_v, i);
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
			/*
			if (cf_debug_enabled()) {
				dump_sockaddr_in("testing service address",sin);
			}
			*/
			if (0 == cl_cluster_node_get_byaddr(asc, sin)) {
				if (cf_debug_enabled()) {
					cf_print_sockaddr_in("pinging",sin);
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
citrusleaf_cluster_change_tend_speed(cl_cluster *asc, int secs)
{
	asc->tend_speed = secs;
}

void 
citrusleaf_cluster_use_nbconnect(struct cl_cluster_s *asc)
{
	asc->nbconnect = true;
}

void
citrusleaf_change_tend_speed(int secs)
{
	g_clust_tend_speed = secs;
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
		
		// if tend period is non zero tend at that period
		// otherwise at default period
		cf_ll_element *e = cf_ll_get_head(&cluster_ll);
		while (e) {
			int period = ((cl_cluster *)e)->tend_speed;
			if (period) {
				if ((cnt % period) == 0) {
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
    if (g_clust_initialized) {
    	return(0);
	}
    
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
