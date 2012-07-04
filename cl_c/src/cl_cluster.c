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

#include <netdb.h> //gethostbyname_r

#include "citrusleaf/citrusleaf.h"
#include "citrusleaf/citrusleaf-internal.h"
#include "citrusleaf/cl_cluster.h"
#include "citrusleaf/proto.h"

// #define INFO_TIMEOUT_MS 100
#define INFO_TIMEOUT_MS 300
//#define DEBUG 1
// #define DEBUG_VERBOSE 1

// Forward references
static void cluster_tend( cl_cluster *asc); 

#include <time.h>
static inline void print_ms(char *pre)
{

	fprintf(stderr,"%s %"PRIu64"\n",pre,cf_getms());
}

int g_clust_initialized = 0;
static int g_clust_tend_speed = 1;
extern int g_cl_turn_debug_on;
extern int g_init_pid;

//
// Debug function. Should be elsewhere.
//

static void
dump_sockaddr_in(char *prefix, struct sockaddr_in *sa_in)
{
	char str[INET_ADDRSTRLEN];
	inet_ntop(AF_INET, &(sa_in->sin_addr), str, INET_ADDRSTRLEN);	
	fprintf(stderr,"%s %s:%d\n",prefix,str,(int)ntohs(sa_in->sin_port));
}

static void
dump_cluster(cl_cluster *asc)
{
	pthread_mutex_lock(&asc->LOCK);
	
	fprintf(stderr, "registered hosts:\n");
	for (uint i=0;i<cf_vector_size(&asc->host_str_v);i++) {
		char *host_s = cf_vector_pointer_get(&asc->host_str_v,i);
		int   port = cf_vector_integer_get(&asc->host_port_v,i);
		fprintf(stderr, " host %d: %s:%d\n",i,host_s,port);
	}
	
	fprintf(stderr, "nodes: %u\n",cf_vector_size(&asc->node_v));
	for (uint i=0;i<cf_vector_size(&asc->node_v);i++) {
		cl_cluster_node *cn = cf_vector_pointer_get(&asc->node_v, i);
		struct sockaddr_in *sa_in = cf_vector_getp(&cn->sockaddr_in_v, 0);
		char str[INET_ADDRSTRLEN];
		inet_ntop(AF_INET, &(sa_in->sin_addr), str, INET_ADDRSTRLEN);
		fprintf(stderr, "%d %s : %s:%d (%d conns) (%d async conns)\n",i,cn->name,str,
			(int)ntohs(sa_in->sin_port),cf_queue_sz(cn->conn_q),
			cf_queue_sz(cn->conn_q_asyncfd));
	}
	
	fprintf(stderr, "partitions: %d\n",asc->n_partitions);
	
	pthread_mutex_unlock(&asc->LOCK);
	
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
        fprintf(stderr, "get or create for host %s:%d\n",host, (int)port);
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
			hostp = (char*) cf_vector_getp(&asc->host_str_v, i);
			portp = (int ) cf_vector_integer_get(&asc->host_port_v, i);
			if (strncmp(host,hostp,strlen(host)+1) && (port == portp)) {
				// Found the cluster object.
				// Increment the reference count
				#ifdef DEBUG
				fprintf(stderr, "host already added on a cluster object. Increment ref_count (%d) and returning pointer - %p\n", asc->ref_count, asc);
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
		fprintf(stderr, "get_or_create - could not create cluster\n");
		return(0);
	}

	// Add the host to the created cluster object	
	int ret = citrusleaf_cluster_add_host(asc, host, port, timeout_ms);
	if (0 != ret) {
		fprintf(stderr, "get_or_create - add_host failed with error %d\n", ret);
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
		fprintf(stderr, "release or destroy for cluster object - %p. ref_count = %d\n",*asc, (*asc)->ref_count);
	} else {
		fprintf(stderr, "release or destroy - asc is  NULL\n");
	}
#endif

	pthread_mutex_lock(&(*asc)->LOCK);
	if (asc && (*asc) && ((*asc)->ref_count > 0 )) {
		(*asc)->ref_count--;
		if (0 == (*asc)->ref_count) {
			pthread_mutex_unlock(&(*asc)->LOCK);
			// Destroy the object as reference count is 0
			#ifdef DEBUG
			fprintf(stderr, "destroying the cluster object as reference count is 0\n");
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
#ifdef DEBUG	
	fprintf(stderr, "adding host %s:%d timeout %d\n",host_in, (int)port, timeout_ms);
#endif	
	// Find if the host has already been added on this cluster object
	char *hostp;
	int portp, found = 0;
	pthread_mutex_lock(&asc->LOCK);
	for (uint32_t i=0; i<asc->host_str_v.len; i++) {
		hostp = (char*) cf_vector_getp(&asc->host_str_v, i);
		portp = (int ) cf_vector_integer_get(&asc->host_port_v, i);
		if (strncmp(host_in,hostp,strlen(host_in)+1) && (port == portp)) {
			// Return OK if host is already added in the list
			pthread_mutex_unlock(&asc->LOCK);
			#ifdef DEBUG
			fprintf(stderr, "host already added in this cluster object. Return OK\n");
			#endif
			return(CITRUSLEAF_OK);
		}
	}

	char *host = strdup(host_in);

	pthread_mutex_unlock(&asc->LOCK);
	// Lookup the address before adding to asc. If lookup fails
	// return CITRUSLEAF_FAIL_CLIENT
	// Resolve - error message need to change.
 	if(cl_lookup(asc, host, port, (cf_vector *)NULL) != 0) {
		return (CITRUSLEAF_FAIL_CLIENT);
	}
	
	// Host not found on this cluster object
	// Add the host and port to the lists of hosts to try when maintaining
	pthread_mutex_lock(&asc->LOCK);
	cf_vector_pointer_append(&asc->host_str_v, host);
	cf_vector_integer_append(&asc->host_port_v, (int) port);
	pthread_mutex_unlock(&asc->LOCK);

	// Fire the normal tender function to speed up resolution
	if (asc->found_all == false)
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
		fprintf(stderr, "add host: required %d tends %"PRIu64"ms to set right\n",n_tends,cf_getms()-start_ms);
#endif		
	}
	
	if(!asc->found_all){
		return CITRUSLEAF_FAIL_TIMEOUT;
	}

	return(0); // CITRUSLEAF_OK;
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
		//fprintf(stderr, "Adding the mapping %x:%s->%s\n", newmap, orig, alt);
	} 
	else {
		//fprintf(stderr, "Mapping %s->%s already exists\n", oldmap->orig, oldmap->alt);
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
//	fprintf(stderr, " cluster get: %s\n",url);

	// make sure it's a citrusleaf url
	char *urlx = strdup(url);
	char *proto = strchr(urlx, ':');
	if (!proto) {
		fprintf(stderr, "warning: url %s illegal for citrusleaf connect\n",url);
		free(urlx);
		return(0);
	}
	*proto = 0;
	if (strcmp(proto, "citrusleaf") == 0) {
		fprintf(stderr, "warning: url %s illegal for citrusleaf connect\n",url);
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

	// fprintf(stderr, " cluster get: host %s port %d\n",host, port_i);

	// search the cluster list for matching url open names
	cl_cluster *asc = 0;
	pthread_mutex_lock(&cluster_ll_LOCK);
	cf_ll_element *e = cf_ll_get_head(&cluster_ll);
	while (e && asc == 0) {
		cl_cluster *cl_asc = (cl_cluster *) e;

		// fprintf(stderr, " cluster get: comparing against %p\n",cl_asc);

		uint i;
		pthread_mutex_lock(&cl_asc->LOCK);
		for (i=0;i<cf_vector_size(&cl_asc->host_str_v);i++) {
			char *cl_host_str = cf_vector_pointer_get(&cl_asc->host_str_v, i);
			int   cl_port_i = cf_vector_integer_get(&cl_asc->host_port_v, i);

			// fprintf(stderr, " cluster get: comparing against %s %d\n",cl_host_str, cl_port_i);

			if (strcmp(cl_host_str, host)!= 0)	continue;
			if (cl_port_i == port_i) {
				// found
				asc = cl_asc;
				break;
			}
			// fprintf(stderr, " cluster get: comparing against %p\n",cl_asc);
		}
		pthread_mutex_unlock(&cl_asc->LOCK);

		e = cf_ll_get_next(e);
	}
	pthread_mutex_unlock(&cluster_ll_LOCK);

	if (asc) {
		// fprintf(stderr, " cluster get: reusing cluster %p\n",asc);
		free(urlx);
		return(asc);
	}

	// doesn't exist yet? create a new one
	asc = citrusleaf_cluster_create();
	citrusleaf_cluster_add_host(asc, host, port_i, 0);

    // check to see if we actually got some initial node
	uint node_v_sz = cf_vector_size(&asc->node_v);
    if (node_v_sz==0) {
		fprintf(stderr, " no node added in initial create \n");
        citrusleaf_cluster_destroy(asc);
    	free(urlx);
        return NULL;
    }
        
    
	// fprintf(stderr, " cluster get: new cluster %p\n",asc);

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
		fprintf(stderr, "cluster node get: no nodes in this cluster\n");
#endif		
		return(0);
	}

	uint i=0;
	do {
		asc->last_node ++;
		if (asc->last_node >= node_v_sz)	asc->last_node = 0;
		int node_i = asc->last_node;

#ifdef DEBUG_VERBOSE		
		fprintf(stderr, "cluster node get: vsize %d choosing %d\n",
			cf_vector_size(&asc->node_v),node_i);
#endif		

		cn = cf_vector_pointer_get(&asc->node_v, node_i);
		i++;
        
	} while( (cn->dunned == true) && (i <= node_v_sz) );

#ifdef DEBUG    
    fprintf(stderr,"   random node chosen: %s\n",cn->name);
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
		fprintf(stderr, "cluster node get: found match key %"PRIx64" node %s (%s):\n",
											*(uint64_t*)d, cn->name, write?"write":"read");
#endif		
		cf_client_rc_reserve(cn);
		pthread_mutex_unlock(&asc->LOCK);
		return(cn);
	}
#ifdef DEBUG_VERBOSE	
	fprintf(stderr, "cluster node get: not found, try random key %"PRIx64"\n",*(uint64_t *)d);
#endif	
	
	cn = cl_cluster_node_get_random(asc);
	pthread_mutex_unlock(&asc->LOCK);
	return(cn);
}

void 
cl_cluster_get_node_names(cl_cluster *asc, int *n_nodes, char **node_names)
{
	if (node_names) {
		*node_names = NULL;
	}
	if (n_nodes) {
		*n_nodes = 0;
	}

	pthread_mutex_lock(&asc->LOCK);
	if (n_nodes) {
		*n_nodes = cf_vector_size(&asc->node_v);
	}
	if (node_names) {
		*node_names = malloc(NODE_NAME_SIZE*cf_vector_size(&asc->node_v));
		if (*node_names==NULL) {
			pthread_mutex_unlock(&asc->LOCK);
			return;	
		}		

		char *nptr = *node_names;
		for (uint i=0;i<cf_vector_size(&asc->node_v);i++) {
			cl_cluster_node *cn = cf_vector_pointer_get(&asc->node_v, i);
			memcpy(nptr, cn->name,NODE_NAME_SIZE);
			nptr+=NODE_NAME_SIZE;
		}	
	}
	pthread_mutex_unlock(&asc->LOCK);
}

cl_cluster_node *
cl_cluster_node_get_byname(cl_cluster *asc, char *name)
{
	pthread_mutex_lock(&asc->LOCK);
	for (uint i=0;i<cf_vector_size(&asc->node_v);i++) {
		cl_cluster_node *cn = cf_vector_pointer_get(&asc->node_v, i);
		if (strcmp(name, cn->name) == 0) {
			pthread_mutex_unlock(&asc->LOCK);
			return(cn);
		}
	}
	pthread_mutex_unlock(&asc->LOCK);
	return(0);
	
}

cl_cluster_node *
cl_cluster_node_get_byaddr(cl_cluster *asc, struct sockaddr_in *sa_in)
{
	pthread_mutex_lock(&asc->LOCK);
	for (uint i=0;i<cf_vector_size(&asc->node_v);i++) {
		cl_cluster_node *cn = cf_vector_pointer_get(&asc->node_v, i);
		for (uint j=0;j<cf_vector_size(&cn->sockaddr_in_v);j++) {
			struct sockaddr_in *node_sa_in = cf_vector_getp(&cn->sockaddr_in_v, j);
			if (memcmp(sa_in, node_sa_in, sizeof(struct sockaddr_in) ) == 0 ) {
				pthread_mutex_unlock(&asc->LOCK);
				return(cn);
			}
		}
	}
	pthread_mutex_unlock(&asc->LOCK);
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
		fprintf(stderr, "node %s health decreased %d\n", cn->name, score);
	}
#endif

	if (cf_atomic32_add(&cn->dun_score, score) > NODE_DUN_THRESHOLD) {
		cn->dunned = true;

#ifdef DEBUG
		fprintf(stderr, "dunning node %s\n", cn->name);
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
		fprintf(stderr, "could not allocate a socket, serious problem\n");
#endif			
		return(-1);
	}
#ifdef DEBUG_VERBOSE		
	else {
		fprintf(stderr, "new socket: fd %d node %s\n",fd, cn->name);
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
				dump_sockaddr_in("Connecting to ", sa_in);
				fprintf(stderr, "Non-blocking connect returned EINPROGRESS as expected\n");
#endif				
				goto Done;
			}
			// todo: remove this sockaddr from the list, or dun the node?
			if (errno == ECONNREFUSED) {
				fprintf(stderr, "a host is refusing connections\n");
			}
			else {
				fprintf(stderr, "connect fail: errno %d\n",errno);
			}
		}
	}
	close(fd);
	fd = -1;
		
Done:
	endtime = cf_getms();
	//fprintf(stderr, "Time taken to open a new connection = %"PRIu64"", (endtime - starttime));
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
	} else {
		q = cn->conn_q;
	}
	
	cf_queue_push(q, &fd);
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

//
// Process new partitions information
// namespace:part_id;namespace:part_id
//
// Update the cluster with the new information

static void
cluster_partitions_process(cl_cluster *asc, cl_cluster_node *cn, char *partitions, bool write) 
{
#ifdef DEBUG	
	fprintf(stderr, "process partitions: for node %s %s\n",cn->name, write?"write":"read");
#endif
	
	// use a create instead of a define because we know the size, and the size will likely be larger
	// than a stack allocation
	cf_vector *partitions_v = cf_vector_create(sizeof(void *), asc->n_partitions+1, 0);
	str_split(';',partitions, partitions_v);
	// partition_v is a vector of namespace:part_id
	for (uint i=0;i<cf_vector_size(partitions_v);i++) {
		char *partition_str = cf_vector_pointer_get(partitions_v, i);
		cf_vector_define(partition_v, sizeof(void *), 0);
		str_split(':', partition_str, &partition_v);
		if (cf_vector_size(&partition_v) == 2) {
			char *namespace_s = cf_vector_pointer_get(&partition_v,0);
			char *partid_s = cf_vector_pointer_get(&partition_v,1);
			int partid = atoi(partid_s);
			// it's coming over the wire, so validate it
			if (strlen(namespace_s) > 30) {
				fprintf(stderr, "cluster partitions process: bad namespace: len %zd space %s\n",strlen(namespace_s),
					namespace_s);
				goto Next;
			}
			if (partid > asc->n_partitions) {
				fprintf(stderr, "cluster partitions process: partitions out of scale: found %d max %d\n",
					partid, asc->n_partitions);
				goto Next;
			}
				
			pthread_mutex_lock(&asc->LOCK);
			cl_partition_table_set(asc, cn, namespace_s, partid, write);
			pthread_mutex_unlock(&asc->LOCK);
#ifdef DEBUG_VERBOSE			
			fprintf(stderr, "process_partitions: node %s responsible for %s partition: %s : %d\n",
				cn->name,write ? "write" : "read",namespace_s,partid);
#endif			
		}
Next:
		cf_vector_destroy(&partition_v);
	}
	cf_vector_destroy(partitions_v);
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
	fprintf(stderr, "cluster ping node: %s\n",cn->name);
#endif	
	
	// for all elements in the sockaddr_in list - ping and add hosts, if not
	// already extant
	for (uint i=0;i<cf_vector_size(&cn->sockaddr_in_v);i++) {
		struct sockaddr_in *sa_in = cf_vector_getp(&cn->sockaddr_in_v, i);
		
		char *values = 0;
		if (0 != citrusleaf_info_host(sa_in, "node\npartition-generation\nservices", &values, INFO_TIMEOUT_MS, false)) {
			// todo: this address is no longer right for this node, update the node's list
			// and if there's no addresses left, dun node
			cl_cluster_node_dun(cn, NODE_DUN_INFO_ERR);
			continue;
		}

		cl_cluster_node_ok(cn);

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
						fprintf(stderr, "node name has changed!!!\n");
						cl_cluster_node_dun(cn, NODE_DUN_INFO_ERR);
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
	
	
	if (update_partitions == true) {
//		fprintf(stderr, "node %s: partitions have changed, need update\n",cn->name);

		// remove all current values, then add up-to-date values
		pthread_mutex_lock(&asc->LOCK);
		cl_partition_table_remove_node(asc, cn);
		pthread_mutex_unlock(&asc->LOCK);

		for (uint i=0;i<cf_vector_size(&cn->sockaddr_in_v);i++) {
			struct sockaddr_in *sa_in = cf_vector_getp(&cn->sockaddr_in_v, i);
			char *values = 0;
			if (0 != citrusleaf_info_host(sa_in, "replicas-read\nreplicas-write", &values, INFO_TIMEOUT_MS, false)) {
                // it's a little peculiar to have just talked to the host then have this call
                // fail, but sometimes strange things happen.
                goto Updated;
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

					
					if (strcmp(name, "replicas-read")== 0)
						cluster_partitions_process(asc, cn, value, false);

					else if (strcmp(name, "replicas-write")==0)
						cluster_partitions_process(asc, cn, value, true);
						
				}
				cf_vector_destroy(&pair_v);
			}
			cf_vector_destroy(&lines_v);
			
			free(values);
			
			goto Updated;
		}
	}
Updated:	
	;
}



//
// Ping this address, get its node name, see if it's unique
// side effect: causes creation of node if new
static void
cluster_ping_address(cl_cluster *asc, struct sockaddr_in *sa_in)
{
		
	char *values = 0;
	if (0 != citrusleaf_info_host(sa_in, "node", &values, INFO_TIMEOUT_MS, false)){
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
		if (g_cl_turn_debug_on) {
			fprintf(stderr, "%s node unknown, creating new\n", value);
			dump_sockaddr_in("New node is ",sa_in);
		}
		cl_cluster_node *node = cl_cluster_node_create(value, sa_in);
		cf_vector_pointer_append(&asc->node_v, node);
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
		if (0 != citrusleaf_info_host(sa_in, "partitions", &values, INFO_TIMEOUT_MS, false)) {
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
cluster_tend( cl_cluster *asc) 
{

#ifdef DEBUG
	print_ms("cluster tend");
	fprintf(stderr, "************ cluster tend:\n");
#endif	

	// Start off by removing dunned hosts
	pthread_mutex_lock(&asc->LOCK);
	if (asc->state & CLS_FREED) {
		pthread_mutex_unlock(&asc->LOCK);
		return;
	}
	asc->state |= CLS_TENDER_RUNNING;
	for (uint i=0;i<cf_vector_size(&asc->node_v);i++) {
		cl_cluster_node *cn = cf_vector_pointer_get(&asc->node_v, i);
		if (!cn)	continue;
		if (cn->dunned) {
			fprintf(stderr, " DELETE DUNNED NODE %s %p i %d\n",cn->name,cn,i);
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
		
        if (g_cl_turn_debug_on) {
		    char *host = cf_vector_pointer_get(&asc->host_str_v, i);
		    int port = cf_vector_integer_get(&asc->host_port_v, i);
    		fprintf(stderr, "lookup hosts: %s:%d\n",host,port);
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

	// Now, ping known nodes to see if there's an update
	for (uint i=0;i<cf_vector_size(&asc->node_v);i++) {
		cl_cluster_node *cn = cf_vector_pointer_get(&asc->node_v, i);
		cluster_ping_node(asc, cn, &sockaddr_in_v);
		for (uint j=0;j<cf_vector_size(&cn->sockaddr_in_v);j++) {
			struct sockaddr_in *sa_in = cf_vector_getp(&cn->sockaddr_in_v,j);
			cf_vector_append_unique(&sockaddr_in_v, sa_in);
		}
	}
	
//	fprintf(stderr, "CLUSTER TEND: sockaddr_in_v len %d %s\n",cf_vector_size(&sockaddr_in_v), asc->found_all?"foundall":"notfound" );
	
	// Compare all services with known nodes - explore if new
	if (asc->follow == true) {
		int n_new = 0;
		for (uint i=0;i<cf_vector_size(&sockaddr_in_v);i++) {
			struct sockaddr_in *sin = cf_vector_getp(&sockaddr_in_v, i);
			if (g_cl_turn_debug_on) {
				dump_sockaddr_in("testing service address",sin);
			}		
			if (0 == cl_cluster_node_get_byaddr(asc, sin)) {
				if (g_cl_turn_debug_on) {
					dump_sockaddr_in("pinging",sin);
				}		
				cluster_ping_address(asc, sin);
				n_new++;
			}
		}
		if (n_new == 0)	{
			//fprintf(stderr, "CLUSTER TEND: *** FOUND ALL ***\n");
			asc->found_all = true;
		}
		//fprintf(stderr, "CLUSTER TEND: n_new is %d foundall %d\n",n_new,asc->found_all);
	}

	cf_vector_destroy(&sockaddr_in_v);

#ifdef DEBUG	
	dump_cluster(asc);
#endif	

	pthread_mutex_lock(&asc->LOCK);
	asc->state &= ~CLS_TENDER_RUNNING;
	pthread_mutex_unlock(&asc->LOCK);

//	print_ms("end tend");
	
	return;
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


