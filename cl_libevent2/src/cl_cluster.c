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

#include <ctype.h>
#include <pthread.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <event2/dns.h>
#include <event2/event.h>

#include "citrusleaf/cf_alloc.h"
#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_base_types.h"
#include "citrusleaf/cf_byte_order.h"
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_digest.h"
#include "citrusleaf/cf_errno.h"
#include "citrusleaf/cf_ll.h"
#include "citrusleaf/cf_log_internal.h"
#include "citrusleaf/cf_queue.h"
#include "citrusleaf/cf_socket.h"
#include "citrusleaf/cf_vector.h"
#include "citrusleaf/proto.h"

#include "citrusleaf_event2/ev2citrusleaf.h"
#include "citrusleaf_event2/ev2citrusleaf-internal.h"

#include "citrusleaf_event2/cl_cluster.h"


extern void ev2citrusleaf_base_hop(cl_request *req);

// #define CLDEBUG_VERBOSE 1
// #define CLDEBUG_DUN 1

//
// Number of requests, in a row, that need to fail before the node
// is considered bad
//

#define CL_NODE_DUN_THRESHOLD 800

//
// Intervals on which tending happens
//

// BFIX - this should be higher like 1.5 sec to be above the connect timeout

// this one is a little cheaper - looks for locally dunned nodes and ejects them
struct timeval g_cluster_tend_timeout = {1,200000};
// struct timeval g_cluster_tend_timeout = {1,000000};
// struct timeval g_cluster_tend_timeout = {0,500000};


// this one can be expensive because it makes a request of the server
// struct timeval g_node_tend_timeout = {0,400000};
struct timeval g_node_tend_timeout = {1,1};


// Forward references
void cluster_tend( ev2citrusleaf_cluster *asc); 
void cluster_new_sockaddr(ev2citrusleaf_cluster *asc, struct sockaddr_in *new_sin);
int ev2citrusleaf_cluster_add_host_internal(ev2citrusleaf_cluster *asc, char *host_in, short port_in);


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

ev2citrusleaf_cluster *
cluster_create()
{
	ev2citrusleaf_cluster *asc = (ev2citrusleaf_cluster*)malloc(sizeof(ev2citrusleaf_cluster) + event_get_struct_event_size() );
	if (!asc) return(0);
	memset((void*)asc,0,sizeof(ev2citrusleaf_cluster) + event_get_struct_event_size());
	MUTEX_ALLOC(asc->node_v_lock);
	MUTEX_ALLOC(asc->request_q_lock);
	return(asc);
}

void
cluster_destroy(ev2citrusleaf_cluster *asc) {
	if (asc->dns_base) {
		evdns_base_free(asc->dns_base, 0);
	}

	if (asc->internal_mgr) {
		event_base_free(asc->base);
	}

	MUTEX_FREE(asc->request_q_lock);
	MUTEX_FREE(asc->node_v_lock);
	memset((void*)asc, 0, sizeof(ev2citrusleaf_cluster) + event_get_struct_event_size() );
	free(asc);
	return;
}

struct event *
cluster_get_timer_event(ev2citrusleaf_cluster *asc)
{
	return( (struct event *) &asc->event_space[0] );
}


cl_cluster_node*
cluster_node_create()
{
	size_t size = sizeof(cl_cluster_node) + (2 * event_get_struct_event_size());
	cl_cluster_node* cn = (cl_cluster_node*)cf_client_rc_alloc(size);

	if (cn) {
		memset((void*)cn, 0, size);
	}

	return cn;
}

static inline struct event*
cluster_node_get_timer_event(cl_cluster_node* cn)
{
	return (struct event*)cn->event_space;
}

static inline struct event*
cluster_node_get_info_event(cl_cluster_node* cn)
{
	return (struct event*)(cn->event_space + event_get_struct_event_size());
}


//
// Parse a services string of the form:
// host:port;host:port
// Into the unique cf_vector of sockaddr_t
//
// We're guarenteed at this point that the services vector is all a.b.c.d, so
// using the actual async resolver is not necessary
//
// This routine now adds the found objects to whatever host lists it can find.
// It's important to add to the general host list juts in case we go to 0 hosts
// and it's important to add to the sockaddr list to start pinging the new
// hosts immediately for partition data and starting to route traffic

static void
cluster_services_parse(ev2citrusleaf_cluster *asc, char *services) 
{
	cf_vector_define(host_str_v, sizeof(void *), 0);
	str_split(';',services, &host_str_v);
	for (uint32_t i=0;i<cf_vector_size(&host_str_v);i++) {
		char *host_str = (char*)cf_vector_pointer_get(&host_str_v, i);
		cf_vector_define(host_port_v, sizeof(void *), 0);
		str_split(':', host_str, &host_port_v);
		if (cf_vector_size(&host_port_v) == 2) {
			char *host_s = (char*)cf_vector_pointer_get(&host_port_v,0);
			char *port_s = (char*)cf_vector_pointer_get(&host_port_v,1);
			int port = atoi(port_s);
			struct sockaddr_in sin;
			if (0 == cl_lookup_immediate(host_s, port, &sin)) {
				cluster_new_sockaddr(asc, &sin);
				// add the string representation to our host list
				ev2citrusleaf_cluster_add_host_internal(asc, host_s, port);
			}
			
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

	// Trim leading space.
	while (isspace(*begin)) {
		begin++;
	}

	if(*begin == 0) {
		return begin;
	}

	// Trim trailing space. Go to end first so whitespace is preserved in the
	// middle of the string.
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
//
// This is a function I'm always worried about taking too long
//

static void
cluster_partitions_process(ev2citrusleaf_cluster *asc, cl_cluster_node *cn, char *partitions, bool write) 
{
	cf_atomic_int_incr(&g_cl_stats.partition_process);
	uint64_t _s = cf_getms();

	// Partitions format: <namespace1>:<partition id1>;<namespace2>:<partition id2>; ...
	// Warning: This method walks on partitions string argument.
	char *p = partitions;

	while (*p)
	{
		char *partition_str = p;
		// Loop until split and set it to null.
		while ((*p) && (*p != ';')) {
			p++;
		}
		if (*p == ';') {
			*p = 0;
			p++;
		}
		cf_vector_define(partition_v, sizeof(void *), 0);
		str_split(':', partition_str, &partition_v);

		unsigned int vsize = cf_vector_size(&partition_v);
		if (vsize == 2) {
			char *namespace_s = (char*)cf_vector_pointer_get(&partition_v,0);
			char *partid_s = (char*)cf_vector_pointer_get(&partition_v,1);

			// It's coming over the wire, so validate it.
			char *ns = trim(namespace_s);
			int len = (int)strlen(ns);

			if (len == 0 || len > 31) {
				cf_warn("Invalid partition namespace %s", ns);
				goto Next;
			}

			int partid = atoi(partid_s);

			if (partid < 0 || partid >= (int)asc->n_partitions) {
				cf_warn("Invalid partition id %s, max %u", partid, asc->n_partitions);
				goto Next;
			}

			cl_partition_table_set(asc, cn, ns, partid, write);
		}
		else {
			cf_warn("Invalid partition vector size %u, element %s", vsize, partition_str);
		}
Next:
		cf_vector_destroy(&partition_v);
	}

	uint64_t delta = cf_getms() - _s;
	if (delta > CL_LOG_DELAY_INFO) cf_info("CL_DELAY: partition process: %lu", delta);
}


// List of all current clusters so the tender can maintain them
// 
cf_ll		cluster_ll;


void
cluster_timer_fn(evutil_socket_t fd, short event, void *udata)
{
	ev2citrusleaf_cluster *asc = (ev2citrusleaf_cluster *)udata;
	uint64_t _s = cf_getms();
	
	if (asc->MAGIC != CLUSTER_MAGIC) {
		cf_warn("cluster timer on non-cluster object %p", asc);
		return;
	}

	cluster_tend(asc);
	
	if (time(0) % CL_LOG_STATS_INTERVAL == 0) {
		cl_partition_table_dump(asc);
		ev2citrusleaf_print_stats();
		cf_info("requests in progress: %d", cf_atomic_int_get(asc->requests_in_progress));
	}
                                                                
	if (0 != event_add(cluster_get_timer_event(asc), &g_cluster_tend_timeout)) {
		cf_warn("cluster can't reschedule timer, fatal error, no one to report to");
	}
	
	uint64_t delta = cf_getms() - _s;
	if (delta > CL_LOG_DELAY_INFO) cf_info("CL_DELAY: cluster timer: %lu", delta);
}


static void* run_cluster_mgr(void* base) {
	// Blocks until there are no more added events, or until something calls
	// event_base_loopbreak() or event_base_loopexit().
	int result = event_base_dispatch((struct event_base*)base);

	if (result != 0) {
		cf_warn("cluster manager event_base_dispatch() returned %d", result);
	}

	return NULL;
}


ev2citrusleaf_cluster *
ev2citrusleaf_cluster_create(struct event_base *base,
		ev2citrusleaf_cluster_options *opts)
{
	if (! g_ev2citrusleaf_initialized) {
		cf_warn("must call ev2citrusleaf_init() before ev2citrusleaf_cluster_create()");
		return 0;
	}

	ev2citrusleaf_cluster *asc = cluster_create();
	if (!asc)	return(0);
	
	asc->MAGIC = CLUSTER_MAGIC;
	asc->follow = true;
	asc->last_node = 0;

	if (base) {
		asc->internal_mgr = false;
		asc->base = base;
	}
	else {
		asc->internal_mgr = true;
		asc->base = event_base_new();

		if (! asc->base) {
			cf_warn("error creating cluster manager event base");
			return NULL;
		}
	}

	// Note - this keeps this base's event loop alive even with no events added.
	asc->dns_base = evdns_base_new(asc->base, 1);

	// Copy the cluster options if any are passed in.
	if (opts) {
		asc->options = *opts;
	}
	// else defaults are all 0, from memset() in cluster_create()

	// bookkeeping for the set hosts
	cf_vector_pointer_init(&asc->host_str_v, 10, VECTOR_FLAG_BIGLOCK);
	cf_vector_integer_init(&asc->host_port_v, 10, VECTOR_FLAG_BIGLOCK);

	// all the nodes
	cf_vector_pointer_init(&asc->node_v, 10, 0 /*flag*/);
	
	asc->request_q = cf_queue_create(sizeof(void *), true);
	if (asc->request_q == 0) {
		cluster_destroy(asc);
		return(0);
	}
	
	cf_ll_append(&cluster_ll, (cf_ll_element *) asc);

	asc->n_partitions = 0;
	asc->partition_table_head = 0;
	
	evtimer_assign(cluster_get_timer_event(asc), asc->base, cluster_timer_fn, asc);
	if (0 != event_add(cluster_get_timer_event(asc), &g_cluster_tend_timeout)) {
		cf_warn("could not add the cluster timeout");
		cf_queue_destroy(asc->request_q);
		// BFIX - the next line should be in
		cf_ll_delete( &cluster_ll , (cf_ll_element *) asc);
		cluster_destroy(asc);
		return(0);
	}

	if (asc->internal_mgr &&
			0 != pthread_create(&asc->mgr_thread, NULL, run_cluster_mgr, (void*)asc->base)) {
		cf_warn("error creating cluster manager thread");
		event_del(cluster_get_timer_event(asc));
		cf_queue_destroy(asc->request_q);
		cf_ll_delete(&cluster_ll, (cf_ll_element*)asc);
		cluster_destroy(asc);
		return NULL;
	}

	return(asc);
}

int
ev2citrusleaf_cluster_get_active_node_count(ev2citrusleaf_cluster *asc)
{
	// *AN* likes to call with a null pointer. Shame.
	if (!asc)					return(-1);
	
	if (asc->MAGIC != CLUSTER_MAGIC) {
		cf_warn("cluster get_active_node on non-cluster object %p", asc);
		return(0);
	}

	int count = 0;

	MUTEX_LOCK(asc->node_v_lock);
	
	for (uint32_t i=0;i<cf_vector_size(&asc->node_v);i++) {
		cl_cluster_node *node = (cl_cluster_node*)cf_vector_pointer_get(&asc->node_v, i);
		
		if (node->MAGIC != CLUSTER_NODE_MAGIC) {
			cf_error("node in cluster list has no magic!");
			continue;
		}
		
		if (node->name[0] == 0) {
			cf_warn("cluster node %d has no name (this is likely a serious internal confusion)", i);
			continue; // nodes with no name have never been pinged
		}
		
		if (cf_atomic_int_get(node->dunned)) {
			cf_info("cluster node %s (%d) is dunned", node->name, i);
			continue; // dunned nodes aren't active
		}
		
		if (cf_vector_size(&node->sockaddr_in_v)==0) {
			cf_warn("cluster node %s (%d) has no address", node->name, i);
			continue; // nodes with no IP addresses aren't active
		}
		
		// maybe there are some other statistics, like the last good transaction...
		
		count++;
	}

	int rv = cf_vector_size(&asc->node_v);

	MUTEX_UNLOCK(asc->node_v_lock);

	cf_info("cluster has %d nodes, %d ok", rv, count);

	return(rv);
}


int ev2citrusleaf_cluster_requests_in_progress(ev2citrusleaf_cluster *cl) {
	return (int)cf_atomic_int_get(cl->requests_in_progress);
}


void
ev2citrusleaf_cluster_refresh_partition_tables(ev2citrusleaf_cluster *asc)
{
	if (! asc) {
		cf_warn("cluster refresh_partition_tables with null cluster");
		return;
	}

	if (asc->MAGIC != CLUSTER_MAGIC) {
		cf_warn("cluster refresh_partition_tables with non-cluster object %p", asc);
		return;
	}

	MUTEX_LOCK(asc->node_v_lock);

	for (uint32_t i = 0; i < cf_vector_size(&asc->node_v); i++) {
		cl_cluster_node* node = (cl_cluster_node*)cf_vector_pointer_get(&asc->node_v, i);

		if (node->MAGIC != CLUSTER_NODE_MAGIC) {
			cf_error("node in cluster list has no magic!");
			continue;
		}

		cf_info("forcing cluster node %s to get partition info", node->name, i);

		cf_atomic_int_set(&node->partition_generation, (cf_atomic_int_t)-1);
	}

	MUTEX_UNLOCK(asc->node_v_lock);
}

static void
node_info_req_shutdown(cl_cluster_node* cn)
{
	// Saves us from "hand-cranking" a node info request at shutdown.
	if (cn->info_req.type != INFO_REQ_NONE) {
		event_del(cluster_node_get_info_event(cn));
		cl_cluster_node_release(cn, "I-");
	}
}

void
ev2citrusleaf_cluster_destroy(ev2citrusleaf_cluster *asc)
{
	cf_info("cluster destroy: %p", asc);

	if (asc->MAGIC != CLUSTER_MAGIC) {
		cf_warn("cluster destroy on non-cluster object %p", asc);
		return;
	}

	if (asc->internal_mgr) {
		// Exit the cluster manager event loop.
		event_base_loopbreak(asc->base);

		void* pv_value;
		pthread_join(asc->mgr_thread, &pv_value);
	}

	if (cf_atomic_int_get(asc->requests_in_progress)) {
		cf_warn("cluster destroy with requests in progress");
		// Proceed and hope for the best (will likely at least leak memory)...
	}

	// Clear cluster manager timer.
	event_del(cluster_get_timer_event(asc));

	// Clear all node timers and node info requests.
	for (uint32_t i = 0; i < cf_vector_size(&asc->node_v); i++) {
		cl_cluster_node *cn = (cl_cluster_node*)cf_vector_pointer_get(&asc->node_v, i);
		node_info_req_shutdown(cn);
		event_del(cluster_node_get_timer_event(cn));
		// ... so the event_del() in cl_cluster_node_release() will be a no-op.
	}

	// Clear all outstanding (non-node) internal info requests.
	while (cf_atomic_int_get(asc->pings_in_progress)) {
		// Note - if the event base dispatcher is still active, this generates
		// reentrancy warnings, and may otherwise have unknown effects...
		int loop_result = event_base_loop(asc->base, EVLOOP_ONCE);

		if (loop_result != 0) {
			cf_warn("cluster destroy event_base_loop() returns %d",
				loop_result);
			// Proceed and hope for the best...
			break;
		}
	}

	// Destroy all the nodes.
	for (uint32_t i = 0; i < cf_vector_size(&asc->node_v); i++) {
		cl_cluster_node *cn = (cl_cluster_node*)cf_vector_pointer_get(&asc->node_v, i);
		cl_cluster_node_release(cn, "C-");
		cl_cluster_node_release(cn, "L-");
	}

	cf_queue_destroy(asc->request_q);
	asc->request_q = 0;

	for (uint32_t i = 0; i < cf_vector_size(&asc->host_str_v); i++) {
		char *host_str = (char*)cf_vector_pointer_get(&asc->host_str_v, i);
		free(host_str);
	}

	cf_vector_destroy(&asc->host_str_v);
	cf_vector_destroy(&asc->host_port_v);
	cf_vector_destroy(&asc->node_v);

	cl_partition_table_destroy_all(asc);

	cf_ll_delete(&cluster_ll , (cf_ll_element *)asc);

	cluster_destroy(asc);
}


int
ev2citrusleaf_cluster_add_host_internal(ev2citrusleaf_cluster *asc, char *host_in, short port_in)
{
	// check for uniqueness - do we need a lock here?
	for (uint32_t i=0;i<cf_vector_size(&asc->host_str_v);i++) {
		char *host_str = (char*)cf_vector_pointer_get(&asc->host_str_v, i);
		int   port = cf_vector_integer_get(&asc->host_port_v, i);
		if ( ( 0 == strcmp(host_str, host_in) ) && (port_in == port) ) {
			return(0); // already here - don't add
		}
	}

	// Add the host and port to the lists of hosts to try when maintaining
	char *host = strdup(host_in);
	if (!host)	return(-1);

	cf_vector_pointer_append(&asc->host_str_v, host);
	cf_vector_integer_append(&asc->host_port_v, (int) port_in);

	return(0);
}



int
ev2citrusleaf_cluster_add_host(ev2citrusleaf_cluster *asc, char *host_in, short port_in)
{
	cf_debug("adding host %s:%d", host_in, (int)port_in);

	if (asc->MAGIC != CLUSTER_MAGIC) {
		cf_warn("cluster destroy on non-cluster object %p", asc);
		return(-1);
	}
	
	int rv = ev2citrusleaf_cluster_add_host_internal(asc, host_in, port_in);
	if (0 != rv)	return(rv);

	// Fire the normal tender function to speed up resolution
	cluster_tend(asc);
	
	return(0);
}

void
ev2citrusleaf_cluster_follow(ev2citrusleaf_cluster *asc, bool flag)
{
	asc->follow = flag;
}


//
// NODES NODES NODES
//


//==========================================================
// Periodic node timer functionality.
//

const char INFO_STR_CHECK[] = "node\npartition-generation\nservices\n";
const char INFO_STR_GET_REPLICAS[] = "replicas-read\nreplicas-write\npartition-generation\n";

static void node_info_req_start(cl_cluster_node* cn, node_info_req_type req_type);
// The libevent2 event handler for node info socket events:
static void node_info_req_event(evutil_socket_t fd, short event, void* udata);

static void
node_info_req_free(node_info_req* ir)
{
	if (ir->wbuf) {
		free(ir->wbuf);
	}

	if (ir->rbuf) {
		free(ir->rbuf);
	}

	// Includes setting type to INFO_REQ_NONE.
	memset((void*)ir, 0, sizeof(node_info_req));
}

static void
node_info_req_done(cl_cluster_node* cn)
{
	// Success - reuse the socket and approve the node.
	cl_cluster_node_ok(cn);

	node_info_req_free(&cn->info_req);
	cl_cluster_node_release(cn, "I-");
	cf_atomic_int_incr(&g_cl_stats.node_info_successes);
}

static void
node_info_req_fail(cl_cluster_node* cn, cl_cluster_dun_type dun_type)
{
	// The socket may have unprocessed data or otherwise be untrustworthy.
	cf_close(cn->info_fd);
	cn->info_fd = -1;

	// Disapprove the node.
	if (dun_type != DONT_DUN) {
		cl_cluster_node_dun(cn, dun_type);
	}

	node_info_req_free(&cn->info_req);
	cl_cluster_node_release(cn, "I-");
	cf_atomic_int_incr(&g_cl_stats.node_info_failures);
}

static void
node_info_req_timeout(cl_cluster_node* cn)
{
	event_del(cluster_node_get_info_event(cn));
	node_info_req_fail(cn, DUN_INFO_TIMEOUT);
	cf_atomic_int_incr(&g_cl_stats.node_info_timeouts);
}

static void
node_info_req_parse_check(cl_cluster_node* cn)
{
	bool get_replicas = false;

	cf_vector_define(lines_v, sizeof(void*), 0);
	str_split('\n', (char*)cn->info_req.rbuf, &lines_v);

	for (uint32_t i = 0; i < cf_vector_size(&lines_v); i++) {
		char* line = (char*)cf_vector_pointer_get(&lines_v, i);

		cf_vector_define(pair_v, sizeof(void*), 0);
		str_split('\t', line, &pair_v);

		if (cf_vector_size(&pair_v) != 2) {
			// Will happen if a requested field is returned empty.
			cf_vector_destroy(&pair_v);
			continue;
		}

		char* name = (char*)cf_vector_pointer_get(&pair_v, 0);
		char* value = (char*)cf_vector_pointer_get(&pair_v, 1);

		if (strcmp(name, "node") == 0) {
			if (strcmp(value, cn->name) != 0) {
				cf_warn("node name changed from %s to %s", cn->name, value);
				cf_vector_destroy(&pair_v);
				cf_vector_destroy(&lines_v);
				node_info_req_fail(cn, DUN_BAD_NAME);
				return;
			}
		}
		else if (strcmp(name, "partition-generation") == 0) {
			int client_gen = (int)cf_atomic_int_get(cn->partition_generation);
			int server_gen = atoi(value);

			// If generations don't match, flag for replicas request.
			if (client_gen != server_gen) {
				get_replicas = true;

				cf_debug("node %s partition generation %d needs update to %d",
						cn->name, client_gen, server_gen);
			}
		}
		else if (strcmp(name, "services") == 0) {
			// This spawns an independent info request.
			cluster_services_parse(cn->asc, value);
		}
		else {
			cf_warn("node %s info check did not request %s", cn->name, name);
		}

		cf_vector_destroy(&pair_v);
	}

	cf_vector_destroy(&lines_v);

	node_info_req_done(cn);

	if (get_replicas) {
		cf_info("making partition request of node %s", cn->name);

		node_info_req_start(cn, INFO_REQ_GET_REPLICAS);
	}
}

static void
node_info_req_parse_replicas(cl_cluster_node* cn)
{
	// Remove this node from the partition table.
	cl_partition_table_remove_node(cn->asc, cn);

	// Returned list format is name1\tvalue1\nname2\tvalue2\n...
	cf_vector_define(lines_v, sizeof(void*), 0);
	str_split('\n',(char*)cn->info_req.rbuf, &lines_v);

	for (uint32_t j = 0; j < cf_vector_size(&lines_v); j++) {
		char* line = (char*)cf_vector_pointer_get(&lines_v, j);

		cf_vector_define(pair_v, sizeof(void *), 0);
		str_split('\t', line, &pair_v);

		if (cf_vector_size(&pair_v) != 2) {
			// Will happen if a requested field is returned empty.
			cf_vector_destroy(&pair_v);
			continue;
		}

		char* name = (char*)cf_vector_pointer_get(&pair_v, 0);
		char* value = (char*)cf_vector_pointer_get(&pair_v, 1);

		if (strcmp(name, "replicas-read") == 0) {
			// Add this node to the partition table's read replicas.
			cluster_partitions_process(cn->asc, cn, value, false);
		}
		else if (strcmp(name, "replicas-write") == 0) {
			// Add this node to the partition table's write replicas.
			cluster_partitions_process(cn->asc, cn, value, true);
		}
		else if (strcmp(name, "partition-generation") == 0) {
			int gen = atoi(value);

			// Update to the new partition generation.
			cf_atomic_int_set(&cn->partition_generation, (cf_atomic_int_t)gen);

			cf_debug("node %s got partition generation %d", cn->name, gen);
		}
		else {
			cf_warn("node %s info replicas did not request %s", cn->name, name);
		}

		cf_vector_destroy(&pair_v);
	}

	cf_vector_destroy(&lines_v);

	node_info_req_done(cn);
}

static bool
node_info_req_handle_send(cl_cluster_node* cn)
{
	node_info_req* ir = &cn->info_req;

	while(true) {
		// Loop until everything is sent or we get would-block.

		if (ir->wbuf_pos >= ir->wbuf_size) {
			cf_error("unexpected write event");
			node_info_req_fail(cn, DONT_DUN);
			return true;
		}

		int rv = send(cn->info_fd,
				(cf_socket_data_t*)&ir->wbuf[ir->wbuf_pos],
				(cf_socket_size_t)(ir->wbuf_size - ir->wbuf_pos),
				MSG_DONTWAIT | MSG_NOSIGNAL);

		if (rv > 0) {
			ir->wbuf_pos += rv;

			// If done sending, switch to receive mode.
			if (ir->wbuf_pos == ir->wbuf_size) {
				event_assign(cluster_node_get_info_event(cn), cn->asc->base,
						cn->info_fd, EV_READ, node_info_req_event, cn);
				break;
			}

			// Loop, send what's left.
		}
		else if (rv == 0 || (errno != EAGAIN && errno != EWOULDBLOCK)) {
			// send() supposedly never returns 0.
			cf_debug("send failed: fd %d rv %d errno %d", cn->info_fd, rv, errno);
			node_info_req_fail(cn, DUN_INFO_NETWORK_ERROR);
			return true;
		}
		else {
			// Got would-block.
			break;
		}
	}

	// Will re-add event.
	return false;
}

static bool
node_info_req_handle_recv(cl_cluster_node* cn)
{
	node_info_req* ir = &cn->info_req;

	while (true) {
		// Loop until everything is read from socket or we get would-block.

		if (ir->hbuf_pos < sizeof(cl_proto)) {
			// Read proto header.

			int rv = recv(cn->info_fd,
					(cf_socket_data_t*)&ir->hbuf[ir->hbuf_pos],
					(cf_socket_size_t)(sizeof(cl_proto) - ir->hbuf_pos),
					MSG_DONTWAIT | MSG_NOSIGNAL);

			if (rv > 0) {
				ir->hbuf_pos += rv;
				// Loop, read more header or start reading body.
			}
			else if (rv == 0) {
				// Connection has been closed by the server.
				cf_debug("recv connection closed: fd %d", cn->info_fd);
				node_info_req_fail(cn, DUN_INFO_NETWORK_ERROR);
				return true;
			}
			else if (errno != EAGAIN && errno != EWOULDBLOCK) {
				cf_debug("recv failed: rv %d errno %d", rv, errno);
				node_info_req_fail(cn, DUN_INFO_NETWORK_ERROR);
				return true;
			}
			else {
				// Got would-block.
				break;
			}
		}
		else {
			// Done with header, read corresponding body.

			// Allocate the read buffer if we haven't yet.
			if (! ir->rbuf) {
				cl_proto* proto = (cl_proto*)ir->hbuf;

				cl_proto_swap(proto);

				ir->rbuf_size = proto->sz;
				ir->rbuf = (uint8_t*)malloc(ir->rbuf_size);

				if (! ir->rbuf) {
					cf_error("node info request rbuf allocation failed");
					node_info_req_fail(cn, DONT_DUN);
					return true;
				}
			}

			if (ir->rbuf_pos >= ir->rbuf_size) {
				cf_error("unexpected read event");
				node_info_req_fail(cn, DONT_DUN);
				return true;
			}

			int rv = recv(cn->info_fd,
					(cf_socket_data_t*)&ir->rbuf[ir->rbuf_pos],
					(cf_socket_size_t)(ir->rbuf_size - ir->rbuf_pos),
					MSG_DONTWAIT | MSG_NOSIGNAL);

			if (rv > 0) {
				ir->rbuf_pos += rv;

				if (ir->rbuf_pos == ir->rbuf_size) {
					// Done with proto body - assume no more protos.

					// If we are fully dunned and removed from the partition
					// tree already, there's no point continuing.
					// TODO - extra dunned state?
					if (cf_atomic_int_get(cn->dunned) == 1) {
						node_info_req_fail(cn, DONT_DUN);
						return true;
					}

					switch (ir->type) {
					case INFO_REQ_CHECK:
						// May start a INFO_REQ_GET_REPLICAS request!
						node_info_req_parse_check(cn);
						break;
					case INFO_REQ_GET_REPLICAS:
						node_info_req_parse_replicas(cn);
						break;
					default:
						// Since we can't assert:
						cf_error("node info request invalid type %d", ir->type);
						node_info_req_fail(cn, DONT_DUN);
						break;
					}

					return true;
				}

				// Loop, read more body.
			}
			else if (rv == 0) {
				// Connection has been closed by the server.
				cf_debug("recv connection closed: fd %d", cn->info_fd);
				node_info_req_fail(cn, DUN_INFO_NETWORK_ERROR);
				return true;
			}
			else if (errno != EAGAIN && errno != EWOULDBLOCK) {
				cf_debug("recv failed: rv %d errno %d", rv, errno);
				node_info_req_fail(cn, DUN_INFO_NETWORK_ERROR);
				return true;
			}
			else {
				// Got would-block.
				break;
			}
		}
	}

	// Will re-add event.
	return false;
}

// The libevent2 event handler for node info socket events:
static void
node_info_req_event(evutil_socket_t fd, short event, void* udata)
{
	cl_cluster_node* cn = (cl_cluster_node*)udata;

	if (cn->MAGIC != CLUSTER_NODE_MAGIC) {
		// Serious - can't do anything else with cn, including dunning node.
		cf_error("node info socket event found bad node magic");
		return;
	}

	bool transaction_done;

	if (event & EV_WRITE) {
		// Handle write phase.
		transaction_done = node_info_req_handle_send(cn);
	}
	else if (event & EV_READ) {
		// Handle read phase.
		transaction_done = node_info_req_handle_recv(cn);
	}
	else {
		// Should never happen.
		cf_error("unexpected event flags %d", event);
		node_info_req_fail(cn, DONT_DUN);
		return;
	}

	if (! transaction_done) {
		// There's more to do, re-add event.
		if (0 != event_add(cluster_node_get_info_event(cn), 0)) {
			cf_error("node info request add event failed");
			node_info_req_fail(cn, DONT_DUN);
		}
	}
}

static bool
node_info_req_prep_fd(cl_cluster_node* cn)
{
	if (cn->info_fd != -1) {
		// Socket was left open - check it.
		int result = ev2citrusleaf_is_connected(cn->info_fd);

		switch (result) {
		case CONNECTED:
			// It's still good.
			return true;
		case CONNECTED_NOT:
			// Can't use it - the remote end closed it.
			// TODO - dun?
			cf_close(cn->info_fd);
			cn->info_fd = -1;
			break;
		case CONNECTED_ERROR:
			// Some other problem, could have to do with remote end.
			cl_cluster_node_dun(cn, DUN_INFO_RESTART_FD);
			cf_close(cn->info_fd);
			cn->info_fd = -1;
			break;
		case CONNECTED_BADFD:
			// Local problem, don't dun, don't try closing.
			cf_warn("node %s info request bad fd %d", cn->name, cn->info_fd);
			cn->info_fd = -1;
			break;
		default:
			// Since we can't assert:
			cf_error("node %s info request connect state unknown", cn->name);
			cf_close(cn->info_fd);
			cn->info_fd = -1;
			return false;
		}
	}

	if (cn->info_fd == -1) {
		if (cf_vector_size(&cn->sockaddr_in_v) == 0) {
			cl_cluster_node_dun(cn, DUN_NO_SOCKADDR);
			return false;
		}

		struct sockaddr_in sa_in;

		cf_vector_get(&cn->sockaddr_in_v, 0, &sa_in);
		cn->info_fd = cf_socket_create_and_connect_nb(&sa_in);

		if (cn->info_fd == -1) {
			// TODO - loop over all sockaddrs?
			cl_cluster_node_dun(cn, DUN_INFO_CONNECT_FAIL);
			return false;
		}
	}

	return true;
}

static void
node_info_req_start(cl_cluster_node* cn, node_info_req_type req_type)
{
	if (! node_info_req_prep_fd(cn)) {
		cf_info("node %s couldn't open fd for info request", cn->name);
		return;
	}

	cn->info_req.wbuf_size = sizeof(cl_proto);

	const char* names;
	size_t names_len;

	switch (req_type) {
	case INFO_REQ_CHECK:
		names = INFO_STR_CHECK;
		names_len = sizeof(INFO_STR_CHECK) - 1;
		break;
	case INFO_REQ_GET_REPLICAS:
		names = INFO_STR_GET_REPLICAS;
		names_len = sizeof(INFO_STR_GET_REPLICAS) - 1;
		break;
	default:
		// Since we can't assert:
		cf_error("node %s info request invalid type %d", cn->name, req_type);
		return;
	}

	cn->info_req.wbuf_size += names_len;
	cn->info_req.wbuf = (uint8_t*)malloc(cn->info_req.wbuf_size);

	if (! cn->info_req.wbuf) {
		cf_error("node %s info request wbuf allocation failed", cn->name);
		return;
	}

	cl_proto* proto = (cl_proto*)cn->info_req.wbuf;

	proto->sz = cn->info_req.wbuf_size - sizeof(cl_proto);
	proto->version = CL_PROTO_VERSION;
	proto->type = CL_PROTO_TYPE_INFO;
	cl_proto_swap(proto);

	strncpy((char*)(cn->info_req.wbuf + sizeof(cl_proto)), names, names_len);

	event_assign(cluster_node_get_info_event(cn), cn->asc->base,
			cn->info_fd, EV_WRITE, node_info_req_event, cn);

	if (0 != event_add(cluster_node_get_info_event(cn), 0)) {
		cf_error("node %s info request add event failed", cn->name);
	}
	else {
		cn->info_req.type = req_type;
		cl_cluster_node_reserve(cn, "I+");
	}
}

// The libevent2 event handler for node periodic timer events:
void
node_timer_fn(evutil_socket_t fd, short event, void* udata)
{
	cl_cluster_node* cn = (cl_cluster_node*)udata;

	if (cn->MAGIC != CLUSTER_NODE_MAGIC) {
		// Serious - can't do anything else with cn, including dunning node.
		cf_error("node timer event found bad node magic");
		return;
	}

	uint64_t _s = cf_getms();
	bool is_dunned = cf_atomic_int_get(cn->dunned) != 0;

	cf_debug("node %s timer event:%s references %d", cn->name,
			is_dunned ? " is dunned," : "", cf_client_rc_count(cn));

	if (is_dunned) {
		ev2citrusleaf_cluster* asc = cn->asc;

		cf_info("node %s fully dunned, remove from cluster %p", cn->name, asc);

		// Release references in the partition table.
		cl_partition_table_remove_node(asc, cn);

		// Remove this node object from the cluster list, if there.
		bool deleted = false;

		MUTEX_LOCK(asc->node_v_lock);

		for (uint32_t i = 0; i < cf_vector_size(&asc->node_v); i++) {
			if (cn == (cl_cluster_node*)cf_vector_pointer_get(&asc->node_v, i)) {
				cf_vector_delete(&asc->node_v, i);
				deleted = true;
				break;
			}
		}

		MUTEX_UNLOCK(asc->node_v_lock);

		// Release cluster's reference, if there was one.
		if (deleted) {
			cl_cluster_node_release(cn, "C-");
		}

		// Release periodic timer reference.
		cl_cluster_node_release(cn, "L-");

		uint64_t delta = cf_getms() - _s;

		if (delta > CL_LOG_DELAY_INFO) {
			cf_info("CL_DELAY: node dunned: %lu", delta);
		}

		// Stops the periodic timer.
		return;
	}

	if (cn->info_req.type != INFO_REQ_NONE) {
		// There's still a node info request in progress. If it's taking too
		// long, cancel it and start over.

		// TODO - more complex logic to decide whether to cancel or let it ride.

		if (++cn->info_req.intervals >= NODE_INFO_REQ_MAX_INTERVALS) {
			cf_debug("canceling node %s info request after %u sec", cn->name,
					cn->info_req.intervals);

			node_info_req_type type = cn->info_req.type;

			node_info_req_timeout(cn);
			node_info_req_start(cn, type);
		}
		else {
			cf_debug("node %s info request incomplete after %u sec", cn->name,
					cn->info_req.intervals);
		}
	}

	if (cn->info_req.type == INFO_REQ_NONE) {
		node_info_req_start(cn, INFO_REQ_CHECK);
	}

	if (0 != event_add(cluster_node_get_timer_event(cn), &g_node_tend_timeout)) {
		// Serious - stops periodic timer! TODO - dun node?
		cf_error("node %s timer event add failed", cn->name);
	}

	uint64_t delta = cf_getms() - _s;

	if (delta > CL_LOG_DELAY_INFO) {
		cf_info("CL_DELAY: node timer: %lu", delta);
	}
}

//
// END - Periodic node timer functionality.
//==========================================================


cl_cluster_node*
cl_cluster_node_create(const char* name, ev2citrusleaf_cluster* asc)
{
	cf_info("cl_cluster: creating node, name %s, cluster %p", name, asc);

	// Allocate object (including space for events) and zero everything.
	cl_cluster_node* cn = cluster_node_create();

	if (! cn) {
		cf_warn("node %s can't allocate node object", name);
		return NULL;
	}

	cf_atomic_int_incr(&g_cl_stats.nodes_created);

	// To balance the ref-count logs, we need this:
	cf_debug("node reserve: %s %s %p : %d", "O+", name, cn, cf_client_rc_count(cn));

	cn->MAGIC = CLUSTER_NODE_MAGIC;
	strcpy(cn->name, name);
	cf_vector_init(&cn->sockaddr_in_v, sizeof(struct sockaddr_in), 5, VECTOR_FLAG_BIGLOCK);
	cn->asc = asc;
	cn->conn_q = cf_queue_create(sizeof(int), true);

	if (! cn->conn_q) {
		cf_warn("node %s can't create file descriptor queue", name);
		cl_cluster_node_release(cn, "O-");
		return NULL;
	}

	cn->partition_generation = (cf_atomic_int_t)-1;
	cn->info_fd = -1;

	// Start node's periodic timer.
	cl_cluster_node_reserve(cn, "L+");
	evtimer_assign(cluster_node_get_timer_event(cn), asc->base, node_timer_fn, cn);

	if (0 != event_add(cluster_node_get_timer_event(cn), &g_node_tend_timeout)) {
		cf_warn("node %s can't add periodic timer", name);
		cl_cluster_node_release(cn, "L-");
		cl_cluster_node_release(cn, "O-");
		return NULL;
	}

	// Add node to cluster.
	cl_cluster_node_reserve(cn, "C+");
	MUTEX_LOCK(asc->node_v_lock);
	cf_vector_pointer_append(&asc->node_v, cn);
	MUTEX_UNLOCK(asc->node_v_lock);

	// At this point we have "L" and "C" references, don't need "O" any more.
	cl_cluster_node_release(cn, "O-");

	return cn;
}

void
cl_cluster_node_release(cl_cluster_node *cn, char *msg)
{
	// msg key:
	// O:  original alloc
	// L:  node timer loop
	// C:  cluster node list
	// I:  node info request
	// PR: partition table, read
	// PW: partition table, write
	// T:  transaction

	cf_debug("node release: %s %s %p : %d", msg, cn->name, cn, cf_client_rc_count(cn));

	if (0 == cf_client_rc_release(cn)) {
		cf_info("************* cluster node destroy: node %s : %p", cn->name, cn);

		cf_atomic_int_incr(&g_cl_stats.nodes_destroyed);

		// Note - if we call event_del() before assigning the event (possible
		// within a second of startup) the libevent library logs the following:
		//
		// [warn] event_del: event has no event_base set.
		//
		// For now I'm not bothering with flags to avoid this.

		event_del(cluster_node_get_info_event(cn));
		event_del(cluster_node_get_timer_event(cn));

		if (cn->info_fd != -1) {
			cf_close(cn->info_fd);
		}

		node_info_req_free(&cn->info_req);

		if (cn->conn_q) {
			int fd;

			while (cf_queue_pop(cn->conn_q, &fd, CF_QUEUE_NOWAIT) == CF_QUEUE_OK) {
				cf_atomic_int_incr(&g_cl_stats.conns_destroyed);
				shutdown(fd, SHUT_RDWR); // be good to remote end - worried this might block though? // TODO - really?
				cf_close(fd);
			}

			cf_queue_destroy(cn->conn_q);
		}

		cf_vector_destroy(&cn->sockaddr_in_v);

		// Be safe and destroy the magic.
		memset((void*)cn, 0xff, sizeof(cl_cluster_node));

		cf_client_rc_free(cn);
	}
}

void
cl_cluster_node_reserve(cl_cluster_node *cn, char *msg)
{
	// msg key:
	// O:  original alloc
	// L:  node timer loop
	// C:  cluster node list
	// I:  node info request
	// PR: partition table, read
	// PW: partition table, write
	// T:  transaction

	cf_debug("node reserve: %s %s %p : %d", msg, cn->name, cn, cf_client_rc_count(cn));

	cf_client_rc_reserve(cn);
}


//
// Get a likely-healthy node for communication
// 

cl_cluster_node *
cl_cluster_node_get_random(ev2citrusleaf_cluster *asc)
{
	
	cl_cluster_node *cn = 0;
	uint32_t i = 0;
	uint32_t node_v_sz = 0;

	do {
		// get a node from the node list round-robin
		MUTEX_LOCK(asc->node_v_lock);

		node_v_sz = cf_vector_size(&asc->node_v);
		if (node_v_sz == 0) {
	
			MUTEX_UNLOCK(asc->node_v_lock);	
			cf_debug("cluster node get: no nodes in this cluster");
			return(0);
		}

		uint32_t node_i = (uint32_t)cf_atomic_int_incr(&asc->last_node);
		if (node_i >= node_v_sz) {
			node_i = 0;
			cf_atomic_int_set(&asc->last_node, 0);
		}
		
		cn = (cl_cluster_node*)cf_vector_pointer_get(&asc->node_v, node_i);
		i++;
		
		if (cn->MAGIC != CLUSTER_NODE_MAGIC) {
			MUTEX_UNLOCK(asc->node_v_lock);	
			cf_error("cluster node get random: bad magic in node %x", cn->MAGIC);
			return(0);
		}

		if (cf_atomic_int_get(cn->dunned)) {
//			cf_debug("dunned node %s in random list!", cn->name);
			cn = 0;
		}

		if (cn) {
			cl_cluster_node_reserve(cn, "T+");
		}

		MUTEX_UNLOCK(asc->node_v_lock);

	} while( cn == 0 && i < node_v_sz );

	return(cn);
}

cl_cluster_node *
cl_cluster_node_get(ev2citrusleaf_cluster *asc, const char *ns, const cf_digest *d, bool write)
{
	cl_cluster_node *cn = 0;
	
	if (asc->n_partitions) {
		// first, try to get one that matches this digest
		cn = cl_partition_table_get(asc, ns, cl_partition_getid(asc->n_partitions, d) , write);
		if (cn) {
			if (cn->MAGIC != CLUSTER_NODE_MAGIC) {
				// this is happening. when it happens, clear out this pointer for safety.
				// more importantly, fix the bug!
				cf_error("cluster node get: got node with bad magic %x (%p), abort", cn->MAGIC, cn);
				cl_cluster_node_release(cn, "bang"); // unclear this is safe
				cl_partition_table_remove_node(asc,cn);
				cn = 0;
				// raise(SIGINT);
			}
			else if (cf_atomic_int_get(cn->dunned)) { 
				cl_cluster_node_release(cn, "T-");
				cn = 0;
			}
		}
	}
	
	if (!cn) cn = cl_cluster_node_get_random(asc);

	return( cn );
}

cl_cluster_node *
cl_cluster_node_get_byname(ev2citrusleaf_cluster *asc, char *name)
{
	MUTEX_LOCK(asc->node_v_lock);
	for (uint32_t i=0;i<cf_vector_size(&asc->node_v);i++) {
		cl_cluster_node *node = (cl_cluster_node*)cf_vector_pointer_get(&asc->node_v, i);
		if (strcmp(name, node->name) == 0) {
			MUTEX_UNLOCK(asc->node_v_lock);
			return(node);
		}
	}
	MUTEX_UNLOCK(asc->node_v_lock);
	return(0);
	
}

// Put the node back, whatever that means (release the reference count?)

void
cl_cluster_node_put(cl_cluster_node *cn)
{
	cl_cluster_node_release(cn, "T-");
}


// MUST be in sync with cl_cluster_dun_type enum:
const char* cl_cluster_dun_human[] = {
		"bad name",
		"no sockaddr",
		"connect fail",
		"restart fd",
		"network error",
		"user timeout",
		"info connect fail",
		"info restart fd",
		"info network error",
		"info timeout",
};

void
cl_cluster_node_dun(cl_cluster_node *cn, cl_cluster_dun_type type)
{
	if (cn->MAGIC != CLUSTER_NODE_MAGIC) {
		cf_error("attempt to dun node without magic. Fail");
		return;
	}
	
	int dun_factor;
	switch (type) {
		case DUN_USER_TIMEOUT:
			if (cf_atomic_int_get(cn->dun_count) % 50 == 0) {
				cf_debug("dun node: %s reason: %s count: %d",
						cn->name, cl_cluster_dun_human[type], cf_atomic_int_get(cn->dun_count));
			}
			dun_factor = 1;
			break;
		case DUN_INFO_TIMEOUT:
			cf_info("dun node: %s reason: %s count: %d",
					cn->name, cl_cluster_dun_human[type], cf_atomic_int_get(cn->dun_count));
			dun_factor = 20;
			break;
		case DUN_CONNECT_FAIL:
		case DUN_RESTART_FD:
		case DUN_NETWORK_ERROR:
		case DUN_INFO_CONNECT_FAIL:
		case DUN_INFO_RESTART_FD:
		case DUN_INFO_NETWORK_ERROR:
			cf_info("dun node: %s reason: %s count: %d",
					cn->name, cl_cluster_dun_human[type], cf_atomic_int_get(cn->dun_count));
			dun_factor = 50;
			break;
		case DUN_BAD_NAME:
		case DUN_NO_SOCKADDR:
			cf_info("dun node: %s reason: %s count: %d",
					cn->name, cl_cluster_dun_human[type], cf_atomic_int_get(cn->dun_count));
			dun_factor = 1000;
			break;
		default:
			cf_error("dun node: %s UNKNOWN REASON count: %d",
					cn->name, cf_atomic_int_get(cn->dun_count));
			dun_factor = 1;
			break;
	}

	int dun_count = (int)cf_atomic_int_add(&cn->dun_count, dun_factor);

	if (dun_count > CL_NODE_DUN_THRESHOLD) {
		cf_info("dun node: node %s fully dunned %d", cn->name, dun_count);

		cf_atomic_int_set(&cn->dunned, 1);
	}
}

void
cl_cluster_node_ok(cl_cluster_node *cn)
{
	if (cn->MAGIC != CLUSTER_NODE_MAGIC) {
		cf_error("ok node but no magic, fail");
		return;
	}

	int dun_count = cf_atomic_int_get(cn->dun_count);

	// TODO - extra dunned state for removed from cluster?
	if (cf_atomic_int_get(cn->dunned) == 1) {
		cf_info("ok node: %s had dun_count %d", cn->name, dun_count);
	}
	else if (dun_count > 0) {
		cf_debug("ok node: %s had dun_count %d", cn->name, dun_count);
	}

	cf_atomic_int_set(&cn->dun_count, 0);
	cf_atomic_int_set(&cn->dunned, 0);
}

// Return values:
// -1 try again right away
// -2 don't try again right away
int
cl_cluster_node_fd_get(cl_cluster_node *cn)
{
	int fd;
	int rv = cf_queue_pop(cn->conn_q, &fd, CF_QUEUE_NOWAIT);

	if (rv == CF_QUEUE_OK) {
		// Check to see if existing fd is still connected.
		int rv2 = ev2citrusleaf_is_connected(fd);

		switch (rv2) {
			case CONNECTED:
				// It's still good.
				return fd;
			case CONNECTED_NOT:
				// Can't use it - the remote end closed it.
				// TODO - dun?
				cf_atomic_int_incr(&g_cl_stats.conns_destroyed);
				cf_atomic_int_incr(&g_cl_stats.conns_destroyed_queue);
				cf_close(fd);
				return -1;
			case CONNECTED_ERROR:
				// Some other problem, could have to do with remote end.
				cl_cluster_node_dun(cn, DUN_RESTART_FD);
				cf_atomic_int_incr(&g_cl_stats.conns_destroyed);
				cf_atomic_int_incr(&g_cl_stats.conns_destroyed_queue);
				cf_close(fd);
				return -1;
			case CONNECTED_BADFD:
				// Local problem, don't dun, don't try closing.
				cf_warn("bad file descriptor in queue: fd %d", fd);
				return -1;
			default:
				// Since we can't assert:
				cf_error("bad return value from ev2citrusleaf_is_connected");
				cf_atomic_int_incr(&g_cl_stats.conns_destroyed);
				cf_atomic_int_incr(&g_cl_stats.conns_destroyed_queue);
				cf_close(fd);
				return -2;
		}
	}
	else if (rv != CF_QUEUE_EMPTY) {
		// Since we can't assert:
		cf_error("bad return value from cf_queue_pop");
		return -2;
	}

	// Queue was empty, open a new socket and (start) connect.

	if (cf_vector_size(&cn->sockaddr_in_v) == 0) {
		cl_cluster_node_dun(cn, DUN_NO_SOCKADDR);
		return -2;
	}

	if (-1 == (fd = cf_socket_create_nb())) {
		// Local problem, don't dun.
		return -2;
	}

	cf_debug("new socket: fd %d node %s", fd, cn->name);

	// Try socket addresses until we connect.
	for (uint32_t i = 0; i < cf_vector_size(&cn->sockaddr_in_v); i++) {
		struct sockaddr_in sa_in;

		cf_vector_get(&cn->sockaddr_in_v, i, &sa_in);

		if (0 == cf_socket_start_connect_nb(fd, &sa_in)) {
			cf_atomic_int_incr(&g_cl_stats.conns_connected);
			return fd;
		}
		// TODO - else remove this sockaddr from the list, or dun the node?
	}

	cl_cluster_node_dun(cn, DUN_CONNECT_FAIL);
	cf_close(fd);

	return -2;
}

void
cl_cluster_node_fd_put(cl_cluster_node *cn, int fd)
{
	if (! cf_queue_push_limit(cn->conn_q, &fd, 300)) {
		cf_atomic_int_incr(&g_cl_stats.conns_destroyed);
		cf_close(fd);
	}
}

//
// Debug function. Should be elsewhere.
//

void
sockaddr_in_dump(char *prefix, struct sockaddr_in *sa_in)
{
	char str[INET_ADDRSTRLEN];
	inet_ntop(AF_INET, &(sa_in->sin_addr), str, INET_ADDRSTRLEN);	
	cf_info("%s %s:%d", prefix, str, (int)ntohs(sa_in->sin_port));
}

void
cluster_dump(ev2citrusleaf_cluster *asc)
{
	if (! cf_debug_enabled()) {
		return;
	}

	cf_debug("=*=*= cluster %p dump =*=*=", asc);
	
	cf_debug("registered hosts:");
	for (uint32_t i=0;i<cf_vector_size(&asc->host_str_v);i++) {
		char *host_s = (char*)cf_vector_pointer_get(&asc->host_str_v,i);
		int   port = cf_vector_integer_get(&asc->host_port_v,i);
		cf_debug(" host %d: %s:%d", i, host_s, port);
	}
	
	MUTEX_LOCK(asc->node_v_lock);
	cf_debug("nodes: %u", cf_vector_size(&asc->node_v));
	for (uint32_t i=0;i<cf_vector_size(&asc->node_v);i++) {
		cl_cluster_node *cn = (cl_cluster_node*)cf_vector_pointer_get(&asc->node_v, i);
		struct sockaddr_in sa_in;
		cf_vector_get(&cn->sockaddr_in_v, 0, &sa_in);
		char str[INET_ADDRSTRLEN];
		inet_ntop(AF_INET, &(sa_in.sin_addr), str, INET_ADDRSTRLEN);
		cf_debug(" %d %s : %s:%d (%d conns)", i, cn->name, str,
			(int)ntohs(sa_in.sin_port), cf_queue_sz(cn->conn_q));
	}
	MUTEX_UNLOCK(asc->node_v_lock);
	
	cf_debug("=*=*= cluster %p end dump =*=*=", asc);
}



typedef struct ping_node_data_s {
	struct sockaddr_in	sa_in;
	ev2citrusleaf_cluster *asc;
} ping_nodes_data;


//
// per-node 'node' request comes back here - we now know the name associated with this sockaddr
// Check to see whether this node is new or taken, and create new 
//
// Early on, the request also gets the number of partitions
//
// The PND was alloc'd must be freed

static void
cluster_ping_node_fn(int return_value, char *values, size_t values_len, void *udata)
{
	ping_nodes_data *pnd = (ping_nodes_data *)udata;
	
	cf_atomic_int_decr(&pnd->asc->pings_in_progress);
	
	if (pnd->asc->shutdown)
	
	cf_info("ping node fn: rv %d node value retrieved: %s", return_value, values);
	
	if ((return_value != 0) || (pnd->asc->shutdown == true)) {
		cf_info("ping node function: error on return %d", return_value);
		if (values) free(values);
		// BFIX - need to free the data here, otherwise LEAK
		free(udata);
		return;
	}

	ev2citrusleaf_cluster *asc = pnd->asc;
	
	cf_vector_define(lines_v, sizeof(void *), 0);
	str_split('\n',values,&lines_v);
	for (uint32_t i=0;i<cf_vector_size(&lines_v);i++) {
		char *line = (char*)cf_vector_pointer_get(&lines_v, i);
		cf_vector_define(pair_v, sizeof(void *), 0);
		str_split('\t',line, &pair_v);
		
		if (cf_vector_size(&pair_v) == 2) {
			char *name = (char*)cf_vector_pointer_get(&pair_v, 0);
			char *value = (char*)cf_vector_pointer_get(&pair_v, 1);
			
			if (strcmp(name, "node") == 0) {

				// make sure this host already exists, create & add if not
				cl_cluster_node *cn = cl_cluster_node_get_byname(asc, value);
				if (!cn) {
					cn = cl_cluster_node_create(value /*nodename*/, asc);
				}

				if (cn) {
					// add this address to node list
					cf_vector_append_unique(&cn->sockaddr_in_v,&pnd->sa_in);
				}
			}
			else if (strcmp(name, "partitions")==0) {
				asc->n_partitions = atoi(value);
			}
		}
		cf_vector_destroy(&pair_v);
	}
	cf_vector_destroy(&lines_v);
	
	if (values) free(values);
	free(pnd);
	pnd = 0;
	
	// if the cluster had waiting requests, try to restart
	MUTEX_LOCK(asc->node_v_lock);
	int sz = cf_vector_size(&asc->node_v);
	MUTEX_UNLOCK(asc->node_v_lock);
	if (sz != 0) {
		cl_request *req;
		MUTEX_LOCK(asc->request_q_lock);
		while (CF_QUEUE_OK == cf_queue_pop(asc->request_q, (void *)&req,0)) {
			ev2citrusleaf_base_hop(req);
		}
		MUTEX_UNLOCK(asc->request_q_lock);
	}
}


//
// This function is called when we complete a resolution
// on a name added by the user. We'll have a list of sockaddr_in that we probably already
// know about. Calls the function that checks uniqueness and starts a 'ping' to get
// the nodename
//

void
cluster_tend_hostname_resolve(int result, cf_vector *sockaddr_v, void *udata  )
{
	ev2citrusleaf_cluster *asc = (ev2citrusleaf_cluster *)udata;

	cf_info("cluster tend host resolve");

	if ((result == 0) && (sockaddr_v)) {
		for (uint32_t i=0;i<cf_vector_size(sockaddr_v);i++) {
			struct sockaddr_in sin;
			cf_vector_get(sockaddr_v, i, &sin);
			cluster_new_sockaddr(asc, &sin);
		}
	}
}

//
// Call this routine whenever you've discovered a new sockaddr.
// Maybe we already know about it, maybe we don't - this routine will
// 'debounce' efficiently and launch an 'add' cycle if it appears new.
//

void
cluster_new_sockaddr(ev2citrusleaf_cluster *asc, struct sockaddr_in *new_sin)
{
	if (asc->shutdown == true)	return;
	
	// Lookup the sockaddr in the node list. This is inefficient, but works
	// Improve later if problem...
	
	cf_vector *node_v = &asc->node_v;
	MUTEX_LOCK(asc->node_v_lock);
	for (uint32_t j=0;j<cf_vector_size(node_v);j++) {
		cl_cluster_node *cn = (cl_cluster_node*)cf_vector_pointer_get(node_v,j);
		for (uint32_t k=0;k<cf_vector_size(&cn->sockaddr_in_v);k++) {
			struct sockaddr_in sin;
			cf_vector_get(&cn->sockaddr_in_v, k, &sin);

			if (memcmp(&sin, new_sin, sizeof(struct sockaddr_in)) == 0) {
				// it's old - get out
				MUTEX_UNLOCK(asc->node_v_lock);
				return;
			}
		}
	}
	MUTEX_UNLOCK(asc->node_v_lock);
	
	// have new never-pinged hosts. Do the info_host call to get its name
	// The callback will add the node if it's new
	if (cf_info_enabled()) {
		sockaddr_in_dump("new sockaddr found: ", new_sin);
	}

	ping_nodes_data *pnd = (ping_nodes_data*)malloc(sizeof(ping_nodes_data));
	if (!pnd)	return;
	pnd->sa_in = *new_sin;
	pnd->asc = asc;

	if (0 != ev2citrusleaf_info_host(asc->base,new_sin, asc->n_partitions == 0 ? "node\npartitions" : "node", 
						0, cluster_ping_node_fn, pnd)) {
		free(pnd);
	}
	else {
		cf_atomic_int_incr(&asc->pings_in_progress);
	}
}


void
cluster_tend( ev2citrusleaf_cluster *asc) 
{
	cf_debug("cluster tend: cluster %p", asc);

	cluster_dump(asc);

	// For all registered names --- kick off a resolver
	// to see if there are new IP addresses
	// this is kind of expensive, so might need to do it only rarely
	// because, realistically, it never changes. Only go searching for nodes
	// if there are no nodes in the cluster - we've fallen off the edge of the earth
	MUTEX_LOCK(asc->node_v_lock);
	int sz = cf_vector_size(&asc->node_v);
	MUTEX_UNLOCK(asc->node_v_lock);

	if (0 == sz) {
		cf_debug("no nodes remaining: lookup original hosts hoststr size %d");

		uint32_t n_hosts = cf_vector_size(&asc->host_str_v);
		for (uint32_t i=0;i<n_hosts;i++) {
			
			char *host_s = (char*)cf_vector_pointer_get(&asc->host_str_v, i);
			int  port = cf_vector_integer_get(&asc->host_port_v, i);
	
			cf_debug("lookup hosts: %s:%d", host_s, port);
	
			struct sockaddr_in sin;
			if (0 == cl_lookup_immediate(host_s, port, &sin)) {
				cluster_new_sockaddr(asc, &sin);
			}
			else {
				// BFIX - if this returns error, ???
				cl_lookup(	asc->dns_base,
							(char*)cf_vector_pointer_get(&asc->host_str_v, i),
							cf_vector_integer_get(&asc->host_port_v, i),
							cluster_tend_hostname_resolve, asc);
			}
		}
	}

	cf_debug("end tend");
}


//


//
// Initialize the thread that keeps track of the cluster
//
int citrusleaf_cluster_init()
{
	
	// I'm going to leave this linked list for the moment; it's good for debugging
	cf_ll_init(&cluster_ll, 0, false);
	
	return(0);	
}

//
// I actually don't think there will be a lot of shutdowns,
// but use this to remove all the clusters that might have been added
//
int citrusleaf_cluster_shutdown()
{

	cf_ll_element *e;
	while ((e = cf_ll_get_head(&cluster_ll))) {
		ev2citrusleaf_cluster *asc = (ev2citrusleaf_cluster *)e; 
		ev2citrusleaf_cluster_destroy(asc);
	}
	
	return(0);	
}

