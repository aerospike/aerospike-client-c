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
#include <event2/dns.h>
#include <event2/event.h>

#include <citrusleaf/cf_alloc.h>
#include <citrusleaf/cf_atomic.h>
#include <citrusleaf/cf_byte_order.h>
#include <citrusleaf/cf_client_rc.h>
#include <citrusleaf/cf_clock.h>
#include <citrusleaf/cf_digest.h>
#include <citrusleaf/cf_errno.h>
#include <citrusleaf/cf_ll.h>
#include <citrusleaf/cf_log_internal.h>
#include <citrusleaf/cf_proto.h>
#include <citrusleaf/cf_queue.h>
#include <citrusleaf/cf_socket.h>
#include <citrusleaf/cf_types.h>
#include <citrusleaf/cf_vector.h>

#include "citrusleaf_event2/ev2citrusleaf.h"
#include "citrusleaf_event2/ev2citrusleaf-internal.h"

#include "citrusleaf_event2/cl_cluster.h"


// Define to use the old info replicas protocol:
//#define OLD_REPLICAS_PROTOCOL

extern void ev2citrusleaf_base_hop(cl_request *req);

//
// Cumulative contiguous problem score above which the node is considered bad.
//
#define CL_NODE_DUN_THRESHOLD 800

//
// Intervals on which tending happens.
//
struct timeval g_cluster_tend_timeout = {1,200000};
struct timeval g_node_tend_timeout = {1,1};


// Forward references
void cluster_print_stats(ev2citrusleaf_cluster* asc);
void cluster_tend( ev2citrusleaf_cluster *asc);
void cluster_new_sockaddr(ev2citrusleaf_cluster *asc, struct sockaddr_in *new_sin);
int ev2citrusleaf_cluster_add_host_internal(ev2citrusleaf_cluster *asc, char *host_in, short port_in);


//
// Utility for splitting a null-terminated string into a vector of sub-strings.
// The vector will have pointers to all the (null-terminated) sub-strings.
// This modifies the input string by inserting nulls.
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
	MUTEX_ALLOC(asc->runtime_options.lock);
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
	MUTEX_FREE(asc->runtime_options.lock);
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


// List of all current clusters so the tender can maintain them
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

	if (++asc->tender_intervals % CL_LOG_STATS_INTERVAL == 0) {
		cl_partition_table_dump(asc);
		cluster_print_stats(asc);
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


const ev2citrusleaf_cluster_runtime_options DEFAULT_RUNTIME_OPTIONS =
{
	300,	// socket_pool_max
	false,	// read_master_only
	false,	// throttle_reads
	false,	// throttle_writes
	2,		// throttle_threshold_failure_pct
	15,		// throttle_window_seconds
	10		// throttle_factor
};

int
ev2citrusleaf_cluster_get_runtime_options(ev2citrusleaf_cluster* asc,
		ev2citrusleaf_cluster_runtime_options* opts)
{
	if (! (asc && opts)) {
		cf_error("ev2citrusleaf_cluster_get_runtime_options() - null param");
		return EV2CITRUSLEAF_FAIL_CLIENT_ERROR;
	}

	opts->socket_pool_max = cf_atomic32_get(asc->runtime_options.socket_pool_max);

	opts->read_master_only = cf_atomic32_get(asc->runtime_options.read_master_only) != 0;

	opts->throttle_reads = cf_atomic32_get(asc->runtime_options.throttle_reads) != 0;
	opts->throttle_writes = cf_atomic32_get(asc->runtime_options.throttle_writes) != 0;

	opts->throttle_threshold_failure_pct = asc->runtime_options.throttle_threshold_failure_pct;
	opts->throttle_window_seconds = asc->runtime_options.throttle_window_seconds;
	opts->throttle_factor = asc->runtime_options.throttle_factor;

	return EV2CITRUSLEAF_OK;
}

int
ev2citrusleaf_cluster_set_runtime_options(ev2citrusleaf_cluster* asc,
		const ev2citrusleaf_cluster_runtime_options* opts)
{
	if (! (asc && opts)) {
		cf_error("ev2citrusleaf_cluster_set_runtime_options() - null param");
		return EV2CITRUSLEAF_FAIL_CLIENT_ERROR;
	}

	// Really basic sanity checks.
	if (opts->throttle_threshold_failure_pct > 100 ||
		opts->throttle_window_seconds == 0 ||
		opts->throttle_window_seconds > MAX_THROTTLE_WINDOW) {
		cf_warn("ev2citrusleaf_cluster_set_runtime_options() - illegal option");
		return EV2CITRUSLEAF_FAIL_CLIENT_ERROR;
	}

	cf_atomic32_set(&asc->runtime_options.socket_pool_max, opts->socket_pool_max);

	cf_atomic32_set(&asc->runtime_options.read_master_only, opts->read_master_only ? 1 : 0);

	cf_atomic32_set(&asc->runtime_options.throttle_reads, opts->throttle_reads ? 1 : 0);
	cf_atomic32_set(&asc->runtime_options.throttle_writes, opts->throttle_writes ? 1 : 0);

	MUTEX_LOCK(asc->runtime_options.lock);

	asc->runtime_options.throttle_threshold_failure_pct = opts->throttle_threshold_failure_pct;
	asc->runtime_options.throttle_window_seconds = opts->throttle_window_seconds;
	asc->runtime_options.throttle_factor = opts->throttle_factor;

	MUTEX_UNLOCK(asc->runtime_options.lock);

	cf_info("set runtime options:");
	cf_info("   socket-pool-max %u", opts->socket_pool_max);
	cf_info("   read-master-only %s",
			opts->read_master_only ? "true" : "false");
	cf_info("   throttle-reads %s, writes %s",
			opts->throttle_reads ? "true" : "false",
			opts->throttle_writes ? "true" : "false");
	cf_info("   throttle-threshold-failure-pct %u, window-seconds %u, factor %u",
			opts->throttle_threshold_failure_pct,
			opts->throttle_window_seconds,
			opts->throttle_factor);

	return EV2CITRUSLEAF_OK;
}


ev2citrusleaf_cluster *
ev2citrusleaf_cluster_create(struct event_base *base,
		const ev2citrusleaf_cluster_static_options *opts)
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
		asc->static_options = *opts;
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

	ev2citrusleaf_cluster_set_runtime_options(asc, &DEFAULT_RUNTIME_OPTIONS);

	return(asc);
}


int
ev2citrusleaf_cluster_get_active_node_count(ev2citrusleaf_cluster* asc)
{
	if (! asc) {
		return EV2CITRUSLEAF_FAIL_CLIENT_ERROR;
	}

	if (asc->MAGIC != CLUSTER_MAGIC) {
		cf_error("cluster get_active_node_count on non-cluster object %p", asc);
		return EV2CITRUSLEAF_FAIL_CLIENT_ERROR;
	}

	MUTEX_LOCK(asc->node_v_lock);

	uint32_t n_nodes = cf_vector_size(&asc->node_v);
	uint32_t n_active_nodes = 0;

	for (uint32_t i = 0; i < n_nodes; i++) {
		cl_cluster_node* node = (cl_cluster_node*)
				cf_vector_pointer_get(&asc->node_v, i);

		if (node->MAGIC != CLUSTER_NODE_MAGIC) {
			cf_error("cluster node %u has bad magic", i);
			continue;
		}

		if (node->name[0] == 0) {
			cf_warn("cluster node %u has no name", i);
			continue;
		}

		if (cf_vector_size(&node->sockaddr_in_v) == 0) {
			cf_warn("cluster node %s (%u) has no address", node->name, i);
			continue;
		}

		n_active_nodes++;
	}

	MUTEX_UNLOCK(asc->node_v_lock);

	cf_info("cluster has %u nodes, %u ok", n_nodes, n_active_nodes);

	return n_active_nodes;
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

void node_info_req_cancel(cl_cluster_node* cn);

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
		node_info_req_cancel(cn);
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

// INFO_STR_MAX_LEN must be >= longest of these strings.
const char INFO_STR_CHECK[] = "node\npartition-generation\nservices\n";
#ifdef OLD_REPLICAS_PROTOCOL
const char INFO_STR_GET_REPLICAS[] = "partition-generation\nreplicas-read\nreplicas-write\n";
#else // OLD_REPLICAS_PROTOCOL
const char INFO_STR_GET_REPLICAS[] = "partition-generation\nreplicas-master\nreplicas-prole\n";
#endif // OLD_REPLICAS_PROTOCOL

void node_info_req_start(cl_cluster_node* cn, node_info_req_type req_type);
// The libevent2 event handler for node info socket events:
void node_info_req_event(evutil_socket_t fd, short event, void* udata);

void
node_info_req_free(node_info_req* ir)
{
	if (ir->rbuf) {
		free(ir->rbuf);
	}

	// Includes setting type to INFO_REQ_NONE.
	memset((void*)ir, 0, sizeof(node_info_req));
}

void
node_info_req_done(cl_cluster_node* cn)
{
	// Success - reuse the socket and approve the node.
	cl_cluster_node_had_success(cn);

	node_info_req_free(&cn->info_req);
	cf_atomic_int_incr(&cn->asc->n_node_info_successes);
}

void
node_info_req_fail(cl_cluster_node* cn, bool remote_failure)
{
	// The socket may have unprocessed data or otherwise be untrustworthy.
	cf_close(cn->info_fd);
	cn->info_fd = -1;
	cf_atomic32_decr(&cn->n_fds_open);

	// If the failure was possibly the server node's fault, disapprove.
	if (remote_failure) {
		cl_cluster_node_had_failure(cn);
	}

	node_info_req_free(&cn->info_req);
	cf_atomic_int_incr(&cn->asc->n_node_info_failures);
}

void
node_info_req_cancel(cl_cluster_node* cn)
{
	if (cn->info_req.type != INFO_REQ_NONE) {
		event_del(cluster_node_get_info_event(cn));
		node_info_req_free(&cn->info_req);
	}

	if (cn->info_fd != -1) {
		cf_close(cn->info_fd);
		cn->info_fd = -1;
		cf_atomic32_decr(&cn->n_fds_open);
	}
}

void
node_info_req_timeout(cl_cluster_node* cn)
{
	event_del(cluster_node_get_info_event(cn));
	node_info_req_fail(cn, true);
	cf_atomic_int_incr(&cn->asc->n_node_info_timeouts);
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

// TODO - should probably move base 64 stuff to cf_base so C client can use it.
const uint8_t EV2_CF_BASE64_DECODE_ARRAY[] = {
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

#define B64DA EV2_CF_BASE64_DECODE_ARRAY

void
ev2_cf_base64_decode(const char* in, int len, uint8_t* out)
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

	ev2_cf_base64_decode(p_encoded_bitmap, encoded_bitmap_len, bitmap);

	// Then expand the bitmap into our bool array.
	for (int i = 0; i < n_partitions; i++) {
		if ((bitmap[i >> 3] & (0x80 >> (i & 7))) != 0) {
			p_map->owns[i] = true;
		}
	}
}

// Parse the old protocol (to be deprecated):
void
parse_replicas_list(char* list, int n_partitions, cf_vector* p_maps_v)
{
	uint64_t _s = cf_getms();

	// Format: <namespace1>:<partition id1>;<namespace2>:<partition id2>; ...
	// Warning: This method walks on partitions string argument.
	char* p = list;

	while (*p) {
		char* list_ns = p;

		// Loop until : and set it to null.
		while (*p && *p != ':') {
			p++;
		}

		if (*p == ':') {
			*p++ = 0;
		}
		else {
			cf_warn("ns %s has no pid", list_ns);
			break;
		}

		char* list_pid = p;

		// Loop until ; and set it to null.
		while (*p && *p != ';') {
			p++;
		}

		if (*p == ';') {
			*p++ = 0;
		}

		char* ns = trim(list_ns);
		size_t len = strlen(ns);

		if (len == 0 || len > 31) {
			cf_warn("invalid partition namespace %s", ns);
			continue;
		}

		int pid = atoi(list_pid);

		if (pid < 0 || pid >= n_partitions) {
			cf_warn("invalid pid %s", list_pid);
			continue;
		}

		ns_partition_map* p_map =
				ns_partition_map_get(p_maps_v, ns, n_partitions);

		if (p_map) {
			p_map->owns[pid] = true;
		}
	}

	uint64_t delta = cf_getms() - _s;

	if (delta > CL_LOG_DELAY_INFO) {
		cf_info("CL_DELAY: partition process: %lu", delta);
	}
}

// Parse the new protocol:
void
parse_replicas_map(char* list, int n_partitions, cf_vector* p_maps_v)
{
	uint64_t _s = cf_getms();

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

	uint64_t delta = cf_getms() - _s;

	if (delta > CL_LOG_DELAY_INFO) {
		cf_info("CL_DELAY: partition process: %lu", delta);
	}
}

void
node_info_req_parse_replicas(cl_cluster_node* cn)
{
	cf_vector_define(read_maps_v, sizeof(ns_partition_map*), 0);
	cf_vector_define(write_maps_v, sizeof(ns_partition_map*), 0);

	// Returned list format is name1\tvalue1\nname2\tvalue2\n...
	cf_vector_define(lines_v, sizeof(void*), 0);
	str_split('\n', (char*)cn->info_req.rbuf, &lines_v);

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

			cf_info("node %s got partition generation %d", cn->name, gen);
		}
		// Old protocol (to be deprecated):
		else if (strcmp(name, "replicas-read") == 0) {
			// Parse the read replicas.
			parse_replicas_list(value, cn->asc->n_partitions, &read_maps_v);
		}
		else if (strcmp(name, "replicas-write") == 0) {
			// Parse the write replicas.
			parse_replicas_list(value, cn->asc->n_partitions, &write_maps_v);
		}
		// New protocol:
		else if (strcmp(name, "replicas-master") == 0) {
			// Parse the new-format master replicas.
			parse_replicas_map(value, cn->asc->n_partitions, &write_maps_v);
		}
		else if (strcmp(name, "replicas-prole") == 0) {
			// Parse the new-format prole replicas.
			parse_replicas_map(value, cn->asc->n_partitions, &read_maps_v);
		}
		else {
			cf_warn("node %s info replicas did not request %s", cn->name, name);
		}

		cf_vector_destroy(&pair_v);
	}

	cf_vector_destroy(&lines_v);

	// Apply write and read replica maps as masters and proles. For the existing
	// protocol, the read replicas will have proles and masters, but the update
	// function will process write replicas (masters) first and ignore the read
	// replicas' redundant masters.
	//
	// For both old and new protocol, p_read_map will not be null in the single
	// node case. We also assume it's impossible for a node to have no masters.

	uint32_t n_write_maps = cf_vector_size(&write_maps_v);

	for (uint32_t i = 0; i < n_write_maps; i++) {
		ns_partition_map* p_write_map = (ns_partition_map*)
				cf_vector_pointer_get(&write_maps_v, i);
		ns_partition_map* p_read_map = ns_partition_map_get(&read_maps_v,
				p_write_map->ns, cn->asc->n_partitions);

		cl_partition_table_update(cn, p_write_map->ns, p_write_map->owns,
				p_read_map->owns);
	}

	ns_partition_map_destroy(&write_maps_v);
	ns_partition_map_destroy(&read_maps_v);

	cf_vector_destroy(&write_maps_v);
	cf_vector_destroy(&read_maps_v);

	node_info_req_done(cn);
}

void
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
				node_info_req_fail(cn, true);
				return;
			}
		}
		else if (strcmp(name, "partition-generation") == 0) {
			int client_gen = (int)cf_atomic_int_get(cn->partition_generation);
			int server_gen = atoi(value);

			// If generations don't match, flag for replicas request.
			if (client_gen != server_gen) {
				get_replicas = true;

				cf_info("node %s partition generation %d needs update to %d",
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
		node_info_req_start(cn, INFO_REQ_GET_REPLICAS);
	}
}

bool
node_info_req_handle_send(cl_cluster_node* cn)
{
	node_info_req* ir = &cn->info_req;

	while(true) {
		// Loop until everything is sent or we get would-block.

		if (ir->wbuf_pos >= ir->wbuf_size) {
			cf_error("unexpected write event");
			node_info_req_fail(cn, false);
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
			node_info_req_fail(cn, true);
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

bool
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
				node_info_req_fail(cn, true);
				return true;
			}
			else if (errno != EAGAIN && errno != EWOULDBLOCK) {
				cf_debug("recv failed: rv %d errno %d", rv, errno);
				node_info_req_fail(cn, true);
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
				ir->rbuf = (uint8_t*)malloc(ir->rbuf_size + 1);

				if (! ir->rbuf) {
					cf_error("node info request rbuf allocation failed");
					node_info_req_fail(cn, false);
					return true;
				}

				// Null-terminate this buffer for easier text parsing.
				ir->rbuf[ir->rbuf_size] = 0;
			}

			if (ir->rbuf_pos >= ir->rbuf_size) {
				cf_error("unexpected read event");
				node_info_req_fail(cn, false);
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
						node_info_req_fail(cn, false);
						break;
					}

					return true;
				}

				// Loop, read more body.
			}
			else if (rv == 0) {
				// Connection has been closed by the server.
				cf_debug("recv connection closed: fd %d", cn->info_fd);
				node_info_req_fail(cn, true);
				return true;
			}
			else if (errno != EAGAIN && errno != EWOULDBLOCK) {
				cf_debug("recv failed: rv %d errno %d", rv, errno);
				node_info_req_fail(cn, true);
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
void
node_info_req_event(evutil_socket_t fd, short event, void* udata)
{
	cl_cluster_node* cn = (cl_cluster_node*)udata;

	if (cn->MAGIC != CLUSTER_NODE_MAGIC) {
		// Serious - can't do anything else with cn, including removing node.
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
		node_info_req_fail(cn, false);
		return;
	}

	if (! transaction_done) {
		// There's more to do, re-add event.
		if (0 != event_add(cluster_node_get_info_event(cn), 0)) {
			cf_error("node info request add event failed");
			node_info_req_fail(cn, false);
		}
	}
}

bool
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
		case CONNECTED_ERROR:
			// Some other problem, could have to do with remote end.
			cf_close(cn->info_fd);
			cn->info_fd = -1;
			cf_atomic32_decr(&cn->n_fds_open);
			break;
		case CONNECTED_BADFD:
			// Local problem, don't try closing.
			cn->info_fd = -1;
			break;
		default:
			// Since we can't assert:
			cf_error("node %s info request connect state unknown", cn->name);
			cf_close(cn->info_fd);
			cn->info_fd = -1;
			cf_atomic32_decr(&cn->n_fds_open);
			return false;
		}
	}

	// Try to open a new socket. We'll count any failures here as transaction
	// failures even though we never really start the transaction.

	if (cf_vector_size(&cn->sockaddr_in_v) == 0) {
		cf_warn("node %s has no sockaddrs", cn->name);
		cl_cluster_node_had_failure(cn);
		return false;
	}

	struct sockaddr_in sa_in;

	cf_vector_get(&cn->sockaddr_in_v, 0, &sa_in);
	cn->info_fd = cf_socket_create_and_connect_nb(&sa_in);

	if (cn->info_fd == -1) {
		// TODO - loop over all sockaddrs?
		cl_cluster_node_had_failure(cn);
		return false;
	}

	cf_atomic32_incr(&cn->n_fds_open);

	return true;
}

void
node_info_req_start(cl_cluster_node* cn, node_info_req_type req_type)
{
	if (! node_info_req_prep_fd(cn)) {
		cf_info("node %s couldn't open fd for info request", cn->name);
		cf_atomic_int_incr(&cn->asc->n_node_info_failures);
		return;
	}

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

	cn->info_req.wbuf_size = sizeof(cl_proto) + names_len;

	cl_proto* proto = (cl_proto*)cn->info_req.wbuf;

	proto->sz = names_len;
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
	}
}

// TODO - add to runtime options?
#define MAX_THROTTLE_PCT 90

void
node_throttle_control(cl_cluster_node* cn)
{
	// Get the throttle control parameters.
	threadsafe_runtime_options* p_opts = &cn->asc->runtime_options;

	MUTEX_LOCK(p_opts->lock);

	uint32_t threshold_failure_pct = p_opts->throttle_threshold_failure_pct;
	uint32_t history_intervals_to_use = p_opts->throttle_window_seconds - 1;
	uint32_t throttle_factor = p_opts->throttle_factor;

	MUTEX_UNLOCK(p_opts->lock);

	// Collect and reset the latest counts. TODO - atomic get and clear?
	uint32_t new_successes = cf_atomic32_get(cn->n_successes);
	uint32_t new_failures = cf_atomic32_get(cn->n_failures);

	cf_atomic32_set(&cn->n_successes, 0);
	cf_atomic32_set(&cn->n_failures, 0);

	// Figure out where to start summing history, and if there's enough history
	// to base throttling on. (If not, calculate sums anyway for debug logging.)
	uint32_t start_interval = 0;
	bool enough_history = false;

	if (cn->current_interval >= history_intervals_to_use) {
		start_interval = cn->current_interval - history_intervals_to_use;
		enough_history = true;
	}

	// Calculate the sums.
	uint64_t successes_sum = new_successes;
	uint64_t failures_sum = new_failures;

	for (uint32_t i = start_interval; i < cn->current_interval; i++) {
		uint32_t index = i % MAX_HISTORY_INTERVALS;

		successes_sum += cn->successes[index];
		failures_sum += cn->failures[index];
	}

	// Update the history. Keep max history in case runtime options change.
	uint32_t current_index = cn->current_interval % MAX_HISTORY_INTERVALS;

	cn->successes[current_index] = new_successes;
	cn->failures[current_index] = new_failures;

	// So far we only use this for throttle control - increment it here.
	cn->current_interval++;

	// Calculate the failure percentage. Work in units of tenths-of-a-percent
	// for finer resolution of the resulting throttle percent.
	uint64_t sum = failures_sum + successes_sum;
	uint32_t failure_tenths_pct =
			sum == 0 ? 0 : (uint32_t)((failures_sum * 1000) / sum);

	// TODO - anything special for a 100% failure rate? Several seconds of all
	// failures with 0 successes might mean we should destroy this node?

	// Calculate and apply the throttle rate.
	uint32_t throttle_pct = 0;
	uint32_t threshold_tenths_pct = threshold_failure_pct * 10;

	if (enough_history && failure_tenths_pct > threshold_tenths_pct) {
		throttle_pct = ((failure_tenths_pct - threshold_tenths_pct) *
				throttle_factor) / 10;

		if (throttle_pct > MAX_THROTTLE_PCT) {
			throttle_pct = MAX_THROTTLE_PCT;
		}
	}

	cf_debug("node %s recent successes %lu, failures %lu, failure-tenths-pct %u, throttle-pct %u",
			cn->name, successes_sum, failures_sum, failure_tenths_pct, throttle_pct);

	cf_atomic32_set(&cn->throttle_pct, throttle_pct);
}

// The libevent2 event handler for node periodic timer events:
void
node_timer_fn(evutil_socket_t fd, short event, void* udata)
{
	cl_cluster_node* cn = (cl_cluster_node*)udata;

	if (cn->MAGIC != CLUSTER_NODE_MAGIC) {
		// Serious - can't do anything else with cn, including removing node.
		cf_error("node timer event found bad node magic");
		return;
	}

	uint64_t _s = cf_getms();

	cf_debug("node %s timer event", cn->name);

	// Check if this node is in the partition map. (But skip the first time this
	// node's timer fires, since the node can't be in the map yet.)
	if (cn->intervals_absent == 0 || cl_partition_table_is_node_present(cn)) {
		cn->intervals_absent = 1;
	}
	else if (cn->intervals_absent++ > MAX_INTERVALS_ABSENT) {
		// This node has been out of the map for MAX_INTERVALS_ABSENT laps.

		ev2citrusleaf_cluster* asc = cn->asc;

		cf_info("node %s not in map, removing from cluster %p", cn->name, asc);

		// If there's still a node info request in progress, cancel it.
		node_info_req_cancel(cn);

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
			cf_info("CL_DELAY: node removed: %lu", delta);
		}

		// Stops the periodic timer.
		return;
	}

	node_throttle_control(cn);

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
		// Serious - stops periodic timer! TODO - remove node?
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

	cf_atomic_int_incr(&asc->n_nodes_created);

#ifdef DEBUG_NODE_REF_COUNT
	// To balance the ref-count logs, we need this:
	cf_debug("node reserve: %s %s %p : %d", "O+", name, cn, cf_client_rc_count(cn));
#endif

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
	// PR: partition table, read
	// PW: partition table, write
	// T:  transaction

#ifdef DEBUG_NODE_REF_COUNT
	cf_debug("node release: %s %s %p : %d", msg, cn->name, cn, cf_client_rc_count(cn));
#endif

	if (0 == cf_client_rc_release(cn)) {
		cf_info("************* cluster node destroy: node %s : %p", cn->name, cn);

		cf_atomic_int_incr(&cn->asc->n_nodes_destroyed);

		node_info_req_cancel(cn);

		// AKG
		// If we call event_del() before assigning the event - possible in some
		// failures of cl_cluster_node_create() - the libevent library logs the
		// following:
		//
		// [warn] event_del: event has no event_base set.
		//
		// For now I'm not bothering with a flag to avoid this.

		event_del(cluster_node_get_timer_event(cn));

		if (cn->conn_q) {
			int fd;

			while (cf_queue_pop(cn->conn_q, &fd, CF_QUEUE_NOWAIT) == CF_QUEUE_OK) {
				cf_close(fd);
				cf_atomic32_decr(&cn->n_fds_open);
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
	// PR: partition table, read
	// PW: partition table, write
	// T:  transaction

#ifdef DEBUG_NODE_REF_COUNT
	cf_debug("node reserve: %s %s %p : %d", msg, cn->name, cn, cf_client_rc_count(cn));
#endif

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

		if (cf_atomic32_get(cn->throttle_pct) != 0) {
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

		if (cn && cn->MAGIC != CLUSTER_NODE_MAGIC) {
			// TODO - is this really happening any more?
			cf_error("cluster node get: got node with bad magic %x (%p), abort", cn->MAGIC, cn);
			cn = 0;
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
			case CONNECTED_ERROR:
				// Some other problem, could have to do with remote end.
				cf_close(fd);
				cf_atomic32_decr(&cn->n_fds_open);
				return -1;
			case CONNECTED_BADFD:
				// Local problem, don't try closing.
				cf_warn("bad file descriptor in queue: fd %d", fd);
				return -1;
			default:
				// Since we can't assert:
				cf_error("bad return value from ev2citrusleaf_is_connected");
				cf_close(fd);
				cf_atomic32_decr(&cn->n_fds_open);
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
		cf_warn("node %s has no sockaddrs", cn->name);
		return -2;
	}

	if (-1 == (fd = cf_socket_create_nb())) {
		// Local problem.
		return -2;
	}

	cf_debug("new socket: fd %d node %s", fd, cn->name);

	// Try socket addresses until we connect.
	for (uint32_t i = 0; i < cf_vector_size(&cn->sockaddr_in_v); i++) {
		struct sockaddr_in sa_in;

		cf_vector_get(&cn->sockaddr_in_v, i, &sa_in);

		if (0 == cf_socket_start_connect_nb(fd, &sa_in)) {
			cf_atomic32_incr(&cn->n_fds_open);
			return fd;
		}
		// TODO - else remove this sockaddr from the list?
	}

	cf_close(fd);

	return -2;
}

void
cl_cluster_node_fd_put(cl_cluster_node *cn, int fd)
{
	if (! cf_queue_push_limit(cn->conn_q, &fd,
			cf_atomic32_get(cn->asc->runtime_options.socket_pool_max))) {
		cf_close(fd);
		cf_atomic32_decr(&cn->n_fds_open);
	}
}


bool
cl_cluster_node_throttle_drop(cl_cluster_node* cn)
{
	uint32_t throttle_pct = cf_atomic32_get(cn->throttle_pct);

	if (throttle_pct == 0) {
		return false;
	}

	return ((uint32_t)rand() % 100) < throttle_pct;
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
	ping_nodes_data* pnd = (ping_nodes_data*)udata;
	ev2citrusleaf_cluster* asc = pnd->asc;

	cf_atomic_int_decr(&asc->pings_in_progress);

	if (return_value != 0) {
		cf_info("ping node function: error on return %d", return_value);
		if (values) free(values);
		// BFIX - need to free the data here, otherwise LEAK
		free(udata);
		cf_atomic_int_incr(&asc->n_ping_failures);
		return;
	}

	cf_atomic_int_incr(&asc->n_ping_successes);

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
		cf_atomic_int_incr(&asc->n_ping_failures);
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

