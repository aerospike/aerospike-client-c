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
#include <fcntl.h>
#include <arpa/inet.h>
#include <signal.h>


#include "citrusleaf_event/evcitrusleaf.h"
#include "citrusleaf_event/evcitrusleaf-internal.h"
#include "citrusleaf_event/cl_cluster.h"
#include "citrusleaf_event/proto.h"
#include "citrusleaf_event/cf_clock.h"

extern int evcitrusleaf_restart(cl_request *req);

// #define CLDEBUG_VERBOSE 1
// #define CLDEBUG_DUN 1

//
// Number of requests, in a row, that need to fail before the node
// is considered bad
//

#define CL_NODE_DUN_THRESHOLD 800

//
// Number of milliseconds between requests for the partition table.
// better for clients to run slightly out of date than be hammering the server
//

#define CL_NODE_PARTITION_MAX_MS (5000)

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
void cluster_tend( evcitrusleaf_cluster *asc); 
void cluster_new_sockaddr(evcitrusleaf_cluster *asc, struct sockaddr_in *new_sin);
int evcitrusleaf_cluster_add_host_internal(evcitrusleaf_cluster *asc, char *host_in, short port_in);


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
cluster_services_parse(evcitrusleaf_cluster *asc, char *services) 
{
	cf_vector_define(host_str_v, sizeof(void *), 0);
	str_split(';',services, &host_str_v);
	for (uint i=0;i<cf_vector_size(&host_str_v);i++) {
		char *host_str = cf_vector_pointer_get(&host_str_v, i);
		cf_vector_define(host_port_v, sizeof(void *), 0);
		str_split(':', host_str, &host_port_v);
		if (cf_vector_size(&host_port_v) == 2) {
			char *host_s = cf_vector_pointer_get(&host_port_v,0);
			char *port_s = cf_vector_pointer_get(&host_port_v,1);
			int port = atoi(port_s);
			struct sockaddr_in sin;
			if (0 == cl_lookup_immediate(host_s, port, &sin)) {
				cluster_new_sockaddr(asc, &sin);
				// add the string representation to our host list
				evcitrusleaf_cluster_add_host_internal(asc, host_s, port);
			}
			
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
//
// This is a function I'm always worried about taking too long
//

static void
cluster_partitions_process(evcitrusleaf_cluster *asc, cl_cluster_node *cn, char *partitions, bool write) 
{
	g_cl_stats.partition_process++;
	uint64_t _s = cf_getms();
	
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

				CL_LOG(CL_INFO, "cluster partitions process: bad namespace: len %zd space %s\n",strlen(namespace_s), namespace_s);
				goto Next;
			}
			if (partid > asc->n_partitions) {
				CL_LOG(CL_WARNING, "cluster partitions process: partitions out of scale: found %d max %d\n",
					partid, asc->n_partitions);
				goto Next;
			}
			
			cl_partition_table_set(asc, cn, namespace_s, partid, write); 
	
			CL_LOG(CL_VERBOSE, "node %s responsible for %s partition: %s : %d\n",
				cn->name,write ? "write" : "read",namespace_s,partid);

		}
Next:		
		cf_vector_destroy(&partition_v);
	}
	cf_vector_destroy(partitions_v);
	
	uint64_t delta = cf_getms() - _s;
	if (delta > CL_LOG_DELAY_WARN) CL_LOG(CL_WARNING," CL_DELAY: partition process: %"PRIu64"\n",delta);

}


// List of all current clusters so the tender can maintain them
// 
cf_ll		cluster_ll;


void
cluster_timer_fn(int fd, short event, void *udata)
{
	evcitrusleaf_cluster *asc = (evcitrusleaf_cluster *)udata;
	uint64_t _s = cf_getms();
	
	if (asc->MAGIC != CLUSTER_MAGIC) {
		CL_LOG(CL_WARNING, "cluster timer on non-cluster object %p\n",asc);
		return;
	}
	
	asc->timer_set = false;

	cluster_tend(asc);
	
	if (time(0) % CL_LOG_STATS_INTERVAL == 0) {
		evcitrusleaf_print_stats();
		CL_LOG(CL_INFO, " requests in progress: %d\n",asc->requests_in_progress);	
	}
                                                                
	if (0 != event_add(&asc->timer_event, &g_cluster_tend_timeout)) {
		CL_LOG(CL_WARNING, " cluster can't reschedule timer, fatal error, no one to report to\n");
	}
	else {
		asc->timer_set = true;
	}
	
	uint64_t delta = cf_getms() - _s;
	if (delta > CL_LOG_DELAY_WARN) CL_LOG(CL_WARNING," CL_DELAY: cluster timer: %"PRIu64"\n",delta);
	
}


evcitrusleaf_cluster *
evcitrusleaf_cluster_create(void)
{
	evcitrusleaf_cluster *asc = malloc(sizeof(evcitrusleaf_cluster));
	if (!asc)	return(0);
	memset(asc, 0, sizeof(evcitrusleaf_cluster));
	
	asc->MAGIC = CLUSTER_MAGIC;
	asc->follow = true;
	asc->last_node = 0;
	
	// bookkeeping for the set hosts
	cf_vector_pointer_init(&asc->host_str_v, 10, 0);
	cf_vector_integer_init(&asc->host_port_v, 10, 0);

	// all the nodes
	cf_vector_pointer_init(&asc->node_v, 10, 0);
	
	asc->request_q = cf_queue_create(sizeof(void *), false /*no threads in libevent!*/);
	if (asc->request_q == 0) {
		free(asc);
		return(0);
	}
	
	cf_ll_append(&cluster_ll, (cf_ll_element *) asc);

	asc->n_partitions = 0;
	asc->partition_table_head = 0;
	
	event_set(&asc->timer_event, -1, EV_TIMEOUT, cluster_timer_fn, asc);
	if (0 != event_add(&asc->timer_event, &g_cluster_tend_timeout)) {
		CL_LOG(CL_WARNING," could not add the cluster timeout");
		cf_queue_destroy(asc->request_q);
		// BFIX - the next line should be in
		cf_ll_delete( &cluster_ll , (cf_ll_element *) asc);
		free(asc);
		return(0);
	}
	asc->timer_set = true;
	
	return(asc);
}

int
evcitrusleaf_cluster_get_active_node_count(evcitrusleaf_cluster *asc)
{
	// *AN* likes to call with a null pointer. Shame.
	if (!asc)					return(-1);
	
	if (asc->MAGIC != CLUSTER_MAGIC) {
		CL_LOG(CL_WARNING, "cluster get_active_node on non-cluster object %p\n",asc);
		return(0);
	}
	
	CL_LOG(CL_VERBOSE, "cluster get active node count: vector size %d\n",cf_vector_size(&asc->node_v) );

	int count = 0;
	
	for (uint i=0;i<cf_vector_size(&asc->node_v);i++) {
		cl_cluster_node *node = cf_vector_pointer_get(&asc->node_v, i);
		
		if (node->MAGIC != CLUSTER_NODE_MAGIC) {
			CL_LOG(CL_WARNING, " node in cluster list has no magic!\n");
			continue;
		}
		
		if (node->name[0] == 0) {

			CL_LOG( CL_WARNING, "cluster node %d has no name (this is likely a serious internal confusion)\n",i);
			continue; // nodes with no name have never been pinged
		}
		
		if (node->dunned == true) {
		
			CL_LOG(CL_DEBUG, "cluster node %s (%d) is dunned\n",node->name,i);
			
			continue; // dunned nodes aren't active
		}
		
		if (cf_vector_size(&node->sockaddr_in_v)==0) {

			CL_LOG(CL_WARNING, "cluster ndoe %s (%d) has no address\n",node->name,i);

			continue; // nodes with no IP addresses aren't active
		}
		
		// maybe there are some other statistics, like the last good transaction...
		
		count++;
		
	}

	int rv = cf_vector_size(&asc->node_v);

	CL_LOG(CL_VERBOSE, "get active node count: %d active nodes\n",rv);
		
	return(rv);
}


//
// Major TODO!
// * destroy all the linked hosts
// * remove self from cluster list
//
// Attempt to let our queue drain without blocking by
// calling event_loop until we have no transactions in progress
//

void
evcitrusleaf_cluster_destroy(evcitrusleaf_cluster *asc)
{
	CL_LOG(CL_INFO, "cluster destroy: %p\n");
	
	if (asc->MAGIC != CLUSTER_MAGIC) {
		CL_LOG(CL_WARNING, "cluster destroy on non-cluster object %p\n",asc);
		return;
	}

	if (asc->timer_set) {
		event_del(&asc->timer_event);
		asc->timer_set = false;
	}
	
	// mark the cluster as shutting down - will prevent further info requests at very least
	asc->shutdown = true;

	CL_LOG(CL_INFO, "cluster destroy: infos in progress %d trans in progress %d (may have many)\n",
		asc->infos_in_progress, asc->requests_in_progress ); 

	// mark all nodes as fully dunned so they have a chance to time out while we're doing the rest of
	// the processing
	for (uint i=0;i<cf_vector_size(&asc->node_v);i++) {
		cl_cluster_node *cn = cf_vector_pointer_get(&asc->node_v, i);
		cn->dunned = true;
	}

	// wait for all info requests to complete
	CL_LOG(CL_DEBUG, "cluster destroy: waiting for infos\n"); 
	while ((asc->infos_in_progress > 0) && 
		   (0 == event_loop(EVLOOP_ONCE) ) )    ;

	// terminate all queued requests (rare to have queued requests)
	cl_request *req;
	while (CF_QUEUE_OK == cf_queue_pop(asc->request_q, (void *)&req,0)) {
		
		CL_LOG(CL_DEBUG, "have node now, restart request %p\n",req);

		evcitrusleaf_request_complete(req, true /*timedout*/);
	}

	CL_LOG(CL_DEBUG, "cluster destroy: waiting for requests\n"); 
	// wait for all pending requests to complete.
	// I think if we're called while the event loop is running, we'll get errors 
	while ((asc->requests_in_progress > 0) && 
		   (0 == event_loop(EVLOOP_ONCE) ) )    ;

	CL_LOG(CL_DEBUG, "cluster destroy: waiting for nodes\n"); 
	
	// all nodes have been dunned, wait for termination of such
	while ((cf_vector_size(&asc->node_v) > 0) && 
		   (0 == event_loop(EVLOOP_ONCE) ) )    ;
	

	CL_LOG(CL_INFO, "cluster termination: infos %d reqs %d nodes %d (should be all 0)\n",
		asc->infos_in_progress, asc->requests_in_progress, cf_vector_size(&asc->node_v));
	
	//
	// now it's all just teardown 
	//	   
	cf_queue_destroy(asc->request_q);
	asc->request_q = 0;
	
	for (uint i=0;i<cf_vector_size(&asc->host_str_v);i++) {
		char * host_str = cf_vector_pointer_get(&asc->host_str_v, i);
		free(host_str);
	}
	cf_vector_destroy(&asc->host_str_v);
	cf_vector_destroy(&asc->host_port_v);

	cf_vector_destroy(&asc->node_v);
	
	cl_partition_table_destroy_all(asc);
	
	cf_ll_delete( &cluster_ll , (cf_ll_element *) asc);
	
	memset(asc,0, sizeof(evcitrusleaf_cluster) );
	free(asc);
}


int
evcitrusleaf_cluster_add_host_internal(evcitrusleaf_cluster *asc, char *host_in, short port_in)
{

	CL_LOG(CL_VERBOSE, "adding host internally %s:%d\n",host_in, (int)port_in);

	// check for uniqueness - do we need a lock here?
	for (uint i=0;i<cf_vector_size(&asc->host_str_v);i++) {
		char *host_str = cf_vector_pointer_get(&asc->host_str_v, i);
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
evcitrusleaf_cluster_add_host(evcitrusleaf_cluster *asc, char *host_in, short port_in)
{

	CL_LOG(CL_DEBUG, "adding host %s:%d\n",host_in, (int)port_in);

	if (asc->MAGIC != CLUSTER_MAGIC) {
		CL_LOG(CL_WARNING, "cluster destroy on non-cluster object %p\n",asc);
		return(-1);
	}
	
	int rv = evcitrusleaf_cluster_add_host_internal(asc, host_in, port_in);
	if (0 != rv)	return(rv);

	// Fire the normal tender function to speed up resolution
	cluster_tend(asc);
	
	return(0);
}

void
evcitrusleaf_cluster_follow(evcitrusleaf_cluster *asc, bool flag)
{
	asc->follow = flag;
}

//
// NODES NODES NODES
//


void
node_replicas_fn(int return_value, char *response, size_t response_len, void *udata)
{
	cl_cluster_node *cn = (cl_cluster_node *) udata;
	
	if (cn->MAGIC != CLUSTER_NODE_MAGIC) {
		CL_LOG(CL_WARNING, "warning! node replicas function: node has no magic");
		return;
	}
	
	cn->asc->infos_in_progress--;
	
// this is almost always too verbose because it prints a 40k string
//#ifdef CLDEBUG_VERBOSE
//	CL_LOG( "node replicas: rv: %d response %s\n",return_value, response);
//#endif	

	CL_LOG(CL_DEBUG, "node replicas: node %s rv: %d\n",cn->name,return_value);
	
	// This is surprisngly important. It's crucial this node doesn't get inserted
	// into the partition table in particular, because the refcount might be
	// illegal
	if ((cn->dunned == true) || (cn->asc->shutdown)) {
		goto Done;
	}

	// if we have an error, dun this node
	if (return_value != 0) {
		cl_cluster_node_dun(cn,DUN_REPLICAS_FETCH);
		goto Done;
	}
	cl_cluster_node_ok(cn);

	// remove all current values, then add up-to-date values
	cl_partition_table_remove_node(cn->asc, cn);
	cn->partition_last_req_ms = cf_getms();

	// reminder: returned list is name1\tvalue1\nname2\tvalue2\n
	cf_vector_define(lines_v, sizeof(void *), 0);
	str_split('\n',response,&lines_v);
	for (uint j=0;j<cf_vector_size(&lines_v);j++) {
		char *line = cf_vector_pointer_get(&lines_v, j);
		cf_vector_define(pair_v, sizeof(void *), 0);
		str_split('\t',line, &pair_v);
		
		if (cf_vector_size(&pair_v) == 2) {
			char *name = cf_vector_pointer_get(&pair_v,0);
			char *value = cf_vector_pointer_get(&pair_v,1);

			
			if (strcmp(name, "replicas-read")== 0)
				cluster_partitions_process(cn->asc, cn, value, false);

			else if (strcmp(name, "replicas-write")==0)
				cluster_partitions_process(cn->asc, cn, value, true);
			
			else if (strcmp(name, "partition-generation")==0) {
				cn->partition_generation = atoi(value);

				CL_LOG(CL_DEBUG, "received new partition generation %d node %s\n",cn->partition_generation,cn->name);
				
			}
				
		}
		cf_vector_destroy(&pair_v);
	}
	cf_vector_destroy(&lines_v);

Done:	
	cl_cluster_node_release(cn, "6-");	
	if (response) free(response);
	return;
}

//
// callback from evcitrusleaf_info on the node itself
//
void
node_timer_infocb_fn(int return_value, char *response, size_t response_len, void *udata)
{
	cl_cluster_node *this_cn = (cl_cluster_node *) udata;
		
	CL_LOG(CL_VERBOSE, "node info: rv: %d response %s\n",return_value, response);

	if (this_cn->MAGIC != CLUSTER_NODE_MAGIC) {
		CL_LOG(CL_WARNING, " timer infocb fun: this node has no magic!\n");
		return;
	}
	
	CL_LOG(CL_DEBUG, "infocb fn: asc %p in progress %d\n",this_cn->asc, this_cn->asc->infos_in_progress);
	this_cn->asc->infos_in_progress--;
	
	if (this_cn->dunned || this_cn->asc->shutdown) {
		goto Done;
	}

	// if we have an error, dun this node
	if (return_value != 0) {
		cl_cluster_node_dun(this_cn,DUN_INFO_FAIL);
		goto Done;
	}
	cl_cluster_node_ok(this_cn);

	cf_vector_define(lines_v, sizeof(void *), 0);
	str_split('\n',response,&lines_v);
	for (uint i=0;i<cf_vector_size(&lines_v);i++) {
		char *line = cf_vector_pointer_get(&lines_v, i);
		cf_vector_define(pair_v, sizeof(void *), 0);
		str_split('\t',line, &pair_v);
		
		if (cf_vector_size(&pair_v) == 2) {
			char *name = cf_vector_pointer_get(&pair_v, 0);
			char *value = cf_vector_pointer_get(&pair_v, 1);
			
			if (strcmp(name, "node") == 0) {
				if (strcmp(value, this_cn->name) != 0) {
	
					CL_LOG(CL_WARNING, "node name has changed - was %s now %s - likely a bug - dun\n",this_cn->name,value);
					
					cl_cluster_node_dun(this_cn, DUN_BAD_NAME);
					cf_vector_destroy(&pair_v);
					cf_vector_destroy(&lines_v);
					goto Done;
				}
			}
			else if (strcmp(name, "partition-generation") == 0) {
				
				
				if ( this_cn->partition_generation != (uint32_t) atoi(value)) {
						
					uint64_t now = cf_getms();
					if (this_cn->partition_last_req_ms + CL_NODE_PARTITION_MAX_MS < cf_getms() ) {


						CL_LOG(CL_INFO, "making partition request of node %s\n",this_cn->name);
						
						this_cn->partition_last_req_ms = now;
					
						if (cf_vector_size(&this_cn->sockaddr_in_v) > 0) {
	
							cl_cluster_node_reserve(this_cn, "3+");
							
							struct sockaddr_in *sa_in = cf_vector_getp(&this_cn->sockaddr_in_v, 0);
						
							// start new async services request to this host
							if (0 != evcitrusleaf_info_host(sa_in ,"replicas-read\nreplicas-write\npartition-generation",0, node_replicas_fn, udata )) {
				
								// dun and don't come back?
								CL_LOG(CL_DEBUG," error calling replicas from node %s\n",this_cn->name);

								cl_cluster_node_release(this_cn, "7-");
							}
							else {
								this_cn->asc->infos_in_progress++;
							}
						}
					}
				}
			}
			else if (strcmp(name, "services") == 0) {
				cluster_services_parse(this_cn->asc, value);	
			}
		}
		cf_vector_destroy(&pair_v);
	}
	cf_vector_destroy(&lines_v);
	
Done:	
	cl_cluster_node_release(this_cn, "7-");
	if (response) free(response);
	return;
}

//
// when the node timer kicks, pull in the "services" string again
// see if there's any new services

void
node_timer_fn(int fd, short event, void *udata)
{
	cl_cluster_node *cn = (cl_cluster_node *)udata;
	if (cn->MAGIC != CLUSTER_NODE_MAGIC) {
		CL_LOG(CL_WARNING, " node called with no magic in timer, bad\n");
		return;
	}
	
	uint64_t _s = cf_getms();
	
	// have a reference count coming in
	cn->timer_event_registered = false;
		

	CL_LOG(CL_DEBUG, "node timer function called: %s dunned %d references %d\n",cn->name, cn->dunned, cf_rc_count(cn));

	if (cn->dunned) {

		CL_LOG(CL_INFO, "node %s fully dunned, removed from cluster and node timer\n",cn->name);
		
		// remove self from cluster's references
		if (cn->asc) {
			CL_LOG(CL_INFO, "node %s removing self from cluster %p\n",cn->name, cn->asc);
			evcitrusleaf_cluster *asc = cn->asc;
			for (uint i=0;i<cf_vector_size(&asc->node_v);i++) {
				cl_cluster_node *iter_node = cf_vector_pointer_get(&asc->node_v, i);
				if (iter_node == cn) {
					cf_vector_delete(&asc->node_v, i);
					cl_cluster_node_release(cn, "9-");
					break;
				}
			}
		}
		
		cl_cluster_node_release(cn, "2-");
		
		uint64_t delta = cf_getms() - _s;
		if (delta > CL_LOG_DELAY_WARN) CL_LOG(CL_WARNING," CL_DELAY: node dunned: %"PRIu64"\n",delta);

		return;
	}

	// can't really handle looking up more than one of these names.
	// always use the first one. If that stops working, perhaps we can
	// always delete the first one and try the second
	
	if (cf_vector_size(&cn->sockaddr_in_v) > 0) {
		struct sockaddr_in *sa_in = cf_vector_getp(&cn->sockaddr_in_v, 0);

		CL_LOG( CL_VERBOSE, "info host from node timer\n");
		
		// start new async services request to this host - will steal my event
		if (0 != evcitrusleaf_info_host(sa_in ,"node\npartition-generation\nservices",0, node_timer_infocb_fn, cn )) {
			// can't ping host? hope we can later

			CL_LOG(CL_INFO," error calling info from node\n");
			
			cl_cluster_node_dun(cn,DUN_INFO_FAIL);
		}
		else {
			// extra reservation for infohost
			cl_cluster_node_reserve(cn, "7+");
			cn->asc->infos_in_progress++;
		}
	}
	else {
		// node has no addrs --- remove
		cl_cluster_node_dun(cn, DUN_NO_SOCKADDR);
		uint64_t delta = cf_getms() - _s;
		if (delta > CL_LOG_DELAY_WARN) CL_LOG(CL_WARNING, " CL_DELAY: node no addrs: %"PRIu64"\n",delta);
	}


	if (0 != event_add(&cn->timer_event, &g_node_tend_timeout)) {
		CL_LOG(CL_WARNING,"event_add failed: node timer: node %s\n",cn->name);
		
	}
	else {
		cn->timer_event_registered = true;
	}

	uint64_t delta = cf_getms() - _s;
	if (delta > CL_LOG_DELAY_WARN) CL_LOG(CL_WARNING," CL_DELAY: node timer: %"PRIu64"\n",delta);
	
}



cl_cluster_node *
cl_cluster_node_create(char *name, evcitrusleaf_cluster *asc)
{

	CL_LOG(CL_INFO, " cl_cluster: creating node, name %s, cluster %p\n",name,asc);

	
	cl_cluster_node *cn = cf_rc_alloc( sizeof(cl_cluster_node ) );
	if (!cn)	return(0);
	
	cn->MAGIC = CLUSTER_NODE_MAGIC;
	
	strcpy(cn->name, name);
	cn->dunned = false;
	cn->dun_count = 0;
	cn->timer_event_registered = false;
	
	cf_vector_init(&cn->sockaddr_in_v, sizeof( struct sockaddr_in ), 5, 0);
	
	cn->conn_q = cf_queue_create( sizeof(int), false );
	if (cn->conn_q == 0) {
		CL_LOG(CL_WARNING," cl_cluster create: can't make a file descriptor queue\n");
		cf_rc_free(cn);
		return(0);
	}
	
	//
	cn->partition_generation = 0xFFFFFFFF;
	cn->partition_last_req_ms = 0;
	
	// Hand off a copy of the object to the health system
	cf_rc_reserve(cn);
	event_set(&cn->timer_event, -1, EV_TIMEOUT , node_timer_fn, cn);
	if (0 != event_add(&cn->timer_event, &g_node_tend_timeout)) {
		CL_LOG(CL_WARNING, " can't add perpetual node timer, can't pretend node exists\n");
		// looksl like a stutter, but we really have two outstanding
		cl_cluster_node_release(cn, "could not schedule event");
		cl_cluster_node_release(cn, "could not schedule event2");
		return(0);
	}
	cn->timer_event_registered = true;

	// link node to cluster and cluster to node
	cf_rc_reserve(cn);
	cn->asc = asc;
	cf_vector_pointer_append(&asc->node_v, cn); 
	
	g_cl_stats.nodes_created++;
	
	return(cn);
}

void
cl_cluster_node_release(cl_cluster_node *cn, char *msg)
{
	CL_LOG(CL_VERBOSE, "node release: %s %s %p : %d\n",msg,cn->name,cn,cf_rc_count(cn));
	
	if (0 == cf_rc_release(cn)) {

		CL_LOG(CL_INFO, "************* cluster node destroy: node %s : %p\n",cn->name,cn);
		
		// reach into the cluster and destroy references in the partition table
		if (cn->asc) {
			cl_partition_table_remove_node(cn->asc, cn);
		
		}
		else {
			CL_LOG(CL_WARNING, "destructing node with no cluster!\n");
		}

		g_cl_stats.nodes_destroyed++;

		cf_vector_destroy(&cn->sockaddr_in_v);
		
		// Drain out the queue and close the FDs
		int rv;
		do {
			int	fd;
			rv = cf_queue_pop(cn->conn_q, &fd, CF_QUEUE_NOWAIT);
			if (rv == CF_QUEUE_OK) {
				g_cl_stats.conns_destroyed++; // playing it safe, expect asc good
				shutdown(fd, SHUT_RDWR); // be good to remote endpoint - worried this might block though?
				close(fd);
			}
		} while (rv == CF_QUEUE_OK);
		cf_queue_destroy(cn->conn_q);
		event_del(&cn->timer_event);

		// rare, might as well be safe - and destroy the magic
		memset(cn, 0xff, sizeof(cl_cluster_node));
		
		cf_rc_free(cn);
		
		
	}
	
}

void
cl_cluster_node_reserve(cl_cluster_node *cn, char *msg)
{
//	CL_LOG(CL_VERBOSE, "node reserve: %s %s %p : %d\n",msg,cn->name,cn,cf_rc_count(cn));
	
	cf_rc_reserve(cn);
}


//
// Get a likely-healthy node for communication
// 

cl_cluster_node *
cl_cluster_node_get_random(evcitrusleaf_cluster *asc)
{
	
	// get a node from the node list round-robbin
	uint node_v_sz = cf_vector_size(&asc->node_v);
	if (node_v_sz == 0) {

		CL_LOG(CL_DEBUG, "cluster node get: no nodes in this cluster\n");

		return(0);
	}

	cl_cluster_node *cn = 0;
	uint i=0;
	do {
		asc->last_node ++;
		if (asc->last_node >= node_v_sz)	asc->last_node = 0;
		int node_i = asc->last_node;
		
//	CL_LOG( "cluster node get: vsize %d choosing %d\n",
//		cf_vector_size(&asc->node_v),node_i);
		
		cn = cf_vector_pointer_get(&asc->node_v, node_i);
		i++;
		
		if (cn->MAGIC != CLUSTER_NODE_MAGIC) {
			CL_LOG(CL_WARNING, " warning: bad magic in node %x\n",cn->MAGIC);
			return(0);
		}
		
//		if (cn->dunned == true)
//			CL_LOG(CL_DEBUG, "dunned node %s in random list!\n",cn->name);
		
	} while( (cn->dunned == true) && (i <= node_v_sz) );

	return(cn);
}

cl_cluster_node *
cl_cluster_node_get(evcitrusleaf_cluster *asc, char *ns, cf_digest *d, bool write)
{
	cl_cluster_node *cn = 0;
	
	if (asc->n_partitions) {
		// first, try to get one that matches this digest
		cn = cl_partition_table_get(asc, ns, cl_partition_getid(asc->n_partitions, d) , write);
		if (cn) {
			if (cn->MAGIC != CLUSTER_NODE_MAGIC) {
				// this is happening. when it happens, clear out this pointer for safety.
				// more importantly, fix the bug!
				CL_LOG(CL_WARNING, "cluster node get: got node with bad magic %x (%p), abort\n",cn->MAGIC, cn);
				cl_partition_table_remove_node(asc,cn);
				cn = 0;
				// raise(SIGINT);
			}
			else if (cn->dunned) { 
				cn = 0;
			}
		}
	}
	
	if (!cn) cn = cl_cluster_node_get_random(asc);

	// neither of these functions gets a reservation, so get one myself
	if (cn)	cl_cluster_node_reserve(cn, "1+");
	
	return( cn );
}

cl_cluster_node *
cl_cluster_node_get_byname(evcitrusleaf_cluster *asc, char *name)
{
	for (uint i=0;i<cf_vector_size(&asc->node_v);i++) {
		cl_cluster_node *node = cf_vector_pointer_get(&asc->node_v, i);
		if (strcmp(name, node->name) == 0) {
			cl_cluster_node_reserve(node,"2+");
			return(node);
		}
	}
	return(0);
	
}

// Put the node back, whatever that means (release the reference count?)

void
cl_cluster_node_put(cl_cluster_node *cn)
{
	cl_cluster_node_release(cn, "3-");
}

//
// Todo: will dunned hosts be in the host list with a flag, or in a different list?
//

char *cl_cluster_dun_human[] = {"user timeout","info fail","replicas fetch","network error","restart fd","no sockaddr"};


void
cl_cluster_node_dun(cl_cluster_node *cn, enum cl_cluster_dun_type type)
{
	if (cn->MAGIC != CLUSTER_NODE_MAGIC) {
		CL_LOG(CL_WARNING, " attempt to dun node without magic. Fail\n");
		return;
	}
	
	int dun_factor;
	switch (type) {
		case DUN_USER_TIMEOUT:
			if (cn->dun_count == 0) 			CL_LOG(CL_DEBUG, "dun node: %s reason: %s count: %d\n",cn->name,cl_cluster_dun_human[type],cn->dun_count);
			if (cn->dun_count == 50) 		CL_LOG(CL_DEBUG, "dun node: %s reason: %s count: %d\n",cn->name,cl_cluster_dun_human[type],cn->dun_count);
			if (cn->dun_count == 100) 		CL_LOG(CL_DEBUG, "dun node: %s reason: %s count: %d\n",cn->name,cl_cluster_dun_human[type],cn->dun_count);
			if (cn->dun_count == 200) 		CL_LOG(CL_DEBUG, "dun node: %s reason: %s count: %d\n",cn->name,cl_cluster_dun_human[type],cn->dun_count);
			dun_factor = 1;
			break;
		case DUN_INFO_FAIL:
		case DUN_REPLICAS_FETCH:
		case DUN_NO_SOCKADDR:
			CL_LOG(CL_DEBUG, "dun node: %s reason: %s count: %d\n",cn->name,cl_cluster_dun_human[type],cn->dun_count);
			dun_factor = 1000;
			break;
		case DUN_NETWORK_ERROR:
		case DUN_RESTART_FD:
			CL_LOG(CL_DEBUG, "dun node: %s reason: %s count: %d\n",cn->name,cl_cluster_dun_human[type],cn->dun_count);
			dun_factor = 50;
			break;
		default:
			CL_LOG(CL_DEBUG, "dun node: %s UNKNOWN REASON count: %d\n",cn->name,cn->dun_count);
			dun_factor = 1;
			break;
			
	}

	cn->dun_count += dun_factor;
	
	if (cn->dun_count > CL_NODE_DUN_THRESHOLD) {

		CL_LOG(CL_INFO, "dun node: node %s fully dunned %d\n",cn->name,cn->dun_count);
		
		cn->dunned = true;
	}
	
	
		
}

void
cl_cluster_node_ok(cl_cluster_node *cn)
{
	if (cn->MAGIC != CLUSTER_NODE_MAGIC) {
		CL_LOG(CL_WARNING," ok node but no magic, fail\n");
		return;
	}
	
	CL_LOG(CL_VERBOSE, "ok node: %s\n",cn->name);

	cn->dun_count = 0;
	cn->dunned = false;
}

//
// -1 try again - just got a stale element
// -2 transient error, maybe, add some dun to the node
// -3 true failure - will not succeed



int
cl_cluster_node_fd_get(cl_cluster_node *cn)
{
	
	int fd = -2;
	int rv = cf_queue_pop(cn->conn_q, &fd, CF_QUEUE_NOWAIT);
	if (rv == CF_QUEUE_OK) {
		// check to see if connected
		int rv2 = evcitrusleaf_is_connected(fd);
		switch(rv2) {
			case CONNECTED:
				return(fd);
			case CONNECTED_NOT:
				g_cl_stats.conns_destroyed++;
				g_cl_stats.conns_destroyed_queue++;
				close(fd);
				return(-1);
			case CONNECTED_ERROR:
				g_cl_stats.conns_destroyed++;
				g_cl_stats.conns_destroyed_queue++;
				close(fd);
				cl_cluster_node_dun(cn,  DUN_RESTART_FD );				
				return(-2);
			case CONNECTED_BADFD:
				// internal error, should always be a good fd, don't dun node
				// or free fd
				CL_LOG(CL_WARNING, "bad file descriptor in queue: fd %d\n",fd);
				return(cl_cluster_node_fd_get(cn));
			default:
				CL_LOG(CL_WARNING, "bad return value from evcitrusleaf_is_connected\n");
				return(-2);
		}
	}
	// unknown error or return
	if (rv != CF_QUEUE_EMPTY) 
		return(-2);		
	
	// ok, queue was empty - do a connect
	if (-1 == (fd = socket ( AF_INET, SOCK_STREAM, 0))) {

		CL_LOG(CL_DEBUG, "could not allocate a socket, serious problem\n");

		return(-2);
	}
	else {
		CL_LOG(CL_DEBUG, "new socket: fd %d node %s\n",fd, cn->name);
	}
	
	// set nonblocking - not so important since we're using recv/send, but critical for connect
	int flags;
	if (-1 == (flags = fcntl(fd, F_GETFL, 0)))
		flags = 0;
	if (-1 == fcntl(fd, F_SETFL, flags | O_NONBLOCK)) {
		g_cl_stats.conns_destroyed++;

		close(fd);
		CL_LOG(CL_WARNING, "could not set nonblocking\n");
		return(-2);
	}
	
	g_cl_stats.conns_created++;

	for (uint i=0;i< cf_vector_size(&cn->sockaddr_in_v);i++) {
		struct sockaddr_in *sa_in = cf_vector_getp(&cn->sockaddr_in_v, i);
		
		if (0 == connect(fd, (struct sockaddr *) sa_in, sizeof(struct sockaddr_in) ) )
		{
			rv = 0;
			// nonblocking connect: don't really know the connection has suceeded,
			// but it's a good place for the counter
			goto Done;
		}
		else {
			if (errno == EINPROGRESS) {
				goto Done;
			}
	
			// todo: remove this sockaddr from the list, or dun the node?
			else if (errno == ECONNREFUSED) {
				CL_LOG(CL_DEBUG, "a host is refusing connections\n");
			}
			else {
				CL_LOG(CL_INFO, "connect fail: errno %d\n",errno);
			}

		}
	}
	g_cl_stats.conns_destroyed++;
	close(fd);
	return(-2);
		
Done:
	g_cl_stats.conns_connected++;
	return(fd);
}

void
cl_cluster_node_fd_put(cl_cluster_node *cn, int fd)
{

	cf_queue_push(cn->conn_q, &fd);
	
}


//
// Debug function. Should be elsewhere.
//

void
sockaddr_in_dump(int level, char *prefix, struct sockaddr_in *sa_in)
{
	char str[INET_ADDRSTRLEN];
	inet_ntop(AF_INET, &(sa_in->sin_addr), str, INET_ADDRSTRLEN);	
	CL_LOG(level, "%s %s:%d\n",prefix,str,(int)ntohs(sa_in->sin_port));
}

void
cluster_dump(evcitrusleaf_cluster *asc)
{
	if (false == CL_LOG_CHK(CL_INFO)) return;
	
	CL_LOG(CL_DEBUG, "=*=*= cluster %p dump =*=*=\n",asc);
	
	CL_LOG(CL_DEBUG, "registered hosts:\n");
	for (uint i=0;i<cf_vector_size(&asc->host_str_v);i++) {
		char *host_s = cf_vector_pointer_get(&asc->host_str_v,i);
		int   port = cf_vector_integer_get(&asc->host_port_v,i);
		CL_LOG(CL_DEBUG, " host %d: %s:%d\n",i,host_s,port);
	}
	
	CL_LOG(CL_DEBUG, "nodes: %u\n",cf_vector_size(&asc->node_v));
	for (uint i=0;i<cf_vector_size(&asc->node_v);i++) {
		cl_cluster_node *cn = cf_vector_pointer_get(&asc->node_v, i);
		struct sockaddr_in *sa_in = cf_vector_getp(&cn->sockaddr_in_v, 0);
		char str[INET_ADDRSTRLEN];
		inet_ntop(AF_INET, &(sa_in->sin_addr), str, INET_ADDRSTRLEN);
		CL_LOG(CL_DEBUG, "%d %s : %s:%d (%d conns)\n",i,cn->name,str,
			(int)ntohs(sa_in->sin_port),cf_queue_sz(cn->conn_q));
	}
	
	CL_LOG(CL_DEBUG, "=*=*= cluster %p end dump =*=*=\n",asc);
}



typedef struct ping_node_data_s {
	struct sockaddr_in	sa_in;
	evcitrusleaf_cluster *asc;
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
	
	pnd->asc->infos_in_progress--;
	
	if (pnd->asc->shutdown)
	
	CL_LOG(CL_INFO, " ping node fn: rv %d node value retrieved: %s\n",return_value,values);
	
	if ((return_value != 0) || (pnd->asc->shutdown == true)) {
		CL_LOG(CL_INFO, " ping node function: error on return %d\n",return_value);
		if (values) free(values);
		// BFIX - need to free the data here, otherwise LEAK
		free(udata);
		return;
	}

	evcitrusleaf_cluster *asc = pnd->asc;
	
	cf_vector_define(lines_v, sizeof(void *), 0);
	str_split('\n',values,&lines_v);
	for (uint i=0;i<cf_vector_size(&lines_v);i++) {
		char *line = cf_vector_pointer_get(&lines_v, i);
		cf_vector_define(pair_v, sizeof(void *), 0);
		str_split('\t',line, &pair_v);
		
		if (cf_vector_size(&pair_v) == 2) {
			char *name = cf_vector_pointer_get(&pair_v, 0);
			char *value = cf_vector_pointer_get(&pair_v, 1);
			
			if (strcmp(name, "node") == 0) {

				// make sure this host already exists, create & add if not
				cl_cluster_node *cn = cl_cluster_node_get_byname(asc, value);
				if (!cn) {
			//			CL_LOG( " node unknown, creating new\n");
					cn = cl_cluster_node_create(value /*nodename*/, asc);
				}

				if (cn) {
					// add this address to node list
					cf_vector_append_unique(&cn->sockaddr_in_v,&pnd->sa_in);
				
					cl_cluster_node_release(cn, "4-");
					cn = 0;
				}
			}
			else if (strcmp(name, "partitions")==0) {
				asc->n_partitions = atoi(value);
	
				CL_LOG(CL_VERBOSE, " cluster partitions: %d\n",asc->n_partitions);
				
			}
		}
		cf_vector_destroy(&pair_v);
	}
	cf_vector_destroy(&lines_v);
	
//		else {
//			CL_LOG( " node known, ignoring \n");
//		}
	
	if (values) free(values);
	free(pnd);
	pnd = 0;
	
	// if the cluster had waiting requests, try to restart
	if (cf_vector_size(&asc->node_v) != 0) {
		void *req;
		while (CF_QUEUE_OK == cf_queue_pop(asc->request_q, (void *)&req,0)) {

			CL_LOG(CL_DEBUG, "have node now, restart request %p\n",req);
	
			evcitrusleaf_restart(req);
		}
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
	evcitrusleaf_cluster *asc = (evcitrusleaf_cluster *)udata;

	CL_LOG(CL_INFO, "cluster tend host resolve:\n");
	
	if ((result == 0) && (sockaddr_v)) {
	
		for (uint i=0;i<cf_vector_size(sockaddr_v);i++) {
			struct sockaddr_in *sin;
			sin = cf_vector_getp(sockaddr_v,i);		
			
			cluster_new_sockaddr(asc, sin);
				
		}
	}

}

//
// Call this routine whenever you've discovered a new sockaddr.
// Maybe we already know about it, maybe we don't - this routine will
// 'debounce' efficiently and launch an 'add' cycle if it appears new.
//

void
cluster_new_sockaddr(evcitrusleaf_cluster *asc, struct sockaddr_in *new_sin)
{
	if (asc->shutdown == true)	return;
	
	// Lookup the sockaddr in the node list. This is inefficient, but works
	// Improve later if problem...
		
	cf_vector *node_v = &asc->node_v;
	for (uint j=0;j<cf_vector_size(node_v);j++) {
		cl_cluster_node *cn = cf_vector_pointer_get(node_v,j);
		for (uint k=0;k<cf_vector_size(&cn->sockaddr_in_v);k++) {
			struct sockaddr_in *sin = cf_vector_getp(&cn->sockaddr_in_v, k);

			if (memcmp(sin, new_sin, sizeof(struct sockaddr_in)) == 0) {
				// it's old - get out

				return;
			}
		}
	}

	
	// have new never-pinged hosts. Do the info_host call to get its name
	// The callback will add the node if it's new
	sockaddr_in_dump(CL_INFO,"new sockaddr found: ",new_sin);
	
	ping_nodes_data *pnd = malloc(sizeof(ping_nodes_data));
	if (!pnd)	return;
	pnd->sa_in = *new_sin;
	pnd->asc = asc;

	if (0 != evcitrusleaf_info_host(new_sin, asc->n_partitions == 0 ? "node\npartitions" : "node", 
						0, cluster_ping_node_fn, pnd)) {
		free(pnd);
	}
	else {
		asc->infos_in_progress++;
	}
	
}


void
cluster_tend( evcitrusleaf_cluster *asc) 
{


	CL_LOG(CL_DEBUG,"cluster tend: cluster %p\n",asc);

	cluster_dump(asc);

	// For all registered names --- kick off a resolver
	// to see if there are new IP addresses
	// this is kind of expensive, so might need to do it only rarely
	// because, realistically, it never changes. Only go searching for nodes
	// if there are no nodes in the cluster - we've fallen off the edge of the earth
	if (0 == cf_vector_size(&asc->node_v)) {


		CL_LOG(CL_DEBUG, "no nodes remaining: lookup original hosts hoststr size %d\n");

		uint n_hosts = cf_vector_size(&asc->host_str_v);
		for (uint i=0;i<n_hosts;i++) {
			
			char *host_s = cf_vector_pointer_get(&asc->host_str_v, i);
			int  port = cf_vector_integer_get(&asc->host_port_v, i);
	
			CL_LOG(CL_DEBUG, "lookup hosts: %s:%d\n",host_s,port);
	
			struct sockaddr_in sin;
			if (0 == cl_lookup_immediate(host_s, port, &sin)) {
				cluster_new_sockaddr(asc, &sin);
			}
			else {
				// BFIX - if this returns error, ???
				cl_lookup(	cf_vector_pointer_get(&asc->host_str_v, i), 
							cf_vector_integer_get(&asc->host_port_v, i),
							cluster_tend_hostname_resolve, asc);
			}
		}
	}
	
	CL_LOG(CL_DEBUG, "end tend\n");
	
	return;
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
		evcitrusleaf_cluster *asc = (evcitrusleaf_cluster *)e; 
		evcitrusleaf_cluster_destroy(asc);
	}
	
	return(0);	
}

