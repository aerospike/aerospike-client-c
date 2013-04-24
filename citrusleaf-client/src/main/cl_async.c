/*
 *  Citrusleaf Aerospike
 *  cl_async.c - Implementations specific to async command execution
 *
 *  Copyright 2008-2011 by Citrusleaf.  All rights reserved.
 *  THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE.  THE COPYRIGHT NOTICE
 *  ABOVE DOES NOT EVIDENCE ANY ACTUAL OR INTENDED PUBLICATION.
 */

#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <citrusleaf/citrusleaf.h>
#include <citrusleaf/cl_cluster.h>
#include <citrusleaf/citrusleaf-internal.h>
#include <citrusleaf/proto.h>
#include <citrusleaf/cf_atomic.h>
#include <citrusleaf/cf_socket.h>
#include <citrusleaf/cf_shash.h>

extern int g_init_pid;

// Structure used to maintain information about the work submitted
// for asynchronous command execution
#define MAX_ASYNC_RECEIVER_THREADS	32	//Max number of receiver threads for async work
#define ONEASYNCFD			0
typedef struct async_stats {
	cf_atomic_int	retries;
	cf_atomic_int	dropouts;
} async_stats;

// forward refs..
static void* async_receiver_fn(void *thdata);

//Global variables related to async work
static cf_atomic32 	g_async_initialized = 0;
cf_queue	   *g_cl_async_q = 0;
cf_queue	   *g_cl_workitems_freepool_q;
static int		    g_async_q_szlimit = 0;
static int		    g_async_nw_progress_timeout = 1000;
static pthread_t	g_async_reciever[MAX_ASYNC_RECEIVER_THREADS];
static cf_atomic32 	g_async_num_threads = 0;
static cf_atomic32 	g_thread_count = 0;
//Hashtable used in case of single async FD per node
shash		  		*g_cl_async_hashtab;
static uint32_t	    g_async_h_szlimit = 0;
static uint32_t	    g_async_h_buckets = 0;
static async_stats	g_async_stats;
static cl_async_fail_cb g_fail_cb_fn = NULL;
static cl_async_success_cb g_success_cb_fn = NULL;

void citrusleaf_async_getstats(uint64_t *retries, uint64_t *dropouts, int *workitems)
{
	*retries = g_async_stats.retries;
	*dropouts = g_async_stats.dropouts;
#if ONEASYNCFD
	*workitems = shash_get_size(g_cl_async_hashtab);
#else
	*workitems = cf_queue_sz(g_cl_async_q);
#endif
}

void citrusleaf_async_set_nw_timeout(int timeout)
{
	g_async_nw_progress_timeout = timeout;
}

//A trivial hash function to distribute the trids. We simply do a modulo
//of the number of buckets of the hashtable. As the trid is random enough
//this hash function should fare well.
static uint32_t 
async_trid_hash(void *udata)
{
	return (*((uint64_t *)udata) % g_async_h_buckets);
}

int
cl_del_node_asyncworkitems(void *key, void *value, void *clnode)
{
	if (((cl_async_work *)value)->node == (cl_cluster_node *)clnode) {
		return SHASH_REDUCE_DELETE;
	} else {
		return 0;
	}
}

static void* 
async_receiver_fn(void *thdata)
{
	int 		rv = -1;
	bool 		network_error = false;
	cl_async_work	*workitem = NULL;
	cl_async_work	*tmpworkitem = NULL;
	as_msg 		msg;
	cf_queue	*q_to_use = NULL;
	cl_cluster_node	*thisnode = NULL;

	uint8_t		rd_stack_buf[STACK_BUF_SZ];	
	uint8_t		*rd_buf = rd_stack_buf;
	size_t		rd_buf_sz = 0;

	uint64_t	acktrid;
	uint64_t	starttime, endtime;
	int		progress_timeout_ms;
	unsigned int 	thread_id = cf_atomic32_incr(&g_thread_count);

	if (thdata == NULL) {
		q_to_use = g_cl_async_q;
	} else {
		thisnode = (cl_cluster_node *)thdata;
		q_to_use = thisnode->asyncwork_q;
	}
    
	//Infinite loop which keeps picking work items from the list and try to find the end result 
	while(1) {
		network_error = false;
#if ONEASYNCFD
		if(thisnode->dunned == true) {
			do {
				rv = cf_queue_pop(thisnode->asyncwork_q, &workitem, CF_QUEUE_NOWAIT);
				if (rv == CF_QUEUE_OK) {
					cl_cluster_node_put(thisnode);
					free(workitem);
				}
			} while (rv == CF_QUEUE_OK);

			//We want to delete all the workitems of this node
			shash_reduce_delete(g_cl_async_hashtab, cl_del_node_asyncworkitems, thisnode);
			break;
		}
#endif
		//This call will block if there is no element in the queue
		cf_queue_pop(q_to_use, &workitem, CF_QUEUE_FOREVER);

		// Check for thread shutdown message.
		if (workitem->fd == -1) {
			// Exit thread. Workitem will be freed in citrusleaf_async_shutdown().
			pthread_exit(NULL);
		}

		//TODO: What if the node gets dunned while this pop call is blocked ?
#if ONEASYNCFD
		//cf_debug("Elements remaining in this node's queue=%d, Hash table size=%d",
		//		cf_queue_sz(thisnode->asyncwork_q), shash_get_size(g_cl_async_hashtab));
#endif

		// If we have no progress in 50ms, we should move to the next workitem 
		// and revisit this workitem at a later stage
		progress_timeout_ms = DEFAULT_PROGRESS_TIMEOUT;

		// Read into this fine cl_msg, which is the short header
		rv = cf_socket_read_timeout(workitem->fd, (uint8_t *) &msg, sizeof(as_msg), workitem->deadline, progress_timeout_ms);
		if (rv) {
#if DEBUG
			cf_debug("Citrusleaf: error when reading header from server - rv %d fd %d", rv, workitem->fd);
#endif
			if (rv != ETIMEDOUT) {
				cf_error("Citrusleaf: error when reading header from server - rv %d fd %d",rv,workitem->fd);
				network_error = true;
				goto Error;
			} else {
				goto Retry;
			}

		}
#ifdef DEBUG_VERBOSE
		dump_buf("read header from cluster", (uint8_t *) &msg, sizeof(cl_msg));
#endif
		cl_proto_swap(&msg.proto);
		cl_msg_swap_header(&msg.m);

		// second read for the remainder of the message 
		rd_buf_sz =  msg.proto.sz  - msg.m.header_sz;
		if (rd_buf_sz > 0) {
			if (rd_buf_sz > sizeof(rd_stack_buf)) {
				rd_buf = malloc(rd_buf_sz);
				if (!rd_buf) {
					cf_error("malloc fail: trying %zu",rd_buf_sz);
					rv = -1; 
					goto Error; 
				}
			}

			rv = cf_socket_read_timeout(workitem->fd, rd_buf, rd_buf_sz, workitem->deadline, progress_timeout_ms);
			if (rv) {
				//We already read some part of the message before but failed to read the
				//remaining data for whatever reason (network error or timeout). We cannot
				//reread as we already read partial data. Declare this as error.
				cf_error("Timeout after reading the header but before reading the body");
				goto Error;
			}
#ifdef DEBUG_VERBOSE
			dump_buf("read body from cluster", rd_buf, rd_buf_sz);
#endif	
		}

		rv = CITRUSLEAF_OK;
		goto Ok;

Retry:
		//We are trying to postpone the reading
		if (workitem->deadline && workitem->deadline < cf_getms()) {
			cf_error("async receiver: out of time : node %s: deadline %"PRIu64" now %"PRIu64,
					workitem->node->name, workitem->deadline, cf_getms());
			//cf_error("async receiver: Workitem missed the final deadline");
			rv = CITRUSLEAF_FAIL_TIMEOUT;
			goto Error;
		} else {
			//We have time. Push the element back to the queue to be considered later
			cf_queue_push(q_to_use, &workitem);
		}

		//If we allocated memory in this loop, release it.
		if (rd_buf && (rd_buf != rd_stack_buf)) {
			free(rd_buf);
		}

		cf_atomic_int_incr(&g_async_stats.retries);

		continue;

Error:
		if (network_error == true) {
			/* 
			 * In case of Async work (for XDS), it may be extreme to
			 * dun a node in case of network error. We just cleanup
			 * things and retry to connect to the remote cluster.
			 * The network error may be a transient one.
			 */
		} 

#if ONEASYNCFD
//Do not close FD
#else
		//We do not know the state of FD. It may have pending data to be read.
		//We cannot reuse the FD. So, close it to be on safe side.
		cf_error("async receiver: Closing the fd %d because of error", workitem->fd);
		close(workitem->fd);
		workitem->fd = -1;
#endif
		cf_atomic_int_incr(&g_async_stats.dropouts);
		//Continue down with what we do during an Ok

		//Inform the caller that there is no response from the server for this workitem.
		//No response does not mean that the work is not done. The work might be 
		//successfully completed on the server side, we just didnt get response for it.
		if (g_fail_cb_fn) {
			g_fail_cb_fn(workitem->udata, rv, workitem->starttime);
		}
Ok:
		//rd_buf may not be there during an error condition.
		if (rd_buf && (rv == CITRUSLEAF_OK)) {
			//As of now, async functionality is there only for put call.
			//In put call, we do not get anything back other than the trid field.
			//So, just pass variable to get back the trid and ignore others.
			if (0 != cl_parse(&msg.m, rd_buf, rd_buf_sz, NULL, NULL, NULL, &acktrid, NULL)) {
				rv = CITRUSLEAF_FAIL_UNKNOWN;
			}
			else {
				rv = msg.m.result_code;
				if (workitem->trid != acktrid) {
#if ONEASYNCFD
					//It is likely that we may get response for a different trid.
					//Just delete the correct one from the queue 
					//put back the current workitem back in the queue.
					shash_get(g_cl_async_hashtab, &acktrid, &tmpworkitem);
					cf_queue_delete(q_to_use, &tmpworkitem, true);
					cf_queue_push(q_to_use, &workitem);
					//From now on workitem will be the one for which we got ack
					workitem = tmpworkitem;
#endif
#ifdef DEBUG
					cf_debug("Got reply for a different trid. Expected=%"PRIu64" Got=%"PRIu64" FD=%d",
							workitem->trid, acktrid, workitem->fd);
#endif
				}
			}

			if (g_success_cb_fn) {
				g_success_cb_fn(workitem->udata, rv, workitem->starttime);
			}
		}

		//Remember to put back the FD into the pool, if it is re-usable.
		if (workitem->fd != -1) {
			cl_cluster_node_fd_put(workitem->node, workitem->fd, true);
		}
		//Also decrement the reference count for this node
		cl_cluster_node_put(workitem->node);

#if ONEASYNCFD
		//Delete the item from the global hashtable
		if (shash_delete(g_cl_async_hashtab, &workitem->trid) != SHASH_OK)
		{
#if DEBUG
			cf_debug("Failure while trying to delete trid=%"PRIu64" from hashtable", workitem->trid);
#endif
		}
#endif

		//Push it back into the free pool. If the attempt fails, free it.
		if (cf_queue_push(g_cl_workitems_freepool_q, &workitem) == -1) {
			free(workitem);
		}

		//If we allocated memory in this loop, release it.
		if (rd_buf && (rd_buf != rd_stack_buf)) {
			free(rd_buf);
		}

		// Kick this thread out if its ID is greater than total
		if (thread_id > cf_atomic32_get(g_async_num_threads)) {
			cf_atomic32_decr(&g_thread_count);
			return NULL;
		}
	}//The infnite loop

	return NULL;
}

//Same as do_the_full_monte, but only till the command is sent to the node.
//Most of the code is duplicated. Bad.
int
cl_do_async_monte(cl_cluster *asc, int info1, int info2, const char *ns, const char *set, const cl_object *key,
			const cf_digest *digest, cl_bin **values, cl_operator operator, cl_operation **operations, 
			int *n_values, uint32_t *cl_gen, const cl_write_parameters *cl_w_p, uint64_t *trid, void *udata)

{
	cl_async_work	*workitem = NULL;

	uint8_t		wr_stack_buf[STACK_BUF_SZ];
	uint8_t		*wr_buf = wr_stack_buf;
	size_t		wr_buf_sz = sizeof(wr_stack_buf);
	int        	progress_timeout_ms;
	uint64_t 	deadline_ms;
	uint64_t	starttime, endtime;
	bool 		network_error;
	int 		fd = -1;
	int		rv = CITRUSLEAF_FAIL_CLIENT;	//Assume that this is a failure;

	as_msg 		msg;
	cf_digest	d_ret;
	cl_cluster_node	*node = 0;

#if ONEASYNCFD
	if (shash_get_size(g_cl_async_hashtab) >= g_async_h_szlimit) {
		//cf_error("Async hashtab is full. Cannot insert any more elements");
		return CITRUSLEAF_FAIL_ASYNCQ_FULL;
	}
#else
	//If the async buffer is at the max limit, do not entertain more requests.
	if (cf_queue_sz(g_cl_async_q) >= cf_atomic32_get(g_async_q_szlimit)) {
		//cf_error("Async buffer is full. Cannot insert any more elements");
		return CITRUSLEAF_FAIL_ASYNCQ_FULL;
	}
#endif

	//Allocate memory for work item that will be added to the async work list

	if (cf_queue_sz(g_cl_workitems_freepool_q) > 0) {
		cf_queue_pop(g_cl_workitems_freepool_q, &workitem, CF_QUEUE_FOREVER);
	} else {
		workitem = malloc(sizeof(cl_async_work));
		if (workitem == NULL) {
			return CITRUSLEAF_FAIL_CLIENT;
		}
	}

	//Compile the write buffer to be sent to the cluster
	if (n_values && ( values || operations) ){
		cl_compile(info1, info2, 0, ns, set, key, digest, values?*values:NULL, operator, operations?*operations:NULL,
				*n_values , &wr_buf, &wr_buf_sz, cl_w_p, &d_ret, *trid,NULL);
	}else{
		cl_compile(info1, info2, 0, ns, set, key, digest, 0, 0, 0, 0, &wr_buf, &wr_buf_sz, cl_w_p, &d_ret, *trid,NULL);
	}	

	deadline_ms = 0;
	progress_timeout_ms = 0;
	if (cl_w_p && cl_w_p->timeout_ms) {
		deadline_ms = cf_getms() + cl_w_p->timeout_ms;
		// policy: if asking for a long timeout, give enough time to try twice
		if (cl_w_p->timeout_ms > 700) {
			progress_timeout_ms = cl_w_p->timeout_ms / 2;
		}
		else {
			progress_timeout_ms = cl_w_p->timeout_ms;
		}
	}
	else {
		progress_timeout_ms = g_async_nw_progress_timeout;
	}

	//Initialize the async work unit
	workitem->trid = *trid;
	workitem->deadline = deadline_ms;
	workitem->starttime = cf_getms();
	workitem->udata = udata;

	int try = 0;
	// retry request based on the write_policy
	do {
		network_error = false;
		try++;
#ifdef DEBUG		
		if (try > 1) {
			cf_debug("request retrying try %d tid %zu", try, (uint64_t)pthread_self());
		}
#endif        

		// Get an FD from a cluster. First get the probable node for the given digest.
		node = cl_cluster_node_get(asc, ns, &d_ret, info2 & CL_MSG_INFO2_WRITE ? true : false);
		if (!node) {
#ifdef DEBUG
			cf_debug("warning: no healthy nodes in cluster, retrying");
#endif
			usleep(10000);	//Sleep for 10ms
			goto Retry;
		}

		// Now get the dedicated async FD of this node
		starttime = cf_getms();
		fd = cl_cluster_node_fd_get(node, true, asc->nbconnect);
		endtime = cf_getms();
		if ((endtime - starttime) > 10) {
			cf_debug("Time to get FD for a node (>10ms)=%"PRIu64, (endtime - starttime));
		}
		if (fd == -1) {
#ifdef DEBUG			
			cf_debug("warning: node %s has no async file descriptors, retrying transaction (tid %zu)",node->name,(uint64_t)pthread_self() );
#endif			
			usleep(1000);
			goto Retry;
		}

		// Hate special cases, but we have to clear the verify bit on delete verify
		if ( (info2 & CL_MSG_INFO2_DELETE) && (info1 & CL_MSG_INFO1_VERIFY))
		{
			as_msg *msgp = (as_msg *)wr_buf;
			msgp->m.info1 &= ~CL_MSG_INFO1_VERIFY;
		}
		
		// Send the command to the node
		starttime = cf_getms();
		rv = cf_socket_write_timeout(fd, wr_buf, wr_buf_sz, deadline_ms, progress_timeout_ms);
		endtime = cf_getms();
		if ((endtime - starttime) > 10) {
			cf_debug("Time to write to the socket (>10ms)=%"PRIu64, (endtime - starttime));
		}
		if (rv != 0) {
			cf_debug("Citrusleaf: write timeout or error when writing header to server - %d fd %d errno %d (tid %zu)",
					rv,fd,errno,(uint64_t)pthread_self());
			if (rv != ETIMEDOUT)
				network_error = true;
			goto Retry;
		}
		goto Ok;

Retry:
		if (network_error == true) {
			/* 
			 * In case of Async work (for XDS), it may be extreme to
			 * dun a node in case of network error. We just cleanup
			 * things and retry to connect to the remote cluster.
			 * The network error may be a transient one. As this is a
			 * network error, its is better to wait for some significant
			 * time before retrying.
			 */
			sleep(1);	//Sleep for 1sec
#if ONEASYNCFD
//Do not close the FD
#else
			cf_error("async sender: Closing the fd %d because of network error", fd);
			close(fd);
			fd = -1;
#endif
		}

		if (fd != -1) {
			cf_error("async sender: Closing the fd %d because of retry", fd);
			close(fd);
			fd = -1;
		}

		if (node) {
			cl_cluster_node_put(node); 
			node = 0; 
		}

		if (deadline_ms && (deadline_ms < cf_getms() ) ) {
#ifdef DEBUG            
			cf_debug("async sender: out of time : deadline %"PRIu64" now %"PRIu64, deadline_ms, cf_getms());
#endif            
			rv = CITRUSLEAF_FAIL_TIMEOUT;
			goto Error;
		}
	} while ( (cl_w_p == 0) || (cl_w_p->w_pol == CL_WRITE_RETRY) );

Error:	
#ifdef DEBUG	
	cf_debug("exiting with failure: network_error %d wpol %d timeleft %d rv %d",
			(int)network_error, (int)(cl_w_p ? cl_w_p->w_pol : 0), 
			(int)(deadline_ms - cf_getms() ), rv );
#endif	

	if (wr_buf != wr_stack_buf) {
		free(wr_buf);
	}

#if ONEASYNCFD
	//Do not close the FD
#else
	//If it is a network error, the fd would be closed and set to -1.
	//So, we reach this place with a valid FD in case of timeout.
	if (fd != -1) {
		cf_error("async sender: Closing the fd %d because of timeout", fd);
		close(fd);
	}
#endif

	return(rv);
Ok:
	/*
	 * We cannot release the node here as the asyc FD associated
	 * with this node may get closed. We should do it only when
	 * we got back the ack for the async command that we just did.
	 */

	//As we sent the command successfully, add it to the async work list
	workitem->node = node;
	workitem->fd = fd;
	//We are storing only the pointer to the workitem
#if ONEASYNCFD
	if (shash_put_unique(g_cl_async_hashtab, trid, &workitem) != SHASH_OK) {
		//This should always succeed.
		cf_error("Unable to add unique entry into the hash table");
	}
	cf_queue_push(node->asyncwork_q, &workitem);	//Also put in the node's q
#else
	cf_queue_push(g_cl_async_q, &workitem);
#endif

	if (wr_buf != wr_stack_buf) {
		free(wr_buf);
	}

	rv = CITRUSLEAF_OK;
	return rv;

}

int citrusleaf_async_reinit(int size_limit, unsigned int num_receiver_threads)
{
	int num_threads;

	if (0 == cf_atomic32_get(g_async_initialized)) {
		cf_error("Async client not initialized cannot reinit");
		return -1;
	}
	
	if (num_receiver_threads > MAX_ASYNC_RECEIVER_THREADS) {
			//Limit the threads to the max value even if caller asks for it
			num_receiver_threads = MAX_ASYNC_RECEIVER_THREADS;
	}

	// If number of thread is increased create more threads
	if (num_receiver_threads > g_async_num_threads) {
		unsigned int i;
		for (i = g_async_num_threads; i < num_receiver_threads; i++) {
			pthread_create(&g_async_reciever[i], 0, async_receiver_fn, NULL);
		}
	}
	else {
		// else just reset the number the async threads will kill themselves
		cf_atomic32_set(&g_async_num_threads, num_receiver_threads);
	}

	cf_atomic32_set(&g_async_q_szlimit , size_limit);
	return ( 0 );

}

//
// Initialize async queue and async worker threads.
//
// size_limit: Maximum number of items allowed in queue. Puts are rejected when maximum is reached.
//
// num_receiver_threads: Number of worker threads to create.
//     If running in multi-process mode from python or perl, num_receiver_threads should be 1.
//     The maximum num_receiver_threads is 32.
//
// fail_cb_fn: Callback for failed transactions. Use null if callback is not desired.
// success_cb_fn: Callback for successful transactions. Use null if callback is not desired.
//
int citrusleaf_async_init(int size_limit, int num_receiver_threads, cl_async_fail_cb fail_cb_fn, cl_async_success_cb success_cb_fn)
{
	int i, num_threads;

	//Make sure that we do the initialization only once
	if (1 == cf_atomic32_incr(&g_async_initialized)) {

		// Start the receiver threads
		num_threads = num_receiver_threads;
		if (num_threads > MAX_ASYNC_RECEIVER_THREADS) {
			//Limit the threads to the max value even if caller asks for it
			num_threads = MAX_ASYNC_RECEIVER_THREADS;
		}

#if ONEASYNCFD
		g_async_h_szlimit = size_limit * 3;	//Max number of elements in the hash table
		g_async_h_buckets = g_async_h_szlimit/10;//Number of buckets in the hash table

		if (shash_create(&g_cl_async_hashtab, async_trid_hash, sizeof(uint64_t), sizeof(cl_async_work *),
					g_async_h_buckets, SHASH_CR_MT_BIGLOCK) != SHASH_OK) {
			cf_error("Failed to initialize the async work hastable");
			cf_atomic32_decr(&g_async_initialized);
			return -1;
		}
#else
		// create work queue
		g_async_q_szlimit = size_limit;
		if ((g_cl_async_q = cf_queue_create(sizeof(cl_async_work *), true)) == NULL) {
			cf_error("Failed to initialize the async work queue");
			cf_atomic32_decr(&g_async_initialized);
			return -1;
		}

		for (i=0; i<num_threads; i++) {
			pthread_create(&g_async_reciever[i], 0, async_receiver_fn, NULL);
		}
		g_async_num_threads = num_threads;
#endif

		if ((g_cl_workitems_freepool_q = cf_queue_create(sizeof(cl_async_work *), true)) == NULL) {
			cf_error("Failed to create memory pool for workitems");
			return -1;
		}

		g_fail_cb_fn = fail_cb_fn;
		g_success_cb_fn = success_cb_fn;

		// Initialize the stats
		g_async_stats.retries = 0;
		g_async_stats.dropouts = 0;

	}
	
	return(0);	
}

//
// Close async worker threads gracefully.
//
void
citrusleaf_async_shutdown()
{
	if (g_cl_async_q == 0)
		return;

	/*
	 * If a process is forked, the threads in it do not get spawned in the child process.
	 * In citrusleaf_init(), we are remembering the process id(g_init_pid) of the process who spawned the
	 * background threads. If the current process is not the process who spawned the background threads
	 * then it cannot call pthread_join() on the threads which does not exist in this process.
	 */
	if(g_init_pid == getpid()) {
		// Send shutdown message to each worker thread.
		cl_async_work *workitem = malloc(sizeof(cl_async_work));
		memset(workitem, 0, sizeof(cl_async_work));
		workitem->fd = -1;

		uint i;

		for (i = 0; i < g_async_num_threads; i++) {
			cf_queue_push(g_cl_async_q, &workitem);
		}

		for (i = 0; i < g_async_num_threads; i++) {
			pthread_join(g_async_reciever[i], NULL);
		}

		free(workitem);
		cf_queue_destroy(g_cl_async_q);
		g_cl_async_q = 0;
	}
}
