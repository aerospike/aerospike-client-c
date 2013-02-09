/*
 * cl_libevent2/src/cl_batch.c
 *
 * Batch operations.
 *
 * Citrusleaf, 2013.
 * All rights reserved.
 */


//==========================================================
// Includes
//

#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <event2/event.h>
#include <sys/socket.h>

#include "citrusleaf/cf_base_types.h"
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_digest.h"
#include "citrusleaf/cf_errno.h"
#include "citrusleaf/cf_log_internal.h"
#include "citrusleaf/proto.h"

#include "citrusleaf_event2/cl_cluster.h"
#include "citrusleaf_event2/ev2citrusleaf.h"
#include "citrusleaf_event2/ev2citrusleaf-internal.h"


//==========================================================
// Constants
//

#define MAX_NODES 128


//==========================================================
// Forward Declarations
//

typedef struct cl_batch_job_s cl_batch_job;
typedef struct cl_batch_node_req_s cl_batch_node_req;

static int get_many(ev2citrusleaf_cluster* cl, const char* ns,
		const cf_digest* digests, int n_digests, const char** bins, int n_bins,
		bool get_bin_data, int timeout_ms, ev2citrusleaf_get_many_cb cb,
		void* udata, struct event_base* base);


//==========================================================
// cl_batch_job Class Header
//

//------------------------------------------------
// Function Declarations
//

static cl_batch_job* cl_batch_job_create(struct event_base* base,
		ev2citrusleaf_get_many_cb user_cb, void* user_data, int n_digests,
		int timeout_ms);
static void cl_batch_job_destroy(cl_batch_job* this);
static inline struct event_base* cl_batch_job_get_base(cl_batch_job* this);
static inline uint32_t cl_batch_job_clepoch_seconds(cl_batch_job* this);
static bool cl_batch_job_add_node_unique(cl_batch_job* this,
		cl_cluster_node* p_node);
static bool cl_batch_job_compile(cl_batch_job* this, const char* ns,
		const cf_digest* digests, const char** bins, int n_bins,
		bool get_bin_data, cl_cluster_node** nodes);
static bool cl_batch_job_start(cl_batch_job* this);
static inline ev2citrusleaf_rec* cl_batch_job_get_rec(cl_batch_job* this);
static inline void cl_batch_job_rec_done(cl_batch_job* this);
static void cl_batch_job_node_done(cl_batch_job* this,
		cl_batch_node_req* p_node_req, int node_result);
// The libevent2 timer event handler:
static void cl_batch_job_timeout_event(int fd, short event, void* pv_this);

//------------------------------------------------
// Data
//

struct cl_batch_job_s {
	// All events use this base.
	struct event_base*			p_event_base;

	// User supplied callback and data.
	ev2citrusleaf_get_many_cb	user_cb;
	void*						user_data;

	// Array of node request object pointers.
	cl_batch_node_req*			node_reqs[MAX_NODES];
	int							n_node_reqs;

	// How many node requests are complete.
	int							n_node_reqs_done;

	// Overall result.
	int							node_result;

	// Total number of records queried.
	int							n_digests;

	// Array of records accumulated by all node requests' responses.
	ev2citrusleaf_rec*			recs;
	int							n_recs;

	// Citrusleaf epoch time used for calculating expirations of returned
	// records - hopefully temporary until expirations are returned by server.
	uint32_t					now;

	// The timeout event.
	bool						timer_event_added;
	uint8_t						timer_event_space[];
};


//==========================================================
// cl_batch_node_req Class Header
//

//------------------------------------------------
// Function Declarations
//

static cl_batch_node_req* cl_batch_node_req_create(cl_batch_job* p_job,
		cl_cluster_node* p_node);
static void cl_batch_node_req_destroy(cl_batch_node_req* this);
static inline const cl_cluster_node* cl_batch_node_req_get_node(
		cl_batch_node_req* this);
static inline void cl_batch_node_req_add_digest(cl_batch_node_req* this);
static bool cl_batch_node_req_compile(cl_batch_node_req* this, const char* ns,
		size_t ns_len, const cf_digest* all_digests, int n_all_digests,
		const char** bins, int n_bins, bool get_bin_data,
		cl_cluster_node** nodes);
static uint8_t* cl_batch_node_req_write_fields(cl_batch_node_req* this,
		uint8_t* p_write, const char* ns, size_t ns_len,
		const cf_digest* all_digests, int n_all_digests,
		cl_cluster_node** nodes, size_t digests_size);
static bool cl_batch_node_req_get_fd(cl_batch_node_req* this);
static void cl_batch_node_req_start(cl_batch_node_req* this);
// The libevent2 event handler:
static void cl_batch_node_req_event(int fd, short event, void* pv_this);
static bool cl_batch_node_req_handle_send(cl_batch_node_req* this);
static bool cl_batch_node_req_handle_recv(cl_batch_node_req* this);
static int cl_batch_node_req_parse_proto_body(cl_batch_node_req* this,
		bool* p_is_last);
static void cl_batch_node_req_done(cl_batch_node_req* this, int node_result);

//------------------------------------------------
// Data
//

struct cl_batch_node_req_s {
	// The parent batch job object.
	cl_batch_job*				p_job;

	// The node for this request.
	cl_cluster_node*			p_node;

	// Number of records queried on this node.
	int							n_digests;

	// Number of records accumulated by this node request's response.
	int							n_recs;

	// This node request's socket.
	int							fd;

	// Buffer for writing to socket.
	uint8_t*					wbuf;
	size_t						wbuf_size;
	size_t						wbuf_pos;

	// Buffer for reading proto header from socket.
	uint8_t						hbuf[sizeof(cl_proto)];
	size_t						hbuf_pos;

	// Buffer for reading proto body from socket.
	uint8_t*					rbuf;
	size_t						rbuf_size;
	size_t						rbuf_pos;

	// The network event for this node request.
	bool						event_added;
	uint8_t						event_space[];
};


//==========================================================
// Public API
//

int
ev2citrusleaf_get_many_digest(ev2citrusleaf_cluster* cl, const char* ns,
		const cf_digest* digests, int n_digests, const char** bins, int n_bins,
		int timeout_ms, ev2citrusleaf_get_many_cb cb, void* udata,
		struct event_base* base)
{
	return get_many(cl, ns, digests, n_digests, bins, n_bins, true, timeout_ms,
			cb, udata, base);
}

int
ev2citrusleaf_exists_many_digest(ev2citrusleaf_cluster* cl, const char* ns,
		const cf_digest* digests, int n_digests, int timeout_ms,
		ev2citrusleaf_get_many_cb cb, void* udata, struct event_base* base)
{
	return get_many(cl, ns, digests, n_digests, NULL, 0, false, timeout_ms, cb,
			udata, base);
}


//==========================================================
// Private Functions
//

//------------------------------------------------
// Public APIs pass through to this. Creates a
// batch job object, and a node request object for
// each node to be queried. Compiles requests for
// these nodes and starts their transactions.
//
static int
get_many(ev2citrusleaf_cluster* cl, const char* ns, const cf_digest* digests,
		int n_digests, const char** bins, int n_bins, bool get_bin_data,
		int timeout_ms, ev2citrusleaf_get_many_cb cb, void* udata,
		struct event_base* base)
{
	// Quick sanity check for parameters.
	if (! (cl && ns && *ns && digests && n_digests > 0 && cb && base)) {
		cf_error("invalid parameter");
		return EV2CITRUSLEAF_FAIL_CLIENT_ERROR;
	}

	// Allocate an array of node pointers, one per digest. There may be a very
	// large number of digests, so don't use the stack.
	cl_cluster_node** nodes = (cl_cluster_node**)
			malloc(n_digests * sizeof(cl_cluster_node*));

	if (! nodes) {
		cf_error("node pointer array allocation failed");
		return EV2CITRUSLEAF_FAIL_CLIENT_ERROR;
	}

	// Make a cl_batch_job object.
	cl_batch_job* p_job = cl_batch_job_create(base, cb, udata, n_digests,
			timeout_ms);

	if (! p_job) {
		cf_error("can't create batch job");
		free(nodes);
		return EV2CITRUSLEAF_FAIL_CLIENT_ERROR;
	}

	// Find the nodes to query, make a cl_batch_node_req object for each.
	for (int i = 0; i < n_digests; i++) {
		// This increments the node's ref-count, so overall a given node's
		// ref-count increases by the number of (these) digests on that node.
		nodes[i] = cl_cluster_node_get(cl, ns, &digests[i], true);

		if (! nodes[i]) {
			cf_error("can't get node for digest index %d", i);
			cl_batch_job_destroy(p_job);
			free(nodes);
			return EV2CITRUSLEAF_FAIL_CLIENT_ERROR;
		}

		if (! cl_batch_job_add_node_unique(p_job, nodes[i])) {
			cf_error("can't create batch request for node %s", nodes[i]->name);
			cl_batch_job_destroy(p_job);
			free(nodes);
			return EV2CITRUSLEAF_FAIL_CLIENT_ERROR;
		}
	}

	// Compile the requests.
	if (! cl_batch_job_compile(p_job, ns, digests, bins, n_bins, get_bin_data,
			nodes)) {
		cf_error("failed batch job compile");
		cl_batch_job_destroy(p_job);
		free(nodes);
		return EV2CITRUSLEAF_FAIL_CLIENT_ERROR;
	}

	// Start all the requests.
	if (! cl_batch_job_start(p_job)) {
		cf_error("failed batch job start");
		cl_batch_job_destroy(p_job);
		free(nodes);
		return EV2CITRUSLEAF_FAIL_CLIENT_ERROR;
	}

	free(nodes);

	return EV2CITRUSLEAF_OK;
}


//==========================================================
// cl_batch_job Class Function Definitions
//

//------------------------------------------------
// Create a cl_batch_job object. Adds the timeout
// event.
//
static cl_batch_job*
cl_batch_job_create(struct event_base* base, ev2citrusleaf_get_many_cb user_cb,
		void* user_data, int n_digests, int timeout_ms)
{
	size_t size = sizeof(cl_batch_job) + event_get_struct_event_size();
	cl_batch_job* this = (cl_batch_job*)malloc(size);

	if (! this) {
		cf_error("batch request allocation failed");
		return NULL;
	}

	memset((void*)this, 0, size);

	// Add the timeout event right away. Note that "cross-threaded" requests are
	// not safe against this timer firing before the end of this non-blocking
	// get_many() call - for now we just rely on reasonable timeout values.

	evtimer_assign((struct event*)this->timer_event_space, base,
			cl_batch_job_timeout_event, this);

	struct timeval tv;

	tv.tv_sec = timeout_ms / 1000;
	tv.tv_usec = (timeout_ms % 1000) * 1000;

	if (0 != evtimer_add((struct event*)this->timer_event_space, &tv)) {
		cf_error("batch job add timer event failed");
		free(this);
		return NULL;
	}

	this->timer_event_added = true;

	this->p_event_base = base;
	this->user_cb = user_cb;
	this->user_data = user_data;
	this->n_digests = n_digests;

	size_t recs_size = n_digests * sizeof(ev2citrusleaf_rec);

	this->recs = (ev2citrusleaf_rec*)malloc(recs_size);

	if (! this->recs) {
		cf_error("batch request recs allocation failed");
		free(this);
		return NULL;
	}

	return this;
}

//------------------------------------------------
// Destroy a cl_batch_job object. Destroys any
// outstanding node requests, and frees any bins
// accumulated. (User is responsible for freeing
// bins' objects.)
//
static void
cl_batch_job_destroy(cl_batch_job* this)
{
	if (this->timer_event_added) {
		evtimer_del((struct event*)this->timer_event_space);
	}

	for (int n = 0; n < this->n_node_reqs; n++) {
		if (this->node_reqs[n]) {
			cl_batch_node_req_destroy(this->node_reqs[n]);
		}
	}

	for (int i = 0; i < this->n_recs; i++) {
		if (this->recs[i].bins) {
			free(this->recs[i].bins);
		}
	}

	free(this->recs);
	free(this);
}

//------------------------------------------------
// Member access function.
//
static inline struct event_base*
cl_batch_job_get_base(cl_batch_job* this)
{
	return this->p_event_base;
}

//------------------------------------------------
// Get Citrusleaf epoch time used for calculating
// expirations. Lazily set this so it's as late as
// possible.
//
static inline uint32_t cl_batch_job_clepoch_seconds(cl_batch_job* this)
{
	if (this->now == 0) {
		this->now = cf_clepoch_seconds();
	}

	return this->now;
}

//------------------------------------------------
// For specified node, create a node request and
// add it to the array if it hasn't already been
// done. If it has, increment that node request's
// digest count.
//
static bool
cl_batch_job_add_node_unique(cl_batch_job* this, cl_cluster_node* p_node)
{
	int n;

	// Check if this node already has a node request in the list.
	for (n = 0; n < this->n_node_reqs; n++) {
		if (p_node == cl_batch_node_req_get_node(this->node_reqs[n])) {
			// It is already there.
			cl_batch_node_req_add_digest(this->node_reqs[n]);
			break;
		}
	}

	// It is not already there - add it.
	if (n == this->n_node_reqs) {
		cl_batch_node_req* p_node_req = cl_batch_node_req_create(this, p_node);

		if (! p_node_req) {
			return false;
		}

		this->node_reqs[this->n_node_reqs++] = p_node_req;
	}

	return true;
}

//------------------------------------------------
// Call all the node request's compile methods.
//
static bool
cl_batch_job_compile(cl_batch_job* this, const char* ns,
		const cf_digest* digests, const char** bins, int n_bins,
		bool get_bin_data, cl_cluster_node** nodes)
{
	size_t ns_len = strlen(ns);

	// AKG - This isn't optimal for big clusters and very large batches: for n
	// nodes and d digests, we do n*d operations. We could gain a factor of 2 by
	// inverting, so that we do 1 loop over digests, and for each digest an
	// average of n/2 checks to find the node. But I'm not going to do that now,
	// I'd rather keep the compile methods looking like those in the C client.

	for (int n = 0; n < this->n_node_reqs; n++) {
		if (! cl_batch_node_req_compile(this->node_reqs[n], ns, ns_len, digests,
				this->n_digests, bins, n_bins, get_bin_data, nodes)) {
			cf_error("can't compile batch node request %d", n);
			return false;
		}
	}

	return true;
}

//------------------------------------------------
// Get a socket for each node request, then start
// all the requests' network transactions.
//
static bool
cl_batch_job_start(cl_batch_job* this)
{
	// Get all the sockets before adding any events - it's easier to unwind on
	// failure without worrying about event callbacks.
	for (int n = 0; n < this->n_node_reqs; n++) {
		if (! cl_batch_node_req_get_fd(this->node_reqs[n])) {
			cf_error("can't get fd for batch node request %d", n);
			return false;
		}
	}

	// From this point on, we'll always give a callback.
	for (int n = 0; n < this->n_node_reqs; n++) {
		cl_batch_node_req_start(this->node_reqs[n]);
	}

	return true;
}

//------------------------------------------------
// Get pointer to current record-to-fill. Node
// requests' responses will accumulate records by
// filling this.
//
static inline ev2citrusleaf_rec*
cl_batch_job_get_rec(cl_batch_job* this)
{
	return &this->recs[this->n_recs];
}

//------------------------------------------------
// Advance index of current record-to-fill.
//
static inline void
cl_batch_job_rec_done(cl_batch_job* this)
{
	this->n_recs++;
}

//------------------------------------------------
// Called by node requests that are complete. If
// it's the last node request, make the user
// callback and clean up.
//
static void
cl_batch_job_node_done(cl_batch_job* this, cl_batch_node_req* p_node_req,
		int node_result)
{
	// Destroy the completed node request.
	cl_batch_node_req_destroy(p_node_req);

	// Make sure cl_batch_job destructor skips already destroyed node request.
	for (int n = 0; n < this->n_node_reqs; n++) {
		if (this->node_reqs[n] == p_node_req) {
			this->node_reqs[n] = NULL;
		}
	}

	// This just reports the result from the last node that doesn't succeed.
	// TODO - report results per node ???
	if (node_result != EV2CITRUSLEAF_OK) {
		this->node_result = node_result;
	}

	this->n_node_reqs_done++;

	if (this->n_node_reqs_done < this->n_node_reqs) {
		// Some node requests are still going, we'll be back.
		return;
	}

	// All node requests are done.

	// Make the user callback.
	(*this->user_cb)(this->node_result, this->recs, this->n_recs,
			this->user_data);

	// Destroy self. This aborts the timeout event.
	cl_batch_job_destroy(this);
}

//------------------------------------------------
// The libevent2 timer event callback function.
// Make the user callback with whatever we have so
// far, and clean up.
//
static void
cl_batch_job_timeout_event(int fd, short event, void* pv_this)
{
	cl_batch_job* this = (cl_batch_job*)pv_this;

	this->timer_event_added = false;

	// Make the user callback. This reports partial results from any node
	// requests that finished.
	(*this->user_cb)(EV2CITRUSLEAF_FAIL_TIMEOUT, this->recs, this->n_recs,
			this->user_data);

	// Destroy self. This aborts and destroys all outstanding node requests.
	cl_batch_job_destroy(this);
}


//==========================================================
// cl_batch_node_req Class Function Definitions
//

//------------------------------------------------
// Create a cl_batch_node_req object.
//
static cl_batch_node_req*
cl_batch_node_req_create(cl_batch_job* p_job, cl_cluster_node* p_node)
{
	size_t size = sizeof(cl_batch_node_req) + event_get_struct_event_size();
	cl_batch_node_req* this = (cl_batch_node_req*)malloc(size);

	if (! this) {
		cf_error("batch node request allocation failed");
		return NULL;
	}

	memset((void*)this, 0, size);

	this->p_job = p_job;
	this->p_node = p_node;
	this->n_digests = 1;

	this->fd = -1;

	return this;
}

//------------------------------------------------
// Destroy a cl_batch_node_req object. Aborts
// ongoing transaction if needed.
//
static void
cl_batch_node_req_destroy(cl_batch_node_req* this)
{
	if (this->event_added) {
		event_del((struct event*)this->event_space);
	}

	if (this->fd > -1) {
		// We only get here if the batch job timed out and is aborting this node
		// request. We can't re-use the socket - it may have unprocessed data.
		close(this->fd);
		cl_cluster_node_dun(this->p_node, DUN_USER_TIMEOUT);
	}

	// This balances the ref-counts we incremented in get_many().
	for (int i = 0; i < this->n_digests; i++) {
		cl_cluster_node_put(this->p_node);
	}

	if (this->wbuf) {
		free(this->wbuf);
	}

	if (this->rbuf) {
		free(this->rbuf);
	}

	free(this);
}

//------------------------------------------------
// Member access function.
//
static inline const cl_cluster_node*
cl_batch_node_req_get_node(cl_batch_node_req* this)
{
	return this->p_node;
}

//------------------------------------------------
// Increment number of digests to be queried on
// this node.
//
static inline void
cl_batch_node_req_add_digest(cl_batch_node_req* this)
{
	this->n_digests++;
}

//------------------------------------------------
// Fill the write buffer with the proto data for
// this node request.
//
static bool
cl_batch_node_req_compile(cl_batch_node_req* this, const char* ns,
		size_t ns_len, const cf_digest* all_digests, int n_all_digests,
		const char** bins, int n_bins, bool get_bin_data,
		cl_cluster_node** nodes)
{
	size_t digests_size = this->n_digests * sizeof(cf_digest);

	// Calculate total message size.

	// AKG - The C client has an extra +1 in the digests field size which I
	// think is wrong (though harmless). I removed that here.
	size_t msg_size =
			sizeof(as_msg) +								// header
			sizeof(cl_msg_field) + ns_len +					// namespace field
			sizeof(cl_msg_field) + digests_size;			// digests field

	for (int b = 0; b < n_bins; b++) {
		msg_size += sizeof(cl_msg_op) + strlen(bins[b]);	// ops (bin names)
	}

	// Allocate the buffer.
	this->wbuf = (uint8_t*)malloc(msg_size);

	if (! this->wbuf) {
		cf_error("batch node request wbuf allocation failed");
		return false;
	}

	this->wbuf_size = msg_size;

	// Write the header.
	uint8_t* p_write = cl_write_header(this->wbuf, msg_size,
			CL_MSG_INFO1_READ | (get_bin_data ? 0 : CL_MSG_INFO1_NOBINDATA), 0,
			0, 0, 0, 2, n_bins);

	// Write the (two) fields.
	p_write = cl_batch_node_req_write_fields(this, p_write, ns, ns_len,
			all_digests, n_all_digests, nodes, digests_size);

	// Write the ops (bin name filter) if any.
	cl_msg_op* op = (cl_msg_op*)p_write;

	for (int b = 0; b < n_bins; b++) {
		size_t len = strlen(bins[b]);

		op->op_sz = sizeof(cl_msg_op) + (uint32_t)len - sizeof(uint32_t);
		op->op = CL_MSG_OP_READ;
		op->particle_type = CL_PARTICLE_TYPE_NULL;
		op->version = 0;
		op->name_sz = (uint8_t)len;

		memcpy(op->name, bins[b], len);

		cl_msg_op* op_tmp = cl_msg_op_get_next(op);
		cl_msg_swap_op(op);
		op = op_tmp;
	}

	return true;
}

//------------------------------------------------
// Compile helper - fill in the message fields.
//
static uint8_t*
cl_batch_node_req_write_fields(cl_batch_node_req* this, uint8_t* p_write,
		const char* ns, size_t ns_len, const cf_digest* all_digests,
		int n_all_digests, cl_cluster_node** nodes, size_t digests_size)
{
	cl_msg_field* mf = (cl_msg_field*)p_write;

	// Write the namespace field.
	mf->type = CL_MSG_FIELD_TYPE_NAMESPACE;
	mf->field_sz = 1 + ns_len;

	memcpy(mf->data, ns, ns_len);

	cl_msg_field* mf_tmp = cl_msg_field_get_next(mf);
	cl_msg_swap_field(mf);
	mf = mf_tmp;

	// Write the digests field.
	mf->type = CL_MSG_FIELD_TYPE_DIGEST_RIPE_ARRAY;
	mf->field_sz = 1 + digests_size;

	cf_digest* p_digest = (cf_digest*)mf->data;

	for (int i = 0; i < n_all_digests; i++) {
		if (nodes[i] == this->p_node) {
			*p_digest++ = all_digests[i];
		}
	}

	mf_tmp = cl_msg_field_get_next(mf);
	cl_msg_swap_field(mf);

	return (uint8_t*)mf_tmp;
}

//------------------------------------------------
// Get a socket for this node request.
//
static bool
cl_batch_node_req_get_fd(cl_batch_node_req* this)
{
	while (this->fd == -1) {
		this->fd = cl_cluster_node_fd_get(this->p_node);
		// Note - apparently 0 is a legitimate fd value.

		if (this->fd < -1) {
			cl_cluster_node_dun(this->p_node, DUN_RESTART_FD);
			// This object's destructor will release node.
			return false;
		}
	};

	return true;
}

//------------------------------------------------
// Start this node request's transaction.
//
static void
cl_batch_node_req_start(cl_batch_node_req* this)
{
	event_assign((struct event*)this->event_space,
			cl_batch_job_get_base(this->p_job), this->fd, EV_WRITE,
			cl_batch_node_req_event, this);

	// In "cross-threaded" requests, don't access member data after adding the
	// event - the callback may occur and destroy this object immediately.

	this->event_added = true;

	if (0 != event_add((struct event*)this->event_space, 0)) {
		cf_warn("batch node request add event failed: will get partial result");
		this->event_added = false;
	}
}

//------------------------------------------------
// The libevent2 socket event callback function.
// Used during both send and receive phases. Hands
// off to appropriate handler, and re-adds event
// if transaction is not done.
//
static void
cl_batch_node_req_event(int fd, short event, void* pv_this)
{
	cl_batch_node_req* this = (cl_batch_node_req*)pv_this;

	this->event_added = false;

	bool transaction_done;

	if (event & EV_WRITE) {
		// Handle write phase.
		transaction_done = cl_batch_node_req_handle_send(this);
	}
	else if (event & EV_READ) {
		// Handle read phase.
		transaction_done = cl_batch_node_req_handle_recv(this);
	}
	else {
		// Should never happen.
		cf_error("unexpected event flags %d", event);
		cl_batch_node_req_done(this, EV2CITRUSLEAF_FAIL_CLIENT_ERROR);
		return;
	}

	if (! transaction_done) {
		// There's more to do, re-add event.
		if (0 == event_add((struct event*)this->event_space, 0)) {
			this->event_added = true;
		}
		else {
			cf_error("batch node request add event failed");
			cl_batch_node_req_done(this, EV2CITRUSLEAF_FAIL_CLIENT_ERROR);
		}
	}
}

//------------------------------------------------
// Handle send phase socket callbacks. Switches
// event to read mode when send phase is done.
//
static bool
cl_batch_node_req_handle_send(cl_batch_node_req* this)
{
	while(true) {
		// Loop until everything is sent or we get would-block.

		if (this->wbuf_pos >= this->wbuf_size) {
			cf_error("unexpected write event");
			cl_batch_node_req_done(this, EV2CITRUSLEAF_FAIL_CLIENT_ERROR);
			return true;
		}

		int rv = send(this->fd, &this->wbuf[this->wbuf_pos],
				this->wbuf_size - this->wbuf_pos, MSG_DONTWAIT | MSG_NOSIGNAL);

		if (rv > 0) {
			this->wbuf_pos += rv;

			// If done sending, switch to receive mode.
			if (this->wbuf_pos == this->wbuf_size) {
				event_assign((struct event*)this->event_space,
						cl_batch_job_get_base(this->p_job), this->fd, EV_READ,
						cl_batch_node_req_event, this);
				break;
			}

			// Loop, send what's left.
		}
		else if (rv == 0 || (errno != EAGAIN && errno != EWOULDBLOCK)) {
			// send() supposedly never returns 0.
			cf_debug("send failed: fd %d rv %d errno %d", this->fd, rv, errno);
			cl_batch_node_req_done(this, EV2CITRUSLEAF_FAIL_UNKNOWN);
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

//------------------------------------------------
// Handle receive phase socket callbacks. Parses
// received proto data, detects when transaction
// is complete, and reports to parent batch job.
//
static bool
cl_batch_node_req_handle_recv(cl_batch_node_req* this)
{
	while (true) {
		// Loop until everything is read from socket or we get would-block.

		if (this->hbuf_pos < sizeof(cl_proto)) {
			// Read proto header.

			int rv = recv(this->fd, &this->hbuf[this->hbuf_pos],
					sizeof(cl_proto) - this->hbuf_pos,
					MSG_DONTWAIT | MSG_NOSIGNAL);

			if (rv > 0) {
				this->hbuf_pos += rv;
				// Loop, read more header or start reading body.
			}
			else if (rv == 0) {
				// Connection has been closed by the server.
				cf_debug("recv connection closed: fd %d", this->fd);
				cl_batch_node_req_done(this, EV2CITRUSLEAF_FAIL_UNKNOWN);
				return true;
			}
			else if (errno != EAGAIN && errno != EWOULDBLOCK) {
				cf_debug("recv failed: rv %d errno %d", rv, errno);
				cl_batch_node_req_done(this, EV2CITRUSLEAF_FAIL_UNKNOWN);
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
			if (! this->rbuf) {
				cl_proto* proto = (cl_proto*)this->hbuf;

				cl_proto_swap(proto);

				this->rbuf_size = proto->sz;
				this->rbuf = (uint8_t*)malloc(this->rbuf_size);

				if (! this->rbuf) {
					cf_error("batch node request rbuf allocation failed");
					cl_batch_node_req_done(this,
							EV2CITRUSLEAF_FAIL_CLIENT_ERROR);
					return true;
				}
			}

			if (this->rbuf_pos >= this->rbuf_size) {
				cf_error("unexpected read event");
				cl_batch_node_req_done(this, EV2CITRUSLEAF_FAIL_CLIENT_ERROR);
				return true;
			}

			int rv = recv(this->fd, &this->rbuf[this->rbuf_pos],
					this->rbuf_size - this->rbuf_pos,
					MSG_DONTWAIT | MSG_NOSIGNAL);

			if (rv > 0) {
				this->rbuf_pos += rv;

				if (this->rbuf_pos == this->rbuf_size) {
					// Done with proto body.

					bool is_last;
					int result = cl_batch_node_req_parse_proto_body(this,
							&is_last);

					if (is_last || result != EV2CITRUSLEAF_OK) {
						// Done with last proto (or parse error).
						cl_batch_node_req_done(this, result);
						return true;
					}
					else {
						// We expect another proto - reset read buffers.
						this->hbuf_pos = 0;
						free(this->rbuf);
						this->rbuf = NULL;
						this->rbuf_size = 0;
						this->rbuf_pos = 0;
					}
				}

				// Loop, read more body or next header.
			}
			else if (rv == 0) {
				// Connection has been closed by the server.
				cf_debug("recv connection closed: fd %d", this->fd);
				cl_batch_node_req_done(this, EV2CITRUSLEAF_FAIL_UNKNOWN);
				return true;
			}
			else if (errno != EAGAIN && errno != EWOULDBLOCK) {
				cf_debug("recv failed: rv %d errno %d", rv, errno);
				cl_batch_node_req_done(this, EV2CITRUSLEAF_FAIL_UNKNOWN);
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

//------------------------------------------------
// Parse messages in proto body. Report record
// results to parent batch job.
//
static int
cl_batch_node_req_parse_proto_body(cl_batch_node_req* this, bool* p_is_last)
{
	// A proto body should contain either:
	// a batch of record results where each record result is a cl_msg, or:
	// a single cl_msg marked "last" but otherwise empty.

	*p_is_last = false;

	uint8_t* p_read = this->rbuf;
	uint8_t* p_end = p_read + this->rbuf_size;

	while (p_read < p_end) {
		// Parse the header.
		cl_msg* msg = (cl_msg*)p_read;

		if (msg->data > p_end) {
			cf_warn("illegal response header format");
			return EV2CITRUSLEAF_FAIL_UNKNOWN;
		}

		cl_msg_swap_header(msg);

		// If this is the last proto body, we're done.
		if (msg->info3 & CL_MSG_INFO3_LAST) {
			*p_is_last = true;

			// Some sanity checks.
			if (msg->result_code != CL_RESULT_OK || msg->data < p_end) {
				cf_warn("bad last proto body");
				return EV2CITRUSLEAF_FAIL_UNKNOWN;
			}

			return EV2CITRUSLEAF_OK;
		}

		// Record result codes other than OK and NOTFOUND should never come from
		// the server.
		if (msg->result_code != CL_RESULT_OK &&
				msg->result_code != CL_RESULT_NOTFOUND) {
			cf_warn("batch response record result %u", msg->result_code);
			// Let it become the node result.
			return (int)msg->result_code;
		}

		ev2citrusleaf_rec* p_rec = cl_batch_job_get_rec(this->p_job);

		p_rec->result = (int)msg->result_code;
		p_rec->generation = msg->generation;

		uint32_t now = cl_batch_job_clepoch_seconds(this->p_job);

		p_rec->expiration = msg->record_ttl > now ? msg->record_ttl - now : 0;

		// Parse the fields.
		bool got_digest = false;
		cl_msg_field* mf = (cl_msg_field*)msg->data;

		for (int i = 0; i < (int)msg->n_fields; i++) {
			if ((uint8_t*)(mf + 1) > p_end) {
				cf_warn("illegal response field format");
				return EV2CITRUSLEAF_FAIL_UNKNOWN;
			}

			cl_msg_swap_field(mf);

			cl_msg_field* next_mf = cl_msg_field_get_next(mf);

			if ((uint8_t*)next_mf > p_end) {
				cf_warn("illegal response field data format");
				return EV2CITRUSLEAF_FAIL_UNKNOWN;
			}

			switch (mf->type) {
			case CL_MSG_FIELD_TYPE_DIGEST_RIPE:
				p_rec->digest = *(cf_digest*)mf->data;
				got_digest = true;
				break;
			default:
				// Skip fields we don't care about, including namespace and set.
				break;
			}

			mf = next_mf;
		}

		if (! got_digest) {
			cf_warn("batch response missing digest");
			return EV2CITRUSLEAF_FAIL_UNKNOWN;
		}

		// Parse the ops, if any - this is the bin data.
		cl_msg_op* op = (cl_msg_op*)mf;

		p_rec->bins = NULL;
		p_rec->n_bins = (int)msg->n_ops;

		if (msg->n_ops > 0) {
			p_rec->bins = (ev2citrusleaf_bin*)
					malloc(msg->n_ops * sizeof(ev2citrusleaf_bin));

			if (! p_rec->bins) {
				cf_error("batch response bins allocation failed");
				return EV2CITRUSLEAF_FAIL_CLIENT_ERROR;
			}
		}

		for (int i = 0; i < (int)msg->n_ops; i++) {
			if ((uint8_t*)(op + 1) > p_end) {
				cf_warn("illegal response op format");
				free(p_rec->bins);
				return EV2CITRUSLEAF_FAIL_UNKNOWN;
			}

			cl_msg_swap_op(op);

			cl_msg_op* next_op = cl_msg_op_get_next(op);

			if ((uint8_t*)next_op > p_end) {
				cf_warn("illegal response op data format");
				free(p_rec->bins);
				return EV2CITRUSLEAF_FAIL_UNKNOWN;
			}

			cl_set_value_particular(op, &p_rec->bins[i]);
			op = next_op;
		}

		p_read = (uint8_t*)op;

		// Inform the job object it now owns this record, and is responsible for
		// freeing the bins.
		cl_batch_job_rec_done(this->p_job);

		this->n_recs++;

		// Sanity check, ignore extra data.
		if (this->n_recs == this->n_digests && p_read < p_end) {
			cf_warn("got last record in batch response but there's more data");
			break;
		}
	}

	return this->n_recs == this->n_digests ?
			EV2CITRUSLEAF_OK : EV2CITRUSLEAF_FAIL_UNKNOWN;
}

//------------------------------------------------
// Report that this node request is complete. If
// it succeeded entirely, replace the socket in
// the pool for re-use.
//
static void
cl_batch_node_req_done(cl_batch_node_req* this, int node_result)
{
	if (node_result == EV2CITRUSLEAF_OK) {
		// The socket is ok, re-use it and approve the node.

		// AKG - We trust there's no more data in the socket. We'll re-use this
		// socket and if there's more to read, the next transaction will suffer.

		cl_cluster_node_fd_put(this->p_node, this->fd);
		cl_cluster_node_ok(this->p_node);
	}
	else {
		// The socket may have unprocessed data or otherwise be untrustworthy,
		// close it and disapprove the node.
		close(this->fd);
		cl_cluster_node_dun(this->p_node, DUN_NETWORK_ERROR);
	}

	// Reset this->fd so the destructor doesn't close it.
	this->fd = -1;

	// Tell the job object this node request is done (destroys this object).
	cl_batch_job_node_done(this->p_job, this, node_result);
}
