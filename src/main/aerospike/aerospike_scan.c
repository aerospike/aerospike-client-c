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

#include <aerospike/aerospike_scan.h>
#include <aerospike/aerospike_info.h>
#include <aerospike/as_key.h>

#include <citrusleaf/as_scan.h>
#include <citrusleaf/cl_scan.h>
#include <citrusleaf/cf_random.h>

#include "_log.h"
#include "_policy.h"
#include "_shim.h"

/******************************************************************************
 * LOCAL STRUCTURES
 *****************************************************************************/

/******************************************************************************
 * TYPES
 *****************************************************************************/

typedef struct scan_bridge_s {

	// user-provided data
	void * udata;

	// user-provided callback
	aerospike_scan_foreach_callback	callback;

} scan_bridge;

/******************************************************************************
 * FUNCTION DECLS
 *****************************************************************************/

as_status aerospike_scan_init(aerospike * as, as_error * err);

/******************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/

static void as_scan_toclscan(const as_scan * scan, const as_policy_scan * policy, cl_scan * clscan, bool background, uint64_t * job_id) 
{
	clscan->job_id = 0;
	clscan->ns = (char *) scan->ns;
	clscan->setname = (char *) scan->set;
	clscan->params.fail_on_cluster_change = policy->fail_on_cluster_change;
	clscan->params.priority = (cl_scan_priority)scan->priority;
	clscan->params.pct = scan->percent;
	clscan->params.concurrent = scan->concurrent;

	clscan->udf.type = CL_SCAN_UDF_NONE;
	clscan->udf.filename = NULL;
	clscan->udf.function = NULL;
	clscan->udf.arglist = NULL;

	if ( background ) {
		if ( job_id != NULL ) {
			clscan->job_id = *job_id;
		}

		if ( clscan->job_id == 0 ) {
			clscan->job_id = (cf_get_rand64())/2;
			if ( job_id != NULL ) {
				*job_id = clscan->job_id;
			}
		}

		if ( scan->apply_each.module[0] != '\0' && scan->apply_each.function[0] != '\0' ) {
			clscan->udf.type = CL_SCAN_UDF_BACKGROUND;
			clscan->udf.filename = (char *) scan->apply_each.module;
			clscan->udf.function = (char *) scan->apply_each.function;
			clscan->udf.arglist = scan->apply_each.arglist;
		}
	}
}

static as_status process_node_response(cf_vector *v, as_error *err)
{
	as_status rc = AEROSPIKE_OK;
	
	// This returns a vector of return values, the size of which is the size of the cluster
	int sz = cf_vector_size(v);
	cl_node_response resp;
	for(int i=0; i < sz; i++) {

		cf_vector_get(v, i, &resp);
		// Even if one of the node responded with an error, set the overall status as error
		if (resp.node_response != CITRUSLEAF_OK) {
			rc = as_error_fromrc(err, resp.node_response);
		}

		// Set the resp back to zero
		memset(&resp, 0, sizeof(cl_node_response));
	}

	// Free the result vector
	cf_vector_destroy(v);

	return rc;
}

/**
 * The is the proxy callback function. This will be passed to the old simple-scan
 * mechanism. When this gets called, it will create an as_val structure out of the
 * record and will call the callback that user supplied (folded into the udata structure)
 */
static int simplescan_cb(char *ns, cf_digest *keyd, char *set, cl_object *key,
		int result, uint32_t generation, uint32_t record_void_time,
		cl_bin *bins, uint16_t n_bins, void *udata)
{
	scan_bridge * bridge = (scan_bridge *) udata;

	// Fill the bin data
	as_record _rec, * rec = &_rec;
	as_record_inita(rec, n_bins);
	clbins_to_asrecord(bins, (uint32_t)n_bins, rec);

	// Fill the metadata
	askey_from_clkey(&rec->key, ns, set, key);
	memcpy(rec->key.digest.value, keyd, sizeof(cf_digest));
	rec->key.digest.init = true;
	rec->gen = generation;
	rec->ttl = record_void_time;

	// Call the callback that user wanted to callback
	bridge->callback((as_val *) rec, bridge->udata);

	// The responsibility to free the bins is on the called callback function
	// In scan case, only LIST & MAP will have an active free

	citrusleaf_bins_free(bins, (int)n_bins);
	// release the record
	as_record_destroy(rec);

	return 0;
}

/**
 * The is the proxy callback function. This will be passed to the old simple-scan
 * mechanism. When this gets called, it will create an as_val structure out of the
 * record and will call the callback that user supplied (folded into the udata structure)
 */
static int generic_cb(as_val * val, void * udata)
{
	scan_bridge * bridge = (scan_bridge *) udata;
	
	// Call the callback that user wanted to callback
	bridge->callback(val, bridge->udata);
	
	return 0;
}

/**
 * This is the main driver function which can cater to different types of
 * scan interfaces exposed to the outside world. This functions should not be
 * exposed to the outside world. This generic function exists because we dont
 * want to duplicate too much of code.
 * 
 * @param as        - the aerospike cluster to connect to.
 * @param err       - the error is populated if the return value is not AEROSPIKE_OK.
 * @param policy    - the policy to use for this operation. If NULL, then the default policy will be used.
 * @param node      - the name of the node to perform the scan on.
 * @param scan      - the scan to perform
 * @param callback  - the function to be called for each record scanned.
 * @param udata     - user-data to be passed to the callback
 *
 * @return AEROSPIKE_OK on success. Otherwise an error occurred.
 */
static as_status aerospike_scan_generic(
	aerospike * as, as_error * err, const as_policy_scan * policy,
	const char * node, const as_scan * scan,
	aerospike_scan_foreach_callback callback, void * udata)
{

	cl_rv clrv;
	as_status rc = AEROSPIKE_OK;

	cl_scan clscan;
	as_scan_toclscan(scan, policy, &clscan, false, NULL);

	if ( clscan.udf.type == CL_SCAN_UDF_NONE ) {

		scan_bridge bridge_udata = {
			.udata = udata,
			.callback = callback
		};

		struct cl_scan_parameters_s params = {
			.fail_on_cluster_change = clscan.params.fail_on_cluster_change,
			.priority = clscan.params.priority,
			.concurrent = clscan.params.concurrent,
			.threads_per_node = 0
		};

		int n_bins = scan->select.size;
		cl_bin * bins = NULL;
		if ( n_bins > 0 ) {
			bins = (cl_bin *) alloca(sizeof(cl_bin) * n_bins);
			for( int i = 0; i < n_bins; i++ ) {
				strcpy(bins[i].bin_name, scan->select.entries[i]);
				citrusleaf_object_init_null(&bins[i].object);
			}
		}


		// If the user want to execute only on a single node...
		if (node) {
			clrv = citrusleaf_scan_node(as->cluster, (char *) node, (char *) scan->ns, (char *) scan->set, bins, n_bins, 
						scan->no_bins, scan->percent, simplescan_cb, &bridge_udata, &params);
			rc = as_error_fromrc(err, clrv);
		} 
		else {

			// We are not using the very old citrusleaf_scan() call here. First of all, its
			// very inefficient. It makes a single node on the cluster coordinate the job
			// of scan. Moreover, it does not accept params like priority etc.
			cf_vector *v = citrusleaf_scan_all_nodes(as->cluster, (char *) scan->ns, (char *) scan->set, bins, n_bins, 
						scan->no_bins, scan->percent, simplescan_cb, &bridge_udata, &params);
			rc = process_node_response(v, err);
		}

	}
	else {
		// If the user want to execute only on a single node...
		if (node) {
			clrv = citrusleaf_udf_scan_node(as->cluster, &clscan, (char *)node, generic_cb, udata);
			rc = as_error_fromrc(err, clrv);
		} 
		else {

			cf_vector *v = citrusleaf_udf_scan_all_nodes(as->cluster, &clscan, generic_cb, udata);
			rc = process_node_response(v, err);
		}
	}

    // If completely successful, make the callback that signals completion.
	if (rc == AEROSPIKE_OK) {
		callback(NULL, udata);
	}

	return rc;
}

// Wrapper for background scan info.
typedef struct bg_scan_info_s {
	char job_id[32];
	int job_id_len;
	as_scan_info * info;
} bg_scan_info;

const char JOB_STATUS_TAG[] = "job_status=";
const int JOB_STATUS_TAG_LEN = sizeof(JOB_STATUS_TAG) - 1;

const char JOB_PROGRESS_TAG[] = "job_progress(%)=";
const int JOB_PROGRESS_TAG_LEN = sizeof(JOB_PROGRESS_TAG) - 1;

const char SCANNED_RECORDS_TAG[] = "scanned_records=";
const int SCANNED_RECORDS_TAG_LEN = sizeof(SCANNED_RECORDS_TAG) - 1;

/**
 * The info callback made for each node when doing aerospike_scan_info().
 */
static bool
scan_info_cb(const as_error * err, const as_node * node, const char * req, char * res, void * udata)
{
	bg_scan_info* p_bsi = (bg_scan_info*)udata;

	// For now, fast and dirty parsing for exactly what we're looking for...
	// If we can't find the expected tag on this node, something's wrong, but
	// try the others. (OK? Or should we ever return false and wipe the info?)

	char* p_read = strstr(res, p_bsi->job_id);
	if (! p_read) {
		return true;
	}
	p_read += p_bsi->job_id_len;

	// If any node is aborted, we're aborted overall, don't bother parse status.
	if (p_bsi->info->status != AS_SCAN_STATUS_ABORTED) {
		p_read = strstr(p_read, JOB_STATUS_TAG);
		if (! p_read) {
			return true;
		}
		p_read += JOB_STATUS_TAG_LEN;

		if (strncmp(p_read, "ABORTED", 7) == 0) {
			p_bsi->info->status = AS_SCAN_STATUS_ABORTED;
		}
		else if (strncmp(p_read, "IN PROGRESS", 11) == 0) {
			// Otherwise if any node is in progress, we're in progress overall.
			p_bsi->info->status = AS_SCAN_STATUS_INPROGRESS;
		}
		else if (p_bsi->info->status == AS_SCAN_STATUS_UNDEF &&
				strncmp(p_read, "DONE", 4) == 0) {
			// Only if we haven't modified the status - if a prior node was in
			// progress, overall we're in progress.
			p_bsi->info->status = AS_SCAN_STATUS_COMPLETED;
		}
	}

	p_read = strstr(p_read, JOB_PROGRESS_TAG);
	if (! p_read) {
		return true;
	}
	p_read += JOB_PROGRESS_TAG_LEN;

	// Be pessimistic - use the slowest node's progress.
	uint32_t pct = atoi(p_read);
	if (p_bsi->info->progress_pct == 0 || pct < p_bsi->info->progress_pct) {
		p_bsi->info->progress_pct = pct;
	}

	p_read = strstr(p_read, SCANNED_RECORDS_TAG);
	if (! p_read) {
		return true;
	}
	p_read += SCANNED_RECORDS_TAG_LEN;

	// Accumulate total.
	p_bsi->info->records_scanned += atoi(p_read);

	return true;
}


/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

/**
 *	Scan the records in the specified namespace and set in the cluster.
 *
 *	Scan will be run in the background by a thread on client side.
 *	No callback will be called in this case.
 *	
 *	~~~~~~~~~~{.c}
 *	as_scan scan;
 *	as_scan_init(&scan, "test", "demo");
 *	as_scan_apply_each(&scan, "udf_module", "udf_function", NULL);
 *	
 *	uint64_t scanid = 0;
 *	
 *	if ( aerospike_scan_background(&as, &err, NULL, &scan, &scanid) != AEROSPIKE_OK ) {
 *		fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *	}
 *	else {
 *		printf("Running background scan job: %ll", scanid);
 *	}
 *
 *	as_scan_destroy(&scan);
 *	~~~~~~~~~~
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param scan 		The scan to execute against the cluster.
 *	@param scan_id		The id for the scan job, which can be used for querying the status of the scan.
 *
 *	@return AEROSPIKE_OK on success. Otherwise an error occurred.
 */
as_status aerospike_scan_background(
	aerospike * as, as_error * err, const as_policy_scan * policy, 
	const as_scan * scan, uint64_t * scan_id
	)
{
	// we want to reset the error so, we have a clean state
	as_error_reset(err);
	
	// resolve policies
	as_policy_scan p;
	as_policy_scan_resolve(&p, &as->config.policies, policy);
	
	if ( aerospike_scan_init(as, err) != AEROSPIKE_OK ) {
		return err->code;
	}

	cl_scan clscan;
	as_scan_toclscan(scan, &p, &clscan, true, scan_id);

	cf_vector *v = citrusleaf_udf_scan_background(as->cluster, &clscan);
	as_status rc = process_node_response(v, err);
	return rc;
}

/**
 *	Check on a background scan running on the server.
 *
 *	~~~~~~~~~~{.c}
 *	uint64_t scan_id = 1234;
 *	as_scan_info scan_info;
 *
 *	if ( aerospike_scan_info(&as, &err, NULL, &scan, scan_id, &scan_info) != AEROSPIKE_OK ) {
 *		fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *	}
 *	else {
 *		printf("Scan id=%ll, status=%s", scan_id, scan_info.status);
 *	}
 *	~~~~~~~~~~
 *
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param scan_id		The id for the scan job to check the status of.
 *	@param info			Information about this scan, to be populated by this operation.
 *
 *	@return AEROSPIKE_OK on success. Otherwise an error occurred.
 */
as_status aerospike_scan_info(
	aerospike * as, as_error * err, const as_policy_info * policy,
	uint64_t scan_id, as_scan_info * info
	)
{
	// Initialize the info...
	info->status = AS_SCAN_STATUS_UNDEF;
	info->progress_pct = 0;
	info->records_scanned = 0;

	bg_scan_info bsi;
	bsi.job_id_len = sprintf(bsi.job_id, "job_id=%" PRIu64 ":", scan_id);
	bsi.info = info;

	return aerospike_info_foreach(as, err, policy, "scan-list\n", scan_info_cb, (void *) &bsi);
}

/**
 *	Scan the records in the specified namespace and set in the cluster.
 *
 *	Call the callback function for each record scanned. When all records have 
 *	been scanned, then callback will be called with a NULL value for the record.
 *
 *	~~~~~~~~~~{.c}
 *	as_scan scan;
 *	as_scan_init(&scan, "test", "demo");
 *	
 *	if ( aerospike_scan_foreach(&as, &err, NULL, &scan, callback, NULL) != AEROSPIKE_OK ) {
 *		fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *	}
 *
 *	as_scan_destroy(&scan);
 *	~~~~~~~~~~
 *	
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param scan			The scan to execute against the cluster.
 *	@param callback		The function to be called for each record scanned.
 *	@param udata		User-data to be passed to the callback.
 *
 *	@return AEROSPIKE_OK on success. Otherwise an error occurred.
 */
as_status aerospike_scan_foreach(
	aerospike * as, as_error * err, const as_policy_scan * policy, 
	const as_scan * scan, 
	aerospike_scan_foreach_callback callback, void * udata) 
{
	// we want to reset the error so, we have a clean state
	as_error_reset(err);
	
	// resolve policies
	as_policy_scan p;
	as_policy_scan_resolve(&p, &as->config.policies, policy);
	
	if ( aerospike_scan_init(as, err) != AEROSPIKE_OK ) {
		return err->code;
	}

	return aerospike_scan_generic(as, err, &p, NULL, scan, callback, udata);
}

/**
 * Initialize scan environment
 */
as_status aerospike_scan_init(aerospike * as, as_error * err) 
{
	// TODO - failure cases?
	cl_cluster_scan_init(as->cluster);
	return AEROSPIKE_OK;
}
