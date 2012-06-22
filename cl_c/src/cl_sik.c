/*
 * The batch interface has some cleverness, it makes parallel requests
 * under the covers to different servers
 *
 *
 * Brian Bulkowski, 2011
 * All rights reserved
 */

#include <sys/types.h>
#include <stdio.h>
#include <errno.h> //errno
#include <stdlib.h> //fprintf
#include <unistd.h> // close
#include <string.h>
#include <pthread.h>
#include <fcntl.h>
#include <assert.h>
#include <zlib.h>


#include "citrusleaf/citrusleaf.h"
#include "citrusleaf/cl_cluster.h"
#include "citrusleaf/citrusleaf-internal.h"
#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/proto.h"
#include "citrusleaf/cf_socket.h"

//#define SIK_DEBUG


int   NumNodes  = 0;
int   Responses = 0;
static cl_rv citrusleaf_sik_traversal(cl_cluster *asc, char *ns, const cf_digest *digests, int n_digests, cl_bin *bins, int n_bins, bool get_key, citrusleaf_get_many_cb cb, void *udata, 
			unsigned int mrjid, char *lua_mapf, char *lua_rdcf, char *lua_fnzf, int imatch, map_args_t *margs, int reg_mrjid) {

    int lmflen = lua_mapf ? strlen(lua_mapf) : 0;
    int lrflen = lua_rdcf ? strlen(lua_rdcf) : 0;
    int lfflen = lua_fnzf ? strlen(lua_fnzf) : 0;

#ifdef SIK_DEBUG
    printf("citrusleaf_sik_traversal: mrjid: %d "\
           "lmflen: %d lrflen: %d lfflen: %d\n",
           mrjid, lmflen, lrflen, lfflen);
#endif
	int	n_nodes = cf_vector_size(&asc->node_v);
    NumNodes = n_nodes; //NOTE: used in callbacks -> num responses
	cl_cluster_node **nodes = malloc(sizeof(cl_cluster_node *) * n_nodes);
	if (!nodes) { fprintf(stderr, " allocation failed "); return(-1); }
    for (int i = 0; i < n_nodes; i++) {
        nodes[i] = cf_vector_pointer_get(&asc->node_v, i);
    }

#ifdef SIK_DEBUG
    printf("citrusleaf_sik_traversal: n_digests: %d n_bins: %d\n",
           n_digests, n_bins);
#endif

	// Note:  The digest exists case does not retrieve bin data.
	cl_batch_work work;
	work.asc        = asc;
	work.info1      = CL_MSG_INFO1_READ;
	work.info2      = 0;
	work.info3      = 0;
	work.ns         = ns;
	work.digests    = (cf_digest *)digests;
	work.nodes      = nodes;
	work.n_digests  = n_digests;
	work.get_key    = get_key;
	work.bins       = bins;
	work.operator   = CL_OP_READ;
	work.operations = 0;
	work.n_ops      = n_bins;
	work.cb         = cb;
	work.udata      = udata;
    work.mrjid      = mrjid;
    work.lua_mapf   = lua_mapf;
    work.lmflen     = lmflen;
    work.lua_rdcf   = lua_rdcf;
    work.lrflen     = lrflen;
    work.lua_fnzf   = lua_fnzf;
    work.lfflen     = lfflen;
    work.imatch     = imatch;
    work.margs      = margs;
    work.reg_mrjid  = reg_mrjid;
	work.complete_q = cf_queue_create(sizeof(int),true);

	// dispatch work to the worker queue to allow the transactions in parallel
	for (int i=0;i<n_nodes;i++) {
		// fill in per-request specifics
		work.my_node = nodes[i];
        //printf("%d: n_nodes: %d my_node: %p\n", i, n_nodes, work.my_node);
		work.my_node_digest_count = n_digests;
		work.index = i;
		// dispatch - copies data
		cf_queue_push(g_batch_q, &work);
	}
	
	// wait for the work to complete
	int retval = 0;
	for (int i=0;i<n_nodes;i++) {
		int z;
		cf_queue_pop(work.complete_q, &z, CF_QUEUE_FOREVER);
		if (z != 0) retval = z;
	}
	
	// free and return what needs freeing and putting
	cf_queue_destroy(work.complete_q);
	//for (int i=0;i<n_digests;i++) { cl_cluster_node_put(nodes[i]); }
	free(nodes);

	if (retval != 0) return( CITRUSLEAF_FAIL_CLIENT );
	else             return 0;
}

//
// 
//

cl_rv citrusleaf_get_sik_digest(cl_cluster *asc, char *ns, const cf_digest *digests, int n_digests, cl_bin *bins, int n_bins, bool get_key, citrusleaf_get_many_cb cb, void *udata, int imatch) {
    Responses = 0;
    return citrusleaf_sik_traversal(asc, ns, digests, n_digests, bins, n_bins, get_key, cb, udata, 0, NULL, NULL, NULL, imatch, NULL, 0);
}


int   CurrentMRJid      = -1;

cl_rv citrusleaf_run_mr_sik_digest(cl_cluster *asc, char *ns, const cf_digest *digests, int n_digests, cl_bin *bins, int n_bins, bool get_key, citrusleaf_get_many_cb cb, void *udata, int mrjid, int imatch, map_args_t *margs) {
    CurrentMRJid = mrjid;
    Responses    = 0;
printf("citrusleaf_run_mr_sik_digest: CurrentMRJid: %d Responses: %d\n", CurrentMRJid, Responses);
    return citrusleaf_sik_traversal(asc, ns, digests, n_digests, bins, n_bins, get_key, cb, udata, mrjid, NULL, NULL, NULL, imatch, margs, 0);
}



//
// Registering LUA functions
// 


char *CurrentLuaMapFunc = NULL;
char *CurrentLuaRdcFunc = NULL;
char *CurrentLuaFnzFunc = NULL;

cl_rv citrusleaf_register_lua_function(cl_cluster *asc, char *ns, citrusleaf_get_many_cb cb, char *lua_mapf, char *lua_rdcf, char *lua_fnzf, int reg_mrjid) {
    CurrentLuaMapFunc = lua_mapf;
    CurrentLuaRdcFunc = lua_rdcf;
    CurrentLuaFnzFunc = lua_fnzf;
    return citrusleaf_sik_traversal(asc, ns, NULL, 0, NULL, 0, 0, cb, NULL, 0, lua_mapf, lua_rdcf, lua_fnzf, -1, NULL, reg_mrjid);
}


