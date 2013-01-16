#define _GNU_SOURCE
#include <stdio.h>
#include <errno.h> //errno
#include <stdlib.h> //fprintf
#include <unistd.h> // close
#include <string.h>
#include <pthread.h>
#include <fcntl.h>

#include "citrusleaf/citrusleaf.h"
#include "citrusleaf/citrusleaf-internal.h"
#include "citrusleaf/cl_shm.h"
#include "citrusleaf/cl_request.h"
#include "citrusleaf/cf_socket.h"
#include "citrusleaf/cf_log.h"

bool g_shared_memory;

/* Shared memory global variables */
static shm_info g_shm_info;

/* Shared memory pointer */
static shm * g_shm_pt;

/* Shared memory updater thread */
static pthread_t g_shm_update_thr;

static int g_max_nodes;

/* This variable is set to true when we first time do the shm initialization,
 * so that next time if we call shm_init then looking at it we will come to
 * know that we have already done the shm initialization and we do not
 * initialize shm again */
static bool g_shm_initiated = false;

// Is this the process that issues server info requests and updates shared memory.
static bool g_shm_updater = false;

// Seed nodes contain the original nodes created by the non-shared-memory tender.
typedef struct {
	char name[NODE_NAME_SIZE];
	struct sockaddr_in address_array[MAX_ADDRESSES_PER_NODE];
	int address_count;
} cl_seed_node;

static cl_seed_node* g_seed_array = 0;
static int g_seed_count = 0;

static cl_seed_node*
cl_shm_init_seed_array(int seed_count)
{
	if (seed_count > g_seed_count) {
		if (g_seed_array) {
			free(g_seed_array);
		}
		g_seed_array = (cl_seed_node*) malloc(seed_count * sizeof(cl_seed_node));
		g_seed_count = seed_count;
	}
	return g_seed_array;
}

static void
cl_shm_free_seed_array()
{
	if (g_seed_array) {
		free(g_seed_array);
		g_seed_array = 0;
	}
	g_seed_count = 0;
}

void* cl_shm_updater_fn(void *);

/* Initialize shared memory segment */
int citrusleaf_use_shm(int num_nodes, key_t key) 
{
	if (g_shm_initiated) {
		return SHM_OK;
	}

	g_max_nodes = (num_nodes > 0)? num_nodes : DEFAULT_NUM_NODES_FOR_SHM;
	
	if (0==key) {
		key = DEFAULT_SHM_KEY;
	}

	/* Time to use shared memory */
	g_shared_memory = true;
	
	cf_debug("Shared memory key is %d",key);

	if (key == -1) {
		return SHM_ERROR;
	}
	void * shm_pt = (void*)0;
	
	/* The size of the shared memory is the size of the shm structure*/
	g_shm_info.shm_sz = sizeof(shm) + sizeof(shm_ninfo) * g_max_nodes;
	
	/* Fix the update thread end condition to false and update period to 1*/
	g_shm_info.update_thread_end_cond = false;
	g_shm_info.update_period = 1;
	
	// Checking if the value of kernel.shmmax is less than g_shm_info.shm_sz
	// In that case we should exit specifying the error
	size_t shm_max;
	FILE *f = fopen(SHMMAX_SYS_FILE, "r");

	if (!f) {
		cf_error("Failed to open file: %s", SHMMAX_SYS_FILE);
		return SHM_ERROR;
	}

	if (fscanf(f, "%zu", &shm_max) != 1) {
		cf_error("Failed to read shmmax from file: %s", SHMMAX_SYS_FILE);
		fclose(f);
		return SHM_ERROR;
	}

	fclose(f);

	if(shm_max < g_shm_info.shm_sz) {
		cf_error("Shared memory size %zu exceeds system max %zu.", g_shm_info.shm_sz, shm_max);
		cf_error("You can increase shared memory size by: sysctl -w kernel.shmmax=<new_size>");
		return SHM_ERROR;
	}
	
	// First try to exclusively create a shared memory, for this only one process will succeed.
 	// others will fail giving an errno of EEXIST
	 if((g_shm_info.id = shmget(key,g_shm_info.shm_sz,IPC_CREAT | IPC_EXCL | 0666))<0) {
		// if there are any other errors apart from EEXIST, we can return gracefully 
		if(errno != EEXIST) {
			cf_error("Error in getting shared memory exclusively: %s", strerror(errno));
			return SHM_ERROR;	
		}
		else {
			// For all the processes that failed to create with EEXIST,
			// we try to get the shared memory again so that we
			// have a valid shmid
			if((g_shm_info.id = shmget(key,g_shm_info.shm_sz,IPC_CREAT | 0666))<0) {
				cf_error("Error in attaching to shared memory: %s", strerror(errno));
				return SHM_ERROR;
			}
	
			// Attach to the shared memory
			if((shm_pt = shmat(g_shm_info.id,NULL,0))==(void*)-1) {
				cf_error("Error in attaching to shared memory: %s pid: %d", strerror(errno), getpid());
				return SHM_ERROR;
			}
		
			// The shared memory base pointer
			g_shm_pt = (shm*)shm_pt;
		}
	}
	else {
		// The process who got the shared memory in the exclusive case
		cf_debug("Succeeded in creating shm : pid %d shmid %d", getpid(), g_shm_info.id);

		// Attach to the shared memory
		if ((shm_pt = shmat(g_shm_info.id,NULL,0))==(void*)-1) {
			cf_error("Error in attaching to shared memory: %s pid: %d", strerror(errno), getpid());
			return SHM_ERROR;
		}

		// The shared memory base pointer
		g_shm_pt = (shm*)shm_pt;
		memset(g_shm_pt,0,g_shm_info.shm_sz);

		// If you are the one who created the shared memory, only you can initialize the mutexes.
		// Seems fair!
		// because if we let everyone initialize the mutexes, we are in deep deep trouble
		pthread_mutexattr_t attr;
		pthread_mutexattr_init (&attr);
		pthread_mutexattr_setpshared (&attr, PTHREAD_PROCESS_SHARED);
		pthread_mutexattr_setrobust_np(&attr, PTHREAD_MUTEX_ROBUST_NP);
		if(pthread_mutex_init (&(g_shm_pt->shm_lock), &attr)!=0) {
			cf_error("Mutex init failed pid %d",getpid());
			pthread_mutexattr_destroy(&attr);
			return SHM_ERROR;
		}
		pthread_mutexattr_destroy(&attr);
	}
	pthread_create(&g_shm_update_thr, 0, cl_shm_updater_fn, 0);

	// all well? return aok!
	g_shm_initiated = true;
	return SHM_OK;
}

// Lock shared memory node exclusively.
int
cl_shm_node_lock(shm_ninfo* shared_node)
{
	int status = pthread_mutex_lock(&shared_node->ninfo_lock);

	if (status) {
		/* Check if the lock is in recoverable state */
		if (status == EOWNERDEAD){
			/* Make the lock consistent if the last owner of the lock died while holding the lock */
			/* We should ideally clean the memory here and then make the lock consistent */
			pthread_mutex_consistent_np(&shared_node->ninfo_lock);
		}
		else {
			 /* With Robust mutexes the lock gets in not recoverable state when the process holding the lock
			  * dies and the other process that gets the lock unlocks it without making it consistent
			  * (via pthread_mutex_consistent function) */
			cf_warn("Failed to lock shared memory node.");
			return SHM_ERROR;
		}
	}
	return SHM_OK;
}

// Unlock shared memory node.
void
cl_shm_node_unlock(shm_ninfo* shared_node)
{
	pthread_mutex_unlock(&shared_node->ninfo_lock);
}

int
cl_shm_get_partition_count()
{
	return g_shm_pt->partition_count;
}

shm_ninfo*
cl_shm_find_node_from_name(const char* node_name)
{
    // Search nodes for node name
    for (int i = 0; i < g_shm_pt->node_count; i++) {
    	shm_ninfo* node_info = &g_shm_pt->node_info[i];

    	if (strcmp(node_info->node_name, node_name) == 0) {
			return node_info;
		}
    }
    return 0;
}

shm_ninfo*
cl_shm_find_node_from_address(struct sockaddr_in* sa_in)
{
    // Search nodes for sockaddr
    for (int i = 0; i < g_shm_pt->node_count; i++) {
    	shm_ninfo* node_info = &g_shm_pt->node_info[i];
    	int max = node_info->address_count;

        //Search address for the current sockaddr
    	for (int j = 0; j < max; j++) {
    		if (memcmp(&node_info->address_array[j], sa_in, sizeof(struct sockaddr_in)) == 0) {
    			return node_info;
    		}
		}
    }
    return 0;
}

static void
cl_shm_copy_addresses(cl_seed_node* src, shm_ninfo* trg)
{
	trg->address_count = src->address_count;

	for (int i = 0; i < src->address_count; i++) {
		memcpy(&trg->address_array[i], &src->address_array[i], sizeof(struct sockaddr_in));
	}
}

static shm_ninfo*
cl_shm_add_node(cl_seed_node* seed)
{
	if (g_shm_pt->node_count >= g_max_nodes) {
		cf_error("Shared memory node limit breached: %d", g_max_nodes);
		return 0;
	}

	// Place the node at the last offset.
	shm_ninfo* shared_node = &(g_shm_pt->node_info[g_shm_pt->node_count]);

	// Copy node addresses.
	cl_shm_copy_addresses(seed, shared_node);

	// Initialise the lock - only the updater can initialize the node level mutex.
	size_t selfpid = getpid();

	if (selfpid == g_shm_pt->updater_id) {
	    pthread_mutexattr_t attr;
	    pthread_mutexattr_init(&attr);
	    pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);
	    pthread_mutexattr_setrobust_np(&attr, PTHREAD_MUTEX_ROBUST_NP);

	    if (pthread_mutex_init(&shared_node->ninfo_lock, &attr) != 0) {
			cf_warn("Shared memory node level mutex init failed pid %d", getpid());
			pthread_mutexattr_destroy(&attr);
			return 0;
	    }
	    pthread_mutexattr_destroy(&attr);
	}
	// Do not increase node count just yet.
	return shared_node;
}

static int
cl_shm_request_n_partitions(struct sockaddr_in* address_array, int address_count, int* n_partitions)
{
	// Loop through node's addresses and make request.
	for (int i = 0; i < address_count; i++) {
		if (cl_request_n_partitions(&address_array[i], n_partitions) == 0) {
			// Return on first successful request.
			return SHM_OK;
		}
	}
	return SHM_ERROR;
}

static int
cl_shm_request_node_info(struct sockaddr_in* address_array, int address_count, cl_node_info* node_info)
{
	// Loop through node's addresses and make request.
	for (int i = 0; i < address_count; i++) {
		if (cl_request_node_info(&address_array[i], node_info) == 0) {
			// Return on first successful request.
			return SHM_OK;
		}
	}
	return SHM_ERROR;
}

static int
cl_shm_request_replicas(struct sockaddr_in* address_array, int address_count, cl_replicas* replicas)
{
	// Loop through node's addresses and make request.
	for (int i = 0; i < address_count; i++) {
		if (cl_request_replicas(&address_array[i], replicas) == 0) {
			// Return on first successful request.
			return SHM_OK;
		}
	}
	return SHM_ERROR;
}

static int
cl_shm_node_ping(cl_seed_node* seed)
{
	// cf_debug("Ping %s", seed->name);

	// Set partition_count only once.
	if (g_shm_pt->partition_count == 0) {
		cl_shm_request_n_partitions(seed->address_array, seed->address_count, &g_shm_pt->partition_count);
	}

	// Request node information.
	cl_node_info request;

	if (cl_shm_request_node_info(seed->address_array, seed->address_count, &request) != SHM_OK) {
		return -1;
	}

	shm_ninfo* shared = cl_shm_find_node_from_name(seed->name);
	bool add = false;

	if (shared == 0) {
		add = true;
		shared = cl_shm_add_node(seed);

		if (shared == 0) {
			cl_node_info_free(&request);
			return -2;
		}
	}

	cl_shm_node_lock(shared);

	if (seed->address_count != shared->address_count) {
		cl_shm_copy_addresses(seed, shared);
	}

	// Set node name.
	cl_strncpy(shared->node_name, request.node_name, sizeof(shared->node_name));

	// Set partition generation after determining if replicas should be requested.
	bool request_replicas = false;
	if (shared->partition_generation != request.partition_generation) {
		shared->partition_generation = request.partition_generation;
		request_replicas = true;
	}

	if (cl_strncpy(shared->services, request.services, sizeof(shared->services))) {
		cf_warn("Shared memory services full: size=%d", sizeof(shared->services));
	}
	shared->dun = request.dun;

	cl_shm_node_unlock(shared);
	cl_node_info_free(&request);

	if (add) {
		// A new node was added, increase the count
		g_shm_pt->node_count++;
	}

	// Request replicas only if necessary.
	if (request_replicas) {
		cl_replicas replicas;

		if (cl_shm_request_replicas(seed->address_array, seed->address_count, &replicas) != SHM_OK) {
			return -3;
		}

		cl_shm_node_lock(shared);

		if (cl_strncpy(shared->write_replicas, replicas.write_replicas, sizeof(shared->write_replicas))) {
			cf_warn("Shared memory write replicas buffer full: size=%d", sizeof(shared->write_replicas));
		}

		if (cl_strncpy(shared->read_replicas, replicas.read_replicas, sizeof(shared->read_replicas))) {
			cf_warn("Shared memory read replicas buffer full: size=%d", sizeof(shared->read_replicas));
		}

		cl_shm_node_unlock(shared);
		cl_replicas_free(&replicas);
	}
	return SHM_OK;
}

/* This function (cl_shm_update) should only be called from cl_shm_updater_fn
 * because shm should not be tempered without taking the lock on it and 
 * we take the lock on shm in cl_shm_updater_fn
 */
static void
cl_shm_update(cl_cluster * asc)
{
	// Update the updater id in shared memory.
	g_shm_pt->updater_id = getpid();

	// Make copy of node names and addresses.
	pthread_mutex_lock(&asc->LOCK);
	int node_count = cf_vector_size(&asc->node_v);

	if (node_count <= 0) {
		pthread_mutex_unlock(&asc->LOCK);
		return;
	}

	cl_seed_node* seed_array = cl_shm_init_seed_array(node_count);

	for (int i = 0; i < node_count; i++) {
		cl_cluster_node* src = cf_vector_pointer_get(&asc->node_v, i);
		cl_seed_node* trg = &seed_array[i];
		cl_strncpy(trg->name, src->name, NODE_NAME_SIZE);

		trg->address_count = cf_vector_size(&src->sockaddr_in_v);

		if (trg->address_count > MAX_ADDRESSES_PER_NODE) {
			cf_debug("Node %s addresses truncated. Requested size=%d", src->name, trg->address_count);
			trg->address_count = MAX_ADDRESSES_PER_NODE;
		}

		for (int j = 0; j < trg->address_count ; j++) {
			struct sockaddr_in *sa_in = cf_vector_getp(&src->sockaddr_in_v, j);
			memcpy(&trg->address_array[j], sa_in, sizeof(struct sockaddr_in));
		}
	}
	pthread_mutex_unlock(&asc->LOCK);

	// Ping all nodes and update shared memory.
	for (int i = 0; i < node_count; i++) {
		cl_shm_node_ping(&seed_array[i]);
	}
}

void*
cl_shm_updater_fn(void* gas)
{
	//Take Lock - only one process can update
	int status = pthread_mutex_lock(&(g_shm_pt->shm_lock));

	if (g_shm_info.update_thread_end_cond) {
		pthread_exit(0);
	}

	/* There are only two valid cases when we can access the protected data
	 * 1. We get the lock without any error and
	 * 2. The thread holding the lock died without releasing it */
	if (status != 0) {
		/* Make the lock consistent if the last owner of the lock died while holding the lock */
		/* We should ideally clean the memory here and then make the lock consistent */
		if (status == EOWNERDEAD) {
			pthread_mutex_consistent_np(&(g_shm_pt->shm_lock));
		}
		else {
			/* With Robust mutexes the lock gets in not recoverable state when the process holding the lock
			 * dies and the other process that gets the lock unlocks it without making it consistent
			 * (via pthread_mutex_consistent function) */
			cf_error("Failed to lock shared memory in tend thread. Exiting thread.");
			pthread_exit(0);
		}
	}

	cf_debug("Process %d took over control with pthread_mutex_lock returning %d", getpid(), status);
	g_shm_updater = true;

	do {
		/* Check if the thread is set to exit or not. If it is, gracefully exit*/
		if (g_shm_info.update_thread_end_cond) {
			cl_shm_free_seed_array();
			pthread_exit(0);
		}

		sleep(g_shm_info.update_period);

		if (g_shm_info.update_thread_end_cond) {
			cl_shm_free_seed_array();
			pthread_exit(0);
		}

		// Tend all clusters.
		cf_ll_element *e = cf_ll_get_head(&cluster_ll);

		while(e) {
			cl_shm_update((cl_cluster*)e);
			e = cf_ll_get_next(e);
		}
	} while(1);

	return 0;
}

// Detach and remove shared memory
int
citrusleaf_shm_free()
{
	g_shared_memory = false;
	g_shm_info.update_thread_end_cond = true;

	// Only the master updater should join with update thread.
	if (g_shm_updater) {
		// Signal the update thread to exit and wait till it exits.
		pthread_join(g_shm_update_thr, NULL);
	}

	// Destroying mutexes should ONLY be done when there are no more
	// processes using shared memory.  Removing shared memory
	// should invalidate the mutexes, so it's probably safer
	// to never explicitly destroy the mutexes.
	/*
	pthread_mutex_destroy(&(g_shm_pt->shm_lock));

	for (int i = 0; i < g_shm_pt->node_count; i++) {
		pthread_mutex_destroy(&(g_shm_pt->node_info[i].ninfo_lock));
	}
	*/

	// Detach shared memory
	if (shmdt(g_shm_pt) < 0 ) {
		return SHM_ERROR;
	}

	// Try removing the shared memory - it will fail if any other process is still attached.
	// Failure is normal behavior, so don't check return code.
	shmctl(g_shm_info.id, IPC_RMID, 0);
	return SHM_OK;
}
