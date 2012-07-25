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
#include "citrusleaf/cl_shm.h"
#include "citrusleaf/cf_socket.h"

/* Shared memory global variables */
void * g_shm_base;
int g_shmid;
bool SHARED_MEMORY=true;
extern int errno;
pthread_mutex_t *g_shm_mutex_update;
pthread_mutex_t *g_shm_mutex_read;
size_t *g_shm_last_offset;
//Where the major chunk of the memory starts
void *g_shm_chunk_base;

shm_header_info g_shm_header_info[SHM_HEADER_COUNT];
void cl_shm_header_info_init() {
	char names[SHM_HEADER_COUNT][64] = {	"node\n",
						"node\npartition-generation\nservices\n",
						"replicas-read\nreplicas-write\n",
						"partitions\n"
					   };
	size_t size[SHM_HEADER_COUNT] = {	32 + 32, //String name + node name
					//string name + node name + partition-generation + max neighbours * size of each 
						32*3 + 32+8+(NUM_NODES-1)*32,
					//string name + (namespace + : + partition_number + ; ) * max number of partitions * number of nodes
						2*(32 + (32+1+4+1)*4096*NUM_NAMESPACES),
						32 + 4
					};
						
	for(int i=0;i<SHM_HEADER_COUNT;i++) {
		memcpy(g_shm_header_info[i].name,names[i],64);
		if(i==0) {
			g_shm_header_info[i].offset = 0;
		}
		else {
			g_shm_header_info[i].offset = g_shm_header_info[i-1].offset + g_shm_header_info[i-1].size;
		}
		g_shm_header_info[i].size = size[i];
	}
}
static size_t g_shm_node_sz;
static size_t g_shm_sz;
/* Initialize shared memory segment */
int cl_shm_init() {
	key_t key = 12349;
	void * shm = (void*)0;
	cl_shm_header_info_init();
	bool eexist=false;
	
	/*The size of the shared memory to be allocated is determined by adding the size of all the data
 	 * each header can carry and then multiplying the sum by the total no of nodes */
	for(int i=0;i<SHM_HEADER_COUNT;i++) {
		g_shm_node_sz = g_shm_node_sz +  g_shm_header_info[i].size;
	}
	g_shm_sz = g_shm_node_sz * NUM_NODES;
	
	/*First try to exclusively create a shared memory, for this only one process will succeed.
 	 * others will fail giving an errno of EEXIST*/
	 if((g_shmid=shmget(key,g_shm_sz,IPC_CREAT | IPC_EXCL | 0666))<0) {
		fprintf(stderr,"%s\n",strerror(errno));
		/*if there are any other errors apart from EEXIST, we can return gracefully */
		if(errno != EEXIST) {
			return SHM_ERROR;	
		}
		else {
			/* set eexist to true */
			eexist = true;
			fprintf(stderr,"I tried creating an already existing shared memory : pid %d\n",getpid());
			
		}
	}
	else {
		fprintf(stderr,"I succeeded in creating shm : pid %d shmid %d\n",getpid(),g_shmid);
		/* Attach to the shared memory */	
		if((shm = shmat(g_shmid,NULL,0))==(void*)-1) {
			printf("Error in attaching\n");
			return SHM_ERROR;
		}
		/*The shared memory base pointer*/	
		g_shm_base = shm;
		memset(g_shm_base,0,g_shm_sz);	
		/* The actual data starts from here, after the updater_id and two locks*/
		g_shm_chunk_base = (void*)((char*)g_shm_base + 2*sizeof(size_t) + 2*sizeof(pthread_mutex_t));
	
		/* Last allocated offset -- put in shared memory and initialise to zero*/
		g_shm_last_offset = (size_t *)(g_shm_base);
		*g_shm_last_offset = sizeof(size_t);
	
		/*Then comes the update lock. Allocate it some space and then move the last offset to base + sizeof one lock*/
		g_shm_mutex_update = (pthread_mutex_t*)(g_shm_base + *g_shm_last_offset);
		*g_shm_last_offset = *g_shm_last_offset + sizeof(pthread_mutex_t);
	
		/*Then add the read/block level lock*/
		g_shm_mutex_read = (pthread_mutex_t*)(g_shm_base + *g_shm_last_offset);
		*g_shm_last_offset = *g_shm_last_offset + sizeof(pthread_mutex_t);
	
		/* If you are the one who created the shared memory, only you can initialize the mutexes. Seems fair!
 	 	* because if we let everyone initialize the mutexes, we are in deep deep trouble */	
		pthread_mutexattr_t attr;
		pthread_mutexattr_init (&attr);
		pthread_mutexattr_setpshared (&attr, PTHREAD_PROCESS_SHARED);
		pthread_mutexattr_setrobust_np (&attr, PTHREAD_MUTEX_ROBUST_NP);
		if(pthread_mutex_init (g_shm_mutex_update, &attr)!=0) {
			fprintf(stderr,"mutex init failed pid %d\n",getpid());
			return SHM_ERROR;
		}
		return SHM_OK;	
	}
	/*For all the processes that failed to create with EEXIST, we try to get the shared memory again so that we
 	* have a valid shmid */
	if((g_shmid=shmget(key,g_shm_sz,IPC_CREAT | 0666))<0) {
		return SHM_ERROR;
	}
	
	/* Attach to the shared memory */	
	if((shm = shmat(g_shmid,NULL,0))==(void*)-1) {
		printf("Error in attaching\n");
		return SHM_ERROR;
	}
	
	/*The shared memory base pointer*/	
	g_shm_base = shm;
	
	/* The actual data starts from here, after the updater_id and two locks*/
	g_shm_chunk_base = (void*)((char*)g_shm_base + 2*sizeof(size_t) + 2*sizeof(pthread_mutex_t));

	/* Last allocated offset -- put in shared memory and initialise to zero*/
	g_shm_last_offset = (size_t *)(g_shm_base);
	*g_shm_last_offset = sizeof(size_t);
	
	/*Then comes the update lock. Allocate it some space and then move the last offset to base + sizeof one lock*/
	g_shm_mutex_update = (pthread_mutex_t*)(g_shm_base + *g_shm_last_offset);
	*g_shm_last_offset = *g_shm_last_offset + sizeof(pthread_mutex_t);
	
	/*Then add the read/block level lock*/
	g_shm_mutex_read = (pthread_mutex_t*)(g_shm_base + *g_shm_last_offset);
	*g_shm_last_offset = *g_shm_last_offset + sizeof(pthread_mutex_t);
	

	/* all well? return aok!*/
	return SHM_OK;
}
bool update_thread_end=false;
/* Detach and remove shared memory */
int cl_shm_free() {
	/* signal the update thread to exit and wait till it exits */
	update_thread_end = true;
	pthread_join(shm_update_thr,NULL);

	pthread_mutex_destroy(g_shm_mutex_update);
	if(shmdt(g_shm_base) < 0 ) return SHM_ERROR;
	if(shmctl(g_shmid,IPC_RMID,0) < 0 ) return SHM_ERROR;
}

/*Just for testing purposes, we want to see who is updating the shared memory*/
size_t *g_shm_updater_id;
void cl_shm_set_updater_id(size_t pid) {
	g_shm_updater_id = (size_t*)(g_shm_base + *g_shm_last_offset);
	*g_shm_last_offset = *g_shm_last_offset + sizeof(size_t);
	*g_shm_updater_id = pid;
	return;
}

/*Get the maximum size under each header which is saved in the shared memory*/
int cl_shm_get_size(char * name){
	for(int i=0;i<SHM_HEADER_COUNT;i++){
		if(memcmp(name,g_shm_header_info[i].name,strlen(name))==0)
			return g_shm_header_info[i].size;
	}
	return -1;
}


void* cl_shm_alloc(struct sockaddr_in * sa_in, char * names) {
	int place=-1;
	for(int i=0;i<SHM_HEADER_COUNT;i++) {
		if(memcmp(g_shm_header_info[i].name,names,strlen(names))==0) {
			place = i;
			break;
		}
	}
	if(place==-1) {
		printf("Dude its a problem\n");
		return NULL;
	}
	//The chunk of the memory starts after updater_id and the mutex lock
	if(!g_shm_base){ 
		return NULL;
	}
	void * pt = g_shm_chunk_base;
	int found = 0;
	//Search for the current sockaddr
	for(int i=0;i<NUM_NODES;i++) {
		if(memcmp(pt,sa_in,sizeof(struct sockaddr_in))==0) {
			found = 1;
			break;
		}
		else {
			pt = pt + g_shm_node_sz;
		}
	}
	//Found the socket address! Yay! Just need to place the data
	if(found==1) {
		return (pt + SZ_SOCK + g_shm_header_info[place].offset);
	}
	else {
		memcpy(g_shm_base + *g_shm_last_offset,sa_in,sizeof(struct sockaddr_in));
		pt = g_shm_base + *g_shm_last_offset;
		*g_shm_last_offset = *g_shm_last_offset + g_shm_node_sz;
		return (pt + SZ_SOCK + g_shm_header_info[place].offset);
	}	
}

int cl_shm_info_host(struct sockaddr_in * sa_in, char * names, char ** values, int timeout_ms, bool send_asis) {
	int rv = -1;
    	int io_rv;
	*values = 0;
	
	// Deal with the incoming 'names' parameter
	// Translate interior ';'  in the passed-in names to \n
	uint32_t	slen = 0;
	if (names) {
		if (send_asis) {
			slen = strlen(names);
		} else {
			char *_t = names;
			while (*_t) { 
				slen++; 
				if ((*_t == ';') || (*_t == ':') || (*_t == ',')) *_t = '\n'; 
				_t++; 
			}
		}
	}
	
	// Sometimes people forget/cant add the trailing '\n'. Be nice and add it for them.
	// using a stack allocated variable so we dn't have to clean up, Pain in the ass syntactically
	// but a nice little thing
	if (names) {
		if (names[slen-1] == '\n') {
			slen = 0;
		} else { 
			slen++; if (slen > 1024) { return(-1); } 
		}
	}
	char names_with_term[slen+1];
	if (slen) { 
		strcpy(names_with_term, names);
		names_with_term[slen-1] = '\n';
		names_with_term[slen] = 0;
		names = names_with_term;
	}
		
	// Actually doing a non-blocking connect
	int fd = cf_create_nb_socket(sa_in, timeout_ms);
	if (fd == -1) {
		return (-1);
	}

	cl_proto 	*req;
	uint8_t		buf[1024];
	uint		buf_sz;
	
	if (names) {
		uint sz = strlen(names);
		buf_sz = sz + sizeof(cl_proto);
		if (buf_sz < 1024)
			req = (cl_proto *) buf;
		else
			req = (cl_proto *) malloc(buf_sz);
		if (req == NULL)	goto Done;

		req->sz = sz;
		memcpy(req->data,names,sz);
	}
	else {
		req = (cl_proto *) buf;
		req->sz = 0;
		buf_sz = sizeof(cl_proto);
	}
		
	req->version = CL_PROTO_VERSION;
	req->type = CL_PROTO_TYPE_INFO;
	cl_proto_swap(req);
	
    if (timeout_ms)
        io_rv = cf_socket_write_timeout(fd, (uint8_t *) req, buf_sz, 0, timeout_ms);
    else
        io_rv = cf_socket_write_forever(fd, (uint8_t *) req, buf_sz);
    
	if ((uint8_t *)req != buf){
		free(req->data);
		free(req);
	}
	if (io_rv != 0) {
#ifdef DEBUG        
		fprintf(stderr, "info returned error, rv %d errno %d bufsz %d\n",io_rv, errno, buf_sz);
#endif        
		goto Done;
	}
	
	cl_proto	*rsp = (cl_proto *)buf;
    if (timeout_ms) 
        io_rv = cf_socket_read_timeout(fd, buf, 8, 0, timeout_ms);
    else
        io_rv = cf_socket_read_forever(fd, buf, 8);
    
    if (0 != io_rv) {
#ifdef DEBUG        
		fprintf(stderr, "info read not 8 bytes, fail, rv %d errno %d\n",rv, errno);
#endif        
		goto Done;
	}
	cl_proto_swap(rsp);
	
	if (rsp->sz) {
		/* Allocate a buffer in the local memory which you can send to the server to get the values*/
		uint8_t *v_buf = malloc(rsp->sz + 1);
		if (!v_buf) goto Done;
        
        if (timeout_ms)
            io_rv = cf_socket_read_timeout(fd, v_buf, rsp->sz, 0, timeout_ms);
        else
            io_rv = cf_socket_read_forever(fd, v_buf, rsp->sz);
        
        if (io_rv != 0) {
            free(v_buf);
            goto Done;
		}
			
		v_buf[rsp->sz] = 0;
		/*This buffer is then copied to *values and sent back to the program that called for it*/
		*values = (char*)cl_shm_alloc(sa_in,names);
		memcpy(*values,v_buf,rsp->sz + 1);
		
	}                                                                                               
	else {
		*values = 0;
	}
	rv = 0;

Done:	
	shutdown(fd, SHUT_RDWR);
	close(fd);
	
	return(rv);
	
}

int cl_shm_read(struct sockaddr_in * sa_in, char *names, char **values, int timeout, bool send_as_is){
	//Search in the shared memory with sa_in. 
	int place=-1;
	for(int i=0;i<SHM_HEADER_COUNT;i++) {
		if(memcmp(g_shm_header_info[i].name,names,strlen(names))==0) {
			place = i;
			break;
		}
	}
	if(place==-1) {
		printf("Dude its a problem\n");
		return -1;
	}

	int n_nodes = NUM_NODES;
	void * pt = g_shm_chunk_base;
	if(!pt) {
		fprintf(stderr,"My chunk base is NULL : pid %d\n",getpid());
		return -1;
	}
	for(int i=0;i<NUM_NODES;i++){
		if(memcmp(pt + g_shm_node_sz*i,sa_in,sizeof(struct sockaddr_in))==0){
			void *socket_data_ptr = pt + g_shm_node_sz*i+ SZ_SOCK + g_shm_header_info[place].offset;
			if(*((char*)(socket_data_ptr)) != 0) {
				memcpy(*values, socket_data_ptr, g_shm_header_info[place].size);
				return 0;
			}
			else {
				return -1;
			}
		}
		
	}
	return -1;	
}

#define INFO_TIMEOUT_MS 300
//extern int errno;
void cl_shm_update(cl_cluster * asc) {
	/* Check if the thread is set to exit or not. If it is, gracefully exit*/	
	if(update_thread_end) {
		pthread_exit(NULL);
	}
	//Take Lock
	int rv = pthread_mutex_trylock(g_shm_mutex_update);
	if(rv==0) {
		//Update and print owner id
		size_t self_pid = getpid();
		cl_shm_set_updater_id(self_pid);
		//Update shared memory
		uint n_hosts = cf_vector_size(&asc->host_str_v);
		cf_vector_define(sockaddr_in_v, sizeof( struct sockaddr_in ), 0);
		for (uint i=0;i<n_hosts;i++) {
			//For debug
			char *host = cf_vector_pointer_get(&asc->host_str_v, i);	
	        	int port = cf_vector_integer_get(&asc->host_port_v, i);
			
			//Resolve hosts and store them in sockaddr_in_v
			cl_lookup(asc, cf_vector_pointer_get(&asc->host_str_v, i), 
					cf_vector_integer_get(&asc->host_port_v, i),
					&sockaddr_in_v);
		}
		for (uint i=0;i<cf_vector_size(&sockaddr_in_v);i++) {
			struct sockaddr_in *sa_in = cf_vector_getp(&sockaddr_in_v,i);
			char * values;
			cl_shm_info_host(sa_in,"node",&values,INFO_TIMEOUT_MS, false);
			cl_shm_info_host(sa_in,"node\npartition-generation\nservices",&values,INFO_TIMEOUT_MS, false);
			cl_shm_info_host(sa_in,"replicas-read\nreplicas-write",&values,INFO_TIMEOUT_MS,false);
			cl_shm_info_host(sa_in,"partitions",&values,INFO_TIMEOUT_MS,false);
		}
		
	}
	else {
		return;
	}
}
int g_shm_update_speed = 1;
void * cl_shm_updater_fn(void * gcc_is_ass) {
	uint64_t cnt = 1;
	do {
		sleep(1);
		cf_ll_element *e = cf_ll_get_head(&cluster_ll);
		while(e) {
			if((cnt % g_shm_update_speed) == 0) {
				cl_shm_update((cl_cluster*)e);
			}
		}
		cnt++;
	} while(1);
	return (0);
}
