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
pthread_mutex_t *g_shm_mutex;
size_t g_shm_last_offset;
/* Initialize shared memory segment */
int cl_shm_init() {
	key_t key = 12349;
	void * shm = (void*)0;
	//size_t sz = SHM_ARRAY_SZ * SZ_PARTITION_TABLE_NODE + SZ_CL_PARTITION_ID + SZ_HEAD_POINTER;
	if((g_shmid=shmget(key,SZ_SHM,IPC_CREAT | 0666))<0) {
		fprintf(stderr,"%s\n",strerror(errno));
		return SHM_ERROR;	
	}
	if((shm = shmat(g_shmid,NULL,0))==(void*)-1) {
		printf("Error in attaching\n");
		return SHM_ERROR;
	}
	g_shm_base = shm;
	g_shm_last_offset = 0;
	g_shm_mutex = (pthread_mutex_t*)(g_shm_base + g_shm_last_offset);
	g_shm_last_offset = g_shm_last_offset + sizeof(pthread_mutex_t);
	int * buffer = (int*)malloc(sizeof(int));
	FILE * r = fopen("./init.txt","r");
	fread(buffer,sizeof(int),1,r);
	fclose(r);
	if(*buffer==1234) {
		return SHM_OK;
	}
	pthread_mutexattr_t attr;
	//fprintf(stderr,"I am here, just about to init\n");
	pthread_mutexattr_init (&attr);
	pthread_mutexattr_setpshared (&attr, PTHREAD_PROCESS_SHARED);
	pthread_mutexattr_setrobust_np (&attr, PTHREAD_MUTEX_ROBUST_NP);
	pthread_mutex_init (g_shm_mutex, &attr);
	FILE * p = fopen("./init.txt","w");
	int *inno = (int*)malloc(sizeof(int));
	*inno = 1234;
	int len = sizeof(int);
	fwrite(inno,len,1,p);
	fclose(p);

	return SHM_OK;
}

/* Detach and remove shared memory */
int cl_shm_free() {
	memset(g_shm_base,0,SZ_SHM);
	if(shmdt(g_shm_base) < 0 ) return SHM_ERROR;
	if(shmctl(g_shmid,IPC_RMID,0) < 0 ) return SHM_ERROR;
}
size_t *g_shm_updater_id;
void cl_shm_set_updater_id(size_t pid) {
	g_shm_updater_id = (size_t*)(g_shm_base + g_shm_last_offset);
	g_shm_last_offset = g_shm_last_offset + sizeof(size_t);
	*g_shm_updater_id = pid;
	return;
}
//enum SHM_HEADER_NAMES = {"node\n"=1,
//			"node\npartition-generation\nservices\n",
//			"replicas-read\nreplicas-write\n",
//			"partitions\n"}
//Where the major chunk of the memory starts
void *g_shm_chunk_base;
uint8_t* cl_shm_alloc(struct sockaddr_in * sa_in, char * names) {
	int place=0;
//	fprintf(stderr,"In alloc names = %s\n",names);
	if(strcmp(names,"node\n")==0) {
		place = 0;
	//	fprintf(stderr,"Compared with node\n");
	}
	else if(strcmp(names,"node\npartition-generation\nservices\n")==0) {
		place = 1;
//		fprintf(stderr,"Compared with node\npartition-generation\nservices\n");
	}
	else if(strcmp(names,"replicas-read\nreplicas-write\n")==0) {
		place = 2;
//		fprintf(stderr,"Compared with replicas-read\nreplicas-write\n");
	}
	else if(strcmp(names,"partitions\n")==0) {
		place = 3;
//		fprintf(stderr,"Compared with partitions\n");
	}
	//The chunk of the memory starts after updater_id and the mutex lock
	g_shm_chunk_base = (void*)((char*)g_shm_base + sizeof(size_t) + sizeof(pthread_mutex_t));
	void * pt = g_shm_chunk_base;
	int found = 0;
	//Search for the current sockaddr
	for(int i=0;i<NUM_NODES;i++) {
		if(memcmp(pt,sa_in,sizeof(struct sockaddr_in))==0) {
//			fprintf(stderr,"Found socket address in memory %d\n",place);
			found = 1;
			break;
		}
		else {
			pt = pt + SZ_NODE;
		}
	}
	//Found the socket address! Yay! Just need to place the data
	if(found==1) {
		return (pt + SZ_SOCK + place*SZ_SOCK_DATA);
	}
	else {
//		fprintf(stderr,"Allocating memory for %d\n",place);
		memcpy(g_shm_base + g_shm_last_offset,sa_in,sizeof(struct sockaddr_in));
		pt = g_shm_base + g_shm_last_offset;
		g_shm_last_offset = g_shm_last_offset + SZ_NODE;
		return (pt + SZ_SOCK + place*SZ_SOCK_DATA);
	}	
	//return NULL;
}

int cl_shm_info_host(struct sockaddr_in * sa_in, char * names, char ** values, int timeout_ms, bool send_asis) {
//	fprintf(stderr,"Names %s\n",names);
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
		//req->data = (uint8_t*)malloc(sz+1);
		memcpy(req->data,names,sz);
	//	fprintf(stderr,"Req data in loop = %s\n",req->data);
		fprintf(stderr,"Name in loop = %s\n",names);
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
		//fprintf(stderr,"Req data in loop = %s\n",req->data);
		//fprintf(stderr,"Name in loop = %s\n",names);
		uint8_t *v_buf = cl_shm_alloc(sa_in,names );
		//uint8_t *v_buf = malloc(rsp->sz + 1);
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
		*values = (char *) v_buf;
		
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
	int place=0;
	fprintf(stderr,"In alloc names = %s\n",names);
	if(strcmp(names,"node\n")==0) {
		place = 0;
		fprintf(stderr,"Compared with node\n");
	}
	else if(strcmp(names,"node\npartition-generation\nservices\n")==0) {
		place = 1;
		fprintf(stderr,"Compared with node\npartition-generation\nservices\n");
	}
	else if(strcmp(names,"replicas-read\nreplicas-write\n")==0) {
		place = 2;
		fprintf(stderr,"Compared with replicas-read\nreplicas-write\n");
	}
	else if(strcmp(names,"partitions\n")==0) {
		place = 3;
		fprintf(stderr,"Compared with partitions\n");
	}
	int n_nodes = NUM_NODES;
	fprintf(stderr,"num of nodes = %d\n",n_nodes);
	void * pt = g_shm_chunk_base;
	int i;
	for(i=0;i<NUM_NODES;i++){
		if(memcmp(pt + SZ_NODE*i,sa_in,sizeof(struct sockaddr_in))==0){
			fprintf(stderr,"Yes! Found socket address\n");
			void *socket_data_ptr = pt +SZ_NODE*i+ SZ_SOCK + place * SZ_SOCK_DATA;
			fprintf(stderr,"socket_data_ptr = %p\n",socket_data_ptr);
			fprintf(stderr,"stored value in this is = %c\n",*((char*)socket_data_ptr));
			fprintf(stderr,"stored value in this is = %s\n",(char*)socket_data_ptr);
			if(*((char*)(socket_data_ptr)) != 0) {
				fprintf(stderr,"Before memcopy\n");
				int socket_data_sz = SZ_SOCK_DATA;
				memcpy(*values, socket_data_ptr, SZ_SOCK_DATA);
				return 0;
			}
			else {
				return -1;
			}
		}
//		else {
//			pt = pt + SZ_NODE;
//		}
		
	}
	return -1*(i+1);	
}

#define INFO_TIMEOUT_MS 300
//extern int errno;
void cl_shm_update(cl_cluster * asc) {
	//Take Lock
	int rv = pthread_mutex_trylock(g_shm_mutex);
	if(rv==0) {
		//Update and print owner id
		size_t self_pid = getpid();
		cl_shm_set_updater_id(self_pid);
		//fprintf(stderr,"Update pid = %d\n",*g_shm_updater_id);	
		//Update shared memory
		uint n_hosts = cf_vector_size(&asc->host_str_v);
		cf_vector_define(sockaddr_in_v, sizeof( struct sockaddr_in ), 0);
		for (uint i=0;i<n_hosts;i++) {
			//For debug
			char *host = cf_vector_pointer_get(&asc->host_str_v, i);	
	        	int port = cf_vector_integer_get(&asc->host_port_v, i);
    			//fprintf(stderr, "lookup hosts: %s:%d\n",host,port);
			
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
		//sleep(1);
		cf_ll_element *e = cf_ll_get_head(&cluster_ll);
		while(e) {
		//	fprintf(stderr,"Here here here!!\n");
			if((cnt % g_shm_update_speed) == 0) {
				cl_shm_update((cl_cluster*)e);
				//update function
			}
		}
		cnt++;
	} while(1);
	return (0);
}
