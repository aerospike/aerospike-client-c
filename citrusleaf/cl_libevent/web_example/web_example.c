#include <event.h>
#include <evdns.h>
#include <pthread.h>
#include <memory.h>
#include <stdio.h>
#include <unistd.h>

#include "citrusleaf_event/evcitrusleaf.h"

static void read_write_test(evcitrusleaf_cluster *clc);
static void write_callback(int return_value, evcitrusleaf_bin *bins, int n_bins, 
               uint32_t generation, void *udata);
static void read_callback(int return_value, evcitrusleaf_bin *bins, int n_bins,
                          uint32_t generation, void *udata);


typedef void * pthread_fn(void *);

int main(int argc, char **argv) 
  {
  evcitrusleaf_cluster   *clc;
 // int          return_value;
  pthread_t event_thread;
  int node_count;
  int tries = 0;

  // initialize Libevent's structures - including the dynamic dns system
  event_init();
  evdns_init();
  evdns_resolv_conf_parse(DNS_OPTIONS_ALL, "/etc/resolv.conf"); 
  
  // initialize Citrusleaf's structures
  evcitrusleaf_init();

  // Create a cluster with a particular starting host
  clc = evcitrusleaf_cluster_create();
  if (!clc)    return(-1);
  evcitrusleaf_cluster_add_host(clc, "192.168.4.22", 3000);

  pthread_create(&event_thread, 0, (pthread_fn *) event_dispatch, 0);

  // Up to the application: wait to see if this cluster has good nodes, or just
  // start using? 
  do {
    node_count = evcitrusleaf_cluster_get_active_node_count(clc);
    if (node_count > 0)		break;
    usleep( 50 * 1000 );
    tries++;
  } while ( tries < 20);
	
  if( tries >= 20 ){
    printf("example: could not connect to cluster, configuration bad?\n");
    evcitrusleaf_cluster_destroy(clc);
    return(-1);
  }

  read_write_test(clc);
	
  // join the event thread, when it's finished
  void *value_ptr;
  pthread_join(event_thread, &value_ptr); 

  evcitrusleaf_cluster_destroy(clc);
  evcitrusleaf_shutdown(true);

  return(0);
}

static void 
read_write_test(evcitrusleaf_cluster *clc)
{
  // initialize two bin objects
  evcitrusleaf_bin values[2];
  evcitrusleaf_object mykey;
  strcpy(values[0].bin_name, "test_bin_one");
  evcitrusleaf_object_init_str(&values[0].object, "example_value_one");
  strcpy(values[1].bin_name, "test_bin_two");
  evcitrusleaf_object_init_int(&values[1].object, 0xDEADBEEF);
	
  // a key can be any valid type - in this case, let's make it 
  // a string.
  evcitrusleaf_object_init_str(&mykey, "example_key");

  evcitrusleaf_write_parameters wparam;
  evcitrusleaf_write_parameters_init(&wparam);

  if (0 != evcitrusleaf_put(clc, "test", "myset", &mykey, values, 2,
                            &wparam, 0, write_callback, clc)) {
    fprintf(stderr, "citrusleaf put could not dispatch write!\n");
    struct timeval x = { 0, 0 };
    event_loopexit(&x);
    return;
  }
  fprintf(stderr, "citrusleaf put dispatched\n");
}

static void
write_callback(int return_value, evcitrusleaf_bin *bins, int n_bins, 
               uint32_t generation, void *udata)
{
  evcitrusleaf_cluster *clc = (evcitrusleaf_cluster *) udata;
	
  evcitrusleaf_object mykey;
  evcitrusleaf_object_init_str(&mykey, "example_key");

  if (return_value != EVCITRUSLEAF_OK) {
    fprintf(stderr, "put failed: return code %d\n",return_value);
    struct timeval x = { 0, 0 };
    event_loopexit(&x);
    return;
  }

  if (bins)		evcitrusleaf_bins_free(bins, n_bins);

  // Get all the values in this key (enjoy the fine c99 standard)
  if (0 != evcitrusleaf_get_all(clc, "test", "myset", &mykey, 100,
           read_callback, clc)) {
    fprintf(stderr, "get after put could not dispatch\n");
    struct timeval x = { 0, 0 };
    event_loopexit(&x);
    return;
  }
  fprintf(stderr, "get all dispatched\n");
}

static void read_callback(int return_value, evcitrusleaf_bin *bins, int n_bins,
                          uint32_t generation, void *udata)
{

  if (return_value != EVCITRUSLEAF_OK) {
    fprintf(stderr, "get failed: return code %d\n", return_value);
    struct timeval x = { 0, 0 };
    event_loopexit(&x);
    return;
  }
	
  fprintf(stderr, "get all returned %d bins:\n",n_bins);
  for (int i=0;i<n_bins;i++) {
    fprintf(stderr, "%d:  bin %s ",i,bins[i].bin_name);
    switch (bins[i].object.type) {
    case CL_STR:
      fprintf(stderr, "type string: value %s\n", bins[i].object.u.str);
      break;
    case CL_INT:
      fprintf(stderr, "type int: value %"PRId64"\n",bins[i].object.u.i64);
      break;
    default:
      fprintf(stderr, "type unknown! (%d)\n",(int)bins[i].object.type);
      break;
    }
  }
	
  // free any data allocated
  if (bins)		evcitrusleaf_bins_free(bins, n_bins);
  fprintf(stderr,"citrusleaf getall succeeded\n");
	
  // and now terminate the test.
  struct timeval x = { 0, 0 };
  event_loopexit(&x);
}

