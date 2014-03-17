
#include <aerospike/aerospike.h>
#include <aerospike/aerospike_batch.h>
#include <aerospike/aerospike_key.h>

#include <aerospike/as_batch.h>
#include <aerospike/as_error.h>
#include <aerospike/as_status.h>

#include <aerospike/as_record.h>
#include <aerospike/as_integer.h>
#include <aerospike/as_string.h>
#include <aerospike/as_list.h>
#include <aerospike/as_arraylist.h>
#include <aerospike/as_map.h>
#include <aerospike/as_hashmap.h>
#include <aerospike/as_val.h>
#include <pthread.h>
#include <unistd.h>

#include "../test.h"

/******************************************************************************
 * GLOBAL VARS
 *****************************************************************************/

extern aerospike * as;

#define NAMESPACE "test"
#define SET "test"
#define N_KEYS 200

cf_atomic32 num_threads = 0;
pthread_rwlock_t rwlock;

/******************************************************************************
 * TYPES
 *****************************************************************************/

typedef struct batch_read_data_s {
	uint32_t thread_id;
    uint32_t total;
    uint32_t found;
    uint32_t errors;
    uint32_t last_error;
} batch_read_data;

/******************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/


bool batch_get_1_callback(const as_batch_read * results, uint32_t n, void * udata)
{
    batch_read_data * data = (batch_read_data *) udata;
	
    data->total = n;

    for (uint32_t i = 0; i < n; i++) {

        if (results[i].result == AEROSPIKE_OK) {
            data->found++;

            int64_t key = as_integer_getorelse((as_integer *) results[i].key->valuep, -1);
            int64_t val = as_record_get_int64(&results[i].record, "val", -1);
            if ( key != val ) {
                warn("key(%d) != val(%d)",key,val);
                data->errors++;
                data->last_error = -2;
            }
        }
        else if (results[i].result != AEROSPIKE_ERR_RECORD_NOT_FOUND) {
            data->errors++;
            data->last_error = results[i].result;
            warn("batch callback thread(%d) error(%d)", data->thread_id, data->last_error);
        }
    }

    info("total: %d, found: %d, errors: %d", data->total, data->found, data->errors);

    return true;
}


/******************************************************************************
 * TEST CASES
 *****************************************************************************/

TEST( batch_get_pre , "Pre: Create Records" )
{
    as_error err;

    as_record rec;
    as_record_inita(&rec, 1);

    for (uint32_t i = 1; i < N_KEYS+1; i++) {

        as_key key;
        as_key_init_int64(&key, NAMESPACE, SET, (int64_t) i);

        as_record_set_int64(&rec, "val", (int64_t) i);

        aerospike_key_put(as, &err, NULL, &key, &rec);

        if ( err.code != AEROSPIKE_OK ) {
            info("error(%d): %s", err.code, err.message);
        }

        assert_int_eq( err.code , AEROSPIKE_OK );
    }
}

TEST( batch_get_1 , "Simple" )
{
    as_error err;

    as_batch batch;
    as_batch_inita(&batch, N_KEYS);

    for (uint32_t i = 0; i < N_KEYS; i++) {
        as_key_init_int64(as_batch_keyat(&batch,i), NAMESPACE, SET, i+1);
    }

    batch_read_data data = {0};

    aerospike_batch_get(as, &err, NULL, &batch, batch_get_1_callback, &data);
    if ( err.code != AEROSPIKE_OK && err.code != AEROSPIKE_ERR_INDEX_FOUND ) {
        info("error(%d): %s", err.code, err.message);
    }
    assert_int_eq( err.code , AEROSPIKE_OK );

    assert_int_eq( data.found , N_KEYS );
    assert_int_eq( data.errors , 0 );
}

TEST( batch_get_post , "Post: Remove Records" )
{
    as_error err;

    for (uint32_t i = 1; i < N_KEYS+1; i++) {

        as_key key;
        as_key_init_int64(&key, NAMESPACE, SET, (int64_t) i);

        aerospike_key_remove(as, &err, NULL, &key);

        if ( err.code != AEROSPIKE_OK ) {
            info("error(%d): %s", err.code, err.message);
        }

        assert_int_eq( err.code , AEROSPIKE_OK );
    }
}

void *batch_get_function(void  *thread_id)
{
    int thread_num = *(int*)thread_id;
    as_error err;
	
    as_batch batch;
    as_batch_inita(&batch, 20);
	
    int start_index = thread_num * 20;
    int end_index = start_index + 20;
    int j = 0;
    for (uint32_t i = start_index; i < end_index; i++) {
        as_key_init_int64(as_batch_keyat(&batch,j++), NAMESPACE, SET, i+1);
    }
	
    batch_read_data data = {thread_num, 0, 0, 0, 0};
	
    cf_atomic32_incr(&num_threads);
    pthread_rwlock_rdlock(&rwlock);
	
	aerospike_batch_get(as, &err, NULL, &batch, batch_get_1_callback, &data);
	
	pthread_rwlock_unlock(&rwlock);

    if ( err.code != AEROSPIKE_OK && err.code != AEROSPIKE_ERR_INDEX_FOUND ) {
        info("multi-thread error(%d): %s", err.code, err.message);
    }
	
    return(0);
}

TEST( multithreaded_batch_get , "Batch Get - with multiple threads ")
{
    int threads = 10;
	
    pthread_t batch_thread[threads];
	int ids[threads];
    pthread_rwlock_init(&rwlock, NULL);
    pthread_rwlock_wrlock( &rwlock);
    for (uint32_t i = 0; i < threads; i++) {
        ids[i] = i;
        pthread_create( &batch_thread[i], 0, batch_get_function, &ids[i]);
    }
	
    while ( cf_atomic32_get(num_threads) < threads ) {
        sleep(1);
    }
    pthread_rwlock_unlock( &rwlock);
	
    for ( uint32_t i = 0; i < threads; i++) {
        pthread_join( batch_thread[i], NULL);
    }
    pthread_rwlock_destroy( &rwlock);
}

/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

SUITE( batch_get, "aerospike_batch_get tests" ) {
    suite_add( batch_get_pre );
    suite_add( batch_get_1 );
    suite_add( multithreaded_batch_get );
    suite_add( batch_get_post );
}
