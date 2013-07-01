#include "citrusleaf/citrusleaf.h"
#include <memory.h>
#include <malloc.h>

// some simple api tests -

static const char *ns       = "test";
static const char *myset    = "myset";
static const char *bin1     = "email";
static const char *bin2     = "hits";
static const char *bin3     = "blob";
static const char *host     = "192.168.4.22";
static const char *badhost  = "192.168.5.2"; // this one doesn't exist
static const char *blobData = "foobar"; 
static const char *blobData2 = "barfoo";
static uint64_t    intData  = 314;
static const char *strData  = "support@citrusleaf.com";
static const char *strData2 = "caza@citrusleaf.com";
static const char *myKey    = "myKey";
static const char *myKey2   = "myKey2";

static int test_getall(cl_cluster *clc);
static int read_mod_write(cl_cluster *clc);
static int test_unique(cl_cluster *clc);
static int test_operate(cl_cluster *clc);
static int test_batch(cl_cluster *clc);

int main(int argc, char **argv){
	cl_cluster   *clc;
	cl_rv        return_value;
	cl_object key1;
	cl_object key2;

	printf(" STARTING TESTS\n");
	// initialize internal citrusleaf structures just once
	citrusleaf_init();

	// Create a cluster with a particular starting host
	printf(" STARTING CLUSTER CREATION TEST .... \n");
	clc = citrusleaf_cluster_create();
	if (!clc){
		printf("TEST FAILED: Could not create cluster object");
	    return(-1);
	}
/*
	return_value = citrusleaf_cluster_add_host(clc, badhost, 3000, 1000);
	if( return_value != CITRUSLEAF_FAIL_TIMEOUT ){
		printf("TEST FAILED: return value on bad host connection incorrect. %d\n", return_value);
		return(-1);
	}
	// this test fails.  The code internally checks for cluster completeness before it returns - that is,
	// whether we have a connection to all the nodes we know about.  However, since the host
	// I've given it in this test is not a node, the check succeeds. The cluster is complete at 0 nodes
	// --CSW
*/

	return_value = citrusleaf_cluster_add_host(clc, host, 3000, 1000);
	if( return_value != CITRUSLEAF_OK ){
		printf("TEST FAILED - cannot connect to host\n");
		return -1;
	}

	// XXX - need to do some info calls with a bigger cluster!

	printf(" DONE\n");


	// set up the key. Create a stack object, set its value to a string
	cl_object    key_obj;
	citrusleaf_object_init_str(&key_obj, myKey);

	// set up a specific bins to fetch
	// the response will be in this value
	cl_bin       values[3];
	strcpy( &values[0].bin_name[0], bin1 );
	citrusleaf_object_init_str( &values[0].object, strData);
	strcpy( &values[1].bin_name[0], bin2);
	citrusleaf_object_init_int( &values[1].object, intData );
	strcpy( &values[2].bin_name[0], bin3);
	citrusleaf_object_init_blob( &values[2].object, blobData, strlen(blobData)+1);

	printf("params to put are clc %p, ns %s, set %s, key %p, values %p\n", clc, ns, myset, &key_obj, values);
	return_value = citrusleaf_put(clc, ns, myset, &key_obj, values, 3, NULL);
	if( return_value != CITRUSLEAF_OK ){
		printf(" TEST FAILS - INITIAL PUT FAILS, value is %d\n", return_value);
		return(-1);
	}

	citrusleaf_object_init(&values[0].object);
	citrusleaf_object_init(&values[1].object);
	citrusleaf_object_init(&values[2].object);

	return_value = citrusleaf_get(clc, ns, myset, &key_obj, 
		values, 3, 0, NULL, NULL);

	switch (return_value) {
    	case CITRUSLEAF_OK:
       		if (values[0].object.type != CL_STR) {
			printf(" TEST FAILS - value has unexpected type %d\n",values[0].object.type);
			goto cleanup;
       		} else if( strcmp(values[0].object.u.str, strData) ){
			printf("TEST FAILS - WRITE DOES NOT RETURN WHAT WAS WRITTEN: %s, %s\n", values[0].object.u.str, strData);
			goto cleanup;
		} 
	
       		if (values[1].object.type != CL_INT) {     
			printf(" TEST FAILS - value has unexpected type %d\n",values[1].object.type);
			goto cleanup;
       		} else if( values[1].object.u.i64 != intData){
			printf("TEST FAILS - WRITE DOES NOT RETURN WHAT WAS WRITTEN, %lu, %lu\n", values[1].object.u.i64, intData);
			goto cleanup;
       		} 
		if( values[2].object.type != CL_BLOB) {
			printf(" TEST FAILS - value has unexpected type %d\n",values[1].object.type);
			goto cleanup;
		}else if( strcmp(values[2].object.u.blob, blobData) ){
			printf(" TEST FAILS - WRITE DOES NOT RETURN CORRECT BLOB DATA\n");
			goto cleanup;
		}
      		break;
    	case CITRUSLEAF_FAIL_NOTFOUND:
    		printf(" TEST FAILS - citrusleaf says that key does not exist\n");
		goto cleanup;
    		break;
    	case CITRUSLEAF_FAIL_CLIENT:
    		printf(" TEST FAILS - citrusleaf client error: local error\n");
		goto cleanup;
    		break;
    	case CITRUSLEAF_FAIL_PARAMETER:
    		printf(" TEST FAILS - citrusleaf - bad parameter passed in \n");
		goto cleanup;
    		break;
    	case CITRUSLEAF_FAIL_TIMEOUT:
		printf(" TEST FAILS - citrusleaf - timeout on get\n");
		goto cleanup;
		break;
    	case CITRUSLEAF_FAIL_UNKNOWN:
		printf(" TEST _FAILS - citrusleaf - unknown server error\n");
		goto cleanup;
		break;
    	default :
		printf(" TEST_FAILS - error %d\n", return_value);
		goto cleanup;

	}
	// clean up the retrieved objects
	citrusleaf_object_free(&values[0].object);
	citrusleaf_object_free(&values[1].object);

	if( test_getall(clc) )    goto cleanup;
	if( read_mod_write(clc) ) goto cleanup;
	if( test_unique(clc) )    goto cleanup;
	if( test_operate(clc) )   goto cleanup;
	if( test_batch(clc) )     goto cleanup;
	
	printf("TEST SUCCESSFUL!\n");

cleanup:

	citrusleaf_object_init_str(&key1,myKey);
	citrusleaf_object_init_str(&key2,myKey2);
	citrusleaf_delete(clc, ns, myset, &key1, NULL);
	citrusleaf_delete(clc, ns, myset, &key2, NULL);
	// Clean up the cluster object
	citrusleaf_cluster_destroy(clc);
	// Clean up the unit
	citrusleaf_shutdown();
  
}

// This test looks at basic get_all functionality.  The values are
// assumed to have been previously set up, and include a single int, string, and blob.
// In addition to testing data validity, we also test that the internal 
// 'free' pointers have been correctly set so that we do not leak (or duplicate free)
// memory.

int test_getall(cl_cluster *clc){

	// set up the key.
	cl_object      key_obj;
	citrusleaf_object_init_str(&key_obj, myKey);

	// create variables to return all values
	cl_bin         *bins;
	int            n_bins;

	// do the get
	citrusleaf_get_all(clc, ns, myset, &key_obj, 
		&bins, &n_bins, 0, NULL, NULL);

	// check the contained values
	int haveStr = 0;
	int haveInt = 0;
	int haveBlob = 0;
	if( n_bins != 3 ){
		printf(" TEST FAILED - get_all returns wrong number of bins, %d\n", n_bins);
		return -1;
	}

	for (int i=0;i<n_bins;i++) {
     		printf (" bin %d name %s\n",i,bins[i].bin_name);
     		if (bins[i].object.type == CL_STR){
			if( strcmp(bins[i].object.u.str, strData) ){
				printf(" TEST FAILED - str output of get_all does not match input\n"); 
				return -1;
			}
			if( !bins[i].object.free ){
				printf(" TEST FAILED - string allocated, but free pointer not set\n");
				return -1;
			}
			haveStr = 1;
     		}else if (bins[i].object.type == CL_INT ){
			if( bins[i].object.u.i64 != intData ){
				printf(" TEST FAILED - int output of get_all does not match input\n");
				return -1;
			}
			if( bins[i].object.free ){
				printf(" TEST FAILED - int output indicated as allocated but is not\n");
				return -1;
			}
			haveInt = 1;
		}else if( bins[i].object.type == CL_BLOB ){
			haveBlob = 1; 
			if( strcmp(bins[i].object.u.blob, blobData) ){
				printf(" TEST FAILED - blob output does not match input\n");
				return -1;
			}
			// check - free pointer set?
			if( !bins[i].object.free ){
				printf(" TEST FAILED - blob allocated, but free pointer not set\n");
				return -1;
			}
    		}else{ 
       			printf("TEST FAILED - unexpected bin type %d\n",(int) bins[i].object.type);
			return(-1);
		}
    	}

	if( !(haveInt && haveStr && haveBlob ) ){
		printf("TEST FAILED - not all values have correct types\n");
		return -1;
	}
	// free the allocated memory
	for( int i=0; i<n_bins;i++) {
    		citrusleaf_object_free(&bins[i].object);
	}	
	free(bins);
	return 0;
}

// This is a read-modify-write test. We read the data and the generation count, and 
// then write the data using varous wp parameters
int read_mod_write(cl_cluster *clc)
{
	cl_object key;
	cl_bin    bin;
	cl_rv     rv;
	uint32_t  gen_count;

	citrusleaf_object_init_str(&key, myKey);
	strcpy(&bin.bin_name[0],bin1);
	citrusleaf_object_init(&bin.object);
	rv = citrusleaf_get(clc, ns, myset, &key, &bin, 1, 0, &gen_count, NULL);

	if( rv != CITRUSLEAF_OK ){
		printf(" TEST FAILED - Get returns value %d\n", rv);
		return -1;
	}

	// reuse old bin - must free memory allocated by system first
	citrusleaf_object_free(&bin.object); // check - does free reset the free pointer? XXX
	if( bin.object.free ){
		printf(" TEST FAILED - free pointer not reset on object_free \n");
		return -1;
	}
	citrusleaf_object_init_str(&bin.object,strData2); 
	cl_write_parameters cl_wp;
	cl_write_parameters_set_default(&cl_wp);
	cl_write_parameters_set_generation(&cl_wp, gen_count);

	// now attempt to write with the same gen count - should work just fine
	citrusleaf_object_init_str(&bin.object, strData2);
	rv = citrusleaf_put(clc, ns, myset, &key, &bin, 1, &cl_wp);

	if( rv != CITRUSLEAF_OK ){
		printf(" TEST FAILED - put with gen count fails!\n");
		return -1;
	} 

	// now attempt to write again - gen count on server should have changed!
	citrusleaf_object_init_str(&bin.object, "badData");
	rv = citrusleaf_put(clc, ns, myset, &key, &bin, 1, &cl_wp);
	if( rv != CITRUSLEAF_FAIL_GENERATION ){
		printf(" TEST FAILED - generation count should fail, actual return value is %d\n", rv);
		return -1;
	}

	// check that value has not changed
	citrusleaf_object_init(&bin.object);
	uint32_t new_gen;
	rv = citrusleaf_get(clc, ns, myset, &key, &bin, 1,0, &new_gen, NULL);
	if( rv != CITRUSLEAF_OK ){
		printf(" TEST FAILED - get in rmw is failing\n");
		return -1;
	}

	if( strcmp(bin.object.u.str, strData2) ){
		printf(" TEST FAILED - data on server changes despite generation count!!\n");
		return -1;
	}
	citrusleaf_object_free(&bin.object);


	// one more time - use the generation gt thing...
	cl_write_parameters_set_default(&cl_wp);
	cl_write_parameters_set_generation_gt(&cl_wp, gen_count+2);

	citrusleaf_object_init_str(&bin.object, strData);
	rv = citrusleaf_put(clc, ns, myset, &key, &bin, 1, &cl_wp);

	if( rv != CITRUSLEAF_OK ){
		printf(" TEST FAILED - put with gen count gt fails! err %d gen count %d\n", rv, gen_count);
		return -1;
	} 

	// check that value is correct - and get the new gen_count
	citrusleaf_object_init(&bin.object);
	rv = citrusleaf_get(clc, ns, myset, &key, &bin, 1, 0, &gen_count, NULL);
	if( rv != CITRUSLEAF_OK ){
		printf(" TEST FAILED - get in rmw is failing\n");
		return -1;
	}

	if( strcmp(bin.object.u.str, strData) ){
		printf(" TEST FAILED - data on server changes despite generation count!!\n");
		return -1;
	}
	citrusleaf_object_free(&bin.object);

	// now attempt to write again - gen count on server should have changed!
	citrusleaf_object_init_str(&bin.object, "badData");
	cl_write_parameters_set_default(&cl_wp);
	cl_write_parameters_set_generation_gt(&cl_wp, gen_count);
	rv = citrusleaf_put(clc, ns, myset, &key, &bin, 1, &cl_wp);
	if( rv != CITRUSLEAF_FAIL_GENERATION ){
		printf(" TEST FAILED - generation count should fail, actual return value is %d\n", rv);
		return -1;
	}

	// check that value has not changed
	citrusleaf_object_init(&bin.object);
	rv = citrusleaf_get(clc, ns, myset, &key, &bin, 1, 0, NULL, NULL);
	if( rv != CITRUSLEAF_OK ){
		printf(" TEST FAILED - get in rmw is failing\n");
		return -1;
	}

	if( strcmp(bin.object.u.str, strData) ){
		printf(" TEST FAILED - data on server changes despite generation count!!\n");
		return -1;
	}

	// at the end of this function, bin1 is strdata
	return 0;
}

int test_unique(cl_cluster *clc)
{
	cl_object key;
	cl_object key2;
	cl_bin    bin;
	cl_rv     rv;
	cl_write_parameters cl_wp;

	citrusleaf_object_init_str(&key, myKey);
	citrusleaf_object_init_str(&key2, myKey2);
	strcpy(&bin.bin_name[0],bin1);
	citrusleaf_object_init_str(&bin.object, strData2);

	cl_write_parameters_set_default(&cl_wp);
	cl_wp.unique = 1;

	rv = citrusleaf_put(clc, ns, myset, &key, &bin, 1, &cl_wp);
	if( rv != CITRUSLEAF_FAIL_KEY_EXISTS ){
		printf(" TEST FAILED - test unique: should return key exists, returns %d\n", rv);
		return -1;
	}

	rv = citrusleaf_put(clc, ns, myset, &key2, &bin, 1, &cl_wp);
	if( rv != CITRUSLEAF_OK ){
		printf(" TEST FAILED - test unique: value should have been able to be written, actual value %d\n",rv);
		return -1;
	}
	
	return 0;
}

int test_operate(cl_cluster *clc)
{
	cl_object key;
	cl_operation ops[3];
	cl_rv rv;
	
	citrusleaf_object_init_str(&key, myKey);
	strcpy(&ops[0].bin.bin_name[0],bin1);
	strcpy(&ops[1].bin.bin_name[0],bin2);
	strcpy(&ops[2].bin.bin_name[0],bin3);
	citrusleaf_object_init(&ops[0].bin.object);
	citrusleaf_object_init_int(&ops[1].bin.object, 2);
	citrusleaf_object_init_blob(&ops[2].bin.object, blobData2, strlen(blobData2)+1);
	
	ops[0].op = CL_OP_READ;
	ops[1].op = CL_OP_INCR;
	ops[2].op = CL_OP_WRITE;
	
	rv = citrusleaf_operate(clc, ns, myset, &key, &ops[0], 3, NULL, NULL);
	if( rv != CITRUSLEAF_OK ){
		printf(" TEST FAILED - go-right case of Operate is failing with %d\n", rv);
		return -1;
	}
	// and look at the value we read...
	if( strcmp(ops[0].bin.object.u.str, strData) ){
		printf( "TEST FAILED - Operate did not read back correct data! %s, %s\n", ops[0].bin.object.u.str, strData);
		return -1;
	}

	// and release that value...
	citrusleaf_object_free(&ops[0].bin.object);

	// now read the values back.
	ops[0].op = CL_OP_READ;
	ops[1].op = CL_OP_READ;
	ops[2].op = CL_OP_READ;
	
	citrusleaf_object_init(&ops[0].bin.object);
	citrusleaf_object_init(&ops[1].bin.object);
	citrusleaf_object_init(&ops[2].bin.object);

	rv = citrusleaf_operate(clc, ns, myset, &key, &ops[0], 3, NULL, NULL);
	if( rv != CITRUSLEAF_OK ){
		printf(" TEST FAILED - go-right case of Operate is failing with %d\n", rv);
		return -1;
	}

	// check the values...
	if( strcmp(ops[0].bin.object.u.str, strData) ){
		printf(" TEST FAILED - did not read back the same string\n");
		return -1;
	}

	if( ops[1].bin.object.u.i64 != intData+2 ){
		printf(" TEST FAILED - did not read back correct int %lu %lu\n", ops[1].bin.object.u.i64, intData+2);
		return -1;
	}

	if( strcmp(ops[2].bin.object.u.blob, blobData2 ) ){
		printf(" TEST FAILED - did not read back blob correctly %s, %s\n", (char *)ops[2].bin.object.u.blob, blobData2);
		return -1;
	}

	// and free them all...
	citrusleaf_object_free(&ops[0].bin.object);	
	citrusleaf_object_free(&ops[1].bin.object);	
	citrusleaf_object_free(&ops[2].bin.object);
	
	// what happens if I request something that doesn't exist?  // XXX - should do this elsewhere...
/*	strcpy(&ops[0].bin.bin_name[0], "doesnt exist");
	rv = citrusleaf_operate(clc, mySet, myKey, &ops[3], 3, NULL );
*/		

	return 0;	
}


int count = 0;
int batch_cb(char *ns, cl_object *key, cf_digest *ked, uint32_t generation, uint32_t record_ttl, cl_bin *bins, int n_bins, 
	bool is_last, void *udata)
{
	printf(" batch cb - number is %d\n", count++);
	if( is_last ){
		printf(" batch cb - last call\n");
		count=0;
	}

	printf(" batch cb - we've got namespace %s, key %p, digest %p, generation %d, ttl %d, bins %p, n_bins %d, is last %d, udata %s\n", 
		ns, key, ked, generation, record_ttl, bins, n_bins, is_last, (char *)udata);
	
	
	// XXX what happens with the return value here?

	return 1;
}

int test_batch(cl_cluster *clc)
{
	cl_rv rv;
	cl_bin bins[3];
	cl_object keys[2];
	cf_digest digests[2];
	char *userData = "foobar";

	citrusleaf_object_init_str(&keys[0],myKey);
	citrusleaf_object_init_str(&keys[1],myKey2);

	citrusleaf_object_init(&bins[0].object); 
	citrusleaf_object_init(&bins[1].object); 
	citrusleaf_object_init(&bins[2].object);

	strcpy(&bins[0].bin_name[0], bin1); 	
	strcpy(&bins[1].bin_name[0], bin2); 
	strcpy(&bins[2].bin_name[0], bin3); 

	citrusleaf_calculate_digest(myset, &keys[0], &digests[0]);
	citrusleaf_calculate_digest(myset, &keys[1], &digests[1]);
	
	rv = citrusleaf_get_many_digest(clc, (char *)ns, digests, 2, bins, 3, false, batch_cb, userData);

	if( rv != CITRUSLEAF_OK ){
		printf(" TEST FAILS - get many (batch) fails with %d\n", rv);
		return -1;
	}

	return 0;
}
// test batch
// test operate function - and the add functionality

/*
rv = citrusleaf_put(clc, "test", "myset", &key, &bin, 1, &cl_wp);
}

void test3(cl_cluster *clc){
cl_object key;
cl_bin    bin;
cl_rv     rv;
cl_write_parameters cl_wp;

char *myOldKeyStr="key";
char *myOldBinName="bin1";
char *myOldBinValue="value1";
char *myOldNamespace="test";
char *myOldSet="myset";
uint32_t myOldGenCount=3;
*/
/* we assume variables 'myOldKeyStr', 'myOldBinName', 'myOldBinValue', 'myOldNamespace, 'myOldSet'
and 'myOldGenCount' have been initialized elsewhere */
/*
citrusleaf_object_init_str(&key, myOldKeyStr);
citrusleaf_object_init_str(&bin.object, myOldBinValue);
strcpy(&bin.bin_name[0], myOldBinName);
cl_write_parameters_set_default(&cl_wp);
cl_write_parameters_set_generation_gt(&cl_wp, myOldGenCount);

rv = citrusleaf_put(clc, myOldNamespace, myOldSet, &key, &bin, 1, &cl_wp);
}

void test4(cl_cluster *clc){
// set up the key.
cl_object      key_obj;
citrusleaf_object_init_str(&key_obj, "mykey");

// set up the operations – write the new email address, read the zip code
cl_operation   ops[2];
ops[0].op = CL_OP_WRITE;
strcpy(ops[0].bin.bin_name, "email");
citrusleaf_object_init_str(&ops[0].bin.object,"brian@bulkowski.org");
ops[1].op = CL_OP_READ;
strcpy(ops[0].bin.bin_name, "zipcode");
citrusleaf_object_init(&ops[0].bin.object);

// the operate call does all
citrusleaf_operate(clc, "mynamespace", "myset", &key_obj, ops, 2, 0);

// print the zipcode for fun
if (ops[1].bin.object.type == CL_STR)
    printf("  zip code is %s\n",ops[1].bin.object.u.str);
else
    printf("  zip code is unexpected type\n");
citrusleaf_object_free(&ops[1].bin.object);
}
*/
