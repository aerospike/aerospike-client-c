
#include <aerospike/aerospike.h>
#include <aerospike/aerospike_udf.h>
#include <aerospike/aerospike_key.h>

#include <aerospike/as_error.h>
#include <aerospike/as_status.h>

#include <aerospike/as_record.h>
#include <aerospike/as_integer.h>
#include <aerospike/as_string.h>
#include <aerospike/as_list.h>
#include <aerospike/as_arraylist.h>
#include <aerospike/as_map.h>
#include <aerospike/as_nil.h>
#include <aerospike/as_hashmap.h>
#include <aerospike/as_stringmap.h>
#include <aerospike/as_val.h>
#include <aerospike/as_udf.h>

#include <time.h>

#include "../test.h"
#include "../util/udf.h"
#include "../util/info_util.h"

/******************************************************************************
 * GLOBAL VARS
 *****************************************************************************/

extern aerospike * as;

/******************************************************************************
 * MACROS
 *****************************************************************************/

#define LUA_FILE "src/test/lua/udf_record.lua"
#define UDF_FILE "udf_record"

#define WAIT_MS(__ms) nanosleep((struct timespec[]){{0, __ms##000000}}, NULL)

/******************************************************************************
 * TEST CASES
 *****************************************************************************/

TEST( udf_record_pre , "upload udf_record.lua" ) {

  const char * filename = UDF_FILE".lua";

  as_error err;
  as_bytes content;

  info("reading: %s",LUA_FILE);
  udf_readfile(LUA_FILE, &content);

  info("uploading: %s",filename);
  aerospike_udf_put(as, &err, NULL, filename, AS_UDF_TYPE_LUA, &content);

  assert_int_eq( err.code, AEROSPIKE_OK );

  WAIT_MS(100);

  as_bytes_destroy(&content);
}

TEST( udf_record_post , "remove udf_record.lua" ) {

  const char * filename = UDF_FILE".lua";

  as_error err;

  aerospike_udf_remove(as, &err, NULL, filename);

  assert_int_eq( err.code, AEROSPIKE_OK );

  WAIT_MS(100);

}

bool udf_record_update_map_foreach(const as_val * key, const as_val * value, void * udata) {
  char * k = as_val_tostring(key);
  char * v = as_val_tostring(value);
  fprintf(stderr, "%s=%s\n", k, v);
  free(k);
  free(v);
  return true;
}

TEST( udf_record_update_map, "udf_record.update_map()" ) {

  as_error err;

  as_key key;
  as_key_init(&key, "test", "test", "test");

  as_arraylist args;
  as_arraylist_init(&args,2,0);
  as_arraylist_append_str(&args, "a");
  as_arraylist_append_int64(&args, 2);

  as_val * val = NULL;

  aerospike_key_apply(as, &err, NULL, &key, "udf_record", "update_map", (as_list *) &args, &val);
  
  assert_int_eq( err.code, AEROSPIKE_OK );
  assert_int_eq( as_val_type(val), AS_STRING );

  char * s = as_val_tostring(val);
  info(s);
  free(s);

  // as_map * mval = as_map_fromval(val);
  // assert_int_eq( as_map_size(mval), 3);
  // assert_int_eq( as_stringmap_get_int64(mval,"b"), as_stringmap_get_int64(mval,"a") + 1);
  // assert_int_eq( as_stringmap_get_int64(mval,"c"), as_stringmap_get_int64(mval,"a") + 2);

  // as_map_foreach(mval, udf_record_update_map_foreach, NULL);
  
  as_val_destroy(val);
  as_key_destroy(&key);
}

/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

SUITE( udf_record, "aerospike_udf record tests" ) {
  suite_add( udf_record_pre );
  suite_add( udf_record_update_map );
  suite_add( udf_record_post );
}
