/*
 * Copyright 2008-2018 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
#include <aerospike/aerospike.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/aerospike_index.h>

#include <aerospike/as_error.h>
#include <aerospike/as_hashmap.h>
#include <aerospike/as_map_operations.h>
#include <aerospike/as_status.h>
#include <aerospike/as_stringmap.h>
#include <aerospike/as_val.h>

#include "../test.h"
#include "../util/index_util.h"

/******************************************************************************
 * GLOBAL VARS
 *****************************************************************************/

extern aerospike *as;

/******************************************************************************
 * MACROS
 *****************************************************************************/

#define NAMESPACE "test"
#define SET "map_index"

typedef struct {
	const char *bin_name;
	const char *index_name;
	as_index_type index_type;
	as_index_datatype index_datatype;
} test_index;

static const test_index index_table[] = {
		{"map_keystr_bin", "idx_map_keystr_bin", AS_INDEX_TYPE_MAPKEYS, AS_INDEX_STRING},
		{"map_valstr_bin", "idx_map_valstr_bin", AS_INDEX_TYPE_MAPVALUES, AS_INDEX_STRING},
};
static const size_t index_table_size = sizeof(index_table) / sizeof(test_index);

as_map_order types[] = {
		AS_MAP_UNORDERED,
		AS_MAP_KEY_ORDERED,
		AS_MAP_KEY_VALUE_ORDERED,
};
const size_t types_count = sizeof(types) / sizeof(as_map_order);


/******************************************************************************
 * TEST CASES
 *****************************************************************************/

TEST( map_index_pre , "create indexes" )
{
	as_hashmap items;
	as_hashmap_init(&items, 1);
	as_stringmap_set_str((as_map *)&items, "key", "value");

	for (size_t type_i = 0; type_i < types_count; type_i++) {
		as_map_order type = types[type_i];
		info("type = %d", type);
		as_map_policy pol;
		as_map_policy_set(&pol, type, 0);

		as_key key;
		as_key_init_int64(&key, NAMESPACE, SET, type_i);

		for (size_t i = 0; i < index_table_size; i++) {
			as_error err;
			as_operations ops;

			as_operations_init(&ops, 2);
			as_val_reserve((as_val *)&items);
			as_operations_add_write(&ops, index_table[i].bin_name, (as_bin_value *)&items);
			as_operations_add_map_set_policy(&ops, index_table[i].bin_name, &pol);

			aerospike_key_operate(as, &err, NULL, &key, &ops, NULL);
			as_operations_destroy(&ops);
		}

		as_key_destroy(&key);
	}

	as_hashmap_destroy(&items);

	for (size_t i = 0; i < index_table_size; i++) {
		as_error err;
		as_index_task task;
		as_status status;

		as_error_reset(&err);
		status = aerospike_index_create_complex(as, &err, &task, NULL, NAMESPACE, SET, index_table[i].bin_name, index_table[i].index_name, index_table[i].index_type, index_table[i].index_datatype);

		if (! index_process_return_code(status, &err, &task)) {
			assert_int_eq( status , AEROSPIKE_OK );
		}
	}
}

TEST( map_index_post , "drop indexes" )
{
	for (size_t i = 0; i < index_table_size; i++) {
		as_error err;
		as_error_reset(&err);

		aerospike_index_remove(as, &err, NULL, NAMESPACE, index_table[i].index_name);
		if ( err.code != AEROSPIKE_OK ) {
			info("error(%d): %s", err.code, err.message);
		}
		assert_int_eq( err.code, AEROSPIKE_OK );
	}
}

TEST( map_index_update, "update map" )
{
	as_hashmap items;
	as_hashmap_init(&items, 4);
	as_stringmap_set_str((as_map *)&items, "key0", "value0");
	as_stringmap_set_str((as_map *)&items, "key1", "value1");
	as_stringmap_set_str((as_map *)&items, "key2", "value2");
	as_stringmap_set_str((as_map *)&items, "key3", "value3");

	for (size_t type_i = 0; type_i < types_count; type_i++) {
		as_map_order type = types[type_i];
		as_map_policy pol;
		as_map_policy_set(&pol, type, 0);

		as_key key;
		as_key_init_int64(&key, NAMESPACE, SET, type_i);

		for (size_t i = 0; i < index_table_size; i++) {
			as_operations ops;
			as_operations_init(&ops, 1);

			as_val_reserve((as_val *)&items);
			as_operations_add_map_put_items(&ops, index_table[i].bin_name, &pol, (as_map *)&items);

			as_error err;
			aerospike_key_operate(as, &err, NULL, &key, &ops, NULL);
			as_operations_destroy(&ops);
		}

		as_key_destroy(&key);
	}

	as_hashmap_destroy(&items);
}

/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

SUITE( map_index, "aerospike_map index tests" ) {
	suite_add( map_index_pre );
	suite_add( map_index_update );
	suite_add( map_index_post );
}
