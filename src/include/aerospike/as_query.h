/*
 * Copyright 2008-2024 Aerospike, Inc.
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
#pragma once

#include <aerospike/aerospike_index.h>
#include <aerospike/as_bin.h>
#include <aerospike/as_key.h>
#include <aerospike/as_list.h>
#include <aerospike/as_partition_filter.h>
#include <aerospike/as_udf.h>

#include <stdarg.h>

#ifdef __cplusplus
extern "C" {
#endif

//---------------------------------
// Macros
//---------------------------------

/**
 * Filter on string bins.
 *
 * ~~~~~~~~~~{.c}
 * as_query_where(query, "bin1", as_string_equals("abc"));
 * ~~~~~~~~~~
 *
 * @relates as_query
 * @ingroup query_operations
 */
#define as_string_equals(__val) AS_PREDICATE_EQUAL, AS_INDEX_TYPE_DEFAULT, AS_INDEX_STRING, __val

/**
 * Filter on blob bins.
 * Requires server version 7.0+.
 *
 * ~~~~~~~~~~{.c}
 * // as_blob_equals(uint8_t* bytes, uint32_t size, bool free)
 * as_query_where(query, "bin1", as_blob_equals(bytes, size, true));
 * ~~~~~~~~~~
 *
 * @relates as_query
 * @ingroup query_operations
 */
#define as_blob_equals(__val, __size, __free) AS_PREDICATE_EQUAL, AS_INDEX_TYPE_DEFAULT, AS_INDEX_BLOB, __val, __size, __free

/**
 * Filter on integer bins.
 *
 * ~~~~~~~~~~{.c}
 * as_query_where(query, "bin1", as_integer_equals(123));
 * ~~~~~~~~~~
 *
 * @relates as_query
 * @ingroup query_operations
 */
#define as_integer_equals(__val) AS_PREDICATE_EQUAL, AS_INDEX_TYPE_DEFAULT, AS_INDEX_NUMERIC, (int64_t)__val

/**
 * Ranger filter on integer bins.
 *
 * ~~~~~~~~~~{.c}
 * as_query_where(query, "bin1", as_integer_range(1,100));
 * ~~~~~~~~~~
 * 
 * @relates as_query
 * @ingroup query_operations
 */
#define as_integer_range(__min, __max) AS_PREDICATE_RANGE, AS_INDEX_TYPE_DEFAULT, AS_INDEX_NUMERIC, (int64_t)__min, (int64_t)__max

/**
 * Range filter on list/map elements.
 *
 * ~~~~~~~~~~{.c}
 * as_query_where(query, "bin1", as_range(LIST,NUMERIC,1,100));
 * ~~~~~~~~~~
 * 
 * @relates as_query
 * @ingroup query_operations
 */
#define as_range(indextype, datatype, __min, __max) AS_PREDICATE_RANGE, AS_INDEX_TYPE_ ##indextype, AS_INDEX_ ##datatype, __min, __max

/**
 * Contains filter on list/map elements.
 *
 * ~~~~~~~~~~{.c}
 * as_query_where(query, "bin1", as_contains(LIST,STRING,"val"));
 * ~~~~~~~~~~
 * 
 * @relates as_query
 * @ingroup query_operations
 */
#define as_contains(indextype, datatype, __val) AS_PREDICATE_EQUAL, AS_INDEX_TYPE_ ##indextype, AS_INDEX_ ##datatype, __val

/**
 * Contains blob filter on list/map elements.
 * Requires server version 7.0+.
 *
 * ~~~~~~~~~~{.c}
 * // as_blob_contains(type, uint8_t* bytes, uint32_t size, bool free)
 * as_query_where(query, "bin1", as_blob_equals(LIST, bytes, size, true));
 * ~~~~~~~~~~
 *
 * @relates as_query
 * @ingroup query_operations
 */
#define as_blob_contains(indextype, __val, __size, __free) AS_PREDICATE_EQUAL, AS_INDEX_TYPE_ ##indextype, AS_INDEX_BLOB, __val, __size, __free

/**
 * Filter specified type on bins.
 *
 * ~~~~~~~~~~{.c}
 * as_query_where(query, "bin1", as_equals(NUMERIC,5));
 * ~~~~~~~~~~
 * 
 * @relates as_query
 * @ingroup query_operations
 */
#define as_equals(datatype, __val) AS_PREDICATE_EQUAL, AS_INDEX_TYPE_DEFAULT, AS_INDEX_ ##datatype, __val

/**
 * Within filter on GEO bins.
 *
 * ~~~~~~~~~~{.c}
 * as_query_where(query, "bin1", as_geo_within(region));
 * ~~~~~~~~~~
 * 
 * @relates as_query
 * @ingroup query_operations
 */
#define as_geo_within(__val) AS_PREDICATE_RANGE, AS_INDEX_TYPE_DEFAULT, AS_INDEX_GEO2DSPHERE, __val

/**
 * Contains filter on GEO bins.
 *
 * ~~~~~~~~~~{.c}
 * as_query_where(query, "bin1", as_geo_contains(region));
 * ~~~~~~~~~~
 * 
 * @relates as_query
 * @ingroup query_operations
 */
#define as_geo_contains(__val) AS_PREDICATE_RANGE, AS_INDEX_TYPE_DEFAULT, AS_INDEX_GEO2DSPHERE, __val

//---------------------------------
// Types
//---------------------------------

struct as_operations_s;

/**
 * Union of supported predicates
 */
typedef union as_predicate_value_u {
	int64_t integer;

	struct {
		char* string;
		bool _free;
	} string_val;

	struct {
		uint8_t* bytes;
		uint32_t bytes_size;
		bool _free;
	} blob_val;

	struct {
		int64_t min;
		int64_t max;
	} integer_range;
} as_predicate_value;

/**
 * The types of predicates supported.
 */
typedef enum as_predicate_type_e {

	/**
	 * String Equality Predicate. 
	 * Requires as_predicate_value.string to be set.
	 */
	AS_PREDICATE_EQUAL,

	AS_PREDICATE_RANGE
} as_predicate_type;

/**
 * Defines a predicate, including the bin, type of predicate and the value
 * for the predicate.
 */
typedef struct as_predicate_s {

	/**
	 * Bin to apply the predicate to
	 */
	as_bin_name bin;

	/**
	 * The CDT context to query. Use as_query_where_with_ctx() to set.
	 */
	struct as_cdt_ctx* ctx;

	/**
	 * The size of the CDT context. Use as_query_where_with_ctx() to set.
	 */
	uint32_t ctx_size;

	/**
	 * Should ctx be destroyed on as_query_destroy(). Default: false.
	 */
	bool ctx_free;

	/**
	 * The predicate type, dictates which values to use from the union
	 */
	as_predicate_type type;

	/**
	 * The value for the predicate.
	 */
	as_predicate_value value;

	/*
	 * The type of data user wants to query
	 */

	as_index_datatype dtype;

	/*
	 * The type of index predicate is on
	 */
	as_index_type itype;
} as_predicate;

/**
 * Enumerations defining the direction of an ordering.
 */
typedef enum as_order_e {

	/**
	 * Ascending order
	 */
	AS_ORDER_ASCENDING = 0,

	/**
	 * bin should be in ascending order
	 */
	AS_ORDER_DESCENDING = 1

} as_order;


/**
 * Defines the direction a bin should be ordered by.
 */
typedef struct as_ordering_s {

	/**
	 * Name of the bin to sort by
	 */
	as_bin_name bin;

	/**
	 * Direction of the sort
	 */
	as_order order;

} as_ordering;

/**
 * Sequence of bins which should be selected during a query.
 *
 * Entries can either be initialized on the stack or on the heap.
 *
 * Initialization should be performed via a query object, using:
 * - as_query_select_init()
 * - as_query_select_inita()
 */
typedef struct as_query_bins_s {

	/**
	 * @private
	 * If true, then as_query_destroy() will free this instance.
	 */
	bool _free;

	/**
	 * Number of entries allocated
	 */
	uint16_t capacity;

	/**
	 * Number of entries used
	 */
	uint16_t size;

	/**
	 * Sequence of entries
	 */
	as_bin_name * entries;

} as_query_bins;

/**
 * Sequence of predicates to be applied to a query.
 *
 * Entries can either be initialized on the stack or on the heap.
 *
 * Initialization should be performed via a query object, using:
 * - as_query_where_init()
 * - as_query_where_inita()
 */
typedef struct as_query_predicates_s {

	/**
	 * @private
	 * If true, then as_query_destroy() will free this instance.
	 */
	bool _free;

	/**
	 * Number of entries allocated
	 */
	uint16_t capacity;

	/**
	 * Number of entries used
	 */
	uint16_t size;

	/**
	 * Sequence of entries
	 */
	as_predicate* entries;

} as_query_predicates;

/**
 * The as_query object is used define a query to be executed in the database.
 *
 * ## Initialization
 * 
 * Before using an as_query, it must be initialized via either: 
 * - as_query_init()
 * - as_query_new()
 * 
 * as_query_init() should be used on a stack allocated as_query. It will
 * initialize the as_query with the given namespace and set. On success,
 * it will return a pointer to the initialized as_query. Otherwise, NULL 
 * is returned.
 *
 * ~~~~~~~~~~{.c}
 * as_query query;
 * as_query_init(&query, "namespace", "set");
 * ~~~~~~~~~~
 *
 * as_query_new() should be used to allocate and initialize a heap allocated
 * as_query. It will allocate the as_query, then initialized it with the 
 * given namespace and set. On success, it will return a pointer to the 
 * initialized as_query. Otherwise, NULL is returned.
 *
 * ~~~~~~~~~~{.c}
 * as_query* query = as_query_new("namespace", "set");
 * ~~~~~~~~~~
 *
 * ## Destruction
 *
 * When you are finished with the as_query, you can destroy it and associated
 * resources:
 *
 * ~~~~~~~~~~{.c}
 * as_query_destroy(query);
 * ~~~~~~~~~~
 *
 * ## Usage
 *
 * The following explains how to use an as_query to build a query.
 *
 * ### Selecting Bins
 *
 * as_query_select() is used to specify the bins to be selected by the query.
 * 
 * ~~~~~~~~~~{.c}
 * as_query_select(query, "bin1");
 * as_query_select(query, "bin2");
 * ~~~~~~~~~~
 *
 * Before adding bins to select, the select structure must be initialized via
 * either:
 * - as_query_select_inita() - Initializes the structure on the stack.
 * - as_query_select_init() - Initializes the structure on the heap.
 *
 * Both functions are given the number of bins to be selected.
 *
 * A complete example using as_query_select_inita()
 *
 * ~~~~~~~~~~{.c}
 * as_query_select_inita(query, 2);
 * as_query_select(query, "bin1");
 * as_query_select(query, "bin2");
 * ~~~~~~~~~~
 *
 * ### Predicates on Bins
 *
 * as_query_where() is used to specify predicates to be added to the the query.
 *
 * **Note:** Currently, a single where predicate is supported. To do more advanced filtering,
 * you will want to use a UDF to process the result set on the server.
 * 
 * ~~~~~~~~~~{.c}
 * as_query_where(query, "bin1", as_string_equals("abc"));
 * ~~~~~~~~~~
 *
 * The predicates that you can apply to a bin include:
 * - as_string_equals() - Test for string equality.
 * - as_integer_equals() - Test for integer equality.
 * - as_integer_range() - Test for integer within a range.
 *
 * Before adding predicates, the where structure must be initialized. To
 * initialize the where structure, you can choose to use one of the following:
 * - as_query_where_inita() - Initializes the structure on the stack.
 * - as_query_where_init() - Initializes the structure on the heap.
 * 
 * Both functions are given the number of predicates to be added.
 *
 * A complete example using as_query_where_inita():
 *
 * ~~~~~~~~~~{.c}
 * as_query_where_inita(query, 1);
 * as_query_where(query, "bin1", as_string_equals("abc"));
 * ~~~~~~~~~~
 *
 * ### Applying a UDF to Query Results
 *
 * A UDF can be applied to the results of a query.
 *
 * To define the UDF for the query, use as_query_apply().
 *
 * ~~~~~~~~~~{.c}
 * as_query_apply(query, "udf_module", "udf_function", arglist);
 * ~~~~~~~~~~
 *
 * @ingroup query_operations
 */
typedef struct as_query_s {

	/**
	 * @private
	 * If true, then as_query_destroy() will free this instance.
	 */
	bool _free;

	/**
	 * Namespace to be queried.
	 *
	 * Should be initialized via either:
	 * - as_query_init() - To initialize a stack allocated query.
	 * - as_query_new() - To heap allocate and initialize a query.
	 */
	as_namespace ns;

	/**
	 * Set to be queried.
	 *
	 * Should be initialized via either:
	 * - as_query_init() - To initialize a stack allocated query.
	 * - as_query_new() - To heap allocate and initialize a query.
	 */
	as_set set;

	/**
	 * Name of bins to select.
	 * 
	 * Use either of the following function to initialize:
	 * - as_query_select_init() - To initialize on the heap.
	 * - as_query_select_inita() - To initialize on the stack.
	 *
	 * Use as_query_select() to populate.
	 */
	as_query_bins select;

	/**
	 * Predicates for filtering.
	 * 
	 * Use either of the following function to initialize:
	 * - as_query_where_init() -	To initialize on the heap.
	 * - as_query_where_inita() - To initialize on the stack.
	 *
	 * Use as_query_where() to populate.
	 */
	as_query_predicates where;

	/**
	 * UDF to apply to results of a background query or a foreground aggregation query.
	 *
	 * Should be set via `as_query_apply()`.
	 */
	as_udf_call apply;

	/**
	 * Perform write operations on a background query.
	 * If ops is set, ops will be destroyed when as_query_destroy() is called.
	 */
	struct as_operations_s* ops;

	/**
	 * Status of all partitions.
	 */
	as_partitions_status* parts_all;

	/**
	 * Approximate number of records to return to client. This number is divided by the
	 * number of nodes involved in the query.  The actual number of records returned
	 * may be less than max_records if node record counts are small and unbalanced across
	 * nodes.
	 *
	 * Default: 0 (do not limit record count)
	 */
	uint64_t max_records;

	/**
	 * Limit returned records per second (rps) rate for each server.
	 * Do not apply rps limit if records_per_second is zero.
	 *
	 * Default: 0
	 */
	uint32_t records_per_second;

	/**
	 * The time-to-live (expiration) of the record in seconds. Note that ttl
	 * is only used on background query writes.
	 *
	 * There are also special values that can be set in the record ttl:
	 * <ul>
	 * <li>AS_RECORD_DEFAULT_TTL: Use the server default ttl from the namespace.</li>
	 * <li>AS_RECORD_NO_EXPIRE_TTL: Do not expire the record.</li>
	 * <li>AS_RECORD_NO_CHANGE_TTL: Keep the existing record ttl when the record is updated.</li>
	 * <li>AS_RECORD_CLIENT_DEFAULT_TTL: Use the default client ttl in as_policy_write.</li>
	 * </ul>
	 */
	uint32_t ttl;

	/**
	 * Should records be read in pages in conjunction with max_records policy.
	 *
	 * Default: false
	 */
	bool paginate;

	/**
	 * Set to true if query should only return keys and no bin data.
	 *
	 * Default: false.
	 */
	bool no_bins;

} as_query;

//---------------------------------
// Instance Functions
//---------------------------------

/**
 * Initialize a stack allocated as_query.
 *
 * ~~~~~~~~~~{.c}
 * as_query query;
 * as_query_init(&query, "test", "demo");
 * ~~~~~~~~~~
 *
 * @param query 	The query to initialize.
 * @param ns 		The namespace to query.
 * @param set 		The set to query.
 *
 * @return On success, the initialized query. Otherwise NULL.
 *
 * @relates as_query
 * @ingroup query_operations
 */
AS_EXTERN as_query*
as_query_init(as_query* query, const char* ns, const char* set);

/**
 * Create and initialize a new heap allocated as_query.
 *
 * ~~~~~~~~~~{.c}
 * as_query* query = as_query_new("test", "demo");
 * ~~~~~~~~~~
 * 
 * @param ns 		The namespace to query.
 * @param set 		The set to query.
 *
 * @return On success, the new query. Otherwise NULL.
 *
 * @relates as_query
 * @ingroup query_operations
 */
AS_EXTERN as_query*
as_query_new(const char* ns, const char* set);

/**
 * Destroy the query and associated resources.
 * 
 * @relates as_query
 * @ingroup query_operations
 */
AS_EXTERN void
as_query_destroy(as_query* query);

//---------------------------------
// Select Functions
//---------------------------------

/** 
 * Initializes `as_query.select` with a capacity of `n` using `alloca`
 *
 * For heap allocation, use `as_query_select_init()`.
 *
 * ~~~~~~~~~~{.c}
 * as_query_select_inita(&query, 2);
 * as_query_select(&query, "bin1");
 * as_query_select(&query, "bin2");
 * ~~~~~~~~~~
 * 
 * @param __query	The query to initialize.
 * @param __n		The number of bins to allocate.
 *
 * @relates as_query
 * @ingroup query_operations
 */
#define as_query_select_inita(__query, __n) \
	do { \
		if ((__query)->select.entries == NULL) {\
			(__query)->select.entries = (as_bin_name*) alloca(sizeof(as_bin_name) * (__n));\
			if ( (__query)->select.entries ) { \
				(__query)->select.capacity = (__n);\
				(__query)->select.size = 0;\
				(__query)->select._free = false;\
			}\
	 	} \
	} while(0)

/** 
 * Initializes `as_query.select` with a capacity of `n` using `malloc()`.
 * 
 * For stack allocation, use `as_query_select_inita()`.
 *
 * ~~~~~~~~~~{.c}
 * as_query_select_init(&query, 2);
 * as_query_select(&query, "bin1");
 * as_query_select(&query, "bin2");
 * ~~~~~~~~~~
 *
 * @param query		The query to initialize.
 * @param n			The number of bins to allocate.
 *
 * @return On success, the initialized. Otherwise an error occurred.
 *
 * @relates as_query
 * @ingroup query_operations
 */
AS_EXTERN bool
as_query_select_init(as_query* query, uint16_t n);

/**
 * Select bins to be projected from matching records.
 *
 * You have to ensure as_query.select has sufficient capacity, prior to 
 * adding a bin. If capacity is sufficient then false is returned.
 *
 * ~~~~~~~~~~{.c}
 * as_query_select_init(&query, 2);
 * as_query_select(&query, "bin1");
 * as_query_select(&query, "bin2");
 * ~~~~~~~~~~
 *
 * @param query		The query to modify.
 * @param bin 		The name of the bin to select.
 *
 * @return On success, true. Otherwise an error occurred.
 * @relates as_query
 * @ingroup query_operations
 */
AS_EXTERN bool
as_query_select(as_query* query, const char * bin);

//---------------------------------
// Where Functions
//---------------------------------

/** 
 * Initializes `as_query.where` with a capacity of `n` using `alloca()`.
 *
 * For heap allocation, use `as_query_where_init()`.
 *
 * ~~~~~~~~~~{.c}
 * as_query_where_inita(&query, 1);
 * as_query_where(&query, "bin1", as_string_equals("abc"));
 * ~~~~~~~~~~
 *
 * @param __query	The query to initialize.
 * @param __n		The number of as_predicate to allocate.
 *
 * @return On success, true. Otherwise an error occurred.
 *
 * @relates as_query
 * @ingroup query_operations
 */
#define as_query_where_inita(__query, __n) \
	do { \
		if ((__query)->where.entries == NULL) {\
			(__query)->where.entries = (as_predicate*) alloca(sizeof(as_predicate) * (__n));\
			if ( (__query)->where.entries ) {\
				(__query)->where.capacity = (__n);\
				(__query)->where.size = 0;\
				(__query)->where._free = false;\
			}\
	 	} \
	} while(0)

/** 
 * Initializes `as_query.where` with a capacity of `n` using `malloc()`.
 *
 * For stack allocation, use `as_query_where_inita()`.
 *
 * ~~~~~~~~~~{.c}
 * as_query_where_init(&query, 1);
 * as_query_where(&query, "bin1", as_integer_equals(123));
 * ~~~~~~~~~~
 *
 * @param query	The query to initialize.
 * @param n		The number of as_predicate to allocate.
 *
 * @return On success, true. Otherwise an error occurred.
 *
 * @relates as_query
 * @ingroup query_operations
 */
AS_EXTERN bool
as_query_where_init(as_query* query, uint16_t n);

/**
 * Add a predicate to the query.
 *
 * You have to ensure as_query.where has sufficient capacity, prior to
 * adding a predicate. If capacity is insufficient then false is returned.
 *
 * String predicates are not owned by as_query.  If the string is allocated
 * on the heap, the caller is responsible for freeing the string after the query
 * has been executed.  as_query_destroy() will not free this string predicate.
 *
 * ~~~~~~~~~~{.c}
 * as_query_where_init(&query, 3);
 * as_query_where(&query, "bin1", as_string_equals("abc"));
 * as_query_where(&query, "bin1", as_integer_equals(123));
 * as_query_where(&query, "bin1", as_integer_range(0,123));
 * ~~~~~~~~~~
 *
 * @param query		The query add the predicate to.
 * @param bin		The name of the bin the predicate will apply to.
 * @param type		The type of predicate.
 * @param itype		The type of index.
 * @param dtype		The underlying data type that the index is based on.
 * @param ... 		The values for the predicate.
 * 
 * @return On success, true. Otherwise an error occurred.
 *
 * @relates as_query
 * @ingroup query_operations
 */
AS_EXTERN bool
as_query_where(
	as_query* query, const char * bin, as_predicate_type type, as_index_type itype,
	as_index_datatype dtype, ...
	);

/**
 * Add a predicate and context to the query.
 *
 * You have to ensure as_query.where has sufficient capacity, prior to
 * adding a predicate. If capacity is insufficient then false is returned.
 *
 * String predicates are not owned by as_query.  If the string is allocated
 * on the heap, the caller is responsible for freeing the string after the query
 * has been executed.  as_query_destroy() will not free this string predicate.
 *
 * ~~~~~~~~~~{.c}
 * as_cdt_ctx ctx;
 * as_cdt_ctx_init(&ctx, 1);
 * as_cdt_ctx_add_list_rank(&ctx, -1);
 * as_query_where_init(&query, 3);
 * as_query_where_with_ctx(&query, "bin1", &ctx, as_string_equals("abc"));
 * as_query_where_with_ctx(&query, "bin1", &ctx, as_integer_equals(123));
 * as_query_where_with_ctx(&query, "bin1", &ctx, as_integer_range(0,123));
 * ~~~~~~~~~~
 *
 * @param query		The query add the predicate to.
 * @param bin		The name of the bin the predicate will apply to.
 * @param ctx		The CDT context describing the path to locate the data to be indexed.
 * @param type		The type of predicate.
 * @param itype		The type of index.
 * @param dtype		The underlying data type that the index is based on.
 * @param ... 		The values for the predicate.
 *
 * @return On success, true. Otherwise an error occurred.
 *
 * @relates as_query
 * @ingroup query_operations
 */
AS_EXTERN bool
as_query_where_with_ctx(
	as_query* query, const char* bin, struct as_cdt_ctx* ctx, as_predicate_type type,
	as_index_type itype, as_index_datatype dtype, ...
	);

//---------------------------------
// Background Query Functions
//---------------------------------

/**
 * Apply a function to the results of the query.
 *
 * ~~~~~~~~~~{.c}
 * as_query_apply(&query, "my_module", "my_function", NULL);
 * ~~~~~~~~~~
 *
 * @param query			The query to apply the function to.
 * @param module		The module containing the function to invoke.
 * @param function		The function in the module to invoke.
 * @param arglist		The arguments to use when calling the function.
 *
 * @return On success, true. Otherwise an error occurred.
 *
 * @relates as_query
 * @ingroup query_operations
 */
AS_EXTERN bool
as_query_apply(as_query* query, const char* module, const char* function, const as_list* arglist);

//---------------------------------
// Paginate Functions
//---------------------------------

/**
 * Set if records should be read in pages in conjunction with max_records policy.
 * If true, the client will save the status of all partitions after the query completes.
 * The partition status can be used to resume the query if terminated early by
 * error, user callback, or max_records being reached. Use as_query_set_partitions()
 * or as_partition_filter_set_partitions() to resume a query.
 *
 * The partition status will be destroyed when as_query_destroy() is called.
 *
 * @relates as_query
 * @ingroup query_operations
 */
static inline void
as_query_set_paginate(as_query* query, bool paginate)
{
	query->paginate = paginate;
}

/**
 * Set completion status of all partitions from a previous query that ended early.
 * The query will resume from this point.
 *
 * @relates as_query
 * @ingroup query_operations
 */
static inline void
as_query_set_partitions(as_query* query, as_partitions_status* parts_all)
{
	query->parts_all = as_partitions_status_reserve(parts_all);
}

/**
 * If using query pagination, did the previous paginated query with this query instance
 * return all records?
 *
 * @relates as_query
 * @ingroup query_operations
 */
static inline bool
as_query_is_done(as_query* query)
{
	return query->parts_all && query->parts_all->done;
}

//---------------------------------
// Serialization Functions
//---------------------------------

/**
 * Serialize query definition to bytes.
 *
 * @relates as_query
 * @ingroup query_operations
 */
AS_EXTERN bool
as_query_to_bytes(const as_query* query, uint8_t** bytes, uint32_t* bytes_size);

/**
 * Deserialize bytes to query definition. Query definition is assumed to be on the stack.
 * as_query_destroy() should be called when done with the query definition.
 *
 * @returns true on success and false on failure.
 *
 * @relates as_query
 * @ingroup query_operations
 */
AS_EXTERN bool
as_query_from_bytes(as_query* query, const uint8_t* bytes, uint32_t bytes_size);

/**
 * Create query definition on the heap and deserialize bytes to that query definition.
 * as_query_destroy() should be called when done with the query definition.
 *
 * @returns query definition on success and NULL on failure.
 * 
 * @relates as_query
 * @ingroup query_operations
 */
AS_EXTERN as_query*
as_query_from_bytes_new(const uint8_t* bytes, uint32_t bytes_size);

/**
 * Compare query objects.
 * @private
 * @relates as_query
 * @ingroup query_operations
 */
AS_EXTERN bool
as_query_compare(as_query* q1, as_query* q2);

#ifdef __cplusplus
} // end extern "C"
#endif
