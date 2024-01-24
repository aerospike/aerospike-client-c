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

#include <aerospike/as_bin.h>
#include <aerospike/as_key.h>
#include <aerospike/as_partition_filter.h>
#include <aerospike/as_udf.h>

#ifdef __cplusplus
extern "C" {
#endif

//---------------------------------
// Macros
//---------------------------------

/**
 * Default value for as_scan.no_bins
 */
#define AS_SCAN_NOBINS_DEFAULT false

/**
 * Default value for as_scan.concurrent
 */
#define AS_SCAN_CONCURRENT_DEFAULT false

/**
 * Default value for as_scan.deserialize_list_map
 */
#define AS_SCAN_DESERIALIZE_DEFAULT true

//---------------------------------
// Types
//---------------------------------

struct as_operations_s;

/**
 * The status of a particular background scan.
 */
typedef enum as_scan_status_e {

	/**
	 * The scan status is undefined.
	 * This is likely due to the status not being properly checked.
	 */
	AS_SCAN_STATUS_UNDEF,

	/**
	 * The scan is currently running.
	 */
	AS_SCAN_STATUS_INPROGRESS,

	/**
	 * The scan was aborted. Due to failure or the user.
	 */
	AS_SCAN_STATUS_ABORTED,

	/**
	 * The scan completed successfully.
	 */
	AS_SCAN_STATUS_COMPLETED,

} as_scan_status;

/**
 * Information about a particular background scan.
 */
typedef struct as_scan_info_s {

	/**
	 * Status of the scan.
	 */
	as_scan_status status;

	/**
	 * Progress estimate for the scan, as percentage.
	 */
	uint32_t progress_pct;

	/**
	 * How many records have been scanned.
	 */
	uint32_t records_scanned;

} as_scan_info;

/**
 * Sequence of bins which should be selected during a scan.
 *
 * Entries can either be initialized on the stack or on the heap.
 *
 * Initialization should be performed via a query object, using:
 * - as_scan_select_init()
 * - as_scan_select_inita()
 */
typedef struct as_scan_bins_s {

	/**
	 * Sequence of entries
	 */
	as_bin_name* entries;

	/**
	 * Number of entries allocated
	 */
	uint16_t capacity;

	/**
	 * Number of entries used
	 */
	uint16_t size;

	/**
	 * @private
	 * If true, then as_scan_destroy() will free this instance.
	 */
	bool _free;

} as_scan_bins;

/**
 * In order to execute a scan using the Scan API, an as_scan object
 * must be initialized and populated.
 *
 * ## Initialization
 * 
 * Before using an as_scan, it must be initialized via either: 
 * - as_scan_init()
 * - as_scan_new()
 * 
 * as_scan_init() should be used on a stack allocated as_scan. It will
 * initialize the as_scan with the given namespace and set. On success,
 * it will return a pointer to the initialized as_scan. Otherwise, NULL 
 * is returned.
 *
 * ~~~~~~~~~~{.c}
 * as_scan scan;
 * as_scan_init(&scan, "namespace", "set");
 * ~~~~~~~~~~
 *
 * as_scan_new() should be used to allocate and initialize a heap allocated
 * as_scan. It will allocate the as_scan, then initialized it with the 
 * given namespace and set. On success, it will return a pointer to the 
 * initialized as_scan. Otherwise, NULL is returned.
 *
 * ~~~~~~~~~~{.c}
 * as_scan* scan = as_scan_new("namespace", "set");
 * ~~~~~~~~~~
 *
 * ## Destruction
 *
 * When you are finished with the as_scan, you can destroy it and associated
 * resources:
 *
 * ~~~~~~~~~~{.c}
 * as_scan_destroy(scan);
 * ~~~~~~~~~~
 *
 * ## Usage
 *
 * An initialized as_scan can be populated with additional fields.
 *
 * ### Selecting Bins
 *
 * as_scan_select() is used to specify the bins to be selected by the scan.
 * If a scan specifies bins to be selected, then only those bins will be 
 * returned. If no bins are selected, then all bins will be returned.
 * 
 * ~~~~~~~~~~{.c}
 * as_scan_select(query, "bin1");
 * as_scan_select(query, "bin2");
 * ~~~~~~~~~~
 *
 * Before adding bins to select, the select structure must be initialized via
 * either:
 * - as_scan_select_inita() - Initializes the structure on the stack.
 * - as_scan_select_init() - Initializes the structure on the heap.
 *
 * Both functions are given the number of bins to be selected.
 *
 * A complete example using as_scan_select_inita()
 *
 * ~~~~~~~~~~{.c}
 * as_scan_select_inita(query, 2);
 * as_scan_select(query, "bin1");
 * as_scan_select(query, "bin2");
 * ~~~~~~~~~~
 *
 * ### Returning only meta data
 *
 * A scan can return only record meta data, and exclude bins.
 *
 * ~~~~~~~~~~{.c}
 * as_scan_set_nobins(scan, true);
 * ~~~~~~~~~~
 *
 * ### Scan nodes in parallel
 *
 * A scan can be made to scan all the nodes in parallel
 * 
 * ~~~~~~~~~~{.c}
 * as_scan_set_concurrent(scan, true);
 * ~~~~~~~~~~
 *
 * ### Scan a Percentage of Records
 *
 * A scan can define the percentage of record in the cluster to be scaned.
 *
 * ~~~~~~~~~~{.c}
 * as_scan_set_percent(scan, 100);
 * ~~~~~~~~~~
 *
 * ### Scan a Priority
 *
 * To set the priority of the scan, the set as_scan.priority.
 *
 * The priority of a scan can be defined as either:
 * - `AS_SCAN_PRIORITY_AUTO`
 * - `AS_SCAN_PRIORITY_LOW`
 * - `AS_SCAN_PRIORITY_MEDIUM`
 * - `AS_SCAN_PRIORITY_HIGH`
 *
 * ~~~~~~~~~~{.c}
 * as_scan_set_priority(scan, AS_SCAN_PRIORITY_LOW);
 * ~~~~~~~~~~
 *
 * ### Applying a UDF to each Record Scanned
 *
 * A UDF can be applied to each record scanned.
 *
 * To define the UDF for the scan, use as_scan_apply_each().
 *
 * ~~~~~~~~~~{.c}
 * as_scan_apply_each(scan, "udf_module", "udf_function", arglist);
 * ~~~~~~~~~~
 *
 * @ingroup scan_operations
 */
typedef struct as_scan_s {

	/**
	 * @memberof as_scan
	 * Namespace to be scanned.
	 *
	 * Should be initialized via either:
	 * - as_scan_init() - To initialize a stack allocated scan.
	 * - as_scan_new() - To heap allocate and initialize a scan.
	 */
	as_namespace ns;

	/**
	 * Set to be scanned.
	 *
	 * Should be initialized via either:
	 * - as_scan_init() - To initialize a stack allocated scan.
	 * - as_scan_new() - To heap allocate and initialize a scan.
	 */
	as_set set;

	/**
	 * Name of bins to select.
	 * 
	 * Use either of the following function to initialize:
	 * - as_scan_select_init() - To initialize on the heap.
	 * - as_scan_select_inita() - To initialize on the stack.
	 *
	 * Use as_scan_select() to populate.
	 */
	as_scan_bins select;

	/**
	 * UDF to apply to results of the background scan.
	 *
	 * Should be set via `as_scan_apply_each()`.
	 */
	as_udf_call apply_each;

	/**
	 * Perform write operations on a background scan.
	 * If ops is set, ops will be destroyed when as_scan_destroy() is called.
	 */
	struct as_operations_s* ops;

	/**
	 * Status of all partitions.
	 */
	as_partitions_status* parts_all;

	/**
	 * The time-to-live (expiration) of the record in seconds. Note that ttl
	 * is only used on background scan writes.
	 *
	 * There are also special values that can be set in the record ttl:
	 * <ul>
	 * <li>AS_RECORD_DEFAULT_TTL: Use the server default ttl from the namespace.</li>
	 * <li>AS_RECORD_NO_EXPIRE_TTL: Do not expire the record.</li>
	 * <li>AS_RECORD_NO_CHANGE_TTL: Keep the existing record ttl when the record is updated.</li>
	 * <li>AS_RECORD_CLIENT_DEFAULT_TTL: Use the default client ttl in as_policy_scan.</li>
	 * </ul>
	 */
	uint32_t ttl;

	/**
	 * Set to true if as_policy_scan.max_records is set and you need to scan data in pages.
	 *
	 * Default: false
	 */
	bool paginate;

	/**
	 * Set to true if the scan should return only the metadata of the record.
	 *
	 * Default value is AS_SCAN_NOBINS_DEFAULT.
	 */
	bool no_bins;

	/**
	 * Set to true if the scan should scan all the nodes in parallel
	 *
	 * Default value is AS_SCAN_CONCURRENT_DEFAULT.
	 */
	bool concurrent;

	/**
	 * Set to true if the scan should deserialize list and map raw bytes.
	 * Set to false for backup programs that just need access to raw bytes.
	 *
	 * Default value is AS_SCAN_DESERIALIZE_DEFAULT.
	 */
	bool deserialize_list_map;
	
	/**
	 * @private
	 * If true, then as_scan_destroy() will free this instance.
	 */
	bool _free;

} as_scan;

//---------------------------------
// Instance Functions
//---------------------------------

/**
 * Initializes a scan.
 * 
 * ~~~~~~~~~~{.c}
 * as_scan scan;
 * as_scan_init(&scan, "test", "demo");
 * ~~~~~~~~~~
 *
 * When you no longer require the scan, you should release the scan and 
 * related resources via `as_scan_destroy()`.
 *
 * @param scan		The scan to initialize.
 * @param ns 		The namespace to scan.
 * @param set 		The set to scan.
 *
 * @returns On succes, the initialized scan. Otherwise NULL.
 *
 * @relates as_scan
 * @ingroup scan_operations
 */
AS_EXTERN as_scan*
as_scan_init(as_scan* scan, const char* ns, const char* set);

/**
 * Create and initializes a new scan on the heap.
 * 
 * ~~~~~~~~~~{.c}
 * as_scan* scan = as_scan_new("test","demo");
 * ~~~~~~~~~~
 *
 * When you no longer require the scan, you should release the scan and 
 * related resources via `as_scan_destroy()`.
 *
 * @param ns 		The namespace to scan.
 * @param set 		The set to scan.
 *
 * @returns On success, a new scan. Otherwise NULL.
 *
 * @relates as_scan
 * @ingroup scan_operations
 */
AS_EXTERN as_scan*
as_scan_new(const char* ns, const char* set);

/**
 * Releases all resources allocated to the scan.
 * 
 * ~~~~~~~~~~{.c}
 * as_scan_destroy(scan);
 * ~~~~~~~~~~
 *
 * @relates as_scan
 * @ingroup scan_operations
 */
AS_EXTERN void
as_scan_destroy(as_scan* scan);

//---------------------------------
// Select Functions
//---------------------------------

/** 
 * Initializes `as_scan.select` with a capacity of `n` using `alloca`
 *
 * For heap allocation, use `as_scan_select_init()`.
 *
 * ~~~~~~~~~~{.c}
 * as_scan_select_inita(&scan, 2);
 * as_scan_select(&scan, "bin1");
 * as_scan_select(&scan, "bin2");
 * ~~~~~~~~~~
 * 
 * @param __scan	The scan to initialize.
 * @param __n		The number of bins to allocate.
 *
 * @relates as_scan
 * @ingroup scan_operations
 */
#define as_scan_select_inita(__scan, __n) \
	do {\
		if ((__scan)->select.entries == NULL) {\
			(__scan)->select.entries = (as_bin_name*) alloca(sizeof(as_bin_name) * (__n));\
			if ((__scan)->select.entries) {\
				(__scan)->select.capacity = (__n);\
				(__scan)->select.size = 0;\
				(__scan)->select._free = false;\
			}\
	 	}\
	} while(0)

/** 
 * Initializes `as_scan.select` with a capacity of `n` using `malloc()`.
 * 
 * For stack allocation, use `as_scan_select_inita()`.
 *
 * ~~~~~~~~~~{.c}
 * as_scan_select_init(&scan, 2);
 * as_scan_select(&scan, "bin1");
 * as_scan_select(&scan, "bin2");
 * ~~~~~~~~~~
 *
 * @param scan	The scan to initialize.
 * @param n		The number of bins to allocate.
 *
 * @return On success, true. Otherwise an error occurred.
 *
 * @relates as_scan
 * @ingroup scan_operations
 */
AS_EXTERN bool
as_scan_select_init(as_scan* scan, uint16_t n);

/**
 * Select bins to be projected from matching records.
 *
 * You have to ensure as_scan.select has sufficient capacity, prior to 
 * adding a bin. If capacity is insufficient then false is returned.
 *
 * ~~~~~~~~~~{.c}
 * as_scan_select_init(&scan, 2);
 * as_scan_select(&scan, "bin1");
 * as_scan_select(&scan, "bin2");
 * ~~~~~~~~~~
 *
 * @param scan 	The scan to modify.
 * @param bin 	The name of the bin to select.
 *
 * @return On success, true. Otherwise an error occurred.
 *
 * @relates as_scan
 * @ingroup scan_operations
 */
AS_EXTERN bool
as_scan_select(as_scan* scan, const char * bin);

//---------------------------------
// Modifier Functions
//---------------------------------

/**
 * Do not return bins. This will only return the metadata for the records.
 * 
 * ~~~~~~~~~~{.c}
 * as_scan_set_nobins(&q, true);
 * ~~~~~~~~~~
 *
 * @param scan 			The scan to set the priority on.
 * @param nobins		If true, then do not return bins.
 *
 * @return On success, true. Otherwise an error occurred.
 *
 * @relates as_scan
 * @ingroup scan_operations
 */
AS_EXTERN bool
as_scan_set_nobins(as_scan* scan, bool nobins);

/**
 * Scan all the nodes in prallel
 * 
 * ~~~~~~~~~~{.c}
 * as_scan_set_concurrent(&q, true);
 * ~~~~~~~~~~
 *
 * @param scan 			The scan to set the concurrency on.
 * @param concurrent	If true, scan all the nodes in parallel
 *
 * @return On success, true. Otherwise an error occurred.
 *
 * @relates as_scan
 * @ingroup scan_operations
 */
AS_EXTERN bool
as_scan_set_concurrent(as_scan* scan, bool concurrent);

//---------------------------------
// Background Scan Functions
//---------------------------------

/**
 * Apply a UDF to each record scanned on the server.
 * 
 * ~~~~~~~~~~{.c}
 * as_arraylist arglist;
 * as_arraylist_init(&arglist, 2, 0);
 * as_arraylist_append_int64(&arglist, 1);
 * as_arraylist_append_int64(&arglist, 2);
 * 
 * as_scan_apply_each(&q, "module", "func", (as_list *) &arglist);
 *
 * as_arraylist_destroy(&arglist);
 * ~~~~~~~~~~
 *
 * @param scan 			The scan to apply the UDF to.
 * @param module 		The module containing the function to execute.
 * @param function 		The function to execute.
 * @param arglist 		The arguments for the function.
 *
 * @return On success, true. Otherwise an error occurred.
 *
 * @relates as_scan
 * @ingroup scan_operations
 */
AS_EXTERN bool
as_scan_apply_each(as_scan* scan, const char* module, const char* function, as_list* arglist);

//---------------------------------
// Paginate Functions
//---------------------------------

/**
 * Set to true if as_policy_scan.max_records is set and you need to scan data in pages.
 * 
 * @relates as_scan
 * @ingroup scan_operations
 */
static inline void
as_scan_set_paginate(as_scan* scan, bool paginate)
{
	scan->paginate = paginate;
}

/**
 * Set completion status of all partitions from a previous scan that ended early.
 * The scan will resume from this point.
 *
 * @relates as_scan
 * @ingroup scan_operations
 */
static inline void
as_scan_set_partitions(as_scan* scan, as_partitions_status* parts_all)
{
	scan->parts_all = as_partitions_status_reserve(parts_all);
}

/**
 * If using scan pagination, did previous paginated scan with this scan instance 
 * return all records?
 *
 * @relates as_scan
 * @ingroup scan_operations
 */
static inline bool
as_scan_is_done(as_scan* scan)
{
	return scan->parts_all && scan->parts_all->done;
}

//---------------------------------
// Serialization Functions
//---------------------------------

/**
 * Serialize scan definition to bytes.
 *
 * @returns true on success and false on failure.
 *
 * @relates as_scan
 * @ingroup scan_operations
 */
AS_EXTERN bool
as_scan_to_bytes(const as_scan* scan, uint8_t** bytes, uint32_t* bytes_size);

/**
 * Deserialize bytes to scan definition. Scan definition is assumed to be on the stack.
 * as_scan_destroy() should be called when done with the scan definition.
 *
 * @returns true on success and false on failure.
 *
 * @relates as_scan
 * @ingroup scan_operations
 */
AS_EXTERN bool
as_scan_from_bytes(as_scan* scan, const uint8_t* bytes, uint32_t bytes_size);

/**
 * Create scan definition on the heap and deserialize bytes to that scan definition.
 * as_scan_destroy() should be called when done with the scan definition.
 *
 * @returns scan definition on success and NULL on failure.
 * 
 * @relates as_scan
 * @ingroup scan_operations
 */
AS_EXTERN as_scan*
as_scan_from_bytes_new(const uint8_t* bytes, uint32_t bytes_size);

/**
 * Compare scan objects.
 * @private
 * @relates as_scan
 * @ingroup scan_operations
 */
AS_EXTERN bool
as_scan_compare(as_scan* s1, as_scan* s2);

#ifdef __cplusplus
} // end extern "C"
#endif
