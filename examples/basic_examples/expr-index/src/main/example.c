/*******************************************************************************
 * Copyright 2008-2025 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 ******************************************************************************/


//==========================================================
// Includes
//

#include <stddef.h>
#include <stdlib.h>
#include <limits.h>

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/aerospike_query.h>
#include <aerospike/as_exp.h>
#include <aerospike/as_error.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>
#include <aerospike/as_query.h>

#include <aerospike/as_arraylist.h>

#include "example_utils.h"


//==========================================================
// Forward Declarations
//

static int do_create_expression(void);
static int do_insert_data(void);
static int do_remove_data(void);
static int do_query_by_age(int, bool);

static void insert(aerospike*, int, const char*, int, const char*);
static void remove_rec(aerospike*, int);
static bool query_cb(const as_val*, void*);
static bool print_bin_cb(const char*, const as_val*, void*);

//==========================================================
// Expression Index Examples (supporting online tutorial docs)
//

int
main(int argc, char* argv[])
{
	// Parse command line arguments.
	if (! example_get_opts(argc, argv, EXAMPLE_BASIC_OPTS_SUBCMD)) {
		exit(-1);
	}

	if (! strcmp(g_subcommand, "create-expression")) {
		return do_create_expression();
	}

	if (! strcmp(g_subcommand, "insert-data")) {
		return do_insert_data();
	}

	if (! strcmp(g_subcommand, "remove-data")) {
		return do_remove_data();
	}

	if (! strcmp(g_subcommand, "query-age-5")) {
		return do_query_by_age(5, true);
	}

	if (! strcmp(g_subcommand, "query-age-25")) {
		return do_query_by_age(25, true);
	}

	if (! strcmp(g_subcommand, "query-no-index")) {
		return do_query_by_age(0, false);
	}

	LOG("Unrecognized sub-command: %s", g_subcommand);
	LOG("Available sub-commands include:");
	LOG("  create-expression -- creates expression and prints base-64 code");
	LOG("  insert-data    -- inserts example data");
	LOG("  remove-data    -- removes example data");
	LOG("  query-age-5    -- prints records with age >= 5, subject to index");
	LOG("  query-age-25   -- prints records with age >= 25, subject to index");
	LOG("  query-no-index -- prints all records, without index");

	return 1;
}


//==========================================================
// Helpers
//

static int
do_create_expression(void)
{
	as_arraylist countries_of_interest;
	as_arraylist_init(&countries_of_interest, 3, 3);
	as_arraylist_append_str(&countries_of_interest, "Australia");
	as_arraylist_append_str(&countries_of_interest, "Canada");
	as_arraylist_append_str(&countries_of_interest, "Botswana");

	as_exp_build(filter_exp,
		as_exp_cond(
			as_exp_and(
				as_exp_cmp_ge(
					as_exp_bin_int("age"),
					as_exp_int(18)),
				as_exp_list_get_by_value_list(
					NULL, AS_LIST_RETURN_EXISTS,
					as_exp_bin_list("country"),
					as_exp_val(&countries_of_interest))),

			// If true, return the value in the age bin
			as_exp_bin_int("age"),

			// return unknown to exclude value from index
			as_exp_unknown()
		));

	LOG("Expression base64 = %s", as_exp_to_base64(filter_exp));
	return 0;
}

// lHuTEJMEk1ECo2FnZRKVfwEAkxYNk1EDp2NvdW50cnmSfpOqA0F1c3RyYWxpYacDQ2FuYWRhqQNCb3Rzd2FuYZNRAqNhZ2WRAA Java
// lHuTEJMEk1ECo2FnZRKVfwEAkxYNk1EDp2NvdW50cnmSfpOqA0F1c3RyYWxpYacDQ2FuYWRhqQNCb3Rzd2FuYZNRAqNhZ2WRAA Python
// lHuTEJMEk1ECo2FnZRKVfwEAkxYNk1EDp2NvdW50cnmSfpOqA0F1c3RyYWxpYacDQ2FuYWRhqQNCb3Rzd2FuYZNRAqNhZ2WRAA C#
// lHuTEJMEk1ECo2FnZRKVfwEAkxcNk1EEp2NvdW50cnmSfpOqA0F1c3RyYWxpYacDQ2FuYWRhqQNCb3Rzd2FuYZNRAqNhZ2WRAA C
//                                ^-- ONE character difference.  TODO: Is this significant?

static int
do_insert_data(void)
{
	aerospike as;
	example_connect_to_aerospike(&as);

	insert(&as, 1, "Tim", 312, "Australia");
	insert(&as, 2, "Bob", 47, "Canada");
	insert(&as, 3, "Jo", 15, "USA"); // not indexed
	insert(&as, 4, "Steven", 23, "Botswana");
	insert(&as, 5, "Susan", 32, "Canada");
	insert(&as, 6, "Jess", 17, "Botswana"); // not indexed
	insert(&as, 7, "Sam", 18, "USA"); // not indexed
	insert(&as, 8, "Alex", 47, "Canada");
	insert(&as, 9, "Pam", 56, "Australia");
	insert(&as, 10, "Vivek", 12, "India"); // not indexed
	insert(&as, 11, "Kiril", 22, "Sweden"); // not indexed
	insert(&as, 12, "Bill", 23, "UK"); // not indexed

	LOG("Records inserted OK");

	example_cleanup(&as);
	return 0;
}

static void
insert(aerospike* as, int key, const char* name, int age, const char* country)
{
	as_error err;
	as_error_init(&err);

	as_key int_key;
	as_key_init_int64(&int_key, g_namespace, g_set, key);

	as_record rec;
	as_record_inita(&rec, 3);
	as_record_set_str(&rec, "name", name);
	as_record_set_int64(&rec, "age", age);
	as_record_set_str(&rec, "country", country);

	LOG("Attempting to insert record "
	    "(key=%d, name=\"%s\", age=%d, country=\"%s\")",
		key, name, age, country);
	if (aerospike_key_put(as, &err, NULL, &int_key, &rec) != AEROSPIKE_OK) {
		LOG("aerospike_key_put() returned %d - %s", err.code, err.message);
		example_cleanup(as);
		exit(-1);
	}

	as_record_destroy(&rec);
}

static int
do_query_by_age(int age, bool use_index)
{
	aerospike as;
	example_connect_to_aerospike(&as);

	as_query query;
	as_query_init(&query, g_namespace, g_set);

	if (use_index) {
		as_query_where_init(&query, 1);
		// Remember that the result of the cond-expression returns the age field.
		// The predicate in the following function tests against this field.
		as_query_where_with_index_name(&query, "cust_index",
		                               as_integer_range(age, INT_MAX));
	}

	as_error err;
	if (aerospike_query_foreach(&as, &err, NULL, &query, &query_cb, NULL) !=
			AEROSPIKE_OK) {
		LOG("aerospike_query_foreach() returned %d - %s", err.code, err.message);
		example_cleanup(&as);
		return -1;
	}

	example_cleanup(&as);
	return 0;
}

static bool
query_cb(const as_val* val, void* udata) {
	if (! val) {
		// Query is complete.
		return false;
	}

	as_record* rec = as_record_fromval(val);

	as_key* key = &rec->key;

	if (key->valuep != NULL) {
		char* key_value_str = as_val_tostring(key->valuep);
		printf("key=(%s), ", key_value_str);
		free(key_value_str);
	}
	else {
		printf("key=(unknown), ");
	}

	as_record_foreach(rec, print_bin_cb, NULL);
	printf("\n");

	return true;
}

static bool
print_bin_cb(const char* name, const as_val* val, void* udata) {
	printf("%s=", name);

	uint64_t bin_type = as_val_type(val);
	switch (bin_type) {
	case AS_STRING:
		printf("\"%s\", ", as_string_get(as_string_fromval(val)));
		return true;

	case AS_INTEGER:
		printf("%lld, ", as_integer_get(as_integer_fromval(val)));
		return true;

	default:
		LOG("print_bin_cb: unknown type %llu for bin named %s", bin_type, name);
		return false;
	}
}


static int
do_remove_data(void)
{
	aerospike as;
	example_connect_to_aerospike(&as);

	for (int i = 1; i < 13; i++) {
		remove_rec(&as, i);
	}

	LOG("Records removed OK");

	example_cleanup(&as);
	return 0;
}

static void
remove_rec(aerospike* as, int key) {
	as_error err;
	as_error_init(&err);

	as_key int_key;
	as_key_init_int64(&int_key, g_namespace, g_set, key);

	LOG("Attempting to remove record (key=%d, ...)", key);
	if (aerospike_key_remove(as, &err, NULL, &int_key) != AEROSPIKE_OK) {
		LOG("aerospike_key_remove() returned %d - %s", err.code, err.message);
		example_cleanup(as);
		exit(-1);
	}
}
