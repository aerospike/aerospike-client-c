#include <stdio.h>

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/as_exp.h>
#include <aerospike/as_exp_operations.h>
#include <aerospike/as_operations.h>
#include <aerospike/as_record.h>
#include <aerospike/as_record_iterator.h>

#define log(...)   fprintf(stderr, __VA_ARGS__)
#define NAMESPACE  "test"
#define SET        "products"


static void
example_dump_bin(const as_bin* p_bin)
{
	if (! p_bin) {
		log("  null as_bin object");
		return;
	}

	char* val_as_str = as_val_tostring(as_bin_get_value(p_bin));

	log("  %s : %s", as_bin_get_name(p_bin), val_as_str);

	free(val_as_str);
}

void
example_dump_record(const as_record* p_rec)
{
	if (! p_rec) {
		log("  null as_record object");
		return;
	}

	if (p_rec->key.valuep) {
		char* key_val_as_str = as_val_tostring(p_rec->key.valuep);

		log("  key: %s", key_val_as_str);

		free(key_val_as_str);
	}

	uint16_t num_bins = as_record_numbins(p_rec);

	log("  generation %u, ttl %u, %u bin%s", p_rec->gen, p_rec->ttl, num_bins,
			num_bins == 0 ? "s" : (num_bins == 1 ? ":" : "s:"));

	as_record_iterator it;
	as_record_iterator_init(&it, p_rec);

	while (as_record_iterator_has_next(&it)) {
		example_dump_bin(as_record_iterator_next(&it));
	}

	as_record_iterator_destroy(&it);
}


int
main(int argc, char* argv[])
{
	as_status status;
	as_error err;

	log("Entering qs001\n");

    as_key key;
	as_key_init_str(&key, NAMESPACE, SET, "catalog");

	as_config config;
	as_config_init(&config);
	as_config_add_host(&config, "127.0.0.1", 3000);

	aerospike as;
	aerospike_init(&as, &config);

	status = aerospike_connect(&as, &err);
	if (status != AEROSPIKE_OK) {
		log("qs001: Error %d: aerospike_connect() failed", status);
		goto fail_aerospike_connect;
	}

	// Product-level filter: featured == true

	as_exp_build(filter_on_featured,
		as_exp_cmp_eq(
			as_exp_map_get_by_key(
				NULL, AS_MAP_RETURN_VALUE, AS_EXP_TYPE_BOOL,
				as_exp_str("featured"),
				as_exp_loopvar_map(AS_EXP_LOOPVAR_VALUE)  // loop variable points to each product map
			),
			as_exp_bool(false)
		)
	);
	if (!filter_on_featured) {
		goto fail_filter_on_featured;
	}

	// Variant-level filter: quantity > 0

	as_exp_build(filter_on_variant_inventory,
		as_exp_cmp_gt(
			as_exp_map_get_by_key(
				NULL, AS_MAP_RETURN_VALUE, AS_EXP_TYPE_INT,
				as_exp_str("quantity"),
				as_exp_loopvar_map(AS_EXP_LOOPVAR_VALUE)  // loop variable points to each variant object
			),
			as_exp_int(0)
		)
	);
	if (!filter_on_variant_inventory) {
		goto fail_filter_on_variant_inventory;
	}

	// Operation

	as_cdt_ctx ctx;
	as_cdt_ctx_inita(&ctx, 4);
	as_cdt_ctx_add_all_children(&ctx);
	as_cdt_ctx_add_all_children_with_filter(&ctx, filter_on_featured);
	as_cdt_ctx_add_map_key(
			&ctx, (as_val*)as_string_new((char*)"variants", false));
	as_cdt_ctx_add_all_children_with_filter(&ctx, filter_on_variant_inventory);

	as_operations ops;
	as_operations_inita(&ops, 1);
	status = as_operations_select_by_path(&err, &ops, "inventory", &ctx, 0);
	if (status != AEROSPIKE_OK) {
		log("qs001: Error %d: as_operations_select_by_path() failed\n", status);
		goto fail_select_by_path;
	}

	as_record* rec = NULL;
	status = aerospike_key_operate(&as, &err, NULL, &key, &ops, &rec);
	if (status != AEROSPIKE_OK) {
		log("qs001: Error %d: aerospike_key_operate() failed\n", status);
		goto fail_key_operate;
	}

	as_map* map = as_record_get_map(rec, "inventory");
	example_dump_record(rec);

	// deliberate fall through to failure-aware cleanup code.

fail_key_operate:

	// no need to free map, since it is a pointer to within the record.

	if (rec != NULL) {
		as_record_destroy(rec);
	}

fail_select_by_path:

	as_operations_destroy(&ops);

fail_filter_on_variant_inventory:

	if (filter_on_variant_inventory != NULL) {
		as_exp_destroy(filter_on_variant_inventory);
	}

fail_filter_on_featured:

	if (filter_on_featured != NULL) {
		as_exp_destroy(filter_on_featured);
	}

	aerospike_close(&as, &err);
	aerospike_destroy(&as);

fail_aerospike_connect:

	log("Leaving qs001\n");
}


#if 0
{
	as_key rkey;
	as_key_init_int64(&rkey, NAMESPACE, SET, 215);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &rkey);
	assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	as_arraylist list0;
	as_arraylist_init(&list0, 10, 10);

	for (int i = 0; i < 20; i++) {
		as_arraylist* list1 = as_arraylist_new(10, 10);

		for (int j = 0; j < 10; j++) {
			as_arraylist_append_int64(list1, j + 10);
		}

		as_arraylist_append_list(&list0, (as_list*)list1);
	}

	as_record *rec = as_record_new(1);
	as_record_set_list(rec, BIN_NAME, (as_list*)&list0);
	status = aerospike_key_put(as, &err, NULL, &rkey, rec);
	assert_true(status == AEROSPIKE_OK);
	as_record_destroy(rec);
	rec = NULL;

	// Get and check.
	status = aerospike_key_get(as, &err, NULL, &rkey, &rec);
	assert_int_eq(status, AEROSPIKE_OK);

	as_record_destroy(rec);
	rec = NULL;

	as_exp_build(exp1,
		as_exp_bool(true));

	as_exp_build(exp2,
		as_exp_and(
			as_exp_cmp_ge(
				as_exp_loopvar_int(AS_EXP_LOOPVAR_VALUE),
				as_exp_int(14)),
			as_exp_cmp_lt(
				as_exp_loopvar_int(AS_EXP_LOOPVAR_VALUE),
				as_exp_int(16))));

	assert_not_null(exp1);
	assert_not_null(exp2);

	// negative test; expect an error when &ctx == NULL.

	as_operations ops;
	as_operations_inita(&ops, 1);

	assert_int_eq(as_operations_select_by_path(&err, &ops, BIN_NAME, NULL, 0), AEROSPIKE_ERR_PARAM);

	// negative test; expect an error when &ctx != NULL /\ ctx is empty.

	as_operations_inita(&ops, 1);

	as_cdt_ctx ctx;
	as_cdt_ctx_inita(&ctx, 2);

	assert_int_eq(as_operations_select_by_path(&err, &ops, BIN_NAME, &ctx, 0), AEROSPIKE_ERR_PARAM);

	// Back to positive testing

	as_operations_inita(&ops, 1);

	as_cdt_ctx_inita(&ctx, 2);
	as_cdt_ctx_add_all_children_with_filter(&ctx, exp1);
	as_cdt_ctx_add_all_children_with_filter(&ctx, exp2);

	assert_int_eq(as_operations_select_by_path(&err, &ops, BIN_NAME, &ctx, 0), AEROSPIKE_OK);

	rec = NULL;
	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);

	as_list* check0 = as_record_get_list(rec, BIN_NAME);
	assert_not_null(check0);
	assert_int_eq(as_list_size(check0), 20);
	for (uint32_t i = 0; i < as_list_size(check0); i++) {
		as_list* check1 = as_list_get_list(check0, i);
		assert_not_null(check1);
		assert_int_eq(as_list_size(check1), 2);
		assert_int_eq(as_list_get_int64(check1, 0), 14);
		assert_int_eq(as_list_get_int64(check1, 1), 15);
	}
	as_record_destroy(rec);
	rec = NULL;
	as_exp_destroy(exp1);
	as_exp_destroy(exp2);
	as_cdt_ctx_destroy(&ctx);
}
#endif


