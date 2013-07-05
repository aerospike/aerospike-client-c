
/**
 *	Lookup a record by key, then apply the UDF.
 *
 *	~~~~~~~~~~{.c}
 *		as_key key;
 *		as_key_init(&key, "ns", "set", "key");
 *
 *		as_arraylist args;
 *		as_arraylist_inita(&args, 2);
 *		as_arraylist_append_int64(&args, 1);
 *		as_arraylist_append_int64(&args, 2);
 *		
 *		as_val * res = NULL;
 *		
 *		if ( aerospike_key_apply(&as, &err, NULL, &key, "math", "add", &args, &res) != AEROSPIKE_OK ) {
 *			fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *		}
 *		else {
 *			as_val_destroy(res);
 *		}
 *		
 *		as_arraylist_destroy(&args);
 *	~~~~~~~~~~
 *
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param key			The key of the record.
 *	@param module		The module containing the function to execute.
 *	@param function 	The function to execute.
 *	@param arglist 		The arguments for the function.
 *	@param result 		The return value from the function.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 */
as_status aerospike_ldt_apply(
	aerospike * as, as_error * err, const as_policy_read * policy, 
	const as_key * key, const as_ldt * ldt, 
	const char * function, as_list * arglist, 
	as_val ** result
	);



as_ldt ldt;
as_ldt_init(&ldt, "lstack", "foo");

as_lstack lstack;
as_lstack_init(&lstack, "foo");

aerospike_ldt_apply(&as, &err, NULL, &key, &ldt)

as_arraylist list;
as_arraylist_inita(&list, 10);

if ( aerospike_lstack_peek(&as, &err, NULL, &key, &lstack, 10, (as_list *) &list) != AEROSPIKE_OK ) {
	fprintf(stderr, "%d - %s\n", err.code, err.message);
}
else {
	for(int i=0 l<10; i++) {
		int64_t item = as_integer_getorelse(as_arraylist_get_integer(&list, i), -1);
		if ( item == -1 ) {
			fprintf(stderr, "? - oops @ %d", i);
		}
		else {
			fprintf(stdout, "I got me a %ld", item);
		}
	}
}
