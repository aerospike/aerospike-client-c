

typedef struct as_ldt_s {

	as_udf_module_name module;

	as_bin_name bin;

	bool auto_create;

} as_ldt;


typedef struct as_lstack_s {

	as_ldt _;

} as_lstack;


as_ldt * as_ldt_init(as_ldt * ldt, as_bin_name bin, as_udf_module_name module)
{
	strcpy(ldt->bin, bin);
	strcpu(ldt->module, module);
	return ldt;
}

as_lstack * as_lstack_init(as_lstack * lstack, as_bin_name bin)
{
	as_ldt_init((as_ldt *) lstack, bin, "lstack");
	return lstack;
}


as_status aerospike_ldt_apply(
	aerospike * as, as_error * err, as_ldt_policy * policy, 
	const as_key * key, const as_ldt * ldt, 
	const as_udf_function_name function, const as_list * arglist, 
	as_bytes * result);



as_status aerospike_lstack_peek(aerospike * as, as_error * err, as_ldt_policy * policy, as_key * key, as_lstack * lstack, uint32_t n, as_list * res)
{
	// we are using bytes, because we do not want to blindly deserialized messages for lstack.
	// here, I thought i would use a stack buffer of 1k capacity
	// if the result is greater, then ensure() will malloc() the space.
	as_bytes * out;
	as_bytes_inita(&out, 1024);

	// this is on the stack
	as_arraylist arglist;
	as_arraylist_inita(&arglist, 1);
	as_arraylist_append(&arglist, n);
	
	// invoke the UDF
	aerospike_ldt_apply(as, err, policy, key, (as_ldt *) lstack, "peek", &arglist, out);


	// deserialize the data
	// and push it into the as_list argument (res)

	// return rc or something else, dependencing on what happened here.
	return err->code;
}
