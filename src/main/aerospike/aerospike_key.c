/*
 * Copyright 2008-2015 Aerospike, Inc.
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
#include <aerospike/as_bin.h>
#include <aerospike/as_buffer.h>
#include <aerospike/as_command.h>
#include <aerospike/as_error.h>
#include <aerospike/as_key.h>
#include <aerospike/as_list.h>
#include <aerospike/as_log.h>
#include <aerospike/as_msgpack.h>
#include <aerospike/as_operations.h>
#include <aerospike/as_policy.h>
#include <aerospike/as_record.h>
#include <aerospike/as_serializer.h>
#include <aerospike/as_status.h>
#include <citrusleaf/cf_clock.h>
#include <citrusleaf/cf_random.h>

#include "as_stap.h"

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

static inline void
as_command_node_init(as_command_node* cn, const char* ns,
	const uint8_t* digest, as_policy_replica replica, bool write)
{
	cn->node = 0;
	cn->ns = ns;
	cn->digest = digest;
	cn->replica = replica;
	cn->write = write;
}

/**
 *	Look up a record by key, then return all bins.
 *	
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param key			The key of the record.
 *	@param rec 			The record to be populated with the data from request.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 */
as_status aerospike_key_get(
	aerospike * as, as_error * err, const as_policy_read * policy, 
	const as_key * key, as_record ** rec)
{
	as_error_reset(err);
	
	if (! policy) {
		policy = &as->config.policies.read;
	}

	int status = as_key_set_digest(err, (as_key*)key);
	
	if (status != AEROSPIKE_OK) {
		return status;
	}
	
	uint16_t n_fields;
	size_t size = as_command_key_size(policy->key, key, &n_fields);
		
	uint8_t* cmd = as_command_init(size);
	uint8_t* p = as_command_write_header_read(cmd, AS_MSG_INFO1_READ | AS_MSG_INFO1_GET_ALL, policy->consistency_level, policy->timeout, n_fields, 0);
	p = as_command_write_key(p, policy->key, key);
	size = as_command_write_end(cmd, p);
	
	as_command_node cn;
	as_command_node_init(&cn, key->ns, key->digest.value, policy->replica, false);
	
	as_command_parse_result_data data;
	data.record = rec;
	data.deserialize = policy->deserialize;
	
	status = as_command_execute(as->cluster, err, &cn, cmd, size, policy->timeout, policy->retry, as_command_parse_result, &data);
	
	as_command_free(cmd, size);
	return status;
}

/**
 *	Lookup a record by key, then return specified bins.
 *	
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param key			The key of the record.
 *	@param bins			The bins to select. A NULL terminated array of NULL terminated strings.
 *	@param rec 			The record to be populated with the data from request.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 */
as_status aerospike_key_select(
	aerospike * as, as_error * err, const as_policy_read * policy, 
	const as_key * key, const char * bins[], as_record ** rec)
{
	as_error_reset(err);
	
	if (! policy) {
		policy = &as->config.policies.read;
	}
	
	int status = as_key_set_digest(err, (as_key*)key);
	
	if (status != AEROSPIKE_OK) {
		return status;
	}
	
	uint16_t n_fields;
	size_t size = as_command_key_size(policy->key, key, &n_fields);
	int nvalues = 0;
	
	for (nvalues = 0; bins[nvalues] != NULL && bins[nvalues][0] != '\0'; nvalues++) {
		status = as_command_bin_name_size(err, bins[nvalues], &size);
		
		if (status != AEROSPIKE_OK) {
			return status;
		}
	}
	
	uint8_t* cmd = as_command_init(size);
	uint8_t* p = as_command_write_header_read(cmd, AS_MSG_INFO1_READ, policy->consistency_level, policy->timeout, n_fields, nvalues);
	p = as_command_write_key(p, policy->key, key);
	
	for (int i = 0; i < nvalues; i++) {
		p = as_command_write_bin_name(p, bins[i]);
	}
	size = as_command_write_end(cmd, p);
	
	as_command_node cn;
	as_command_node_init(&cn, key->ns, key->digest.value, policy->replica, false);
	
	as_command_parse_result_data data;
	data.record = rec;
	data.deserialize = policy->deserialize;

	status = as_command_execute(as->cluster, err, &cn, cmd, size, policy->timeout, policy->retry, as_command_parse_result, &data);
	
	as_command_free(cmd, size);
	return status;
}

/**
 *	Check if a record exists in the cluster via its key.
 *	
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param key			The key of the record.
 *	@param record       The record to populated with metadata if record exists, otherwise NULL
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 */
as_status aerospike_key_exists(
	aerospike * as, as_error * err, const as_policy_read * policy, 
	const as_key * key, as_record ** rec)
{
	as_error_reset(err);
	
	if (! policy) {
		policy = &as->config.policies.read;
	}
	
	as_status status = as_key_set_digest(err, (as_key*)key);
	
	if (status != AEROSPIKE_OK) {
		return status;
	}

	uint16_t n_fields;
	size_t size = as_command_key_size(policy->key, key, &n_fields);
	
	uint8_t* cmd = as_command_init(size);
	uint8_t* p = as_command_write_header_read(cmd, AS_MSG_INFO1_READ | AS_MSG_INFO1_GET_NOBINDATA, policy->consistency_level, policy->timeout, n_fields, 0);
	p = as_command_write_key(p, policy->key, key);
	size = as_command_write_end(cmd, p);
	
	as_command_node cn;
	as_command_node_init(&cn, key->ns, key->digest.value, policy->replica, false);
	
	as_proto_msg msg;
	status = as_command_execute(as->cluster, err, &cn, cmd, size, policy->timeout, policy->retry, as_command_parse_header, &msg);
	
	as_command_free(cmd, size);

	if (rec) {
		if (status == AEROSPIKE_OK) {
			as_record* r = *rec;
			
			if (r == 0) {
				r = as_record_new(0);
				*rec = r;
			}
			r->gen = (uint16_t)msg.m.generation;
			r->ttl = cf_server_void_time_to_ttl(msg.m.record_ttl);
		}
		else {
			*rec = 0;
		}
	}
	return status;
}

/**
 *	Store a record in the cluster.  Note that the TTL (time to live) value
 *	is specified inside of the rec (as_record) object.
 *	
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param key			The key of the record.
 *	@param rec 			The record containing the data to be written.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 */
as_status aerospike_key_put(
	aerospike * as, as_error * err, const as_policy_write * policy, 
	const as_key * key, as_record * rec) 
{
	as_error_reset(err);
	
	if (! policy) {
		policy = &as->config.policies.write;
	}
	
	as_status status = as_key_set_digest(err, (as_key*)key);
	
	if (status != AEROSPIKE_OK) {
		return status;
	}
	
	uint16_t n_fields;
	size_t size = as_command_key_size(policy->key, key, &n_fields);
	
	as_bin* bins = rec->bins.entries;
	uint32_t n_bins = rec->bins.size;
	as_buffer* buffers = (as_buffer*)alloca(sizeof(as_buffer) * n_bins);
	memset(buffers, 0, sizeof(as_buffer) * n_bins);

	for (uint32_t i = 0; i < n_bins; i++) {
		size += as_command_bin_size(&bins[i], &buffers[i]);
	}
	
#if defined(USE_SYSTEMTAP)
	size += as_command_field_size(8);
	n_fields++;
#endif

	uint8_t* cmd = as_command_init(size);
	uint8_t* p = as_command_write_header(cmd, 0, AS_MSG_INFO2_WRITE,
					policy->commit_level, 0, policy->exists, policy->gen,
					rec->gen, rec->ttl, policy->timeout, n_fields, n_bins);
		
	p = as_command_write_key(p, policy->key, key);

#if defined(USE_SYSTEMTAP)
	uint64_t task_id = cf_get_rand64() / 2;
	p = as_command_write_field_uint64(p, AS_FIELD_TASK_ID, task_id);
#endif

	for (uint32_t i = 0; i < n_bins; i++) {
		p = as_command_write_bin(p, AS_OPERATOR_WRITE, &bins[i], &buffers[i]);
	}
	
	size = as_command_write_end(cmd, p);

	uint8_t *comp_cmd, *comp_cmd_stack;
	comp_cmd = comp_cmd_stack = NULL;
	size_t comp_size = 0;
	if ((policy->compression_threshold != 0)
			&& (size > policy->compression_threshold)) {

		// Send a stack buffer. If the compressed buffer fits in it, no extra malloc is done
		comp_cmd = comp_cmd_stack = alloca(AS_STACK_BUF_SIZE);
		comp_size = AS_STACK_BUF_SIZE;
		as_command_compress(cmd, size, &comp_cmd, &comp_size);
	}

	as_command_node cn;
	as_command_node_init(&cn, key->ns, key->digest.value, AS_POLICY_REPLICA_MASTER, true);
	
	AEROSPIKE_PUT_EXECUTE_STARTING(task_id);

	as_proto_msg msg;

	status = as_command_execute(as->cluster, err, &cn, 
				comp_cmd ? comp_cmd : cmd, comp_cmd ? comp_size : size,
				policy->timeout, policy->retry, as_command_parse_header, &msg);

	AEROSPIKE_PUT_EXECUTE_FINISHED(task_id);
	
	for (uint32_t i = 0; i < n_bins; i++) {
		as_buffer* buffer = &buffers[i];
		
		if (buffer->data) {
			cf_free(buffer->data);
		}
	}
	as_command_free(cmd, size);
	if (comp_cmd && (comp_cmd != comp_cmd_stack)) {
		cf_free(comp_cmd);
	}

	return status;
}

/**
 *	Remove a record from the cluster.
 *
 *	~~~~~~~~~~{.c}
 *		as_key key;
 *		as_key_init(&key, "ns", "set", "key");
 *
 *		if ( aerospike_key_remove(&as, &err, NULL, &key) != AEROSPIKE_OK ) {
 *			fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *		}
 *	~~~~~~~~~~
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param key			The key of the record.
 *
 *	@return AEROSPIKE_OK if successful and AEROSPIKE_ERR_RECORD_NOT_FOUND if the record was not found. Otherwise an error.
 */
as_status aerospike_key_remove(
	aerospike * as, as_error * err, const as_policy_remove * policy, const as_key * key)
{
	as_error_reset(err);
	
	if (! policy) {
		policy = &as->config.policies.remove;
	}
	
	as_status status = as_key_set_digest(err, (as_key*)key);
	
	if (status != AEROSPIKE_OK) {
		return status;
	}

	uint16_t n_fields;
	size_t size = as_command_key_size(policy->key, key, &n_fields);
		
	uint8_t* cmd = as_command_init(size);
	uint8_t* p = as_command_write_header(cmd, 0, AS_MSG_INFO2_WRITE | AS_MSG_INFO2_DELETE, policy->commit_level, 0, AS_POLICY_EXISTS_IGNORE, policy->gen, policy->generation, 0, policy->timeout, n_fields, 0);
	p = as_command_write_key(p, policy->key, key);
	size = as_command_write_end(cmd, p);
	
	as_command_node cn;
	as_command_node_init(&cn, key->ns, key->digest.value, AS_POLICY_REPLICA_MASTER, true);
	
	as_proto_msg msg;
	status = as_command_execute(as->cluster, err, &cn, cmd, size, policy->timeout, policy->retry, as_command_parse_header, &msg);
	
	as_command_free(cmd, size);
	return status;
}

/**
 *	Lookup a record by key, then perform specified operations.
 *
 *	~~~~~~~~~~{.c}
 *		as_key key;
 *		as_key_init(&key, "ns", "set", "key");
 *
 *		as_operations ops;
 *		as_operations_inita(&ops,2);
 *		as_operations_append_int64(&ops, AS_OPERATOR_INCR, "bin1", 456);
 *		as_operations_append_str(&ops, AS_OPERATOR_APPEND, "bin1", "def");
 *
 *		if ( aerospike_key_remove(&as, &err, NULL, &key, &ops) != AEROSPIKE_OK ) {
 *			fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *		}
 *	~~~~~~~~~~
 *	
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param key			The key of the record.
 *	@param ops			The operations to perform on the record.
 *	@param rec			The record to be populated with the data from AS_OPERATOR_READ operations.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 */
as_status aerospike_key_operate(
	aerospike * as, as_error * err, const as_policy_operate * policy, 
	const as_key * key, const as_operations * ops,
	as_record ** rec)
{
	as_error_reset(err);
	
	if (! policy) {
		policy = &as->config.policies.operate;
	}

	int status = as_key_set_digest(err, (as_key*)key);
	
	if (status != AEROSPIKE_OK) {
		return status;
	}

	uint32_t n_operations = ops->binops.size;
	as_buffer* buffers = (as_buffer*)alloca(sizeof(as_buffer) * n_operations);
	memset(buffers, 0, sizeof(as_buffer) * n_operations);
	
	uint16_t n_fields;
	size_t size = as_command_key_size(policy->key, key, &n_fields);
	uint8_t read_attr = 0;
	uint8_t write_attr = 0;
	
	for (int i = 0; i < n_operations; i++) {
		as_binop* op = &ops->binops.entries[i];
		
		switch (op->op)	{
			case AS_OPERATOR_CDT_READ:
			case AS_OPERATOR_READ:
				read_attr |= AS_MSG_INFO1_READ;
				break;

			default:
				write_attr |= AS_MSG_INFO2_WRITE;
				break;
		}
		size += as_command_bin_size(&op->bin, &buffers[i]);
	}

	uint8_t* cmd = as_command_init(size);
	uint8_t* p = as_command_write_header(cmd, read_attr, write_attr, policy->commit_level, policy->consistency_level,
				 AS_POLICY_EXISTS_IGNORE, policy->gen, ops->gen, ops->ttl, policy->timeout, n_fields, n_operations);
	p = as_command_write_key(p, policy->key, key);
	
	for (uint32_t i = 0; i < n_operations; i++) {
		as_binop* op = &ops->binops.entries[i];
		p = as_command_write_bin(p, op->op, &op->bin, &buffers[i]);
	}

	size = as_command_write_end(cmd, p);
	
	as_command_node cn;
	as_command_node_init(&cn, key->ns, key->digest.value, policy->replica, write_attr != 0);
	
	as_command_parse_result_data data;
	data.record = rec;
	data.deserialize = policy->deserialize;

	status = as_command_execute(as->cluster, err, &cn, cmd, size, policy->timeout, policy->retry, as_command_parse_result, &data);
	
	for (uint32_t i = 0; i < n_operations; i++) {
		as_buffer* buffer = &buffers[i];
		
		if (buffer->data) {
			cf_free(buffer->data);
		}
	}
	as_command_free(cmd, size);
	return status;
}

/**
 *	Lookup a record by key, then apply the UDF.
 *
 *	~~~~~~~~~~{.c}
 *		as_key key;
 *		as_key_init(&key, "ns", "set", "key");
 *
 *		as_arraylist args;
 *		as_arraylist_init(&args, 2, 0);
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
as_status aerospike_key_apply(
	aerospike * as, as_error * err, const as_policy_apply * policy, 
	const as_key * key, 
	const char * module, const char * function, as_list * arglist, 
	as_val ** result) 
{
	as_error_reset(err);
	
	if (! policy) {
		policy = &as->config.policies.apply;
	}

	int status = as_key_set_digest(err, (as_key*)key);
	
	if (status != AEROSPIKE_OK) {
		return status;
	}
	
	uint16_t n_fields;
	size_t size = as_command_key_size(policy->key, key, &n_fields);
	size += as_command_string_field_size(module);
	size += as_command_string_field_size(function);
	
	as_serializer ser;
	as_msgpack_init(&ser);
	as_buffer args;
	as_buffer_init(&args);
	as_serializer_serialize(&ser, (as_val*)arglist, &args);
	size += as_command_field_size(args.size);
	n_fields += 3;

	uint8_t* cmd = as_command_init(size);
	uint8_t* p = as_command_write_header(cmd, 0, AS_MSG_INFO2_WRITE, policy->commit_level, 0, 0, 0, 0, policy->ttl, policy->timeout, n_fields, 0);
	p = as_command_write_key(p, policy->key, key);
	p = as_command_write_field_string(p, AS_FIELD_UDF_PACKAGE_NAME, module);
	p = as_command_write_field_string(p, AS_FIELD_UDF_FUNCTION, function);
	p = as_command_write_field_buffer(p, AS_FIELD_UDF_ARGLIST, &args);
	size = as_command_write_end(cmd, p);
	
	as_command_node cn;
	as_command_node_init(&cn, key->ns, key->digest.value, AS_POLICY_REPLICA_MASTER, true);
	
	status = as_command_execute(as->cluster, err, &cn, cmd, size, policy->timeout, 0, as_command_parse_success_failure, result);
	
	as_command_free(cmd, size);
	as_buffer_destroy(&args);
	as_serializer_destroy(&ser);
	return status;
}

bool
aerospike_has_double(aerospike* as)
{
	as_nodes* nodes = as_nodes_reserve(as->cluster);
	
	if (nodes->size == 0) {
		as_nodes_release(nodes);
		return false;
	}
	
	for (uint32_t i = 0; i < nodes->size; i++) {
		if (! nodes->array[i]->has_double) {
			as_nodes_release(nodes);
			return false;
		}
	}
	as_nodes_release(nodes);
	return true;
}

bool
aerospike_has_geo(aerospike* as)
{
	as_nodes* nodes = as_nodes_reserve(as->cluster);
	
	if (nodes->size == 0) {
		as_nodes_release(nodes);
		return false;
	}
	
	for (uint32_t i = 0; i < nodes->size; i++) {
		if (! nodes->array[i]->has_geo) {
			as_nodes_release(nodes);
			return false;
		}
	}
	as_nodes_release(nodes);
	return true;
}
