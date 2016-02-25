/*
 * Copyright 2008-2016 Aerospike, Inc.
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
#include <aerospike/aerospike_udf.h>
#include <aerospike/aerospike_info.h>
#include <aerospike/as_cluster.h>
#include <aerospike/as_error.h>
#include <aerospike/as_log.h>
#include <aerospike/as_policy.h>
#include <aerospike/as_status.h>

#include <citrusleaf/alloc.h>
#include <citrusleaf/cf_b64.h>
#include <citrusleaf/cf_crypto.h>

#include <stdio.h>
#include <openssl/sha.h>

/******************************************************************************
 * TYPES
 *****************************************************************************/

typedef struct as_udf_file_ptr_s {
	char* name;
	char* hash;
	char* type;
} as_udf_file_ptr;

char* as_udf_type_str[] = {"LUA", 0};

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

static void
as_udf_parse_file(const char* token, char* p, as_udf_file_ptr* ptr)
{
	if (strcmp(token, "filename") == 0) {
		ptr->name = p;
		return;
	}
	
	if (strcmp(token, "hash") == 0) {
		ptr->hash = p;
		return;
	}
	
	if (strcmp(token, "type") == 0) {
		ptr->type = p;
		return;
	}
}

/**
 * @return AEROSPIKE_OK if successful. Otherwise an error occurred.
 */
as_status aerospike_udf_list(
	aerospike * as, as_error * err, const as_policy_info * policy, 
	as_udf_files * files)
{
	as_error_reset(err);
	
	if (! policy) {
		policy = &as->config.policies.info;
	}

	char* response = 0;
	as_status status = aerospike_info_any(as, err, policy, "udf-list", &response);
	
	if (status) {
		return status;
	}
	
	// response := udf-list\tfilename=<name>,hash=<hash>,type=<type>;[filename=<name>...]
	char* p = strchr(response, '\t');
	
	if (!p) {
		as_error_update(err, AEROSPIKE_ERR_PARAM, "Invalid udf-list response: %s", response);
		cf_free(response);
		return AEROSPIKE_ERR_PARAM;
	}
	p++;
	
	uint32_t capacity = (files->capacity <= 0) ? 500 : files->capacity;
	
	as_vector ptrs;
	as_vector_inita(&ptrs, sizeof(as_udf_file_ptr), capacity);
	
	as_udf_file_ptr ptr = {0,0,0};
	char* token = p;
	
	while (*p) {
		switch (*p) {
			case '=':
				*p++ = 0;
				as_udf_parse_file(token, p, &ptr);
				break;
	
			case ',':
				*p++ = 0;
				token = p;
				break;
				
			case ';':
				*p++ = 0;
				token = p;
				as_vector_append(&ptrs, &ptr);
				ptr.name = 0;
				ptr.hash = 0;
				ptr.type = 0;
				break;
				
			default:
				p++;
				break;
		}
	}
	
	if (files->capacity == 0 && files->entries == NULL) {
		as_udf_files_init(files, ptrs.size);
	}

	uint32_t limit = ptrs.size < files->capacity ? ptrs.size : files->capacity;
	files->size = limit;

	for (uint32_t i = 0; i < limit; i++) {
		as_udf_file_ptr* ptr = as_vector_get(&ptrs, i);
		as_udf_file* file = &files->entries[i];
		
		if (ptr->name) {
			as_strncpy(file->name, ptr->name, AS_UDF_FILE_NAME_SIZE);
		}
		else {
			file->name[0] = 0;
		}
		
		if (ptr->hash) {
			as_strncpy((char*)file->hash, ptr->hash, AS_UDF_FILE_HASH_SIZE+1);
		}
		else {
			file->hash[0] = 0;
		}
		
		file->type = AS_UDF_TYPE_LUA;
		file->content._free = false;
		file->content.size = 0;
		file->content.capacity = 0;
		file->content.bytes = NULL;
	}

	as_vector_destroy(&ptrs);
	cf_free(response);
	return AEROSPIKE_OK;
}

/**
 * @return AEROSPIKE_OK if successful. Otherwise an error occurred.
 */
as_status aerospike_udf_get(
	aerospike * as, as_error * err, const as_policy_info * policy, 
	const char * filename, as_udf_type type, as_udf_file * file)
{
	as_error_reset(err);
	
	if (! policy) {
		policy = &as->config.policies.info;
	}
		
	char command[512];
	snprintf(command, sizeof(command), "udf-get:filename=%s;", filename);
	
	char* response = 0;
	as_status status = aerospike_info_any(as, err, policy, command, &response);
	
	if (status) {
		return status;
	}
	
	// response := <command>\tgen=<string>;type=<string>;content=<string>;
	char* p = strchr(response, '\t');
	
	if (!p) {
		as_error_update(err, AEROSPIKE_ERR_PARAM, "Invalid udf-get response: %s", response);
		cf_free(response);
		return AEROSPIKE_ERR_PARAM;
	}
	p++;
	
	p = strstr(p, "content=");
	
	if (!p) {
		as_error_update(err, AEROSPIKE_ERR_PARAM, "Invalid udf-get response: %s", response);
		cf_free(response);
		return AEROSPIKE_ERR_PARAM;
	}
	p += 8;
	
	as_strncpy(file->name, filename, AS_UDF_FILE_NAME_SIZE);
	file->type = AS_UDF_TYPE_LUA;

	char* content = p;
	while (*p) {
		if (*p == ';') {
			*p = 0;
			break;
		}
		p++;
	}
	
	uint32_t len = (uint32_t)(p - content);
	uint32_t size;
	cf_b64_validate_and_decode_in_place((uint8_t*)content, len, &size);

	// Update file hash
	unsigned char hash[SHA_DIGEST_LENGTH];
#ifdef __APPLE__
	// Openssl is deprecated on mac, but the library is still included.
	// Save old settings and disable deprecated warnings.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#endif
	SHA1((uint8_t*)content, size, hash);
#ifdef __APPLE__
	// Restore old settings.
#pragma GCC diagnostic pop
#endif
	cf_convert_sha1_to_hex(hash, file->hash);

	file->content._free = true;
	file->content.size = size;
	file->content.capacity = size;
	file->content.bytes = cf_malloc(size);
	memcpy(file->content.bytes, content, size);
	
	cf_free(response);
	return AEROSPIKE_OK;
}

/**
 * @return AEROSPIKE_OK if successful. Otherwise an error occurred.
 */
as_status aerospike_udf_put(
	aerospike * as, as_error * err, const as_policy_info * policy, 
	const char * filename, as_udf_type type, as_bytes * content)
{
	if (type != AS_UDF_TYPE_LUA) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM, "Invalid udf type: %d", type);
	}
	
	as_error_reset(err);
	
	if (! policy) {
		policy = &as->config.policies.info;
	}
		
	as_string filename_string;
	const char* filebase = as_basename(&filename_string, filename);
		
	uint32_t encoded_len = cf_b64_encoded_len(content->size);
	char* content_base64 = cf_malloc(encoded_len + 1);
	
	cf_b64_encode(content->value, content->size, content_base64);
	content_base64[encoded_len] = 0;
	
	size_t size = encoded_len + 1024;
	char* command = cf_malloc(size);
	
	if (snprintf(command, size, "udf-put:filename=%s;content=%s;content-len=%d;udf-type=%s;",
				   filebase, content_base64, encoded_len, as_udf_type_str[type]) >= size) {
		as_string_destroy(&filename_string);
		cf_free(content_base64);
		cf_free(command);
		return as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "Udf put snprintf failed");
	}
	as_string_destroy(&filename_string);
	
	char* response = 0;
	as_status status = aerospike_info_any(as, err, policy, command, &response);
	cf_free(content_base64);
	cf_free(command);
	
	if (status) {
		return status;
	}
	
	cf_free(response);
	return AEROSPIKE_OK;
}

static bool
aerospike_udf_put_is_done(aerospike* as, as_error * err, const as_policy_info* policy, char* filter)
{
	// Query all nodes for task completion status.
	bool done = true;
	as_nodes* nodes = as_nodes_reserve(as->cluster);
	
	for (uint32_t i = 0; i < nodes->size && done; i++) {
		as_node* node = nodes->array[i];
		struct sockaddr_in* sa_in = as_node_get_address(node);
		
		char* response = 0;
		as_status status = aerospike_info_socket_address(as, err, policy, sa_in, "udf-list", &response);
		
		if (status == AEROSPIKE_OK) {
			char* p = strstr(response, filter);
			
			if (! p) {
				done = false;
			}
			cf_free(response);
		}
	}
	as_nodes_release(nodes);
	return done;
}

as_status aerospike_udf_put_wait(
	aerospike * as, as_error * err, const as_policy_info * policy,
	const char * filename, uint32_t interval_ms)
{
	if (! policy) {
		policy = &as->config.policies.info;
	}

	char filter[256];
	snprintf(filter, sizeof(filter), "filename=%s", filename);
	
	uint32_t interval_micros = (interval_ms <= 0)? 1000 * 1000 : interval_ms * 1000;
	
	bool done;
		
	do {
		usleep(interval_micros);
		done = aerospike_udf_put_is_done(as, err, policy, filter);
	} while (! done);
	
	return AEROSPIKE_OK;
}

/**
 * @return AEROSPIKE_OK if successful. Otherwise an error occurred.
 */
as_status aerospike_udf_remove(
	aerospike * as, as_error * err, const as_policy_info * policy, 
	const char * filename)
{
	as_error_reset(err);
	
	if (! policy) {
		policy = &as->config.policies.info;
	}
	
	char command[512];
	snprintf(command, sizeof(command), "udf-remove:filename=%s;", filename);
	
	char* response = 0;
	as_status status = aerospike_info_any(as, err, policy, command, &response);
	
	if (status) {
		return status;
	}
	
	cf_free(response);
	return AEROSPIKE_OK;
}
