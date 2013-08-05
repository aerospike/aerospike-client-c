/******************************************************************************
 * Copyright 2008-2013 by Aerospike.
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
 *****************************************************************************/

#include <citrusleaf/cf_b64.h>
#include <citrusleaf/cf_proto.h>

#include <aerospike/as_bytes.h>
#include <aerospike/as_nil.h>
#include <aerospike/as_msgpack.h>

#include <citrusleaf/cl_udf.h>
#include <citrusleaf/cl_write.h>
#include <citrusleaf/cl_parsers.h>

#include "internal.h"

/******************************************************************************
 * MACROS
 ******************************************************************************/

#define LOG(msg, ...) \
	// { printf("%s@%s:%d - ", __func__, __FILE__, __LINE__); printf(msg, ##__VA_ARGS__ ); printf("\n"); }

/*
 * Mapping between string udf type and integer type
 */
#define MAX_UDF_TYPE 1
char * cl_udf_type_str[] = {"LUA", 0};

/******************************************************************************
 * TYPES
 ******************************************************************************/

typedef struct cl_udf_filelist_s {
	int         	capacity;
	int         	size;
	cl_udf_file *   files;
} cl_udf_filelist;

/******************************************************************************
 * STATIC FUNCTIONS
 ******************************************************************************/

static void * cl_udf_info_parse(char * key, char * value, void * context)
{
	LOG("key = %s, value=%s", key, value);

	cl_udf_info * info = (cl_udf_info *) context;

	if ( strcmp(key,"error") == 0 ) {
		info->error = strdup(value);
	}
	if ( strcmp(key,"filename") == 0 ) {
		strncpy(info->filename, value, strlen(value));
	}
	else if ( strcmp(key,"gen") == 0 ) {
		info->gen = strdup(value);
	}
	else if ( strcmp(key,"content") == 0 ) {
		as_bytes_destroy(&info->content);
		int c_len = strlen(value);
		uint8_t * c = (uint8_t *) malloc(c_len + 1);
		memcpy(c, value, c_len);
		c[c_len] = 0;
		as_bytes_init_wrap(&info->content, c, c_len + 1, true /*memcpy*/);
	}
	// else if ( strcmp(key,"files") == 0 ) {
	// 	info->files = strdup(value);
	// }
	// else if ( strcmp(key,"count") == 0 ) {
	// 	info->count = atoi(value);
	// }
	else if (strcmp(key, "hash") == 0 ) {
		memcpy(info->hash, value, strlen(value));
	}
	return info;
}


static void * cl_udf_file_parse(char * key, char * value, void * context)
{
	LOG("key = %s, value=%s", key, value);

	cl_udf_file * file = (cl_udf_file *) context;

	if ( strcmp(key,"filename") == 0 ) {
		strncpy(file->name, value, strlen(value));
	}
	else if ( strcmp(key,"content") == 0 ) {
		as_bytes_destroy(file->content);
		int c_len = strlen(value);
		uint8_t * c = (uint8_t *) malloc(c_len + 1);
		memcpy(c, value, c_len);
		c[c_len] = 0;
		as_bytes_init_wrap(file->content, c, c_len + 1, true /*memcpy*/);
	}
	else if (strcmp(key, "hash") == 0 ) {
		memcpy(file->hash, value, strlen(value));
	}
	else if (strcmp(key, "type") == 0 ) {
		file->type = AS_UDF_LUA;
	}

	return file;
}

static void *  cl_udf_filelist_parse(char * filedata, void * context) {

	cl_udf_filelist * filelist = (cl_udf_filelist *) context;
	
	LOG("filedata = %s, cap = %d, size = %d", filedata, filelist->capacity, filelist->size);

	if ( filelist->size < filelist->capacity ) {
		cl_parameters_parser parser = {
			.delim = ',',
			.context = &filelist->files[filelist->size],
			.callback = cl_udf_file_parse
		};
		cl_parameters_parse(filedata, &parser);
		filelist->size++;
	}

	return filelist;
}

/*
static void * cl_udf_list_parse(char * key, char * value, void * context)
{
	LOG("key = %s, value=%s", key, value);

	cl_udf_filelist * filelist = (cl_udf_filelist *) context;
	
	if ( strcmp(key,"files") == 0 ) {
		cl_seq_parser parser = {
			.delim = ';',
			.context = filelist,
			.callback = cl_udf_filelist_parse
		};
		cl_seq_parse(value, &parser);
	}

	return context;
}
*/

static int print_buffer(as_buffer * buff)
{
	msgpack_sbuffer sbuf;
	msgpack_sbuffer_init(&sbuf);

	sbuf.data = (char *) buff->data;
	sbuf.size = buff->size;
	sbuf.alloc = buff->capacity;

	msgpack_zone mempool;
	msgpack_zone_init(&mempool, 2048);

	msgpack_object deserialized;
	msgpack_unpack(sbuf.data, sbuf.size, NULL, &mempool, &deserialized);
	
	msgpack_object_print(stdout, deserialized);
	
	msgpack_zone_destroy(&mempool);
	return 0;
}


static as_val * cl_udf_bin_to_val(as_serializer * ser, cl_bin * bin) {

	as_val * val = NULL;

	switch( bin->object.type ) {
		case CL_INT : {
			val = (as_val *) as_integer_new(bin->object.u.i64);
			break;
		}
		case CL_STR : {
			// steal the pointer from the object into the val
			val = (as_val *) as_string_new(strdup(bin->object.u.str), true /*ismalloc*/);
			break;
		}
		case CL_BLOB:
		case CL_JAVA_BLOB:
		case CL_CSHARP_BLOB:
		case CL_PYTHON_BLOB:
		case CL_RUBY_BLOB:
		case CL_ERLANG_BLOB:
		{
			uint8_t *b = malloc(sizeof(bin->object.sz));
			memcpy(b, bin->object.u.blob, bin->object.sz);
			val = (as_val *)as_bytes_new_wrap(b, bin->object.sz, true /*ismalloc*/);
		}
		case CL_LIST :
		case CL_MAP : {
			// use a temporary buffer, which doesn't need to be destroyed
			as_buffer buf = {
				.capacity = (uint32_t) bin->object.sz,
				.size = (uint32_t) bin->object.sz,
				.data = (uint8_t *) bin->object.u.blob
			};
			// print_buffer(&buf);
			as_serializer_deserialize(ser, &buf, &val);
			break;
		}
		case CL_NULL : {
			val = (as_val*) &as_nil;
			break;
		}
		default : {
			val = NULL;
			break;
		}
	}
	return val;
}

as_val * citrusleaf_udf_bin_to_val(as_serializer * ser, cl_bin * bin) {
	return cl_udf_bin_to_val(ser, bin);
}

/******************************************************************************
 * FUNCTIONS
 ******************************************************************************/



cl_rv citrusleaf_udf_record_apply(cl_cluster * cl, const char * ns, const char * set, const cl_object * key, 
	const char * filename, const char * function, as_list * arglist, int timeout_ms, as_result * res) {

	cl_rv rv = CITRUSLEAF_OK;
	char err_str[256];

	as_serializer ser;
	as_msgpack_init(&ser);

	as_string file;
	as_string_init(&file, (char *) filename, true /*ismalloc*/);

	as_string func;
	as_string_init(&func, (char *) function, true /*ismalloc*/);
	
	as_buffer args;
	as_buffer_init(&args);

	as_serializer_serialize(&ser, (as_val *) arglist, &args);

	as_call call = {
		.file = &file,
		.func = &func,
		.args = &args
	};

	uint64_t trid = 0;

	cl_write_parameters wp;
	cl_write_parameters_set_default(&wp);
	wp.timeout_ms = timeout_ms;

	cl_bin *bins = 0;
	int n_bins = 0;

	// print_buffer(&args);

	rv = do_the_full_monte( 
		cl, 0, CL_MSG_INFO2_WRITE, 0, 
		ns, set, key, 0, &bins, CL_OP_WRITE, 0, &n_bins, 
		NULL, &wp, &trid, NULL, &call, NULL
	);

	as_buffer_destroy(&args);

	if (! (rv == CITRUSLEAF_OK || rv == CITRUSLEAF_FAIL_UDF_BAD_RESPONSE)) {
		// Add the exact error-code to return value
		//snprintf(err_str, 256, "None UDF failure Error-code: %d", rv);
		snprintf(err_str, 256, "Error in parsing udf params Error-code: %d", rv);
		as_result_setfailure(res, (as_val *) as_string_new(err_str,false));
	} else if ( n_bins == 1  ) {

		cl_bin *bin = &bins[0];

		as_val *val = cl_udf_bin_to_val(&ser, bin);

		if ( val ) {
			if ( strcmp(bin->bin_name,"FAILURE") == 0 ) {
				snprintf(err_str, 256, "Failure in converting udf-bin to value for type :%d", val->type);
				as_result_setfailure(res, (as_val *) as_string_new(err_str,false));
				as_val_destroy(val);
				//	as_result_setfailure(res, val);
			}
			else if ( strcmp(bin->bin_name,"SUCCESS") == 0 ) {
				as_result_setsuccess(res, val);
			}
			else {
				snprintf(err_str, 256, "Invalid response in converting udf-bin to value for type :%d", val->type);
				as_result_setfailure(res, (as_val *) as_string_new(err_str,false));
				as_val_destroy(val);
				//as_result_setfailure(res, (as_val *) as_string_new("Invalid response. (1)",false/*ismalloc*/));
			}
		}
		else {
			snprintf(err_str, 256, "Null value returned in converting udf-bin to value ");
			as_result_setfailure(res, (as_val *) as_string_new(err_str,false));
		   // as_result_setfailure(res, (as_val *) as_string_new("Invalid response. (2)",false/*ismalloc*/));
		}

	}
	else {
		snprintf(err_str, 256, " Generic parser error for udf-apply, Error-code: %d", rv);
		as_result_setfailure(res, (as_val *) as_string_new(err_str,false));
	   // as_result_setfailure(res, (as_val *) as_string_new("Invalid response. (3)",false/*ismalloc*/));
	}

	if (bins) {
		citrusleaf_bins_free(bins, n_bins);
		free(bins);
	}

	as_serializer_destroy(&ser);
	
	return rv;
}



cl_rv citrusleaf_udf_list(cl_cluster *asc, cl_udf_file ** files, int * count, char ** error) {
	
	*files = NULL;
	*count = 0;

	char *  query   = "udf-list";
	char *  result  = 0;

	if ( citrusleaf_info_cluster(asc, query, &result, true, /* check bounds */ true, 100) ) {
		if ( error ) {
			const char * emsg = "failed_request: ";
			int emsg_len = strlen(emsg);
			int query_len = strlen(query);
			*error = (char *) calloc ((emsg_len + query_len), sizeof(char));
			strncpy(*error, emsg, emsg_len);
			strncpy(*error+emsg_len, query, query_len);
		}
		return -1;
	}

	if ( !result ) {
		if ( error ) {
			*error = strdup("invalid_response");
		}
		return -2;
	}
	
	/**
	 * result   := {request}\t{response}
	 * response := filename=<name>,hash=<hash>,type=<type>[:filename=<name>...]
	 */
	
	char * response = strchr(result, '\t') + 1;

	// Calculate number of files mentioned in response
	// Entry for each file is seperated by delim ';'
	char * haystack = response;
	while ( (haystack = strstr (haystack, "filename")) != 0 ) 
	{
		*count = *count + 1;
		haystack = haystack + 8;
	}
	
	if (*count == 0)
	{
		// No files at server
		*files = NULL;
		return 0;
	}
	
	// Allocate memory for filelist
	// caller has to free memory for .files
	cl_udf_filelist filelist = {
		.capacity   = *count,              // Allocated size
		.size       = 0,                   // Actual entries
		.files      = (cl_udf_file *) calloc((*count), sizeof(cl_udf_file)) 
	};

	cl_seq_parser parser = {
		.delim = ';',
		.context = &filelist,
		.callback = cl_udf_filelist_parse
	};
	cl_seq_parse(response, &parser);

	*files = filelist.files;
	*count = filelist.size;

	free(result);
	result = NULL;

	return 0;
}

cl_rv citrusleaf_udf_get(cl_cluster *asc, const char * filename, cl_udf_file * file, cl_udf_type udf_type, char ** error) {
	return citrusleaf_udf_get_with_gen(asc, filename, file, 0, NULL, error);
}

cl_rv citrusleaf_udf_get_with_gen(cl_cluster *asc, const char * filename, cl_udf_file * file, cl_udf_type udf_type, char **gen, char ** error) {

	if ( file->content ) return -1;

	char    query[512]  = {0};
	char *  result      = NULL;

	snprintf(query, sizeof(query), "udf-get:filename=%s;", filename);


	// fprintf(stderr, "QUERY: |%s|\n", query);


	if ( citrusleaf_info_cluster(asc, query, &result, true, /* check bounds */ true, 100) ) {
		if ( error ) {
			const char * emsg = "failed_request: ";
			int emsg_len = strlen(emsg);
			int query_len = strlen(query);
			*error = (char *) malloc(sizeof(char) * (emsg_len + query_len));
			strncpy(*error, emsg, emsg_len);
			strncpy(*error+emsg_len, query, query_len);
		}
		return -1;
	}

	if ( !result ) {
		if ( error ) *error = strdup("invalid_response");
		return -2;
	}
	
	/**
	 * result   := {request}\t{response}
	 * response := gen=<string>;content=<string>
	 */

	char * response = strchr(result, '\t') + 1;
	
	cl_udf_info info = { NULL };

	cl_parameters_parser parser = {
		.delim = ';',
		.context = &info,
		.callback = cl_udf_info_parse
	};
	cl_parameters_parse(response, &parser);

	free(result);
	result = NULL;

	if ( info.error ) {
		if ( error ) {
			*error = info.error;
			info.error = NULL;
		}
		cl_udf_info_destroy(&info);
		return 1;
	}

	if ( info.content.size == 0 ) {
		*error = strdup("file_not_found");
		cl_udf_info_destroy(&info);
		return 2;
	}

	uint8_t *   content = info.content.value;
	int         clen    = info.content.size - 1; // this is a byte array, not string. last char is NULL.

	cf_base64_decode_inplace(content, &clen, true);
	
	file->content = as_bytes_new_wrap(content, clen, true);

	info.content.value = NULL;
	info.content.free = false;
	info.content.size = 0;
	info.content.capacity = 0;

	as_bytes_destroy(&info.content);
   
	strcpy(file->name, filename);

	// Update file hash
	unsigned char hash[SHA_DIGEST_LENGTH];
	SHA1(info.content.value, info.content.size, hash);

	cf_convert_sha1_to_hex(hash, file->hash);
	
	if ( gen ) {
		*gen = info.gen;
		info.gen = NULL;
	}

	cl_udf_info_destroy(&info);

	return 0;
}

cl_rv citrusleaf_udf_put(cl_cluster *asc, const char * filename, as_bytes *content, cl_udf_type udf_type, char ** error) {

	if ( !filename || !(content)) {
		fprintf(stderr, "filename and content required\n");
		return CITRUSLEAF_FAIL_CLIENT;
	}

	char * query = NULL;
	char *  filepath    = strdup(filename);
	char *  filebase    = basename(filepath);

	int  clen = content->size;
	if (udf_type < 0 || udf_type > (MAX_UDF_TYPE - 1))
	{
		fprintf(stderr, "Invalid UDF type");
		return CITRUSLEAF_FAIL_PARAMETER;
	}
	char * content_base64 = malloc(cf_base64_encode_maxlen(clen));
	cf_base64_tostring(content->value, content_base64, &clen);

	if (! asprintf(&query, "udf-put:filename=%s;content=%s;content-len=%d;udf-type=%s;", filebase, content_base64, clen, cl_udf_type_str[udf_type])) {
		fprintf(stderr, "Query allocation failed");
		return CITRUSLEAF_FAIL_CLIENT;
	}
	
	// fprintf(stderr, "QUERY: |%s|\n",query);

	free(filepath);

	int rc = 0;
	rc = citrusleaf_info_cluster(asc, query, (void *)(error), true, false, 1000);
	if (  rc ) {
		if ( error ) {
			const char * emsg = "failed_request: ";
			int emsg_len = strlen(emsg);
			int query_len = strlen(query);
			*error = (char *) malloc(sizeof(char) * (emsg_len + query_len));
			strncpy(*error, emsg, emsg_len);
			strncpy(*error+emsg_len, query, query_len);
		}
		free(query);
		free(content_base64);
		return -1;
	}

	free(query);
	free(content_base64);
	content_base64 = 0;
	query = NULL;
	return 0;
}

cl_rv citrusleaf_udf_remove(cl_cluster *asc, const char * filename, char ** error) {

	char    query[512]  = {0};
	char *  result      = NULL;

	snprintf(query, sizeof(query), "udf-remove:filename=%s;", filename);

	if ( citrusleaf_info_cluster(asc, query, &result, true, /* check bounds */ true, 100) ) {
		if ( error ) {
			const char * emsg = "failed_request: ";
			int emsg_len = strlen(emsg);
			int query_len = strlen(query);
			*error = (char *) malloc(sizeof(char) * (emsg_len + query_len));
			strncpy(*error, emsg, emsg_len);
			strncpy(*error+emsg_len, query, query_len);
		}
		return -1;
	}

	if ( !result ) {
		if ( error ) *error = strdup("invalid_response");
		return -2;
	}
	
	/**
	 * result   := {request}\t{response}
	 * response := gen=<string>;content=<string>
	 */

	char * response = strchr(result, '\t') + 1;
	
	cl_udf_info info = { NULL };

	cl_parameters_parser parser = {
		.delim = ';',
		.context = &info,
		.callback = cl_udf_info_parse
	};
	cl_parameters_parse(response, &parser);

	free(result);
	result = NULL;

	if ( info.error ) {
		if ( error ) {
			*error = info.error;
			info.error = NULL;
		}
		cl_udf_info_destroy(&info);
		return 1;
	}
	
	cl_udf_info_destroy(&info);
	return 0;
	
}

void cl_udf_info_destroy(cl_udf_info * info)
{
	if ( info->error ) {
		free(info->error);
	}
	
	as_val_destroy(&info->content);
	
	if ( info->gen ) {
		free(info->gen);
	}
	
	// if ( info->files ) {
	// 	free(info->files);
	// }

	info->error = NULL;
	info->gen = NULL;
	// info->files = NULL;
	// info->count = 0;
}
