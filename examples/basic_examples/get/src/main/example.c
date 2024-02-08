#include <stddef.h>
#include <stdlib.h>
#include <stdio.h>

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/as_error.h>
#include <aerospike/as_record.h>
#include <aerospike/as_sleep.h>
#include <aerospike/as_status.h>
#include <aerospike/as_dir.h>
#include <aerospike/as_proto.h>
#include <aerospike/as_command.h>

#include "example_utils.h"

typedef struct {
	uint32_t rec_count;
	uint32_t bytes;
} stats;

static as_status
parse_record(uint8_t** pp, as_msg* msg, as_error* err)
{
	// Parse normal record values.
	as_record rec;
	as_record_inita(&rec, msg->n_ops);
	
	rec.gen = msg->generation;
	rec.ttl = cf_server_void_time_to_ttl(msg->record_ttl);

	uint64_t bval = 0;
	*pp = as_command_parse_key(*pp, msg->n_fields, &rec.key, &bval);

	as_status status = as_command_parse_bins(pp, err, &rec, msg->n_ops, true);

	if (status != AEROSPIKE_OK) {
		as_record_destroy(&rec);
		return status;
	}

	as_record_destroy(&rec);
	return AEROSPIKE_OK;
}

static as_status
parse_records(as_error* err, uint8_t* buf, size_t size)
{
	uint8_t* p = buf;
	uint8_t* end = buf + size;
	as_status status;

	while (p < end) {
		as_msg* msg = (as_msg*)p;
		as_msg_swap_header_from_be(msg);
		p += sizeof(as_msg);

		if (msg->info3 & AS_MSG_INFO3_LAST) {
			if (msg->result_code != AEROSPIKE_OK) {
				// The server returned a fatal error.
				return as_error_set_message(err, msg->result_code, as_error_string(msg->result_code));
			}
			return AEROSPIKE_NO_MORE_RECORDS;
		}

		if (msg->info3 & AS_MSG_INFO3_PARTITION_DONE) {
			// When an error code is received, mark partition as unavailable
			// for the current round. Unavailable partitions will be retried
			// in the next round. Generation is overloaded as partition id.
			if (msg->result_code != AEROSPIKE_OK) {
				//as_partition_tracker_part_unavailable(task->pt, task->np, msg->generation);
			}
			continue;
		}

		if (msg->result_code != AEROSPIKE_OK) {
			// Background scans return AEROSPIKE_ERR_RECORD_NOT_FOUND
			// when the set does not exist on the target node.
			if (msg->result_code == AEROSPIKE_ERR_RECORD_NOT_FOUND) {
				// Non-fatal error.
				return AEROSPIKE_NO_MORE_RECORDS;
			}
			return as_error_set_message(err, msg->result_code, as_error_string(msg->result_code));
		}

		status = parse_record(&p, msg, err);
		
		if (status != AEROSPIKE_OK) {
			return status;
		}
	}
	return AEROSPIKE_OK;
}

static as_status
read_messages(as_error* err, FILE* fp, stats* stats)
{
	size_t capacity = 0;
	uint8_t* buf = NULL;
	size_t size;
	as_proto proto;
	as_status status;

	while (true) {
		// Read header
		size_t rv = fread((uint8_t*)&proto, 1, sizeof(as_proto), fp);
		
		if (rv != sizeof(as_proto)) {
			if (rv == 0) {
				// End of file.
				status = AEROSPIKE_NO_MORE_RECORDS;
			}
			else {
				status = as_error_update(err, AEROSPIKE_ERR_CLIENT, "Header fread failed: %u, %u\n", (uint32_t)rv, (uint32_t)sizeof(as_proto));
			}
			break;
		}

		stats->bytes += sizeof(proto);
		status = as_proto_parse(err, &proto);

		if (status != AEROSPIKE_OK) {
			break;
		}

		size = proto.sz;

		if (size == 0) {
			continue;
		}

		// Prepare buffer
		if (size > capacity) {
			as_command_buffer_free(buf, capacity);
			capacity = (size + 16383) & ~16383; // Round up in 16KB increments.
			buf = as_command_buffer_init(capacity);
		}
		
		// Read remaining message bytes in group
		rv = fread(buf, 1, size, fp);

		if (rv != size) {
			status = as_error_update(err, AEROSPIKE_ERR_CLIENT, "Detail fread failed: %u, %u\n", (uint32_t)rv, (uint32_t)size);
			break;
		}
		
		stats->bytes += size;

		if (proto.type == AS_MESSAGE_TYPE) {
			status = parse_records(err, buf, size);
			stats->rec_count++;
		}
		else if (proto.type == AS_COMPRESSED_MESSAGE_TYPE) {
			status = as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "Unexpected compress!\n");
			break;
		}
		else {
			status = as_proto_type_error(err, &proto, AS_MESSAGE_TYPE);
			break;
		}

		if (status != AEROSPIKE_OK) {
			if (status == AEROSPIKE_NO_MORE_RECORDS) {
				status = AEROSPIKE_OK;
			}
			break;
		}
	}
	as_command_buffer_free(buf, capacity);
	return status;
}

static as_status
read_file(as_error* err, const char* path, stats* stats) {
	FILE* fp = fopen(path, "r");
	
	if (!fp) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Failed to open: %s\n", path);
	}

	as_error_reset(err);
	as_status status;
	
	while (true) {
		status = read_messages(err, fp, stats);
		
		if (status != AEROSPIKE_OK) {
			break;
		}
	}
	
	fclose(fp);
	
	if (status == AEROSPIKE_NO_MORE_RECORDS) {
		return AEROSPIKE_OK;
	}
	return status;
}

int
main(int argc, char* argv[])
{
	if (argc < 2) {
		printf("Usage %s <dir>\n", argv[0]);
		return -1;
	}
		
	const char* dir_path = argv[1];
	as_dir dir;

	if (! as_dir_open(&dir, dir_path)) {
		printf("Failed to open directory: %s\n", dir_path);
		return -1;
	}

	char path[256];
	const char* entry;

	as_error err;
	as_status status = AEROSPIKE_OK;
	stats st;

	while ((entry = as_dir_read(&dir)) != NULL) {
		if (entry[0] == '.') {
			continue;
		}
		
		st.rec_count = 0;
		st.bytes = 0;

		snprintf(path, sizeof(path), "%s/%s", dir_path, entry);
		status = read_file(&err, path, &st);
		
		if (status == AEROSPIKE_OK) {
			printf("%s %u %u\n", path, st.rec_count, st.bytes);
		}
		else {
			printf("Failed to read file %s: %u,%u,%d,%s\n", path, st.rec_count, st.bytes, err.code, err.message);
			break;
		}
	}

	as_dir_close(&dir);
	return status;
}
