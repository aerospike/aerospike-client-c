/*
 * Copyright 2008-2019 Aerospike, Inc.
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
#include <aerospike/as_proto.h>
#include <citrusleaf/cf_byte_order.h>
#include <string.h>
#include <zlib.h>

// Byte swap proto from current machine byte order to network byte order (big endian).
void
as_proto_swap_to_be(as_proto *p)
{
	uint8_t	version = (uint8_t)p->version;
	uint8_t type = (uint8_t)p->type;
	p->version = p->type = 0;
	p->sz = cf_swap_to_be64(*(uint64_t*)p);
	p->version = version;
	p->type = type;
}

// Byte swap proto from network byte order (big endian) to current machine byte order.
void
as_proto_swap_from_be(as_proto *p)
{
	uint8_t	version = (uint8_t)p->version;
	uint8_t type = (uint8_t)p->type;
	p->version = p->type = 0;
	p->sz = cf_swap_from_be64(*(uint64_t*)p);
	p->version = version;
	p->type = type;
}

// Byte swap header from network byte order (big endian) to current machine byte order.
void
as_msg_swap_header_from_be(as_msg *m)
{
	m->generation = cf_swap_from_be32(m->generation);
	m->record_ttl =  cf_swap_from_be32(m->record_ttl);
	m->transaction_ttl = cf_swap_from_be32(m->transaction_ttl);
	m->n_fields = cf_swap_from_be16(m->n_fields);
	m->n_ops= cf_swap_from_be16(m->n_ops);
}

as_status
as_proto_version_error(as_error* err, as_proto* proto)
{
	return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Invalid proto version: %u Expected: %u",
						   (uint32_t)proto->version, AS_PROTO_VERSION);
}

as_status
as_proto_type_error(as_error* err, as_proto* proto, uint8_t expected)
{
	return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Invalid proto type: %u Expected: %u",
						   (uint32_t)proto->type, (uint32_t)expected);
}

as_status
as_proto_size_error(as_error* err, size_t size)
{
	return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Invalid proto size: %zu", size);
}

as_status
as_compressed_size_error(as_error* err, size_t size)
{
	return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Invalid compressed size: %zu", size);
}

as_status
as_proto_parse(as_error* err, as_proto* proto)
{
	if (proto->version != AS_PROTO_VERSION) {
		return as_proto_version_error(err, proto);
	}

	as_proto_swap_from_be(proto);

	if (proto->sz > PROTO_SIZE_MAX) {
		return as_proto_size_error(err, (size_t)proto->sz);
	}
	return AEROSPIKE_OK;
}

as_status
as_proto_decompress(as_error* err, uint8_t* trg, size_t trg_sz, uint8_t* src, size_t src_sz)
{
	uLongf sz = (uLongf)trg_sz;
	int rv = uncompress(trg, &sz, src + sizeof(uint64_t), (uLong)(src_sz - sizeof(uint64_t)));

	if (rv != Z_OK) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Decompress failed: %d", rv);
	}

	if (sz != trg_sz) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT,
							   "Decompressed size %zu is not expected %zu", (size_t)sz, trg_sz);
	}

	as_proto* proto = (as_proto*)trg;
	as_proto_swap_from_be(proto);

	if (! (proto->version == AS_PROTO_VERSION &&
		   proto->type == AS_MESSAGE_TYPE &&
		   sizeof(as_proto) + proto->sz == sz)) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT,
							   "Invalid decompressed proto(%d,%d,%zu,%zu)",
							   (int)proto->version, (int)proto->type, (size_t)proto->sz, (size_t)sz);
	}
	return AEROSPIKE_OK;
}
