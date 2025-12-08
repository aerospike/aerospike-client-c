/*
 * Copyright 2008-2025 Aerospike, Inc.
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

#include <aerospike/as_ml_vector.h>
#include <aerospike/as_bytes.h>
#include <aerospike/as_error.h>
#include <aerospike/as_status.h>
#include <aerospike/as_vector.h>
#include <citrusleaf/cf_byte_order.h>
#include <stdlib.h>
#include <string.h>

//---------------------------------
// Functions
//---------------------------------

uint32_t
as_ml_vector_element_size(as_ml_vector_element_type element_type)
{
	switch (element_type) {
		case AS_ML_VECTOR_FLOAT32:
		case AS_ML_VECTOR_INT32:
			return 4;
		case AS_ML_VECTOR_FLOAT64:
		case AS_ML_VECTOR_INT64:
			return 8;
		default:
			return 0;
	}
}

as_status
as_ml_vector_init_float32(as_vector* vector, float* data, uint32_t count)
{
	if (!vector || !data || count == 0) {
		return AEROSPIKE_ERR_PARAM;
	}

	as_vector_init(vector, sizeof(float), count);

	for (uint32_t i = 0; i < count; i++) {
		as_vector_append(vector, &data[i]);
	}

	return AEROSPIKE_OK;
}

as_status
as_ml_vector_init_float64(as_vector* vector, double* data, uint32_t count)
{
	if (!vector || !data || count == 0) {
		return AEROSPIKE_ERR_PARAM;
	}

	as_vector_init(vector, sizeof(double), count);

	for (uint32_t i = 0; i < count; i++) {
		as_vector_append(vector, &data[i]);
	}

	return AEROSPIKE_OK;
}

as_status
as_ml_vector_init_int32(as_vector* vector, int32_t* data, uint32_t count)
{
	if (!vector || !data || count == 0) {
		return AEROSPIKE_ERR_PARAM;
	}

	as_vector_init(vector, sizeof(int32_t), count);

	for (uint32_t i = 0; i < count; i++) {
		as_vector_append(vector, &data[i]);
	}

	return AEROSPIKE_OK;
}

as_status
as_ml_vector_init_int64(as_vector* vector, int64_t* data, uint32_t count)
{
	if (!vector || !data || count == 0) {
		return AEROSPIKE_ERR_PARAM;
	}

	as_vector_init(vector, sizeof(int64_t), count);

	for (uint32_t i = 0; i < count; i++) {
		as_vector_append(vector, &data[i]);
	}

	return AEROSPIKE_OK;
}

as_status
as_ml_vector_serialize(const as_vector* vector, as_ml_vector_element_type element_type, as_bytes* bytes)
{
	if (!vector || !bytes || !vector->list || vector->size == 0) {
		return AEROSPIKE_ERR_PARAM;
	}

	uint32_t element_size = as_ml_vector_element_size(element_type);
	if (element_size == 0) {
		return AEROSPIKE_ERR_PARAM;
	}

	// Verify that the vector's item_size matches the element type
	if (vector->item_size != element_size) {
		return AEROSPIKE_ERR_PARAM;
	}

	// Calculate total size: header (16 bytes) + data
	uint32_t data_size = vector->size * element_size;
	uint32_t total_size = 16 + data_size;

	// Allocate buffer
	uint8_t* buffer = malloc(total_size);
	if (!buffer) {
		return AEROSPIKE_ERR_CLIENT;
	}

	uint8_t* p = buffer;

	// Write magic number (native byte order)
	*(uint32_t*)p = AS_ML_VECTOR_MAGIC_NUMBER;
	p += 4;

	// Write version (native byte order)
	*(uint32_t*)p = AS_ML_VECTOR_VERSION;
	p += 4;

	// Write element count (native byte order)
	*(uint32_t*)p = vector->size;
	p += 4;

	// Write element type (native byte order)
	*(uint32_t*)p = (uint32_t)element_type;
	p += 4;

	// Write vector data (native byte order)
	memcpy(p, vector->list, data_size);

	// Initialize as_bytes with the buffer
	as_bytes_init_wrap(bytes, buffer, total_size, true);

	// Set the correct particle type for vectors
	bytes->type = AS_BYTES_VECTOR;

	return AEROSPIKE_OK;
}

as_status
as_ml_vector_deserialize(const as_bytes* bytes, as_vector* vector, as_ml_vector_element_type* element_type)
{
	if (!bytes || !vector || !element_type || !bytes->value || bytes->size < 16) {
		return AEROSPIKE_ERR_PARAM;
	}

	uint8_t* p = bytes->value;

	// Read and validate magic number (native byte order)
	uint32_t magic = *(uint32_t*)p;
	if (magic != AS_ML_VECTOR_MAGIC_NUMBER) {
		return AEROSPIKE_ERR_PARAM;
	}
	p += 4;

	// Read and validate version (native byte order)
	uint32_t version = *(uint32_t*)p;
	if (version != AS_ML_VECTOR_VERSION) {
		return AEROSPIKE_ERR_PARAM;
	}
	p += 4;

	// Read element count (native byte order)
	uint32_t element_count = *(uint32_t*)p;
	p += 4;

	// Read element type (native byte order)
	uint32_t element_type_raw = *(uint32_t*)p;
	p += 4;

	// Validate element type
	as_ml_vector_element_type elem_type = (as_ml_vector_element_type)element_type_raw;
	uint32_t element_size = as_ml_vector_element_size(elem_type);
	if (element_size == 0) {
		return AEROSPIKE_ERR_PARAM;
	}

	// Calculate expected data size
	uint32_t expected_data_size = element_count * element_size;
	if (bytes->size != 16 + expected_data_size) {
		return AEROSPIKE_ERR_PARAM;
	}

	// Initialize vector
	as_vector_init(vector, element_size, element_count);

	// Copy vector data
	uint8_t* data_ptr = p;
	for (uint32_t i = 0; i < element_count; i++) {
		as_vector_append(vector, data_ptr + (i * element_size));
	}

	*element_type = elem_type;
	return AEROSPIKE_OK;
}
