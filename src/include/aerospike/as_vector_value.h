/*
 * Copyright 2008-2026 Aerospike, Inc.
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
#pragma once

#include <aerospike/as_std.h>
#include <aerospike/as_bytes.h>

#ifdef __cplusplus
extern "C" {
#endif

//---------------------------------
// Macros
//---------------------------------

/**
 * Current vector wire format version.
 */
#define AS_VECTOR_VALUE_VERSION 1

/**
 * Size in bytes of the fixed vector header (version + element_type +
 * dimensions + reserved).
 */
#define AS_VECTOR_VALUE_HEADER_SIZE 8

//---------------------------------
// Types
//---------------------------------

/**
 * Vector element type. Identifies how each element of an as_vector_value is
 * encoded on the wire.
 */
typedef enum as_vector_element_type_e {
	/**
	 * float16: high-density vectors (IEEE 754 half). Since C has no native
	 * float16 type, each element is represented by its raw 16-bit pattern.
	 */
	AS_VECTOR_ELEMENT_TYPE_FLOAT16 = 0x01,

	/**
	 * int32: integer-based embeddings.
	 */
	AS_VECTOR_ELEMENT_TYPE_INT32 = 0x02,

	/**
	 * float (fp32): standard FP32.
	 */
	AS_VECTOR_ELEMENT_TYPE_FLOAT32 = 0x03,

	/**
	 * double (fp64): high-precision FP64.
	 */
	AS_VECTOR_ELEMENT_TYPE_FLOAT64 = 0x04
} as_vector_element_type;

/**
 * Vector of numeric elements, used for vector similarity search. A vector is
 * defined by the following wire format:
 *
 * ~~~~~~~~~~
 * Offset  Size (bytes)  Field         Description
 * 0       1             version       The version of the vector format.
 * 1       1             element_type  Enum identifying the element type (see as_vector_element_type).
 * 2       4             dimensions    Number of dimensions (elements) in the vector.
 * 6       2             reserved      Reserved for future use & padding (8-byte alignment).
 * 8       variable      data          Contiguous array of elements.
 * ~~~~~~~~~~
 *
 * An as_vector_value is created via one of the as_vector_value_new_*()
 * functions and destroyed via as_vector_value_destroy().
 *
 * An as_vector_value is not part of the as_val family and cannot be used
 * directly as a bin value. To write a vector to a bin, serialize it into an
 * as_bytes instance tagged with AS_BYTES_VECTOR using
 * as_vector_value_to_bytes(), then pass that as_bytes to
 * as_record_set_bytes().
 */
typedef struct as_vector_value_s {
	/**
	 * Vector format version.
	 */
	uint8_t version;

	/**
	 * Vector element type.
	 */
	as_vector_element_type element_type;

	/**
	 * Number of dimensions (elements) in this vector.
	 */
	uint32_t dimensions;

	/**
	 * Contiguous array of elements. The element type/count must be
	 * interpreted using `element_type` and `dimensions`.
	 */
	void* data;

	/**
	 * If true, `data` is owned by this instance and will be freed when
	 * as_vector_value_destroy() is called.
	 */
	bool free_data;
} as_vector_value;

//---------------------------------
// Functions
//---------------------------------

/**
 * Create a heap allocated vector of raw float16 (IEEE 754 half precision)
 * elements. Since C has no native float16 type, each element is passed as
 * its raw 16-bit bit pattern. The data array is copied.
 *
 * @param data		Array of raw float16 bit patterns.
 * @param count		Number of elements in `data`.
 *
 * @relatesalso as_vector_value
 */
AS_EXTERN as_vector_value*
as_vector_value_new_float16(const uint16_t* data, uint32_t count);

/**
 * Create a heap allocated vector of int32 elements. The data array is
 * copied.
 *
 * @param data		Array of int32 elements.
 * @param count		Number of elements in `data`.
 *
 * @relatesalso as_vector_value
 */
AS_EXTERN as_vector_value*
as_vector_value_new_int32(const int32_t* data, uint32_t count);

/**
 * Create a heap allocated vector of float (fp32) elements. The data array is
 * copied.
 *
 * @param data		Array of float elements.
 * @param count		Number of elements in `data`.
 *
 * @relatesalso as_vector_value
 */
AS_EXTERN as_vector_value*
as_vector_value_new_float32(const float* data, uint32_t count);

/**
 * Create a heap allocated vector of double (fp64) elements. The data array
 * is copied.
 *
 * @param data		Array of double elements.
 * @param count		Number of elements in `data`.
 *
 * @relatesalso as_vector_value
 */
AS_EXTERN as_vector_value*
as_vector_value_new_float64(const double* data, uint32_t count);

/**
 * Destroy a heap allocated as_vector_value and release its resources.
 *
 * @relatesalso as_vector_value
 */
AS_EXTERN void
as_vector_value_destroy(as_vector_value* vec);

/**
 * Return the number of bytes used to encode a single element of the given
 * type.
 *
 * @relatesalso as_vector_value
 */
AS_EXTERN uint32_t
as_vector_element_type_size(as_vector_element_type type);

/**
 * Return the number of bytes needed to serialize this vector in wire format
 * (header plus element data).
 *
 * @relatesalso as_vector_value
 */
static inline uint32_t
as_vector_value_get_wire_size(const as_vector_value* vec)
{
	return AS_VECTOR_VALUE_HEADER_SIZE + (vec->dimensions * as_vector_element_type_size(vec->element_type));
}

/**
 * Serialize this vector into the wire format at the given buffer offset.
 * The caller must ensure `buffer` has at least
 * `offset + as_vector_value_get_wire_size(vec)` bytes available.
 *
 * @param vec		Vector to serialize.
 * @param buffer	Destination buffer.
 * @param offset	Offset into `buffer` to begin writing.
 *
 * @return Number of bytes written, equal to as_vector_value_get_wire_size(vec).
 *
 * @relatesalso as_vector_value
 */
AS_EXTERN uint32_t
as_vector_value_write(const as_vector_value* vec, uint8_t* buffer, uint32_t offset);

/**
 * Serialize this vector and wrap the result in the given as_bytes instance,
 * tagged with AS_BYTES_VECTOR. This is the bridge between an as_vector_value
 * and the generic as_val/as_bytes bin value machinery (analogous to how
 * HLL values are represented as an as_bytes tagged with AS_BYTES_HLL).
 *
 * The serialized buffer is heap allocated and owned by `bytes` (i.e.
 * `bytes->free` will be true), so the caller is not responsible for freeing
 * it. `bytes` should be released via as_bytes_destroy() when no longer
 * needed, independently of `vec`.
 *
 * ~~~~~~~~~~{.c}
 * as_vector_value* vec = as_vector_value_new_float32(data, 128);
 *
 * as_bytes bytes;
 * as_vector_value_to_bytes(vec, &bytes);
 * as_record_set_bytes(rec, "embedding", &bytes);
 *
 * as_vector_value_destroy(vec);
 * ~~~~~~~~~~
 *
 * @param vec		Vector to serialize.
 * @param bytes		Uninitialized as_bytes to initialize with the serialized vector.
 *
 * @return true on success.
 *
 * @relatesalso as_vector_value
 */
AS_EXTERN bool
as_vector_value_to_bytes(const as_vector_value* vec, as_bytes* bytes);

#ifdef __cplusplus
} // end extern "C"
#endif
