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
#include <aerospike/as_list.h>
#include <aerospike/as_map.h>

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

/**
 * Maximum size in bytes of a vector element array.
 */
#define AS_VECTOR_VALUE_MAX_ELEMENTS_BYTES (1u << 18)

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
 * Distance metric used to compare two vectors in a vector distance expression.
 * See as_exp_vector_dist().
 */
typedef enum as_vector_distance_metric_e {
	/**
	 * Squared Euclidean (L2) distance. Smaller values indicate closer vectors.
	 */
	AS_VECTOR_DISTANCE_EUCLIDEAN_SQUARED = 0,

	/**
	 * Dot product. Larger values indicate more similar vectors.
	 */
	AS_VECTOR_DISTANCE_DOT_PRODUCT = 1,

	/**
	 * Cosine similarity. Larger values indicate closer vectors.
	 */
	AS_VECTOR_DISTANCE_COSINE_SIMILARITY = 2
} as_vector_distance_metric;

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
 * Create with one of the as_vector_value_new_*() functions and destroy with
 * as_vector_value_destroy().
 *
 * as_vector_value is not an as_val and cannot be used directly as a bin
 * value. Convert to/from an as_bytes tagged AS_BYTES_VECTOR using
 * as_vector_value_to_bytes()/as_vector_value_to_bytes_new() and
 * as_vector_value_from_bytes().
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
 * @return New vector, or NULL if `data` is NULL or `count` is outside the
 *		   server's supported range.
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
 * @return New vector, or NULL if `data` is NULL or `count` is outside the
 *		   server's supported range.
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
 * @return New vector, or NULL if `data` is NULL or `count` is outside the
 *		   server's supported range.
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
 * @return New vector, or NULL if `data` is NULL or `count` is outside the
 *		   server's supported range.
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
 * Return the maximum number of elements permitted for the given type.
 *
 * @relatesalso as_vector_value
 */
static inline uint32_t
as_vector_element_type_max_dimensions(as_vector_element_type type)
{
	uint32_t size = as_vector_element_type_size(type);
	return size == 0 ? 0 : AS_VECTOR_VALUE_MAX_ELEMENTS_BYTES / size;
}

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
 * @return Number of bytes written, or 0 if `vec` is invalid.
 *
 * @relatesalso as_vector_value
 */
AS_EXTERN uint32_t
as_vector_value_write(const as_vector_value* vec, uint8_t* buffer, uint32_t offset);

/**
 * Serialize this vector and wrap the result in the given as_bytes instance,
 * tagged with AS_BYTES_VECTOR (analogous to how HLL values are represented
 * as an as_bytes tagged with AS_BYTES_HLL).
 *
 * The serialized buffer is heap allocated and owned by `bytes` (bytes->free
 * is true). Destroy `bytes` via as_bytes_destroy() independently of `vec`.
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
 * @return true on success, or false if `vec` is invalid or `bytes` is NULL.
 *
 * @relatesalso as_vector_value
 */
AS_EXTERN bool
as_vector_value_to_bytes(const as_vector_value* vec, as_bytes* bytes);

/**
 * Serialize this vector into a newly heap allocated as_bytes tagged with
 * AS_BYTES_VECTOR. Unlike as_vector_value_to_bytes(), the result is itself an
 * as_val, so it can be appended directly to a collection (as_arraylist,
 * as_hashmap, ...) to nest a vector in a list or map bin. A plain as_bytes
 * would default to AS_BYTES_BLOB and lose the vector tag.
 *
 * ~~~~~~~~~~{.c}
 * as_vector_value* vec = as_vector_value_new_float32(data, 128);
 *
 * as_arraylist list;
 * as_arraylist_init(&list, 1, 0);
 * as_arraylist_append(&list, (as_val*)as_vector_value_to_bytes_new(vec));
 *
 * as_vector_value_destroy(vec);
 * ~~~~~~~~~~
 *
 * @param vec		Vector to serialize.
 *
 * @return New as_bytes on success, or NULL on allocation failure. Destroy
 *		   with as_bytes_destroy(), or let the owning collection do so.
 *
 * @relatesalso as_vector_value
 */
AS_EXTERN as_bytes*
as_vector_value_to_bytes_new(const as_vector_value* vec);

/**
 * Serialize and append a vector to a list.
 *
 * The list owns the serialized value on success. `vec` remains caller-owned.
 *
 * @return true on success, or false if an argument is invalid, serialization
 *		   fails, or the list cannot append the value.
 *
 * @relatesalso as_vector_value
 */
AS_EXTERN bool
as_vector_value_append_to_list(as_list* list, const as_vector_value* vec);

/**
 * Serialize and set a vector value in a map.
 *
 * On success, ownership of `key` follows as_map_set(), and the map owns the
 * serialized vector value. `vec` remains caller-owned.
 *
 * @return true on success, or false if an argument is invalid, serialization
 *		   fails, or the map cannot set the value.
 *
 * @relatesalso as_vector_value
 */
AS_EXTERN bool
as_vector_value_set_in_map(
	as_map* map, const as_val* key, const as_vector_value* vec
	);

/**
 * Deserialize a vector from the wire format at the given buffer offset. The
 * returned as_vector_value owns its own copy of the element data, converted
 * from little-endian wire format to host byte order.
 *
 * @param buffer	Buffer containing the vector wire format.
 * @param offset	Offset into `buffer` where the vector starts.
 * @param length	Number of bytes available starting at `offset` (at least
 *					AS_VECTOR_VALUE_HEADER_SIZE).
 *
 * @return New as_vector_value on success, or NULL if `buffer` is malformed
 *		   (too short, unknown element type, or dimensions exceeding
 *		   `length`). Destroy with as_vector_value_destroy().
 *
 * @relatesalso as_vector_value
 */
AS_EXTERN as_vector_value*
as_vector_value_from(const uint8_t* buffer, uint32_t offset, uint32_t length);

/**
 * Deserialize a vector from an as_bytes instance, the inverse of
 * as_vector_value_to_bytes()/as_vector_value_to_bytes_new(). Typically used
 * on an as_bytes read from a record bin or collection element (both are
 * tagged AS_BYTES_VECTOR by the client).
 *
 * ~~~~~~~~~~{.c}
 * as_bytes* bytes = as_record_get_bytes(rec, "embedding");
 * as_vector_value* vec = as_vector_value_from_bytes(bytes);
 * // ... use vec ...
 * as_vector_value_destroy(vec);
 * ~~~~~~~~~~
 *
 * @param bytes		Serialized vector.
 *
 * @return New as_vector_value on success, or NULL if `bytes` is NULL or does
 *		   not contain a valid vector. Destroy with as_vector_value_destroy().
 *
 * @relatesalso as_vector_value
 */
AS_EXTERN as_vector_value*
as_vector_value_from_bytes(const as_bytes* bytes);

/**
 * Return the vector's data as a float16 array, or NULL if element_type is not
 * AS_VECTOR_ELEMENT_TYPE_FLOAT16. Has `vec->dimensions` elements, owned by `vec`.
 *
 * @relatesalso as_vector_value
 */
static inline const uint16_t*
as_vector_value_get_float16(const as_vector_value* vec)
{
	return (vec && vec->element_type == AS_VECTOR_ELEMENT_TYPE_FLOAT16) ? (const uint16_t*)vec->data : NULL;
}

/**
 * Return the vector's data as an int32 array, or NULL if element_type is not
 * AS_VECTOR_ELEMENT_TYPE_INT32. Has `vec->dimensions` elements, owned by `vec`.
 *
 * @relatesalso as_vector_value
 */
static inline const int32_t*
as_vector_value_get_int32(const as_vector_value* vec)
{
	return (vec && vec->element_type == AS_VECTOR_ELEMENT_TYPE_INT32) ? (const int32_t*)vec->data : NULL;
}

/**
 * Return the vector's data as a float (fp32) array, or NULL if element_type
 * is not AS_VECTOR_ELEMENT_TYPE_FLOAT32. Has `vec->dimensions` elements, owned by `vec`.
 *
 * @relatesalso as_vector_value
 */
static inline const float*
as_vector_value_get_float32(const as_vector_value* vec)
{
	return (vec && vec->element_type == AS_VECTOR_ELEMENT_TYPE_FLOAT32) ? (const float*)vec->data : NULL;
}

/**
 * Return the vector's data as a double (fp64) array, or NULL if element_type
 * is not AS_VECTOR_ELEMENT_TYPE_FLOAT64. Has `vec->dimensions` elements, owned by `vec`.
 *
 * @relatesalso as_vector_value
 */
static inline const double*
as_vector_value_get_float64(const as_vector_value* vec)
{
	return (vec && vec->element_type == AS_VECTOR_ELEMENT_TYPE_FLOAT64) ? (const double*)vec->data : NULL;
}

/**
 * Return a pointer to the vector's contiguous element array (without the 8-byte
 * header) and its size in bytes. This is the query-vector layout expected by a
 * vector distance expression (as_exp_vector_dist()): the server reinterprets the
 * bytes using the stored bin's element type.
 *
 * The returned pointer is owned by `vec` and valid for its lifetime. The bytes
 * are in the vector's little-endian wire order on little-endian hosts (the only
 * platforms the vector format supports).
 *
 * WORK IN PROGRESS: used only by the not-yet-shipped vector distance expression.
 *
 * @param vec		Vector to inspect.
 * @param size		Set to the element-array size in bytes.
 *
 * @return Pointer to the element bytes, or NULL if `vec` is NULL.
 *
 * @relatesalso as_vector_value
 */
static inline const uint8_t*
as_vector_value_element_bytes(const as_vector_value* vec, uint32_t* size)
{
	if (vec == NULL) {
		if (size) {
			*size = 0;
		}
		return NULL;
	}

	if (size) {
		*size = vec->dimensions * as_vector_element_type_size(vec->element_type);
	}
	return (const uint8_t*)vec->data;
}

#ifdef __cplusplus
} // end extern "C"
#endif
