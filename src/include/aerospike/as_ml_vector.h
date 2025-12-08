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
#pragma once

/**
 * @defgroup ml_vector_operations ML Vector Operations
 * @ingroup client_operations
 *
 * The Aerospike ML Vector Operations provide the ability to work with mathematical
 * vectors for machine learning and similarity search operations.
 */

#include <aerospike/as_bytes.h>
#include <aerospike/as_error.h>
#include <aerospike/as_status.h>
#include <aerospike/as_vector.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

//---------------------------------
// Constants
//---------------------------------

/**
 * Vector blob format magic number "VECT"
 */
#define AS_ML_VECTOR_MAGIC_NUMBER 0x56454354

/**
 * Current vector blob format version
 */
#define AS_ML_VECTOR_VERSION 0x00000001

//---------------------------------
// Types
//---------------------------------

/**
 * Mathematical vector element data types for ML/similarity operations
 */
typedef enum as_ml_vector_element_type_e {
	AS_ML_VECTOR_FLOAT32 = 1,
	AS_ML_VECTOR_FLOAT64 = 2,
	AS_ML_VECTOR_INT32 = 3,
	AS_ML_VECTOR_INT64 = 4
} as_ml_vector_element_type;

//---------------------------------
// Functions
//---------------------------------

/**
 * Initialize an as_vector with float32 data for ML operations
 *
 * @param vector		The as_vector to initialize
 * @param data			Array of float32 values
 * @param count			Number of elements in the array
 *
 * @return AEROSPIKE_OK on success, otherwise an error
 * @ingroup ml_vector_operations
 */
AS_EXTERN as_status
as_ml_vector_init_float32(as_vector* vector, float* data, uint32_t count);

/**
 * Initialize an as_vector with float64 data for ML operations
 *
 * @param vector		The as_vector to initialize
 * @param data			Array of float64 values
 * @param count			Number of elements in the array
 *
 * @return AEROSPIKE_OK on success, otherwise an error
 * @ingroup ml_vector_operations
 */
AS_EXTERN as_status
as_ml_vector_init_float64(as_vector* vector, double* data, uint32_t count);

/**
 * Initialize an as_vector with int32 data for ML operations
 *
 * @param vector		The as_vector to initialize
 * @param data			Array of int32 values
 * @param count			Number of elements in the array
 *
 * @return AEROSPIKE_OK on success, otherwise an error
 * @ingroup ml_vector_operations
 */
AS_EXTERN as_status
as_ml_vector_init_int32(as_vector* vector, int32_t* data, uint32_t count);

/**
 * Initialize an as_vector with int64 data for ML operations
 *
 * @param vector		The as_vector to initialize
 * @param data			Array of int64 values
 * @param count			Number of elements in the array
 *
 * @return AEROSPIKE_OK on success, otherwise an error
 * @ingroup ml_vector_operations
 */
AS_EXTERN as_status
as_ml_vector_init_int64(as_vector* vector, int64_t* data, uint32_t count);

/**
 * Serialize an ML vector into the Aerospike vector blob format
 *
 * Vector Blob Format:
 * [4 bytes] - Magic number (0x56454354 = "VECT")
 * [4 bytes] - Version (0x00000001)
 * [4 bytes] - Element count (number of vector elements)
 * [4 bytes] - Element type (1=float32, 2=float64, 3=int32, 4=int64)
 * [N bytes] - Vector data (elements in native byte order)
 *
 * @param vector		The as_vector containing ML vector data
 * @param element_type	The type of elements in the vector
 * @param bytes			Output bytes object to hold serialized data
 *
 * @return AEROSPIKE_OK on success, otherwise an error
 * @ingroup ml_vector_operations
 */
AS_EXTERN as_status
as_ml_vector_serialize(const as_vector* vector, as_ml_vector_element_type element_type, as_bytes* bytes);

/**
 * Deserialize an ML vector from the Aerospike vector blob format
 *
 * @param bytes			Bytes object containing serialized vector data
 * @param vector		Output as_vector to hold deserialized data
 * @param element_type	Output parameter for the element type
 *
 * @return AEROSPIKE_OK on success, otherwise an error
 * @ingroup ml_vector_operations
 */
AS_EXTERN as_status
as_ml_vector_deserialize(const as_bytes* bytes, as_vector* vector, as_ml_vector_element_type* element_type);

/**
 * Get the size in bytes required for a vector element based on type
 *
 * @param element_type	The vector element type
 *
 * @return Size in bytes, or 0 if type is invalid
 * @ingroup ml_vector_operations
 */
AS_EXTERN uint32_t
as_ml_vector_element_size(as_ml_vector_element_type element_type);

#ifdef __cplusplus
} // end extern "C"
#endif
