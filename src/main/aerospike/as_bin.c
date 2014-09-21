/*
 * Copyright 2008-2014 Aerospike, Inc.
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
#include <aerospike/as_bin.h>
#include <aerospike/as_integer.h>
#include <aerospike/as_string.h>
#include <aerospike/as_bytes.h>
#include <aerospike/as_list.h>
#include <aerospike/as_map.h>
#include <aerospike/as_val.h>

/******************************************************************************
 *	INLINE FUNCTIONS
 *****************************************************************************/

/**
 *	Get the name of the bin.
 *
 *	~~~~~~~~~~{.c}
 *	char * name = as_bin_get_name(bin);
 *	~~~~~~~~~~
 *
 *
 *	@parameter bin 	The bin to get the name of.
 *
 *	@return The name of the bin.
 *
 *	@relates as_bin
 *	@ingroup as_record_t
 */
extern inline char * as_bin_get_name(const as_bin * bin);

/**
 *	Get the value of the bin.
 *
 *	~~~~~~~~~~{.c}
 *	as_bin_value * val = as_bin_get_value(bin);
 *	~~~~~~~~~~
 *
 *
 *	@parameter bin 	The bin to get the value of.
 *
 *	@return The value of the bin. If NULL is returned, then the bin did not contain a value.
 *
 *	@relates as_bin
 *	@ingroup as_record_t
 */
extern inline as_bin_value * as_bin_get_value(const as_bin * bin);

/**
 *	Get the type for the value of the bin.
 *
 *	~~~~~~~~~~{.c}
 *	as_val_t type = as_bin_get_type(bin);
 *	~~~~~~~~~~
 *
 *
 *	@parameter bin 	The bin inquire.
 *
 *	@return The type of the bin's value. If AS_VAL_UNDEF is returned, then the bin did not contain a value.
 *
 *	@relates as_bin
 *	@ingroup as_record_t
 */
extern inline as_val_t as_bin_get_type(const as_bin * bin);
