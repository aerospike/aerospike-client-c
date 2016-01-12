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
#pragma once 

#include <aerospike/as_bin.h>
#include <aerospike/as_map.h>
#include <aerospike/as_udf.h>

#ifdef __cplusplus
extern "C" {
#endif

/******************************************************************************
 *	CONSTANTS
 *****************************************************************************/
/**
 *	Enumeration of Large Data Types
 */
typedef enum as_ldt_type_e {

	AS_LDT_LLIST,
	AS_LDT_LMAP,
	AS_LDT_LSET,
	AS_LDT_LSTACK

} as_ldt_type;


/******************************************************************************
 *	TYPES
 *****************************************************************************/

/**
 *	Represents a bin containing an LDT value. 
 *
 *	@ingroup client_objects
 */
typedef struct as_ldt_s {

	/**
	 *	@private
	 *	If true, then as_ldt_destroy() will free this instance.
	 */
	bool _free;

	/**
	 *	Bin name.
	 */
	as_bin_name name;

	/**
	 *	LDT Type.
	 */
	as_ldt_type type;

	/**
	 *	LDT UDF Module
	 */
	as_udf_module_name module;
	
} as_ldt;

/******************************************************************************
 *	FUNCTIONS
 *****************************************************************************/

/**
 *	Creates and initializes a heap allocated as_ldt.
 *
 *	~~~~~~~~~~{.c}
 *	as_ldt * ldt = as_ldt_new("mystack", AS_LDT_LSTACK, NULL);
 *	~~~~~~~~~~
 *
 *	Use as_ldt_destroy() to release resources allocated to as_ldt via
 *	this function.
 *	
 *	@param name		The name of the bin to contain the ldt.
 *	@param type		The type of ldt data to store in the bin.
 *	@param module	The name of ldt customization module to use for this initialization.
 *
 *	@return The initialized as_key on success. Otherwise NULL.
 *
 *	@relates as_ldt
 *	@ingroup as_ldt_object
 */
as_ldt * as_ldt_new(const as_bin_name name, const as_ldt_type type, const as_udf_module_name module);


/**
 *	Initialize a stack allocated as_ldt.
 *
 *	~~~~~~~~~~{.c}
 *	as_ldt ldt;
 *	as_ldt_init(&ldt, "mystack", AS_LDT_LSTACK, NULL);
 *	~~~~~~~~~~
 *
 *	Use as_ldt_destroy() to release resources allocated to as_ldt via
 *	this function.
 *	
 *	@param ldt		The ldt to initialize.
 *	@param name		The name of the bin to contain the ldt.
 *	@param type		The type of ldt data to store in the bin.
 *	@param module	The name of ldt customization module to use for this initialization.
 *
 *	@return The initialized as_ldt on success. Otherwise NULL.
 *
 *	@relates as_ldt
 *	@ingroup as_ldt_object
 */
as_ldt * as_ldt_init(as_ldt * ldt, const as_bin_name name, const as_ldt_type type, const as_udf_module_name module);

/**
 *	Destroy the as_ldt, releasing resources.
 *
 *	~~~~~~~~~~{.c}
 *	as_ldt_destroy(ldt);
 *	~~~~~~~~~~
 *
 *	@param ldt The as_ldt to destroy.
 *
 *	@relates as_ldt
 *	@ingroup as_ldt_object
 */
void as_ldt_destroy(as_ldt * ldt);

#ifdef __cplusplus
} // end extern "C"
#endif
