/******************************************************************************
 *	Copyright 2008-2013 by Aerospike.
 *
 *	Permission is hereby granted, free of charge, to any person obtaining a copy 
 *	of this software and associated documentation files (the "Software"), to 
 *	deal in the Software without restriction, including without limitation the 
 *	rights to use, copy, modify, merge, publish, distribute, sublicense, and/or 
 *	sell copies of the Software, and to permit persons to whom the Software is 
 *	furnished to do so, subject to the following conditions:
 *	
 *	The above copyright notice and this permission notice shall be included in 
 *	all copies or substantial portions of the Software.
 *	
 *	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING 
 *	FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 *	IN THE SOFTWARE.
 *****************************************************************************/

#pragma once 

#include <aerospike/as_bin.h>
#include <aerospike/as_map.h>

/******************************************************************************
 *	CONSTANTS
 *****************************************************************************/

#define AS_LDT_LLIST "llist"
#define AS_LDT_LMAP "lmap"
#define AS_LDT_LSET "lset"
#define AS_LDT_LSTACK "lstack"

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
	 *	Bin name.
	 */
	as_bin_name name;

	/**
	 *	LDT Type.
	 */
	const char * type;

	/**
	 *	LDT Module
	 */
	const char * module;
	
} as_ldt;

/******************************************************************************
 *	FUNCTIONS
 *****************************************************************************/

/**
 *	Creates and initializes a heap allocated as_ldt.
 *
 *	~~~~~~~~~~{.c}
 *	as_ldt * ldt = as_ldt_new("stack", "lstack", NULL);
 *	~~~~~~~~~~
 *
 *	Use as_ldt_destroy() to release resources allocated to as_ldt via
 *	this function.
 *	
 *	@param name		The name of the bin to contain the ldt.
 *	@param type		The type of ldt bin.
 *	@param module		The LDT module for this bin.
 *
 *	@return The initialized as_key on success. Otherwise NULL.
 *
 *	@relates as_ldt
 *	@ingroup as_ldt_object
 */
as_ldt * as_ldt_new(as_bin_name name, const char * type, const char * module);


/**
 *	Initialize a stack allocated as_ldt.
 *
 *	~~~~~~~~~~{.c}
 *	as_ldt ldt;
 *	as_ldt_init(&ldt, "stack", "lstack", NULL);
 *	~~~~~~~~~~
 *
 *	Use as_ldt_destroy() to release resources allocated to as_ldt via
 *	this function.
 *	
 *	@param ldt		The ldt to initialize.
 *	@param name		The name of the bin to contain the ldt.
 *	@param type		The type of ldt bin.
 *	@param module		The LDT module for this bin.
 *
 *	@return The initialized as_key on success. Otherwise NULL.
 *
 *	@relates as_ldt
 *	@ingroup as_ldt_object
 */
as_ldt * as_ldt_init(as_ldt * ldt, as_bin_name name, const char * type, as_map * module);

/**
 *	Destory the as_ldt, releasing resources.
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
