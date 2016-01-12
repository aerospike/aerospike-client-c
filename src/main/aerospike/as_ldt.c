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
#include <aerospike/as_ldt.h>
#include <aerospike/as_key.h>
#include <aerospike/as_udf.h>

#include <citrusleaf/alloc.h>

#include <stdlib.h>
#include <stdio.h>


/******************************************************************************
 *	STATIC FUNCTIONS
 *****************************************************************************/


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
as_ldt * as_ldt_new(const as_bin_name name, const as_ldt_type type, const as_udf_module_name module)
{
	as_ldt * ldt = (as_ldt *) cf_malloc(sizeof(as_ldt));
	if ( ldt ) {
		if (!as_ldt_init(ldt, name, type, module)) {
			cf_free(ldt);
			return NULL;
		}
		ldt->_free = true;
	}

	return ldt;
}


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
as_ldt * as_ldt_init(as_ldt * ldt, const as_bin_name name, const as_ldt_type type, const as_udf_module_name module)
{
	if (!name || name[0] == '\0' || strlen(name) > AS_BIN_NAME_MAX_LEN
		 || (module && strlen(module) > AS_UDF_MODULE_MAX_LEN) )
	{
		return NULL;
	}
	if (ldt) {
		ldt->_free = false;
		ldt->type = type;
		strcpy(ldt->name,name);
		if (!module || *module=='\0') {
			ldt->module[0] = '\0';
		} else {
			strcpy(ldt->module,module);
		}
	}
	return ldt;
}

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
void as_ldt_destroy(as_ldt * ldt)
{
	if (ldt && ldt->_free) {
		cf_free(ldt);
	}
}

