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

#include <aerospike/as_ldt.h>
#include <aerospike/as_key.h>
#include <aerospike/as_udf.h>

#include <stdlib.h>
#include <stdio.h>

static char * DEFAULT_LSTACK_PACKAGE = "lstack";
static char * DEFAULT_LSET_PACKAGE = "lset";
static char * DEFAULT_LLIST_PACKAGE = "llist";
static char * DEFAULT_LMAP_PACKAGE = "lmap";


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
	as_ldt * ldt = (as_ldt *) malloc(sizeof(as_ldt));
	if ( ldt ) {
		if (!as_ldt_init(ldt, name, type, module)) {
			free (ldt);
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
    if (!module || *module=='\0') {
    	switch (type) {
			case AS_LDT_LSTACK:
				module = DEFAULT_LSTACK_PACKAGE;
				break;
			case AS_LDT_LSET:
				module = DEFAULT_LSET_PACKAGE;
				break;
			case AS_LDT_LLIST:
				module = DEFAULT_LLIST_PACKAGE;
				break;
			case AS_LDT_LMAP:
				module = DEFAULT_LMAP_PACKAGE;
				break;
			default:
				return NULL;
		}
    }
	if (!(name && *name != '\0' && strlen(name) < AS_BIN_NAME_MAX_SIZE
		 && (module && strlen(module) < AS_UDF_MODULE_MAX_SIZE) ))
	{
		return NULL;
	}
	if (ldt) {
		ldt->_free = false;
		ldt->type = type;
		strcpy(ldt->name,name);
		strcpy(ldt->module,module);
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
		free (ldt);
	}
}

