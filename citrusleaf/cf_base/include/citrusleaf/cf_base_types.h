/*
 * Copyright 2013 Aerospike. All rights reserved.
 */

#pragma once

#ifndef CF_WINDOWS
//====================================================================
// Linux
//

// A real pity that Linux requires this for bool, true & false:
#include <stdbool.h>

// Use alloca() instead of variable-sized stack arrays for non-gcc portability.
#include <alloca.h>


#else // CF_WINDOWS
//====================================================================
// Windows
//

// For alloca():
#include <malloc.h>


#endif // CF_WINDOWS
