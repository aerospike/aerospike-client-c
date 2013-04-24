/*
 * A general purpose vector
 * Uses locks, so only moderately fast
 * If you need to deal with sparse data, really sparse data,
 * use a hash table. This assumes that packed data is a good idea.
 * Does the fairly trivial realloc thing for extension,
 * so 
 * And you can keep adding cool things to it
 * Copywrite 2008 Brian Bulkowski
 * All rights reserved
 */

#pragma once
 
#include <stdint.h>

#include "citrusleaf/cf_base_types.h"


#ifdef __cplusplus
extern "C" {
#endif


typedef struct cf_average_s {
	int			flags;
	uint32_t	n_points;
	uint64_t	points_sum;
} cf_average;



cf_average *cf_average_create(uint32_t initial_size, uint32_t flags);
void cf_average_destroy( cf_average *avg);
void cf_average_clear(cf_average *avg);
int cf_average_add(cf_average *avgp, uint64_t value);   // warning! this fails if too many samples
double cf_average_calculate(cf_average *avg, bool clear);

// maybe it would be nice to have a floating point version?



#ifdef __cplusplus
} // end extern "C"
#endif



