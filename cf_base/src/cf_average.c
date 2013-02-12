/*
 * A general purpose hashtable implementation
 * Good at multithreading
 * Just, hopefully, the last reasonable hash table you'll ever need
 * Copywrite 2008 Brian Bulkowski
 * All rights reserved
 */

#include <stdint.h>
#include <stdlib.h>

#include "citrusleaf/cf_base_types.h"

#include "citrusleaf/cf_average.h"


cf_average *
cf_average_create(uint32_t initial_size, uint32_t flags)
{
	cf_average *a;

	a = (cf_average*)malloc(sizeof(cf_average) + (sizeof(uint64_t) * initial_size) );
	if (!a)	return(0);
	
	a->flags = flags;
	a->n_points = 0;
	a->points_sum = 0;

	return(a);
}

void
cf_average_destroy( cf_average *a )
{
	free(a);
	return;
}

void
cf_average_clear(cf_average *avg)
{

	avg->n_points = 0;
	avg->points_sum = 0;

}


// eaiser threadsafe version which won't autogrow

int
cf_average_add(cf_average *a, uint64_t value)
{

	int rv = 0;
	
	a->points_sum += value;
	a->n_points++;
	
	return(rv);
	
}



double
cf_average_calculate(cf_average *a, bool clear)
{

	
	if (a->n_points == 0) {
		return(0.0);
	}
	
	double avg = ((double)a->points_sum) / ((double)a->n_points);

	if (clear) 	{
		a->n_points = 0;
		a->points_sum = 0;
	}
	
	return(avg);
}


