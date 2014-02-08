/******************************************************************************
 * Copyright 2008-2013 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy 
 * of this software and associated documentation files (the "Software"), to 
 * deal in the Software without restriction, including without limitation the 
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or 
 * sell copies of the Software, and to permit persons to whom the Software is 
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in 
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING 
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 *****************************************************************************/

#include <stdint.h>
#include <stdlib.h>

#include <citrusleaf/cf_types.h>
#include <citrusleaf/cf_average.h>


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


