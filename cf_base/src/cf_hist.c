/*
 *  Citrusleaf Foundation
 *  src/timer.c - timer functionality
 *
 *  Copyright 2008 by Citrusleaf.  All rights reserved.
 *  THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE.  THE COPYRIGHT NOTICE
 *  ABOVE DOES NOT EVIDENCE ANY ACTUAL OR INTENDED PUBLICATION.
 */
#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <inttypes.h>
#include <unistd.h>
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_hist.h"


int
bits_find_last_set(uint32_t v)
{

	int r;
	uint32_t t, tt;
	
	if ((tt = v >> 16))
		r = (t = tt >> 8) ? (24 + LogTable256[t]) : (16 + LogTable256[tt]);
	else
		r = (t = v >> 8) ? (8 + LogTable256[t]) : LogTable256[v];
	return (r);
}

int
bits_find_last_set_64(uint64_t v) 
{
	uint64_t t;
	if ((t = v >> 32))
		return( bits_find_last_set(t) + 32 );
	else
		return( bits_find_last_set(v) );
}


cf_histogram * 
cf_histogram_create(char *name)
{
	cf_histogram * h = malloc(sizeof(cf_histogram));
	if (!h)	return(0);
	if (strlen(name) >= sizeof(h->name)-1) { free(h); return(0); }
	strcpy(h->name, name);
	h->n_counts = 0;
	memset(&h->count, 0, sizeof(h->count));
	return(h);
}

void cf_histogram_dump( cf_histogram *h )
{
	char printbuf[100];
	int pos = 0; // location to print from
	printbuf[0] = '\0';
	
	fprintf(stderr, "histogram dump: %s (%zu total)\n",h->name, h->n_counts);
	int i, j;
	int k = 0;
	for (j=N_COUNTS-1 ; j >= 0 ; j-- ) if (h->count[j]) break;
	for (i=0;i<N_COUNTS;i++) if (h->count[i]) break;
	for (; i<=j;i++) {
		if (h->count[i] > 0) { // print only non zero columns
			int bytes = sprintf((char *) (printbuf + pos), " (%02d: %010zu) ", i, h->count[i]);
			if (bytes <= 0) 
			{
				fprintf(stderr, "histogram printing error. Bailing ...");
				return;
			}
			pos += bytes;
		    if (k % 4 == 3){
		    	 fprintf(stderr, "%s\n", (char *) printbuf);
		    	 pos = 0;
		    	 printbuf[0] = '\0';
		    }
		    k++;
		}
	}
	if (pos > 0) 
	    fprintf(stderr, "%s\n", (char *) printbuf);
}

void cf_histogram_insert_data_point( cf_histogram *h, uint64_t start)
{
    cf_atomic_int_incr(&h->n_counts);
	
    uint64_t end = cf_getms(); 
    uint64_t delta = end - start;
	
	int index = bits_find_last_set_64(delta);
	if (index < 0) index = 0;   
	if (start > end)
	{
	    // Need to investigate why in some cases start is a couple of ms greater than end
		// Could it be rounding error (usually the difference is 1 but sometimes I have seen 2
	    // cf_info(AS_INFO, "start = %"PRIu64" > end = %"PRIu64"", start, end);
		index = 0;
	}   
       
	cf_atomic_int_incr( &h->count[ index ] );
	
}

void cf_histogram_get_counts(cf_histogram *h, cf_histogram_counts *hc)
{
	for (int i=0;i<N_COUNTS;i++)
		hc->count[i] = h->count[i];
	return;
}


