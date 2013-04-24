/*
 *  Citrusleaf Foundation
 *  src/timer.c - timer functionality
 *
 *  Copyright 2008 by Citrusleaf.  All rights reserved.
 *  THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE.  THE COPYRIGHT NOTICE
 *  ABOVE DOES NOT EVIDENCE ANY ACTUAL OR INTENDED PUBLICATION.
 */

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_log_internal.h"

#include "citrusleaf/cf_hist.h"


int
cf_bits_find_last_set(uint32_t v)
{

	int r;
	uint32_t t, tt;
	
	if ((tt = v >> 16))
		r = (t = tt >> 8) ? (24 + cf_LogTable256[t]) : (16 + cf_LogTable256[tt]);
	else
		r = (t = v >> 8) ? (8 + cf_LogTable256[t]) : cf_LogTable256[v];
	return (r);
}

int
cf_bits_find_last_set_64(uint64_t v)
{
	uint64_t t;
	if ((t = v >> 32))
		return( cf_bits_find_last_set((uint32_t)t) + 32 );
	else
		return( cf_bits_find_last_set((uint32_t)v) );
}


cf_histogram * 
cf_histogram_create(char *name)
{
	cf_histogram * h = (cf_histogram*)malloc(sizeof(cf_histogram));
	if (!h)	return(0);
	if (strlen(name) >= sizeof(h->name)-1) { free(h); return(0); }
	strcpy(h->name, name);
	h->n_counts = 0;
	memset((void*)&h->count, 0, sizeof(h->count));
	return(h);
}

////////////////////////////////////////////////////////////////////////

/**
 * This is the new body of "cf_histogram_dump()"
 * If caller provides the outbuff, output will be place there. If not, output
 * goes to cf_debug.  If the caller provides the outbuff, it is the callers
 * responsibility to ensure it is large enough to hold the output
 */
void 
cf_histogram_dump_new( cf_histogram *h, char *outbuff, size_t outbuff_len)
{
  char printbuf[256];
  int pos = 0; // location to print from
  printbuf[0] = '\0';

  sprintf(printbuf, "histogram dump: %s (%zu total)",h->name, h->n_counts);
  if (outbuff) {
    strncat(outbuff, printbuf, outbuff_len - strlen(outbuff) - 1);
            strncat(outbuff, "  |", outbuff_len - strlen(outbuff) - 1);
  } else {
    cf_debug("%s", printbuf);
  }

  int i, j;
  int k = 0;
  for (j=CF_N_HIST_COUNTS-1 ; j >= 0 ; j-- ) if (h->count[j]) break;
  for (i=0;i<CF_N_HIST_COUNTS;i++) if (h->count[i]) break;
  for (; i<=j;i++) {
    if (h->count[i] > 0) { // print only non zero columns
      int bytes =
        sprintf((char *) (printbuf + pos), " (%02d: %010zu) ", i, h->count[i]);
      if (bytes <= 0) {
        cf_debug("histogram printing error. (bytes < 0 ) Bailing ...");
        return;
      }
      pos += bytes;
      if (k % 4 == 3){
        if (outbuff) {
          strncat(outbuff, printbuf, outbuff_len - strlen(outbuff) - 1);
          strncat(outbuff, "   ", outbuff_len - strlen(outbuff) - 1);
        } else {
          cf_debug("%s", (char *) printbuf);
        }
        pos = 0;
        printbuf[0] = '\0';
      }
      k++;
    }
  } // end for each
  if (pos > 0) {
    if (outbuff) {
      strncat(outbuff, printbuf, outbuff_len - strlen(outbuff) - 1);
    } else {
      cf_debug("%s", (char *) printbuf);
    }
  } // end if pos > 0
} // end cf_histogram_dump_new()

/**
 *  This is now redefined to call "cf_histogram_dump_new()", which allows
 *  us to pass in a buffer to be filled in.  The supplied buffer version
 *  is needed by the Erlang Client.
 */
void
cf_histogram_dump( cf_histogram *h )
{
    cf_histogram_dump_new( h, NULL, 0);
} // end histogram_dump()

////////////////////////////////////////////////////////////////////////

void 
cf_histogram_insert_data_point( cf_histogram *h, uint64_t start)
{
    cf_atomic_int_incr(&h->n_counts);
	
    uint64_t end = cf_getms(); 
    uint64_t delta = end - start;
	
	int index = cf_bits_find_last_set_64(delta);
	if (index < 0) index = 0;   
	if (start > end)
	{
	    // Need to investigate why in some cases start is a couple of ms greater than end
		// Could it be rounding error (usually the difference is 1 but sometimes I have seen 2
		index = 0;
	}   
       
	cf_atomic_int_incr( &h->count[ index ] );
	
}

void 
cf_histogram_get_counts(cf_histogram *h, cf_histogram_counts *hc)
{
	for (int i=0;i<CF_N_HIST_COUNTS;i++)
		hc->count[i] = h->count[i];
	return;
}


