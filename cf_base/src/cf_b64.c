/*
 *  Citrusleaf Tools
 *  b64.c - a general purpose base64 encoder and decoder that's C flavored
 *
 *  Copyright 2009 by Citrusleaf.  All rights reserved.
 *  THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE.  THE COPYRIGHT NOTICE
 *  ABOVE DOES NOT EVIDENCE ANY ACTUAL OR INTENDED PUBLICATION.
 */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdint.h>
#include <stdbool.h>

#include <citrusleaf/cf_b64.h>

/*
** Base 64 Encoding
**
*/

static const uint8_t base64_bytes[] = { 
	'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N',
	'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', 
	'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n',
	'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
	'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '+', '/'
};

static const char base64_chars[] = { 
	'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N',
	'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', 
	'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n',
	'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
	'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '+', '/'
};


static const bool base64_valid_a[256] = {
	    /*00*/ /*01*/ /*02*/ /*03*/ /*04*/ /*05*/ /*06*/ /*07*/   /*08*/ /*09*/ /*0A*/ /*0B*/ /*0C*/ /*0D*/ /*0E*/ /*0F*/
/*00*/	false, false, false, false, false, false, false, false,   false, false, false, false, false, false, false, false,
/*10*/  false, false, false, false, false, false, false, false,   false, false, false, false, false, false, false, false,
/*20*/	false, false, false, false, false, false, false, false,   false, false, false,  true, false, false, false,  true,
/*30*/	 true,  true,  true,  true,  true,  true,  true,  true,    true,  true, false, false, false, false, false, false,
/*40*/	false,  true,  true,  true,  true,  true,  true,  true,    true,  true,  true,  true,  true,  true,  true,  true,
/*50*/	 true,  true,  true,  true,  true,  true,  true,  true,    true,  true,  true, false, false, false, false, false,
/*60*/	false,  true,  true,  true,  true,  true,  true,  true,    true,  true,  true,  true,  true,  true,  true,  true,
/*70*/	 true,  true,  true,  true,  true,  true,  true,  true,    true,  true,  true, false, false, false, false, false,
/*80*/	false, false, false, false, false, false, false, false,   false, false, false, false, false, false, false, false,
/*90*/	false, false, false, false, false, false, false, false,   false, false, false, false, false, false, false, false,
/*A0*/	false, false, false, false, false, false, false, false,   false, false, false, false, false, false, false, false,
/*B0*/	false, false, false, false, false, false, false, false,   false, false, false, false, false, false, false, false,
/*C0*/	false, false, false, false, false, false, false, false,   false, false, false, false, false, false, false, false,
/*D0*/	false, false, false, false, false, false, false, false,   false, false, false, false, false, false, false, false,
/*E0*/	false, false, false, false, false, false, false, false,   false, false, false, false, false, false, false, false,
/*F0*/	false, false, false, false, false, false, false, false,   false, false, false, false, false, false, false, false
};

static const uint8_t base64_decode_a[256] = {
	    /*00*/ /*01*/ /*02*/ /*03*/ /*04*/ /*05*/ /*06*/ /*07*/   /*08*/ /*09*/ /*0A*/ /*0B*/ /*0C*/ /*0D*/ /*0E*/ /*0F*/
/*00*/	    0,     0,     0,     0,     0,     0,     0,     0,       0,     0,     0,     0,     0,     0,     0,     0,
/*10*/      0,     0,     0,     0,     0,     0,     0,     0,       0,     0,     0,     0,     0,     0,     0,     0,
/*20*/	    0,     0,     0,     0,     0,     0,     0,     0,       0,     0,     0,    62,     0,     0,     0,    63,
/*30*/	   52,    53,    54,    55,    56,    57,    58,    59,      60,    61,     0,     0,     0,     0,     0,     0,
/*40*/	    0,     0,     1,     2,     3,     4,     5,     6,       7,     8,     9,    10,    11,    12,    13,    14,
/*50*/	   15,    16,    17,    18,    19,    20,    21,    22,      23,    24,    25,     0,     0,     0,     0,     0,
/*60*/	    0,    26,    27,    28,    29,    30,    31,    32,      33,    34,    35,    36,    37,    38,    39,    40,
/*70*/	   41,    42,    43,    44,    45,    46,    47,    48,      49,    50,    51,     0,     0,     0,     0,     0,
/*80*/	    0,     0,     0,     0,     0,     0,     0,     0,       0,     0,     0,     0,     0,     0,     0,     0,
/*90*/	    0,     0,     0,     0,     0,     0,     0,     0,       0,     0,     0,     0,     0,     0,     0,     0,
/*A0*/	    0,     0,     0,     0,     0,     0,     0,     0,       0,     0,     0,     0,     0,     0,     0,     0,
/*B0*/	    0,     0,     0,     0,     0,     0,     0,     0,       0,     0,     0,     0,     0,     0,     0,     0,
/*C0*/	    0,     0,     0,     0,     0,     0,     0,     0,       0,     0,     0,     0,     0,     0,     0,     0,
/*D0*/	    0,     0,     0,     0,     0,     0,     0,     0,       0,     0,     0,     0,     0,     0,     0,     0,
/*E0*/	    0,     0,     0,     0,     0,     0,     0,     0,       0,     0,     0,     0,     0,     0,     0,     0,
/*F0*/	    0,     0,     0,     0,     0,     0,     0,     0,       0,     0,     0,     0,     0,     0,     0,     0
};


static inline bool base64_valid_char(uint8_t c)
{
	return(base64_valid_a[c]);
}

bool
cf_base64_validate_input(const uint8_t *b, const int len)
{
	// length must be 4-aligned
	if (len % 4 != 0) {
		fprintf(stderr, "base64 validate input: bad length %d must be 4 length\n",len);
		return(false);
	}

	for (int i=0;i<len - 2;i++) {
		if (! base64_valid_a[ b[i] ] ) {
			fprintf(stderr, "base64: bad character offset %d\n",i);
			return(false);
		}
	}

	// last one or two are allowed to be '=' but [0] only if [1]
	if (b[1] == '=' && b[0] == '=')	return(true);

	// if they're not '=', still need to validate
	if (! base64_valid_a[ b[0] ] ) {
		fprintf(stderr, "next to last not = nor valid: is %c\n",b[0]);
		return(false);
	}
	if (! base64_valid_a[ b[1] ] ) {
		fprintf(stderr, " last not = nor valid: is %c\n",b[1]);
		return(false);
	}
	
	// check last four for correct '=' pattern - only l
	
	return(true);
}

// use this function to make sure you're allocating enough space to the encode function

int
cf_base64_encode_maxlen(int len)
{
	len = (len / 3) + 1;
	len <<= 2;
	return(len+1);
}


// Assumes the output buffer has been allocated to a good size: everyone should know it needs be 4/3 bigger



void
cf_base64_encode(uint8_t * in_bytes, uint8_t *out_bytes, int *len_r) {
	
  int len = *len_r;
  int i = 0;
  int j = 0;

  while (len >= 3) {
      out_bytes[j+0] = base64_bytes[ (in_bytes[i+0] & 0xfc) >> 2 ];
      out_bytes[j+1] = base64_bytes[ ((in_bytes[i+0] & 0x03) << 4) + ((in_bytes[i+1] & 0xf0) >> 4) ];
      out_bytes[j+2] = base64_bytes[ ((in_bytes[i+1] & 0x0f) << 2) + ((in_bytes[i+2] & 0xc0) >> 6) ];
      out_bytes[j+3] = base64_bytes[ in_bytes[i+2] & 0x3f ];

	  i += 3;
	  j += 4;
	  len -= 3;
  }

  if (len == 1) {
      out_bytes[j+0] = base64_bytes[ (in_bytes[i+0] & 0xfc) >> 2 ];
      out_bytes[j+1] = base64_bytes[ ((in_bytes[i+0] & 0x03) << 4) ];
      out_bytes[j+2] = '=';
	  out_bytes[j+3] = '=';
	  j += 4;
  }
  else if (len == 2) {
      out_bytes[j+0] = base64_bytes[ (in_bytes[i+0] & 0xfc) >> 2 ];
      out_bytes[j+1] = base64_bytes[ ((in_bytes[i+0] & 0x03) << 4) + ((in_bytes[i+1] & 0xf0) >> 4) ];
      out_bytes[j+2] = base64_bytes[ ((in_bytes[i+1] & 0x0f) << 2) ];
      out_bytes[j+3] = '=';
	  j += 4;
  }
  
  *len_r = j;

}

void
cf_base64_tostring(uint8_t * in_bytes, char *out_bytes, int *len_r) {
	
  int len = *len_r;
  int i = 0;
  int j = 0;

  while (len >= 3) {
      out_bytes[j+0] = base64_chars[ (in_bytes[i+0] & 0xfc) >> 2 ];
      out_bytes[j+1] = base64_chars[ ((in_bytes[i+0] & 0x03) << 4) + ((in_bytes[i+1] & 0xf0) >> 4) ];
      out_bytes[j+2] = base64_chars[ ((in_bytes[i+1] & 0x0f) << 2) + ((in_bytes[i+2] & 0xc0) >> 6) ];
      out_bytes[j+3] = base64_chars[ in_bytes[i+2] & 0x3f ];

	  i += 3;
	  j += 4;
	  len -= 3;
  }

  if (len == 1) {
      out_bytes[j+0] = base64_chars[ (in_bytes[i+0] & 0xfc) >> 2 ];
      out_bytes[j+1] = base64_chars[ ((in_bytes[i+0] & 0x03) << 4) ];
      out_bytes[j+2] = '=';
	  out_bytes[j+3] = '=';
	  j += 4;
  }
  else if (len == 2) {
      out_bytes[j+0] = base64_chars[ (in_bytes[i+0] & 0xfc) >> 2 ];
      out_bytes[j+1] = base64_chars[ ((in_bytes[i+0] & 0x03) << 4) + ((in_bytes[i+1] & 0xf0) >> 4) ];
      out_bytes[j+2] = base64_chars[ ((in_bytes[i+1] & 0x0f) << 2) ];
      out_bytes[j+3] = '=';
	  j += 4;
  }
  
  out_bytes[j++] = 0;
  
  *len_r = j;

}

#define da base64_decode_a

// outbuf always shorter than inbuf, use it?

int
cf_base64_decode_inplace(uint8_t *bytes, int *len_r, bool validate)
{
	if (validate) {
		if (true != cf_base64_validate_input(bytes, *len_r)) {
			fprintf(stderr, "base64 decode: length must be 4 aligned\n");
			return(-1);
		}
	}

	
  int len = *len_r;
  int i = 0;
  int j = 0;

  int len_cor;
  if (bytes[len-1] == '=') {
	  len_cor = 1;
	  if (bytes[len-2] == '=') {
		  len_cor = 2;
	  }
  }
  else
	  len_cor = 0;
  
  while (i < len) {
	  uint8_t b0, b1, b2;
      b0 = (da[ bytes[i+0] ] << 2) | (da[ bytes[i+1]]  >> 4);
      b1 = (da[ bytes[i+1] ] << 4) | (da[ bytes[i+2] ] >> 2);
      b2 = (da[ bytes[i+2] ] << 6) |  da[ bytes[i+3] ];

	  bytes[j] = b0;
	  bytes[j+1] = b1;
	  bytes[j+2] = b2;
	  
	  i += 4;
	  j += 3;
  }

  *len_r = j - len_cor;
  return(0);

}



int
cf_base64_decode(uint8_t *in_bytes, uint8_t *out_bytes, int *len_r, bool validate)
{

	if (validate) {
		if (true != cf_base64_validate_input(in_bytes, *len_r)) {
			fprintf(stderr, "base64 decode: length must be 4 aligned\n");
			return(-1);
		}
	}

  int len = *len_r;
  int i = 0;
  int j = 0;
  
  while (i < len) {
	  
      out_bytes[j+0] = (da[ in_bytes[i+0] ] << 2) | (da[ in_bytes[i+1]]  >> 4);
      out_bytes[j+1] = (da[ in_bytes[i+1] ] << 4) | (da[ in_bytes[i+2] ] >> 2);
      out_bytes[j+2] = (da[ in_bytes[i+2] ] << 6) |  da[ in_bytes[i+3] ];

	  i += 4;
	  j += 3;
  }

  if (in_bytes[i-1] == '=') j--;
  if (in_bytes[i-2] == '=') j--;
  
  *len_r = j;
  return(0);
}

//
// Unit test
//

#define TEST_LEN_1 100

int
cf_base64_test()
{
	uint8_t buf[TEST_LEN_1];
	for (uint i=0 ; i < sizeof(buf) ; i++)
		buf[i] = i;
	
	uint8_t b64_buf[ cf_base64_encode_maxlen( sizeof(buf) ) ];
	int len = sizeof(buf);
	cf_base64_encode( buf , b64_buf , &len);
	b64_buf[len] = 0;
	
	fprintf(stderr, "b64 test: encoded. start len %zd new len %d\n",sizeof(buf), len);
	fprintf(stderr, "  %s\n",b64_buf);
	
	cf_base64_decode_inplace(b64_buf, &len, true);
	
	if (len != sizeof(buf) ) {
		fprintf(stderr, " test fail: length should be %zd is %d\n",sizeof(buf),len);
		return(-1);
	}
	
	for (uint i=0 ; i < sizeof(buf) ; i++) {
		if (buf[i] != b64_buf[i]) {
			fprintf(stderr, " test fail: byte %d is %02x should be %02x\n",i,buf[i],b64_buf[i]);
		}
	}
	
	return(0);
}


