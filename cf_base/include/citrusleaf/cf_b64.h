/*
 * A general purpose base64 encoder and decoder
 * Copywrite 2009 Brian Bulkowski
 * All rights reserved
 */

#pragma once
 
#include <inttypes.h>
#include <stdint.h>
#include <stdbool.h>

// Move these to a CF file at some point if they work
bool cf_base64_validate_input(const uint8_t *b, const int len);
int cf_base64_encode_maxlen(int len);
void cf_base64_encode(uint8_t * in_bytes, uint8_t *out_bytes, int *len_r);
void cf_base64_tostring(uint8_t * in_bytes, char *out_bytes, int *len_r);
int cf_base64_decode_inplace(uint8_t *bytes, int *len_r, bool validate);
int cf_base64_decode(uint8_t *in_bytes, uint8_t *out_bytes, int *len_r, bool validate);

int cf_base64_test();

