#pragma once

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

// Forward declaration
struct as_command_s;

/**
 * Fuzz the command buffer in place
 * 
 * @param cmd - Pointer to the as_command structure containing the buffer to fuzz
 */
void fuzz(struct as_command_s* cmd);



typedef struct as_msg_field_s {
	uint32_t field_sz; // includes type
	uint8_t type;
	uint8_t data[0];
} __attribute__((__packed__)) as_msg_field;

typedef struct as_msg_op_s {
	uint32_t op_sz; // includes everything past this
	uint8_t op;
	uint8_t particle_type;
	uint8_t has_lut: 1;
	uint8_t unused_flags: 7;
	uint8_t name_sz;
	uint8_t name[0];
	// Note - optional metadata (lut) and op value follows name.
} __attribute__((__packed__)) as_msg_op;
// /**
//  * Enable/disable fuzzing globally
//  * 
//  * @param enabled - true to enable fuzzing, false to disable
//  */
// void fuzz_set_enabled(bool enabled);

// /**
//  * Set the fuzzing probability (0.0 to 1.0)
//  * 
//  * @param probability - Probability of fuzzing each byte (0.0 = never, 1.0 = always)
//  */
// void fuzz_set_probability(double probability); 