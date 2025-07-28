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

/**
 * Enable/disable fuzzing globally
 * 
 * @param enabled - true to enable fuzzing, false to disable
 */
void fuzz_set_enabled(bool enabled);

/**
 * Set the fuzzing probability (0.0 to 1.0)
 * 
 * @param probability - Probability of fuzzing each byte (0.0 = never, 1.0 = always)
 */
void fuzz_set_probability(double probability); 