#include <aerospike/fuzzer.h>
#include <aerospike/as_command.h>
#include <stdlib.h>
#include <time.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>

// Global fuzzer state
static bool g_fuzz_enabled = false;
static double g_fuzz_probability = 0.01;  // 1% chance by default
static bool g_fuzz_initialized = false;

// Initialize the fuzzer (seed random number generator and check env vars)
static void fuzz_init(void) {
    if (!g_fuzz_initialized) {
        srand((unsigned int)time(NULL));
        
        // Check environment variables for configuration
        const char* fuzz_enable = getenv("AEROSPIKE_FUZZ_ENABLE");
        if (fuzz_enable && (strcmp(fuzz_enable, "1") == 0 || strcmp(fuzz_enable, "true") == 0)) {
            g_fuzz_enabled = true;
            fprintf(stderr, "Fuzzer: ENABLED via environment variable\n");
        }
        
        const char* fuzz_prob = getenv("AEROSPIKE_FUZZ_PROBABILITY");
        if (fuzz_prob) {
            double prob = atof(fuzz_prob);
            if (prob >= 0.0 && prob <= 1.0) {
                g_fuzz_probability = prob;
                fprintf(stderr, "Fuzzer: probability set to %.3f via environment variable\n", g_fuzz_probability);
            }
        }
        
        g_fuzz_initialized = true;
    }
}

void fuzz_set_enabled(bool enabled) {
    g_fuzz_enabled = enabled;
    if (enabled) {
        fuzz_init();
        fprintf(stderr, "Fuzzer: ENABLED (probability: %.3f)\n", g_fuzz_probability);
    } else {
        fprintf(stderr, "Fuzzer: DISABLED\n");
    }
}

void fuzz_set_probability(double probability) {
    if (probability < 0.0) probability = 0.0;
    if (probability > 1.0) probability = 1.0;
    g_fuzz_probability = probability;
    fprintf(stderr, "Fuzzer: probability set to %.3f\n", g_fuzz_probability);
}

void fuzz(as_command* cmd) {
    if (!g_fuzz_enabled || !cmd || !cmd->buf || cmd->buf_size == 0) {
        return;
    }
    
    fuzz_init();
    
    size_t mutations = 0;
    
    // Iterate through the buffer and potentially fuzz each byte
    for (size_t i = 0; i < cmd->buf_size; i++) {
        // Check if we should fuzz this byte based on probability
        double random_val = (double)rand() / RAND_MAX;
        if (random_val < g_fuzz_probability) {
            uint8_t original = cmd->buf[i];
            
            // Choose a fuzzing strategy (randomly)
            int strategy = rand() % 4;
            
            switch (strategy) {
                case 0: // Bit flip
                {
                    int bit = rand() % 8;
                    cmd->buf[i] ^= (1 << bit);
                    break;
                }
                case 1: // Random byte
                    cmd->buf[i] = (uint8_t)(rand() % 256);
                    break;
                case 2: // Add/subtract small value
                {
                    int delta = (rand() % 21) - 10;  // -10 to +10
                    cmd->buf[i] = (uint8_t)(cmd->buf[i] + delta);
                    break;
                }
                case 3: // Zero out byte
                    cmd->buf[i] = 0;
                    break;
            }
            
            if (cmd->buf[i] != original) {
                mutations++;
            }
        }
    }
    
    if (mutations > 0) {
        fprintf(stderr, "Fuzzer: mutated %zu bytes in %zu byte buffer\n", mutations, cmd->buf_size);
    }
} 