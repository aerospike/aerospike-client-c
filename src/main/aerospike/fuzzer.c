#include <aerospike/fuzzer.h>
#include <aerospike/as_command.h>
#include <aerospike/as_proto.h>
#include <stdlib.h>
#include <time.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>

// Global fuzzer state
static bool g_fuzz_enabled = false;
static double g_fuzz_probability = 0.01;  // 1% chance by default
static bool g_fuzz_initialized = false;


/**
 * Check if fuzzing is enabled by checking the environment variable
 * AEROSPIKE_FUZZ_ENABLE.
 * 
 * @return true if fuzzing is enabled, false otherwise
 */
static bool fuzzing_enabled(void){
    const char* fuzz_enable = getenv("AEROSPIKE_FUZZ_ENABLE");
    if(!fuzz_enable) {
        fprintf(stderr, "Fuzzer: AEROSPIKE_FUZZ_ENABLE is not set\n");
        return false;
    }
    if(strcmp(fuzz_enable, "1") == 0 || strcmp(fuzz_enable, "true") == 0) {
        return true;
    }
    return false;
}


/**
 * Initialize the fuzzer 
 *  - seed PRNG and set globals based on env vars:
 *      - g_fuzz_probability
 *      - g_fuzz_initialized
 */
static void fuzz_init(void) {
    if (!g_fuzz_initialized) {
        fprintf(stderr, "Fuzzer: initializing...\n");
        srand((unsigned int)time(NULL));

        // Check environment variables for configuration
        const char* fuzz_prob = getenv("AEROSPIKE_FUZZ_PROBABILITY");
        fprintf(stderr, "Fuzzer: AEROSPIKE_FUZZ_PROBABILITY = '%s'\n", fuzz_prob ? fuzz_prob : "NULL");
        if (fuzz_prob) {
            double prob = atof(fuzz_prob);
            if (prob >= 0.0 && prob <= 1.0) {
                g_fuzz_probability = prob;
                fprintf(stderr, "Fuzzer: probability set to %.3f via environment variable\n", g_fuzz_probability);
            } else {
                fprintf(stderr, "Fuzzer: invalid probability value: %f\n", prob);
            }
        }
        g_fuzz_initialized = true;
    }
}

// void fuzz_set_enabled(bool enabled) {
//     g_fuzz_enabled = enabled;
//     if (enabled) {
//         fuzz_init();
//         fprintf(stderr, "Fuzzer: ENABLED (probability: %.3f)\n", g_fuzz_probability);
//     } else {
//         fprintf(stderr, "Fuzzer: DISABLED\n");
//     }
// }

// void fuzz_set_probability(double probability) {
//     if (probability < 0.0) probability = 0.0;
//     if (probability > 1.0) probability = 1.0;
//     g_fuzz_probability = probability;
//     fprintf(stderr, "Fuzzer: probability set to %.3f\n", g_fuzz_probability);
// }

#define print_err(fmt, ...) fprintf(stderr, fmt, ##__VA_ARGS__)

void parse_cmd(as_command* cmd){
    as_proto_msg* proto_msg = (as_proto_msg*) cmd->buf;
    as_msg* msg = &proto_msg->m;
    fprintf(stderr, "---DEBUG--- in parse_cmd:\n");
    print_err("Parsing msg:");
    print_err("info1: %d\n", msg->info1);
    print_err("info2: %d\n", msg->info2);
    print_err("info3: %d\n", msg->info3);
    print_err("info4: %d\n", msg->unused);
    print_err("result_code: %d\n", msg->result_code);
    print_err("generation: %d\n", msg->generation);
    print_err("record_ttl: %d\n", msg->record_ttl);
    print_err("transaction_ttl: %d\n", msg->transaction_ttl);
    print_err("n_fields: %d\n", msg->n_fields);
    print_err("n_ops: %d\n", msg->n_ops);
    print_err("data: %s\n", msg->data);
}


void fuzz(as_command* cmd) {
    parse_cmd(cmd);
    g_fuzz_enabled = fuzzing_enabled();
    fprintf(stderr, "Fuzzer: fuzz() called, enabled=%s\n", 
            g_fuzz_enabled ? "true" : "false");

    if (!g_fuzz_enabled || !cmd || !cmd->buf || cmd->buf_size == 0) {
        if (!g_fuzz_enabled) {
            fprintf(stderr, "Fuzzer: not enabled\n");
        }
        if (!cmd) {
            fprintf(stderr, "Fuzzer: cmd is null\n");
        }
        if (cmd && !cmd->buf) {
            fprintf(stderr, "Fuzzer: cmd->buf is null\n");
        }
        if (cmd && cmd->buf_size == 0) {
            fprintf(stderr, "Fuzzer: cmd->buf_size is 0\n");
        }
        return;
    }

    // Initialize the first time fuzz is called AND enabled
    fuzz_init(); // becomes no op after first successful call

    fprintf(stderr, "Fuzzer: proceeding with fuzzing, buf_size=%zu, probability=%.3f\n", 
            cmd->buf_size, g_fuzz_probability);

    size_t mutations = 0;

    // Iterate through the buffer and potentially fuzz each byte
    for (size_t i = 8; i < cmd->buf_size; i++) {
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
                    int delta = 0;
                    while (delta == 0)
                    {
                        // -10 to +10, non-inclusive of 0
                        delta = (rand() % 21) - 10;  
                    }

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


/**
 * Header (aka as_proto) includes
 *  - version
 *  - type
 *  - 6byte `sz` size field indicating as_msg
 */
void fuzz_proto_header(uint8_t* header){

}

void fuzz_fields(uint8_t* fields){

}

void fuzz_ops(uint8_t* ops){

}

void fuzz_infos(uint8_t* infos){

}

void fuzz_misc(u_int8_t) {
    // fields between info & data:
        // uint8_t result_code;
        // uint32_t generation;
        // uint32_t record_ttl;
        // uint32_t transaction_ttl;
        // uint16_t n_fields;
        // uint16_t n_ops;
}
