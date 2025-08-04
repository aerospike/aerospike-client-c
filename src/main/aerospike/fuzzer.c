#include <aerospike/fuzzer.h>
#include <aerospike/as_command.h>
#include <aerospike/as_proto.h>
#include <citrusleaf/cf_byte_order.h>
#include <stdlib.h>
#include <time.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>
#include <inttypes.h>

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

void parse_cmd_data(as_msg* msg){
    // as_proto_msg* proto_msg = (as_proto_msg*) cmd->buf;
    // as_msg msg = proto_msg->m;
    fprintf(stderr, "---DEBUG--- in parse_cmd_data:\n");
    uint8_t* data = msg + sizeof(as_msg);
    for (size_t i = 0; i < msg->n_fields; i++) {
        as_msg_field* field = (as_msg_field*) data;

        uint32_t field_sz = cf_swap_from_be32(field->field_sz);
        print_err("field[%zu]: type=%u, size=%u\n", i, field->type, field_sz);
        
        // field_sz includes the type byte, so data size is field_sz - 1
        uint8_t* field_data = field->data;
        for (size_t j = 0; j < field_sz - 1; j++) {
            print_err("%02x ", field_data[j]);
        }
        print_err("\n");

        // Advance past the entire field: 4-byte size + field_sz bytes (which includes type + data)
        data += sizeof(uint32_t) + field_sz;
    }
    for (size_t i = 0; i < msg->n_ops; i++) {
        print_err("TODO: print op %d\n", i);
        // as_msg_op* op = (as_msg_op*) data;
        // print_err("op[%zu]: %u\n", i, op->op);
        // data += op->op_sz;
    }

    // as_op* ops = (as_op*) msg->ops;
    // for (size_t i = 0; i < msg->n_ops; i++) {
    //     print_err("op[%zu]: %u\n", i, ops[i].op);
    // }
}

void parse_cmd(as_command* cmd){
    as_proto_msg* proto_msg = (as_proto_msg*) cmd->buf;
    as_msg tmp = proto_msg->m;
    as_msg* msg = &tmp;

    as_msg_swap_header_from_be(msg);
    // msg->n_fields = cf_swap_from_be16(msg->n_fields);
    // msg->n_ops = cf_swap_from_be16(msg->n_ops);
    // msg->generation = cf_swap_from_be32(msg->generation);
    // msg->record_ttl = cf_swap_from_be32(msg->record_ttl);
    // msg->transaction_ttl = cf_swap_from_be32(msg->transaction_ttl);

    fprintf(stderr, "---DEBUG--- in parse_cmd:\n");
    print_err("Parsing msg:\n");
    print_err("info1: %u\n", msg->info1);
    print_err("info2: %u\n", msg->info2);
    print_err("info3: %u\n", msg->info3);
    print_err("info4: %u\n", msg->unused);
    print_err("result_code: %u\n", msg->result_code);
    print_err("generation: %" PRIu32 "\n", msg->generation);
    print_err("record_ttl: %" PRIu32 "\n", msg->record_ttl);
    print_err("transaction_ttl: %" PRIu32 "\n", msg->transaction_ttl);
    print_err("n_fields: %u\n", msg->n_fields);
    print_err("n_ops: %u\n", msg->n_ops);
    size_t data_sz = cmd->buf_size - sizeof(as_proto_msg);
    print_err("data: [binary data, %zu bytes available]\n", data_sz);

    for (size_t i = 0; i < data_sz; i++) {
        print_err("%02x ", cmd->buf[i]);
    }
    print_err("\n");
    // print the data
    parse_cmd_data(msg);
    // print_err("data: [");
    // for (size_t i = 0; i < cmd->buf_size; i++) {
    //     print_err("%02x ", cmd->buf[i]);
    // }
    // print_err("]\n");
}

void parse_ops(as_command* cmd){
    as_proto_msg* proto_msg = (as_proto_msg*) cmd->buf;
    as_msg* msg = &proto_msg->m;

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

void fuzz_misc(u_int8_t other) {
    // fields between info & data:
        // uint8_t result_code;
        // uint32_t generation;
        // uint32_t record_ttl;
        // uint32_t transaction_ttl;
        // uint16_t n_fields;
        // uint16_t n_ops;
}
