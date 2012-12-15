#include <citrusleaf/citrusleaf.h>
#include <citrusleaf/udf.h>
#include <citrusleaf/as_hashmap.h>
#include <citrusleaf/as_buffer.h>
#include <citrusleaf/as_msgpack.h>
#include <stdio.h>


#define HOST "127.0.0.1"
#define PORT 3010
#define TIMEOUT 100

#define LOG(msg, ...) \
    { printf("%s:%d - ", __FILE__, __LINE__); printf(msg, ##__VA_ARGS__ ); printf("\n"); }

int print_buffer(as_buffer * buff) {
    msgpack_sbuffer sbuf;
    msgpack_sbuffer_init(&sbuf);

    sbuf.data = buff->data;
    sbuf.size = buff->size;
    sbuf.alloc = buff->capacity;

    msgpack_zone mempool;
    msgpack_zone_init(&mempool, 2048);

    msgpack_object deserialized;
    msgpack_unpack(sbuf.data, sbuf.size, NULL, &mempool, &deserialized);
    
    msgpack_object_print(stdout, deserialized);

    msgpack_zone_destroy(&mempool);
    return 0;
}

int main(int argc, char ** argv) {
    
    if ( argc != 4 ) {
        LOG("invalid arguments.");
        return 1;
    }

    char * n = argv[1];
    char * s = argv[2];
    char * k = argv[3];

    citrusleaf_init();

    cl_cluster * c = citrusleaf_cluster_create();
    citrusleaf_cluster_add_host(c, HOST, PORT, TIMEOUT);

    cl_object key;
    citrusleaf_object_init_str(&key, k);

    cl_bin * bins = NULL;
    int nbins = 0;
    uint32_t gen = 0;

    int rc = citrusleaf_get_all(c, n, s, &key, &bins, &nbins, TIMEOUT, &gen);

    if ( rc ) {
        printf("error: %d\n",rc);
    }
    else {
        as_serializer ser;
        as_msgpack_init(&ser);

        printf("{");
        for (int i = 0; i < nbins; i++) {
            printf("\"%s\": ",bins[i].bin_name);
            switch (bins[i].object.type) {
                case CL_STR:
                    printf("\"%s\"", bins[i].object.u.str);
                    break;
                case CL_INT:
                    printf("%"PRId64"",bins[i].object.u.i64);
                    break;
                case CL_LIST: 
                case CL_MAP: {

                    as_val * val = NULL;
                    as_buffer buf = {
                        .capacity = (uint32_t) bins[i].object.sz,
                        .size = (uint32_t) bins[i].object.sz,
                        .data = (char *) bins[i].object.u.blob
                    };
                    // print_buffer(&buf);
                    as_serializer_deserialize(&ser, &buf, &val);
                    printf("%s",as_val_tostring(val));
                    break;
                }
                default:
                    printf("<%d>",(int)bins[i].object.type);
                    break;
            }
            if ( i < nbins-1 ) {
                printf(", ");
            }
        }
        printf("}");
        printf("\n");

        as_serializer_destroy(&ser);
    }

    return rc;
}
