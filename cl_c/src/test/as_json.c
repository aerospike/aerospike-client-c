
#include "as_json.h"

#define LOG(msg, ...) \
    // { printf("%s:%d - ", __FILE__, __LINE__); printf(msg, ##__VA_ARGS__ ); printf("\n"); }


as_list * as_json_array_to_list(json_t * a) {

    int size = json_array_size(a);
    as_list * l = as_arraylist_new(size,0);

    for (int i = 0; i < json_array_size(a); i++) {
        as_val * v = as_json_to_val(json_array_get(a,i));
        as_list_append(l, v);
    }

    return l;
}

as_map * as_json_object_to_map(json_t * o) {

    int             n = json_object_size(o);
    as_map *        m = as_hashmap_new(n);
    const char *    k = NULL;
    json_t *        v = NULL;

    json_object_foreach(o, k, v) {
        as_val * key = (as_val *) as_string_new(strdup(k));
        as_val * val = as_json_to_val(v);
        as_map_set(m, key, val);

    }

    return m;
}

as_string * as_json_string_to_string(json_t * s) {
    const char * str = json_string_value(s);
    return as_string_new(strdup(str));
}

as_integer * as_json_number_to_integer(json_t * n) {
    return as_integer_new((int64_t) json_integer_value(n));
}

as_val * as_json_to_val(json_t * j) {
    if ( json_is_array(j) )  return (as_val *) as_json_array_to_list(j);
    if ( json_is_object(j) ) return (as_val *) as_json_object_to_map(j);
    if ( json_is_string(j) ) return (as_val *) as_json_string_to_string(j);
    if ( json_is_number(j) ) return (as_val *) as_json_number_to_integer(j);
    return (as_val *) &as_nil;
}

as_val * as_json_arg(char * arg) {

    as_val * val = NULL;
    json_t * root = NULL;
    json_error_t error;

    root = json_loads(arg, 0, &error);

    if ( !root ) {
        // then it is either a string or integer (i hope)
        char * end = NULL;
        uint64_t i = (uint64_t) strtol(arg, &end, 10);
        if ( *end == '\0' ) {
            val = (as_val *) as_integer_new(i);
        }
        else {
            val = (as_val *) as_string_new(arg);
        }
    }
    else {
        val = as_json_to_val(root);
    }

    return val;
}

as_list * as_json_arglist(int argc, char ** argv) {
    if ( argc == 0 || argv == NULL ) return NULL;//cons(NULL,NULL);
    return cons(as_json_arg(argv[0]), as_json_arglist(argc-1, argv+1));
}

int as_json_print(const as_val * val) {
    if ( !val ) {
        printf("null");
        return 1;
    }
    switch( val->type ) {
        case AS_NIL: {
            printf("null");
            break;
        }
        case AS_BOOLEAN: {
            printf("%s", as_boolean_tobool((as_boolean *) val) ? "true" : "false");
            break;
        }
        case AS_INTEGER: {
            printf("%ld", as_integer_toint((as_integer *) val));
            break;
        }
        case AS_STRING: {
            printf("\"%s\"", as_string_tostring((as_string *) val));
            break;
        }
        case AS_LIST: {
            as_iterator * i = as_list_iterator((as_list *) val);
            bool delim = false;
            printf("[");
            while ( as_iterator_has_next(i) ) {
                if ( delim ) printf(",");
                printf(" ");
                as_json_print(as_iterator_next(i));
                delim = true;
            }
            printf(" ");
            printf("]");
            break;
        }
        case AS_MAP: {
            as_iterator * i = as_map_iterator((as_map *) val);
            bool delim = false;
            printf("{");
            while ( as_iterator_has_next(i) ) {
                as_pair * kv = (as_pair *) as_iterator_next(i);
                if ( delim ) printf(",");
                printf(" ");
                as_json_print(as_pair_1(kv));
                printf(": ");
                as_json_print(as_pair_2(kv));
                delim = true;
            }
            printf(" ");
            printf("}");
            break;
        }
        default: {
            printf("~~<%d>", val->type);
        }
    }
    return 0;
}



