
#include <jansson.h>
#include <citrusleaf/as_types.h>

as_list * as_json_array_to_list(json_t *);
as_map * as_json_object_to_map(json_t *);
as_string * as_json_string_to_string(json_t *);
as_integer * as_json_number_to_integer(json_t *);
as_val * as_json_to_val(json_t *);

int as_json_print(const as_val *);

as_val * as_json_arg(char *);
as_list * as_json_arglist(int, char **);