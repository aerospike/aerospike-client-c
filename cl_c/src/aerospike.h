#pragma once

typedef struct as_client_s  as_client;
typedef struct as_query_s   as_query;
typedef struct as_lookup_s  as_lookup;
typedef struct as_call_s    as_call;

typedef void (* as_result_callback)(int, as_result *, void *);

struct as_client_s {
};

struct as_query_s {
    const char *    ns;
    const char *    set;
    as_filterlist * filters;
    int             limit;
};

struct as_lookup_s {
    const char *    ns;
    const char *    set;
    const char *    key;
};

struct as_call_s {
    const char *    filename;
    const char *    function;
    as_arglist *    arguments;
};

int as_client_apply_record(as_client *, as_lookup *, as_call *, as_result **);
int as_client_apply_record_async(as_client *, as_lookup *, as_call *, void *, as_result_callback);

int as_client_apply_stream(as_client *, as_query *, as_call *, as_result **);
int as_client_apply_stream_async(as_client *, as_query *, as_call *, void *, as_result_callback);





/*****************************************************************************/

as_arglist args;
as_arglist_init(&args, 2);
as_arglist_add_string(&args, "bob");
as_arglist_add_int64(&args, 100);

as_lookup   lookup  = {"test", "demo", "1"};
as_call     foobar  = {"test", "demo", &args};
as_result * result  = NULL;

as_apply_record(&client, &lookup, &foobar, &result);

as_arglist_destroy(&args);
as_result_free(result);

/*****************************************************************************/

as_filterlist filters;
as_filterlist_init(&filters, 3);
as_filterlist_add(&filters, "age", as_predicate_integer_range(200, 400));
as_filterlist_add(&filters, "name", as_predicate_string_eq("Bob"));

as_arglist args;
as_arglist_init(&args, 2);
as_arglist_add_string(&args, "bob");
as_arglist_add_int64(&args, 100);

as_query    query   = {"test", "demo", &filters};
as_call     foobar  = {"foo", "bar", &args};
as_result * result  = NULL;

as_apply_stream(&client, &query, &foobar, &result);

as_arglist_destroy(&args);
as_filterlist_destroy(&filters);
as_result_free(result);