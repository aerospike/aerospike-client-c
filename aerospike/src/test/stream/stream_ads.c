
#include "../test.h"
#include "../util/udf.h"
#include "../util/consumer_stream.h"
#include "../util/test_logger.h"
#include <citrusleaf/as_stream.h>
#include <citrusleaf/as_types.h>
#include <citrusleaf/as_module.h>
#include <citrusleaf/mod_lua.h>
#include <citrusleaf/mod_lua_config.h>
#include <citrusleaf/cl_query.h>
#include <citrusleaf/citrusleaf.h>
#include <limits.h>
#include <stdlib.h>

/******************************************************************************
 * MACROS
 *****************************************************************************/

#define LUA_FILE "src/test/lua/client_stream_ads.lua"
#define UDF_FILE "client_stream_ads"

/******************************************************************************
 * VARAIBLES
 *****************************************************************************/

extern cl_cluster * cluster;

/******************************************************************************
 * TEST CASES
 *****************************************************************************/
 
TEST( stream_ads_exists, UDF_FILE" exists" ) {
    int rc = udf_exists(LUA_FILE);
    assert_int_eq( rc, 0 );
}

/**
 * Creates 25600 records and 1 index.
 *
 * The record structure is:
 *      bid = Number
 *      timestamp = Number
 *      advertiser = Number
 *      campaign = Number
 *      line_item = Number
 *      spend = Number
 *
 * The bid is the record key.
 *
 * The sample will be:
 *      4 advertisers x 4 campaigns x 4 line_items x 4 bids / second @ 100 seconds = 25600 records
 *
 * Timestamps will start at 275273225 (September 22, 1978)
 *
 * The index will be on the timestamp.
 *
 */
TEST( stream_ads_create, "create 25600 records and 1 index" ) {

    int rc = 0;

    char * sindex_resp = NULL;

    // create index on "timestamp"

    rc = citrusleaf_secondary_index_create(cluster, "test", "ads", "test_ads_timestamp", "timestamp", "NUMERIC", &sindex_resp);
    if ( rc != CITRUSLEAF_OK && rc != CITRUSLEAF_FAIL_INDEX_EXISTS ) {
        info("error(%d): %s", rc, sindex_resp);
    }

    if ( sindex_resp ) {
        free(sindex_resp);
        sindex_resp = NULL;
    }


    // insert records

    cl_write_parameters wp;
    cl_write_parameters_set_default(&wp);
    wp.timeout_ms = 1000;
    wp.record_ttl = 864000;

    cl_object okey;
    cl_bin bins[6];
    strcpy(bins[0].bin_name, "bid");
    strcpy(bins[1].bin_name, "timestamp");
    strcpy(bins[2].bin_name, "advertiser");
    strcpy(bins[3].bin_name, "campaign");
    strcpy(bins[4].bin_name, "line_item");
    strcpy(bins[5].bin_name, "spend");


    uint32_t ts = 275273225;
    uint32_t et = 0;

    srand(ts);

    for ( int i = 0; i < 25600; i++ ) {

        if ( i % 4 == 0 ) {
            et++;
        }

        int nbins = 6;

        uint32_t advertiserId = (rand() % 4) + 1;
        uint32_t campaignId = advertiserId * 10 + (rand() % 4) + 1;
        uint32_t lineItemId = campaignId * 10 + (rand() % 4) + 1;
        uint32_t bidId = lineItemId * 100000 + i;
        uint32_t timestamp = ts + et;
        uint32_t spend = advertiserId + campaignId + lineItemId;

        citrusleaf_object_init_int(&okey, bidId);
        citrusleaf_object_init_int(&bins[0].object, bidId);
        citrusleaf_object_init_int(&bins[1].object, timestamp);
        citrusleaf_object_init_int(&bins[2].object, advertiserId);
        citrusleaf_object_init_int(&bins[3].object, campaignId);
        citrusleaf_object_init_int(&bins[4].object, lineItemId);
        citrusleaf_object_init_int(&bins[5].object, spend);

        rc = citrusleaf_put(cluster, "test", "ads", &okey, bins, nbins, &wp);

        assert_int_eq(rc, 0);

        cl_bin *    rbins = NULL;
        int         nrbins = 0;
        uint32_t    rgen = 0;

        rc = citrusleaf_get_all(cluster, "test", "ads", &okey, &rbins, &nrbins, 1000, &rgen);

        if (rbins) {
            citrusleaf_bins_free(rbins, nrbins);
            free(rbins);
        }
        assert_int_eq(rc, 0);
    }

    info("done.");
}

TEST( stream_ads_1, "COUNT(*)" ) {

    int rc = 0;
    cf_atomic32 count = 0;

    as_stream_status consume(as_val * v) {
        if ( v == AS_STREAM_END ) {
            info("count: %d",count);
        }
        else {
            cf_atomic32_incr(&count); 
            as_val_destroy(v);
        }
        return AS_STREAM_OK;
    }

    as_stream * consumer = consumer_stream_new(consume);

    as_query * q = as_query_new("test", "ads");
    as_query_select(q, "bid");
    as_query_where(q, "timestamp", integer_range(0, UINT32_MAX));
    
    rc = citrusleaf_query_stream(cluster, q, consumer);

    assert_int_eq( rc, 0 );
    assert_int_eq( count, 25600 );

    as_query_destroy(q);
    as_stream_destroy(consumer);
}

TEST( stream_ads_2, "SELECT advertiser, campaign, line_item, SUM(spend), MAX(spend), COUNT(spend) WHERE ts BETWEEN (NOW, NOW-100) GROUP BY advertiser, campaign, line_item (w/ map & reduce)" ) {

    int rc = 0;
    int count = 0;
    as_map * result = NULL;

    as_stream_status consume(as_val * v) {
        if ( v == AS_STREAM_END ) {
            info("count: %d",count);
        }
        else {
            result = (as_map *) v;
            count++;
        }
        return AS_STREAM_OK;
    }

    as_stream * consumer = consumer_stream_new(consume);

    as_query * q = as_query_new("test", "ads");
    as_query_where(q, "timestamp", integer_range(0, UINT32_MAX));
    as_query_aggregate(q, UDF_FILE, "stream_ads_2", NULL);
    
    rc = citrusleaf_query_stream(cluster, q, consumer);

    assert_int_eq( rc, 0 );
    assert_int_eq( count, 1 );
    
    info("+------+--------------+--------------+--------------+--------------+--------------+--------------+");
    info("| %-4s | %-12s | %-12s | %-12s | %-12s | %-12s | %-12s |", "#", "advertiser", "campaign", "line_no", "sum(spend)", "max(spend)", "count(spend)");
    info("+------+--------------+--------------+--------------+--------------+--------------+--------------+");
    as_iterator * i = as_map_iterator_new(result);
    int n = 0;
    while ( as_iterator_has_next(i) ) {
        as_pair *   p = (as_pair *) as_iterator_next(i);
        as_map *    v = (as_map *) as_pair_2(p);

        as_string s;

        char * aid = as_val_tostring(as_map_get(v, (as_val *) as_string_init(&s, "advertiser", false)));
        char * cid = as_val_tostring(as_map_get(v, (as_val *) as_string_init(&s, "campaign", false)));
        char * lid = as_val_tostring(as_map_get(v, (as_val *) as_string_init(&s, "line_item", false)));
        char * ss  = as_val_tostring(as_map_get(v, (as_val *) as_string_init(&s, "spend_sum", false)));
        char * sm  = as_val_tostring(as_map_get(v, (as_val *) as_string_init(&s, "spend_max", false)));
        char * sc  = as_val_tostring(as_map_get(v, (as_val *) as_string_init(&s, "spend_num", false)));

        info("| %-4d | %-12s | %-12s | %-12s | %-12s | %-12s | %-12s |", 
            ++n, aid, cid, lid, ss, sm, sc
        );

        free(aid);
        free(cid);
        free(lid);
        free(ss);
        free(sm);
        free(sc);
    }
    as_iterator_destroy(i);
    info("+------+--------------+--------------+--------------+--------------+--------------+--------------+");

    as_val_destroy(result);
    as_query_destroy(q);
    as_stream_destroy(consumer);
}

TEST( stream_ads_3, "SELECT advertiser, campaign, line_item, SUM(spend), MAX(spend), COUNT(spend) WHERE ts BETWEEN (NOW, NOW-100) GROUP BY advertiser, campaign, line_item (w/ aggregate & reduce)" ) {

    int rc = 0;
    int count = 0;
    as_map * result = NULL;

    as_stream_status consume(as_val * v) {
        if ( v == AS_STREAM_END ) {
            info("count: %d",count);
        }
        else {
            result = (as_map *) v;
            count++;
        }
        return AS_STREAM_OK;
    }

    as_stream * consumer = consumer_stream_new(consume);

    as_query * q = as_query_new("test", "ads");
    as_query_where(q, "timestamp", integer_range(0, UINT32_MAX));
    as_query_aggregate(q, UDF_FILE, "stream_ads_3", NULL);
    
    rc = citrusleaf_query_stream(cluster, q, consumer);

    assert_int_eq( rc, 0 );
    assert_int_eq( count, 1 );

    info("+------+--------------+--------------+--------------+--------------+--------------+--------------+");
    info("| %-4s | %-12s | %-12s | %-12s | %-12s | %-12s | %-12s |", "#", "advertiser", "campaign", "line_no", "sum(spend)", "max(spend)", "count(spend)");
    info("+------+--------------+--------------+--------------+--------------+--------------+--------------+");
    int n = 0;
    as_iterator * i = as_map_iterator_new(result);
    while ( as_iterator_has_next(i) ) {
        as_pair *   p = (as_pair *) as_iterator_next(i);
        // as_string * k = (as_string *) as_pair_1(p);
        as_map *    v = (as_map *) as_pair_2(p);

        as_string s;
        char *  aid = as_val_tostring(as_map_get(v, (as_val *) as_string_init(&s, "advertiser", false)));
        char *  cid = as_val_tostring(as_map_get(v, (as_val *) as_string_init(&s, "campaign", false)));
        char *  lid = as_val_tostring(as_map_get(v, (as_val *) as_string_init(&s, "line_item", false)));
        char *  ss  = as_val_tostring(as_map_get(v, (as_val *) as_string_init(&s, "spend_sum", false)));
        char *  sm  = as_val_tostring(as_map_get(v, (as_val *) as_string_init(&s, "spend_max", false)));
        char *  sc  = as_val_tostring(as_map_get(v, (as_val *) as_string_init(&s, "spend_num", false)));

        info("| %-4d | %-12s | %-12s | %-12s | %-12s | %-12s | %-12s |", 
            ++n, aid, cid, lid, ss, sm, sc
        );

        free(aid);
        free(cid);
        free(lid);
        free(ss);
        free(sm);
        free(sc);
    }
    as_iterator_destroy(i);
    info("+------+--------------+--------------+--------------+--------------+--------------+--------------+");
    
    as_val_destroy(result);
    as_query_destroy(q);
    as_stream_destroy(consumer);
}

TEST( stream_ads_4, "SELECT advertiser, campaign, line_item, SUM(spend), MAX(spend), COUNT(spend) WHERE ts BETWEEN (NOW, NOW-100) GROUP BY advertiser, campaign, line_item (w/ aggregate & reduce & nested maps)" ) {

    int rc = 0;
    int count = 0;
    as_map * result = NULL;

    as_stream_status consume(as_val * v) {
        if ( v == AS_STREAM_END ) {
            info("count: %d",count);
        }
        else {
            result = (as_map *) v;
            count++;
        }
        return AS_STREAM_OK;
    }

    as_stream * consumer = consumer_stream_new(consume);

    as_query * q = as_query_new("test", "ads");
    as_query_where(q, "timestamp", integer_range(0, UINT32_MAX));
    as_query_aggregate(q, UDF_FILE, "stream_ads_4", NULL);
    
    rc = citrusleaf_query_stream(cluster, q, consumer);

    assert_int_eq( rc, 0 );
    assert_int_eq( count, 1 );

    info("+------+--------------+--------------+--------------+--------------+--------------+--------------+");
    info("| %-4s | %-12s | %-12s | %-12s | %-12s | %-12s | %-12s |", "#", "advertiser", "campaign", "line_no", "sum(spend)", "max(spend)", "count(spend)");
    info("+------+--------------+--------------+--------------+--------------+--------------+--------------+");
    int n = 0;
    as_string s;
    as_iterator * a = as_map_iterator_new(result);
    while ( as_iterator_has_next(a) ) {
        as_pair *       ap = (as_pair *) as_iterator_next(a);
        as_integer *    ak = (as_integer *) as_pair_1(ap);
        as_map *        av = (as_map *) as_pair_2(ap);

        as_iterator * c = as_map_iterator_new(av);
        while ( as_iterator_has_next(c) ) {
            as_pair *       cp = (as_pair *) as_iterator_next(c);
            as_integer *    ck = (as_integer *) as_pair_1(cp);
            as_map *        cv = (as_map *) as_pair_2(cp);

            as_iterator * l = as_map_iterator_new(cv);
            while ( as_iterator_has_next(l) ) {
                as_pair *       lp = (as_pair *) as_iterator_next(l);
                as_integer *    lk = (as_integer *) as_pair_1(lp);
                as_map *        lv = (as_map *) as_pair_2(lp);

                char *  aid = as_val_tostring(ak);
                char *  cid = as_val_tostring(ck);
                char *  lid = as_val_tostring(lk);
                char *  ss  = as_val_tostring(as_map_get(lv, (as_val *) as_string_init(&s, "spend_sum", false)));
                char *  sm  = as_val_tostring(as_map_get(lv, (as_val *) as_string_init(&s, "spend_max", false)));
                char *  sc  = as_val_tostring(as_map_get(lv, (as_val *) as_string_init(&s, "spend_num", false)));

                info("| %-4d | %-12s | %-12s | %-12s | %-12s | %-12s | %-12s |", 
                    ++n, aid, cid, lid, ss, sm, sc
                );

                free(aid);
                free(cid);
                free(lid);
                free(ss);
                free(sm);
                free(sc);
            }
            as_iterator_destroy(l);
        }
        as_iterator_destroy(c);
    }
    as_iterator_destroy(a);
    info("+------+--------------+--------------+--------------+--------------+--------------+--------------+");
    

    as_val_destroy(result);
    as_query_destroy(q);
    as_stream_destroy(consumer);
}

/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

static bool before(atf_suite * suite) {

    citrusleaf_query_init();
 
    int rc = 0;

    mod_lua_config config = {
        .server_mode    = false,
        .cache_enabled  = false,
        .system_path    = "modules/mod-lua/src/lua",
        .user_path      = "src/test/lua"
    };

    if ( mod_lua.logger == NULL ) {
        mod_lua.logger = test_logger_new();
    }
        
    rc = as_module_configure(&mod_lua, &config);

    if ( rc != 0 ) {
        error("as_module_configure failed: %d", rc);
        return false;
    }

    rc = udf_put(LUA_FILE);
    if ( rc != 0 ) {
        error("failure while uploading: %s (%d)", LUA_FILE, rc);
        return false;
    }

    rc = udf_exists(LUA_FILE);
    if ( rc != 0 ) {
        error("lua file does not exist: %s (%d)", LUA_FILE, rc);
        return false;
    }


    return true;
}

static bool after(atf_suite * suite) {
    
    citrusleaf_query_shutdown();

    if ( mod_lua.logger ) {
        free(mod_lua.logger);
        mod_lua.logger = NULL;
    }
    
    int rc = udf_remove(LUA_FILE);
    if ( rc != 0 ) {
        error("failure while removing: %s (%d)", LUA_FILE, rc);
        return false;
    }

    return true;
}

SUITE( stream_ads, "advertising stream" ) {
    suite_before( before );
    suite_after( after   );
    
    suite_add( stream_ads_create );
    suite_add( stream_ads_1 );
    suite_add( stream_ads_2 );
    suite_add( stream_ads_3 );
    suite_add( stream_ads_4 );
}
