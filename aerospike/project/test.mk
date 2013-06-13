###############################################################################
##  OBJECTS                                                      		 	 ##
###############################################################################

# TEST_KV = 
# TEST_KV += kv/kv_string
 
# TEST_RECORD = 
# TEST_RECORD += record/record_basics
# TEST_RECORD += record/record_lists
 
# TEST_STREAM = 
# TEST_STREAM += stream/stream_simple
# TEST_STREAM += stream/stream_ads

# TEST_UTIL = 
# TEST_UTIL += util/udf
# TEST_UTIL += util/consumer_stream
# TEST_UTIL += util/producer_stream
# TEST_UTIL += util/map_rec
# TEST_UTIL += util/test_aerospike
# TEST_UTIL += util/test_logger
# TEST_UTIL += util/info_util

# TEST_LSTACK = 
# TEST_LSTACK += lstack/lstack_advanced
# TEST_LSTACK += lstack/lstack_basics
# TEST_LSTACK += lstack/lstack_operations
# TEST_LSTACK += lstack/lstack_test
# TEST_LSTACK += lstack/lstack_util
# TEST_LSTACK += lstack/test_config

# TEST_LSET =
# TEST_LSET += lset/lset_advanced
# TEST_LSET += lset/lset_basics
# TEST_LSET += lset/lset_operations
# TEST_LSET += lset/lset_test
# TEST_LSET += lset/lset_util
# TEST_LSET += lset/test_config

# TEST_CLIENT = client_test
# TEST_CLIENT += $(TEST_UTIL) 
# TEST_CLIENT += $(TEST_LSTACK) 
# TEST_CLIENT += $(TEST_LSET)
# TEST_CLIENT += $(TEST_KV) 
# TEST_CLIENT += $(TEST_RECORD) 
# TEST_CLIENT += $(TEST_STREAM)

TEST_AEROSPIKE = aerospike_test.c
TEST_AEROSPIKE += aerospike_digest/*.c
TEST_AEROSPIKE += aerospike_index/*.c
TEST_AEROSPIKE += aerospike_info/*.c
TEST_AEROSPIKE += aerospike_key/*.c
TEST_AEROSPIKE += aerospike_query/*.c
TEST_AEROSPIKE += aerospike_scan/*.c
TEST_AEROSPIKE += aerospike_udf/*.c
TEST_AEROSPIKE += util/*.c

TEST_SOURCE = $(wildcard $(addprefix $(SOURCE_TEST)/, $(TEST_AEROSPIKE)))

TEST_OBJECT = $(patsubst %.c,%.o,$(subst $(SOURCE_TEST)/,$(TARGET_TEST)/,$(TEST_SOURCE)))

###############################################################################
##  FLAGS                                                      		         ##
###############################################################################

TEST_VALGRIND = --tool=memcheck --leak-check=yes --show-reachable=yes --num-callers=20 --track-fds=yes -v

TEST_CFLAGS = -I$(TARGET_INCL)

TEST_LDFLAGS = -lssl -lcrypto -llua -lpthread -lm -lrt

###############################################################################
##  TARGETS                                                      		     ##
###############################################################################

.PHONY: test
test: $(TARGET_TEST)/aerospike_test
	$(TARGET_TEST)/aerospike_test

.PHONY: test-valgrind
test-valgrind: test-build
	valgrind $(TEST_VALGRIND) $(TARGET_TEST)/aerospike_test 1>&2 2>client_test-valgrind

.PHONY: test-build
test-build: $(TARGET_TEST)/aerospike_test

.PHONY: test-clean
test-clean: 
	@rm -rf $(TARGET_TEST)

$(TARGET_TEST)/%/%.o: CFLAGS = $(TEST_CFLAGS)
$(TARGET_TEST)/%/%.o: LDFLAGS += $(TEST_LDFLAGS)
$(TARGET_TEST)/%/%.o: $(SOURCE_TEST)/%/%.c
	$(object)

$(TARGET_TEST)/%.o: CFLAGS = $(TEST_CFLAGS)
$(TARGET_TEST)/%.o: LDFLAGS += $(TEST_LDFLAGS)
$(TARGET_TEST)/%.o: $(SOURCE_TEST)/%.c
	$(object)

$(TARGET_TEST)/aerospike_test: CFLAGS += $(TEST_CFLAGS)
$(TARGET_TEST)/aerospike_test: LDFLAGS += $(TEST_LDFLAGS)
$(TARGET_TEST)/aerospike_test: $(TEST_OBJECT) $(TARGET_TEST)/test.o | build prepare
	$(executable) $(TARGET_LIB)/libcitrusleaf.a $(TEST_LDFLAGS)
