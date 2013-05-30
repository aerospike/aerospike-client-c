###############################################################################
##  OBJECTS                                                      		 	 ##
###############################################################################

TEST_KV = 
TEST_KV += kv/kv_string
 
TEST_RECORD = 
TEST_RECORD += record/record_basics
TEST_RECORD += record/record_lists
 
TEST_STREAM = 
TEST_STREAM += stream/stream_simple
TEST_STREAM += stream/stream_ads

TEST_UTIL = 
TEST_UTIL += util/udf
TEST_UTIL += util/consumer_stream
TEST_UTIL += util/producer_stream
TEST_UTIL += util/map_rec
TEST_UTIL += util/test_aerospike
TEST_UTIL += util/test_logger
TEST_UTIL += util/info_util

TEST_LSTACK = 
TEST_LSTACK += lstack/lstack_advanced
TEST_LSTACK += lstack/lstack_basics
TEST_LSTACK += lstack/lstack_operations
TEST_LSTACK += lstack/lstack_test
TEST_LSTACK += lstack/lstack_util
TEST_LSTACK += lstack/test_config

TEST_LSET =
TEST_LSET += lset/lset_advanced
TEST_LSET += lset/lset_basics
TEST_LSET += lset/lset_operations
TEST_LSET += lset/lset_test
TEST_LSET += lset/lset_util
TEST_LSET += lset/test_config



TEST_CLIENT = client_test
TEST_CLIENT += $(TEST_UTIL) 
TEST_CLIENT += $(TEST_LSTACK) 
TEST_CLIENT += $(TEST_LSET)
TEST_CLIENT += $(TEST_KV) 
TEST_CLIENT += $(TEST_RECORD) 
TEST_CLIENT += $(TEST_STREAM)

###############################################################################
##  FLAGS                                                      		         ##
###############################################################################

TEST_VALGRIND = --tool=memcheck --leak-check=yes --show-reachable=yes --num-callers=20 --track-fds=yes -v

TEST_CFLAGS = -I$(TARGET_INCL)

TEST_LDFLAGS = -lssl -lcrypto -lpthread -lrt -llua  -lm
TEST_LDFLAGS += -L$(MSGPACK)/src/.libs -Wl,-l,:libmsgpack.a 
TEST_LDFLAGS += -L$(TARGET_LIB) -Wl,-l,:libcitrusleaf.a

###############################################################################
##  TARGETS                                                      		     ##
###############################################################################

.PHONY: test
test: test-build
	@$(TARGET_BIN)/test/client_test

.PHONY: test-valgrind
test-valgrind: test-build
	valgrind $(TEST_VALGRIND) $(TARGET_BIN)/test/client_test 1>&2 2>client_test-valgrind

.PHONY: test-build
test-build: test/client_test

.PHONY: test-clean
test-clean: 
	@rm -rf $(TARGET_BIN)/test
	@rm -rf $(TARGET_OBJ)/test

$(TARGET_OBJ)/test/%/%.o: CFLAGS = $(TEST_CFLAGS)
$(TARGET_OBJ)/test/%/%.o: LDFLAGS += $(TEST_LDFLAGS)
$(TARGET_OBJ)/test/%/%.o: $(SOURCE_TEST)/%/%.c
	$(object)

$(TARGET_OBJ)/test/%.o: CFLAGS = $(TEST_CFLAGS)
$(TARGET_OBJ)/test/%.o: LDFLAGS += $(TEST_LDFLAGS)
$(TARGET_OBJ)/test/%.o: $(SOURCE_TEST)/%.c
	$(object)

.PHONY: test/client_test
test/client_test: $(TARGET_BIN)/test/client_test
$(TARGET_BIN)/test/client_test: CFLAGS += $(TEST_CFLAGS)
$(TARGET_BIN)/test/client_test: LDFLAGS += $(TEST_LDFLAGS)
$(TARGET_BIN)/test/client_test: $(TEST_CLIENT:%=$(TARGET_OBJ)/test/%.o) $(TARGET_OBJ)/test/test.o  | build prepare
	$(executable) $(TARGET_LIB)/libcitrusleaf.a
