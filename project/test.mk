###############################################################################
##  OBJECTS                                                      		 	 ##
###############################################################################

TEST_AEROSPIKE = aerospike_test.c
TEST_AEROSPIKE += aerospike_batch/*.c
TEST_AEROSPIKE += aerospike_index/*.c
TEST_AEROSPIKE += aerospike_info/*.c
TEST_AEROSPIKE += aerospike_key/*.c
TEST_AEROSPIKE += aerospike_query/*.c
TEST_AEROSPIKE += aerospike_scan/*.c
TEST_AEROSPIKE += aerospike_udf/*.c
TEST_AEROSPIKE += aerospike_ldt/*.c
TEST_AEROSPIKE += policy/*.c
TEST_AEROSPIKE += util/*.c

TEST_SOURCE = $(wildcard $(addprefix $(SOURCE_TEST)/, $(TEST_AEROSPIKE)))

TEST_OBJECT = $(patsubst %.c,%.o,$(subst $(SOURCE_TEST)/,$(TARGET_TEST)/,$(TEST_SOURCE)))

###############################################################################
##  FLAGS                                                      		         ##
###############################################################################

TEST_VALGRIND = --tool=memcheck --leak-check=yes --show-reachable=yes --num-callers=20 --track-fds=yes -v

TEST_CFLAGS = -I$(TARGET_INCL)

ifeq ($(OS),Darwin)
TEST_LDFLAGS = -L/usr/local/lib -lssl -lcrypto -llua -lpthread -lm
else
TEST_LDFLAGS = -lssl -lcrypto -llua -lpthread -lm -lrt -lz
endif

AS_HOST := 127.0.0.1
AS_PORT := 3000
AS_ARGS := -h $(AS_HOST) -p $(AS_PORT)

###############################################################################
##  TARGETS                                                      		     ##
###############################################################################

.PHONY: test
test: $(TARGET_TEST)/aerospike_test
	$(TARGET_TEST)/aerospike_test $(AS_ARGS)

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
	$(executable) $(TARGET_LIB)/libaerospike.a $(TEST_LDFLAGS)
