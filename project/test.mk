###############################################################################
##  OBJECTS                                                                  ##
###############################################################################

TEST_AEROSPIKE = aerospike_test.c
TEST_AEROSPIKE += aerospike_batch/*.c
TEST_AEROSPIKE += aerospike_bit/*.c
TEST_AEROSPIKE += aerospike_index/*.c
TEST_AEROSPIKE += aerospike_geo/*.c
TEST_AEROSPIKE += aerospike_info/*.c
TEST_AEROSPIKE += aerospike_key/*.c
TEST_AEROSPIKE += aerospike_list/*.c
TEST_AEROSPIKE += aerospike_map/*.c
TEST_AEROSPIKE += aerospike_query/*.c
TEST_AEROSPIKE += aerospike_scan/*.c
TEST_AEROSPIKE += aerospike_udf/*.c
TEST_AEROSPIKE += policy/*.c
TEST_AEROSPIKE += util/*.c
TEST_AEROSPIKE += filter_exp.c
TEST_AEROSPIKE += predexp.c

TEST_SOURCE = $(wildcard $(addprefix $(SOURCE_TEST)/, $(TEST_AEROSPIKE)))

TEST_OBJECT = $(patsubst %.c,%.o,$(subst $(SOURCE_TEST)/,$(TARGET_TEST)/,$(TEST_SOURCE)))

###############################################################################
##  FLAGS                                                                    ##
###############################################################################

TEST_VALGRIND = --tool=memcheck --leak-check=yes --show-reachable=yes --num-callers=20 --track-fds=yes -v

TEST_CFLAGS = -I$(TARGET_INCL)

TEST_LDFLAGS = -L/usr/local/lib $(EXT_LDFLAGS) -lssl -lcrypto $(LIB_LUA) -lpthread -lm -lz

ifeq ($(OS),Darwin)
  TEST_LDFLAGS += -L/usr/local/opt/openssl/lib
  ifeq ($(USE_LUAJIT),1)
    TEST_LDFLAGS += -pagezero_size 10000 -image_base 100000000
  endif
else ifeq ($(OS),FreeBSD)
  TEST_LDFLAGS += -lrt
else
  TEST_LDFLAGS += -lrt -ldl
endif

ifeq ($(EVENT_LIB),libev)
  TEST_LDFLAGS += -lev
endif

ifeq ($(EVENT_LIB),libuv)
  TEST_LDFLAGS += -luv
endif

ifeq ($(EVENT_LIB),libevent)
  TEST_LDFLAGS += -levent_core -levent_pthreads
endif

AS_HOST := 127.0.0.1
AS_PORT := 3000
AS_ARGS := -h $(AS_HOST) -p $(AS_PORT)

###############################################################################
##  TARGETS                                                                  ##
###############################################################################

.PHONY: test
test: $(TARGET_TEST)/aerospike_test
	$(TARGET_TEST)/aerospike_test $(AS_ARGS)

.PHONY: test-valgrind
test-valgrind: test-build
	valgrind $(TEST_VALGRIND) $(TARGET_TEST)/aerospike_test $(AS_ARGS) 1>&2 2>client_test-valgrind

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
$(TARGET_TEST)/aerospike_test: $(TEST_OBJECT) $(TARGET_TEST)/test.o $(TARGET_LIB)/libaerospike.a | build prepare
	$(executable) $(TEST_LDFLAGS)
