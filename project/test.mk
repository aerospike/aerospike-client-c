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
TEST_AEROSPIKE += exp_operate.c
TEST_AEROSPIKE += transaction.c
TEST_AEROSPIKE += transaction_async.c

TEST_SOURCE = $(wildcard $(addprefix $(SOURCE_TEST)/, $(TEST_AEROSPIKE)))

TEST_OBJECT = $(patsubst %.c,%.o,$(subst $(SOURCE_TEST)/,$(TARGET_TEST)/,$(TEST_SOURCE)))

###############################################################################
##  FLAGS                                                                    ##
###############################################################################

TEST_VALGRIND = --tool=memcheck --leak-check=yes --show-reachable=yes --num-callers=20 --track-fds=yes -v

TEST_CFLAGS = -I$(TARGET_INCL)

TEST_LDFLAGS = $(EXT_LDFLAGS)

ifeq ($(OS),Darwin)
  ifneq ($(wildcard /opt/homebrew/lib),)
    # Mac new homebrew external lib path
    TEST_LDFLAGS += -L/opt/homebrew/lib
  else
    # Mac old homebrew external lib path
    TEST_LDFLAGS += -L/usr/local/lib

    ifeq ($(EVENT_LIB),libevent)
      TEST_LDFLAGS += -L/usr/local/opt/libevent/lib
    endif
  endif

  ifneq ($(wildcard /opt/homebrew/opt/openssl/lib),)
    # Mac new homebrew openssl lib path
    TEST_LDFLAGS += -L/opt/homebrew/opt/openssl/lib
  else
    # Mac old homebrew openssl lib path
    TEST_LDFLAGS += -L/usr/local/opt/openssl/lib
  endif

  LINK_SUFFIX =
else ifeq ($(OS),FreeBSD)
  TEST_LDFLAGS += -L/usr/local/lib
  LINK_SUFFIX = -lrt
else
  TEST_LDFLAGS += -L/usr/local/lib
  LINK_SUFFIX = -lrt -ldl
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

TEST_LDFLAGS += -lssl -lcrypto -lpthread -lm -lz $(LINK_SUFFIX)

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

.PHONY: test-gdb
test-gdb: test-build
	gdb -ex=r --args $(TARGET_TEST)/aerospike_test $(AS_ARGS)

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
