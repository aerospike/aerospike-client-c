ROOT = ..
###############################################################################
include $(ROOT)/project/settings.makefile
include modules.makefile
###############################################################################

###############################################################################
##  FLAGS                                                                    ##
###############################################################################

# To Override Optimization Level, run make with the "O" variable set.
# 	make O=0

O=4
MEM_COUNT=1

CFLAGS = -O$(O) -DMEM_COUNT=$(MEM_COUNT)

CC_FLAGS = -std=gnu99 -g -rdynamic -Wall 
CC_FLAGS += -fno-common -fno-strict-aliasing -fPIC 
CC_FLAGS += -DMARCH_$(ARCH) -D_FILE_OFFSET_BITS=64 
CC_FLAGS += -D_REENTRANT -D_GNU_SOURCE -DMEM_COUNT

LD_FLAGS = -lm

INC_PATH += $(COMMON)/$(TARGET_INCL)
INC_PATH += $(MOD_LUA)/$(TARGET_INCL)
INC_PATH += $(MSGPACK)/src

###############################################################################
##  OBJECTS                                                                  ##
###############################################################################

CITRUSLEAF = 
CITRUSLEAF += citrusleaf.o
CITRUSLEAF += cl_async.o
CITRUSLEAF += cl_batch.o
CITRUSLEAF += cl_cluster.o
CITRUSLEAF += cl_info.o
CITRUSLEAF += cl_lookup.o
CITRUSLEAF += cl_partition.o
CITRUSLEAF += cl_query.o
CITRUSLEAF += as_scan.o
CITRUSLEAF += cl_sindex.o
CITRUSLEAF += cl_scan.o
CITRUSLEAF += cl_udf.o
CITRUSLEAF += proto.o
CITRUSLEAF += aerospike_lstack.o
CITRUSLEAF += aerospike_lset.o

OBJECTS =
OBJECTS += $(wildcard $(MOD_LUA)/$(TARGET_OBJ)/*.o)
OBJECTS += $(wildcard $(COMMON)/$(TARGET_OBJ)/client/*.o)
OBJECTS += $(wildcard $(COMMON)/$(TARGET_OBJ)/shared/*.o)
OBJECTS += $(wildcard $(MSGPACK)/src/.libs/*.o)

###############################################################################
##  MAIN TARGETS                                                             ##
###############################################################################

all: modules build prepare

.PHONY: prepare
prepare: $(MOD_LUA)/$(TARGET_INCL)/*.h $(COMMON)/$(TARGET_INCL)/citrusleaf/*.h $(COMMON)/$(TARGET_INCL)/*.h $(TARGET_INCL)/citrusleaf/*.h 

.PHONY: build
build: libcitrusleaf

.PHONY: build-clean
build-clean:
	@rm -rf $(TARGET_BIN)
	@rm -rf $(TARGET_LIB)

.PHONY: libcitrusleaf
libcitrusleaf: libcitrusleaf.a libcitrusleaf.so

.PHONY: libcitrusleaf.a libcitrusleaf.so
libcitrusleaf.a: $(TARGET_LIB)/libcitrusleaf.a
libcitrusleaf.so: $(TARGET_LIB)/libcitrusleaf.so

###############################################################################
##  BUILD TARGETS                                                            ##
###############################################################################

$(TARGET_OBJ)/%.o: $(COMMON)/$(TARGET_LIB)/libcf-client.a $(COMMON)/$(TARGET_LIB)/libcf-shared.a $(MOD_LUA)/$(TARGET_LIB)/libmod_lua.a $(SOURCE_MAIN)/%.c $(SOURCE_INCL)/citrusleaf/*.h
	$(object)

$(TARGET_LIB)/libcitrusleaf.so: $(CITRUSLEAF:%=$(TARGET_OBJ)/%) $(TARGET_OBJ)/version.o | modules
	$(library) $(OBJECTS)

$(TARGET_LIB)/libcitrusleaf.a: $(CITRUSLEAF:%=$(TARGET_OBJ)/%) $(TARGET_OBJ)/version.o | modules
	$(archive) $(OBJECTS)

$(TARGET_INCL)/citrusleaf: | $(TARGET_INCL)
	mkdir -p $@

$(TARGET_INCL)/citrusleaf/%.h:: $(SOURCE_INCL)/%.h | $(TARGET_INCL)/citrusleaf
	cp -p $^ $(TARGET_INCL)/citrusleaf

$(TARGET_INCL)/%.h:: $(COMMON)/%.h | $(TARGET_INCL)
	$(noop)

$(TARGET_INCL)/citrusleaf/%.h:: $(COMMON)/$(TARGET_INCL)/%.h | $(TARGET_INCL)/citrusleaf
	$(noop)

$(TARGET_INCL)/citrusleaf/%.h:: $(MOD_LUA)/$(TARGET_INCL)/%.h | $(TARGET_INCL)/citrusleaf
	$(noop)

###############################################################################
##  MODULE TARGETS                                                           ##
###############################################################################

.PHONY: modules
modules: $(COMMON) $(MOD_LUA) $(MSGPACK)

.PHONY: modules-prepare
modules-prepare: $(COMMON)/$(TARGET_INCL)/*.h $(COMMON)/$(TARGET_INCL)/citrusleaf/*.h $(MOD_LUA)/$(TARGET_INCL)/*.h

.PHONY: modules-clean
modules-clean: 
	$(MAKE) -e -C $(COMMON) clean
	$(MAKE) -e -C $(MOD_LUA) clean

##
## SUBMODULE: common
## 

$(COMMON)/$(TARGET_LIB)/libcf-shared.a:
	$(MAKE) -e -C $(COMMON) libcf-shared.a

$(COMMON)/$(TARGET_LIB)/libcf-client.a:
	$(MAKE) -e -C $(COMMON) libcf-client.a

$(COMMON)/$(TARGET_INCL)/%.h: $(COMMON)/$(SOURCE_INCL)/%.h | $(TARGET_INCL)/citrusleaf
	$(MAKE) -e -C $(COMMON) prepare
	cp -p $^ $(TARGET_INCL)

$(COMMON)/$(TARGET_INCL)/citrusleaf/%.h: $(COMMON)/$(SOURCE_INCL)/client/%.h | $(TARGET_INCL)/citrusleaf
	$(MAKE) -e -C $(COMMON) prepare
	cp -p $^ $(TARGET_INCL)/citrusleaf

.PHONY: common $(COMMON)
$(COMMON): $(COMMON)/$(TARGET_LIB)/libcf-shared.a $(COMMON)/$(TARGET_LIB)/libcf-client.a $(COMMON)/$(TARGET_INCL)/*.h
common: $(COMMON)

##
## SUBMODULE: mod-lua
## 

$(MOD_LUA)/$(TARGET_LIB)/libmod_lua.a:
	$(MAKE) -e -C $(MOD_LUA) libmod_lua.a
	$(MAKE) -e -C $(MOD_LUA) libas_types.a

$(MOD_LUA)/$(TARGET_INCL)/%.h: $(MOD_LUA)/$(SOURCE_INCL)/%.h | $(TARGET_INCL)/citrusleaf
	$(MAKE) -e -C $(MOD_LUA) prepare
	cp -p $^ $(TARGET_INCL)/citrusleaf

.PHONY: mod_lua $(MOD_LUA)
$(MOD_LUA): $(MOD_LUA)/$(TARGET_LIB)/libmod_lua.a $(MOD_LUA)/$(TARGET_INCL)/*.h
mod_lua: $(MOD_LUA)

##
## SUBMODULE: msgpack
## 

$(MSGPACK)/Makefile: 
	cd $(MSGPACK) && ./configure

$(MSGPACK)/src/.libs/libmsgpackc.a: $(MSGPACK)/Makefile
	cd $(MSGPACK) && $(MAKE) CFLAGS="-fPIC"

.PHONY: msgpack $(MSGPACK)
$(MSGPACK): $(MSGPACK)/src/.libs/libmsgpackc.a
msgpack: $(MSGPACK)

###############################################################################
##  TEST TARGETS                                                      		 ##
###############################################################################

TEST_KV = 
TEST_KV += kv/kv_string
 
# TEST_RECORD = 
# TEST_RECORD += record/record_basics
# TEST_RECORD += record/record_lists
 
# TEST_STREAM = 
# TEST_STREAM += stream/stream_simple
# TEST_STREAM += stream/stream_ads

TEST_UTIL = 
TEST_UTIL += util/udf
TEST_UTIL += util/consumer_stream
TEST_UTIL += util/producer_stream
TEST_UTIL += util/map_rec
TEST_UTIL += util/test_aerospike
TEST_UTIL += util/test_logger

TEST_LSTACK = 
TEST_LSTACK += lstack/lstack_advanced
TEST_LSTACK += lstack/lstack_basics
TEST_LSTACK += lstack/lstack_operations
TEST_LSTACK += lstack/lstack_test
TEST_LSTACK += lstack/lstack_util
TEST_LSTACK += lstack/test_config
# TEST_LSTACK += lstack/test_log

# TEST_LSET =
# TEST_LSET += lset/lset_basics
# TEST_LSET += lset/lset_small
# TEST_LSET += lset/lset_medium
# TEST_LSET += lset/lset_large

# TEST_LOLIST =
# TEST_LOLIST += lolist/lol_basics
# TEST_LOLIST += lolist/lol_small
# TEST_LOLIST += lolist/lol_medium
# TEST_LOLIST += lolist/lol_large


TEST_CLIENT = client_test
TEST_CLIENT += $(TEST_UTIL) 
TEST_CLIENT += $(TEST_LSTACK) 
# TEST_CLIENT += $(TEST_KV) 
# TEST_CLIENT += $(TEST_RECORD) 
# TEST_CLIENT += $(TEST_STREAM)
# TEST_CLIENT += $(TEST_LSO)

#-----#

TEST_VALGRIND = --tool=memcheck --leak-check=yes --show-reachable=yes --num-callers=20 --track-fds=yes -v

TEST_CFLAGS = -I$(TARGET_INCL)

TEST_LDFLAGS = -lssl -lcrypto -lpthread -lrt -llua  -lm
TEST_LDFLAGS += -L$(TARGET_LIB) -Wl,-l,:libcitrusleaf.a
TEST_LDFLAGS += -L$(MSGPACK)/src/.libs -Wl,-l,:libmsgpack.a 

#-----#

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
$(TARGET_BIN)/test/client_test: $(TEST_CLIENT:%=$(TARGET_OBJ)/test/%.o) $(TARGET_OBJ)/test/test.o $(TARGET_LIB)/libcitrusleaf.a | build prepare
	$(executable)

###############################################################################
include $(RULES)
###############################################################################
