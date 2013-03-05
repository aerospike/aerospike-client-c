###############################################################################
include project/settings.makefile
###############################################################################

###############################################################################
##  FLAGS                                                                    ##
###############################################################################

# To Override Optimization Level, run make with the "O" variable set.
# 	make O=0

O=4

CFLAGS = -O$(O)

CC_FLAGS = -std=gnu99 -g -rdynamic -Wall 
CC_FLAGS += -fno-common -fno-strict-aliasing -fPIC 
CC_FLAGS += -DMARCH_$(ARCH) -D_FILE_OFFSET_BITS=64 
CC_FLAGS += -D_REENTRANT -D_GNU_SOURCE -DMEM_COUNT

LD_FLAGS = -lm

INC_PATH += modules/common/$(TARGET_INCL)
INC_PATH += modules/mod-lua/$(TARGET_INCL)
INC_PATH += $(MSGPACK_PATH)/src

INSTALL_LIB = /usr/lib

ifndef MSGPACK_PATH
$(warning *** env MSGPACK_PATH not set. Using modules/msgpack as default)
MSGPACK_PATH := modules/msgpack
endif

ifndef JANSSON_PATH
$(error *** env JANSSON_PATH is not set. Please download, build and set JANSSON_PATH)
endif

SUBMODULES = 
SUBMODULES += $(MODULES)/common
SUBMODULES += $(MODULES)/mod-lua
SUBMODULES += $(MODULES)/msgpack

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
CITRUSLEAF += cl_sindex.o
CITRUSLEAF += cl_scan.o
CITRUSLEAF += cl_udf.o
CITRUSLEAF += proto.o

OBJECTS = $(wildcard modules/mod-lua/$(TARGET_OBJ)/*.o)
OBJECTS += $(wildcard modules/common/$(TARGET_OBJ)/client/*.o)
OBJECTS += $(wildcard modules/common/$(TARGET_OBJ)/shared/*.o)
OBJECTS += $(wildcard $(MSGPACK_PATH)/src/.libs/*.o)

###############################################################################
##  MAIN TARGETS                                                             ##
###############################################################################

all: modules build prepare

.PHONY: prepare
prepare: $(TARGET_INCL)

.PHONY: install
install: $(INSTALL_LIB)/libcitrusleaf.so $(INSTALL_LIB)/libcitrusleaf.a

.PHONY: build
build: libcitrusleaf.a libcitrusleaf.so

.PHONY: build-clean
build-clean:
	@rm -rf $(TARGET_BIN)
	@rm -rf $(TARGET_LIB)

.PHONY: libcitrusleaf libcitrusleaf.a libcitrusleaf.so
libcitrusleaf: libcitrusleaf.a libcitrusleaf.so
libcitrusleaf.a: $(TARGET_LIB)/libcitrusleaf.a
libcitrusleaf.so: $(TARGET_LIB)/libcitrusleaf.so

###############################################################################
##  BUILD TARGETS                                                            ##
###############################################################################

#$(TARGET_LIB)/libcitrusleaf.a $(TARGET_LIB)/libcitrusleaf.so: $(CITRUSLEAF:%=$(TARGET_OBJ)/%) $(OBJECTS) | modules
$(TARGET_LIB)/libcitrusleaf.so: $(CITRUSLEAF:%=$(TARGET_OBJ)/%) | modules
	$(library) $(OBJECTS)
$(TARGET_LIB)/libcitrusleaf.a: $(CITRUSLEAF:%=$(TARGET_OBJ)/%) | modules
	$(archive) $(OBJECTS)

$(TARGET_INCL): $(SOURCE_INCL)/*.h modules/mod-lua/$(TARGET_INCL)/*.h modules/common/$(TARGET_INCL)/*.h | modules
	@if [ ! -e "$(TARGET_INCL)/citrusleaf" ]; then mkdir -p $(TARGET_INCL)/citrusleaf; fi
	cp -pR $(SOURCE_INCL)/*.h $(TARGET_INCL)/citrusleaf/.
	cp -p modules/mod-lua/src/include/*.h $(TARGET_INCL)/citrusleaf/.
	cp -p modules/common/$(TARGET_INCL)/*.h $(TARGET_INCL)/.
	cp -p modules/common/$(TARGET_INCL)/citrusleaf/*.h $(TARGET_INCL)/citrusleaf/.

$(INSTALL_LIB)/libcitrusleaf.a: $(TARGET_LIB)/libcitrusleaf.a
	cp -p $(TARGET_LIB)/libcitrusleaf.a /usr/lib/.

$(INSTALL_LIB)/libcitrusleaf.so: $(TARGET_LIB)/libcitrusleaf.so
	cp -p $(TARGET_LIB)/libcitrusleaf.so /usr/lib/.

###############################################################################
##  MODULE TARGETS                                                           ##
###############################################################################

.PHONY: modules
modules: modules/common modules/mod-lua modules/msgpack

.PHONY: modules-prepare
modules-prepare: modules/common/$(TARGET_INCL)/*.h

.PHONY: modules-clean
modules-clean: 
	$(MAKE) -e -C modules/common clean
	$(MAKE) -e -C modules/mod-lua clean

##
## SUBMODULE: common
## 

modules/common/$(TARGET_LIB)/libcf-shared.a:
	$(MAKE) -e -C modules/common libcf-shared.a MEM_COUNT=1

modules/common/$(TARGET_LIB)/libcf-client.a:
	$(MAKE) -e -C modules/common libcf-client.a MEM_COUNT=1

modules/common/$(TARGET_INCL)/*.h:
	$(MAKE) -e -C modules/common prepare MEM_COUNT=1

.PHONY: modules/common
modules/common: modules/common/$(TARGET_LIB)/libcf-shared.a modules/common/$(TARGET_LIB)/libcf-client.a modules/common/$(TARGET_INCL)/*.h

##
## SUBMODULE: mod-lua
## 

modules/mod-lua/$(TARGET_LIB)/libmod_lua.a:
	$(MAKE) -e -C modules/mod-lua libmod_lua.a MEM_COUNT=1
	$(MAKE) -e -C modules/mod-lua libas_types.a MEM_COUNT=1

modules/mod-lua/$(TARGET_INCL)/*.h:
	$(MAKE) -e -C modules/mod-lua prepare MEM_COUNT=1

.PHONY: modules/mod-lua
modules/mod-lua: modules/mod-lua/$(TARGET_LIB)/libmod_lua.a modules/mod-lua/$(TARGET_INCL)/*.h

##
## SUBMODULE: msgpack
## 

modules/msgpack/Makefile: 
	cd $(MSGPACK_PATH) && ./configure

modules/msgpack/src/.libs/libmsgpackc.a: modules/msgpack/Makefile
	cd $(MSGPACK_PATH) && make CFLAGS="-fPIC"

.PHONY: modules/msgpack
modules/msgpack: modules/msgpack/src/.libs/libmsgpackc.a

###############################################################################
##  TEST TARGETS                                                      		 ##
###############################################################################

TEST_VALGRIND = --tool=memcheck --leak-check=yes --show-reachable=yes --num-callers=20 --track-fds=yes -v

TEST_CFLAGS = -I$(TARGET_INCL) -I$(JANSSON_PATH)/src

TEST_LDFLAGS = -lssl -lcrypto -lpthread -lrt -llua  -lm
TEST_LDFLAGS += -L$(TARGET_LIB) -Wl,-l,:libcitrusleaf.a
TEST_LDFLAGS += -L$(JANSSON_PATH)/src/.libs -Wl,-l,:libjansson.a 
TEST_LDFLAGS += -L$(MSGPACK_PATH)/src/.libs -Wl,-l,:libmsgpack.a 

#-----#

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

TEST_CLIENT = client_test
TEST_CLIENT += $(TEST_UTIL) 
TEST_CLIENT += $(TEST_KV) 
TEST_CLIENT += $(TEST_RECORD) 
TEST_CLIENT += $(TEST_STREAM)

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

###############################################################################
include project/rules.makefile
###############################################################################
