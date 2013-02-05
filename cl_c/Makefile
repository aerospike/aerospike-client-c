include project/build.makefile

###############################################################################
##  FLAGS                                                                    ##
###############################################################################

CFLAGS = -std=gnu99 -g -O3 -fno-common -fno-strict-aliasing -rdynamic -fPIC -Wall $(AS_CFLAGS) -DMARCH_$(ARCH) -D_FILE_OFFSET_BITS=64 -D_REENTRANT -D_GNU_SOURCE
LDFLAGS = 

MODULES := common mod-lua msgpack

INC_PATH += modules/common/$(TARGET_INCL)
INC_PATH += $(MSGPACK_PATH)/src
INC_PATH += modules/mod-lua/src/include

ifeq ($(MEM_COUNT),1)
	LDFLAGS += -lm
	CFLAGS += -DMEM_COUNT
endif

INSTALL_LIB = /usr/lib

# ifndef MSGPACK_PATH
# $(warning *** env MSGPACK_PATH not set. Using modules/msgpack as default)
MSGPACK_PATH := modules/msgpack
# endif

# ifndef JANSSON_PATH
# $(error *** env JANSSON_PATH is not set. Please download, build and set JANSSON_PATH)
# endif

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

OBJECTS = $(TARGET_OBJ)/*.o modules/mod-lua/$(TARGET_OBJ)/*.o modules/common/$(TARGET_OBJ)/client/*.o modules/common/$(TARGET_OBJ)/shared/*.o $(MSGPACK_PATH)/src/.libs/*.o

###############################################################################
##  MAIN TARGETS                                                             ##
###############################################################################

all: modules build prepare

.PHONY: modules
modules: $(MODULES:%=modules/%)

.PHONY: build build-clean
build: libcitrusleaf.a libcitrusleaf.so
build-clean:
	@rm -rf $(TARGET_BIN)
	@rm -rf $(TARGET_LIB)

.PHONY: prepare
prepare: $(TARGET_INCL)

.PHONY: install
install: $(INSTALL_LIB)/libcitrusleaf.so $(INSTALL_LIB)/libcitrusleaf.a

.PHONY: libcitrusleaf.a libcitrusleaf.so
libcitrusleaf.a: $(TARGET_LIB)/libcitrusleaf.a
libcitrusleaf.so: $(TARGET_LIB)/libcitrusleaf.so

###############################################################################
##  BUILD TARGETS                                                            ##
###############################################################################

$(TARGET_LIB)/libcitrusleaf.a: $(CITRUSLEAF:%=$(TARGET_OBJ)/%) | $(TARGET_LIB) modules
	$(call archive, $(empty), $(empty), $(empty), $(empty), $(OBJECTS))

$(TARGET_LIB)/libcitrusleaf.so: $(CITRUSLEAF:%=$(TARGET_OBJ)/%) | $(TARGET_LIB) modules
	$(call library, $(empty), $(empty), $(empty), $(empty))

$(TARGET_INCL): modules/mod-lua/$(TARGET_INCL) modules/common/$(TARGET_INCL)
	if [ ! -e "$(TARGET_INCL)/citrusleaf" ]; then mkdir -p $(TARGET_INCL)/citrusleaf; fi
	cp -pR $(SOURCE_INCL)/*.h $(TARGET_INCL)/citrusleaf/.
	cp -p modules/mod-lua/src/include/as_*.h $(TARGET_INCL)/citrusleaf/.
	cp -p modules/common/$(TARGET_INCL)/*.h $(TARGET_INCL)/.
	cp -p modules/common/$(TARGET_INCL)/citrusleaf/*.h $(TARGET_INCL)/citrusleaf/.

$(INSTALL_LIB)/libcitrusleaf.so:
	cp -p $(TARGET_LIB)/libcitrusleaf.so /usr/lib/.

$(INSTALL_LIB)/libcitrusleaf.a:
	cp -p $(TARGET_LIB)/libcitrusleaf.a /usr/lib/.

###############################################################################
##  SUB-MODULES TARGETS                                                      ##
###############################################################################

.PHONY: modules/common
modules/common:
	make -C modules/common all MEM_COUNT=1
modules/common/$(TARGET_INCL):
	make -C modules/common prepare MEM_COUNT=1

.PHONY: modules/mod-lua
modules/mod-lua:
	make -C modules/mod-lua all MEM_COUNT=1
modules/mod-lua/$(TARGET_INCL):
	make -C modules/mod-lua prepare MEM_COUNT=1

modules/msgpack/Makefile: 
	cd $(MSGPACK_PATH) && ./configure

modules/msgpack/src/.libs/libmsgpackc.a: modules/msgpack/Makefile
	cd $(MSGPACK_PATH) && make

.PHONY: modules/msgpack
modules/msgpack: modules/msgpack/src/.libs/libmsgpackc.a

###############################################################################
##  TEST TARGETS                                                      		 ##
###############################################################################

TEST_FLAGS = -L$(TARGET_LIB) -Wl,-l,:libcitrusleaf.a -L$(JANSSON_PATH)/src/.libs -Wl,-l,:libjansson.a -lcrypto -lssl -L$(MSGPACK_PATH)/src/.libs -Wl,-l,:libmsgpack.a -lpthread -lrt 
 
TEST_TYPES = types
TEST_TYPES += types_integer
TEST_TYPES += types_string
TEST_TYPES += types_arraylist
TEST_TYPES += types_linkedlist
TEST_TYPES += types_hashmap
 
TEST_CLIENT = client
TEST_CLIENT += client_string
 
TEST_UDF = udf
TEST_UDF += udf_basics
TEST_UDF += udf_lists

.PHONY: test test-clean
test: CFLAGS += -I$(TARGET_INCL) -I$(JANSSON_PATH)/src
test: test/types test/client test/udf 
	@$(TARGET_BIN)/test/types
	@$(TARGET_BIN)/test/client
	@$(TARGET_BIN)/test/udf
test-clean: 
	@rm -rf $(TARGET_BIN)/test
	@rm -rf $(TARGET_OBJ)/test

.PHONY: test/types
test/types: $(TARGET_BIN)/test/types

.PHONY: test/client
test/client: $(TARGET_BIN)/test/client

.PHONY: test/udf
test/udf: $(TARGET_BIN)/test/udf



$(TARGET_OBJ)/test/%/%.o: $(SOURCE_TEST)/%/%.c
	$(call object, $(empty), $(empty), $(empty), $(empty), $(TEST_FLAGS))
 
$(TARGET_OBJ)/test/%.o: $(SOURCE_TEST)/%.c
	$(call object, $(empty), $(empty), $(empty), $(empty), $(TEST_FLAGS))

$(TARGET_BIN)/test/types: $(TEST_TYPES:%=$(TARGET_OBJ)/test/types/%.o) $(TARGET_OBJ)/test/test.o | libcitrusleaf.a prepare
	$(call executable, $(empty), $(empty), $(empty), $(empty), $(TEST_FLAGS))
 
$(TARGET_BIN)/test/client: $(TEST_CLIENT:%=$(TARGET_OBJ)/test/client/%.o) $(TARGET_OBJ)/test/test.o | libcitrusleaf.a prepare
	$(call executable, $(empty), $(empty), $(empty), $(empty), $(TEST_FLAGS))

$(TARGET_BIN)/test/udf: $(TEST_UDF:%=$(TARGET_OBJ)/test/udf/%.o) $(TARGET_OBJ)/test/test.o | libcitrusleaf.a prepare
	$(call executable, $(empty), $(empty), $(empty), $(empty), $(TEST_FLAGS))
