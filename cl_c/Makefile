include project/build.makefile

###############################################################################
##  FLAGS                                                                    ##
###############################################################################

# CFLAGS = -O3

CC_FLAGS = -std=gnu99 -g -rdynamic -Wall 
CC_FLAGS += -fno-common -fno-strict-aliasing -fPIC 
CC_FLAGS += -DMARCH_$(ARCH) -D_FILE_OFFSET_BITS=64 
CC_FLAGS += -D_REENTRANT -D_GNU_SOURCE -DMEM_COUNT

LD_FLAGS = -lm

INC_PATH += modules/common/$(TARGET_INCL)
INC_PATH += modules/mod-lua/$(TARGET_INCL)
INC_PATH += $(MSGPACK_PATH)/src

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

OBJECTS = modules/mod-lua/$(TARGET_OBJ)/*.o 
OBJECTS += modules/common/$(TARGET_OBJ)/client/*.o 
OBJECTS += modules/common/$(TARGET_OBJ)/shared/*.o 
OBJECTS += $(MSGPACK_PATH)/src/.libs/*.o

###############################################################################
##  MAIN TARGETS                                                             ##
###############################################################################

all: modules build prepare test

.PHONY: modules
modules: modules/common modules/mod-lua modules/msgpack

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
	$(archive) $(OBJECTS)

$(TARGET_LIB)/libcitrusleaf.so: $(CITRUSLEAF:%=$(TARGET_OBJ)/%) | $(TARGET_LIB) modules
	$(library) $(OBJECTS)

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
	$(MAKE) -e -C modules/common all MEM_COUNT=1
modules/common/$(TARGET_INCL):
	$(MAKE) -e -C modules/common prepare MEM_COUNT=1

.PHONY: modules/mod-lua
modules/mod-lua:
	$(MAKE) -e -C modules/mod-lua all MEM_COUNT=1
modules/mod-lua/$(TARGET_INCL):
	$(MAKE) -e -C modules/mod-lua prepare MEM_COUNT=1

modules/msgpack/Makefile: 
	cd $(MSGPACK_PATH) && ./configure

modules/msgpack/src/.libs/libmsgpackc.a: modules/msgpack/Makefile
	cd $(MSGPACK_PATH) && make

.PHONY: modules/msgpack
modules/msgpack: modules/msgpack/src/.libs/libmsgpackc.a

###############################################################################
##  TEST TARGETS                                                      		 ##
###############################################################################

TEST_CFLAGS = -I$(TARGET_INCL) -I$(JANSSON_PATH)/src

TEST_LDFLAGS = -lpthread -lrt -lcrypto -lssl -llua 
TEST_LDFLAGS += -L$(TARGET_LIB) -Wl,-l,:libcitrusleaf.a
TEST_LDFLAGS += -L$(JANSSON_PATH)/src/.libs -Wl,-l,:libjansson.a 
TEST_LDFLAGS += -L$(MSGPACK_PATH)/src/.libs -Wl,-l,:libmsgpack.a 
 
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

.PHONY: test test-clean test/types test/client test/udf

test: test/types test/client test/udf 
	@$(TARGET_BIN)/test/types
	@$(TARGET_BIN)/test/client
	@$(TARGET_BIN)/test/udf

test-clean: 
	@rm -rf $(TARGET_BIN)/test
	@rm -rf $(TARGET_OBJ)/test

test/types: $(TARGET_BIN)/test/types
test/client: $(TARGET_BIN)/test/client
test/udf: $(TARGET_BIN)/test/udf


$(TARGET_OBJ)/test/%/%.o: CFLAGS = $(TEST_CFLAGS)
$(TARGET_OBJ)/test/%/%.o: LDFLAGS += $(TEST_LDFLAGS)
$(TARGET_OBJ)/test/%/%.o: $(SOURCE_TEST)/%/%.c
	$(object)

$(TARGET_OBJ)/test/%.o: CFLAGS = $(TEST_CFLAGS)
$(TARGET_OBJ)/test/%.o: LDFLAGS += $(TEST_LDFLAGS)
$(TARGET_OBJ)/test/%.o: $(SOURCE_TEST)/%.c
	$(object)

$(TARGET_BIN)/test/types: CFLAGS = $(TEST_CFLAGS)
$(TARGET_BIN)/test/types: LDFLAGS += $(TEST_LDFLAGS)
$(TARGET_BIN)/test/types: $(TEST_TYPES:%=$(TARGET_OBJ)/test/types/%.o) $(TARGET_OBJ)/test/test.o $(TARGET_LIB)/libcitrusleaf.a | prepare
	$(executable) 

$(TARGET_BIN)/test/client: CFLAGS += $(TEST_CFLAGS)
$(TARGET_BIN)/test/client: LDFLAGS += $(TEST_LDFLAGS)
$(TARGET_BIN)/test/client: $(TEST_CLIENT:%=$(TARGET_OBJ)/test/client/%.o) $(TARGET_OBJ)/test/test.o $(TARGET_LIB)/libcitrusleaf.a | prepare
	$(executable)

$(TARGET_BIN)/test/udf: CFLAGS = $(TEST_CFLAGS)
$(TARGET_BIN)/test/udf: LDFLAGS = $(TEST_LDFLAGS)
$(TARGET_BIN)/test/udf: $(TEST_UDF:%=$(TARGET_OBJ)/test/udf/%.o) $(TARGET_OBJ)/test/test.o $(TARGET_LIB)/libcitrusleaf.a | prepare
	$(executable)
