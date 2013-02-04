include project/build.makefile

CFLAGS = -std=gnu99 -g -O3 -fno-common -fno-strict-aliasing -rdynamic -fPIC -Wall $(AS_CFLAGS) -DMARCH_$(ARCH) -D_FILE_OFFSET_BITS=64 -D_REENTRANT -D_GNU_SOURCE
LDFLAGS = 

MODULES = common mod-lua msgpack

INC_PATH += modules/common/$(TARGET_INCL)
INC_PATH += $(MSGPACK_PATH)/src
INC_PATH += modules/mod-lua/src/include

ifeq ($(MEM_COUNT),1)
	LDFLAGS += -lm
	CFLAGS += -DMEM_COUNT
endif
	
# ifndef MSGPACK_PATH
# $(warning *** env MSGPACK_PATH not set. Using modules/msgpack as default)
# MSGPACK_PATH := modules/msgpack
# endif

# ifndef JANSSON_PATH
# $(error *** env JANSSON_PATH is not set. Please download, build and set JANSSON_PATH)
# endif

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

##
## MAIN
##
.DEFAULT_GOAL := all

all: libcitrusleaf.so libcitrusleaf.a prepare test 

libcitrusleaf.o: $(call objects, $(CITRUSLEAF))

libcitrusleaf.a: | msgpack mod-lua common libcitrusleaf.o $(TARGET_LIB)
	$(call archive, $(empty), $(empty), $(empty), $(empty), $(OBJECTS))

libcitrusleaf.so: | msgpack mod-lua common libcitrusleaf.o $(TARGET_LIB)
	$(call library, $(empty), $(empty), $(empty), $(empty), $(OBJECTS))

prepare:
	if [ ! -e "$(TARGET_INCL)/citrusleaf" ]; then mkdir -p $(TARGET_INCL)/citrusleaf; fi
	cp -pR $(SOURCE_INCL)/*.h $(TARGET_INCL)/citrusleaf/.
	cp -p modules/mod-lua/src/include/as_*.h $(TARGET_INCL)/citrusleaf/.
	cp -p modules/common/$(TARGET_INCL)/*.h $(TARGET_INCL)/.
	cp -p modules/common/$(TARGET_INCL)/citrusleaf/*.h $(TARGET_INCL)/citrusleaf/.

install:
	cp -p $(TARGET_LIB)/* /usr/lib/.
ifeq ($(wildcard /usr/bin/ascli),)
	ln -s /opt/citrusleaf/bin/ascli /usr/bin/ascli
endif

gendoc:
	cd doc; make html

##
## SUBMODULES
##

msgpack-configure: $(MSGPACK_PATH)/configure
	cd $(MSGPACK_PATH) && ./configure

msgpack-make: $(MSGPACK_PATH)
	make -C $(MSGPACK_PATH) CFLAGS="-fPIC"
	
msgpack: 
ifndef MSGPACK_PATH
	$(warning *** env MSGPACK_PATH not set. Using modules/msgpack as default)
	make msgpack-make CFLAGS="-fPIC" MSGPACK_PATH=modules/msgpack
MSGPACK_PATH=modules/msgpack
else
	make msgpack-make CFLAGS="-fPIC" MSGPACK_PATH=$(MSGPACK_PATH)
endif
	

jansson:
ifndef JANSSON_PATH
	$(error env JANSSON_PATH is not set. Please download, build and set JANSSON_PATH)
endif



mod-lua: 
	make -C modules/mod-lua libtypes.o MEM_COUNT=$(MEM_COUNT) 

common: 
	make -C modules/common client_only MEM_COUNT=$(MEM_COUNT) 


##
## TEST
##
TEST_FLAGS += -L$(TARGET_LIB) -Wl,-l,:libcitrusleaf.a -L$(JANSSON_PATH)/src/.libs -Wl,-l,:libjansson.a -lcrypto -lssl -L$(MSGPACK_PATH)/src/.libs -Wl,-l,:libmsgpack.a -lpthread -lrt 
 
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
 
$(TARGET_OBJ)/test/%/%.o: $(SOURCE_TEST)/%/%.c
	$(call object, $(empty), $(empty), $(empty), $(empty), $(TEST_FLAGS))
 
$(TARGET_OBJ)/test/%.o: $(SOURCE_TEST)/%.c
	$(call object, $(empty), $(empty), $(empty), $(empty), $(TEST_FLAGS))
 
test/types: $(TEST_TYPES:%=$(TARGET_OBJ)/test/types/%.o) $(TARGET_OBJ)/test/test.o
	$(call executable, $(empty), $(empty), $(empty), $(empty), $(TEST_FLAGS))
 
test/client: $(TEST_CLIENT:%=$(TARGET_OBJ)/test/client/%.o) $(TARGET_OBJ)/test/test.o
	$(call executable, $(empty), $(empty), $(empty), $(empty), $(TEST_FLAGS))

test/udf: $(TEST_UDF:%=$(TARGET_OBJ)/test/udf/%.o) $(TARGET_OBJ)/test/test.o
	$(call executable, $(empty), $(empty), $(empty), $(empty), $(TEST_FLAGS))

test: CFLAGS += -I$(TARGET_INCL) -I$(JANSSON_PATH)/src
test: test/types test/client test/udf

test-clean: 
	rm -rf $(TARGET_BIN)/test
	rm -rf $(TARGET_OBJ)/test

test-run: test
	@$(TARGET_BIN)/test/types
	@$(TARGET_BIN)/test/client
	@$(TARGET_BIN)/test/udf