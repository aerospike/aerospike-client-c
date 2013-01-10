include project/build.makefile

ifndef MSGPACK_PATH
MSGPACK_PATH = modules/msgpack
endif

CFLAGS = -std=gnu99 -g -O3 -fno-common -fno-strict-aliasing -rdynamic -fPIC -Wall $(AS_CFLAGS) -DMARCH_$(ARCH) -D_FILE_OFFSET_BITS=64 -D_REENTRANT -D_GNU_SOURCE
LDFLAGS = 

MODULES = common mod-lua

INC_PATH += modules/common/$(TARGET_INCL)
INC_PATH += $(MSGPACK_PATH)/src
INC_PATH += modules/mod-lua/src/include

ifeq ($(MEM_COUNT),1)
  LDFLAGS += -lm
  CFLAGS += -DMEM_COUNT
endif



CITRUSLEAF = 
CITRUSLEAF += citrusleaf.o
CITRUSLEAF += cl_async.o
CITRUSLEAF += cl_batch.o
CITRUSLEAF += cl_cluster.o
CITRUSLEAF += cl_info.o
CITRUSLEAF += cl_lookup.o
CITRUSLEAF += cl_partition.o
CITRUSLEAF += cl_query.o
CITRUSLEAF += cl_scan.o
CITRUSLEAF += cl_udf.o
CITRUSLEAF += proto.o

OBJECTS = $(TARGET_OBJ)/*.o modules/mod-lua/$(TARGET_OBJ)/*.o modules/common/$(TARGET_OBJ)/client/*.o modules/common/$(TARGET_OBJ)/shared/*.o $(MSGPACK_PATH)/src/.libs/*.o

##
## MAIN
##

all: libcitrusleaf.so libcitrusleaf.a prepare

libcitrusleaf.o: $(call objects, $(CITRUSLEAF))

libcitrusleaf.a: | msgpack mod-lua common libcitrusleaf.o $(TARGET_LIB)
	$(call archive, $(empty), $(empty), $(empty), $(empty), $(OBJECTS))

libcitrusleaf.so: | msgpack mod-lua common libcitrusleaf.o $(TARGET_LIB)
	$(call library, $(empty), $(empty), $(empty), $(empty), $(OBJECTS))

prepare:
	mkdir -p $(TARGET_INCL)/citrusleaf
	cp -R $(SOURCE_INCL)/*.h $(TARGET_INCL)/citrusleaf/.
	cp modules/mod-lua/src/include/as_*.h $(TARGET_INCL)/citrusleaf/.
	cp modules/common/$(TARGET_INCL)/*.h $(TARGET_INCL)/.
	cp modules/common/$(TARGET_INCL)/citrusleaf/*.h $(TARGET_INCL)/citrusleaf/.

install: | test libcitrusleaf.a libcitrusleaf.so prepare
	mkdir -p /opt/citrusleaf/bin/commands
	cp $(SOURCE_TEST)/aerospike /opt/citrusleaf/bin/.
	cp $(TARGET_BIN)/test/exists /opt/citrusleaf/bin/commands/.
	cp $(TARGET_BIN)/test/get /opt/citrusleaf/bin/commands/.
	cp $(TARGET_BIN)/test/put /opt/citrusleaf/bin/commands/.
	cp $(TARGET_BIN)/test/remove /opt/citrusleaf/bin/commands/.
	cp $(TARGET_BIN)/test/udf-get /opt/citrusleaf/bin/commands/.
	cp $(TARGET_BIN)/test/udf-list /opt/citrusleaf/bin/commands/.
	cp $(TARGET_BIN)/test/udf-put /opt/citrusleaf/bin/commands/.
	cp $(TARGET_BIN)/test/udf-record-apply /opt/citrusleaf/bin/commands/.
	cp $(TARGET_BIN)/test/udf-remove /opt/citrusleaf/bin/commands/.
	ln -s /opt/citrusleaf/bin/aerospike /usr/bin/aerospike 

gendoc:
	cd doc; make html
	
##
## SUBMODULES
##

modules/msgpack/Makefile: 
	cd $(MSGPACK_PATH) && ./configure

msgpack: modules/msgpack/Makefile
	make -C $(MSGPACK_PATH) all CFLAGS="-fPIC"

mod-lua: 
	make -C modules/mod-lua libtypes.o MEM_COUNT=$(MEM_COUNT) 

common: 
	make -C modules/common client_only MEM_COUNT=$(MEM_COUNT) 

##
## TEST
##
check_jansson:
ifndef JANSSON_PATH
	$(error JANSSON_PATH is not set. Please download, build and set JANSSON_PATH)
endif

test_flags = $(TARGET_LIB)/libcitrusleaf.a -L$(JANSSON_PATH)/src/.libs -Wl,-l,:libjansson.a -lcrypto -lssl -L$(MSGPACK_PATH)/src/.libs -Wl,-l,:libmsgpack.a -lpthread -lrt 

test/%:  $(SOURCE_TEST)/%.c | $(TARGET_BIN) check_jansson
	$(call executable, $(TARGET_INCL) $(JANSSON_PATH)/src, $(empty), $(empty) , $(empty), $(test_flags) )

test/get:  $(SOURCE_TEST)/get.c $(SOURCE_TEST)/as_json.c | $(TARGET_BIN) check_jansson
	$(call executable, $(TARGET_INCL) $(JANSSON_PATH)/src, $(empty), $(empty), $(empty), $(test_flags))

test/put:  $(SOURCE_TEST)/put.c $(SOURCE_TEST)/as_json.c | $(TARGET_BIN) check_jansson
	$(call executable, $(TARGET_INCL) $(JANSSON_PATH)/src, $(empty), $(empty), $(empty), $(test_flags))

test/udf-record-apply:  $(SOURCE_TEST)/udf-record-apply.c $(SOURCE_TEST)/as_json.c | $(TARGET_BIN) check_jansson
	$(call executable, $(TARGET_INCL) $(JANSSON_PATH)/src, $(empty), $(empty), $(empty), $(test_flags))

test: test/quick test/get test/put test/remove test/exists test/udf test/udf-list test/udf-get test/udf-put test/udf-remove test/udf-record-apply
