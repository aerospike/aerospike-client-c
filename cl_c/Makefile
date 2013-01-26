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
	if [ ! -e /opt/citrusleaf/bin/commands ]; then mkdir -p /opt/citrusleaf/bin/commands; fi
	cp -p $(SOURCE_TEST)/ascli.py /opt/citrusleaf/bin/ascli
	cp -p $(TARGET_BIN)/test/exists /opt/citrusleaf/bin/commands/.
	cp -p $(TARGET_BIN)/test/get /opt/citrusleaf/bin/commands/.
	cp -p $(TARGET_BIN)/test/put /opt/citrusleaf/bin/commands/.
	cp -p $(TARGET_BIN)/test/remove /opt/citrusleaf/bin/commands/.
	cp -p $(TARGET_BIN)/test/udf-get /opt/citrusleaf/bin/commands/.
	cp -p $(TARGET_BIN)/test/udf-list /opt/citrusleaf/bin/commands/.
	cp -p $(TARGET_BIN)/test/udf-put /opt/citrusleaf/bin/commands/.
	cp -p $(TARGET_BIN)/test/udf-record-apply /opt/citrusleaf/bin/commands/.
	cp -p $(TARGET_BIN)/test/udf-remove /opt/citrusleaf/bin/commands/.
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

test_flags = $(TARGET_LIB)/libcitrusleaf.a -L$(JANSSON_PATH)/src/.libs -Wl,-l,:libjansson.a -lcrypto -lssl -L$(MSGPACK_PATH)/src/.libs -Wl,-l,:libmsgpack.a -lpthread -lrt 

test/%:  $(SOURCE_TEST)/%.c | $(TARGET_BIN) jansson
	$(call executable, $(TARGET_INCL) $(JANSSON_PATH)/src, $(empty), $(empty) , $(empty), $(test_flags) )

test/get:  $(SOURCE_TEST)/get.c $(SOURCE_TEST)/as_json.c | $(TARGET_BIN)  jansson
	$(call executable, $(TARGET_INCL) $(JANSSON_PATH)/src, $(empty), $(empty), $(empty), $(test_flags))

test/put:  $(SOURCE_TEST)/put.c $(SOURCE_TEST)/as_json.c | $(TARGET_BIN)  jansson
	$(call executable, $(TARGET_INCL) $(JANSSON_PATH)/src, $(empty), $(empty), $(empty), $(test_flags))

test/udf-record-apply:  $(SOURCE_TEST)/udf-record-apply.c $(SOURCE_TEST)/as_json.c | $(TARGET_BIN)  jansson
	$(call executable, $(TARGET_INCL) $(JANSSON_PATH)/src, $(empty), $(empty), $(empty), $(test_flags))

test: test/quick test/get test/put test/remove test/exists test/udf test/udf-list test/udf-get test/udf-put test/udf-remove test/udf-record-apply
