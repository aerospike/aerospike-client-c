include project/build.makefile

CFLAGS = -std=gnu99 -g -O3 -fno-common -fno-strict-aliasing -rdynamic  -Wall $(AS_CFLAGS) -DMARCH_$(ARCH) -D_FILE_OFFSET_BITS=64 -D_REENTRANT -D_GNU_SOURCE
LDFLAGS = -lssl -lpthread -lrt -lcrypto -lz

INC_PATH += modules/common/$(TARGET_INCL)
INC_PATH += modules/aerospike-mod-lua/src/include
INC_PATH += modules/msgpack/src

MODULES += common

CITRUSLEAF = 
CITRUSLEAF += citrusleaf.o
CITRUSLEAF += cl_async.o
CITRUSLEAF += cl_batch.o
CITRUSLEAF += cl_cluster.o
CITRUSLEAF += cl_info.o
CITRUSLEAF += cl_lookup.o
CITRUSLEAF += cl_partition.o
# CITRUSLEAF += cl_query.o
CITRUSLEAF += cl_scan.o
# CITRUSLEAF += cl_sproc.o
CITRUSLEAF += cl_udf.o
CITRUSLEAF += proto.o

ifeq ($(MEM_COUNT),1)
  LDFLAGS += -lm
  CFLAGS += -DMEM_COUNT
endif

all: move

libcitrusleaf.a: $(call objects, $(CITRUSLEAF)) |  common msgpack mod-lua $(TARGET_LIB)
	$(call archive, $(empty), $(empty), $(empty), $(empty))

prepare:
	mkdir -p $(TARGET_INCL)/citrusleaf
	cp -R $(SOURCE_INCL)/*.h $(TARGET_INCL)/citrusleaf/.

move: | common msgpack mod-lua  libcitrusleaf.a prepare
	mkdir -p $(TARGET_INCL)/citrusleaf
	cp $(SOURCE_INCL)/*.h $(TARGET_INCL)/citrusleaf/.

##
## modules
##

modules/msgpack/Makefile: 
	cd modules/msgpack && ./configure

msgpack: modules/msgpack/Makefile
	cd modules/msgpack && make

mod-lua: 
	make -C modules/aerospike-mod-lua all MEM_COUNT=$(MEM_COUNT) 

common: 
	make -C modules/common all MEM_COUNT=$(MEM_COUNT) 

##
## TEST
##

test/%: $(SOURCE_TEST)/%.c |  $(TARGET_BIN)
	$(call executable, $(TARGET_INCL), $(empty), lua, $(empty), $(TARGET_LIB)/libcitrusleaf.a modules/aerospike-mod-lua/$(TARGET_LIB)/libmod_lua.a modules/common/$(TARGET_LIB)/libcf-client.a modules/common/$(TARGET_LIB)/libcf-shared.a modules/msgpack/src/.libs/libmsgpack.a $(LDFLAGS) )

test: test/quick test/udf test/udf-list test/udf-get test/udf-put test/udf-remove test/udf-record-apply