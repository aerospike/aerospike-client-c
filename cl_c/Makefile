include project/build.makefile

CFLAGS = -std=gnu99 -g -O3 -fno-common -fno-strict-aliasing -rdynamic  -Wall $(AS_CFLAGS) -DMARCH_$(ARCH) -D_FILE_OFFSET_BITS=64 -D_REENTRANT -D_GNU_SOURCE 

INC_PATH += modules/common/$(TARGET_INCL)/client modules/aerospike-mod-lua/src/include

MODULES += common

CITRUSLEAF = 
CITRUSLEAF += citrusleaf.o
CITRUSLEAF += cl_async.o
CITRUSLEAF += cl_batch.o
CITRUSLEAF += cl_cluster.o
CITRUSLEAF += cl_info.o
CITRUSLEAF += cl_partition.o
# CITRUSLEAF += cl_query.o
CITRUSLEAF += cl_lookup.o
# CITRUSLEAF += cl_scan.o
# CITRUSLEAF += cl_sproc.o
CITRUSLEAF += cl_udf.o
CITRUSLEAF += proto.o

ifeq ($(MEM_COUNT),1)
  LDFLAGS += -lm
  CFLAGS += -DMEM_COUNT
endif

mod-lua: 
	make -C modules/aerospike-mod-lua all MEM_COUNT=$(MEM_COUNT) 

common: 
	make -C modules/common all MEM_COUNT=$(MEM_COUNT) 

libcitrusleaf.a: $(call objects, $(CITRUSLEAF)) |  common mod-lua $(TARGET_LIB)
	$(call archive, $(empty), $(empty), $(empty), $(empty))

prepare:
	mkdir -p $(TARGET_INCL)/citrusleaf
	cp -R $(SOURCE_INCL)/*.h $(TARGET_INCL)/citrusleaf/.

move: | common mod-lua  libcitrusleaf.a prepare
	mkdir -p ../../../shared/lib
	cp $(TARGET_LIB)/libcitrusleaf.a ../../../shared/lib/.
	mkdir -p ../../../shared/include
	cp -R $(TARGET_INCL)/* ../../../shared/include/.
	cp -R modules/common/$(TARGET_INCL)/client/* ../../../shared/include/.

all: move
