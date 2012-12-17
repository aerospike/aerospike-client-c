include project/build.makefile

CFLAGS = -std=gnu99 -g -O3 -fno-common -fno-strict-aliasing -rdynamic -fPIC -Wall $(AS_CFLAGS) -DMARCH_$(ARCH) -D_FILE_OFFSET_BITS=64 -D_REENTRANT -D_GNU_SOURCE
LDFLAGS = 

MODULES = common mod-lua

INC_PATH += modules/common/$(TARGET_INCL)
INC_PATH += modules/mod-lua/modules/msgpack/src/
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
# CITRUSLEAF += cl_query.o
CITRUSLEAF += cl_scan.o
# CITRUSLEAF += cl_sproc.o
CITRUSLEAF += cl_udf.o
CITRUSLEAF += proto.o

OBJECTS = $(TARGET_OBJ)/*.o modules/mod-lua/$(TARGET_OBJ)/*.o modules/common/$(TARGET_OBJ)/shared/*.o modules/common/$(TARGET_OBJ)/client/*.o

##
## MAIN
##

all: libcitrusleaf.so libcitrusleaf.a prepare

libcitrusleaf.o: $(call objects, $(CITRUSLEAF))

libcitrusleaf.a: | mod-lua common libcitrusleaf.o $(TARGET_LIB)
	$(call archive, $(empty), $(empty), $(empty), $(empty), $(OBJECTS))

libcitrusleaf.so: | mod-lua common libcitrusleaf.o $(TARGET_LIB)
	$(call library, $(empty), $(empty), $(empty), $(empty), $(OBJECTS))

prepare:
	mkdir -p $(TARGET_INCL)/citrusleaf
	cp -R $(SOURCE_INCL)/*.h $(TARGET_INCL)/citrusleaf/.
	cp modules/mod-lua/src/include/as_*.h $(TARGET_INCL)/citrusleaf/.
	cp modules/common/$(TARGET_INCL)/*.h $(TARGET_INCL)/.
	cp modules/common/$(TARGET_INCL)/citrusleaf/*.h $(TARGET_INCL)/citrusleaf/.

install: | libcitrusleaf.a libcitrusleaf.so prepare
	sudo cp $(TARGET_LIB)/* /usr/local/lib/.

##
## SUBMODULES
##

mod-lua: 
	make -C modules/mod-lua all MEM_COUNT=$(MEM_COUNT) 

common: 
	make -C modules/common all MEM_COUNT=$(MEM_COUNT) 

##
## TEST
##

test_flags = -L/usr/local/lib $(TARGET_LIB)/libcitrusleaf.a -llua -lcrypto -lssl -lmsgpack -lpthread -lrt -Wl,-rpath,/usr/local/lib

test/%: $(SOURCE_TEST)/%.c | $(TARGET_BIN)
	$(call executable, $(TARGET_INCL), $(empty), $(empty) , $(empty), $(test_flags) )

test/udf-record-apply: $(SOURCE_TEST)/udf-record-apply.c | $(TARGET_BIN)
	$(call executable, $(TARGET_INCL), $(empty),  jansson, $(empty), $(test_flags))

test: test/get test/put test/quick test/udf test/udf-list test/udf-get test/udf-put test/udf-remove test/udf-record-apply
