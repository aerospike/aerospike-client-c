ROOT = ..
###############################################################################
include $(ROOT)/project/settings.makefile
include project/modules.makefile
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
	$(noop)

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
##  TEST                                                                     ##
###############################################################################

include project/test.makefile

###############################################################################
include $(RULES)
###############################################################################