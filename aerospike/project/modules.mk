###############################################################################
##  BASE MODULE                                                              ##
###############################################################################

ifndef BASE
$(warning ***************************************************************)
$(warning *)
$(warning *  BASE is not defined. )
$(warning *  BASE should be set to a valid path. )
$(warning *)
$(warning ***************************************************************)
$(error )
endif

ifeq ($(wildcard $(BASE)/Makefile),) 
$(warning ***************************************************************)
$(warning *)
$(warning *  BASE is '$(BASE)')
$(warning *  BASE doesn't contain 'BASE'. )
$(warning *  BASE should be set to a valid path. )
$(warning *)
$(warning ***************************************************************)
$(error )
endif

.PHONY: BASE-build
BASE-build: $(BASE)/$(TARGET_LIB)/libcf-client.a

.PHONY: BASE-prepare
BASE-prepare: $(BASE)/$(TARGET_INCL)/citrusleaf/*.h | BASE-make-prepare
	$(noop)

.PHONY: BASE-clean
BASE-clean:
	$(MAKE) -e -C $(BASE) clean

.PHONY: BASE-make-prepare
BASE-make-prepare: $(wildcard $(BASE)/$(SOURCE_INCL)/citrusleaf/*.h)
	$(MAKE) -e -C $(BASE) prepare

$(BASE)/$(TARGET_LIB)/libcf-client.a:
	$(MAKE) -e -C $(BASE) libcf-client.a

$(BASE)/$(TARGET_INCL)/citrusleaf/%.h: $(BASE)/$(SOURCE_INCL)/citrusleaf/%.h | BASE-make-prepare $(TARGET_INCL)/citrusleaf
	cp -p $^ $(TARGET_INCL)/citrusleaf

###############################################################################
##  COMMON MODULE                                                            ##
###############################################################################

ifndef COMMON
$(warning ***************************************************************)
$(warning *)
$(warning *  COMMON is not defined. )
$(warning *  COMMON should be set to a valid path. )
$(warning *)
$(warning ***************************************************************)
$(error )
endif

ifeq ($(wildcard $(COMMON)/Makefile),) 
$(warning ***************************************************************)
$(warning *)
$(warning *  COMMON is '$(COMMON)')
$(warning *  COMMON doesn't contain 'Makefile'. )
$(warning *  COMMON should be set to a valid path. )
$(warning *)
$(warning ***************************************************************)
$(error )
endif

.PHONY: COMMON-build
COMMON-build: $(COMMON)/$(TARGET_LIB)/libcf-common.a

.PHONY: COMMON-prepare
COMMON-prepare:: $(COMMON)/$(TARGET_INCL)/citrusleaf/*.h | COMMON-make-prepare
	$(noop)

COMMON-prepare:: $(COMMON)/$(TARGET_INCL)/aerospike/*.h | COMMON-make-prepare
	$(noop)

.PHONY: COMMON-clean
COMMON-clean:
	$(MAKE) -e -C $(COMMON) clean

.PHONY: COMMON-make-prepare
COMMON-make-prepare: 
	$(MAKE) -e -C $(COMMON) prepare

$(COMMON)/$(TARGET_LIB)/libcf-common.a:
	$(MAKE) -e -C $(COMMON) libcf-common.a

$(COMMON)/$(TARGET_INCL)/aerospike/%.h: $(COMMON)/$(SOURCE_INCL)/aerospike/%.h | COMMON-make-prepare  $(TARGET_INCL)/aerospike
	cp -p $^ $(TARGET_INCL)/aerospike

$(COMMON)/$(TARGET_INCL)/citrusleaf/%.h: $(COMMON)/$(SOURCE_INCL)/citrusleaf/%.h | COMMON-make-prepare  $(TARGET_INCL)/citrusleaf
	cp -p $^ $(TARGET_INCL)/citrusleaf


###############################################################################
##  MOD-LUA MODULE                                                           ##
###############################################################################

ifndef MOD_LUA
$(warning ***************************************************************)
$(warning *)
$(warning *  MOD_LUA is not defined. )
$(warning *  MOD_LUA should be set to a valid path. )
$(warning *)
$(warning ***************************************************************)
$(error )
endif

ifeq ($(wildcard $(MOD_LUA)/Makefile),) 
$(warning ***************************************************************)
$(warning *)
$(warning *  MOD_LUA is '$(MOD_LUA)')
$(warning *  MOD_LUA doesn't contain 'Makefile'. )
$(warning *  MOD_LUA should be set to a valid path. )
$(warning *)
$(warning ***************************************************************)
$(error )
endif

.PHONY: MOD_LUA-build
MOD_LUA-build: $(MOD_LUA)/$(TARGET_LIB)/libmod_lua.a

.PHONY: MOD_LUA-prepare
MOD_LUA-prepare: $(MOD_LUA)/$(TARGET_INCL)/aerospike/*.h
	$(noop)

.PHONY: COMMON-make-prepare
MOD_LUA-make-prepare: 
	$(MAKE) -e -C $(COMMON) prepare

.PHONY: MOD_LUA-clean
MOD_LUA-clean:
	$(MAKE) -e -C $(MOD_LUA) clean COMMON=$(COMMON) MSGPACK=$(MSGPACK)

$(MOD_LUA)/$(TARGET_LIB)/libmod_lua.a:
	$(MAKE) -e -C $(MOD_LUA) libmod_lua.a COMMON=$(COMMON) MSGPACK=$(MSGPACK)

$(MOD_LUA)/$(TARGET_INCL)/aerospike/%.h: $(MOD_LUA)/$(SOURCE_INCL)/aerospike/%.h | $(TARGET_INCL)/aerospike
	$(MAKE) -e -C $(MOD_LUA) prepare COMMON=$(COMMON) MSGPACK=$(MSGPACK)
	cp -p $^ $(TARGET_INCL)/aerospike

###############################################################################
##  MSGPACK MODULE                                                           ##
###############################################################################

ifndef MSGPACK
$(warning ***************************************************************)
$(warning *)
$(warning *  MSGPACK is not defined. )
$(warning *  MSGPACK should be set to a valid path. )
$(warning *)
$(warning ***************************************************************)
$(error )
endif

ifeq ($(wildcard $(MSGPACK)/configure),) 
$(warning ***************************************************************)
$(warning *)
$(warning *  MSGPACK is '$(MSGPACK))')
$(warning *  MSGPACK doesn't contain 'configure'. )
$(warning *  MSGPACK should be set to a valid path. )
$(warning *)
$(warning ***************************************************************)
$(error )
endif

.PHONY: MSGPACK-build
MSGPACK-build: $(MSGPACK)/src/.libs/libmsgpackc.a

.PHONY: MSGPACK-prepare
MSGPACK-prepare: 
	$(noop)

.PHONY: MSGPACK-clean
MSGPACK-clean:
	@if [ -e "$(MSGPACK)/Makefile" ]; then \
		$(MAKE) -e -C $(MSGPACK) clean; \
	fi

$(MSGPACK)/Makefile: $(MSGPACK)/configure
	cd $(MSGPACK) && ./configure 

$(MSGPACK)/src/.libs/libmsgpackc.a: $(MSGPACK)/Makefile
	cd $(MSGPACK) && $(MAKE) -s CFLAGS="-fPIC"
