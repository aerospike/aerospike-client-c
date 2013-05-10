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
COMMON-build: $(COMMON)/$(TARGET_LIB)/libaerospike-common.a

.PHONY: COMMON-clean
COMMON-clean:
	$(MAKE) -e -C $(COMMON) clean MSGPACK=$(MSGPACK)

$(COMMON)/$(TARGET_LIB)/libaerospike-common.a:
	$(MAKE) -e -C $(COMMON) libaerospike-common.a MSGPACK=$(MSGPACK)



COMMON-headers := $(wildcard $(COMMON)/$(SOURCE_INCL)/aerospike/*.h) $(wildcard $(COMMON)/$(SOURCE_INCL)/citrusleaf/*.h)

.PHONY: COMMON-prepare
COMMON-prepare: $(subst $(COMMON)/$(SOURCE_INCL),$(TARGET_INCL),$(COMMON-headers)) 
	$(noop)

$(TARGET_INCL)/aerospike/%.h: $(COMMON)/$(TARGET_INCL)/aerospike/%.h | $(TARGET_INCL)/aerospike
	cp $^ $@

$(TARGET_INCL)/citrusleaf/%.h: $(COMMON)/$(TARGET_INCL)/citrusleaf/%.h | $(TARGET_INCL)/citrusleaf
	cp $^ $@

$(COMMON)/$(TARGET_INCL)/aerospike/%.h: $(COMMON)/$(SOURCE_INCL)/aerospike/%.h
	$(MAKE) -e -C $(COMMON) prepare MSGPACK=$(MSGPACK)

$(COMMON)/$(TARGET_INCL)/citrusleaf/%.h: $(COMMON)/$(SOURCE_INCL)/citrusleaf/%.h
	$(MAKE) -e -C $(COMMON) prepare MSGPACK=$(MSGPACK)


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
BASE-build: $(BASE)/$(TARGET_LIB)/libaerospike-base.a

.PHONY: BASE-clean
BASE-clean:
	$(MAKE) -e -C $(BASE) clean MSGPACK=$(MSGPACK) COMMON=$(COMMON)

$(BASE)/$(TARGET_LIB)/libaerospike-base.a:
	$(MAKE) -e -C $(BASE) libaerospike-base.a MSGPACK=$(MSGPACK) COMMON=$(COMMON)



BASE-headers := $(wildcard $(BASE)/$(SOURCE_INCL)/citrusleaf/*.h)

.PHONY: BASE-prepare
BASE-prepare: $(subst $(BASE)/$(SOURCE_INCL),$(TARGET_INCL),$(BASE-headers)) 
	$(noop)

$(TARGET_INCL)/citrusleaf/%.h: $(BASE)/$(TARGET_INCL)/citrusleaf/%.h | $(TARGET_INCL)/citrusleaf
	cp $^ $@

$(BASE)/$(TARGET_INCL)/citrusleaf/%.h: $(BASE)/$(SOURCE_INCL)/citrusleaf/%.h
	$(MAKE) -e -C $(BASE) prepare MSGPACK=$(MSGPACK)  COMMON=$(COMMON)

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

.PHONY: MOD_LUA-clean
MOD_LUA-clean:
	$(MAKE) -e -C $(MOD_LUA) clean COMMON=$(COMMON) MSGPACK=$(MSGPACK)

$(MOD_LUA)/$(TARGET_LIB)/libmod_lua.a:
	$(MAKE) -e -C $(MOD_LUA) libmod_lua.a COMMON=$(COMMON) MSGPACK=$(MSGPACK)


MOD_LUA-headers := $(wildcard $(MOD_LUA)/$(SOURCE_INCL)/aerospike/*.h)

.PHONY: MOD_LUA-prepare
MOD_LUA-prepare: $(subst $(MOD_LUA)/$(SOURCE_INCL),$(TARGET_INCL),$(MOD_LUA-headers))
	$(noop)

$(TARGET_INCL)/aerospike/%.h: $(MOD_LUA)/$(TARGET_INCL)/aerospike/%.h | $(TARGET_INCL)/aerospike
	cp $^ $@

$(MOD_LUA)/$(TARGET_INCL)/aerospike/%.h: $(MOD_LUA)/$(SOURCE_INCL)/aerospike/%.h
	$(MAKE) -e -C $(MOD_LUA) prepare MSGPACK=$(MSGPACK) COMMON=$(COMMON)
