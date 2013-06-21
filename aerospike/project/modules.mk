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

ifeq ($(wildcard $(MSGPACK)/configure.in),) 
$(warning ***************************************************************)
$(warning *)
$(warning *  MSGPACK is '$(MSGPACK)')
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

PHONY: MSGPACK-clean
MSGPACK-clean:
	@if [ -e "$(MSGPACK)/Makefile" ]; then \
		$(MAKE) -e -C $(MSGPACK) clean; \
		$(MAKE) -e -C $(MSGPACK) distclean; \
	fi
	@if [ -e "$(MSGPACK)/configure" ]; then \
		rm -f $(MSGPACK)/configure; \
	fi

$(MSGPACK)/configure: $(MSGPACK)/configure.in
	cd $(MSGPACK) && autoreconf -v --force

$(MSGPACK)/Makefile: $(MSGPACK)/configure
	cd $(MSGPACK) && ./configure CFLAGS="-fPIC"

$(MSGPACK)/src/.libs/libmsgpackc.a: $(MSGPACK)/Makefile
	$(MAKE) -e -C $(MSGPACK) CFLAGS="-fPIC"

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


.PHONY: COMMON-prepare
COMMON-prepare: COMMON-make-prepare $(subst $(COMMON)/$(SOURCE_INCL),$(TARGET_INCL),$(COMMON-HEADERS)) 
	$(noop)


# COMMON-HEADERS := $(wildcard $(COMMON)/$(SOURCE_INCL)/aerospike/*.h) $(wildcard $(COMMON)/$(SOURCE_INCL)/citrusleaf/*.h)

# .PHONY: COMMON-prepare
# COMMON-prepare: COMMON-make-prepare $(subst $(COMMON)/$(SOURCE_INCL),$(TARGET_INCL),$(COMMON-HEADERS)) 
# 	$(noop)

.PHONY: COMMON-make-prepare
COMMON-make-prepare:
	$(MAKE) -e -C $(COMMON) prepare MSGPACK=$(MSGPACK)

$(TARGET_INCL)/aerospike/%.h: $(COMMON)/$(TARGET_INCL)/aerospike/%.h | $(TARGET_INCL)/aerospike
	 cp $^ $@

$(TARGET_INCL)/citrusleaf/%.h: $(COMMON)/$(TARGET_INCL)/citrusleaf/%.h | $(TARGET_INCL)/citrusleaf
	 cp $^ $@

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


.PHONY: BASE-prepare
BASE-prepare:
	$(noop)

# BASE-HEADERS := $(wildcard $(BASE)/$(SOURCE_INCL)/citrusleaf/*.h)

# .PHONY: BASE-prepare
# BASE-prepare: BASE-make-prepare $(subst $(BASE)/$(SOURCE_INCL),$(TARGET_INCL),$(BASE-HEADERS)) 
# 	$(noop)

# .PHONY: BASE-make-prepare
# BASE-make-prepare:
# 	$(MAKE) -e -C $(BASE) prepare MSGPACK=$(MSGPACK) COMMON=$(COMMON)

# $(TARGET_INCL)/citrusleaf/%.h: $(BASE)/$(TARGET_INCL)/citrusleaf/%.h | $(TARGET_INCL)/citrusleaf
# 	cp $^ $@


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

.PHONY: MOD_LUA-prepare
MOD_LUA-prepare:
	$(noop)

# MOD_LUA-HEADERS := $(wildcard $(MOD_LUA)/$(SOURCE_INCL)/aerospike/*.h)

# .PHONY: MOD_LUA-prepare
# MOD_LUA-prepare: MOD_LUA-make-prepare $(subst $(MOD_LUA)/$(SOURCE_INCL),$(TARGET_INCL),$(MOD_LUA-HEADERS))
# 	$(noop)

# .PHONY: MOD_LUA-make-prepare
# MOD_LUA-make-prepare:
# 	$(MAKE) -e -C $(MOD_LUA) prepare MSGPACK=$(MSGPACK) COMMON=$(COMMON)

# $(TARGET_INCL)/aerospike/%.h: $(MOD_LUA)/$(TARGET_INCL)/aerospike/%.h | $(TARGET_INCL)/aerospike
# 	cp $^ $@
