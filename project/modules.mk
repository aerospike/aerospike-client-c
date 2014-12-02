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
	$(MAKE) -e -C $(COMMON) clean

$(COMMON)/$(TARGET_LIB)/libaerospike-common.a:
	$(MAKE) -e -C $(COMMON) libaerospike-common.a


.PHONY: COMMON-prepare
COMMON-prepare: COMMON-make-prepare $(subst $(COMMON)/$(SOURCE_INCL),$(TARGET_INCL),$(COMMON-HEADERS)) 
	$(noop)

.PHONY: COMMON-make-prepare
COMMON-make-prepare:
	$(MAKE) -e -C $(COMMON) prepare

$(TARGET_INCL)/aerospike/%.h: $(COMMON)/$(TARGET_INCL)/aerospike/%.h | $(TARGET_INCL)/aerospike
	 cp -p $^ $@

$(TARGET_INCL)/citrusleaf/%.h: $(COMMON)/$(TARGET_INCL)/citrusleaf/%.h | $(TARGET_INCL)/citrusleaf
	 cp -p $^ $@

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
	$(MAKE) -e -C $(BASE) clean COMMON=$(COMMON)

$(BASE)/$(TARGET_LIB)/libaerospike-base.a:
	$(MAKE) -e -C $(BASE) libaerospike-base.a COMMON=$(COMMON)

.PHONY: BASE-prepare
BASE-prepare: BASE-make-prepare $(subst $(BASE)/$(SOURCE_INCL),$(TARGET_INCL),$(BASE-HEADERS))
	$(noop)

.PHONY: BASE-make-prepare
BASE-make-prepare:
	$(MAKE) -e -C $(BASE) prepare COMMON=$(COMMON)

$(TARGET_INCL)/citrusleaf/%.h: $(BASE)/$(TARGET_INCL)/citrusleaf/%.h | $(TARGET_INCL)/citrusleaf
	cp -p $^ $@

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
	$(MAKE) -e -C $(MOD_LUA) clean COMMON=$(COMMON) LUA_CORE=$(LUA_CORE)

$(MOD_LUA)/$(TARGET_LIB)/libmod_lua.a:
	$(MAKE) -e -C $(MOD_LUA) libmod_lua.a COMMON=$(COMMON) LUA_CORE=$(LUA_CORE)

.PHONY: MOD_LUA-prepare
MOD_LUA-prepare: MOD_LUA-make-prepare
	$(noop)

.PHONY: MOD_LUA-make-prepare
MOD_LUA-make-prepare:
	$(MAKE) -e -C $(MOD_LUA) prepare COMMON=$(COMMON) LUA_CORE=$(LUA_CORE)

###############################################################################
##  Concurrency Kit                                                          ##
###############################################################################

.PHONY: CK-build
CK-build: $(TARGET_INCL)/ck/ck_md.h

$(CK)/include/ck_md.h:
	(cd $(CK); ./configure)

.PHONY: CK-clean
CK-clean:
	$(noop)

CK_INCLUDES := $(shell find $(CK)/include -type f)
CK_PATSUB := $(patsubst $(CK)/include/%.h,$(TARGET_INCL)/ck/%.h,$(CK_INCLUDES))

$(TARGET_INCL)/ck/%.h: $(CK)/include/%.h
	mkdir -p $(@D)
	cp -p $< $@

CK-prepare: $(CK_PATSUB)
	$(noop)

# .PHONY: CK-prepare
# CK-prepare: $(TARGET_INCL)/ck
# 	@rsync -rp $(CK)/include/* $(TARGET_INCL)/ck

$(TARGET_INCL)/ck: | $(TARGET_INCL)
	mkdir $@
