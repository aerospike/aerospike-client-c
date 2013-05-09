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
$(warning *  BASE is '$(BASE))')
$(warning *  BASE doesn't contain 'BASE'. )
$(warning *  BASE should be set to a valid path. )
$(warning *)
$(warning ***************************************************************)
$(error )
endif

.PHONY: BASE-build
BASE-build: $(BASE)/$(TARGET_LIB)/libaerospike-base-hooked.a

.PHONY: BASE-prepare
BASE-prepare: $(BASE)/$(TARGET_INCL)/citrusleaf/*.h | BASE-make-prepare
	$(noop)

.PHONY: BASE-clean
BASE-clean:
	$(MAKE) -e -C $(BASE) clean

.PHONY: BASE-make-prepare
BASE-make-prepare: $(wildcard $(BASE)/$(SOURCE_INCL)/citrusleaf/*.h)
	$(MAKE) -e -C $(BASE) prepare

$(BASE)/$(TARGET_LIB)/libaerospike-base-hooked.a:
	$(MAKE) -e -C $(BASE) libaerospike-base-hooked.a

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
$(warning *  COMMON is '$(COMMON))')
$(warning *  COMMON doesn't contain 'Makefile'. )
$(warning *  COMMON should be set to a valid path. )
$(warning *)
$(warning ***************************************************************)
$(error )
endif

.PHONY: COMMON-build
COMMON-build: $(COMMON)/$(TARGET_LIB)/libaerospike-common-hooked.a

.PHONY: COMMON-prepare
COMMON-prepare:: $(COMMON)/$(TARGET_INCL)/citrusleaf/*.h | COMMON-make-prepare
	$(noop)

.PHONY: COMMON-clean
COMMON-clean:
	$(MAKE) -e -C $(COMMON) clean

.PHONY: COMMON-make-prepare
COMMON-make-prepare: 
	$(MAKE) -e -C $(COMMON) prepare

$(COMMON)/$(TARGET_LIB)/libaerospike-common-hooked.a:
	$(MAKE) -e -C $(COMMON) libaerospike-common-hooked.a

$(COMMON)/$(TARGET_INCL)/citrusleaf/%.h: $(COMMON)/$(SOURCE_INCL)/citrusleaf/%.h | COMMON-make-prepare  $(TARGET_INCL)/citrusleaf
	cp -p $^ $(TARGET_INCL)/citrusleaf