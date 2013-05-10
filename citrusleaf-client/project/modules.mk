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



COMMON-HEADERS := $(wildcard $(COMMON)/$(SOURCE_INCL)/citrusleaf/*.h)

.PHONY: COMMON-prepare
COMMON-prepare: COMMON-make-prepare $(subst $(COMMON)/$(SOURCE_INCL),$(TARGET_INCL),$(COMMON-HEADERS)) 
	$(noop)

.PHONY: COMMON-make-prepare
COMMON-make-prepare:
	$(MAKE) -e -C $(COMMON) prepare MSGPACK=$(MSGPACK)

# $(TARGET_INCL)/aerospike/%.h: $(COMMON)/$(TARGET_INCL)/aerospike/%.h | $(TARGET_INCL)/aerospike
# 	 cp $^ $@

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



BASE-HEADERS := $(wildcard $(BASE)/$(SOURCE_INCL)/citrusleaf/*.h)

.PHONY: BASE-prepare
BASE-prepare: BASE-make-prepare $(subst $(BASE)/$(SOURCE_INCL),$(TARGET_INCL),$(BASE-HEADERS)) 
	$(noop)

.PHONY: BASE-make-prepare
BASE-make-prepare:
	$(MAKE) -e -C $(BASE) prepare MSGPACK=$(MSGPACK) COMMON=$(COMMON)

$(TARGET_INCL)/citrusleaf/%.h: $(BASE)/$(TARGET_INCL)/citrusleaf/%.h | $(TARGET_INCL)/citrusleaf
	cp $^ $@
