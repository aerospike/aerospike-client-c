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
$(warning *  COMMON doesn't contain 'configure'. )
$(warning *  COMMON should be set to a valid path. )
$(warning *)
$(warning ***************************************************************)
$(error )
endif

.PHONY: COMMON
COMMON: common-prepare

.PHONY: COMMON-prepare
COMMON-prepare: $(COMMON)/$(TARGET_INCL)/aerospike/*.h $(COMMON)/$(TARGET_INCL)/citrusleaf/*.h

.PHONY: COMMON-clean
COMMON-clean:
	$(MAKE) -e -C $(COMMON) clean MSGPACK=$(MSGPACK)

$(COMMON)/$(TARGET_INCL)/aerospike/%.h: $(COMMON)/$(SOURCE_INCL)/aerospike/%.h
	$(MAKE) -e -C $(COMMON) prepare MSGPACK=$(MSGPACK)

$(COMMON)/$(TARGET_INCL)/citrusleaf/%.h: $(COMMON)/$(SOURCE_INCL)/citrusleaf/%.h
	$(MAKE) -e -C $(COMMON) prepare MSGPACK=$(MSGPACK)
