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

COMMON-TARGET := $(subst $(COMMON)/$(SOURCE_INCL),$(TARGET_INCL),$(COMMON-HEADERS))

.PHONY: COMMON-build
COMMON-build: $(COMMON)/$(TARGET_LIB)/libaerospike-common.a

.PHONY: COMMON-clean
COMMON-clean:
	$(MAKE) -e -C $(COMMON) clean

$(COMMON)/$(TARGET_LIB)/libaerospike-common.a:
	$(MAKE) -e -C $(COMMON) libaerospike-common.a

.PHONY: COMMON-prepare
COMMON-prepare: $(COMMON-TARGET)
	$(MAKE) -e -C $(COMMON) prepare

$(TARGET_INCL)/%.h: $(COMMON)/$(SOURCE_INCL)/%.h
	@mkdir -p $(@D)
	cp $< $@

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
	$(MAKE) -e -C $(MOD_LUA) clean COMMON=$(COMMON) LUAMOD=$(LUAMOD)

$(MOD_LUA)/$(TARGET_LIB)/libmod_lua.a:
	$(MAKE) -e -C $(MOD_LUA) COMMON=$(COMMON) LUAMOD=$(LUAMOD) EXT_CFLAGS=-DAS_MOD_LUA_CLIENT

.PHONY: MOD_LUA-prepare
	$(MAKE) -e -C $(MOD_LUA) prepare COMMON=$(COMMON) LUAMOD=$(LUAMOD)
