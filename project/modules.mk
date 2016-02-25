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
	$(noop)

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
	$(MAKE) -e -C $(MOD_LUA) clean COMMON=$(COMMON) LUA_CORE=$(LUA_CORE) USE_LUAJIT=$(USE_LUAJIT) LUAJIT=$(LUAJIT) USE_LUAMOD=$(USE_LUAMOD) LUAMOD=$(LUAMOD)

$(MOD_LUA)/$(TARGET_LIB)/libmod_lua.a:
	$(MAKE) -e -C $(MOD_LUA) libmod_lua.a COMMON=$(COMMON) LUA_CORE=$(LUA_CORE) USE_LUAJIT=$(USE_LUAJIT) LUAJIT=$(LUAJIT) USE_LUAMOD=$(USE_LUAMOD) LUAMOD=$(LUAMOD) EXT_CFLAGS=-DAS_MOD_LUA_CLIENT

.PHONY: MOD_LUA-prepare
MOD_LUA-prepare: MOD_LUA-make-prepare
	$(noop)

.PHONY: MOD_LUA-make-prepare
MOD_LUA-make-prepare:
	@$(MAKE) -e -C $(MOD_LUA) prepare COMMON=$(COMMON) LUA_CORE=$(LUA_CORE) USE_LUAJIT=$(USE_LUAJIT) LUAJIT=$(LUAJIT) USE_LUAMOD=$(USE_LUAMOD) LUAMOD=$(LUAMOD)

###############################################################################
##  LUA MODULE                                                               ##
###############################################################################

ifeq ($(USE_LUAMOD),1)
  ifndef LUAMOD
    $(warning ***************************************************************)
    $(warning *)
    $(warning *  LUAMOD is not defined. )
    $(warning *  LUAMOD should be set to a valid path. )
    $(warning *)
    $(warning ***************************************************************)
    $(error )
  endif

  ifeq ($(wildcard $(LUAMOD)/Makefile),)
    $(warning ***************************************************************)
    $(warning *)
    $(warning *  LUAMOD is '$(LUAMOD)')
    $(warning *  LUAMOD doesn't contain 'Makefile'. )
    $(warning *  LUAMOD should be set to a valid path. )
    $(warning *)
    $(warning ***************************************************************)
    $(error )
  endif
endif

.PHONY: LUAMOD-build
LUAMOD-build:	$(LUAMOD)/src/liblua.a

$(LUAMOD)/src/liblua.a:	$(LUAMOD)/src/luaconf.h
ifeq ($(USE_LUAMOD),1)
	$(MAKE) -C $(LUAMOD) $(LUA_PLATFORM)
endif

$(LUAMOD)/src/luaconf.h:	$(LUAMOD)/src/luaconf.h.orig
ifeq ($(USE_LUAMOD),1)
	(cd $(LUAMOD)/src; ln -s $(notdir $<) $(notdir $@))
endif

.PHONY: LUAMOD-clean
LUAMOD-clean:
ifeq ($(USE_LUAMOD),1)
	$(MAKE) -e -C $(LUAMOD) clean
	(cd $(LUAMOD)/src; $(RM) $(LUAMOD)/src/luaconf.h)
endif

.PHONY: LUAMOD-prepare
LUAMOD-prepare:	$(LUAMOD)/src/luaconf.h

###############################################################################
##  LUA JIT MODULE                                                           ##
###############################################################################

ifeq ($(USE_LUAJIT),1)
  ifndef LUAJIT
    $(warning ***************************************************************)
    $(warning *)
    $(warning *  LUAJIT is not defined. )
    $(warning *  LUAJIT should be set to a valid path. )
    $(warning *)
    $(warning ***************************************************************)
    $(error )
  endif

  ifeq ($(wildcard $(LUAJIT)/Makefile),)
    $(warning ***************************************************************)
    $(warning *)
    $(warning *  LUAJIT is '$(LUAJIT)')
    $(warning *  LUAJIT doesn't contain 'Makefile'. )
    $(warning *  LUAJIT should be set to a valid path. )
    $(warning *)
    $(warning ***************************************************************)
    $(error )
  endif
endif

.PHONY: LUAJIT-build
LUAJIT-build:	$(LUAJIT)/src/libluajit.a

$(LUAJIT)/src/libluajit.a:	$(LUAJIT)/src/luaconf.h
ifeq ($(USE_LUAJIT),1)
	$(MAKE) -C $(LUAJIT) Q= TARGET_SONAME=libluajit.so CCDEBUG=-g CFLAGS= LDFLAGS=
endif

$(LUAJIT)/src/luaconf.h:	$(LUAJIT)/src/luaconf.h.orig
ifeq ($(USE_LUAJIT),1)
	(cd $(LUAJIT)/src; ln -s $(notdir $<) $(notdir $@))
endif

.PHONY: LUAJIT-clean
LUAJIT-clean:	$(LUAJIT)/src/luaconf.h
ifeq ($(USE_LUAJIT),1)
	$(MAKE) -e -C $(LUAJIT) clean
	(cd $(LUAJIT)/src; $(RM) $(LUAJIT)/src/luaconf.h $(LUAJIT)/src/libluajit.a)
endif

.PHONY: LUAJIT-prepare
LUAJIT-prepare:	$(LUAJIT)/src/luaconf.h
