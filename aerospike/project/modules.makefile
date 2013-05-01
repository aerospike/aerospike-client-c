
COMMON 	= $(MODULES)/common
MOD_LUA = $(MODULES)/mod-lua
MSGPACK = $(MODULES)/msgpack

SUBMODULES = $(COMMON) $(MOD_LUA) $(MSGPACK)

ifeq ($(wildcard $(COMMON)/Makefile),) 
$(warning ***************************************************************)
$(warning *)
$(warning *  COMMON ($(COMMON)) doesn't contain a Makefile. )
$(warning *  COMMON must reference a valid path.)
$(warning *)
INVALID_MODULES += $(COMMON)
endif

ifeq ($(wildcard $(MOD_LUA)/Makefile),) 
$(warning ***************************************************************)
$(warning *)
$(warning *  MOD_LUA ($(MOD_LUA)) doesn't contain a Makefile. )
$(warning *  MOD_LUA must reference a valid path.)
$(warning *)
INVALID_MODULES += $(MOD_LUA)
endif

ifeq ($(wildcard $(MSGPACK)/configure),) 
$(warning ***************************************************************)
$(warning *)
$(warning *  MSGPACK ($(MSGPACK)) doesn't contain a configure. )
$(warning *  MSGPACK must reference a valid path.)
$(warning *)
INVALID_MODULES += $(MSGPACK)
endif

ifdef INVALID_MODULES
$(warning ***************************************************************)
ifdef GIT
NSUBMODULES = $(shell cd $(REPO) && git submodule status | awk '{print $$2}' | grep $(addprefix -e , $(INVALID_MODULES:%='%')) | wc -l)
ifneq ($(NSUBMODULES),0)
$(warning *)
$(warning *  Looks like you are using Git submodules.)
$(warning *)
$(warning *  Try running the following:)
$(warning *)
$(warning *     $$ cd .. && git submodule update --init --recursive)
$(warning *)
$(warning *  If that doesn't work, remove the modules directory, )
$(warning *  then retry the steps above:)
$(warning *)
$(warning *     $$ rm -rf modules)
$(warning *)
$(warning ***************************************************************)
endif
endif
$(error )
endif
