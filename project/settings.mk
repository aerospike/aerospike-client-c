###############################################################################
##  BUILD VARIABLES                                                          ##
###############################################################################

export CFLAGS =
export LDFLAGS =
export ARFLAGS =

###############################################################################
##  BUILD ENVIRONMENT                                                        ##
###############################################################################

NAME = $(shell basename $(CURDIR))
OS = $(shell uname)

PROJECT = project
MODULES = modules
SOURCE  = src
TARGET  = target

MODULES = 

###############################################################################
##  BUILD TOOLS                                                              ##
###############################################################################

ifeq ($(OS),Darwin)
  ARCH = $(shell uname -m)
  DYNAMIC_SUFFIX=dylib
  DYNAMIC_FLAG=-dynamiclib
else
  ARCH = $(shell arch)
  DYNAMIC_SUFFIX=so
  DYNAMIC_FLAG=-shared
endif

CC = cc
CC_FLAGS =

LD = cc
LD_FLAGS =

AR = ar
AR_FLAGS =

###############################################################################
##  SOURCE PATHS                                                             ##
###############################################################################

SOURCE_PATH = $(SOURCE)
SOURCE_MAIN = $(SOURCE_PATH)/main
SOURCE_INCL = $(SOURCE_PATH)/include
SOURCE_TEST = $(SOURCE_PATH)/test

VPATH = $(SOURCE_MAIN) $(SOURCE_INCL)

LIB_PATH = 
INC_PATH = $(SOURCE_INCL)

###############################################################################
##  TARGET PATHS                                                             ##
###############################################################################

PLATFORM = $(OS)-$(ARCH)
TARGET_BASE = $(TARGET)/$(PLATFORM)
TARGET_LIB  = $(TARGET_BASE)/lib
TARGET_OBJ  = $(TARGET_BASE)/obj
TARGET_INCL = $(TARGET_BASE)/include
TARGET_TEST = $(TARGET_BASE)/test

###############################################################################
##  FUNCTIONS                                                                ##
###############################################################################

#
# Builds an object, library, archive or executable using the dependencies specified for the target.
# 
# x: [dependencies]
#   $(call <command>, include_paths, library_paths, libraries, flags)
#
# Commands:
# 		build 			- Automatically determine build type based on target name.
# 		object 			- Build an object: .o
# 		library 		- Build a dynamic shared library: .so
# 		archive 		- Build a static library (archive): .a
#		executable 		- Build an executable
# 
# Arguments:
#		include_paths	- Space separated list of search paths for include files.
#						  Relative paths are relative to the project root.
#		library_paths	- Space separated list of search paths for libraries.
#						  Relative paths are relative to the project root.
#		libraries		- space separated list of libraries.
#		flags 			- space separated list of linking flags.
#
# You can optionally define variables, rather than arguments as:
#
#	X_inc_path = [include_paths]
#	X_lib_path = [library_paths]
#	X_lib = [libraries]
# 	X_flags = [flags]
#
# Where X is the name of the build target.
#

define build
	$(if $(filter .o,$(suffix $@)), 
		$(call object, $(1),$(2),$(3),$(4)),
		$(if $(filter .so,$(suffix $@)), 
			$(call library, $(1),$(2),$(3),$(4)),
			$(if $(filter .a,$(suffix $@)), 
				$(call archive, $(1),$(2),$(3),$(4)),
				$(call executable, $(1),$(2),$(3),$(4))
			)
		)
	)
endef

define executable
	@if [ ! -d `dirname $@` ]; then mkdir -p `dirname $@`; fi
	$(strip $(CC) \
		$(addprefix -I, $(INC_PATH)) \
		$(addprefix -L, $(SUBMODULES:%=%/$(TARGET_LIB))) \
		$(addprefix -L, $(LIB_PATH)) \
		$(addprefix -l, $(LIBRARIES)) \
		$(CC_FLAGS) \
		$(CFLAGS) \
		-o $@ \
		$(filter %.o %.a %.so, $^) \
		$(LD_FLAGS) \
	)
endef

define archive
	@if [ ! -d `dirname $@` ]; then mkdir -p `dirname $@`; fi
	$(strip $(AR) \
		rcs \
		$(AR_FLAGS) \
		$(ARFLAGS) \
		$@ \
		$(filter %.o, $^) \
	)
endef

define library
	@if [ ! -d `dirname $@` ]; then mkdir -p `dirname $@`; fi
	$(strip $(CC) $(DYNAMIC_FLAG) \
		$(addprefix -I, $(INC_PATH)) \
		$(addprefix -L, $(SUBMODULES:%=%/$(TARGET_LIB))) \
		$(addprefix -L, $(LIB_PATH)) \
		$(addprefix -l, $(LIBRARIES)) \
		-o $@ \
		$(filter %.o, $^) \
		$(LD_FLAGS) \
	)
endef

define object
	@if [ ! -d `dirname $@` ]; then mkdir -p `dirname $@`; fi
	$(strip $(CC) \
		$(addprefix -I, $(INC_PATH)) \
		$(addprefix -L, $(SUBMODULES:%=%/$(TARGET_LIB))) \
		$(addprefix -L, $(LIB_PATH)) \
		$(CC_FLAGS) \
		$(CFLAGS) \
		-o $@ \
		-c $(filter %.c %.cpp, $^)  \
	)
endef

define make_each
	@for i in $(1); do \
		if [ -e "$$i/Makefile" ]; then \
			make -C $$i $(2);\
		fi \
	done;
endef
