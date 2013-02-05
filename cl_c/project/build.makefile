ROOT = $(CURDIR)
NAME = $(shell basename $(ROOT))
OS = $(shell uname)
ARCH = $(shell arch)

PROJECT = project
MODULES = 

#
# Setup Tools
#

CC = gcc
CFLAGS = -Werror

LD = gcc
LDFLAGS =

AR = ar
ARFLAGS =

#
# Setup Source
#

SOURCE = src
SOURCE_MAIN = $(SOURCE)/main
SOURCE_INCL = $(SOURCE)/include
SOURCE_TEST = $(SOURCE)/test

VPATH = $(SOURCE_MAIN) $(SOURCE_INCL)

LIB_PATH = 
INC_PATH = $(SOURCE_INCL)

#
# Setup Target
#

TARGET = target

ifeq ($(shell test -e $(PROJECT)/target.$(OS)-$(ARCH).makefile && echo 1), 1)
PLATFORM = $(OS)-$(ARCH)
include $(PROJECT)/target.$(PLATFORM).makefile
else
ifeq ($(shell test -e project/target.$(OS)-noarch.makefile && echo 1), 1)
PLATFORM = $(OS)-noarch
include $(PROJECT)/target.$(PLATFORM).makefile
else
PLATFORM = $(OS)-$(ARCH)
endif
endif

TARGET_BASE = $(TARGET)/$(PLATFORM)
TARGET_BIN = $(TARGET_BASE)/bin
TARGET_DOC = $(TARGET_BASE)/doc
TARGET_LIB = $(TARGET_BASE)/lib
TARGET_OBJ = $(TARGET_BASE)/obj
TARGET_INCL = $(TARGET_BASE)/include

#
# Builds an object, library, archive or executable using the dependencies specified for the target.
# 
# x: [dependencies]
#   $(call <command>, include_paths, library_paths, libraries, flags)
#
# Commands:
# 		build 				- Automatically determine build type based on target name.
# 		object 				- Build an object: .o
# 		library 			- Build a dynamic shared library: .so
# 		archive 			- Build a static library (archive): .a
#		executable 			- Build an executable
# 
# Arguments:
#		include_paths		- Space separated list of search paths for include files.
#							  Relative paths are relative to the project root.
#		library_paths		- Space separated list of search paths for libraries.
#							  Relative paths are relative to the project root.
#		libraries			- space separated list of libraries.
#		flags 				- space separated list of linking flags.
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
		$(addprefix -I, $(MODULES:%=modules/%/$(SOURCE_INCL))) \
		$(addprefix -I, $(INC_PATH)) \
		$(addprefix -I, $($@_inc_path)) \
		$(addprefix -I, $(1)) \
		$(addprefix -L, $(MODULES:%=modules/%/$(TARGET_LIB))) \
		$(addprefix -L, $(LIB_PATH)) \
		$(addprefix -L, $($@_lib_path)) \
		$(addprefix -L, $(2)) \
		$(addprefix -l, $($@_lib)) \
		$(addprefix -l, $(3)) \
		$(4) \
		$(LDFLAGS) \
		$($@_flags) \
		-o $@ \
		$^ \
		$(5) \
	)
endef

define archive
	@if [ ! -d `dirname $@` ]; then mkdir -p `dirname $@`; fi
	$(strip $(AR) rcs $(ARFLAGS) $(4) $@ $^ $(5))
endef

define library
	@if [ ! -d `dirname $@` ]; then mkdir -p `dirname $@`; fi
	$(strip $(CC) -shared \
		$(addprefix -I, $(MODULES:%=modules/%/$(SOURCE_INCL))) \
		$(addprefix -I, $(INC_PATH)) \
		$(addprefix -I, $($@_inc_path)) \
		$(addprefix -I, $(1)) \
		$(addprefix -L, $(MODULES:%=modules/%/$(TARGET_LIB))) \
		$(addprefix -L, $(LIB_PATH)) \
		$(addprefix -L, $($@_lib_path)) \
		$(addprefix -L, $(2)) \
		$(addprefix -l, $($@_lib)) \
		$(addprefix -l, $(3)) \
		$(4) \
		$(LDFLAGS) \
		$($@_flags) \
		-o $@ \
		$^ \
		$(5) \
	)
endef

define object
	@if [ ! -d `dirname $@` ]; then mkdir -p `dirname $@`; fi
	$(strip $(CC) \
		-MD \
		$(addprefix -I, $(MODULES:%=modules/%/$(SOURCE_INCL))) \
		$(addprefix -I, $(INC_PATH)) \
		$(addprefix -I, $($@_inc_path)) \
		$(addprefix -I, $(1)) \
		$(addprefix -L, $(MODULES:%=modules/%/$(TARGET_LIB))) \
		$(addprefix -L, $(LIB_PATH)) \
		$(addprefix -L, $($@_lib_path)) \
		$(addprefix -L, $(2)) \
		$(addprefix -l, $($@_lib)) \
		$(addprefix -l, $(3)) \
		$(4) \
		$(CFLAGS) \
		$($@_flags) \
		-o $@ \
		-c $^ \
		$(5) \
	)
endef

#
# Builds the objects specified for use by a build target.
#
# $(call objects, [objects])
#
# Arguments:
# 		objects				- space separated list of object file names (i.e. x.o)
#

define objects
	$(addprefix $(TARGET_OBJ)$(addprefix /, $(2)), $(addprefix /, $(1))) 
endef


define make_each
	@for i in $(1); do \
		make -C $$i $(2);\
	done;
endef

# 
# Common Targets
#

# .PHONY: $(TARGET)
$(TARGET):
	mkdir $@

$(TARGET_BASE): | $(TARGET)
	mkdir $@

$(TARGET_BIN): | $(TARGET_BASE)
	mkdir $@

$(TARGET_DOC): | $(TARGET_BASE)
	mkdir $@

$(TARGET_LIB): | $(TARGET_BASE)
	mkdir $@

$(TARGET_OBJ): | $(TARGET_BASE)
	mkdir $@

.PHONY: info
info:
	@echo
	@echo "  NAME:     " $(NAME) 
	@echo "  OS:       " $(OS)
	@echo "  ARCH:     " $(ARCH)
	@echo
	@echo "  PATHS:"
	@echo "      source:     " $(SOURCE)
	@echo "      target:     " $(TARGET_BASE)
	@echo "      includes:   " $(INC_PATH)
	@echo "      libraries:  " $(LIB_PATH)
	@echo "      modules:    " $(MODULES:%=modules/%)
	@echo
	@echo "  COMPILER:"
	@echo "      command:    " $(CC)
	@echo "      flags:      " $(CFLAGS)
	@echo
	@echo "  LINKER:"
	@echo "      command:    " $(LD)
	@echo "      flags:      " $(LDFLAGS)
	@echo

.PHONY: clean
clean: 
	@rm -rf $(TARGET)
	$(call make_each, $(MODULES:%=modules/%), clean)


.PHONY: $(TARGET_OBJ)/%.o
$(TARGET_OBJ)/%.o : %.c | $(TARGET_OBJ) 
	$(call object)


.DEFAULT_GOAL := all
