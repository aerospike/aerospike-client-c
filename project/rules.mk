###############################################################################
##  COMMON RULES                                                             ##
###############################################################################

$(TARGET):
	mkdir $@

$(TARGET_BASE): | $(TARGET)
	mkdir $@

$(TARGET_LIB): | $(TARGET_BASE)
	mkdir $@

$(TARGET_OBJ): | $(TARGET_BASE)
	mkdir $@

$(TARGET_INCL): | $(TARGET_BASE)
	mkdir $@

$(TARGET_TEST): | $(TARGET_BASE)
	mkdir $@

.PHONY: info
info:
	@echo
	@echo "  NAME:     " $(NAME) 
	@echo "  OS:       " $(OS)
	@echo "  ARCH:     " $(ARCH)
	@echo "  DISTRO:   " $(DISTRO)
	@echo "  PKG:      " $(PKG)
	@echo "  MODULES:  " $(MODULES)
	@echo "  LUA_STATIC_OBJ:  " $(LUA_STATIC_OBJ)
	@echo
	@echo "  PATHS:"
	@echo "      source:     " $(SOURCE)
	@echo "      target:     " $(TARGET_BASE)
	@echo "      includes:   " $(INC_PATH)
	@echo "      libraries:  " $(LIB_PATH)
	@echo
	@echo "  COMPILER:"
	@echo "      command:    " $(CC)
	@echo "      flags:      " $(CC_FLAGS) $(CFLAGS)
	@echo
	@echo "  LINKER:"
	@echo "      command:    " $(LD)
	@echo "      flags:      " $(LD_FLAGS)
	@echo
	@echo "  ARCHIVER:"
	@echo "      command:    " $(AR)
	@echo "      flags:      " $(AR_FLAGS) $(ARFLAGS)
	@echo

.PHONY: $(TARGET_OBJ)/%.o
$(TARGET_OBJ)/%.o : %.c | $(TARGET_OBJ) 
	$(object)

###############################################################################
##  MODULE RULES                                                             ##
###############################################################################

.PHONY: modules
modules: modules-build modules-prepare

.PHONY: modules-build
modules-build: $(MODULES:%=%-build)

.PHONY: modules-prepare
modules-prepare: $(MODULES:%=%-prepare)

.PHONY: modules-clean
modules-clean: $(MODULES:%=%-clean)
