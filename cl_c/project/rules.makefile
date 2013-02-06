###############################################################################
##  COMMON TARGETS                                                           ##
###############################################################################

$(TARGET_PATH):
	mkdir $@

$(TARGET_BASE): | $(TARGET_PATH)
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
	@echo "  DISTRO:   " $(DISTRO_NAME)"-"$(DISTRO_VERS)
	@echo
	@echo "  PATHS:"
	@echo "      source:     " $(SOURCE)
	@echo "      target:     " $(TARGET_BASE)
	@echo "      includes:   " $(INC_PATH)
	@echo "      libraries:  " $(LIB_PATH)
	@echo "      submodules: " $(SUBMODULES)
	@echo
	@echo "  COMPILER:"
	@echo "      command:    " $(CC)
	@echo "      flags:      " $(CC_FLAGS)
	@echo
	@echo "  LINKER:"
	@echo "      command:    " $(LD)
	@echo "      flags:      " $(LD_FLAGS)
	@echo
	@echo "  ARCHIVER:"
	@echo "      command:    " $(AR)
	@echo "      flags:      " $(AR_FLAGS)
	@echo

.PHONY: clean
clean: 
	@rm -rf $(TARGET)
	$(call make_each, $(SUBMODULES), clean)


.PHONY: $(TARGET_OBJ)/%.o
$(TARGET_OBJ)/%.o : %.c | $(TARGET_OBJ) 
	$(object)

.PHONY: all

.DEFAULT_GOAL := all

%.o:
	$(object)

%.a: 
	$(archive)

%.so: 
	$(library)

%: 
	$(executable)
