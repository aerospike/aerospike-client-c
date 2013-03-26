
.PHONY: default
default: all
	@echo "done."

%:
	$(MAKE) -C large_set   $@
	$(MAKE) -C large_stack $@
	$(MAKE) -C large_list $@

