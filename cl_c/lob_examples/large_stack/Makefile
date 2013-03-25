# Citrusleaf Tools
# Makefile

.PHONY: default
default: all
	@echo "done."

clean:
	rm -f obj/*
	rm -f bin/*

%:
	$(MAKE) -C src $@
