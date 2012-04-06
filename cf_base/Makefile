# Citrusleaf Foundation
# Makefile

all: libcf-client
	cp lib/* ../../../shared/lib
	cp -r include/* ../../../shared/include
	@echo "done."

clean:
	rm -f obj/native/*
	rm -f lib/*

%:
#	$(MAKE) -f Makefile.32 -C src $@   # some are having trouble with the xcompile
#	$(MAKE) -f Makefile.64 -C src $@
	$(MAKE) -f Makefile.native -C src $@
