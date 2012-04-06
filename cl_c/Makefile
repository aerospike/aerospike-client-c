# Citrusleaf Foundation
# Makefile

all: libcitrusleaf
	cp lib/* ../../../shared/lib
	cp include/citrusleaf/citrusleaf.h ../../../shared/include/citrusleaf
	cp include/citrusleaf/cl_cluster.h ../../../shared/include/citrusleaf
	@echo "done."

clean:
	rm -f obj/i86/*
	rm -f obj/x64/*
	rm -f obj/native/*
	rm -f lib/*
	rm -f lib32/*
	rm -f lib64/*

%:
#	$(MAKE) -f Makefile.32 -C src $@   # some are having trouble with the xcompile
#	$(MAKE) -f Makefile.64 -C src $@
	$(MAKE) -f Makefile.native -C src $@
