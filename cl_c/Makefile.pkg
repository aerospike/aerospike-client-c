# Aerospike Inc.
# Makefile

all: libcitrusleaf
	$(MAKE) -C example
	$(MAKE) -C tests/key_c 
	$(MAKE) -C tests/loop_c

	@echo "done."

clean:
	rm -f obj/i86/*
	rm -f obj/x64/*
	rm -f obj/native/*
	rm -f lib/*
	rm -f lib32/*
	rm -f lib64/*
	rm -f example/cl_example
	rm -f example/obj/*
	rm -f tests/key_c/obj/*
	rm -f tests/key_c/bin/*
	rm -f tests/loop_c/obj/*
	rm -f tests/loop_c/bin/*

%:
#	$(MAKE) -f Makefile.32 -C src $@   # some are having trouble with the xcompile
#	$(MAKE) -f Makefile.64 -C src $@
	$(MAKE) -f Makefile.native -C src $@

