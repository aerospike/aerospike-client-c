# Citrusleaf Foundation
# Makefile

all: libev2citrusleaf
	$(MAKE) -C example
	$(MAKE) -C example2
	$(MAKE) -C example3
	$(MAKE) -C example4
	$(MAKE) -C example6
	$(MAKE) -C tests/loop_c_ev2
	@echo "done."

clean:
	rm -f obj/i86/*
	rm -f obj/x64/*
	rm -f obj/native/*
	rm -f lib/*
	rm -f lib32/*
	rm -f lib64/*
	rm -f example/cl_ev2example
	rm -f example/obj/*
	rm -f example2/example2
	rm -f example2/obj/*
	rm -f example3/example3
	rm -f example3/obj/*
	rm -f example4/example4
	rm -f example4/obj/*
	rm -f example6/example6
	rm -f example6/obj/*
	rm -f tests/loop_c_ev2/obj/*
	rm -f tests/loop_c_ev2/bin/*


%:
#	$(MAKE) -f Makefile.32 -C src $@
#	$(MAKE) -f Makefile.64 -C src $@
	$(MAKE) -f Makefile.native -C src $@
