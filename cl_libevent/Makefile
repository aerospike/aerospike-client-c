# Citrusleaf Foundation
# Makefile

default: libevcitrusleaf
	$(MAKE) -C example
	$(MAKE) -C example2
	$(MAKE) -C example3
	$(MAKE) -C example4
	$(MAKE) -C example5
	cp lib/libevcitrusleaf.a ../../../shared/lib
	cp -r include/* ../../../shared/include
	rm ../../../shared/include/citrusleaf_event/*internal*
	@echo "done."

clean:
	rm -f obj/i86/*
	rm -f obj/x64/*
	rm -f obj/native/*
	rm -f lib/*
	rm -f lib32/*
	rm -f lib64/*
	rm -f example/cl_evexample
	rm -f example/obj/*
	rm -f example2/cl_evexample
	rm -f example2/obj/*
	rm -f example3/cl_evexample
	rm -f example3/obj/*
	rm -f example4/cl_evexample
	rm -f example4/obj/*
	rm -f example5/cl_eveample
	rm -f example5/obj/*


%:
#	$(MAKE) -f Makefile.32 -C src $@
#	$(MAKE) -f Makefile.64 -C src $@
	$(MAKE) -f Makefile.native -C src $@
