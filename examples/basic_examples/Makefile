.PHONY: default
default: all

%:
	$(MAKE) -C append $@
	$(MAKE) -C expire $@
	$(MAKE) -C generation $@
	$(MAKE) -C get $@
	$(MAKE) -C incr $@
	$(MAKE) -C list $@
	$(MAKE) -C llist $@
	$(MAKE) -C lmap $@
	$(MAKE) -C lset $@
	$(MAKE) -C lstack $@
	$(MAKE) -C map $@
	$(MAKE) -C put $@
	$(MAKE) -C touch $@
	$(MAKE) -C udf $@

