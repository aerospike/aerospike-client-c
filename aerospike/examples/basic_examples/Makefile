.PHONY: default
default: all

%:
	$(MAKE) -C append $@
	$(MAKE) -C expire $@
	$(MAKE) -C generation $@
	$(MAKE) -C get $@
	$(MAKE) -C incr $@
	$(MAKE) -C put $@
	$(MAKE) -C touch $@
	$(MAKE) -C udf $@
	$(MAKE) -C lstack $@
	$(MAKE) -C lset $@
