.PHONY: default
default: all

%:
	$(MAKE) -C aerospike $@
	$(MAKE) -C citrusleaf-base $@
	$(MAKE) -C citrusleaf-client $@
	$(MAKE) -C citrusleaf-libevent $@
