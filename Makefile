
all:
	$(MAKE) -C aerospike
	$(MAKE) -C citrusleaf-base
	$(MAKE) -C citrusleaf-client
	$(MAKE) -C citrusleaf-libevent

clean:
	$(MAKE) -C aerospike clean $@
	$(MAKE) -C citrusleaf-base $@
	$(MAKE) -C citrusleaf-client $@
	$(MAKE) -C citrusleaf-libevent $@
