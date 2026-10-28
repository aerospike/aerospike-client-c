.PHONY: default
default: all

%:
	$(MAKE) -C aggregate $@
	$(MAKE) -C projection $@
	$(MAKE) -C simple $@
	$(MAKE) -C topk $@
	$(MAKE) -C vector_knn $@
