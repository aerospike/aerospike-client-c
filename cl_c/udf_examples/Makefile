#

.PHONY: default
default: all
	@echo "done."


%:
	$(MAKE) -C rec_udf $@
	$(MAKE) -C loop_udf $@
	$(MAKE) -C ad_udf $@

