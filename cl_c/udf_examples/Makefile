#

.PHONY: default
default: all
	@echo "done."


%:
	$(MAKE) -C rec_udf $@
	$(MAKE) -C loop_udf $@
	$(MAKE) -C ad_udf $@
#	$(MAKE) -C lso_udf $@
#	$(MAKE) -C as_set_udf $@

