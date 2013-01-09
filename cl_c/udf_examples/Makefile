# entire sproc_tests project
#

.PHONY: default
default: all
	@echo "done."


%:
	$(MAKE) -C rec_sproc $@
	$(MAKE) -C loop_sproc $@
	$(MAKE) -C ad_sproc $@
	$(MAKE) -C scan_sproc $@
