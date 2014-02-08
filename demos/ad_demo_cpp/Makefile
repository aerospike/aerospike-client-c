.DEFAULT: all

.PHONY: all
all: build

.PHONY: build
build: bin/ad_udf

.PHONY: prepare
prepare: 
	@if [ ! -e bin ]; then mkdir bin; fi

bin/ad_udf: prepare
	$(MAKE) -e -C src/main

clean:
	rm -rf bin

OPTIONS = -v -b 10000 -u 1000 

demo: build
	bin/ad_udf $(OPTIONS)
