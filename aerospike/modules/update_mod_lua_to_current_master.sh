#!/bin/bash

rm -rf mod-lua
git clone git@github.com:aerospike/aerospike-mod-lua.git
mv aerospike-mod-lua mod-lua
git add mod-lua
(cd mod-lua; git submodule init; git submodule update)
