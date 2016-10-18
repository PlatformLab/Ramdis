Ramdis
======
Ramdis is a RAMCloud-based implementation of Redis. Its current form is as a C
library which exports the [hiredis](https://github.com/redis/hiredis) API.
Future forms may include as a server that implements the Redis binary protocol.

# Instructions
* `git submodule update --init --recursive`
* Change these lines in the Makefile to point to your RAMCloud directories:
```
RAMCLOUD_SRC := $(HOME)/RAMCloud/src
RAMCLOUD_LIB := $(HOME)/RAMCloud/obj.master
```
* Similarly change the Makefile in the examples/ directory.
* `make`
* See examples directory for examples of clients.
