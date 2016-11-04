Ramdis
======
Ramdis is a RAMCloud-based implementation of Redis server, which communicates
with clients using the Redis binary protocol. The goal is for any Redis client
to be compatible with Ramdis.

# Instructions
* `git submodule update --init --recursive`
* `cd ramdis-server/`
* Change these lines in the Makefile to point to your RAMCloud directories:
```
RAMCLOUD_SRC := $(HOME)/RAMCloud/src
RAMCLOUD_LIB := $(HOME)/RAMCloud/obj.master
```
* `make`
* Start ramdis-server, using your current RAMCloud coordinator locator string:
```
./ramdis-server basic+udp:host=192.168.1.101,port=12246
```
* Send ramdis-server get and set commands using the Redis binary protocol:
```
printf "*3\r\n\$3\r\nset\r\n\$5\r\nmykey\r\n\$7\r\nmyvalue\r\n" | nc 127.0.0.1
6379
+OK
printf "*2\r\n\$3\r\nget\r\n\$5\r\nmykey\r\n" | nc 127.0.0.1 6379
$7myvalue
```
