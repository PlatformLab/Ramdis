# Ramdis
A RAMCloud-based implementation of Redis

# Instructions
 - Modify the `Makefile` to point to your local RAMCloud directory.
 - Set `LD_LIBRARY_PATH` to the directory of your libramcloud.so library.
 - `make`
 - Run:
```
./ramdis-cli
connect basic+udp:host=192.168.1.110,port=12246
set sally "some silly quote sally would say."
get sally
some silly quote sally would say.
```
