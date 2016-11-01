NOTES
=====
2016/10/28
* ramdis-server code was mostly taken from redis version 3.2.4.
* Currently client requests are put in a queue for worker threads to take and
  execute concurrently. This means that multiple requests from one client might
  execute in parallel and potentially out of order.
