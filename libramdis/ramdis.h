#ifndef __RAMDIS_H
#define __RAMDIS_H

#ifdef __cplusplus
extern "C" {
#endif
  void* connect(char* locator);
  char* ping(void* context, char* msg);
  void set(void* context, void* key, void* value);
  void* get(void* context, void* key);
  long incr(void* context, void* key);
  uint64_t lpush(void* context, void* key, void* value);
  uint64_t rpush(void* context, void* key, void* value);
  void* lpop(void* context, void* key);
  void* rpop(void* context, void* key);
  uint64_t sadd(void* context, void* key, void** values);
  void* spop(void* context, void* key);
  void** lrange(void* context, void* key, long start, long end);
  void mset(void* context, void** keys, void** values);
#ifdef __cplusplus
}
#endif

#endif
