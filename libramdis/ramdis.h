#ifndef __RAMDIS_H
#define __RAMDIS_H

#define LOG_MAX_LEN    1024 /* Default maximum length of syslog messages */

/* Log levels */
#define LL_FATAL 0
#define LL_ERROR 1
#define LL_WARN 2
#define LL_INFO 3
#define LL_DEBUG 4
#define LL_TRACE 5

/* Messages with log level <= VERBOSITY will be printed. */
#ifndef VERBOSITY
#define VERBOSITY LL_INFO
#endif

#ifdef __cplusplus
extern "C" {
#endif
  typedef struct {
    void* data;
    uint32_t len;
  } Object;

  typedef struct {
    Object* array;
    uint32_t len;
  } ObjectArray;

  void printObjectArray(ObjectArray* objArray);
  void* connect(char* locator);
  void disconnect(void* context);
  void freeObject(Object* obj);
  void freeObjectArray(ObjectArray* objArray);
  char* ping(void* context, char* msg);
  void set(void* context, Object* key, Object* value);
  Object* get(void* context, Object* key);
  long incr(void* context, Object* key);
  uint64_t lpush(void* context, Object* key, Object* value);
  uint64_t rpush(void* context, Object* key, Object* value);
  Object* lpop(void* context, Object* key);
  Object* rpop(void* context, Object* key);
  uint64_t sadd(void* context, Object* key, ObjectArray* valuesArray);
  Object* spop(void* context, Object* key);
  ObjectArray* lrange(void* context, Object* key, long start, long end);
  void mset(void* context, ObjectArray* keysArray, ObjectArray* valuesArray);
#ifdef __cplusplus
}
#endif

#endif
