#ifndef __RAMDIS_H
#define __RAMDIS_H

#include <stdint.h>

/* Redis data structures. */
#define REDIS_STRING 1
#define REDIS_LIST 2
#define REDIS_SET 3
#define REDIS_SORTEDSET 4
#define REDIS_HASH 5
#define REDIS_HYPERLOGLOG 6

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

#if VERBOSITY>=LL_FATAL
#define FATAL(fmt, ...) printf("%s:%d:%s(): FATAL: " fmt, __FILE__, __LINE__, __func__, ##__VA_ARGS__)
#else
#define FATAL(...)
#endif

#if VERBOSITY>=LL_ERROR
#define ERROR(fmt, ...) printf("%s:%d:%s(): ERROR: " fmt, __FILE__, __LINE__, __func__, ##__VA_ARGS__)
#else
#define ERROR(...)
#endif

#if VERBOSITY>=LL_WARN
#define WARN(fmt, ...) printf("%s:%d:%s(): WARN: " fmt, __FILE__, __LINE__, __func__, ##__VA_ARGS__)
#else
#define WARN(...)
#endif

#if VERBOSITY>=LL_INFO
#define INFO(fmt, ...) printf("%s:%d:%s(): INFO: " fmt, __FILE__, __LINE__, __func__, ##__VA_ARGS__)
#else
#define INFO(...)
#endif

#if VERBOSITY>=LL_DEBUG
#define DEBUG(fmt, ...) printf("%s:%d:%s(): DEBUG: " fmt, __FILE__, __LINE__, __func__, ##__VA_ARGS__)
#else
#define DEBUG(...)
#endif

#if VERBOSITY>=LL_TRACE
#define TRACE(fmt, ...) printf("%s:%d:%s(): TRACE: " fmt, __FILE__, __LINE__, __func__, ##__VA_ARGS__)
#else
#define TRACE(...)
#endif

#ifdef __cplusplus
extern "C" {
#endif
  typedef struct {
    void* client; // RAMCloud::RamCloud*
    uint64_t tableId;
    int err;
    char errmsg[256];
  } Context;

  typedef struct {
    void* data;
    uint32_t len;
  } Object;

  typedef struct {
    Object* array;
    uint32_t len;
  } ObjectArray;

  /* Connection */
  Context* ramdis_connect(char* locator, uint16_t serverSpan);
  void ramdis_disconnect(Context* c);
  char* ping(Context* c, char* msg);

  /* Strings */
  Object* get(Context* c, Object* key);
  void set(Context* c, Object* key, Object* value);
  void mset(Context* c, ObjectArray* keysArray, ObjectArray* valuesArray);
  long incr(Context* c, Object* key);

  /* Lists */
  uint64_t lpush(Context* c, Object* key, Object* value);
  uint64_t rpush(Context* c, Object* key, Object* value);
  Object* lpop(Context* c, Object* key);
  Object* rpop(Context* c, Object* key);
  ObjectArray* lrange(Context* c, Object* key, long start, long end);

  /* Sets */
  uint64_t sadd(Context* c, Object* key, ObjectArray* valuesArray);
  Object* spop(Context* c, Object* key);

  /* Sorted Sets */

  /* Hashes */

  /* HyperLogLog */

  /* All */
  uint64_t del(Context* c, ObjectArray* keysArray);

  /* Misc. */
  void freeObject(Object* obj);
  void freeObjectArray(ObjectArray* objArray);
  void printObjectArray(ObjectArray* objArray);

#ifdef __cplusplus
}
#endif

#endif
