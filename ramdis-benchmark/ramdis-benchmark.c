#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>
#include <inttypes.h>
#include <pthread.h>

#include "ramdis.h"

const char USAGE[] =
"Ramdis Benchmark.\n"
"\n"
"Usage:\n"
"  ramdis-benchmark -C <coordinator> [OPTIONS]\n"
"  ramdis-benchmark --help\n"
"  ramdis-benchmark --version\n"
"\n"
"Options:\n"
"  -C <coordinator>    Address of RAMCloud coordinator.\n"
"  -c <clients>        Number of benchmark clients to run in parallel \n"
"                      [default: 1]\n"
"  -n <requests>       Number of requests each client should execute \n"
"                      [default: 100000]\n"
"  -d <size>           Size in bytes of value to read/write in \n"
"                      GET/SET/PUSH/POP/SADD/SPOP, etc. [default: 3]\n"
"  -r <keyspacelen>    Execute operations on a random set of keys in the\n"
"                      space from [0,keyspacelen) [default: 1]\n"
"  -t <tests>          Comma separated list of tests to run. Available \n"
"                      tests: all, get, set, incr, lpush, rpush, lpop, \n"
"                      rpop, sadd, spop, lrange, mset. [default: all]\n"
"  -h --help           Show this screen.\n"
"  --version           Show version.\n"
"\n"
"";

const char VERSION[] = "0.1";

uint64_t ustime(void) {
  struct timeval tv;
  uint64_t ust;
  gettimeofday(&tv, NULL);
  ust = ((uint64_t)tv.tv_sec)*1000000;
  ust += tv.tv_usec;
  return ust;
}

int compareUint64_t(const void *a, const void *b) {
  return (*(uint64_t*)a)-(*(uint64_t*)b);
}

struct WorkerArgs {
  char* coordinatorLocator;
  uint64_t requests;
  uint64_t valueSize;
  uint64_t keySpaceLength;
};

struct WorkerStats {
  uint64_t* latencies;
};

void freeWorkerStats(struct WorkerStats* wStats) {
  free(wStats->latencies);
  free(wStats);
}

void reportStats(uint64_t totalTime, struct WorkerStats** wStats, 
    uint64_t clients,  uint64_t requests) {
  uint64_t i;
  for (i = 0; i < clients; i++) {
    qsort(wStats[i]->latencies, requests, sizeof(uint64_t), compareUint64_t);
  }

  printf("\tAverage Request Rate: %.2f op/s\n", 
      (float)(requests * clients) / ((float)totalTime / 1000000.0));

  for (i = 0; i < clients; i++) {
    printf("\tClient %d Stats:\n", i);
    printf("\t\tp50 Latency: %" PRId64 "us\n", 
        wStats[i]->latencies[requests/2]);
    printf("\t\tp90 Latency: %" PRId64 "us\n", 
        wStats[i]->latencies[requests*90/100]);
    printf("\t\tp95 Latency: %" PRId64 "us\n", 
        wStats[i]->latencies[requests*95/100]);
    printf("\t\tp99 Latency: %" PRId64 "us\n", 
        wStats[i]->latencies[requests*99/100]);
    printf("\t\tp99.9 Latency: %" PRId64 "us\n", 
        wStats[i]->latencies[requests*999/1000]);
  }
}

/* Worker thread for executing get command. */
void* getWorkerThread(void* args) {
  struct WorkerArgs* wArgs = (struct WorkerArgs*)args;
  char* coordinatorLocator = wArgs->coordinatorLocator;
  uint64_t requests = wArgs->requests;
  uint64_t valueSize = wArgs->valueSize;
  uint64_t keySpaceLength = wArgs->keySpaceLength;

  Context* context = ramdis_connect(coordinatorLocator);

  struct WorkerStats* wStats = (struct WorkerStats*)malloc(sizeof(struct
        WorkerStats)); 

  uint64_t* latencies = (uint64_t*)malloc(requests*sizeof(uint64_t));

  int i;
  Object key;
  char keyBuf[16];
  key.len = sizeof(keyBuf);
  uint64_t testStart = ustime();
  for (i = 0; i < requests; i++) {
    snprintf(keyBuf, 16, "%015d", rand() % keySpaceLength);
    key.data = keyBuf;

    uint64_t reqStart = ustime();
    Object* value = get(context, &key);
    latencies[i] = ustime() - reqStart;
    freeObject(value);
  }
  uint64_t testEnd = ustime(); 

  wStats->latencies = latencies;

  return wStats;
}

/* Worker thread for executing set command. */
void* setWorkerThread(void* args) {
  struct WorkerArgs* wArgs = (struct WorkerArgs*)args;
  char* coordinatorLocator = wArgs->coordinatorLocator;
  uint64_t requests = wArgs->requests;
  uint64_t valueSize = wArgs->valueSize;
  uint64_t keySpaceLength = wArgs->keySpaceLength;

  Context* context = ramdis_connect(coordinatorLocator);

  struct WorkerStats* wStats = (struct WorkerStats*)malloc(sizeof(struct
        WorkerStats)); 

  uint64_t* latencies = (uint64_t*)malloc(requests*sizeof(uint64_t));

  /* Value to write for set commands. */
  Object value;
  char valBuf[valueSize];
  value.data = valBuf;
  value.len = valueSize;

  int i;
  Object key;
  char keyBuf[16];
  key.len = sizeof(keyBuf);
  uint64_t testStart = ustime();
  for (i = 0; i < requests; i++) {
    snprintf(keyBuf, 16, "%015d", rand() % keySpaceLength);
    key.data = keyBuf;

    uint64_t reqStart = ustime();
    set(context, &key, &value);
    latencies[i] = ustime() - reqStart;
  }
  uint64_t testEnd = ustime(); 

  wStats->latencies = latencies;

  return wStats;
}

/* Worker thread for executing incr command. */
void* incrWorkerThread(void* args) {
  struct WorkerArgs* wArgs = (struct WorkerArgs*)args;
  char* coordinatorLocator = wArgs->coordinatorLocator;
  uint64_t requests = wArgs->requests;
  uint64_t valueSize = wArgs->valueSize;
  uint64_t keySpaceLength = wArgs->keySpaceLength;

  Context* context = ramdis_connect(coordinatorLocator);

  struct WorkerStats* wStats = (struct WorkerStats*)malloc(sizeof(struct
        WorkerStats)); 

  uint64_t* latencies = (uint64_t*)malloc(requests*sizeof(uint64_t));

  int i;
  Object key;
  char keyBuf[16];
  key.len = sizeof(keyBuf);
  uint64_t testStart = ustime();
  for (i = 0; i < requests; i++) {
    snprintf(keyBuf, 16, "%015d", rand() % keySpaceLength);
    key.data = keyBuf;

    uint64_t reqStart = ustime();
    incr(context, &key);
    latencies[i] = ustime() - reqStart;
  }
  uint64_t testEnd = ustime(); 

  wStats->latencies = latencies;

  return wStats;
}

int main(int argc, char* argv[]) {
  char* coordinatorLocator;
  uint64_t clients = 1;
  uint64_t requests = 100000;
  uint64_t valueSize = 3;
  uint64_t keySpaceLength = 1;
  char* tests = "all";

  // Parse command line options.
  uint64_t i;
  for (i = 1; i < argc;) {
    if (strcmp(argv[i], "-C") == 0) {
      coordinatorLocator = argv[i+1];
      i+=2;
    } else if (strcmp(argv[i], "-c") == 0) {
      clients = strtoul(argv[i+1], NULL, 10);
      i+=2;
    } else if (strcmp(argv[i], "-n") == 0) {
      requests = strtoul(argv[i+1], NULL, 10);
      i+=2;
    } else if (strcmp(argv[i], "-d") == 0) {
      valueSize = strtoul(argv[i+1], NULL, 10);
      i+=2;
    } else if (strcmp(argv[i], "-r") == 0) {
      keySpaceLength = strtoul(argv[i+1], NULL, 10);
      i+=2;
    } else if (strcmp(argv[i], "-t") == 0) {
      tests = argv[i+1];
      i+=2;
    } else if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
      printf("%s", USAGE);
      return 0;
    } else if (strcmp(argv[i], "--version") == 0) {
      printf("Version: %s\n", VERSION);
      return 0;
    } else {
      printf("Unrecognized option: %s\n", argv[i]);
      return -1;
    }
  }

  printf("Connecting to %s\n", coordinatorLocator);

  Context* context = ramdis_connect(coordinatorLocator);

  struct WorkerArgs wArgs;
  wArgs.coordinatorLocator = coordinatorLocator;
  wArgs.requests = requests;
  wArgs.valueSize = valueSize;
  wArgs.keySpaceLength = keySpaceLength;

  char* test;
  test = strtok(tests, ",");
  while (test != NULL ) {
    printf("========== %s ==========\n", test);

    void* (*workerThreadFuncPtr)();  

    if (strcmp(test, "get") == 0) {
      workerThreadFuncPtr = getWorkerThread;

      /* Do pre-workload setup. */

      Object value;
      char valBuf[valueSize];
      value.data = valBuf;
      value.len = valueSize;

      Object key;
      char keyBuf[16];
      key.len = 16;
      // Pre-create the objects to perform the read.
      for (i = 0; i < keySpaceLength; i++) {
        snprintf(keyBuf, 16, "%015" PRId64, i);
        key.data = keyBuf;

        set(context, &key, &value);
      }
    } else if (strcmp(test, "set") == 0) {
      workerThreadFuncPtr = setWorkerThread;
    } else if (strcmp(test, "incr") == 0) {
      workerThreadFuncPtr = incrWorkerThread;

      /* Do pre-workload setup. */

      Object value;
      uint64_t initialNumber = 0;
      value.data = (void*)&initialNumber;
      value.len = sizeof(uint64_t);

      Object key;
      char keyBuf[16];
      key.len = 16;
      // Pre-create the objects to perform the incr.
      for (i = 0; i < keySpaceLength; i++) {
        snprintf(keyBuf, 16, "%015" PRId64, i);
        key.data = keyBuf;

        set(context, &key, &value);
      }
    } else if (strcmp(test, "lpush") == 0) {
    } else if (strcmp(test, "rpush") == 0) {
    } else if (strcmp(test, "lpop") == 0) {
    } else if (strcmp(test, "rpop") == 0) {
    } else if (strcmp(test, "sadd") == 0) {
    } else if (strcmp(test, "spop") == 0) {
    } else if (strcmp(test, "lrange") == 0) {
    } else if (strcmp(test, "mset") == 0) {
    } else {
      printf("Unrecognized test: %s\n", test);
      return -1;
    }

    pthread_t threads[clients];
    uint64_t start = ustime();
    for (i = 0; i < clients; i++) {
      pthread_create(&threads[i], NULL, workerThreadFuncPtr, &wArgs);
    }
    
    struct WorkerStats* wStats[clients];
    for (i = 0; i < clients; i++) {
      pthread_join(threads[i], (void*)&wStats[i]);
    }
    uint64_t end = ustime();

    reportStats(end - start, wStats, clients, requests);

    for (i = 0; i < clients; i++) {
      freeWorkerStats(wStats[i]);
    }

    test = strtok(NULL, ",");
  }

  return 0;
}
