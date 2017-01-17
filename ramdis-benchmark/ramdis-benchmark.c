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
"  --clientIndex <i>   Index of this client (first client is 0) \n"
"                      [default: 0].\n"
"  --numClients <n>    Total number of clients running [default: 1].\n"
"  --threads <n>       Number of benchmark client threads to run in parallel\n"
"                      [default: 1]\n"
"  -n <requests>       Number of requests each client should execute \n"
"                      [default: 100000]\n"
"  -d <size>           Size in bytes of value to read/write in \n"
"                      GET/SET/PUSH/POP/SADD/SPOP, etc. [default: 3]\n"
"  -l <lrange>         Get elements [0,lrange] for LRANGE command. Maximum \n"
"                      value is 100000 [default: 100]\n"
"  -r <keyspacelen>    Execute operations on a random set of keys in the\n"
"                      space from [0,keyspacelen) [default: 1]\n"
"  -t <tests>          Comma separated list of tests to run. Available \n"
"                      tests: all, get, set, incr, lpush, rpush, lpop, \n"
"                      rpop, sadd, spop, lrange, mset. [default: all]\n"
"  --outputDir <dir>   Directory to write performance data. If not \n"
"                      specified then no files will be written. \n"
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
  uint64_t lrangeSize;
  uint64_t keySpaceLength;
};

struct WorkerStats {
  uint64_t* latencies;
};

void freeWorkerStats(struct WorkerStats* wStats) {
  free(wStats->latencies);
  free(wStats);
}

void reportStats(char* test, uint64_t totalTime, struct WorkerStats** wStats, 
    uint64_t clientIndex, uint64_t numClients, uint64_t clientThreads,  
    uint64_t requests, char* outputDir) {
  uint64_t i;
  for (i = 0; i < clientThreads; i++) {
    qsort(wStats[i]->latencies, requests, sizeof(uint64_t), compareUint64_t);
  }

  if (totalTime / 1000000 > 0) {
    printf("Total Time: %.2fs\n", (float)totalTime / 1000000.0);
  } else if (totalTime / 1000 > 0) {
    printf("Total Time: %.2fms\n", (float)totalTime / 1000.0);
  } else {
    printf("Total Time: %" PRId64 "us\n", totalTime);
  }

  printf("Average Request Rate: %.2f op/s\n", 
      (float)(requests * clientThreads) / ((float)totalTime / 1000000.0));

  for (i = 0; i < clientThreads; i++) {
    printf("Client %d/%d Stats:\n", clientIndex * clientThreads + i + 1, 
        numClients * clientThreads);
    printf("\tp50 Latency: %" PRId64 "us\n", 
        wStats[i]->latencies[requests/2]);
    printf("\tp90 Latency: %" PRId64 "us\n", 
        wStats[i]->latencies[requests*90/100]);
    printf("\tp95 Latency: %" PRId64 "us\n", 
        wStats[i]->latencies[requests*95/100]);
    printf("\tp99 Latency: %" PRId64 "us\n", 
        wStats[i]->latencies[requests*99/100]);
    printf("\tp99.9 Latency: %" PRId64 "us\n", 
        wStats[i]->latencies[requests*999/1000]);
  }

  /* Write performance data to files. */
  if (outputDir != NULL) {
    for (i = 0; i < clientThreads; i++) {
      uint64_t threadIndex = clientIndex * clientThreads + i + 1;
      uint64_t numThreads = numClients * clientThreads;
      
      char reqLatFN[256];
      snprintf(reqLatFN, sizeof(reqLatFN), 
          "%s/%s_client%d-%d_reqLatencies.dat", 
          outputDir, test, numThreads, threadIndex);
      
      FILE *reqLatFile = fopen(reqLatFN, "w");
      
      if (reqLatFile == NULL) {
        printf("ERROR: Can't open output file %s\n", reqLatFN);
        continue;
      }

      printf("Writing data file: %s\n", reqLatFN);

      uint64_t j;
      for (j = 0; j < requests; j++) {
        fprintf(reqLatFile, "%" PRId64 "\n", wStats[i]->latencies[j]);
      }

      fclose(reqLatFile);

      char execSumFN[256];
      snprintf(execSumFN, sizeof(execSumFN), 
          "%s/%s_client%d-%d_execSummary.dat", 
          outputDir, test, numThreads, threadIndex);
      
      FILE *execSumFile = fopen(execSumFN, "w");
      
      if (execSumFile == NULL) {
        printf("ERROR: Can't open output file %s\n", execSumFN);
        continue;
      }

      printf("Writing data file: %s\n", execSumFN);

      /* Total run time in seconds. */
      fprintf(execSumFile, "totalTime %.2f\n", (float)totalTime / 1000000.0);
      fprintf(execSumFile, "totalOps %" PRId64 "\n", requests);

      fclose(execSumFile);
    }
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
  uint64_t progressUnit = (requests/100) > 0 ? (requests/100) : 1;
  uint64_t testStart = ustime();
  for (i = 0; i < requests; i++) {
    snprintf(keyBuf, 16, "%015d", rand() % keySpaceLength);
    key.data = keyBuf;

    uint64_t reqStart = ustime();
    Object* value = get(context, &key);
    latencies[i] = ustime() - reqStart;
    freeObject(value);

    if (i % progressUnit == 0) {
      printf("Progress: %3d%%\r", i*100/requests);
      fflush(stdout);
    }
  }
  uint64_t testEnd = ustime(); 
  
  wStats->latencies = latencies;

  ramdis_disconnect(context);

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
  uint64_t progressUnit = (requests/100) > 0 ? (requests/100) : 1;
  uint64_t testStart = ustime();
  for (i = 0; i < requests; i++) {
    snprintf(keyBuf, 16, "%015d", rand() % keySpaceLength);
    key.data = keyBuf;

    uint64_t reqStart = ustime();
    set(context, &key, &value);
    latencies[i] = ustime() - reqStart;

    if (i % progressUnit == 0) {
      printf("Progress: %3d%%\r", i*100/requests);
      fflush(stdout);
    }
  }
  uint64_t testEnd = ustime(); 

  wStats->latencies = latencies;

  ramdis_disconnect(context);

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
  uint64_t progressUnit = (requests/100) > 0 ? (requests/100) : 1;
  uint64_t testStart = ustime();
  for (i = 0; i < requests; i++) {
    snprintf(keyBuf, 16, "%015d", rand() % keySpaceLength);
    key.data = keyBuf;

    uint64_t reqStart = ustime();
    incr(context, &key);
    latencies[i] = ustime() - reqStart;

    if (i % progressUnit == 0) {
      printf("Progress: %3d%%\r", i*100/requests);
      fflush(stdout);
    }
  }
  uint64_t testEnd = ustime(); 

  wStats->latencies = latencies;

  ramdis_disconnect(context);

  return wStats;
}

/* Worker thread for executing lpush command. */
void* lpushWorkerThread(void* args) {
  struct WorkerArgs* wArgs = (struct WorkerArgs*)args;
  char* coordinatorLocator = wArgs->coordinatorLocator;
  uint64_t requests = wArgs->requests;
  uint64_t valueSize = wArgs->valueSize;
  uint64_t keySpaceLength = wArgs->keySpaceLength;

  Context* context = ramdis_connect(coordinatorLocator);

  struct WorkerStats* wStats = (struct WorkerStats*)malloc(sizeof(struct
        WorkerStats)); 

  uint64_t* latencies = (uint64_t*)malloc(requests*sizeof(uint64_t));

  /* Value to write for push commands. */
  Object value;
  char valBuf[valueSize];
  value.data = valBuf;
  value.len = valueSize;

  int i;
  Object key;
  char keyBuf[16];
  key.len = sizeof(keyBuf);
  uint64_t progressUnit = (requests/100) > 0 ? (requests/100) : 1;
  uint64_t testStart = ustime();
  for (i = 0; i < requests; i++) {
    snprintf(keyBuf, 16, "%015d", rand() % keySpaceLength);
    key.data = keyBuf;

    uint64_t reqStart = ustime();
    lpush(context, &key, &value);
    latencies[i] = ustime() - reqStart;

    if (i % progressUnit == 0) {
      printf("Progress: %3d%%\r", i*100/requests);
      fflush(stdout);
    }
  }
  uint64_t testEnd = ustime(); 

  wStats->latencies = latencies;

  ramdis_disconnect(context);

  return wStats;
}

/* Worker thread for executing rpush command. */
void* rpushWorkerThread(void* args) {
  struct WorkerArgs* wArgs = (struct WorkerArgs*)args;
  char* coordinatorLocator = wArgs->coordinatorLocator;
  uint64_t requests = wArgs->requests;
  uint64_t valueSize = wArgs->valueSize;
  uint64_t keySpaceLength = wArgs->keySpaceLength;

  Context* context = ramdis_connect(coordinatorLocator);

  struct WorkerStats* wStats = (struct WorkerStats*)malloc(sizeof(struct
        WorkerStats)); 

  uint64_t* latencies = (uint64_t*)malloc(requests*sizeof(uint64_t));

  /* Value to write for push commands. */
  Object value;
  char valBuf[valueSize];
  value.data = valBuf;
  value.len = valueSize;

  int i;
  Object key;
  char keyBuf[16];
  key.len = sizeof(keyBuf);
  uint64_t progressUnit = (requests/100) > 0 ? (requests/100) : 1;
  uint64_t testStart = ustime();
  for (i = 0; i < requests; i++) {
    snprintf(keyBuf, 16, "%015d", rand() % keySpaceLength);
    key.data = keyBuf;

    uint64_t reqStart = ustime();
    rpush(context, &key, &value);
    latencies[i] = ustime() - reqStart;

    if (i % progressUnit == 0) {
      printf("Progress: %3d%%\r", i*100/requests);
      fflush(stdout);
    }
  }
  uint64_t testEnd = ustime(); 

  wStats->latencies = latencies;

  ramdis_disconnect(context);

  return wStats;
}

/* Worker thread for executing lpop command. */
void* lpopWorkerThread(void* args) {
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
  uint64_t progressUnit = (requests/100) > 0 ? (requests/100) : 1;
  uint64_t testStart = ustime();
  for (i = 0; i < requests; i++) {
    snprintf(keyBuf, 16, "%015d", rand() % keySpaceLength);
    key.data = keyBuf;

    uint64_t reqStart = ustime();
    lpop(context, &key);
    latencies[i] = ustime() - reqStart;

    if (i % progressUnit == 0) {
      printf("Progress: %3d%%\r", i*100/requests);
      fflush(stdout);
    }
  }
  uint64_t testEnd = ustime(); 

  wStats->latencies = latencies;

  ramdis_disconnect(context);

  return wStats;
}

/* Worker thread for executing rpop command. */
void* rpopWorkerThread(void* args) {
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
  uint64_t progressUnit = (requests/100) > 0 ? (requests/100) : 1;
  uint64_t testStart = ustime();
  for (i = 0; i < requests; i++) {
    snprintf(keyBuf, 16, "%015d", rand() % keySpaceLength);
    key.data = keyBuf;

    uint64_t reqStart = ustime();
    rpop(context, &key);
    latencies[i] = ustime() - reqStart;

    if (i % progressUnit == 0) {
      printf("Progress: %3d%%\r", i*100/requests);
      fflush(stdout);
    }
  }
  uint64_t testEnd = ustime(); 

  wStats->latencies = latencies;

  ramdis_disconnect(context);

  return wStats;
}

/* Worker thread for executing lrange command. */
void* lrangeWorkerThread(void* args) {
  struct WorkerArgs* wArgs = (struct WorkerArgs*)args;
  char* coordinatorLocator = wArgs->coordinatorLocator;
  uint64_t requests = wArgs->requests;
  uint64_t valueSize = wArgs->valueSize;
  uint64_t lrangeSize = wArgs->lrangeSize;
  uint64_t keySpaceLength = wArgs->keySpaceLength;

  Context* context = ramdis_connect(coordinatorLocator);

  struct WorkerStats* wStats = (struct WorkerStats*)malloc(sizeof(struct
        WorkerStats)); 

  uint64_t* latencies = (uint64_t*)malloc(requests*sizeof(uint64_t));

  int i;
  Object key;
  char keyBuf[16];
  key.len = sizeof(keyBuf);
  uint64_t progressUnit = (requests/100) > 0 ? (requests/100) : 1;
  uint64_t testStart = ustime();
  for (i = 0; i < requests; i++) {
    snprintf(keyBuf, 16, "%015d", rand() % keySpaceLength);
    key.data = keyBuf;

    uint64_t reqStart = ustime();
    ObjectArray* objArray = lrange(context, &key, 0, lrangeSize);
    latencies[i] = ustime() - reqStart;
    freeObjectArray(objArray);

    if (i % progressUnit == 0) {
      printf("Progress: %3d%%\r", i*100/requests);
      fflush(stdout);
    }
  }
  uint64_t testEnd = ustime(); 

  wStats->latencies = latencies;

  ramdis_disconnect(context);

  return wStats;
}

int main(int argc, char* argv[]) {
  char* coordinatorLocator;
  uint64_t clientIndex = 0;
  uint64_t numClients = 1;
  uint64_t clientThreads = 1;
  uint64_t requests = 100000;
  uint64_t valueSize = 3;
  uint64_t lrangeSize = 100;
  uint64_t keySpaceLength = 1;
  char* tests = "all";
  char* outputDir = NULL;

  // Parse command line options.
  uint64_t i;
  for (i = 1; i < argc;) {
    if (strcmp(argv[i], "-C") == 0) {
      coordinatorLocator = argv[i+1];
      i+=2;
    } else if (strcmp(argv[i], "--clientIndex") == 0) {
      clientIndex = strtoul(argv[i+1], NULL, 10);
      i+=2;
    } else if (strcmp(argv[i], "--numClients") == 0) {
      numClients = strtoul(argv[i+1], NULL, 10);
      i+=2;
    } else if (strcmp(argv[i], "--threads") == 0) {
      clientThreads = strtoul(argv[i+1], NULL, 10);
      i+=2;
    } else if (strcmp(argv[i], "-n") == 0) {
      requests = strtoul(argv[i+1], NULL, 10);
      i+=2;
    } else if (strcmp(argv[i], "-d") == 0) {
      valueSize = strtoul(argv[i+1], NULL, 10);
      i+=2;
    } else if (strcmp(argv[i], "-l") == 0) {
      lrangeSize = strtoul(argv[i+1], NULL, 10);
      i+=2;
    } else if (strcmp(argv[i], "-r") == 0) {
      keySpaceLength = strtoul(argv[i+1], NULL, 10);
      i+=2;
    } else if (strcmp(argv[i], "-t") == 0) {
      tests = argv[i+1];
      i+=2;
    } else if (strcmp(argv[i], "--outputDir") == 0) {
      outputDir = argv[i+1];
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
  wArgs.lrangeSize = lrangeSize;
  wArgs.keySpaceLength = keySpaceLength;

  char* test;
  test = strtok(tests, ",");
  while (test != NULL ) {
    printf("========== %s ==========\n", test);

    void* (*workerThreadFuncPtr)();  

    if (strcmp(test, "get") == 0) {
      workerThreadFuncPtr = getWorkerThread;

      /* Do pre-workload setup. */
      printf("Doing pre-workload setup... ");
      fflush(stdout);

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

      printf("Done\n");
    } else if (strcmp(test, "set") == 0) {
      workerThreadFuncPtr = setWorkerThread;
    } else if (strcmp(test, "incr") == 0) {
      workerThreadFuncPtr = incrWorkerThread;

      /* Do pre-workload setup. */
      printf("Doing pre-workload setup... ");
      fflush(stdout);

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

      printf("Done\n");
    } else if (strcmp(test, "lpush") == 0) {
      workerThreadFuncPtr = lpushWorkerThread;
    } else if (strcmp(test, "rpush") == 0) {
      workerThreadFuncPtr = rpushWorkerThread;
    } else if (strcmp(test, "lpop") == 0) {
      workerThreadFuncPtr = lpopWorkerThread;

      /* Do pre-workload setup. */
      printf("Doing pre-workload setup... ");
      fflush(stdout);

      Object value;
      char valBuf[valueSize];
      value.data = valBuf;
      value.len = valueSize;

      Object key;
      char keyBuf[16];
      key.len = 16;
      // Pre-push the list elements to perform the pops.
      for (i = 0; i < keySpaceLength; i++) {
        snprintf(keyBuf, 16, "%015" PRId64, i);
        key.data = keyBuf;
        
        uint64_t j;
        for (j = 0; j < (requests / keySpaceLength); j++) {
          lpush(context, &key, &value);
        }
      }

      printf("Done\n");
    } else if (strcmp(test, "rpop") == 0) {
      workerThreadFuncPtr = rpopWorkerThread;

      /* Do pre-workload setup. */
      printf("Doing pre-workload setup... ");
      fflush(stdout);

      Object value;
      char valBuf[valueSize];
      value.data = valBuf;
      value.len = valueSize;

      Object key;
      char keyBuf[16];
      key.len = 16;
      // Pre-push the list elements to perform the pops.
      for (i = 0; i < keySpaceLength; i++) {
        snprintf(keyBuf, 16, "%015" PRId64, i);
        key.data = keyBuf;
        
        uint64_t j;
        for (j = 0; j < (requests / keySpaceLength); j++) {
          lpush(context, &key, &value);
        }
      }

      printf("Done\n");
    } else if (strcmp(test, "sadd") == 0) {
      printf("Test not yet implemented: %s\n", test);
      return -1;
    } else if (strcmp(test, "spop") == 0) {
      printf("Test not yet implemented: %s\n", test);
      return -1;
    } else if (strcmp(test, "lrange") == 0) {
      workerThreadFuncPtr = lrangeWorkerThread;

      /* Do pre-workload setup. */
      printf("Doing pre-workload setup... ");
      fflush(stdout);

      Object value;
      char valBuf[valueSize];
      value.data = valBuf;
      value.len = valueSize;

      Object key;
      char keyBuf[16];
      key.len = 16;
      // Pre-push the list elements to perform the pops.
      for (i = 0; i < keySpaceLength; i++) {
        snprintf(keyBuf, 16, "%015" PRId64, i);
        key.data = keyBuf;
        
        uint64_t j;
        for (j = 0; j < 10000; j++) {
          lpush(context, &key, &value);
        }
      }

      printf("Done\n");
    } else if (strcmp(test, "mset") == 0) {
    } else {
      printf("Unrecognized test: %s\n", test);
      return -1;
    }

    pthread_t threads[clientThreads];
    uint64_t start = ustime();
    for (i = 0; i < clientThreads; i++) {
      pthread_create(&threads[i], NULL, workerThreadFuncPtr, &wArgs);
    }
    
    struct WorkerStats* wStats[clientThreads];
    for (i = 0; i < clientThreads; i++) {
      pthread_join(threads[i], (void*)&wStats[i]);
    }
    uint64_t end = ustime();

    reportStats(test, end - start, wStats, clientIndex, numClients, 
        clientThreads, requests, outputDir);

    for (i = 0; i < clientThreads; i++) {
      freeWorkerStats(wStats[i]);
    }

    test = strtok(NULL, ",");
  }

  ramdis_disconnect(context);

  return 0;
}
