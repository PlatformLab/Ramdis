#ifndef __RAMDIS_SERVER_H
#define __RAMDIS_SERVER_H

#include <string>
#include <vector>

#include "sds.h"

#define CONFIG_DEFAULT_TCP_BACKLOG       511     /* TCP listen backlog */
#define PROTO_IOBUF_LEN         (1024*16)  /* Generic I/O buffer size */
#define NET_IP_STR_LEN 46 /* INET6_ADDRSTRLEN is 46, but we need to be sure */

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

/* Client request types */
#define PROTO_REQ_INLINE 1
#define PROTO_REQ_MULTIBULK 2

/* Protocol and I/O related defines */
#define PROTO_INLINE_MAX_SIZE   (1024*64) /* Max size of inline reads */
#define PROTO_MBULK_BIG_ARG     (1024*32)

/* Error codes */
#define C_OK                    0
#define C_ERR                   -1

#define serverPanic(_e) _serverPanic(#_e,__FILE__,__LINE__),_exit(1)

/* Used to store client data coming in over the socket and parsing state as the
 * buffer is incrementally parsed for commands. */
struct clientBuffer {
  clientBuffer(int fd) : 
    fd(fd),
    querybuf(sdsempty()),
    argv(),
    reqtype(0),
    multibulklen(0),
    bulklen(-1) {}
  // TODO: write destructor
  int fd;
  sds querybuf;           /* Buffer we use to accumulate client queries. */
  std::vector<std::string> argv; /* Arguments of current command. */
  int reqtype;
  int multibulklen;
  long bulklen;
};

#endif // __RAMDIS_SERVER_H
