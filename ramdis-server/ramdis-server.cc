#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdarg.h>
#include <arpa/inet.h>
#include <thread>
#include <queue>

#include "ramdis-server.h"
#include "RamCloud.h"
#include "docopt.h"

// Queue elements are (file descriptor, request arguements) 
std::queue<std::pair<int, std::vector<std::string>>> requestQ;
std::mutex requestQMutex;
// Queue elements are (file descriptor, response string)
std::queue<std::pair<int, std::string>> responseQ;
std::mutex responseQMutex;

void serverLog(int level, const char *fmt, ...) {
  va_list ap;
  char msg[LOG_MAX_LEN];
  char pmsg[LOG_MAX_LEN];

  if ((level&0xff) > VERBOSITY) return;

  va_start(ap, fmt);
  vsnprintf(msg, sizeof(msg), fmt, ap);
  va_end(ap);

  switch(level) {
    case LL_FATAL:
      snprintf(pmsg, sizeof(pmsg), "FATAL: %s\n", msg);
      break;
    case LL_ERROR:
      snprintf(pmsg, sizeof(pmsg), "ERROR: %s\n", msg);
      break;
    case LL_WARN:
      snprintf(pmsg, sizeof(pmsg), "WARN: %s\n", msg);
      break;
    case LL_INFO:
      snprintf(pmsg, sizeof(pmsg), "INFO: %s\n", msg);
      break;
    case LL_DEBUG:
      snprintf(pmsg, sizeof(pmsg), "DEBUG: %s\n", msg);
      break;
    case LL_TRACE:
      snprintf(pmsg, sizeof(pmsg), "TRACE: %s\n", msg);
      break;
    default:
      break;
  }

  printf(pmsg);
}

/* Convert a string into a long long. Returns 1 if the string could be parsed
 * into a (non-overflowing) long long, 0 otherwise. The value will be set to
 * the parsed value when appropriate. */
int string2ll(const char *s, size_t slen, long long *value) {
    const char *p = s;
    size_t plen = 0;
    int negative = 0;
    unsigned long long v;

    if (plen == slen)
        return 0;

    /* Special case: first and only digit is 0. */
    if (slen == 1 && p[0] == '0') {
        if (value != NULL) *value = 0;
        return 1;
    }

    if (p[0] == '-') {
        negative = 1;
        p++; plen++;

        /* Abort on only a negative sign. */
        if (plen == slen)
            return 0;
    }

    /* First digit should be 1-9, otherwise the string should just be 0. */
    if (p[0] >= '1' && p[0] <= '9') {
        v = p[0]-'0';
        p++; plen++;
    } else if (p[0] == '0' && slen == 1) {
        *value = 0;
        return 1;
    } else {
        return 0;
    }

    while (plen < slen && p[0] >= '0' && p[0] <= '9') {
        if (v > (ULLONG_MAX / 10)) /* Overflow. */
            return 0;
        v *= 10;

        if (v > (ULLONG_MAX - (p[0]-'0'))) /* Overflow. */
            return 0;
        v += p[0]-'0';

        p++; plen++;
    }

    /* Return if not all bytes were used. */
    if (plen < slen)
        return 0;

    if (negative) {
        if (v > ((unsigned long long)(-(LLONG_MIN+1))+1)) /* Overflow. */
            return 0;
        if (value != NULL) *value = -v;
    } else {
        if (v > LLONG_MAX) /* Overflow. */
            return 0;
        if (value != NULL) *value = v;
    }
    return 1;
}

int processInlineBuffer(clientBuffer *c) {
//
//    char *newline;
//    int argc, j;
//    sds *argv, aux;
//    size_t querylen;
//
//    char *reqBuf = cBuf.data();
//
//    /* Search for end of line */
//    newline = strchr(reqBuf,'\n');
//
//    /* Nothing to do without a \r\n */
//    if (newline == NULL) {
//      return 0;
//    }
//
//    /* Handle the \r\n case. */
//    if (newline && newline != reqBuf && *(newline-1) == '\r')
//      newline--;
//
//    /* Split the input buffer up to the \r\n */
//    querylen = newline-(reqBuf);
//    aux = sdsnewlen(reqBuf,querylen);
//    argv = sdssplitargs(aux,&argc);
//    sdsfree(aux);
//    if (argv == NULL) {
//        addReplyError(c,"Protocol error: unbalanced quotes in request");
//        setProtocolError(c,0);
//        return C_ERR;
//    }
//
//    /* Newline from slaves can be used to refresh the last ACK time.
//     * This is useful for a slave to ping back while loading a big
//     * RDB file. */
//    if (querylen == 0 && c->flags & CLIENT_SLAVE)
//        c->repl_ack_time = server.unixtime;
//
//    /* Leave data after the first line of the query in the buffer */
//    sdsrange(reqBuf,querylen+2,-1);
//
//    /* Setup argv array on client structure */
//    if (argc) {
//        if (c->argv) zfree(c->argv);
//        c->argv = zmalloc(sizeof(robj*)*argc);
//    }
//
//    /* Create redis objects for all arguments. */
//    for (c->argc = 0, j = 0; j < argc; j++) {
//        if (sdslen(argv[j])) {
//            c->argv[c->argc] = createObject(OBJ_STRING,argv[j]);
//            c->argc++;
//        } else {
//            sdsfree(argv[j]);
//        }
//    }
//    zfree(argv);
//    return C_OK;
  return 0; // not implemented for now.
}

/* Parse client buffers for request arguments. Partial results are stored in
 * c->agv. When a full request is parsed from the buffer, then it is enqueued
 * in the request queue for later execution. */
int processMultibulkBuffer(clientBuffer *c) {
    char *newline = NULL;
    int pos = 0, ok;
    long long ll;

    if (c->multibulklen == 0) {
        /* Multi bulk length cannot be read without a \r\n */
        newline = strchr(c->querybuf,'\r');
        if (newline == NULL) {
            if (sdslen(c->querybuf) > PROTO_INLINE_MAX_SIZE) {
                serverLog(LL_ERROR, 
                    "Protocol error: too big mbulk count string");
                exit(1);
            }
            return C_ERR;
        }

        /* Buffer should also contain \n */
        if (newline-(c->querybuf) > ((signed)sdslen(c->querybuf)-2))
            return C_ERR;

        /* We know for sure there is a whole line since newline != NULL,
         * so go ahead and find out the multi bulk length. */
        ok = string2ll(c->querybuf+1,newline-(c->querybuf+1),&ll);
        if (!ok || ll > 1024*1024) {
            serverLog(LL_ERROR, 
                    "Protocol error: invalid multibulk length");
            exit(1);
        }

        pos = (newline-c->querybuf)+2;
        if (ll <= 0) {
            sdsrange(c->querybuf,pos,-1);
            return C_OK;
        }

        c->multibulklen = ll;

        /* Setup argv array on client structure */
        if (c->argv.size() > 0) {
            c->argv.clear();
            c->argv.reserve(ll);
        }
    }

    while(c->multibulklen) {
        /* Read bulk length if unknown */
        if (c->bulklen == -1) {
            newline = strchr(c->querybuf+pos,'\r');
            if (newline == NULL) {
                if (sdslen(c->querybuf) > PROTO_INLINE_MAX_SIZE) {
                    serverLog(LL_ERROR, 
                        "Protocol error: too big bulk count string");
                    exit(1);
                }
                break;
            }

            /* Buffer should also contain \n */
            if (newline-(c->querybuf) > ((signed)sdslen(c->querybuf)-2))
                break;

            if (c->querybuf[pos] != '$') {
                serverLog(LL_ERROR, 
                    "Protocol error: expected '$', got '%c'",
                    c->querybuf[pos]);
                exit(1);
            }

            ok = string2ll(c->querybuf+pos+1,newline-(c->querybuf+pos+1),&ll);
            if (!ok || ll < 0 || ll > 512*1024*1024) {
                serverLog(LL_ERROR, 
                    "Protocol error: invalid bulk length");
                exit(1);
            }

            pos += newline-(c->querybuf+pos)+2;
            c->bulklen = ll;
        }

        /* Read bulk argument */
        if (sdslen(c->querybuf)-pos < (unsigned)(c->bulklen+2)) {
            /* Not enough data (+2 == trailing \r\n) */
            break;
        } else {
            c->argv.emplace_back(c->querybuf + pos, c->bulklen);
            pos += c->bulklen+2;

            c->bulklen = -1;
            c->multibulklen--;
        }
    }

    /* Trim to pos */
    if (pos) sdsrange(c->querybuf,pos,-1);

    /* We're done when c->multibulk == 0 */
    if (c->multibulklen == 0) {
        /* Queue the command in the request queue. */
        std::lock_guard<std::mutex> lock(requestQMutex);
        requestQ.emplace(c->fd, c->argv);
        c->argv.clear();
        return C_OK;
    }

    /* Still not ready to process the command */
    return C_ERR;
}

void processInputBuffer(clientBuffer *c) {
  /* Keep processing while there is something in the input buffer */
  while(sdslen(c->querybuf)) {
    /* Determine request type when unknown. */
    if (!c->reqtype) {
      if (c->querybuf[0] == '*') {
        c->reqtype = PROTO_REQ_MULTIBULK;
      } else {
        c->reqtype = PROTO_REQ_INLINE;
      }
    }

    if (c->reqtype == PROTO_REQ_INLINE) {
      if (processInlineBuffer(c) != C_OK) break;
    } else if (c->reqtype == PROTO_REQ_MULTIBULK) {
      if (processMultibulkBuffer(c) != C_OK) break;
    } else {
      serverLog(LL_ERROR, "Unknown request type");
      exit(1);
    }
  }
}

void requestExecutor(int threadNumber) {
  serverLog(LL_DEBUG, "Thread %d started.", threadNumber);
  while (true) {
    int cfd;
    std::vector<std::string> argv;
    while (true) {
      if (requestQ.size() == 0)
        continue;

      std::lock_guard<std::mutex> lock(requestQMutex);
      if (requestQ.size() > 0) {
        std::pair<int, std::vector<std::string>> entry = requestQ.front();
        cfd = entry.first;
        argv = entry.second;
        requestQ.pop();
        break;
      }
    }

    /* Do processing here. */

    {
      std::lock_guard<std::mutex> lock(responseQMutex);
      responseQ.emplace(cfd, "$5\r\ndone!\r\n");
    }
  }
}

static const char USAGE[] =
R"(Ramdis Server.

    Usage:
      ramdis-server [options] RAMCLOUDCOORDLOC

    Arguments:
      RAMCLOUDCOORDLOC  RAMCloud coordinator locator string.

    Options:
      --host=HOST  Host IPv4 address to use [default: 127.0.0.1] 
      --port=PORT  Port number to use [default: 6379]
      --threads=N  Number of request executor threads to run in parallel
      [default: 4]

)";

int main(int argc, char *argv[]) {

  /* Parse command line options. */

  std::map<std::string, docopt::value> args = docopt::docopt(USAGE, 
      {argv + 1, argv + argc},
      true,               // show help if requested
      "Ramdis Server 0.0");      // version string

  for (auto const& arg : args) {
    std::cout << arg.first << ": " << arg.second << std::endl;
  }

  serverLog(LL_INFO, "Server verbosity set to %d", VERBOSITY);

  /* Open a listening socket for the server. */

  struct addrinfo hints;
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_protocol = IPPROTO_TCP;
  hints.ai_flags = AI_PASSIVE | AI_NUMERICSERV;

  struct addrinfo *servinfo = NULL;
  int rv = getaddrinfo(NULL, args["--port"].asString().c_str(), &hints, 
      &servinfo);

  if (rv != 0) {
    serverLog(LL_ERROR, "%s", gai_strerror(rv));
    return -1;
  }

  struct addrinfo *p;
  int sfd;
  for (p = servinfo; p != NULL; p = p->ai_next) {
    if ((sfd = socket(p->ai_family, p->ai_socktype, p->ai_protocol)) == -1)
      continue;

    int yes = 1;
    if (setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) == -1) {
      serverLog(LL_ERROR, "setsockopt SO_REUSEADDR: %s", strerror(errno));
      close(sfd);
      freeaddrinfo(servinfo);
      return -1;
    }

    if (bind(sfd, p->ai_addr, p->ai_addrlen) == -1) {
      serverLog(LL_ERROR, "Bind error: %s", strerror(errno));
      close(sfd);
      freeaddrinfo(servinfo);
      return -1;
    }

    if (listen(sfd, CONFIG_DEFAULT_TCP_BACKLOG) == -1) {
      serverLog(LL_ERROR, "Listen error: %s", strerror(errno));
      close(sfd);
      freeaddrinfo(servinfo);
      return -1;
    }

    serverLog(LL_INFO, "Listening on %s:%s", 
        args["--host"].asString().c_str(), 
        args["--port"].asString().c_str());

    break;
  }

  if (p == NULL) {
    serverLog(LL_ERROR, "Unable to bind socket: %s", strerror(errno));
    freeaddrinfo(servinfo);
    return -1;
  }

  freeaddrinfo(servinfo);

  /* Set the listening socket to non-blocking. This is so that calls to
   * accept() will return immediately if there are no new connections. */

  int flags;
  if ((flags = fcntl(sfd, F_GETFL)) == -1) {
    serverLog(LL_ERROR, "fcntl(F_GETFL): %s", strerror(errno));
    close(sfd);
    return -1;
  }

  flags |= O_NONBLOCK;

  if (fcntl(sfd, F_SETFL, flags) == -1) {
    serverLog(LL_ERROR, "fcntl(F_SETFL,O_NONBLOCK): %s", strerror(errno));
    close(sfd);
    return -1;
  }

  /* Start request executor threads. */
  std::vector<std::thread> threads;
  for (int i = 0; i < (int)args["--threads"].asLong(); i++) {
    threads.emplace_back(requestExecutor, i + 1);
  }

  /* In a loop:
   * 1) accept new client connections
   * 2) read new data on client connections and buffer it
   * 3) parse client data buffers for new requests
   * 4) enqueue new requests on request queue
   * 5) check response queue for new responses
   * 6) send responses to clients
   */

  // Client file descriptor -> buffer of data read from socket
  std::map<int, clientBuffer> clientBuffers;

  fd_set cfds, _cfds;
  FD_ZERO(&cfds);

  while(true) {
    int cfd;
    struct sockaddr_storage sa;
    socklen_t salen = sizeof(sa);
    
    cfd = accept(sfd, (struct sockaddr*)&sa, &salen);
    
    if (cfd == -1) {
      if (errno != EAGAIN && errno != EWOULDBLOCK) {
        serverLog(LL_ERROR, "Accept error: %s", strerror(errno));
        for (auto const& entry : clientBuffers) { 
          close(entry.first);
        }
        close(sfd);
        return -1;
      }
    } else {
      struct sockaddr_in *s = (struct sockaddr_in*)&sa;
      int port = ntohs(s->sin_port);
      char ip[NET_IP_STR_LEN];
      inet_ntop(AF_INET, (void*)&(s->sin_addr), ip, sizeof(ip));
     
      serverLog(LL_INFO, "Received client connection: %s:%d", ip, port);

      clientBuffers.emplace(cfd, cfd);
      FD_SET(cfd, &cfds);
    }

    memcpy(&_cfds, &cfds, sizeof(fd_set));

    struct timeval tv;
    tv.tv_sec = 0;
    tv.tv_usec = 0;
    int retval = select(FD_SETSIZE, &_cfds, NULL, NULL, &tv);

    if (retval < 0) {
      serverLog(LL_ERROR, "Select error: %s", strerror(errno));
      for (auto const& entry : clientBuffers) { 
        close(entry.first);
      }
      close(sfd);
      return -1;
    }

    /* Buffer up any unread client data. */
    if (retval > 0) {
      for (auto it = clientBuffers.begin(); it != clientBuffers.end(); ) {
        int cfd = it->first;
        clientBuffer *cBuf = &it->second;
        if (FD_ISSET(cfd, &_cfds)) {
          size_t qblen = sdslen(cBuf->querybuf);
          cBuf->querybuf = sdsMakeRoomFor(cBuf->querybuf, PROTO_IOBUF_LEN);
          int nbytes = read(cfd, cBuf->querybuf + qblen, PROTO_IOBUF_LEN);
          if (nbytes == -1) {
            if (errno == EAGAIN) {
              /* That's fine, try again later. */
              ++it;
              continue;
            } else {
              /* Got an error. Close the client. */
              serverLog(LL_ERROR, "Read error: %s. Closing client.", 
                  strerror(errno));
              it = clientBuffers.erase(it);
              close(cfd);
              FD_CLR(cfd, &cfds);
              continue;
            }
          } else if (nbytes == 0) {
            /* Client closed connection. */
            it = clientBuffers.erase(it);
            FD_CLR(cfd, &cfds);
            continue;
          } else {
            /* Houston, we have data! */
            sdsIncrLen(cBuf->querybuf, nbytes);
            processInputBuffer(cBuf);
            ++it;
            continue;
          }  
        } else {
          /* This client has no data. */
          ++it;
          continue;
        }
      }
    } 

    while (responseQ.size() > 0) {
      int cfd;
      std::string response;
      {
        std::lock_guard<std::mutex> lock(responseQMutex);
        std::pair<int, std::string> entry = responseQ.front();
        cfd = entry.first;
        response = entry.second;
        responseQ.pop();
      }
     
      if (clientBuffers.find(cfd) != clientBuffers.end()) { 
        int bufpos = 0;
        const char* buf = response.c_str();
        int buflen = response.length();
        while (bufpos != buflen) {
          int nwritten = write(cfd, buf + bufpos, buflen - bufpos);
          if (nwritten == -1) {
            if (errno == EAGAIN) {
              /* Try again. */
              continue;
            } else {
              /* Something bad happened. */
              clientBuffers.erase(cfd);
              close(cfd);
              FD_CLR(cfd, &cfds);
              break;
            }
          }
          bufpos += nwritten;
        }
      } else {
        /* Response is for a client that we already closed the connection for.
         * */
        continue;
      }
    }
  }

  return 0;
}
