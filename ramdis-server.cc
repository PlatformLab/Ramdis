#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdarg.h>
#include <arpa/inet.h>

#include <queue>

#include "ramdis-server.h"
#include "RamCloud.h"
#include "docopt.h"

int processInlineBuffer(std::vector<char> &cBuf, 
    std::vector<std::string> &argv) {
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

/* Return the number of requests parsed from the buffer. */
int processMultibulkBuffer(std::vector<char> &cBuf, 
    std::vector<std::string> &argv) {

    char *newline = NULL;
    int pos = 0, ok;
    long long ll;
    long long multibulklen;
    long long bulklen;

    char *reqBuf = cBuf.data();

    /* Multi bulk length cannot be read without a \r\n */
    newline = strchr(reqBuf, '\r');
    if (newline == NULL) {
      return 0;
    }

    /* Buffer should also contain \n */
    if (newline - reqBuf > cBuf.size() - 2) 
      return 0;

    /* We know for sure there is a whole line since newline != NULL,
     * so go ahead and find out the multi bulk length. */
    int lenLen = newline - (reqBuf + 1);
    char len[lenLen + 1];
    memcpy(len, reqBuf + 1, lenLen);
    len[lenLen] = '\0';
    ll = atoll(len);

    pos = (newline - reqBuf) + 2;
    if (ll <= 0) {
      cBuf.erase(cBuf.begin(), cBuf.begin() + pos);
      return 1;
    }

    multibulklen = ll;

    while(multibulklen) {
      newline = strchr(reqBuf + pos, '\r');
      if (newline == NULL) {
        return 0;
      }

      /* Buffer should also contain \n */
      if (newline - reqBuf > cBuf.size() - 2) {
        return 0;
      }

      lenLen = newline - (reqBuf + pos + 1);
      char len[lenLen + 1];
      memcpy(len, reqBuf + pos + 1, lenLen);
      len[lenLen] = '\0';
      ll = atoll(len);

      pos += newline - (reqBuf + pos) + 2;

      bulklen = ll;

      /* Read bulk argument */
      if (cBuf.size() - pos < bulklen + 2) {
        /* Not enough data (+2 == trailing \r\n) */
        return 0;
      }

      argv.emplace_back(reqBuf + pos, bulklen);
      pos += bulklen + 2;

      multibulklen--;
    }

    /* Trim to pos */
    if (pos) {
      cBuf.erase(cBuf.begin(), cBuf.begin() + pos);
      return 1;
    } else {
      return 0;
    }
}


void processInputBuffer(std::vector<char> &cBuf, 
    std::vector<std::string> &argv) {
  /* Keep processing while there is something in the input buffer */
  while(cBuf.size()) {
    if (cBuf[0] == '*') {
      if (processMultibulkBuffer(cBuf, argv) == 0) break;
    } else {
      // not implemented for now.
      if (processInlineBuffer(cBuf, argv) == 0) break;
    }
  }
}

void serverLog(int level, const char *fmt, ...) {
  va_list ap;
  char msg[LOG_MAX_LEN];

  if ((level&0xff) > VERBOSITY) return;

  va_start(ap, fmt);
  vsnprintf(msg, sizeof(msg), fmt, ap);
  va_end(ap);

  printf(msg);
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
    printf("ERROR: %s", gai_strerror(rv));
    return -1;
  }

  struct addrinfo *p;
  int sfd;
  for (p = servinfo; p != NULL; p = p->ai_next) {
    if ((sfd = socket(p->ai_family, p->ai_socktype, p->ai_protocol)) == -1)
      continue;

    int yes = 1;
    if (setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) == -1) {
      printf("ERROR: setsockopt SO_REUSEADDR: %s\n", strerror(errno));
      close(sfd);
      freeaddrinfo(servinfo);
      return -1;
    }

    if (bind(sfd, p->ai_addr, p->ai_addrlen) == -1) {
      printf("ERROR: bind: %s\n", strerror(errno));
      close(sfd);
      freeaddrinfo(servinfo);
      return -1;
    }

    if (listen(sfd, CONFIG_DEFAULT_TCP_BACKLOG) == -1) {
      printf("ERROR: listen: %s\n", strerror(errno));
      close(sfd);
      freeaddrinfo(servinfo);
      return -1;
    }

    serverLog(LL_INFO, "Listening on %s:%s\n", 
        args["--host"].asString().c_str(), 
        args["--port"].asString().c_str());

    break;
  }

  if (p == NULL) {
    printf("ERROR: unable to bind socket, errno: %d\n", errno);
    freeaddrinfo(servinfo);
    return -1;
  }

  freeaddrinfo(servinfo);

  /* Set the listening socket to non-blocking. This is so that calls to
   * accept() will return immediately if there are no new connections. */

  int flags;
  if ((flags = fcntl(sfd, F_GETFL)) == -1) {
    printf("ERROR: fcntl(F_GETFL): %s\n", strerror(errno));
    close(sfd);
    return -1;
  }

  flags |= O_NONBLOCK;

  if (fcntl(sfd, F_SETFL, flags) == -1) {
    printf("ERROR: fcntl(F_SETFL,O_NONBLOCK): %s\n", strerror(errno));
    close(sfd);
    return -1;
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
  std::map<int, std::vector<char>> clientBuffers;
  // Queue elements are (file descriptor, request arguements) 
  std::queue<std::pair<int, std::vector<std::string>>> requestQ;
  std::mutex requestQMutex;
  // Queue elements are (file descriptor, response string)
  std::queue<std::pair<int, std::string>> responseQ;
  std::mutex responseQMutex;

  fd_set cfds, _cfds;
  FD_ZERO(&cfds);

  while(true) {
    int cfd;
    struct sockaddr_storage sa;
    socklen_t salen = sizeof(sa);
    
    cfd = accept(sfd, (struct sockaddr*)&sa, &salen);
    
    if (cfd == -1) {
      if (errno != EAGAIN && errno != EWOULDBLOCK) {
        printf("ERROR: accept: %s\n", strerror(errno));
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
     
      serverLog(LL_INFO, "Received client connection: %s:%d\n", ip, port);

      clientBuffers[cfd] = {};
      FD_SET(cfd, &cfds);
    }

    memcpy(&_cfds, &cfds, sizeof(fd_set));

    struct timeval tv;
    tv.tv_sec = 0;
    tv.tv_usec = 0;
    int retval = select(FD_SETSIZE, &_cfds, NULL, NULL, &tv);

    if (retval < 0) {
      printf("ERROR: select: %s\n", strerror(errno));
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
        std::vector<char> *cBuf = &it->second;
        if (FD_ISSET(cfd, &_cfds)) {
          char buf[PROTO_IOBUF_LEN];
          int nbytes = read(cfd, buf, PROTO_IOBUF_LEN);
          if (nbytes == -1) {
            if (errno != EAGAIN) {
              serverLog(LL_ERROR, "read: %s\n", strerror(errno));
              // TODO: clean up properly...
              close(sfd);
              return -1;
            }
          } else if (nbytes == 0) {
            it = clientBuffers.erase(it);
            FD_CLR(cfd, &cfds);
            continue;
          }

          cBuf->insert(cBuf->end(), buf, buf + nbytes);
          buf[nbytes] = '\0';
          printf("Got %d:%s\n", cfd, buf);

          // Check client buffer for fully buffered request(s).
          // Want to model code from Redis...
        }

        ++it;
      }
    } 


    
  }

  return 0;
}
