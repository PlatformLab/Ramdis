/*
 * Copyright (c) 2009-2011, Salvatore Sanfilippo <antirez at gmail dot com>
 * Copyright (c) 2010-2011, Pieter Noordhuis <pcnoordhuis at gmail dot com>
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <assert.h>
#include <errno.h>
#include <ctype.h>

#include "ramdis.h"
#include "RamCloud.h"
#include "docopt.h"

static redisReply *createReplyObject(int type);
static void *createStringObject(const redisReadTask *task, char *str, size_t len);
static void *createArrayObject(const redisReadTask *task, int elements);
static void *createIntegerObject(const redisReadTask *task, long long value);
static void *createNilObject(const redisReadTask *task);

/* Default set of functions to build the reply. Keep in mind that such a
 * function returning NULL is interpreted as OOM. */
static redisReplyObjectFunctions defaultFunctions = {
    createStringObject,
    createArrayObject,
    createIntegerObject,
    createNilObject,
    freeReplyObject
};

/* Create a reply object */
static redisReply *createReplyObject(int type) {
    return NULL;
}

/* Free a reply object */
void freeReplyObject(void *reply) {
}

static void *createStringObject(const redisReadTask *task, char *str, size_t len) {
    return NULL;
}

static void *createArrayObject(const redisReadTask *task, int elements) {
    return NULL;
}

static void *createIntegerObject(const redisReadTask *task, long long value) {
    return NULL;
}

static void *createNilObject(const redisReadTask *task) {
    return NULL;
}

static void __redisReaderSetError(redisReader *r, int type, const char *str) {
}

static size_t chrtos(char *buf, size_t size, char byte) {
    return -1;
}

static void __redisReaderSetErrorProtocolByte(redisReader *r, char byte) {
}

static void __redisReaderSetErrorOOM(redisReader *r) {
}

static char *readBytes(redisReader *r, unsigned int bytes) {
    return NULL;
}

/* Find pointer to \r\n. */
static char *seekNewline(char *s, size_t len) {
    return NULL;
}

/* Read a long long value starting at *s, under the assumption that it will be
 * terminated by \r\n. Ambiguously returns -1 for unexpected input. */
static long long readLongLong(char *s) {
    return -1;
}

static char *readLine(redisReader *r, int *_len) {
    return NULL;
}

static void moveToNextTask(redisReader *r) {
}

static int processLineItem(redisReader *r) {
    return -1;
}

static int processBulkItem(redisReader *r) {
    return -1;
}

static int processMultiBulkItem(redisReader *r) {
    return -1;
}

static int processItem(redisReader *r) {
    return -1;
}

redisReader *redisReaderCreate(void) {
    return NULL;
}

void redisReaderFree(redisReader *r) {
}

int redisReaderFeed(redisReader *r, const char *buf, size_t len) {
    return -1;
}

int redisReaderGetReply(redisReader *r, void **reply) {
    return -1;
}

/* Calculate the number of bytes needed to represent an integer as string. */
static int intlen(int i) {
    return -1;
}

/* Helper that calculates the bulk length given a certain string length. */
static size_t bulklen(size_t len) {
    return -1;
}

int redisvFormatCommand(char **target, const char *format, va_list ap) {
    return -1;
}

/* Format a command according to the Redis protocol. This function
 * takes a format similar to printf:
 *
 * %s represents a C null terminated string you want to interpolate
 * %b represents a binary safe string
 *
 * When using %b you need to provide both the pointer to the string
 * and the length in bytes as a size_t. Examples:
 *
 * len = redisFormatCommand(target, "GET %s", mykey);
 * len = redisFormatCommand(target, "SET %s %b", mykey, myval, myvallen);
 */
int redisFormatCommand(char **target, const char *format, ...) {
    va_list ap;
    int len;
    va_start(ap,format);
    len = redisvFormatCommand(target,format,ap);
    va_end(ap);
    return len;
}

/* Format a command according to the Redis protocol. This function takes the
 * number of arguments, an array with arguments and an array with their
 * lengths. If the latter is set to NULL, strlen will be used to compute the
 * argument lengths.
 */
int redisFormatCommandArgv(char **target, int argc, const char **argv, const size_t *argvlen) {
    return -1;
}

void __redisSetError(redisContext *c, int type, const char *str) {
}

static redisContext *redisContextInit(void) {
  redisContext *c;

  c = new redisContext;
  if (c == NULL)
    return NULL;

  c->err = 0;
  c->errstr[0] = '\0';
  return c;
}

void redisFree(redisContext *c) {
  delete (RAMCloud::RamCloud*)c->client;
  delete c;
}

int redisFreeKeepFd(redisContext *c) {
    return -1;
}

/* Connect to a Redis instance. On error the field error in the returned
 * context will be set to the return value of the error function.
 * When no set of reply functions is given, the default set will be used. */
redisContext *redisConnect(const char *ip, int port) {
  redisContext *c;

  c = redisContextInit();
  if (c == NULL)
      return NULL;

  c->flags |= REDIS_BLOCK;

  // Make RAMCloud coordinator locator string from the given ip and port.
  std::stringstream ss;
  ss << "basic+udp:host=" << ip << ",port=" << port;

  c->client = (void*) new RAMCloud::RamCloud(ss.str().c_str());

  return c;
}

redisContext *redisConnectWithTimeout(const char *ip, int port, const struct timeval tv) {
    return NULL;
}

redisContext *redisConnectNonBlock(const char *ip, int port) {
    return NULL;
}

redisContext *redisConnectBindNonBlock(const char *ip, int port,
                                       const char *source_addr) {
    return NULL;
}

redisContext *redisConnectUnix(const char *path) {
    return NULL;
}

redisContext *redisConnectUnixWithTimeout(const char *path, const struct timeval tv) {
    return NULL;
}

redisContext *redisConnectUnixNonBlock(const char *path) {
    return NULL;
}

redisContext *redisConnectFd(int fd) {
    return NULL;
}

/* Set read/write timeout on a blocking socket. */
int redisSetTimeout(redisContext *c, const struct timeval tv) {
    return -1;
}

/* Enable connection KeepAlive. */
int redisEnableKeepAlive(redisContext *c) {
    return -1;
}

/* Use this function to handle a read event on the descriptor. It will try
 * and read some bytes from the socket and feed them to the reply parser.
 *
 * After this function is called, you may use redisContextReadReply to
 * see if there is a reply available. */
int redisBufferRead(redisContext *c) {
    return -1;
}

/* Write the output buffer to the socket.
 *
 * Returns REDIS_OK when the buffer is empty, or (a part of) the buffer was
 * succesfully written to the socket. When the buffer is empty after the
 * write operation, "done" is set to 1 (if given).
 *
 * Returns REDIS_ERR if an error occured trying to write and sets
 * c->errstr to hold the appropriate error string.
 */
int redisBufferWrite(redisContext *c, int *done) {
    return -1;
}

/* Internal helper function to try and get a reply from the reader,
 * or set an error in the context otherwise. */
int redisGetReplyFromReader(redisContext *c, void **reply) {
    return -1;
}

int redisGetReply(redisContext *c, void **reply) {
    return -1;
}


/* Helper function for the redisAppendCommand* family of functions.
 *
 * Write a formatted command to the output buffer. When this family
 * is used, you need to call redisGetReply yourself to retrieve
 * the reply (or replies in pub/sub).
 */
int __redisAppendCommand(redisContext *c, const char *cmd, size_t len) {
    return -1;
}

int redisAppendFormattedCommand(redisContext *c, const char *cmd, size_t len) {
    return -1;
}

int redisvAppendCommand(redisContext *c, const char *format, va_list ap) {
    return -1;
}

int redisAppendCommand(redisContext *c, const char *format, ...) {
    return -1;
}

int redisAppendCommandArgv(redisContext *c, int argc, const char **argv, const size_t *argvlen) {
    return -1;
}

/* Helper function for the redisCommand* family of functions.
 *
 * Write a formatted command to the output buffer. If the given context is
 * blocking, immediately read the reply into the "reply" pointer. When the
 * context is non-blocking, the "reply" pointer will not be used and the
 * command is simply appended to the write buffer.
 *
 * Returns the reply when a reply was succesfully retrieved. Returns NULL
 * otherwise. When NULL is returned in a blocking context, the error field
 * in the context will be set.
 */
static void *__redisBlockForReply(redisContext *c) {
    return NULL;
}

void *redisvCommand(redisContext *c, const char *format, va_list ap) {
  return NULL;
}

void *redisCommand(redisContext *c, const char *format, ...) {
  void *reply = NULL;

  va_list ap;
  va_start(ap,format);
  char buf[128];
  vsprintf(buf,format,ap);
  va_end(ap);

  printf("%s\n", buf);

  return reply;
}

void *redisCommandArgv(redisContext *c, int argc, const char **argv, const size_t *argvlen) {
    return NULL;
}
