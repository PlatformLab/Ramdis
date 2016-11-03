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

std::string unsupportedCommand(clientBuffer *c) {
  std::string res("+Unsupported command.\r\n");
  return res;
}

/* Our command table.
 *
 * Every entry is composed of the following fields:
 *
 * name: a string representing the command name.
 * function: pointer to the C function implementing the command.
 * arity: number of arguments, it is possible to use -N to say >= N
 * sflags: command flags as string. See below for a table of flags.
 * flags: flags as bitmask. Computed by Redis using the 'sflags' field.
 * get_keys_proc: an optional function to get key arguments from a command.
 *                This is only used when the following three fields are not
 *                enough to specify what arguments are keys.
 * first_key_index: first argument that is a key
 * last_key_index: last argument that is a key
 * key_step: step to get all the keys from first to last argument. For instance
 *           in MSET the step is two since arguments are key,val,key,val,...
 * microseconds: microseconds of total execution time for this command.
 * calls: total number of calls of this command.
 *
 * The flags, microseconds and calls fields are computed by Redis and should
 * always be set to zero.
 *
 * Command flags are expressed using strings where every character represents
 * a flag. Later the populateCommandTable() function will take care of
 * populating the real 'flags' field using this characters.
 *
 * This is the meaning of the flags:
 *
 * w: write command (may modify the key space).
 * r: read command  (will never modify the key space).
 * m: may increase memory usage once called. Don't allow if out of memory.
 * a: admin command, like SAVE or SHUTDOWN.
 * p: Pub/Sub related command.
 * f: force replication of this command, regardless of server.dirty.
 * s: command not allowed in scripts.
 * R: random command. Command is not deterministic, that is, the same command
 *    with the same arguments, with the same key space, may have different
 *    results. For instance SPOP and RANDOMKEY are two random commands.
 * S: Sort command output array if called from script, so that the output
 *    is deterministic.
 * l: Allow command while loading the database.
 * t: Allow command while a slave has stale data but is not allowed to
 *    server this data. Normally no command is accepted in this condition
 *    but just a few.
 * M: Do not automatically propagate the command on MONITOR.
 * k: Perform an implicit ASKING for this command, so the command will be
 *    accepted in cluster mode if the slot is marked as 'importing'.
 * F: Fast command: O(1) or O(log(N)) command that should never delay
 *    its execution as long as the kernel scheduler is giving us time.
 *    Note that commands that may trigger a DEL as a side effect (like SET)
 *    are not fast commands.
 */
std::map<const char*, redisCommand> redisCommandTable = {
    {"get", {"get",unsupportedCommand,2,"rF",0,NULL,1,1,1,0,0}},
    {"set", {"set",unsupportedCommand,-3,"wm",0,NULL,1,1,1,0,0}},
    {"setnx", {"setnx",unsupportedCommand,3,"wmF",0,NULL,1,1,1,0,0}},
    {"setex", {"setex",unsupportedCommand,4,"wm",0,NULL,1,1,1,0,0}},
    {"psetex", {"psetex",unsupportedCommand,4,"wm",0,NULL,1,1,1,0,0}},
    {"append", {"append",unsupportedCommand,3,"wm",0,NULL,1,1,1,0,0}},
    {"strlen", {"strlen",unsupportedCommand,2,"rF",0,NULL,1,1,1,0,0}},
    {"del", {"del",unsupportedCommand,-2,"w",0,NULL,1,-1,1,0,0}},
    {"exists", {"exists",unsupportedCommand,-2,"rF",0,NULL,1,-1,1,0,0}},
    {"setbit", {"setbit",unsupportedCommand,4,"wm",0,NULL,1,1,1,0,0}},
    {"getbit", {"getbit",unsupportedCommand,3,"rF",0,NULL,1,1,1,0,0}},
    {"bitfield", {"bitfield",unsupportedCommand,-2,"wm",0,NULL,1,1,1,0,0}},
    {"setrange", {"setrange",unsupportedCommand,4,"wm",0,NULL,1,1,1,0,0}},
    {"getrange", {"getrange",unsupportedCommand,4,"r",0,NULL,1,1,1,0,0}},
    {"substr", {"substr",unsupportedCommand,4,"r",0,NULL,1,1,1,0,0}},
    {"incr", {"incr",unsupportedCommand,2,"wmF",0,NULL,1,1,1,0,0}},
    {"decr", {"decr",unsupportedCommand,2,"wmF",0,NULL,1,1,1,0,0}},
    {"mget", {"mget",unsupportedCommand,-2,"r",0,NULL,1,-1,1,0,0}},
    {"rpush", {"rpush",unsupportedCommand,-3,"wmF",0,NULL,1,1,1,0,0}},
    {"lpush", {"lpush",unsupportedCommand,-3,"wmF",0,NULL,1,1,1,0,0}},
    {"rpushx", {"rpushx",unsupportedCommand,3,"wmF",0,NULL,1,1,1,0,0}},
    {"lpushx", {"lpushx",unsupportedCommand,3,"wmF",0,NULL,1,1,1,0,0}},
    {"linsert", {"linsert",unsupportedCommand,5,"wm",0,NULL,1,1,1,0,0}},
    {"rpop", {"rpop",unsupportedCommand,2,"wF",0,NULL,1,1,1,0,0}},
    {"lpop", {"lpop",unsupportedCommand,2,"wF",0,NULL,1,1,1,0,0}},
    {"brpop", {"brpop",unsupportedCommand,-3,"ws",0,NULL,1,1,1,0,0}},
    {"brpoplpush", {"brpoplpush",unsupportedCommand,4,"wms",0,NULL,1,2,1,0,0}},
    {"blpop", {"blpop",unsupportedCommand,-3,"ws",0,NULL,1,-2,1,0,0}},
    {"llen", {"llen",unsupportedCommand,2,"rF",0,NULL,1,1,1,0,0}},
    {"lindex", {"lindex",unsupportedCommand,3,"r",0,NULL,1,1,1,0,0}},
    {"lset", {"lset",unsupportedCommand,4,"wm",0,NULL,1,1,1,0,0}},
    {"lrange", {"lrange",unsupportedCommand,4,"r",0,NULL,1,1,1,0,0}},
    {"ltrim", {"ltrim",unsupportedCommand,4,"w",0,NULL,1,1,1,0,0}},
    {"lrem", {"lrem",unsupportedCommand,4,"w",0,NULL,1,1,1,0,0}},
    {"rpoplpush", {"rpoplpush",unsupportedCommand,3,"wm",0,NULL,1,2,1,0,0}},
    {"sadd", {"sadd",unsupportedCommand,-3,"wmF",0,NULL,1,1,1,0,0}},
    {"srem", {"srem",unsupportedCommand,-3,"wF",0,NULL,1,1,1,0,0}},
    {"smove", {"smove",unsupportedCommand,4,"wF",0,NULL,1,2,1,0,0}},
    {"sismember", {"sismember",unsupportedCommand,3,"rF",0,NULL,1,1,1,0,0}},
    {"scard", {"scard",unsupportedCommand,2,"rF",0,NULL,1,1,1,0,0}},
    {"spop", {"spop",unsupportedCommand,-2,"wRF",0,NULL,1,1,1,0,0}},
    {"srandmember", {"srandmember",unsupportedCommand,-2,"rR",0,NULL,1,1,1,0,0}},
    {"sinter", {"sinter",unsupportedCommand,-2,"rS",0,NULL,1,-1,1,0,0}},
    {"sinterstore", {"sinterstore",unsupportedCommand,-3,"wm",0,NULL,1,-1,1,0,0}},
    {"sunion", {"sunion",unsupportedCommand,-2,"rS",0,NULL,1,-1,1,0,0}},
    {"sunionstore", {"sunionstore",unsupportedCommand,-3,"wm",0,NULL,1,-1,1,0,0}},
    {"sdiff", {"sdiff",unsupportedCommand,-2,"rS",0,NULL,1,-1,1,0,0}},
    {"sdiffstore", {"sdiffstore",unsupportedCommand,-3,"wm",0,NULL,1,-1,1,0,0}},
    {"smembers", {"smembers",unsupportedCommand,2,"rS",0,NULL,1,1,1,0,0}},
    {"sscan", {"sscan",unsupportedCommand,-3,"rR",0,NULL,1,1,1,0,0}},
    {"zadd", {"zadd",unsupportedCommand,-4,"wmF",0,NULL,1,1,1,0,0}},
    {"zincrby", {"zincrby",unsupportedCommand,4,"wmF",0,NULL,1,1,1,0,0}},
    {"zrem", {"zrem",unsupportedCommand,-3,"wF",0,NULL,1,1,1,0,0}},
    {"zremrangebyscore", {"zremrangebyscore",unsupportedCommand,4,"w",0,NULL,1,1,1,0,0}},
    {"zremrangebyrank", {"zremrangebyrank",unsupportedCommand,4,"w",0,NULL,1,1,1,0,0}},
    {"zremrangebylex", {"zremrangebylex",unsupportedCommand,4,"w",0,NULL,1,1,1,0,0}},
    {"zunionstore", {"zunionstore",unsupportedCommand,-4,"wm",0,NULL,0,0,0,0,0}},
    {"zinterstore", {"zinterstore",unsupportedCommand,-4,"wm",0,NULL,0,0,0,0,0}},
    {"zrange", {"zrange",unsupportedCommand,-4,"r",0,NULL,1,1,1,0,0}},
    {"zrangebyscore", {"zrangebyscore",unsupportedCommand,-4,"r",0,NULL,1,1,1,0,0}},
    {"zrevrangebyscore", {"zrevrangebyscore",unsupportedCommand,-4,"r",0,NULL,1,1,1,0,0}},
    {"zrangebylex", {"zrangebylex",unsupportedCommand,-4,"r",0,NULL,1,1,1,0,0}},
    {"zrevrangebylex", {"zrevrangebylex",unsupportedCommand,-4,"r",0,NULL,1,1,1,0,0}},
    {"zcount", {"zcount",unsupportedCommand,4,"rF",0,NULL,1,1,1,0,0}},
    {"zlexcount", {"zlexcount",unsupportedCommand,4,"rF",0,NULL,1,1,1,0,0}},
    {"zrevrange", {"zrevrange",unsupportedCommand,-4,"r",0,NULL,1,1,1,0,0}},
    {"zcard", {"zcard",unsupportedCommand,2,"rF",0,NULL,1,1,1,0,0}},
    {"zscore", {"zscore",unsupportedCommand,3,"rF",0,NULL,1,1,1,0,0}},
    {"zrank", {"zrank",unsupportedCommand,3,"rF",0,NULL,1,1,1,0,0}},
    {"zrevrank", {"zrevrank",unsupportedCommand,3,"rF",0,NULL,1,1,1,0,0}},
    {"zscan", {"zscan",unsupportedCommand,-3,"rR",0,NULL,1,1,1,0,0}},
    {"hset", {"hset",unsupportedCommand,4,"wmF",0,NULL,1,1,1,0,0}},
    {"hsetnx", {"hsetnx",unsupportedCommand,4,"wmF",0,NULL,1,1,1,0,0}},
    {"hget", {"hget",unsupportedCommand,3,"rF",0,NULL,1,1,1,0,0}},
    {"hmset", {"hmset",unsupportedCommand,-4,"wm",0,NULL,1,1,1,0,0}},
    {"hmget", {"hmget",unsupportedCommand,-3,"r",0,NULL,1,1,1,0,0}},
    {"hincrby", {"hincrby",unsupportedCommand,4,"wmF",0,NULL,1,1,1,0,0}},
    {"hincrbyfloat", {"hincrbyfloat",unsupportedCommand,4,"wmF",0,NULL,1,1,1,0,0}},
    {"hdel", {"hdel",unsupportedCommand,-3,"wF",0,NULL,1,1,1,0,0}},
    {"hlen", {"hlen",unsupportedCommand,2,"rF",0,NULL,1,1,1,0,0}},
    {"hstrlen", {"hstrlen",unsupportedCommand,3,"rF",0,NULL,1,1,1,0,0}},
    {"hkeys", {"hkeys",unsupportedCommand,2,"rS",0,NULL,1,1,1,0,0}},
    {"hvals", {"hvals",unsupportedCommand,2,"rS",0,NULL,1,1,1,0,0}},
    {"hgetall", {"hgetall",unsupportedCommand,2,"r",0,NULL,1,1,1,0,0}},
    {"hexists", {"hexists",unsupportedCommand,3,"rF",0,NULL,1,1,1,0,0}},
    {"hscan", {"hscan",unsupportedCommand,-3,"rR",0,NULL,1,1,1,0,0}},
    {"incrby", {"incrby",unsupportedCommand,3,"wmF",0,NULL,1,1,1,0,0}},
    {"decrby", {"decrby",unsupportedCommand,3,"wmF",0,NULL,1,1,1,0,0}},
    {"incrbyfloat", {"incrbyfloat",unsupportedCommand,3,"wmF",0,NULL,1,1,1,0,0}},
    {"getset", {"getset",unsupportedCommand,3,"wm",0,NULL,1,1,1,0,0}},
    {"mset", {"mset",unsupportedCommand,-3,"wm",0,NULL,1,-1,2,0,0}},
    {"msetnx", {"msetnx",unsupportedCommand,-3,"wm",0,NULL,1,-1,2,0,0}},
    {"randomkey", {"randomkey",unsupportedCommand,1,"rR",0,NULL,0,0,0,0,0}},
    {"select", {"select",unsupportedCommand,2,"lF",0,NULL,0,0,0,0,0}},
    {"move", {"move",unsupportedCommand,3,"wF",0,NULL,1,1,1,0,0}},
    {"rename", {"rename",unsupportedCommand,3,"w",0,NULL,1,2,1,0,0}},
    {"renamenx", {"renamenx",unsupportedCommand,3,"wF",0,NULL,1,2,1,0,0}},
    {"expire", {"expire",unsupportedCommand,3,"wF",0,NULL,1,1,1,0,0}},
    {"expireat", {"expireat",unsupportedCommand,3,"wF",0,NULL,1,1,1,0,0}},
    {"pexpire", {"pexpire",unsupportedCommand,3,"wF",0,NULL,1,1,1,0,0}},
    {"pexpireat", {"pexpireat",unsupportedCommand,3,"wF",0,NULL,1,1,1,0,0}},
    {"keys", {"keys",unsupportedCommand,2,"rS",0,NULL,0,0,0,0,0}},
    {"scan", {"scan",unsupportedCommand,-2,"rR",0,NULL,0,0,0,0,0}},
    {"dbsize", {"dbsize",unsupportedCommand,1,"rF",0,NULL,0,0,0,0,0}},
    {"auth", {"auth",unsupportedCommand,2,"sltF",0,NULL,0,0,0,0,0}},
    {"ping", {"ping",unsupportedCommand,-1,"tF",0,NULL,0,0,0,0,0}},
    {"echo", {"echo",unsupportedCommand,2,"F",0,NULL,0,0,0,0,0}},
    {"save", {"save",unsupportedCommand,1,"as",0,NULL,0,0,0,0,0}},
    {"bgsave", {"bgsave",unsupportedCommand,-1,"a",0,NULL,0,0,0,0,0}},
    {"bgrewriteaof", {"bgrewriteaof",unsupportedCommand,1,"a",0,NULL,0,0,0,0,0}},
    {"shutdown", {"shutdown",unsupportedCommand,-1,"alt",0,NULL,0,0,0,0,0}},
    {"lastsave", {"lastsave",unsupportedCommand,1,"RF",0,NULL,0,0,0,0,0}},
    {"type", {"type",unsupportedCommand,2,"rF",0,NULL,1,1,1,0,0}},
    {"multi", {"multi",unsupportedCommand,1,"sF",0,NULL,0,0,0,0,0}},
    {"exec", {"exec",unsupportedCommand,1,"sM",0,NULL,0,0,0,0,0}},
    {"discard", {"discard",unsupportedCommand,1,"sF",0,NULL,0,0,0,0,0}},
    {"sync", {"sync",unsupportedCommand,1,"ars",0,NULL,0,0,0,0,0}},
    {"psync", {"psync",unsupportedCommand,3,"ars",0,NULL,0,0,0,0,0}},
    {"replconf", {"replconf",unsupportedCommand,-1,"aslt",0,NULL,0,0,0,0,0}},
    {"flushdb", {"flushdb",unsupportedCommand,1,"w",0,NULL,0,0,0,0,0}},
    {"flushall", {"flushall",unsupportedCommand,1,"w",0,NULL,0,0,0,0,0}},
    {"sort", {"sort",unsupportedCommand,-2,"wm",0,NULL,1,1,1,0,0}},
    {"info", {"info",unsupportedCommand,-1,"lt",0,NULL,0,0,0,0,0}},
    {"monitor", {"monitor",unsupportedCommand,1,"as",0,NULL,0,0,0,0,0}},
    {"ttl", {"ttl",unsupportedCommand,2,"rF",0,NULL,1,1,1,0,0}},
    {"touch", {"touch",unsupportedCommand,-2,"rF",0,NULL,1,1,1,0,0}},
    {"pttl", {"pttl",unsupportedCommand,2,"rF",0,NULL,1,1,1,0,0}},
    {"persist", {"persist",unsupportedCommand,2,"wF",0,NULL,1,1,1,0,0}},
    {"slaveof", {"slaveof",unsupportedCommand,3,"ast",0,NULL,0,0,0,0,0}},
    {"role", {"role",unsupportedCommand,1,"lst",0,NULL,0,0,0,0,0}},
    {"debug", {"debug",unsupportedCommand,-1,"as",0,NULL,0,0,0,0,0}},
    {"config", {"config",unsupportedCommand,-2,"lat",0,NULL,0,0,0,0,0}},
    {"subscribe", {"subscribe",unsupportedCommand,-2,"pslt",0,NULL,0,0,0,0,0}},
    {"unsubscribe", {"unsubscribe",unsupportedCommand,-1,"pslt",0,NULL,0,0,0,0,0}},
    {"psubscribe", {"psubscribe",unsupportedCommand,-2,"pslt",0,NULL,0,0,0,0,0}},
    {"punsubscribe", {"punsubscribe",unsupportedCommand,-1,"pslt",0,NULL,0,0,0,0,0}},
    {"publish", {"publish",unsupportedCommand,3,"pltF",0,NULL,0,0,0,0,0}},
    {"pubsub", {"pubsub",unsupportedCommand,-2,"pltR",0,NULL,0,0,0,0,0}},
    {"watch", {"watch",unsupportedCommand,-2,"sF",0,NULL,1,-1,1,0,0}},
    {"unwatch", {"unwatch",unsupportedCommand,1,"sF",0,NULL,0,0,0,0,0}},
    {"cluster", {"cluster",unsupportedCommand,-2,"a",0,NULL,0,0,0,0,0}},
    {"restore", {"restore",unsupportedCommand,-4,"wm",0,NULL,1,1,1,0,0}},
    {"restore-asking", {"restore-asking",unsupportedCommand,-4,"wmk",0,NULL,1,1,1,0,0}},
    {"migrate", {"migrate",unsupportedCommand,-6,"w",0,NULL,0,0,0,0,0}},
    {"asking", {"asking",unsupportedCommand,1,"F",0,NULL,0,0,0,0,0}},
    {"readonly", {"readonly",unsupportedCommand,1,"F",0,NULL,0,0,0,0,0}},
    {"readwrite", {"readwrite",unsupportedCommand,1,"F",0,NULL,0,0,0,0,0}},
    {"dump", {"dump",unsupportedCommand,2,"r",0,NULL,1,1,1,0,0}},
    {"object", {"object",unsupportedCommand,3,"r",0,NULL,2,2,2,0,0}},
    {"client", {"client",unsupportedCommand,-2,"as",0,NULL,0,0,0,0,0}},
    {"eval", {"eval",unsupportedCommand,-3,"s",0,NULL,0,0,0,0,0}},
    {"evalsha", {"evalsha",unsupportedCommand,-3,"s",0,NULL,0,0,0,0,0}},
    {"slowlog", {"slowlog",unsupportedCommand,-2,"a",0,NULL,0,0,0,0,0}},
    {"script", {"script",unsupportedCommand,-2,"s",0,NULL,0,0,0,0,0}},
    {"time", {"time",unsupportedCommand,1,"RF",0,NULL,0,0,0,0,0}},
    {"bitop", {"bitop",unsupportedCommand,-4,"wm",0,NULL,2,-1,1,0,0}},
    {"bitcount", {"bitcount",unsupportedCommand,-2,"r",0,NULL,1,1,1,0,0}},
    {"bitpos", {"bitpos",unsupportedCommand,-3,"r",0,NULL,1,1,1,0,0}},
    {"wait", {"wait",unsupportedCommand,3,"s",0,NULL,0,0,0,0,0}},
    {"command", {"command",unsupportedCommand,0,"lt",0,NULL,0,0,0,0,0}},
    {"geoadd", {"geoadd",unsupportedCommand,-5,"wm",0,NULL,1,1,1,0,0}},
    {"georadius", {"georadius",unsupportedCommand,-6,"w",0,NULL,1,1,1,0,0}},
    {"georadiusbymember", {"georadiusbymember",unsupportedCommand,-5,"w",0,NULL,1,1,1,0,0}},
    {"geohash", {"geohash",unsupportedCommand,-2,"r",0,NULL,1,1,1,0,0}},
    {"geopos", {"geopos",unsupportedCommand,-2,"r",0,NULL,1,1,1,0,0}},
    {"geodist", {"geodist",unsupportedCommand,-4,"r",0,NULL,1,1,1,0,0}},
    {"pfselftest", {"pfselftest",unsupportedCommand,1,"a",0,NULL,0,0,0,0,0}},
    {"pfadd", {"pfadd",unsupportedCommand,-2,"wmF",0,NULL,1,1,1,0,0}},
    {"pfcount", {"pfcount",unsupportedCommand,-2,"r",0,NULL,1,-1,1,0,0}},
    {"pfmerge", {"pfmerge",unsupportedCommand,-2,"wm",0,NULL,1,-1,1,0,0}},
    {"pfdebug", {"pfdebug",unsupportedCommand,-3,"w",0,NULL,0,0,0,0,0}},
    {"latency", {"latency",unsupportedCommand,-2,"aslt",0,NULL,0,0,0,0,0}}
};

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