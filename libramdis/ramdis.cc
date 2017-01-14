#include <string>
#include <sstream>
#include <vector>
#include <stdarg.h>

#include "ramdis.h"
#include "RamCloud.h"
#include "Transaction.h"
#include "ClientException.h"
#include "../PerfUtils/TimeTrace.h"

/* TODO:
 * [ ] Argument checking.
 * [ ] Fix error reporting. The context object is opaque to the user. There
 * user therefore cannot use the err and errmsg fields.
 * [ ] Instead of allocating an ObjectArray on the heap for returning to the
 * user, just return it on the stack. The cost of malloc is greater than the
 * cost of copying the whole structure in the stack.
 * [ ] Handle case where tx.commit() fails and a retry is needed.
 * [ ] Remove list segments from RAMCloud that have their last element popped
 * (?)
 */

struct ObjectMetadata {
  uint8_t type;
};

struct ListIndexEntry {
  int16_t segId;
  uint16_t elemCount;
  uint8_t segSizeKb;
};

#define MAX_LIST_SEG_SIZE_KB 5

struct ListIndex {
  ListIndexEntry* entries;
  uint32_t len;
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

void appendKeyComponent(RAMCloud::Buffer* buf, const char* comp, 
    uint16_t compLen) {
  /* Use appendCopy here because we are not sure if the caller will maintain
   * the memory pointed to for the duration of the use of the buffer. */
  buf->appendCopy((void*)&compLen, sizeof(uint16_t));
  buf->appendCopy(comp, compLen);
}

Context* ramdis_connect(char* locator) {
  Context* c = new Context();
  RAMCloud::RamCloud* client = new RAMCloud::RamCloud(locator);
  c->client = (void*)client;
  c->tableId = client->createTable("default");
  c->err = 0;
  memset(c->errmsg, '\0', sizeof(c->errmsg));
  return c;
}

void ramdis_disconnect(Context* c) {
  RAMCloud::RamCloud* client = (RAMCloud::RamCloud*)c->client;
  delete client;
  delete c;
}

char* ping(Context* c, char* msg) {
  return NULL;
}

Object* get(Context* c, Object* key) {
  RAMCloud::RamCloud* client = (RAMCloud::RamCloud*)c->client;

  RAMCloud::Buffer rootKey;
  appendKeyComponent(&rootKey, (char*)key->data, key->len);

  RAMCloud::Buffer rootValue;
  try {
    client->read(c->tableId, 
        rootKey.getRange(0, rootKey.size()), 
        rootKey.size(), 
        &rootValue);

    if (rootValue.size() < sizeof(struct ObjectMetadata)) {
      ERROR("Data structure malformed. This is a bug.\n");
      DEBUG("Object exists but is missing its metadata.\n");
      c->err = -1;
      snprintf(c->errmsg, sizeof(c->errmsg), 
          "Data structure malformed. This is a bug.");
      return NULL;
    }

    Object* value = (Object*)malloc(sizeof(Object));
    value->len = rootValue.size() - sizeof(struct ObjectMetadata);
    value->data = (void*)malloc(value->len);
    rootValue.copy(sizeof(struct ObjectMetadata), value->len, value->data);
    
    return value;
  } catch (RAMCloud::ObjectDoesntExistException& e) {
    c->err = -1;
    snprintf(c->errmsg, sizeof(c->errmsg), 
        "Unknown key");
    return NULL;
  }
}

void set(Context* c, Object* key, Object* value) {
  RAMCloud::RamCloud* client = (RAMCloud::RamCloud*)c->client;

  RAMCloud::Buffer rootKey;
  appendKeyComponent(&rootKey, (char*)key->data, key->len);

  RAMCloud::Buffer rootValue;

  struct ObjectMetadata objMtd;
  objMtd.type = REDIS_STRING;

  rootValue.append((void*)&objMtd, sizeof(struct ObjectMetadata));
  rootValue.append(value->data, value->len);

  client->write(c->tableId,
        rootKey.getRange(0, rootKey.size()), 
        rootKey.size(),
        rootValue.getRange(0, rootValue.size()), 
        rootValue.size());
}

void mset(Context* c, ObjectArray* keysArray, Object* valuesArray) {

}

long incr(Context* c, Object* key) {
  RAMCloud::RamCloud* client = (RAMCloud::RamCloud*)c->client;

  RAMCloud::Buffer rootKey;
  appendKeyComponent(&rootKey, (char*)key->data, key->len);

  try {
    uint64_t newValue = client->incrementInt64(c->tableId, 
        rootKey.getRange(0, rootKey.size()), 
        rootKey.size(),
        1);
    return (long)newValue;
  } catch (RAMCloud::ObjectDoesntExistException& e) {
    c->err = -1;
    snprintf(c->errmsg, sizeof(c->errmsg), 
        "Unknown key");
    return -1;
  }
}

uint64_t lpush(Context* c, Object* key, Object* value) {
  RAMCloud::RamCloud* client = (RAMCloud::RamCloud*)c->client;
  RAMCloud::Transaction tx(client);

  /* Construct RAMCloud key for the list index. */ 
  RAMCloud::Buffer rootKey;
  appendKeyComponent(&rootKey, (char*)key->data, key->len);

  /* Read the index. */
  RAMCloud::Buffer rootValue;
  bool objectExists = true;
  try {
    tx.read(c->tableId, 
        rootKey.getRange(0, rootKey.size()), 
        rootKey.size(), 
        &rootValue);
  } catch (RAMCloud::ObjectDoesntExistException& e) {
    objectExists = false;
  }

  /* Sanity checks:
   * 1) If the object exists it should have metadata.
   * 2) If the object exists it should be of the correct data type. */
  struct ObjectMetadata* objMtd = NULL;
  if (objectExists) {
    if (rootValue.size() < sizeof(struct ObjectMetadata)) {
      tx.commit();
      ERROR("Data structure malformed. This is a bug.\n");
      DEBUG("Object exists but is missing its metadata.\n");
      c->err = -1;
      snprintf(c->errmsg, sizeof(c->errmsg), 
          "Data structure malformed. This is a bug.");
      return 0;
    } else {
      objMtd = rootValue.getOffset<struct ObjectMetadata>(0);
      if (objMtd->type != REDIS_LIST) {
        tx.commit();
        c->err = -1;
        snprintf(c->errmsg, sizeof(c->errmsg), 
            "WRONGTYPE Operation against a key holding the wrong kind of "
            "value");
        return 0;
      }
    }
  }

  ListIndex index;
  index.entries = NULL;
  index.len = 0;
  bool headSegFull = false;
  uint64_t totalElements = 0;
  if (objectExists && rootValue.size() > sizeof(struct ObjectMetadata)) {
    index.entries = static_cast<ListIndexEntry*>(
        rootValue.getRange(sizeof(struct ObjectMetadata), 
          rootValue.size() - sizeof(struct ObjectMetadata)));  
    index.len = (rootValue.size() - sizeof(struct ObjectMetadata)) 
        / sizeof(ListIndexEntry);

    if (index.entries[0].segSizeKb >= MAX_LIST_SEG_SIZE_KB) {
      headSegFull = true;
    } 

    for (int i = 0; i < index.len; i++) {
      totalElements += index.entries[i].elemCount;
    }
  }


  RAMCloud::Buffer segKey;
  RAMCloud::Buffer newSegValue;
  RAMCloud::Buffer newRootValue;
  if (!objectExists || index.len == 0 || headSegFull) {
    /* If the list doesn't exist, or the index is empty, or the head segment is
     * full, then create a new head segment. */
    int16_t newSegId;
    if (!objectExists || index.len == 0) {
      newSegId = 0;
    } else {
      newSegId = index.entries[0].segId + 1;

      if (newSegId == index.entries[index.len - 1].segId) {
        /* New head segment is colliding with the existing tail segment. */
        /* In this case we need to attempt a compaction of the list segments.
         * This is left for future work. For now return an error. */
        c->err = -1;
        snprintf(c->errmsg, sizeof(c->errmsg), 
            "List is full");
        return 0;
      }
    }
    
    segKey.append(&rootKey);
    appendKeyComponent(&segKey, (char*)&newSegId, sizeof(int16_t));

    uint16_t valueLen = (uint16_t)value->len;
    newSegValue.append((void*)&valueLen, sizeof(uint16_t));
    newSegValue.append(value->data, valueLen);

    /* Either the list does not exist or the index is completely empty of
     * entries. In either case, put a new head segment index entry in the
     * index, write it back, and write a new head segment. */
    ListIndexEntry entry;
    entry.segId = newSegId;
    entry.elemCount = 1;
    entry.segSizeKb = (uint8_t)(newSegValue.size() >> 10);

    if (!objectExists) {
      /* Append new metadata header. */
      struct ObjectMetadata newObjMtd;
      newObjMtd.type = REDIS_LIST;
      newRootValue.appendCopy((void*)&newObjMtd, sizeof(struct
            ObjectMetadata));
    } else {
      /* Append existing metadata header. */
      newRootValue.append((void*)objMtd, sizeof(struct ObjectMetadata));
    }

    newRootValue.append((void*)&entry, sizeof(ListIndexEntry));

    if (index.len > 0) {
      newRootValue.append((void*)index.entries, 
          index.len * sizeof(ListIndexEntry)); 
    }
  } else if (index.entries[0].elemCount == 0) {
    /* The list exists, the index is not empty, but the head segment is empty.
     * In this case we don't need to read the head segment to add the new
     * value, we can just write the new head segment directly for lower
     * latency. */

    segKey.append(&rootKey);
    appendKeyComponent(&segKey, (char*)&index.entries[0].segId, 
        sizeof(int16_t));

    uint16_t valueLen = (uint16_t)value->len;
    newSegValue.append((void*)&valueLen, sizeof(uint16_t));
    newSegValue.append(value->data, valueLen);

    index.entries[0].elemCount = 1;
    index.entries[0].segSizeKb = (uint8_t)(newSegValue.size() >> 10);
      
    newRootValue.append((void*)objMtd, sizeof(struct ObjectMetadata));
    newRootValue.append(index.entries, index.len*sizeof(ListIndexEntry));
  } else {
    /* The list exists, the index is not empty, and the head segment is neither
     * full nor totally empty. In this case we need to read the head segment
     * and add the new value to it. */

    segKey.append(&rootKey);
    appendKeyComponent(&segKey, (char*)&index.entries[0].segId, 
        sizeof(int16_t));

    RAMCloud::Buffer segValue;
    try {
      tx.read(c->tableId,
          segKey.getRange(0, segKey.size()), 
          segKey.size(), 
          &segValue);
    } catch (RAMCloud::ObjectDoesntExistException& e) {
      ERROR("List is corrupted. This is a bug.\n");
      DEBUG("List index entry %d shows segId %d having %d elements, but this segment does not exist.\n", 
          0, 
          index.entries[0].segId,
          index.entries[0].elemCount);
      for (int i = 0; i < index.len; i++) {
        DEBUG("Index entry %5d: segId: %5d, elemCount: %5d, segSizeKb: %5dKb\n",
            i,
            index.entries[i].segId,
            index.entries[i].elemCount,
            index.entries[i].segSizeKb);
      }
      c->err = -1;
      snprintf(c->errmsg, sizeof(c->errmsg), 
          "List is corrupted.");
      return 0;
    }

    uint16_t valueLen = (uint16_t)value->len;
    newSegValue.append((void*)&valueLen, sizeof(uint16_t));
    newSegValue.append(&segValue, 0, 
        index.entries[0].elemCount * sizeof(uint16_t));
    newSegValue.append(value->data, valueLen);
    newSegValue.append(&segValue, 
        index.entries[0].elemCount * sizeof(uint16_t));

    index.entries[0].elemCount++;
    index.entries[0].segSizeKb = (uint8_t)(newSegValue.size() >> 10);

    newRootValue.append((void*)objMtd, sizeof(struct ObjectMetadata));
    newRootValue.append(index.entries, index.len*sizeof(ListIndexEntry));
  }

  tx.write(c->tableId,
      segKey.getRange(0, segKey.size()),
      segKey.size(),
      newSegValue.getRange(0, newSegValue.size()),
      newSegValue.size());

  tx.write(c->tableId, 
      rootKey.getRange(0, rootKey.size()), 
      rootKey.size(), 
      newRootValue.getRange(0, newRootValue.size()),
      newRootValue.size());

  tx.commit();

  return totalElements + 1;
}

uint64_t rpush(Context* c, Object* key, Object* value) {
  RAMCloud::RamCloud* client = (RAMCloud::RamCloud*)c->client;
  RAMCloud::Transaction tx(client);

  /* Construct RAMCloud key for the list index. */ 
  RAMCloud::Buffer rootKey;
  appendKeyComponent(&rootKey, (char*)key->data, key->len);

  /* Read the index. */
  RAMCloud::Buffer rootValue;
  bool objectExists = true;
  try {
    tx.read(c->tableId, 
        rootKey.getRange(0, rootKey.size()), 
        rootKey.size(), 
        &rootValue);
  } catch (RAMCloud::ObjectDoesntExistException& e) {
    objectExists = false;
  }

  /* Sanity checks:
   * 1) If the object exists it should have metadata.
   * 2) If the object exists it should be of the correct data type. */
  struct ObjectMetadata* objMtd = NULL;
  if (objectExists) {
    if (rootValue.size() < sizeof(struct ObjectMetadata)) {
      tx.commit();
      ERROR("Data structure malformed. This is a bug.\n");
      DEBUG("Object exists but is missing its metadata.\n");
      c->err = -1;
      snprintf(c->errmsg, sizeof(c->errmsg), 
          "Data structure malformed. This is a bug.");
      return 0;
    } else {
      objMtd = rootValue.getOffset<struct ObjectMetadata>(0);
      if (objMtd->type != REDIS_LIST) {
        tx.commit();
        c->err = -1;
        snprintf(c->errmsg, sizeof(c->errmsg), 
            "WRONGTYPE Operation against a key holding the wrong kind of "
            "value");
        return 0;
      }
    }
  }

  ListIndex index;
  index.entries = NULL;
  index.len = 0;
  bool tailSegFull = false;
  uint64_t totalElements = 0;
  if (objectExists && rootValue.size() > sizeof(struct ObjectMetadata)) {
    index.entries = static_cast<ListIndexEntry*>(
        rootValue.getRange(sizeof(struct ObjectMetadata), 
          rootValue.size() - sizeof(struct ObjectMetadata)));  
    index.len = (rootValue.size() - sizeof(struct ObjectMetadata)) 
        / sizeof(ListIndexEntry);

    if (index.entries[index.len - 1].segSizeKb >= MAX_LIST_SEG_SIZE_KB) {
      tailSegFull = true;
    } 

    for (int i = 0; i < index.len; i++) {
      totalElements += index.entries[i].elemCount;
    }
  }

  RAMCloud::Buffer segKey;
  RAMCloud::Buffer newSegValue;
  RAMCloud::Buffer newRootValue;
  if (!objectExists || index.len == 0 || tailSegFull) {
    /* If the list doesn't exist, or the index is empty, or the tail segment is
     * full, then create a new head segment. */
    int16_t newSegId;
    if (!objectExists || index.len == 0) {
      newSegId = 0;
    } else {
      newSegId = index.entries[index.len - 1].segId - 1;

      if (newSegId == index.entries[0].segId) {
        /* New tail segment is colliding with the existing head segment. */
        /* In this case we need to attempt a compaction of the list segments.
         * This is left for future work. For now return an error. */
        c->err = -1;
        snprintf(c->errmsg, sizeof(c->errmsg), 
            "List is full");
        return 0;
      }
    }
    
    segKey.append(&rootKey);
    appendKeyComponent(&segKey, (char*)&newSegId, sizeof(int16_t));

    uint16_t valueLen = (uint16_t)value->len;
    newSegValue.append((void*)&valueLen, sizeof(uint16_t));
    newSegValue.append(value->data, valueLen);

    /* Either the list does not exist or the index is completely empty of
     * entries. In either case, put a new head segment index entry in the
     * index, write it back, and write a new head segment. */
    ListIndexEntry entry;
    entry.segId = newSegId;
    entry.elemCount = 1;
    entry.segSizeKb = (uint8_t)(newSegValue.size() >> 10);

    if (!objectExists) {
      /* Append new metadata header. */
      struct ObjectMetadata newObjMtd;
      newObjMtd.type = REDIS_LIST;
      newRootValue.appendCopy((void*)&newObjMtd, sizeof(struct
            ObjectMetadata));
    } else {
      /* Append existing metadata header. */
      newRootValue.append((void*)objMtd, sizeof(struct ObjectMetadata));
    }

    if (index.len > 0) {
      newRootValue.append((void*)index.entries, 
          index.len * sizeof(ListIndexEntry)); 
    }

    newRootValue.append((void*)&entry, sizeof(ListIndexEntry));
  } else if (index.entries[index.len - 1].elemCount == 0) {
    /* The list exists, the index is not empty, but the tail segment is empty.
     * In this case we don't need to read the tail segment to add the new
     * value, we can just write the new tail segment directly for lower
     * latency. */

    segKey.append(&rootKey);
    appendKeyComponent(&segKey, (char*)&index.entries[index.len - 1].segId, 
        sizeof(int16_t));

    uint16_t valueLen = (uint16_t)value->len;
    newSegValue.append((void*)&valueLen, sizeof(uint16_t));
    newSegValue.append(value->data, valueLen);

    index.entries[index.len - 1].elemCount = 1;
    index.entries[index.len - 1].segSizeKb = 
        (uint8_t)(newSegValue.size() >> 10);

    newRootValue.append((void*)objMtd, sizeof(struct ObjectMetadata));
    newRootValue.append(index.entries, index.len*sizeof(ListIndexEntry));
  } else {
    /* The list exists, the index is not empty, and the tail segment is neither
     * full nor totally empty. In this case we need to read the tail segment
     * and add the new value to it. */

    segKey.append(&rootKey);
    appendKeyComponent(&segKey, (char*)&index.entries[index.len - 1].segId, 
        sizeof(int16_t));

    RAMCloud::Buffer segValue;
    try {
      tx.read(c->tableId,
          segKey.getRange(0, segKey.size()), 
          segKey.size(), 
          &segValue);
    } catch (RAMCloud::ObjectDoesntExistException& e) {
      ERROR("List is corrupted. This is a bug.\n");
      DEBUG("List index entry %d shows segId %d having %d elements, but this segment does not exist.\n", 
          index.len - 1, 
          index.entries[index.len - 1].segId,
          index.entries[index.len - 1].elemCount);
      for (int i = 0; i < index.len; i++) {
        DEBUG("Index entry %5d: segId: %5d, elemCount: %5d, segSizeKb: %5dKb\n",
            i,
            index.entries[i].segId,
            index.entries[i].elemCount,
            index.entries[i].segSizeKb);
      }
      c->err = -1;
      snprintf(c->errmsg, sizeof(c->errmsg), 
          "List is corrupted.");
      return 0;
    }

    uint16_t valueLen = (uint16_t)value->len;
    newSegValue.append(&segValue, 0, 
        index.entries[index.len - 1].elemCount * sizeof(uint16_t));
    newSegValue.append((void*)&valueLen, sizeof(uint16_t));
    newSegValue.append(&segValue, 
        index.entries[index.len - 1].elemCount * sizeof(uint16_t));
    newSegValue.append(value->data, valueLen);

    index.entries[index.len - 1].elemCount++;
    index.entries[index.len - 1].segSizeKb = 
        (uint8_t)(newSegValue.size() >> 10);

    newRootValue.append((void*)objMtd, sizeof(struct ObjectMetadata));
    newRootValue.append(index.entries, index.len*sizeof(ListIndexEntry));
  }

  tx.write(c->tableId,
      segKey.getRange(0, segKey.size()),
      segKey.size(),
      newSegValue.getRange(0, newSegValue.size()),
      newSegValue.size());

  tx.write(c->tableId, 
      rootKey.getRange(0, rootKey.size()), 
      rootKey.size(), 
      newRootValue.getRange(0, newRootValue.size()),
      newRootValue.size());

  tx.commit();

  return totalElements + 1;
}

Object* lpop(Context* c, Object* key) {
  RAMCloud::RamCloud* client = (RAMCloud::RamCloud*)c->client;
  RAMCloud::Transaction tx(client);

  /* Construct RAMCloud key for the list index. */ 
  RAMCloud::Buffer rootKey;
  appendKeyComponent(&rootKey, (char*)key->data, key->len);

  /* Read the index. */
  RAMCloud::Buffer rootValue;
  try {
    tx.read(c->tableId, 
        rootKey.getRange(0, rootKey.size()), 
        rootKey.size(), 
        &rootValue);
  } catch (RAMCloud::ObjectDoesntExistException& e) {
    c->err = -1;
    snprintf(c->errmsg, sizeof(c->errmsg), 
        "Unknown key");
    return NULL;
  }

  /* Sanity checks:
   * 1) If the object exists it should have metadata.
   * 2) If the object exists it should be of the correct data type. */
  struct ObjectMetadata* objMtd = NULL;
  if (rootValue.size() < sizeof(struct ObjectMetadata)) {
    tx.commit();
    ERROR("Data structure malformed. This is a bug.\n");
    DEBUG("Object exists but is missing its metadata.\n");
    c->err = -1;
    snprintf(c->errmsg, sizeof(c->errmsg), 
        "Data structure malformed. This is a bug.");
    return 0;
  } else {
    objMtd = rootValue.getOffset<struct ObjectMetadata>(0);
    if (objMtd->type != REDIS_LIST) {
      tx.commit();
      c->err = -1;
      snprintf(c->errmsg, sizeof(c->errmsg), 
          "WRONGTYPE Operation against a key holding the wrong kind of "
          "value");
      return 0;
    }
  }

  if (rootValue.size() == sizeof(struct ObjectMetadata)) {
    /* List exists but it's empty. */
    c->err = -1;
    snprintf(c->errmsg, sizeof(c->errmsg), 
        "List is empty");
    return NULL;
  }

  ListIndex index;
  uint64_t totalElements = 0;
  index.entries = static_cast<ListIndexEntry*>(
      rootValue.getRange(sizeof(struct ObjectMetadata), 
        rootValue.size() - sizeof(struct ObjectMetadata)));  
  index.len = (rootValue.size() - sizeof(struct ObjectMetadata)) 
      / sizeof(ListIndexEntry);

  for (int i = 0; i < index.len; i++) {
    totalElements += index.entries[i].elemCount;
  }

  RAMCloud::Buffer newRootValue;
  newRootValue.append((void*)objMtd, sizeof(struct ObjectMetadata));

  if (totalElements == 0) {
    /* List index has entries but no elements in the list. In this case, reset
     * the index to a default state, if needed. */
    if (index.len != 1 || index.entries[0].segId != 0) {
      ListIndexEntry entry;
      entry.segId = 0;
      entry.elemCount = 0;
      entry.segSizeKb = 0;
      
      newRootValue.append((void*)&entry, sizeof(ListIndexEntry));

      tx.write(c->tableId, 
          rootKey.getRange(0, rootKey.size()),
          rootKey.size(),
          newRootValue.getRange(0, newRootValue.size()),
          newRootValue.size());

      tx.commit();
    }

    c->err = -1;
    snprintf(c->errmsg, sizeof(c->errmsg), 
        "List is empty");
    return NULL;
  }

  /* At this point, we know that the list exists, the index has entries, and
   * the sum of elements in the segments is non-zero. We don't know where the
   * head element is, however. The common case is that it's in the first
   * segment, and not the last element. Sometimes it will be the last element
   * there, and we need to remove that segment from the index. In rare cases,
   * the head segments may be empty and need to be removed on the way to the
   * head element. It may also be the case that after removing the last element
   * from a segment, the following segments are empty. In this case we take the
   * time to do some clean-up and remove those segments from the index. */

  for (int i = 0; i < index.len; i++) {
    if (index.entries[i].elemCount == 0) {
      // Skip over the leading segments that are empty.
      continue;
    } else {
      // First segment that has 1 or more elements.
      RAMCloud::Buffer segKey;

      segKey.append(&rootKey);
      appendKeyComponent(&segKey, (char*)&index.entries[i].segId, 
          sizeof(int16_t));

      RAMCloud::Buffer segValue;
      try {
        tx.read(c->tableId,
            segKey.getRange(0, segKey.size()), 
            segKey.size(), 
            &segValue);
      } catch (RAMCloud::ObjectDoesntExistException& e) {
        c->err = -1;
        snprintf(c->errmsg, sizeof(c->errmsg), 
            "List is corrupted.");
        return 0;
      }

      // Extract value from segment.
      uint16_t len = *static_cast<uint16_t*>(segValue.getRange(
            0, sizeof(uint16_t)));
      Object* obj = (Object*)malloc(sizeof(Object));
      obj->data = (void*)malloc(len);
      obj->len = len;
      segValue.copy(index.entries[i].elemCount * sizeof(uint16_t), len, 
          obj->data);

      if (index.entries[i].elemCount == 1) {
        /* This is the last element in the segment. In this case, remove the
         * segment from the list, as well as any following segments that are
         * empty. */
        if (totalElements == 1) {
          /* This segment contained the very last element in this list. The
           * list is now completely empty. In this case, reset the list to a
           * default state. We currently do this by resetting the index and
           * don't actually remove the segments that have a single object in
           * them from RAMCloud. They will eventually be overwriten if the list
           * is sufficiently filled again. */
          ListIndexEntry entry;
          entry.segId = 0;
          entry.elemCount = 0;
          entry.segSizeKb = 0;

          newRootValue.append((void*)&entry, sizeof(ListIndexEntry));

          tx.write(c->tableId, 
              rootKey.getRange(0, rootKey.size()),
              rootKey.size(),
              newRootValue.getRange(0, newRootValue.size()),
              newRootValue.size());

          tx.commit();
        } else {
          /* There are more elements in segments down the line. Find the next
           * non-empty segment to be the head segment. */
          for (int j = i + 1; j < index.len; j++) {
            if (index.entries[j].elemCount > 0) {
              newRootValue.append(&index.entries[j], 
                  (index.len - j) * sizeof(ListIndexEntry));

              tx.write(c->tableId,
                  rootKey.getRange(0, rootKey.size()),
                  rootKey.size(),
                  newRootValue.getRange(0, newRootValue.size()),
                  newRootValue.size());

              tx.commit();
              break;
            }
          }
        }
      } else {
        /* The element that we just popped was not the last element in the
         * segment. In this case write the new segment value back and update
         * the index. */
        RAMCloud::Buffer newSegValue;
        newSegValue.append(&segValue, sizeof(uint16_t), 
            (index.entries[i].elemCount - 1) * sizeof(uint16_t));
        newSegValue.append(&segValue, 
            (index.entries[i].elemCount * sizeof(uint16_t)) + len);

        tx.write(c->tableId,
            segKey.getRange(0, segKey.size()),
            segKey.size(),
            newSegValue.getRange(0, newSegValue.size()),
            newSegValue.size());

        index.entries[i].elemCount--;
        index.entries[i].segSizeKb = (uint8_t)(newSegValue.size() >> 10);

        newRootValue.append(&index.entries[i], 
            (index.len - i)*sizeof(ListIndexEntry));

        tx.write(c->tableId,
            rootKey.getRange(0, rootKey.size()),
            rootKey.size(),
            newRootValue.getRange(0, newRootValue.size()),
            newRootValue.size());

        tx.commit();
      }
      
      return obj;
    }
  }
}

Object* rpop(Context* c, Object* key) {
  RAMCloud::RamCloud* client = (RAMCloud::RamCloud*)c->client;
  RAMCloud::Transaction tx(client);

  /* Construct RAMCloud key for the list index. */ 
  RAMCloud::Buffer rootKey;
  appendKeyComponent(&rootKey, (char*)key->data, key->len);

  /* Read the index. */
  RAMCloud::Buffer rootValue;
  try {
    tx.read(c->tableId, 
        rootKey.getRange(0, rootKey.size()), 
        rootKey.size(), 
        &rootValue);
  } catch (RAMCloud::ObjectDoesntExistException& e) {
    c->err = -1;
    snprintf(c->errmsg, sizeof(c->errmsg), 
        "Unknown key");
    return NULL;
  }

  /* Sanity checks:
   * 1) If the object exists it should have metadata.
   * 2) If the object exists it should be of the correct data type. */
  struct ObjectMetadata* objMtd = NULL;
  if (rootValue.size() < sizeof(struct ObjectMetadata)) {
    tx.commit();
    ERROR("Data structure malformed. This is a bug.\n");
    DEBUG("Object exists but is missing its metadata.\n");
    c->err = -1;
    snprintf(c->errmsg, sizeof(c->errmsg), 
        "Data structure malformed. This is a bug.");
    return 0;
  } else {
    objMtd = rootValue.getOffset<struct ObjectMetadata>(0);
    if (objMtd->type != REDIS_LIST) {
      tx.commit();
      c->err = -1;
      snprintf(c->errmsg, sizeof(c->errmsg), 
          "WRONGTYPE Operation against a key holding the wrong kind of "
          "value");
      return 0;
    }
  }

  if (rootValue.size() == sizeof(struct ObjectMetadata)) {
    /* List exists but it's empty. */
    c->err = -1;
    snprintf(c->errmsg, sizeof(c->errmsg), 
        "List is empty");
    return NULL;
  }

  ListIndex index;
  uint64_t totalElements = 0;
  index.entries = static_cast<ListIndexEntry*>(
      rootValue.getRange(sizeof(struct ObjectMetadata), 
        rootValue.size() - sizeof(struct ObjectMetadata)));  
  index.len = (rootValue.size() - sizeof(struct ObjectMetadata)) 
      / sizeof(ListIndexEntry);

  for (int i = 0; i < index.len; i++) {
    totalElements += index.entries[i].elemCount;
  }

  RAMCloud::Buffer newRootValue;
  newRootValue.append((void*)objMtd, sizeof(struct ObjectMetadata));

  if (totalElements == 0) {
    /* List index has entries but no elements in the list. In this case, reset
     * the index to a default state, if needed. */
    if (index.len != 1 || index.entries[0].segId != 0) {
      ListIndexEntry entry;
      entry.segId = 0;
      entry.elemCount = 0;
      entry.segSizeKb = 0;

      newRootValue.append((void*)&entry, sizeof(ListIndexEntry));

      tx.write(c->tableId, 
          rootKey.getRange(0, rootKey.size()),
          rootKey.size(),
          newRootValue.getRange(0, newRootValue.size()),
          newRootValue.size());

      tx.commit();
    }

    c->err = -1;
    snprintf(c->errmsg, sizeof(c->errmsg), 
        "List is empty");
    return NULL;
  }

  /* At this point, we know that the list exists, the index has entries, and
   * the sum of elements in the segments is non-zero. We don't know where the
   * tail element is, however. The common case is that it's in the last
   * segment, and not the last element. Sometimes it will be the last element
   * there, and we need to remove that segment from the index. In rare cases,
   * the tail segments may be empty and need to be removed on the way to the
   * tail element. It may also be the case that after removing the last element
   * from a segment, the following segments are empty. In this case we take the
   * time to do some clean-up and remove those segments from the index. */

  for (int i = index.len - 1; i >= 0; i--) {
    if (index.entries[i].elemCount == 0) {
      // Skip over the leading segments that are empty.
      continue;
    } else {
      // First segment that has 1 or more elements.
      RAMCloud::Buffer segKey;

      segKey.append(&rootKey);
      appendKeyComponent(&segKey, (char*)&index.entries[i].segId, 
          sizeof(int16_t));

      RAMCloud::Buffer segValue;
      try {
        tx.read(c->tableId,
            segKey.getRange(0, segKey.size()), 
            segKey.size(), 
            &segValue);
      } catch (RAMCloud::ObjectDoesntExistException& e) {
        c->err = -1;
        snprintf(c->errmsg, sizeof(c->errmsg), 
            "List is corrupted.");
        return 0;
      }

      // Extract value from segment.
      uint16_t len = *static_cast<uint16_t*>(segValue.getRange(
            (index.entries[i].elemCount - 1) * sizeof(uint16_t), 
            sizeof(uint16_t)));
      Object* obj = (Object*)malloc(sizeof(Object));
      obj->data = (void*)malloc(len);
      obj->len = len;
      segValue.copy(segValue.size() - len, len, obj->data);

      if (index.entries[i].elemCount == 1) {
        /* This is the last element in the segment. In this case, remove the
         * segment from the list, as well as any following segments that are
         * empty. */
        if (totalElements == 1) {
          /* This segment contained the very last element in this list. The
           * list is now completely empty. In this case, reset the list to a
           * default state. We currently do this by resetting the index and
           * don't actually remove the segments that have a single object in
           * them from RAMCloud. They will eventually be overwriten if the list
           * is sufficiently filled again. */
          ListIndexEntry entry;
          entry.segId = 0;
          entry.elemCount = 0;
          entry.segSizeKb = 0;

          newRootValue.append((void*)&entry, sizeof(ListIndexEntry));

          tx.write(c->tableId, 
              rootKey.getRange(0, rootKey.size()),
              rootKey.size(),
              newRootValue.getRange(0, newRootValue.size()),
              newRootValue.size());

          tx.commit();
        } else {
          /* There are more elements in segments up the line. Find the next
           * non-empty segment to be the tail segment. */
          for (int j = i - 1; j >= 0; j--) {
            if (index.entries[j].elemCount > 0) {
              newRootValue.append(&index.entries[0], 
                  (j + 1) * sizeof(ListIndexEntry));

              tx.write(c->tableId,
                  rootKey.getRange(0, rootKey.size()),
                  rootKey.size(),
                  newRootValue.getRange(0, newRootValue.size()),
                  newRootValue.size());

              tx.commit();
              break;
            }
          }
        }
      } else {
        /* The element that we just popped was not the last element in the
         * list. In this case write the new segment value back and update the
         * index. */
        RAMCloud::Buffer newSegValue;
        newSegValue.append(&segValue, 0, 
            (index.entries[i].elemCount - 1) * sizeof(uint16_t));
        newSegValue.append(&segValue, 
            (index.entries[i].elemCount * sizeof(uint16_t)),
            segValue.size() - (index.entries[i].elemCount * sizeof(uint16_t))
            - len);

        tx.write(c->tableId,
            segKey.getRange(0, segKey.size()),
            segKey.size(),
            newSegValue.getRange(0, newSegValue.size()),
            newSegValue.size());

        index.entries[i].elemCount--;
        index.entries[i].segSizeKb = (uint8_t)(newSegValue.size() >> 10);

        newRootValue.append(&index.entries[0], 
            (i + 1)*sizeof(ListIndexEntry));

        tx.write(c->tableId,
            rootKey.getRange(0, rootKey.size()),
            rootKey.size(),
            newRootValue.getRange(0, newRootValue.size()),
            newRootValue.size());

        tx.commit();
      }
      
      return obj;
    }
  }
}

ObjectArray* lrange(Context* c, Object* key, long start, long end) {
  RAMCloud::RamCloud* client = (RAMCloud::RamCloud*)c->client;
  RAMCloud::Transaction tx(client);

  /* Construct RAMCloud key for the list index. */ 
  RAMCloud::Buffer rootKey;
  appendKeyComponent(&rootKey, (char*)key->data, key->len);

  /* Read the index. */
  RAMCloud::Buffer rootValue;
  bool objectExists = true;
  try {
    tx.read(c->tableId, 
        rootKey.getRange(0, rootKey.size()), 
        rootKey.size(), 
        &rootValue);
  } catch (RAMCloud::ObjectDoesntExistException& e) {
    objectExists = false;
  }

  /* Sanity checks:
   * 1) If the object exists it should have metadata.
   * 2) If the object exists it should be of the correct data type. */
  struct ObjectMetadata* objMtd = NULL;
  if (objectExists) {
    if (rootValue.size() < sizeof(struct ObjectMetadata)) {
      tx.commit();
      ERROR("Data structure malformed. This is a bug.\n");
      DEBUG("Object exists but is missing its metadata.\n");
      c->err = -1;
      snprintf(c->errmsg, sizeof(c->errmsg), 
          "Data structure malformed. This is a bug.");
      return 0;
    } else {
      objMtd = rootValue.getOffset<struct ObjectMetadata>(0);
      if (objMtd->type != REDIS_LIST) {
        tx.commit();
        c->err = -1;
        snprintf(c->errmsg, sizeof(c->errmsg), 
            "WRONGTYPE Operation against a key holding the wrong kind of "
            "value");
        return 0;
      }
    }
  }

  if (!objectExists) {
    tx.commit();

    c->err = -1;
    snprintf(c->errmsg, sizeof(c->errmsg), 
        "Unknown key");
    
    return NULL;
  } 
  
  ListIndex index;
  index.entries = NULL;
  index.len = 0;
  uint64_t totalElements = 0;
  if (rootValue.size() > sizeof(struct ObjectMetadata)) {
    index.entries = static_cast<ListIndexEntry*>(
        rootValue.getRange(sizeof(struct ObjectMetadata), 
          rootValue.size() - sizeof(struct ObjectMetadata)));  
    index.len = (rootValue.size() - sizeof(struct ObjectMetadata)) 
        / sizeof(ListIndexEntry);

    for (int i = 0; i < index.len; i++) {
      totalElements += index.entries[i].elemCount;
    }
  }
  
  if (index.len == 0) {
    tx.commit();

    ObjectArray* objArray = (ObjectArray*)malloc(sizeof(ObjectArray));
    objArray->array = NULL;
    objArray->len = 0;

    return objArray;
  } else {
    uint64_t rangeStart, rangeEnd;
    if (start < 0) {
      if (totalElements + start < 0) {
        rangeStart = 0;
      } else {
        rangeStart = (uint64_t)(totalElements + start);
      }
    } else {
      rangeStart = (uint64_t)start;
    }

    if (end < 0) {
      if (totalElements + end < 0) {
        rangeEnd = 0;
      } else {
        rangeEnd = (uint64_t)(totalElements + end);
      }
    } else {
      rangeEnd = (uint64_t)end;
    }

    if (rangeEnd < rangeStart) {
      rangeEnd = rangeStart;
    }

    ObjectArray* objArray = (ObjectArray*)malloc(sizeof(ObjectArray));
    objArray->len = (rangeEnd - rangeStart + 1);
    objArray->array = (Object*)malloc(
        sizeof(Object)*(objArray->len));

    /* Count the number of list segments that are straddled by this range. */
    uint64_t elementStartIndex = 0;
    uint64_t elementsPriorToSegRange = 0;
    uint32_t segmentsInRange = 0;
    uint32_t firstSegInRange = 0;
    for (int i = 0; i < index.len; i++) {
      /* Current segment contains elements in the range:
       * [elementIndex, elementIndex + index.entries[i].elemCount - 1]
       * inclusive. */
      uint64_t elementEndIndex = 
          elementStartIndex + index.entries[i].elemCount - 1;
      if (elementEndIndex < rangeStart) {
        firstSegInRange++;
        elementsPriorToSegRange += index.entries[i].elemCount;
      } else if (rangeStart <= elementEndIndex && 
          elementStartIndex <= rangeEnd) {
        segmentsInRange++;
      }

      elementStartIndex += index.entries[i].elemCount;
    }

    RAMCloud::Tub<RAMCloud::Transaction::ReadOp> readOps[segmentsInRange];
    RAMCloud::Buffer segValues[segmentsInRange];

    /* Read segments asynchronously. */
    for (int i = 0; i < segmentsInRange; i++) {
      uint32_t segIndex = firstSegInRange + i;
      RAMCloud::Buffer segKey;
      segKey.append(&rootKey);
      appendKeyComponent(&segKey, (char*)&index.entries[segIndex].segId, 
          sizeof(int16_t));
      readOps[i].construct(&tx, c->tableId, segKey.getRange(0,
            segKey.size()), segKey.size(), &segValues[i], true);
    }

    uint64_t elementIndex = elementsPriorToSegRange;
    RAMCloud::Buffer rangeBuf;
    for (int i = 0; i < segmentsInRange; i++) {
      uint32_t segIndex = firstSegInRange + i;
      readOps[i].get()->wait();

      // Determine index range within this segment (a slice).
      uint32_t sliceStart, sliceEnd;
      if (rangeStart <= elementIndex) {
        sliceStart = 0;
      } else {
        sliceStart = rangeStart - elementIndex;
      }

      if ((elementIndex + index.entries[segIndex].elemCount - 1) <= rangeEnd)
      {
        sliceEnd = index.entries[segIndex].elemCount - 1;
      } else {
        sliceEnd = rangeEnd - elementIndex;
      }

      uint16_t* valLengthArray = static_cast<uint16_t*>(segValues[i].getRange(
            0, index.entries[segIndex].elemCount * sizeof(uint16_t)));

      uint32_t sliceByteOffset = 
          index.entries[segIndex].elemCount * sizeof(uint16_t);
      for (int j = 0; j < sliceStart; j++) {
        sliceByteOffset += valLengthArray[j];
      }

      uint32_t sliceLength = 0;
      for (int j = sliceStart; j <= sliceEnd; j++) {
        sliceLength += valLengthArray[j];
        objArray->array[elementIndex + j - rangeStart].len = valLengthArray[j]; 
      }

      rangeBuf.append(&segValues[i], sliceByteOffset, sliceLength);

      elementIndex += index.entries[segIndex].elemCount;
    }

    tx.commit();

    char* objectData = (char*)malloc(rangeBuf.size());
    rangeBuf.copy(0, rangeBuf.size(), objectData);

    uint32_t offset = 0;
    for (int i = 0; i < objArray-> len; i++) {
      objArray->array[i].data = (void*)(objectData + offset);
      offset += objArray->array[i].len;
    }

    return objArray;
  }
}

uint64_t sadd(Context* c, Object* key, ObjectArray* values) {
  return 0;
}

Object* spop(Context* c, Object* key) {
  return NULL;
}

uint64_t del(Context* c, ObjectArray* keysArray) {
  RAMCloud::RamCloud* client = (RAMCloud::RamCloud*)c->client;
  RAMCloud::Transaction tx(client);
 
  uint64_t delCount = 0; 
  for (int i = 0; i < keysArray->len; i++) {
    try {
      tx.remove(c->tableId, 
          keysArray->array[i].data, 
          keysArray->array[i].len);

      delCount++;
    } catch (RAMCloud::ObjectDoesntExistException& e) {
      continue;
    }
  }

  tx.commit();

  return delCount;
}

void freeObject(Object* obj) {
  free(obj->data);
  free(obj);
}

void freeObjectArray(ObjectArray* objArray) {
  if (objArray->len > 0) {
    free((char*)(objArray->array[0].data));
    free(objArray->array);
  }
  free(objArray);
}
