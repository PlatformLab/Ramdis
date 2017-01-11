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

char* ping(Context* c, char* msg) {
  return NULL;
}

void set(Context* c, Object* key, Object* value) {
  RAMCloud::RamCloud* client = (RAMCloud::RamCloud*)c->client;
  client->write(c->tableId, key->data, key->len, value->data, value->len);
}

Object* get(Context* c, Object* key) {
  RAMCloud::RamCloud* client = (RAMCloud::RamCloud*)c->client;
  RAMCloud::Buffer buffer;
  try {
    client->read(c->tableId, key->data, key->len, &buffer);
    Object* value = (Object*)malloc(sizeof(Object));
    value->data = (void*)malloc(buffer.size());
    value->len = buffer.size();
    buffer.copy(0, value->len, value->data);
    
    return value;
  } catch (RAMCloud::ObjectDoesntExistException& e) {
    c->err = -1;
    snprintf(c->errmsg, sizeof(c->errmsg), 
        "Unknown key");
    return NULL;
  }
}

long incr(Context* c, Object* key) {
  RAMCloud::RamCloud* client = (RAMCloud::RamCloud*)c->client;
  try {
    uint64_t newValue = client->incrementInt64(c->tableId, 
        key->data,
        key->len,
        1);
    return (long)newValue;
  } catch (RAMCloud::ObjectDoesntExistException& e) {
    c->err = -1;
    snprintf(c->errmsg, sizeof(c->errmsg), 
        "Unknown key");
    return -1;
  }
}

void makeKey(RAMCloud::Buffer* buf, const char* key, uint16_t keyLen, 
    const char* suffix, uint8_t suffixLen) {
  buf->append((void*)&keyLen, sizeof(uint16_t));
  buf->append(key, keyLen);
  buf->append((void*)&suffixLen, sizeof(uint8_t));
  buf->append(suffix, suffixLen);
}

uint64_t lpush(Context* c, Object* key, Object* value) {
  RAMCloud::RamCloud* client = (RAMCloud::RamCloud*)c->client;
  RAMCloud::Transaction tx(client);

  /* Construct RAMCloud key for the list index. */ 
  RAMCloud::Buffer indexKey;
  makeKey(&indexKey, (char*)key->data, key->len, "idx", strlen("idx") + 1);

  /* Read the index. */
  RAMCloud::Buffer indexValue;
  bool listExists = true;
  try {
    tx.read(c->tableId, 
        indexKey.getRange(0, indexKey.size()), 
        indexKey.size(), 
        &indexValue);
  } catch (RAMCloud::ObjectDoesntExistException& e) {
    listExists = false;
  }

  ListIndex index;
  bool headSegFull = false;
  uint64_t totalElements = 0;
  if (listExists && indexValue.size() != 0) {
    index.entries = static_cast<ListIndexEntry*>(
        indexValue.getRange(0, indexValue.size()));  
    index.len = indexValue.size() / sizeof(ListIndexEntry);

    if (index.entries[0].segSizeKb >= MAX_LIST_SEG_SIZE_KB) {
      headSegFull = true;
    } 

    for (int i = 0; i < index.len; i++) {
      totalElements += index.entries[i].elemCount;
    }
  }


  RAMCloud::Buffer segKey;
  RAMCloud::Buffer newSegValue;
  RAMCloud::Buffer newIndexValue;
  if (!listExists || indexValue.size() == 0 || headSegFull) {
    /* If the list doesn't exist, or the index is empty, or the head segment is
     * full, then create a new head segment. */
    int16_t newSegId;
    if (!listExists || indexValue.size() == 0) {
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
    
    char suffix[8];
    sprintf(suffix, "%d", newSegId);
    makeKey(&segKey, (char*)key->data, key->len, suffix, strlen(suffix) + 1);

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

    newIndexValue.append((void*)&entry, sizeof(ListIndexEntry));
    if (listExists && indexValue.size() != 0) {
      newIndexValue.append(&indexValue);
    }
  } else if (index.entries[0].elemCount == 0) {
    /* The list exists, the index is not empty, but the head segment is empty.
     * In this case we don't need to read the head segment to add the new
     * value, we can just write the new head segment directly for lower
     * latency. */
    char suffix[8];
    sprintf(suffix, "%d", index.entries[0].segId);
    makeKey(&segKey, (char*)key->data, key->len, suffix, strlen(suffix) + 1);

    uint16_t valueLen = (uint16_t)value->len;
    newSegValue.append((void*)&valueLen, sizeof(uint16_t));
    newSegValue.append(value->data, valueLen);

    index.entries[0].elemCount = 1;
    index.entries[0].segSizeKb = (uint8_t)(newSegValue.size() >> 10);

    newIndexValue.append(index.entries, index.len*sizeof(ListIndexEntry));
  } else {
    /* The list exists, the index is not empty, and the head segment is neither
     * full nor totally empty. In this case we need to read the head segment
     * and add the new value to it. */
    char suffix[8];
    sprintf(suffix, "%d", index.entries[0].segId);
    makeKey(&segKey, (char*)key->data, key->len, suffix, strlen(suffix) + 1);

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

    newIndexValue.append(index.entries, index.len*sizeof(ListIndexEntry));
  }

  tx.write(c->tableId,
      segKey.getRange(0, segKey.size()),
      segKey.size(),
      newSegValue.getRange(0, newSegValue.size()),
      newSegValue.size());

  tx.write(c->tableId, 
      indexKey.getRange(0, indexKey.size()), 
      indexKey.size(), 
      newIndexValue.getRange(0, newIndexValue.size()),
      newIndexValue.size());

  tx.commit();

  return totalElements + 1;
}

uint64_t rpush(Context* c, Object* key, Object* value) {
  RAMCloud::RamCloud* client = (RAMCloud::RamCloud*)c->client;
  RAMCloud::Transaction tx(client);

  /* Construct RAMCloud key for the list index. */ 
  RAMCloud::Buffer indexKey;
  makeKey(&indexKey, (char*)key->data, key->len, "idx", strlen("idx") + 1);

  /* Read the index. */
  RAMCloud::Buffer indexValue;
  bool listExists = true;
  try {
    tx.read(c->tableId, 
        indexKey.getRange(0, indexKey.size()), 
        indexKey.size(), 
        &indexValue);
  } catch (RAMCloud::ObjectDoesntExistException& e) {
    listExists = false;
  }

  ListIndex index;
  bool tailSegFull = false;
  uint64_t totalElements = 0;
  if (listExists && indexValue.size() != 0) {
    index.entries = static_cast<ListIndexEntry*>(
        indexValue.getRange(0, indexValue.size()));  
    index.len = indexValue.size() / sizeof(ListIndexEntry);

    if (index.entries[index.len - 1].segSizeKb >= MAX_LIST_SEG_SIZE_KB) {
      tailSegFull = true;
    } 

    for (int i = 0; i < index.len; i++) {
      totalElements += index.entries[i].elemCount;
    }
  }

  RAMCloud::Buffer segKey;
  RAMCloud::Buffer newSegValue;
  RAMCloud::Buffer newIndexValue;
  if (!listExists || indexValue.size() == 0 || tailSegFull) {
    /* If the list doesn't exist, or the index is empty, or the tail segment is
     * full, then create a new head segment. */
    int16_t newSegId;
    if (!listExists || indexValue.size() == 0) {
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
    
    char suffix[8];
    sprintf(suffix, "%d", newSegId);
    makeKey(&segKey, (char*)key->data, key->len, suffix, strlen(suffix) + 1);

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

    if (listExists && indexValue.size() != 0) {
      newIndexValue.append(&indexValue);
    }
    newIndexValue.append((void*)&entry, sizeof(ListIndexEntry));
  } else if (index.entries[index.len - 1].elemCount == 0) {
    /* The list exists, the index is not empty, but the tail segment is empty.
     * In this case we don't need to read the tail segment to add the new
     * value, we can just write the new tail segment directly for lower
     * latency. */
    char suffix[8];
    sprintf(suffix, "%d", index.entries[index.len - 1].segId);
    makeKey(&segKey, (char*)key->data, key->len, suffix, strlen(suffix) + 1);

    uint16_t valueLen = (uint16_t)value->len;
    newSegValue.append((void*)&valueLen, sizeof(uint16_t));
    newSegValue.append(value->data, valueLen);

    index.entries[index.len - 1].elemCount = 1;
    index.entries[index.len - 1].segSizeKb = 
        (uint8_t)(newSegValue.size() >> 10);

    newIndexValue.append(index.entries, index.len*sizeof(ListIndexEntry));
  } else {
    /* The list exists, the index is not empty, and the tail segment is neither
     * full nor totally empty. In this case we need to read the tail segment
     * and add the new value to it. */
    char suffix[8];
    sprintf(suffix, "%d", index.entries[index.len - 1].segId);
    makeKey(&segKey, (char*)key->data, key->len, suffix, strlen(suffix) + 1);

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

    newIndexValue.append(index.entries, index.len*sizeof(ListIndexEntry));
  }

  tx.write(c->tableId,
      segKey.getRange(0, segKey.size()),
      segKey.size(),
      newSegValue.getRange(0, newSegValue.size()),
      newSegValue.size());

  tx.write(c->tableId, 
      indexKey.getRange(0, indexKey.size()), 
      indexKey.size(), 
      newIndexValue.getRange(0, newIndexValue.size()),
      newIndexValue.size());

  tx.commit();

  return totalElements + 1;
}

Object* lpop(Context* c, Object* key) {
  RAMCloud::RamCloud* client = (RAMCloud::RamCloud*)c->client;
  RAMCloud::Transaction tx(client);

  /* Construct RAMCloud key for the list index. */ 
  RAMCloud::Buffer indexKey;
  makeKey(&indexKey, (char*)key->data, key->len, "idx", strlen("idx") + 1);

  /* Read the index. */
  RAMCloud::Buffer indexValue;
  try {
    tx.read(c->tableId, 
        indexKey.getRange(0, indexKey.size()), 
        indexKey.size(), 
        &indexValue);
  } catch (RAMCloud::ObjectDoesntExistException& e) {
    c->err = -1;
    snprintf(c->errmsg, sizeof(c->errmsg), 
        "Unknown key");
    return NULL;
  }

  if (indexValue.size() == 0) {
    /* List exists but it's empty. */
    c->err = -1;
    snprintf(c->errmsg, sizeof(c->errmsg), 
        "List is empty");
    return NULL;
  }

  ListIndex index;
  uint64_t totalElements = 0;
  index.entries = static_cast<ListIndexEntry*>(
      indexValue.getRange(0, indexValue.size()));  
  index.len = indexValue.size() / sizeof(ListIndexEntry);

  for (int i = 0; i < index.len; i++) {
    totalElements += index.entries[i].elemCount;
  }

  if (totalElements == 0) {
    /* List index has entries but no elements in the list. In this case, reset
     * the index to a default state, if needed. */
    if (index.len != 1 || index.entries[0].segId != 0) {
      ListIndexEntry entry;
      entry.segId = 0;
      entry.elemCount = 0;
      entry.segSizeKb = 0;

      tx.write(c->tableId, 
          indexKey.getRange(0, indexKey.size()),
          indexKey.size(),
          &entry,
          sizeof(ListIndexEntry));

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
      char suffix[8];
      sprintf(suffix, "%d", index.entries[i].segId);
      makeKey(&segKey, (char*)key->data, key->len, suffix, strlen(suffix) + 1);

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

          tx.write(c->tableId, 
              indexKey.getRange(0, indexKey.size()),
              indexKey.size(),
              &entry,
              sizeof(ListIndexEntry));

          tx.commit();
        } else {
          /* There are more elements in segments down the line. Find the next
           * non-empty segment to be the head segment. */
          for (int j = i + 1; j < index.len; j++) {
            if (index.entries[j].elemCount > 0) {
              tx.write(c->tableId,
                  indexKey.getRange(0, indexKey.size()),
                  indexKey.size(),
                  &index.entries[j],
                  (index.len - j)*sizeof(ListIndexEntry));

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

        tx.write(c->tableId,
            indexKey.getRange(0, indexKey.size()),
            indexKey.size(),
            &index.entries[i],
            (index.len - i)*sizeof(ListIndexEntry));

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
  RAMCloud::Buffer indexKey;
  makeKey(&indexKey, (char*)key->data, key->len, "idx", strlen("idx") + 1);

  /* Read the index. */
  RAMCloud::Buffer indexValue;
  try {
    tx.read(c->tableId, 
        indexKey.getRange(0, indexKey.size()), 
        indexKey.size(), 
        &indexValue);
  } catch (RAMCloud::ObjectDoesntExistException& e) {
    c->err = -1;
    snprintf(c->errmsg, sizeof(c->errmsg), 
        "Unknown key");
    return NULL;
  }

  if (indexValue.size() == 0) {
    /* List exists but it's empty. */
    c->err = -1;
    snprintf(c->errmsg, sizeof(c->errmsg), 
        "List is empty");
    return NULL;
  }

  ListIndex index;
  uint64_t totalElements = 0;
  index.entries = static_cast<ListIndexEntry*>(
      indexValue.getRange(0, indexValue.size()));  
  index.len = indexValue.size() / sizeof(ListIndexEntry);

  for (int i = 0; i < index.len; i++) {
    totalElements += index.entries[i].elemCount;
  }

  if (totalElements == 0) {
    /* List index has entries but no elements in the list. In this case, reset
     * the index to a default state, if needed. */
    if (index.len != 1 || index.entries[0].segId != 0) {
      ListIndexEntry entry;
      entry.segId = 0;
      entry.elemCount = 0;
      entry.segSizeKb = 0;

      tx.write(c->tableId, 
          indexKey.getRange(0, indexKey.size()),
          indexKey.size(),
          &entry,
          sizeof(ListIndexEntry));

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
      char suffix[8];
      sprintf(suffix, "%d", index.entries[i].segId);
      makeKey(&segKey, (char*)key->data, key->len, suffix, strlen(suffix) + 1);

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

          tx.write(c->tableId, 
              indexKey.getRange(0, indexKey.size()),
              indexKey.size(),
              &entry,
              sizeof(ListIndexEntry));

          tx.commit();
        } else {
          /* There are more elements in segments up the line. Find the next
           * non-empty segment to be the tail segment. */
          for (int j = i - 1; j >= 0; j--) {
            if (index.entries[j].elemCount > 0) {
              tx.write(c->tableId,
                  indexKey.getRange(0, indexKey.size()),
                  indexKey.size(),
                  &index.entries[0],
                  (j + 1)*sizeof(ListIndexEntry));

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

        tx.write(c->tableId,
            indexKey.getRange(0, indexKey.size()),
            indexKey.size(),
            &index.entries[0],
            (i + 1)*sizeof(ListIndexEntry));

        tx.commit();
      }
      
      return obj;
    }
  }
}

uint64_t sadd(Context* c, Object* key, ObjectArray* values) {
  return 0;
}

Object* spop(Context* c, Object* key) {
  return NULL;
}

ObjectArray* lrange(Context* c, Object* key, long start, long end) {
  RAMCloud::RamCloud* client = (RAMCloud::RamCloud*)c->client;
  RAMCloud::Transaction tx(client);

  /* Construct RAMCloud key for the list index. */ 
  RAMCloud::Buffer indexKey;
  makeKey(&indexKey, (char*)key->data, key->len, "idx", strlen("idx") + 1);

  /* Read the index. */
  RAMCloud::Buffer indexValue;
  bool listExists = true;
  try {
    tx.read(c->tableId, 
        indexKey.getRange(0, indexKey.size()), 
        indexKey.size(), 
        &indexValue);
  } catch (RAMCloud::ObjectDoesntExistException& e) {
    listExists = false;
  }

  if (!listExists) {
    c->err = -1;
    snprintf(c->errmsg, sizeof(c->errmsg), 
        "Unknown key");
    return NULL;
  } else if (indexValue.size() == 0) {
    ObjectArray* objArray = (ObjectArray*)malloc(sizeof(ObjectArray));
    objArray->array = NULL;
    objArray->len = 0;

    return objArray;
  } else {
    ListIndex index;
    index.entries = static_cast<ListIndexEntry*>(
        indexValue.getRange(0, indexValue.size()));  
    index.len = indexValue.size() / sizeof(ListIndexEntry);

    long totalElements = 0;
    for (int i = 0; i < index.len; i++) {
      totalElements += index.entries[i].elemCount;
    }

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
      char suffix[8];
      sprintf(suffix, "%d", index.entries[segIndex].segId);
      makeKey(&segKey, (char*)key->data, key->len, suffix, 
          strlen(suffix) + 1);
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

    // TODO: commit the transaction

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

void mset(Context* c, ObjectArray* keysArray, Object* valuesArray) {

}

//std::string unsupportedCommand(RAMCloud::RamCloud *client,
//    uint64_t tableId,
//    std::vector<std::string> *argv) {
//  std::string res("+Unsupported command.\r\n");
//  return res;
//}
//
//std::string getCommand(RAMCloud::RamCloud *client,
//    uint64_t tableId,
//    std::vector<std::string> *argv) {
//  RAMCloud::Buffer buffer;
//  try {
//    client->read(tableId, (*argv)[1].c_str(),
//        (*argv)[1].length(), &buffer);
//    std::stringstream ss;
//    ss << "$" << buffer.size();
//    const char* data = static_cast<const char*>(buffer.getRange(0,
//            buffer.size()));
//    ss.write(data, buffer.size());
//    ss << "\r\n";
//    return ss.str();
//  } catch (RAMCloud::ObjectDoesntExistException& e) {
//    std::string res("+Unknown key.\r\n");
//    return res;
//  }
//}
//
//std::string incrCommand(RAMCloud::RamCloud *client,
//    uint64_t tableId,
//    std::vector<std::string> *argv) {
//  try {
//    uint64_t newValue = client->incrementInt64(tableId, (*argv)[1].c_str(),
//        (*argv)[1].length(), 1);
//    std::stringstream ss;
//    ss << ":" << newValue;
//    ss << "\r\n";
//    return ss.str();
//  } catch (RAMCloud::ObjectDoesntExistException& e) {
//    std::string res("+Unknown key.\r\n");
//    return res;
//  }
//}
//
//std::string setCommand(RAMCloud::RamCloud *client,
//    uint64_t tableId,
//    std::vector<std::string> *argv) {
//  client->write(tableId, (*argv)[1].c_str(),
//      (*argv)[1].length(),
//      (*argv)[2].c_str());
//  return std::string("+OK\r\n");
//}
//
//std::string lpushCommand(RAMCloud::RamCloud *client,
//    uint64_t tableId,
//    std::vector<std::string> *argv) {
//  
//  // Arg validation.
//  if ((*argv)[2].length() >= (1 << (sizeof(uint16_t)*8))) {
//    std::string res("+List element must be less than 64KB in size.\r\n");
//    return res;
//  }
//
//  // Read out old list, if it exists.
//  RAMCloud::Buffer buffer;
//  bool listExists = true;
//  try {
//    client->read(tableId, (*argv)[1].c_str(),
//        (*argv)[1].length(), &buffer);
//  } catch (RAMCloud::ObjectDoesntExistException& e) {
//    listExists = false;
//  }
//
//  // Append new element to list.
//  size_t newListSize;
//  uint16_t elementLength = (uint16_t)(*argv)[2].length();
//  if (listExists) {
//    newListSize = sizeof(uint16_t) + elementLength + buffer.size();
//  } else {
//    newListSize = sizeof(uint16_t) + elementLength;
//  }
//
//  char* newList = (char*)malloc(newListSize);
//  memcpy(newList, &elementLength, sizeof(uint16_t));
//  memcpy(newList + sizeof(uint16_t), (*argv)[2].c_str(), elementLength);
//
//  if (listExists) {
//    const char* oldList = static_cast<const char*>(buffer.getRange(0,
//            buffer.size()));
//    memcpy(newList + sizeof(uint16_t) + elementLength, oldList, buffer.size());
//  }
//
//  // Write new list.
//  client->write(tableId, 
//      (*argv)[1].c_str(),
//      (*argv)[1].length(),
//      newList, 
//      newListSize);
//
//  // Count number of elements in the new list.
//  uint32_t pos = 0;
//  uint32_t count = 0;
//  while (pos < newListSize) {
//    count++;
//    uint16_t len = *(uint16_t*)(newList + pos);
//    pos += sizeof(uint16_t) + len;
//  }
//
//  free(newList);
//  
//  std::ostringstream oss;
//  oss << ":" << count << "\r\n";
//
//  return oss.str();
//}
//
//std::string rpushCommand(RAMCloud::RamCloud *client,
//    uint64_t tableId,
//    std::vector<std::string> *argv) {
//  
//  // Arg validation.
//  if ((*argv)[2].length() >= (1 << (sizeof(uint16_t)*8))) {
//    std::string res("+List element must be less than 64KB in size.\r\n");
//    return res;
//  }
//
//  // Read out old list, if it exists.
//  RAMCloud::Buffer buffer;
//  bool listExists = true;
//  try {
//    client->read(tableId, (*argv)[1].c_str(),
//        (*argv)[1].length(), &buffer);
//  } catch (RAMCloud::ObjectDoesntExistException& e) {
//    listExists = false;
//  }
//
//  // Append new element to list.
//  size_t newListSize;
//  uint16_t elementLength = (uint16_t)(*argv)[2].length();
//  char* newList;
//  if (listExists) {
//    newListSize = sizeof(uint16_t) + elementLength + buffer.size();
//    newList = (char*)malloc(newListSize);
//    const char* oldList = static_cast<const char*>(buffer.getRange(0,
//            buffer.size()));
//    memcpy(newList, oldList, buffer.size());
//    memcpy(newList + buffer.size(), &elementLength, sizeof(uint16_t));
//    memcpy(newList + buffer.size() + sizeof(uint16_t), (*argv)[2].c_str(), 
//        elementLength);
//  } else {
//    newListSize = sizeof(uint16_t) + elementLength;
//    newList = (char*)malloc(newListSize);
//    memcpy(newList, &elementLength, sizeof(uint16_t));
//    memcpy(newList + sizeof(uint16_t), (*argv)[2].c_str(), elementLength);
//  }
//
//  // Write new list.
//  client->write(tableId, 
//      (*argv)[1].c_str(),
//      (*argv)[1].length(),
//      newList, 
//      newListSize);
//
//  // Count number of elements in the new list.
//  uint32_t pos = 0;
//  uint32_t count = 0;
//  while (pos < newListSize) {
//    count++;
//    uint16_t len = *(uint16_t*)(newList + pos);
//    pos += sizeof(uint16_t) + len;
//  }
//  
//  free(newList);
//
//  std::ostringstream oss;
//  oss << ":" << count << "\r\n";
//
//  return oss.str();
//}
//
//std::string lpopCommand(RAMCloud::RamCloud *client,
//    uint64_t tableId,
//    std::vector<std::string> *argv) {
//  
//  // Read out list.
//  RAMCloud::Buffer buffer;
//  try {
//    client->read(tableId, (*argv)[1].c_str(),
//        (*argv)[1].length(), &buffer);
//  } catch (RAMCloud::ObjectDoesntExistException& e) {
//    return std::string("+Unknown key.");
//  }
//
//  if (buffer.size() == 0) {
//    return std::string("$-1\r\n");
//  }
//
//  const char* list = static_cast<const char*>(buffer.getRange(0, 
//        buffer.size()));
//  uint16_t len = *(uint16_t*)(list);
//  std::string element(list + sizeof(uint16_t), len);
//
//  size_t newListSize = buffer.size() - sizeof(uint16_t) - len;
//  char* newList = (char*)malloc(newListSize);
//  memcpy(newList, list + sizeof(uint16_t) + len, newListSize);
//  
//
//  // Write new list.
//  client->write(tableId, 
//      (*argv)[1].c_str(),
//      (*argv)[1].length(),
//      newList, 
//      newListSize);
//
//  free(newList);
//
//  std::ostringstream oss;
//  oss << "+" << element << "\r\n";
//
//  return oss.str();
//}
//
//std::string rpopCommand(RAMCloud::RamCloud *client,
//    uint64_t tableId,
//    std::vector<std::string> *argv) {
//  
//  // Read out list.
//  RAMCloud::Buffer buffer;
//  try {
//    client->read(tableId, (*argv)[1].c_str(),
//        (*argv)[1].length(), &buffer);
//  } catch (RAMCloud::ObjectDoesntExistException& e) {
//    return std::string("+Unknown key.");
//  }
//
//  if (buffer.size() == 0) {
//    return std::string("$-1\r\n");
//  }
//
//  const char* list = static_cast<const char*>(buffer.getRange(0, 
//        buffer.size()));
//
//  // Find last element of the list.
//  uint32_t pos = 0;
//  uint16_t len;
//  while (true) {
//    len = *(uint16_t*)(list + pos);
//    if (pos + sizeof(uint16_t) + len == buffer.size()) {
//      break;
//    }
//    pos += sizeof(uint16_t) + len;
//  }
//
//  std::string element(list + pos + sizeof(uint16_t), len);
//
//  size_t newListSize = buffer.size() - sizeof(uint16_t) - len;
//  char* newList = (char*)malloc(newListSize);
//  memcpy(newList, list, newListSize);
//
//  // Write new list.
//  client->write(tableId, 
//      (*argv)[1].c_str(),
//      (*argv)[1].length(),
//      newList, 
//      newListSize);
//
//  free(newList);
//
//  std::ostringstream oss;
//  oss << "+" << element << "\r\n";
//
//  return oss.str();
//}
//
//std::string lrangeCommand(RAMCloud::RamCloud *client,
//    uint64_t tableId,
//    std::vector<std::string> *argv) {
//
//  int start = atoi((*argv)[2].c_str());  
//  int end = atoi((*argv)[3].c_str());
//
//  // Read out old list, if it exists.
//  RAMCloud::Buffer buffer;
//  try {
//    client->read(tableId, (*argv)[1].c_str(),
//        (*argv)[1].length(), &buffer);
//  } catch (RAMCloud::ObjectDoesntExistException& e) {
//    std::string res("+Unknown key.\r\n");
//    return res;
//  }
//
//  const char* list = static_cast<const char*>(buffer.getRange(0, 
//        buffer.size()));
//
//  // Parse the elements.
//  uint32_t pos = 0;
//  std::vector<std::string> elements;
//  while (pos < buffer.size()) {
//    uint16_t len = *(uint16_t*)(list + pos);
//    pos += sizeof(uint16_t);
//    elements.emplace_back(list + pos, len);
//    pos += len;
//  }
//
//  uint32_t startIndex;
//  if (start < 0) {
//    startIndex = elements.size() + start;
//  } else {
//    startIndex = start;
//  }
//
//  uint32_t endIndex;
//  if (end < 0) {
//    endIndex = elements.size() + end;
//  } else {
//    if (end >= elements.size()) {
//      endIndex = elements.size() - 1;
//    } else {
//      endIndex = end;
//    }
//  }
//
//  // Generate return message.
//  std::ostringstream oss;
//  uint32_t rangeSize = endIndex - startIndex + 1;
//  oss << "*" << rangeSize << "\r\n";
//
//  uint32_t count = 0;
//  for(auto const& e: elements) {
//    if (count >= start && count <= end) {
//      oss << "$" << e.length() << "\r\n" << e << "\r\n";
//    }
//    count++;
//  }
//
//  return oss.str();
//}

