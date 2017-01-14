#include <limits.h>
#include <gtest/gtest.h>
#include "ramdis.h"

// Global variable for coordinator locator string.
char* coordinatorLocator = NULL;

// Tests GET and SET commands.
TEST(GetSetTest, readWrite) {
  Context* context = ramdis_connect(coordinatorLocator); 

  Object key;
  key.data = (void*)"Robert Tyre Jones Jr.";
  key.len = strlen((char*)key.data) + 1;

  Object value;
  value.data = (void*)"Birthday: 1902/03/17, Height: 5'8\", Weight: 165lb";
  value.len = strlen((char*)value.data) + 1;

  set(context, &key, &value);

  Object* obj = get(context, &key);

  EXPECT_EQ(value.len, obj->len);
  EXPECT_STREQ((char*)value.data, (char*)obj->data);

  freeObject(obj);

  ObjectArray keysArray;
  keysArray.array = &key;
  keysArray.len = 1;

  del(context, &keysArray);

  ramdis_disconnect(context);
}

TEST(LpushTest, pushManyValues) {
  Context* context = ramdis_connect(coordinatorLocator); 

  /* Number of elements to put in the list. */
  uint32_t totalElements = (1<<13);
  /* Size of each element in bytes. */
  size_t elementSize = 8;

  Object key;
  key.data = (void*)"mylist";
  key.len = strlen((char*)key.data) + 1;

  Object value;
  char valBuf[elementSize];
  value.data = (void*)valBuf;
  value.len = elementSize;

  uint32_t elemCount;
  for (uint32_t i = 0; i < totalElements; i++) {
    sprintf(valBuf, "%07d", i);
    elemCount = lpush(context, &key, &value);

    EXPECT_EQ(0, context->err);
    EXPECT_EQ(i + 1, elemCount);
  }

  ObjectArray* objArray;
  objArray = lrange(context, &key, 0, -1);

  EXPECT_EQ(totalElements, objArray->len);

  for (uint32_t i = 0; i < totalElements; i++) {
    sprintf(valBuf, "%07d", totalElements - i - 1);
    EXPECT_STREQ(valBuf, (char*)objArray->array[i].data);
  }

  freeObjectArray(objArray);

  ObjectArray keysArray;
  keysArray.array = &key;
  keysArray.len = 1;

  del(context, &keysArray);

  ramdis_disconnect(context);
}

TEST(RpushTest, pushManyValues) {
  Context* context = ramdis_connect(coordinatorLocator); 

  /* Number of elements to put in the list. */
  uint32_t totalElements = (1<<13);
  /* Size of each element in bytes. */
  size_t elementSize = 8;

  Object key;
  key.data = (void*)"mylist";
  key.len = strlen((char*)key.data) + 1;

  Object value;
  char valBuf[elementSize];
  value.data = (void*)valBuf;
  value.len = elementSize;

  uint32_t elemCount;
  for (uint32_t i = 0; i < totalElements; i++) {
    sprintf(valBuf, "%07d", i);
    elemCount = rpush(context, &key, &value);

    EXPECT_EQ(0, context->err);
    EXPECT_EQ(i + 1, elemCount);
  }

  ObjectArray* objArray;
  objArray = lrange(context, &key, 0, -1);

  EXPECT_EQ(totalElements, objArray->len);

  for (uint32_t i = 0; i < totalElements; i++) {
    sprintf(valBuf, "%07d", i);
    EXPECT_STREQ(valBuf, (char*)objArray->array[i].data);
  }

  freeObjectArray(objArray);

  ObjectArray keysArray;
  keysArray.array = &key;
  keysArray.len = 1;

  del(context, &keysArray);

  ramdis_disconnect(context);
}

// Tests DEL command.
TEST(DelTest, deleteSingleObject) {
  Context* context = ramdis_connect(coordinatorLocator); 

  Object key;
  key.data = (void*)"Robert Tyre Jones Jr.";
  key.len = strlen((char*)key.data) + 1;

  Object value;
  value.data = (void*)"Birthday: 1902/03/17, Height: 5'8\", Weight: 165lb";
  value.len = strlen((char*)value.data) + 1;

  set(context, &key, &value);

  ObjectArray keysArray;
  keysArray.array = &key;
  keysArray.len = 1;

  uint64_t n = del(context, &keysArray);

  EXPECT_EQ(n, 1);

  Object *obj = get(context, &key);

  EXPECT_TRUE(obj == NULL);

  ramdis_disconnect(context);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  
  for (int i = 0; i < argc; i++) {
    if (strcmp(argv[i], "-C") == 0) {
      coordinatorLocator = argv[i+1];
      break;
    }
  }

  if (coordinatorLocator == NULL) {
    printf("ERROR: Required -C argument missing for coordinator locator "
        "string.\n");
    return -1;
  }

  return RUN_ALL_TESTS();
}
