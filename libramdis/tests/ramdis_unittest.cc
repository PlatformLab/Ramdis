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

// Tests INCR command.
TEST(IncrTest, correctValues) {
  Context* context = ramdis_connect(coordinatorLocator); 

  Object key;
  key.data = (void*)"counter";
  key.len = strlen((char*)key.data) + 1;

  uint64_t x = 0;
  Object value;
  value.data = (void*)&x;
  value.len = sizeof(uint64_t);

  set(context, &key, &value);

  uint64_t counterValue;
  for (int i = 0; i < 1000; i++) {
    counterValue = incr(context, &key);
    EXPECT_EQ(i + 1, counterValue);
  }

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
