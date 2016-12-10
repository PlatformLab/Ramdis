#include <stdio.h>
#include <stdint.h>
#include <string.h>

#include "ramdis.h"

void printObjectArray(ObjectArray* objArray) {
  printf("[");
  int i;
  for (i = 0; i < objArray->len; i++) {
    if (i == 0) {
      printf("%s", (char*)(objArray->array[i].data));
    } else {
      printf(", %s", (char*)(objArray->array[i].data));
    }
  }
  printf("]\n");
}

int main(int argc, char* argv[]) {
  printf("Ramdis Client Test\n");
  printf("Connecting to %s\n", argv[1]);
  void* context = connect(argv[1]); 
  
  {
    printf("\nTesting GET/SET\n");
    Object key;
    key.data = "Bobby Jones";
    key.len = strlen(key.data)+1;
    Object value;
    value.data = "Age: 28, Occupation: lawyer, Trophies: 4";
    value.len = strlen(value.data)+1;
    set(context, &key, &value);

    Object* obj = get(context, &key);
    printf("key/value: (%s) : (%s)\n", (char*)key.data, (char*)obj->data);

    freeObject(obj);
  }

  {
    printf("\nTesting INCR\n");

    Object key;
    key.data = "incr test";
    key.len = strlen(key.data)+1;

    long val = incr(context, &key);

    printf("new value: %d\n", val);
  }

  {
    printf("\nTesting lpush, rpush, lpop, rpop, and lrange\n");
    Object key;
    key.data = "list test";
    key.len = strlen(key.data)+1;
   
    uint64_t elems;

    Object value;
    ObjectArray* objArray;
    Object* obj;

    value.data = "a";
    value.len = strlen(value.data)+1;
    printf("lpush %s\n", (char*)value.data);
    elems = lpush(context, &key, &value);
    printf("Now there are %d elements\n", elems);
    objArray = lrange(context, &key, 0, -1);
    printObjectArray(objArray);
    freeObjectArray(objArray);

    value.data = "b";
    value.len = strlen(value.data)+1;
    printf("lpush %s\n", (char*)value.data);
    elems = lpush(context, &key, &value);
    printf("Now there are %d elements\n", elems);
    objArray = lrange(context, &key, 0, -1);
    printObjectArray(objArray);
    freeObjectArray(objArray);

    value.data = "c";
    value.len = strlen(value.data)+1;
    printf("lpush %s\n", (char*)value.data);
    elems = lpush(context, &key, &value);
    printf("Now there are %d elements\n", elems);
    objArray = lrange(context, &key, 0, -1);
    printObjectArray(objArray);
    freeObjectArray(objArray);

    value.data = "d";
    value.len = strlen(value.data)+1;
    printf("rpush %s\n", (char*)value.data);
    elems = rpush(context, &key, &value);
    printf("Now there are %d elements\n", elems);
    objArray = lrange(context, &key, 0, -1);
    printObjectArray(objArray);
    freeObjectArray(objArray);

    value.data = "e";
    value.len = strlen(value.data)+1;
    printf("rpush %s\n", (char*)value.data);
    elems = rpush(context, &key, &value);
    printf("Now there are %d elements\n", elems);
    objArray = lrange(context, &key, 0, -1);
    printObjectArray(objArray);
    freeObjectArray(objArray);

    value.data = "f";
    value.len = strlen(value.data)+1;
    printf("rpush %s\n", (char*)value.data);
    elems = rpush(context, &key, &value);
    printf("Now there are %d elements\n", elems);
    objArray = lrange(context, &key, 0, -1);
    printObjectArray(objArray);
    freeObjectArray(objArray);

    obj = lpop(context, &key);
    printf("lpop: %s\n", (char*)(obj->data));
    freeObject(obj);

    obj = lpop(context, &key);
    printf("lpop: %s\n", (char*)(obj->data));
    freeObject(obj);

    obj = lpop(context, &key);
    printf("lpop: %s\n", (char*)(obj->data));
    freeObject(obj);

    obj = rpop(context, &key);
    printf("rpop: %s\n", (char*)(obj->data));
    freeObject(obj);

    obj = rpop(context, &key);
    printf("rpop: %s\n", (char*)(obj->data));
    freeObject(obj);

    obj = rpop(context, &key);
    printf("rpop: %s\n", (char*)(obj->data));
    freeObject(obj);
  }

  disconnect(context);
  return 0;
}
