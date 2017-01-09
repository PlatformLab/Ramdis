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
  Context* context = connect(argv[1]); 
  
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

  {
    printf("\nTesting l/rpush, l/rpop, and lrange at large scale\n");
    /* Number of elements to put in the big list. */
    uint64_t totalElements = (1<<13);
    /* Size of each element in bytes. */
    size_t elementSize = 8;

    printf("LPUSH'ing %d %dB elements. Total size: %dB\n", 
        totalElements,
        elementSize,
        totalElements * elementSize);

    Object key;
    key.data = "big list test";
    key.len = strlen(key.data)+1;
   
    Object value;
    char valBuf[elementSize];
    value.data = (void*)valBuf;
    value.len = elementSize;

    int i;
    uint64_t elems;
    for (i = 0; i < totalElements; i++) {
      sprintf(valBuf, "%07d", i);
      elems = lpush(context, &key, &value);

      if (context->err != 0) {
        printf("Error: %s\n", context->errmsg);
        return 0;
      }

      if (elems % 100000 == 0 && elems != 0) {
        printf("elems: %d\n", elems);
      }
    }

    printf("Elements LPUSH'd. Checking correctness... \n");

    ObjectArray* objArray;
    objArray = lrange(context, &key, 0, -1);

    if (objArray->len != totalElements) {
      printf("Error: Array length was wrong. Expected: %d, got: %d\n",
          totalElements, objArray->len);
      return 0;
    }

    for (i = 0; i < totalElements; i++) {
      sprintf(valBuf, "%07d", totalElements - i - 1);
      if (strcmp(objArray->array[i].data, valBuf) != 0) {
        printf("Error: Element %d was not correct. Expected: %s, got: %s\n",
            i,
            valBuf,
            objArray->array[i].data);
        return 0;
      }
    }

    freeObjectArray(objArray);

    printf("Good.\n");

    printf("Now RPOP'ing all the elements off the list... \n");

    Object *obj;
    for (i = 0; i < totalElements; i++) {
      sprintf(valBuf, "%07d", i);
      printf("popping element %d\n", i);
      obj = rpop(context, &key);

      if (context->err != 0) {
        printf("Error: Popping element %d: %s\n", i, context->errmsg);
        return 0;
      }

      if (obj == NULL) {
        printf("Error: Returned element %d was null.\n", i);
        return 0;
      }

      if(strcmp(obj->data, valBuf) != 0) {
        printf("Error: Element %d was not correct. Expected: %s, got: %s\n",
            i,
            valBuf,
            obj->data);
        return 0;

      }
      freeObject(obj);
    }

    printf("Good.\n");

    printf("RPUSH'ing %d %dB elements. Total size: %dB\n", 
        totalElements,
        elementSize,
        totalElements * elementSize);

    for (i = 0; i < totalElements; i++) {
      sprintf(valBuf, "%07d", i);
      elems = rpush(context, &key, &value);

      if (context->err != 0) {
        printf("Error: %s\n", context->errmsg);
        return 0;
      }

      if (elems % 100000 == 0 && elems != 0) {
        printf("elems: %d\n", elems);
      }
    }

    printf("Elements RPUSH'd. Checking correctness... \n");

    objArray = lrange(context, &key, 0, -1);

    if (objArray->len != totalElements) {
      printf("Error: Array length was wrong. Expected: %d, got: %d\n",
          totalElements, objArray->len);
      return 0;
    }

    for (i = 0; i < totalElements; i++) {
      sprintf(valBuf, "%07d", i);
      if (strcmp(objArray->array[i].data, valBuf) != 0) {
        printf("Error: Element %d was not correct. Expected: %s, got: %s\n",
            i,
            valBuf,
            objArray->array[i].data);
        return 0;
      }
    }

    freeObjectArray(objArray);

    printf("Good.\n");

    printf("Now LPOP'ing all the elements off the list... \n");

    for (i = 0; i < totalElements; i++) {
      sprintf(valBuf, "%07d", i);
      printf("popping element %d\n", i);
      obj = lpop(context, &key);

      if (context->err != 0) {
        printf("Error: Popping element %d: %s\n", i, context->errmsg);
        return 0;
      }

      if (obj == NULL) {
        printf("Error: Returned element %d was null.\n", i);
        return 0;
      }

      if(strcmp(obj->data, valBuf) != 0) {
        printf("Error: Element %d was not correct. Expected: %s, got: %s\n",
            i,
            valBuf,
            obj->data);
        return 0;

      }
      freeObject(obj);
    }

    printf("Good.\n");
  }

  disconnect(context);
  return 0;
}
