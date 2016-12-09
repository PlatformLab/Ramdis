#include <stdio.h>
#include <stdint.h>
#include <string.h>

#include "ramdis.h"

int main(int argc, char* argv[]) {
  printf("Ramdis Client Test\n");
  printf("Connecting to %s\n", argv[1]);
  void* context = connect(argv[1]); 

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

  disconnect(context);
  return 0;
}
