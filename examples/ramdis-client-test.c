#include <stdio.h>
#include <stdint.h>

#include "ramdis.h"

int main(int argc, char* argv[]) {
  printf("Ramdis Client Test\n");
  printf("Connecting to %s\n", argv[1]);
  void* context = connect(argv[1]); 
  ping(context, "Hi Ramdis!");
  return 0;
}
