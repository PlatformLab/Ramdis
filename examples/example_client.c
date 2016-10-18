#include "ramdis.h"

int main() {
  redisCommand(NULL, "SET %s %s", "bob", "is silly");
  return 0;
}
