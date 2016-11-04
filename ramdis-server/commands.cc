#include <string>
#include <vector>

#include "commands.h"
#include "RamCloud.h"

std::string unsupportedCommand(RAMCloud::RamCloud *client,
    std::vector<std::string> *argv) {
  std::string res("+Unsupported command.\r\n");
  return res;
}

std::string getCommand(RAMCloud::RamCloud *client,
    std::vector<std::string> *argv) {
  std::string res("+Unsupported command.\r\n");
  return res;
}
