#include <string>
#include <vector>

#include "commands.h"
#include "RamCloud.h"

std::string unsupportedCommand(std::vector<std::string> *argv) {
  std::string res("+Unsupported command.\r\n");
  return res;
}

std::string getCommand(std::vector<std::string> *argv) {
  std::string res("+Unsupported command.\r\n");
  return res;
}
