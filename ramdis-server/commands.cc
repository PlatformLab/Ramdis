#include <string>
#include <sstream>
#include <vector>

#include "commands.h"
#include "RamCloud.h"
#include "ClientException.h"

std::string unsupportedCommand(RAMCloud::RamCloud *client,
    uint64_t tableId,
    std::vector<std::string> *argv) {
  std::string res("+Unsupported command.\r\n");
  return res;
}

std::string getCommand(RAMCloud::RamCloud *client,
    uint64_t tableId,
    std::vector<std::string> *argv) {
  RAMCloud::Buffer buffer;
  try {
    client->read(tableId, (*argv)[1].c_str(),
        (*argv)[1].length(), &buffer);
    std::stringstream ss;
    ss << "$" << buffer.size();
    const char* data = static_cast<const char*>(buffer.getRange(0,
            buffer.size()));
    ss.write(data, buffer.size());
    ss << "\r\n";
    return ss.str();
  } catch (RAMCloud::ObjectDoesntExistException& e) {
    std::string res("+Unknown key.\r\n");
    return res;
  }
}

std::string setCommand(RAMCloud::RamCloud *client,
    uint64_t tableId,
    std::vector<std::string> *argv) {
  client->write(tableId, (*argv)[1].c_str(),
      (*argv)[1].length(),
      (*argv)[2].c_str());
  return std::string("+OK\r\n");
}
