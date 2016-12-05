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

std::string incrCommand(RAMCloud::RamCloud *client,
    uint64_t tableId,
    std::vector<std::string> *argv) {
  try {
    uint64_t newValue = client->incrementInt64(tableId, (*argv)[1].c_str(),
        (*argv)[1].length(), 1);
    std::stringstream ss;
    ss << ":" << newValue;
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

std::string lpushCommand(RAMCloud::RamCloud *client,
    uint64_t tableId,
    std::vector<std::string> *argv) {
  
  // Arg validation.
  if ((*argv)[2].length() >= (1 << (sizeof(uint16_t)*8))) {
    std::string res("+List element must be less than 64KB in size.\r\n");
    return res;
  }

  // Read out old list, if it exists.
  RAMCloud::Buffer buffer;
  bool listExists = true;
  try {
    client->read(tableId, (*argv)[1].c_str(),
        (*argv)[1].length(), &buffer);
  } catch (RAMCloud::ObjectDoesntExistException& e) {
    listExists = false;
  }

  // Append new element to list.
  size_t newListSize;
  uint16_t elementLength = (uint16_t)(*argv)[2].length();
  if (listExists) {
    newListSize = sizeof(uint16_t) + elementLength + buffer.size();
  } else {
    newListSize = sizeof(uint16_t) + elementLength;
  }

  char* newList = (char*)malloc(newListSize);
  memcpy(newList, &elementLength, sizeof(uint16_t));
  memcpy(newList + sizeof(uint16_t), (*argv)[2].c_str(), elementLength);

  if (listExists) {
    const char* oldList = static_cast<const char*>(buffer.getRange(0,
            buffer.size()));
    memcpy(newList + sizeof(uint16_t) + elementLength, oldList, buffer.size());
  }

  // Write new list.
  client->write(tableId, 
      (*argv)[1].c_str(),
      (*argv)[1].length(),
      newList, 
      newListSize);

  // Count number of elements in the new list.
  uint32_t pos = 0;
  uint32_t count = 0;
  while (pos < newListSize) {
    count++;
    uint16_t len = *(uint16_t*)(newList + pos);
    pos += sizeof(uint16_t) + len;
  }
  
  std::ostringstream oss;
  oss << ":" << count << "\r\n";

  return oss.str();
}
