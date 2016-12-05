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

std::string rpushCommand(RAMCloud::RamCloud *client,
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
  char* newList;
  if (listExists) {
    newListSize = sizeof(uint16_t) + elementLength + buffer.size();
    newList = (char*)malloc(newListSize);
    const char* oldList = static_cast<const char*>(buffer.getRange(0,
            buffer.size()));
    memcpy(newList, oldList, buffer.size());
    memcpy(newList + buffer.size(), &elementLength, sizeof(uint16_t));
    memcpy(newList + buffer.size() + sizeof(uint16_t), (*argv)[2].c_str(), 
        elementLength);
  } else {
    newListSize = sizeof(uint16_t) + elementLength;
    newList = (char*)malloc(newListSize);
    memcpy(newList, &elementLength, sizeof(uint16_t));
    memcpy(newList + sizeof(uint16_t), (*argv)[2].c_str(), elementLength);
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
  printf("newListSize: %d\n", newListSize);
  while (pos < newListSize) {
    count++;
    uint16_t len = *(uint16_t*)(newList + pos);
    pos += sizeof(uint16_t) + len;
  }
  
  std::ostringstream oss;
  oss << ":" << count << "\r\n";

  return oss.str();
}

std::string lrangeCommand(RAMCloud::RamCloud *client,
    uint64_t tableId,
    std::vector<std::string> *argv) {

  int start = atoi((*argv)[2].c_str());  
  int end = atoi((*argv)[3].c_str());

  // Read out old list, if it exists.
  RAMCloud::Buffer buffer;
  try {
    client->read(tableId, (*argv)[1].c_str(),
        (*argv)[1].length(), &buffer);
  } catch (RAMCloud::ObjectDoesntExistException& e) {
    std::string res("+Unknown key.\r\n");
    return res;
  }

  const char* list = static_cast<const char*>(buffer.getRange(0, 
        buffer.size()));

  // Parse the elements.
  uint32_t pos = 0;
  std::vector<std::string> elements;
  while (pos < buffer.size()) {
    uint16_t len = *(uint16_t*)(list + pos);
    pos += sizeof(uint16_t);
    elements.emplace_back(list + pos, len);
    pos += len;
  }

  uint32_t startIndex;
  if (start < 0) {
    startIndex = elements.size() + start;
  } else {
    startIndex = start;
  }

  uint32_t endIndex;
  if (end < 0) {
    endIndex = elements.size() + end;
  } else {
    if (end >= elements.size()) {
      endIndex = elements.size() - 1;
    }
  }


  // Generate return message.
  std::ostringstream oss;
  oss << "*" << elements.size() << "\r\n";

  uint32_t count = 0;
  for(auto const& e: elements) {
    if (count >= start && count <= end) {
      oss << "$" << e.length() << "\r\n" << e << "\r\n";
    }
    count++;
  }

  return oss.str();
}

