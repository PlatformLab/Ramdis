#ifndef __COMMANDS_H
#define __COMMANDS_H

#include "RamCloud.h"

std::string unsupportedCommand(RAMCloud::RamCloud *, 
    uint64_t,
    std::vector<std::string> *argv);

std::string getCommand(RAMCloud::RamCloud *, 
    uint64_t,
    std::vector<std::string> *argv);

std::string setCommand(RAMCloud::RamCloud *, 
    uint64_t,
    std::vector<std::string> *argv);

#endif
