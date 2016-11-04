#ifndef __COMMANDS_H
#define __COMMANDS_H

#include "RamCloud.h"

std::string unsupportedCommand(RAMCloud::RamCloud *, 
    std::vector<std::string> *argv);

std::string getCommand(RAMCloud::RamCloud *, 
    std::vector<std::string> *argv);

#endif
