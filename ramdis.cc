#include "docopt.h"

#include <iostream>
#include <string>
#include <sstream>
#include <algorithm>
#include <iterator>

#include "RamCloud.h"
#include "ClientException.h"

static const char USAGE[] =
R"(Ramdis.

    Usage:
      ramdis connect RAMCLOUDCOORDLOC
      ramdis get KEY
      ramdis set KEY VALUE
      ramdis quit

)";

int main(int argc, const char** argv)
{
  RAMCloud::RamCloud *client;
  uint64_t defaultTableId;

  while(true) {
    printf("> ");
    std::string input;
    std::getline(std::cin, input);

    std::vector<std::string> tokens;
    bool inWord = false;
    bool inQuote = false;
    int argStartPos = 0;
    for (int i = 0; i < input.size(); i++) {
      if(inWord) {
        if(input[i] == ' ') {
          inWord = false;
          tokens.emplace_back(input.substr(argStartPos, i - argStartPos)); 
        }
      } else if(inQuote) {
        if(input[i] == '\"') {
          inQuote = false;
          tokens.emplace_back(input.substr(argStartPos, i - argStartPos)); 
        }
      } else {
        if(input[i] != ' ' && input[i] != '\"') {
          inWord = true;
          argStartPos = i;
        } else if(input[i] == '\"') {
          inQuote = true;
          argStartPos = i + 1;
        }
      }
    }

    if(inWord) {
      tokens.emplace_back(input.substr(argStartPos, 
            input.size() - argStartPos));
    }

    std::map<std::string, docopt::value> args = docopt::docopt(USAGE, 
        tokens,
        true,               // show help if requested
        "Ramdis 1.0");      // version string

//    for(auto const& arg : args) {
//      std::cout << arg.first << ": " << arg.second << std::endl;
//    }

    if(args["connect"].asBool() == true) {     
      client = new RAMCloud::RamCloud(args["RAMCLOUDCOORDLOC"]
          .asString().c_str());
      defaultTableId = client->createTable("default");
    } else if(args["get"].asBool() == true) {
      RAMCloud::Buffer buffer;
      try {
        client->read(defaultTableId, args["KEY"].asString().c_str(),
            args["KEY"].asString().length(), &buffer);
        printf("%s\n", static_cast<const char*>(buffer.getRange(0,
                buffer.size())));
      } catch (RAMCloud::ObjectDoesntExistException& e) {
        printf("Key doesn't exist.\n");
      }

    } else if(args["set"].asBool() == true) {
      client->write(defaultTableId, args["KEY"].asString().c_str(),
          args["KEY"].asString().length(),
          args["VALUE"].asString().c_str());
    } else if(args["quit"].asBool() == true) {
      break;
    }

  }
 
  delete client;

  return 0;
}
