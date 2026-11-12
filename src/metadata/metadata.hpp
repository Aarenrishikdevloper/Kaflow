#pragma once 
#include "protocol/request_header.hpp"
#include <cstddef>
#include <string>
#include <vector>
struct MetaDataRequest{
    std::vector<std::string> topics;  
    //parse the metadada api v0 request   
    static MetaDataRequest parse(const std::vector<std::byte>&body);
}; 
//handle a metadta api v0 request 
std::vector<std::byte> handle_metadata(const  Requestheader header, const std::vector<std::byte>body);