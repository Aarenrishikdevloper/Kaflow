#pragma once  
#include "../protocol/request_header.hpp"
#include <cstddef>
#include <vector>
//build a complete lenght prefix APIVersion response 
std::vector<std::byte> handle_api_version(const Requestheader& request);
