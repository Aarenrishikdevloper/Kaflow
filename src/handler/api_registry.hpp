#pragma once 
#include <cstdint>
#include <vector>
//describe the supported version range of one kafka api 
struct APIversionInfo{
    std::int16_t api_key; 
    std::int16_t min_version; 
    std::int16_t max_version;
};   
//return API currently supported by this broker 
const std::vector<APIversionInfo>&supported_apis(); 
