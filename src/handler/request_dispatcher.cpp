//parse a request and route it to the correct handler 
#include "request_dispatcher.hpp" 
#include "../protocol/request_header.hpp"
#include "api_registry.hpp"
#include "api_version.hpp"
#include <cstddef>
#include <stdexcept>
#include <vector>
std::vector<std::byte>RequestDispacther::dispatch(const std::vector<std::byte> &payload) const{
    //extract the kafka request header 
    const Requestheader request = Requestheader::parse(payload); 
    //look up api Key in the supported API register 
    const auto&apis = supported_apis();  
    //seach though the APis supported by this broker 
    for(const auto&api :apis){
        if(api.api_key  == request.api_key){
            if(request.api_version < api.min_version || request.api_version > api.max_version){
                throw std::invalid_argument("Unsupported api versions");
            } 
            if(request.api_key == 18){
                return handle_api_version(request);
            }
        }
        
    } 
    //no mnatching handler was found 
    throw std::invalid_argument("Unsupported Kafka API Key");
}