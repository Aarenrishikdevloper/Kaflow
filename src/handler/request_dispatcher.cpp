//parse a request and route it to the correct handler 
#include "request_dispatcher.hpp" 
#include "../protocol/request_header.hpp"
#include "api_registry.hpp"
#include "../utils/utils.hpp"
#include "api_version.hpp"
#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <vector>
std::vector<std::byte>RequestDispacther::dispatch(const std::vector<std::byte> &payload) const{  
    //red the first four bytes 
    ByteReader intial_reader(payload); 
    //read the api_key 
    const std::int16_t api_key = intial_reader.read_int16(); 
    //read the api_version  
    const std::int16_t api_version = intial_reader.read_int16(); 
   //find the api in  our registry  
   const APIversionInfo*api = find_api(api_key);          

    if(api == nullptr){
        throw std::runtime_error("unsupported Api Key");
    } 
    //validate the requeted api version  
    if(api_version< api->min_version || api_version > api->max_version){
        throw  std::runtime_error("Unsupported API versions");
    }
   //select the reuwwt header parsser 
   Requestheader header; 
   //header version 2 uses the flexible format 
   if(api->request_header_version == 2){
      header = Requestheader::parse_flexible(payload);
   }else{
    header = Requestheader::parse(payload);
   }   
   //extract the reuest body 
   const std::size_t body_position = header.header_size; 
   if(body_position > payload.size()){
     throw std::runtime_error("invalid request header size");
   } 
   //copy the remaining byte into the request body 
   const std::vector<std::byte> request_body(payload.begin() + static_cast<std::ptrdiff_t>(body_position), payload.end()); 
   if(api_key == 18){
     return handle_api_version(header);
   } 
   if(api_key == 75){
      throw  std::runtime_error("unsupported api");
   }  
throw std::runtime_error("no handler registered for this api");
}