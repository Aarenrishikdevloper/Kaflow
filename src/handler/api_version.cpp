#include "api_version.hpp"  
#include <cstddef>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <vector> 
#include "../protocol/byte_writer.hpp"
#include "api_registry.hpp"
namespace  { 
  //kafka API KEY 18 identifies API Version 
  constexpr std::int16_t kApiVersionKey = 18; 
  //error code 0 means sucess 
  constexpr std::int16_t kNoerror  =0; 
  //error code 35 means UNSUPPORTED_VERSION 
  constexpr std::int16_t kUnsupportedVersion = 35; 
  //api versions 
  constexpr std::int16_t kMinApiVersions = 0; 
  constexpr std::int16_t kMaxVersions = 2; 
  //build the Apiversion response body 
  std::vector<std::byte> build_response_body(std::int16_t request_version){
    //create a eriter for the response body  
    ByteWriter body; 
    //determine whether the requeted API Version is supported 
    const bool supported = request_version >= kMinApiVersions && request_version <= kMaxVersions;     
    //use the normal reponse for supported requests 
    const std::int16_t response_version = supported? request_version:0;  
    //write the top-level error code 
    body.write_int16(supported?kNoerror:kUnsupportedVersion);  
    //for an unsupported versions 
    if(!supported){
        body.write_int32(0); 
        return body.data();
    }  

  const auto&apis = supported_apis();  
  //write the numbeer of APIS in the response 
   body.write_int32(static_cast<std::int32_t>(apis.size()));    
   //serialize the every api entry 
    for(const auto&api : apis){
       //write the api Key 
       body.write_int16(api.api_key); 
       //write the min supported version 
       body.write_int16(api.min_version); 
       //write the max supported versions 
       body.write_int16(api.max_version); 
       
    }
    if(request_version >= 1){
        body.write_int32(0);
    } 
    //return the serialized reposnse 
    return body.data();

    
  }
}//end of namespace     
//construct a complete kafka response 
std::vector<std::byte>handle_api_version(const Requestheader &request){
  //rejecct api keys that this handler does not own 
  if(request.api_key != kApiVersionKey){
    throw  std::invalid_argument("Api version handler recive a diffrent api key");  
  } 
  //serialize the response body for the requested api version 
  const auto response_body = build_response_body(request.api_version);   
  //create the reponse payload starting with th header 
  ByteWriter response_payload; 
  //echo the corelation ID from the incoming request 
  response_payload.write_int32(request.correlation_id); 
  //append the APIversions response body after the header  
  response_payload.Write_bytes(response_body);
  const auto&payload = response_payload.data();  
  //kafka frmae prefix is a signed 32 bit byte count 
  if(payload.size() > static_cast<std::size_t>(std::numeric_limits<std::int32_t>::max())){
    throw std::length_error("Kafka response is too large");
  } 
  //create the final frame 
  ByteWriter frame; 
  //erite the payload lenght excluding the four byte preifx 
  frame.write_int32(static_cast<std::int32_t>(payload.size())); 
  //append the reaponbnse header and body  
  frame.Write_bytes(payload);  
  //return complete freme ready fro send 
  return  frame.data();

}
