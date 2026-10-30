#pragma  once 
//reprerent the fixed widthe portion of a kafka request header 
#include "byte_reader.hpp"
#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <vector>
struct Requestheader{
   std::int16_t api_key = 0; 
   std::int16_t api_version =0; 
   std::int32_t correlation_id = 0;  
   std::size_t header_size = 0;
   //parse the first eight bytes of a request payload
   static Requestheader parse(const std::vector<std::byte>&payload){
    //start reading at byte zero 
    ByteReader reader(payload);  
    //inttilaize all file d beofre assigning 
    Requestheader header{}; 
    //read the 2 byte api key 
    header.api_key = reader.read_int16(); 
    //read the 2 byte api version 
    header.api_version = reader.read_int16(); 
    //correlation id 
    header.correlation_id = reader.read_int32();  
    //read the nullable client id is null 
    const std::int16_t client_id_lenght = reader.read_int16();      
    //-1 means the client id is null 
    if(client_id_lenght == -1){
      //no client id bytes follow
    } 
    else if(client_id_lenght >=0){
      //skip exactly the no of client Id bytes specified 
      //read_bytes checks that the bytes are avalaible 
      (void)reader.read_bytes(client_id_lenght);
    }else{
        //values -1 are invalid nullable string lenght 
        throw std::invalid_argument("RequestHeader: Invalid Cleint ID");
    } 
    header.header_size = reader.postion(); 
    return header;

   }
};