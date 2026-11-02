#pragma  once 
//reprerent the fixed widthe portion of a kafka request header 
#include "byte_reader.hpp"
#include "compact_reader.hpp"
#include <cstddef>
#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector> 
enum class RequestHeaderFormat{
  LEGACY,
  FLEXIBLE
};
struct Requestheader{
   std::int16_t api_key = 0; 
   std::int16_t api_version =0; 
   std::int32_t correlation_id = 0;  
   std::size_t header_size = 0;   
   std::optional<std::string> cleint_id;
    RequestHeaderFormat format;
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
    std::optional<std::string> cleint_id;
    //-1 means the client id is null 
    if(client_id_lenght == -1){
       cleint_id = std::nullopt;
    } else{
       if(client_id_lenght < -1){
         throw std::runtime_error("Invalid Client id length");
       }   
       //read the client id bytes 
       const auto bytes =reader.read_bytes(static_cast<std::size_t>(client_id_lenght));   
       std::string value; 
       for (std::byte byte : bytes) {
             value.push_back(static_cast<char>(std::to_integer<unsigned char>(byte)));
       } 
       cleint_id = std::move(value);
    }
    
    header.header_size = reader.postion();    
    header.cleint_id = cleint_id;
    header.format = RequestHeaderFormat::LEGACY;
    return header;

   }    
   static Requestheader parse_flexible(const std::vector<std::byte>&payload){
      //read the fixed-sized portion first 
      ByteReader fixed_reader(payload);  
      //read the api_key  
      const std::int16_t api_key = fixed_reader.read_int16(); 
      //read the api_version 
      const std::int16_t api_version = fixed_reader.read_int16(); 
      const std::int32_t correlation_id =fixed_reader.read_int32();  
      //extract the remaing after the fixed fields 
      const std::vector<std::byte>remaing(payload.begin() +static_cast<std::ptrdiff_t>(fixed_reader.postion()),payload.end());   
      //create a compact reader for thr flexible fields 
      CompactReader compact_reader(remaing); 
      //read the nullable client id  
      const std::optional<std::string> cleint_id = compact_reader.read_compoact_nullable_string();  
      //read the tag filed 
      compact_reader.skipped_tag_filed(); 
      //caculate the complete header size 
      const std::size_t header_size = fixed_reader.postion() + compact_reader.position(); 
      return  Requestheader{
         api_key,      
         api_version,  
         correlation_id,   
         header_size,  
         cleint_id,
         RequestHeaderFormat::FLEXIBLE
      };
   } 
};