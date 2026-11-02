#pragma once 
#include "byte_writer.hpp"
#include "compact_writer.hpp"
#include <cstdint>  
enum class ResponseHeaderFormat{
  LEGACY,
  FLEXIBLE
};
struct ResponseHeader{
  //must mathch the incoming request Id 
   std::int32_t correlation_id; 
   //apprnd this header bytes to supplied  output buffer 
   void write(ByteWriter& writer)const{
     writer.write_int32(correlation_id); //write the iDin big indian from
   }     
   void write_flexible(ByteWriter& fixed_writer,CompacrWriter& compact_writer){
     //write correlation id 
     fixed_writer.write_int32(correlation_id); 
     compact_writer.write_emty_tagged_field();
   }
};