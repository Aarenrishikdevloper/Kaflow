#pragma once 
#include "byte_writer.hpp"
#include <cstdint>
struct ResponseHeader{
  //must mathch the incoming request Id 
   std::int32_t correlation_id; 
   //apprnd this header bytes to supplied  output buffer 
   void write(ByteWriter& writer)const{
     writer.write_int32(correlation_id); //write the iDin big indian from
   }
};