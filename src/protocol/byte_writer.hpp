#pragma  once    
#include <cstddef>
#include <cstdint>
#include <vector>
//write protocol value into a byte buffer 
class ByteWriter{
   public: 
     //write a signed 32 bit ineger in big -endian byte order 
     void write_int32(std::int32_t value);    
     void write_int16(std::int16_t value); 
     void Write_bytes(const std::vector<std::byte>&bytes);
     //return the serialize bytes 
     const std::vector<std::byte>&data() const noexcept;  
     private: 
      std::vector<std::byte> data_;
};