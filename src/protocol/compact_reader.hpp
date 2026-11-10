#pragma  once 
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>
class CompactReader{
    public: 
     //store a reference to the request bytes 
     explicit CompactReader(const std::vector<std::byte>& data);   
     //read an unisned vairable lenght integer 
     std::uint32_t read_uninsned_varint(); 
     //read an kafka compact string 
     std::string read_compact_string(); 
     //read andignor tag filed 
     void skipped_tag_filed();   
     std::optional<std::string>read_compoact_nullable_string(); 
     //read a signed 32 bit endian integer  
     std::int32_t read_int32();  
     //read a kafaka boolean 
     bool read_boolean(); 
     std::optional<std::uint32_t>read_compact_array_lenght();   
     std::uint8_t read_uint8();
     //retunr the current reading 
     [[nodiscard]] std::size_t position() const noexcept; 
     private: 
       //the original request data 
       const std::vector<std::byte>&data_;  
       //curent position in request  
       std::size_t postion_ = 0; 
       //read one byte safely 
       std::uint8_t read_byte();
};