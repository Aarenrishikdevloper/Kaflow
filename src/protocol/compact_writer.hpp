#pragma once   
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>
class CompacrWriter{
  public:  
     //encode an unsigned variable-lenght integer
    void write_unsigned_varian(std::uint32_t value);       
    //enfoce a kafka compact string    
    void write_compact_string(const std::string&value); 
    //write an empty tagged-field solution 
    void write_emty_tagged_field(); 
    //return the encoded bytes 
    const std::vector<std::byte>& data()const noexcept;     
    void write_int16(std::int16_t value); 
    void write_int32(std::int32_t value);  
    void wriet_compoact_arraylenght(std::uint32_t count); 
    void write_bytes(std::vector<std::byte>&bytes);
    void Write_booleand(bool value); 
    void write_null_comapct_array();
    private: 
      //hold the encoded protocol data 
      std::vector<std::byte> data_; 
      //append one byte 
      void write_byte(std::uint8_t value);


};