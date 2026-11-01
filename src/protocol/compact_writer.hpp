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
    private: 
      //hold the encoded protocol data 
      std::vector<std::byte> data_; 
      //append one byte 
      void write_byte(std::uint8_t value);


};