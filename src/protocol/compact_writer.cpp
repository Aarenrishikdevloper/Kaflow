#include "compact_writer.hpp"    
#include <bit>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <stdexcept>
#include <vector>
//append one byte to the output 
void CompacrWriter::write_byte(std::uint8_t value){
    data_.push_back(static_cast<std::byte>(value));
}
//encode an unsined vaible-length integer 
void CompacrWriter::write_unsigned_varian(std::uint32_t value){
    //continue while more than seven bits remain 
    while (value >= 0x80) {
       //write the lower seven bits and set the continuation bit 
       write_byte(static_cast<std::uint8_t>(value & 0x7F) | 0x80); 
       //move to the next seven bytes 
       value >>= 7;
    } 
    //write thw final byte without a continuation bit 
    write_byte(static_cast<std::uint8_t>(value));
}  
//encode a compact string 
void CompacrWriter::write_compact_string(const std::string &value){
    //compact string store the length + 1 
    write_unsigned_varian(static_cast<std::uint32_t>(value.size()) + 1); 
    //append the string value 
    for (unsigned char chracter: value) {
       write_byte(chracter);
    }

}  
//enocde an empty tagged-filed section 
void CompacrWriter::write_emty_tagged_field(){
    //zero means there are no tagged field 
    write_unsigned_varian(0);
} 
//return the encoded output 
const std::vector<std::byte>&CompacrWriter::data() const noexcept{
    return data_;
} 
void CompacrWriter::write_bytes(std::vector<std::byte> &bytes){
    //insert all data at th end of output 
    data_.insert(data_.end(),bytes.begin(), bytes.end());
} 
void CompacrWriter::write_int16(std::int16_t value){ 
    //preseve the signed  integer bit pattern 
    const std::uint8_t bits = std::bit_cast<std::uint16_t>(value);
    //write the high first byte  
    write_byte(static_cast<std::uint8_t>(bits >> 8));
    //write the low byte 
     write_byte(static_cast<uint8_t>(bits));
    
} 
void CompacrWriter::write_int32(std::int32_t value){
    const std::uint32_t bits = static_cast<std::uint32_t>(value);  
    //write thw most significant bit 
    write_byte(static_cast<std::uint8_t>(bits >> 24)); 
    //write the next byte 
    write_byte(static_cast<std::uint8_t>(bits >> 16)); 
    //write the next byte 
     write_byte(static_cast<std::uint8_t>(bits >> 8)); 
    //write the lesat sihnificant byte 
     write_byte(static_cast<std::uint8_t>(bits)); 
 
}  
void CompacrWriter::Write_booleand(bool value){
    write_byte(value ? 1:0);
}   
//write a null compact array 
void CompacrWriter::write_null_comapct_array(){
    write_unsigned_varian(0);
}   
void CompacrWriter::wriet_compoact_arraylenght(std::uint32_t count){
    if(count ==UINT32_MAX){
        throw std::length_error("Compact array is too large");
    } 
    //kafka encodes comapct array lengh as count + 1  
    write_unsigned_varian(count+1);
}