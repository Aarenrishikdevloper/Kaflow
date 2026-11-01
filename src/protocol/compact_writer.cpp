#include "compact_writer.hpp"    
#include <cstddef>
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