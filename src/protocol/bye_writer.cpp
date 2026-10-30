#include "byte_writer.hpp"  
#include <bit>
#include <cstddef>
#include <cstdint>
#include <vector>
//serialize a sined 32 bit integer 
void ByteWriter::write_int32(std::int32_t value){
    //preserve the inteher bit pattern  as a unsigned value 
    const std::uint32_t raw = std::bit_cast<std::uint32_t>(value);  
    //write the most significant byte first 
    for (int shift = 24 ; shift >=0; shift -= 8) {
        const auto byte_value = static_cast<std::uint8_t>((raw >> static_cast<unsigned int>(shift)) & 0xFFU);     
        //convert the byte value into std::byte and append it 
        data_.push_back(static_cast<std::byte>(byte_value));

    }
} 
//return the bytes without copying them 
const std::vector<std::byte>&ByteWriter::data() const noexcept{
    return data_;
}  
//serialize a signed 16 bit integer 
void ByteWriter::write_int16(std::int16_t value){
    //preserve the exact bit pattern as unsigned bit 
    const auto raw = std::bit_cast<std::uint16_t>(value); 
    //write the most significant byte first 
    data_.push_back(static_cast<std::byte>((raw >> 8U) & 0xFFU));  
    data_.push_back(static_cast<std::byte>(raw & 0xFFU));
}  
//append bytes from anither vector
void ByteWriter::Write_bytes(const std::vector<std::byte> &bytes){
   //inser the complete range at the end of output 
   data_.insert(data_.end(), bytes.begin(), bytes.end());
}