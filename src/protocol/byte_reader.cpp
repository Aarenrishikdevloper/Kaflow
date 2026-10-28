//private ducntuon belonging to this unknown namespace 
#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <vector> 
#include "byte_reader.hpp" 
#include <bit>
namespace  {
   //decode an unsinned integer  form  big endian type  
   template<typename  UInt> UInt decode_big_endian(const std::vector<std::byte>&data, std::size_t position){
      //start with zero; bytes are added from left to right 
      UInt value = 0; 
      //process exactly sizeof(Uint) bytes 
      for(std::size_t i =0; i< sizeof(UInt); ++i){
         //convert the current byte to an ineteger 
         const auto byteValue  = std::to_integer<unsigned int>(data[position + i]); 
         //shift the existing value left by 8 bits and append this byte 
         value = static_cast<UInt>(value << 8U) | static_cast<UInt>(byteValue);
      } 
      return value;
   }
                               
}//namesapce ends     
ByteReader::ByteReader(const std::vector<std::byte>&data):data_(data){}   
//read a sign 16 bit integer 
std::int16_t ByteReader::read_int16(){
   //two bytes are required for a 16 bit integer 
   if(remaining() < sizeof(std::uint16_t)){
      throw std::out_of_range("ButeReader: not enough bytes for int16");
   } 
   //decode the next two bytes as unsined integer 
   const auto raw = decode_big_endian<std::uint16_t>(data_, position_);   
   //advance the cursor by two bytes  
   position_ += sizeof(std::uint16_t); 
   //preseve the bit pattern while interpreting as signed 
   return std::bit_cast<std::int16_t>(raw);
}
//read a 32 bit integer 
std::int32_t ByteReader::read_int32(){
   //four bytes are required for a 32 bit integer 
   if(remaining() < sizeof(std::uint32_t)){
      throw std::out_of_range("ButeReader: not enough bytes for int32");
   } 
   //decode the next 4 bytes as unsined integer 
   const auto raw = decode_big_endian<std::uint32_t>(data_, position_);   
   //advance the cursor by two bytes  
   position_ += sizeof(std::uint32_t); 
   //preseve the bit pattern while interpreting as signed 
   return std::bit_cast<std::int32_t>(raw);
}   
//read a 64 bit integer 
std::int64_t ByteReader::read_int64(){
   //8 bytes are required for a 32 bit integer 
   if(remaining() < sizeof(std::uint64_t)){
      throw std::out_of_range("ButeReader: not enough bytes for int64");
   } 
   //decode the next 8 bytes as unsined integer 
   const auto raw = decode_big_endian<std::uint64_t>(data_, position_);   
   //advance the cursor by two bytes  
   position_ += sizeof(std::uint64_t); 
   //preseve the bit pattern while interpreting as signed 
   return std::bit_cast<std::int64_t>(raw);
}   
//read an arbitary no of bytes 
   
std::vector<std::byte>ByteReader::read_bytes(std::size_t count){ 
    //never read beyond the end of input vector
    if(count > remaining()){
      throw std::out_of_range("Bytereader: not enough bytes for bytes sequence");    
    }  
    //convert the cursor to the iterators difffrence type 
    using DiffrenceType = std::vector<std::byte>::difference_type ;      
    //locate the beggining of the requeted byte range 
    const auto begin = data_.begin() + static_cast<DiffrenceType>(position_); 
    //locate the end of the requested byte range 
    const auto end = begin + static_cast<DiffrenceType>(count); 
    //copy the requetd bytes into a new vector
    std::vector<std::byte> result(begin, end); 
    //advance the cursor past the bytes that were read 
    position_ +=count; 
    return result;
   
}  
//return the current position 
std::size_t ByteReader::postion()  const noexcept{
   return position_;
} 
//retunr the no of unread bytes 
std::size_t ByteReader::remaining() const noexcept{
   return data_.size() - position_;
} 
//return whether the reader has reached the end 
bool ByteReader::empty() const noexcept{
   return remaining() == 0;
}