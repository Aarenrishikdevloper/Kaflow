#include "compact_reader.hpp"    
#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>
CompactReader::CompactReader(const std::vector<std::byte>& data):data_(data){}    
//read one byte and advance the cursoe 
std::uint8_t CompactReader::read_byte(){
    //do not read beyound the request buffer 
    if(postion_ >= data_.size()){
        throw std::runtime_error("Unexpected end of request");
    } 
    //convert std::byte into integer 
    const auto value = std::to_integer<std::uint8_t>(data_[postion_]);  
    //move to next byte 
    ++postion_; 
    return value;
}  
//decode an unsigned variable-lenght intger 
std::uint32_t CompactReader::read_uninsned_varint(){
   std::uint32_t result = 0; 
   //each byte contains seven data bits 
   for(unsigned int shift =0; shift < 35; shift += 7){
     const std::uint8_t byte = read_byte(); 
     //extract the seven data bits 
     const std::uint32_t value = byte & 0x7F; 
     //reject values  that would overflow uint32_t   
     if(shift == 28 && value > 0x0F){
        throw std::runtime_error("Vairant overflow");
     } 
     //put thsse seven bits into there correct position 
     result |= value << shift; 
     //if the highest bit is zero this is the final byte 
     if((byte & 0x80) == 0){
        return result;
     } 

   } 
   throw std::runtime_error("Invalid unsinnd variant");
} 
//read a kafka compact string   
std::string CompactReader::read_compact_string(){
   //compact string lenght is ecoded as lenght + 1 
   const std::uint32_t encoded_lenght = read_uninsned_varint(); 
   //zero represent null, which this fucnction does not accept  
   if(encoded_lenght == 0){
      throw std::runtime_error("Null compact string");
   } 
   //deoode the actual string length 
   const std::uint32_t length = encoded_lenght - 1; 
   //check that the requested  string fits in the buffer 
   if(length > data_.size() -postion_){
      throw std::runtime_error("Compact string eceeeds requets size");
   } 
   //create a string from the enoded bytes 
   std::string results; 
   results.reserve(length); 
   //copy each byte int the string 
   for (std::uint32_t i =0; i < length; ++i) {
       results.push_back(static_cast<char>(read_byte()));
   } 
   return results;
}  
//skip kafka tagged fields 
void CompactReader::skipped_tag_filed(){
   //the no  of tagged field is an unsigned  varint 
   const std::uint32_t tag_count = read_uninsned_varint(); 
   //each tag has a id a size and a byte payload 
   for(std::uint32_t i =0; i < tag_count; ++i){
      //read  the tag id 
      const std::uint32_t tag_id = read_uninsned_varint(); 
      //read the payload size 
      const std::uint32_t size = read_uninsned_varint(); 
      //check that tag payload is avalaible 
      if(size > data_.size() - postion_){
         throw std::runtime_error("Tagged filed exceees request size");
      } 
      //skip the tag payload
      postion_ += size;   
      (void)tag_id;


   }
} 
//returnn the curent poition 
std::size_t CompactReader::position() const noexcept{
   return postion_;
}