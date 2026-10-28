#pragma  once   
#include <cstddef>
#include <cstdint>
#include <vector>
//read binary values from a byte buffer 
class ByteReader{
    public: 
      //construct a reader over an existing sequence of bytes 
      explicit ByteReader(const std::vector<std::byte> &data); 
      //read a signed 16 bit integer  in big-endian  byte order 
      std::int16_t read_int16();  
      //read a signed 32 bit integer  in big-endian  byte order 
      std::int32_t read_int32();   
      //read a signed 64 bit integer  in big-endian  byte order 
      std::int64_t read_int64();    
      //read exactly count bytes and return them in a new vector 
      std::vector<std::byte> read_bytes(std::size_t count); 
      //return the current reading position 
      [[nodiscard]] std::size_t postion() const noexcept; 
      //return the no of unread bytes 
      [[nodiscard]]  std::size_t remaining()const noexcept;   
      //reurn true when no unread bytes remaining  
      [[nodiscard]] bool empty()const noexcept; 
      private: 
       //reference the original vector without copying it 
       const std::vector<std::byte>&data_; 
       //index of the next byte to read 
        std::size_t position_ = 0;

      
}; 