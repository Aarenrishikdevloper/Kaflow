#include "frame.hpp"
#include <bit>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <stdexcept>
#include <vector>
// attemp to parse one complete kafka frame
std::optional<frame> FrameParser::parse_one(const std::vector<std::byte> &input,
                                            std::size_t &consumed) {
  // assume no bytes are cosumed until a full frame is avaialble
  consumed = 0;
  // a kafka frame starts with a four byte length prefix
  constexpr std::size_t preifx_size = sizeof(std::int32_t);
  // if preifix itself is incomplete request more data
  if (input.size() < preifx_size) {
    return std::nullopt;
  }
  // decode the four bytes prefix as an unsined integer
  std::uint32_t raw_length = 0;
  // read the prefix in big-endian order
  for (std::size_t i = 0; i < preifx_size; ++i) {
    // convert the byte to its numeric value
    const auto byte_value = std::to_integer<std::uint32_t>(input[i]);
    // shift the existing bits and append the next byte
    raw_length = static_cast<std::uint32_t>((raw_length << 8U) | byte_value);
  }
  // interprete the lenghts bit patteren as a signed 32 bit integer
  const auto signed_lenght = std::bit_cast<std::int32_t>(raw_length);
  // anegative frame lenght is invalid
  if (signed_lenght < 0) {
    throw std::invalid_argument("FrameParser: negative frame lenght");
  } 
  //convert to size_t after verifiying that the lenght is nonnegative 
  const auto frame_size = static_cast<std::size_t>(signed_lenght); 
  //reject an excessive large payload  
  if(frame_size > kMaxFrameSize){
    std::invalid_argument("Frameparser: frame ecxeeded max size");
  }  
  //the prefix is alredy pressent so check only payload bytes 
  if(input.size() - preifx_size < frame_size){
    //preserve the existing input and wait for more bytes 
    return std::nullopt;
  }  
  //find the first byte after the four byte prefix 
  using DiffrenceType = std::vector<std::byte>::difference_type; 
  //beiining od payload 
  const auto payload_begin = input.begin() + static_cast<DiffrenceType>(preifx_size);   
  //find the nd of the frames payload 
   const auto payload_end = payload_begin + static_cast<DiffrenceType>(frame_size); 
   //copy the frames payload into its own vector 
    frame Frame{
        std::vector<std::byte>(payload_begin,payload_end)
    };  
    //report from bytes the caller should remove from its buffer
    consumed = preifx_size + frame_size; 
    //return the sucessfully parsed frame 
    return Frame;
}