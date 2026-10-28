#pragma  once 
//represnt one complete kafka frame 
#include <cstddef>
#include <optional>
#include <vector>
struct frame{
    std::vector<std::byte> payload;
}; 
//parse the lenght preffixed kafka frames from a byte vector 
class FrameParser{ 
    public: 
      //max permitted payload size;100MIB 
      static constexpr std::size_t kMaxFrameSize = 100U*1024U*1024U;  
      //try to parse one frame fro, starting at the begging of theinput 
      static std::optional<frame>parse_one(const std::vector<std::byte>&input, std::size_t&consumed);

};