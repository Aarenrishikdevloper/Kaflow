#pragma once 
#include <vector>
//routes kafka request to their corresponding ApI handler 
class RequestDispacther{
   public:  
   //dispatch a 
    std::vector<std::byte>dispatch(const std::vector<std::byte>&payload)const;
};
