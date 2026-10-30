#include "api_registry.hpp" 
#include <vector>
//return the broker supported Api list 
const std::vector<APIversionInfo>&supported_apis(){
    static const std::vector<APIversionInfo> apis = {
        {18,0,2}
    };  
    //return the same registry whenver requetsed 
    return apis;
}