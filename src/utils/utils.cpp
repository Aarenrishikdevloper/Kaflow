#include "utils.hpp"     
const APIversionInfo*find_api(std::int16_t api_key){
    //search through the supported api 
     for (const APIversionInfo&api : supported_apis()) {     
        //check whther this is the requested api 
        if(api.api_key == api_key){
            return &api;
        }
     
     } 
     return nullptr;
}