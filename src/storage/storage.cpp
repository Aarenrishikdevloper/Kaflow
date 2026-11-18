#include "storage.hpp"    
#include <filesystem>
#include <fstream>
#include <ios>
#include <stdexcept>
StorageManager::StorageManager(const std::filesystem::path&data_directory):root_directory_(data_directory),toics_directory_(data_directory / "topics"),metadata_file_(data_directory / "meta.properties"){};   
void StorageManager::intialize(){
    //create the root storage directory 
    std::filesystem::create_directory(root_directory_); 
    //crete the directory that will contain topic data 
    std::filesystem::create_directory(toics_directory_); 
    //create metadata if this is new directory 
    create_metadat_file_missing();
}   
const std::filesystem::path&StorageManager::root_directory() const noexcept{
    return root_directory_;
} 
const std::filesystem::path&StorageManager::topics_directory() const noexcept{
    return toics_directory_;
}   
void StorageManager::create_metadat_file_missing(){
    //do not overwrite existing broker metadata 
    if(std::filesystem::exists(metadata_file_)){
        return;
    } 
    //open the metadata file for writting 
    std::ofstream file(
        metadata_file_,   
        std::ios::out | std::ios::trunc
    ); 
    if(!file.is_open()){
        throw std::runtime_error("Could not create broker metadat file");
    } 
    //write the intially broker indentity 
    file << "broker.id=1\n"; 
    //identitfy the current  metadata format 
    file << "metadata.version=1\n"; 
    //close the file 
    file.flush(); 
    if(!file.good()){
        throw std::runtime_error("Could not write broker metadata");
    }  
    file.close();
}
