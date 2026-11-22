#include "storage/partion_directory_manager.hpp"
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <ios>
#include <sstream>
#include <stdexcept>
#include <string>
namespace  {
   constexpr const char* INITIAL_SEGMENT_NAME = "00000000000000000000"; 
   //conevrt a partion index into its directory name 
   std::string partion_name(std::int32_t partion_index){
     if(partion_index < 0){
         throw std::runtime_error("negative partion index"); 

     } 
     return std::to_string(partion_index);
   } 

}//namepsace ends  
PartionDirectoryManager::PartionDirectoryManager(const std::filesystem::path&topics_directory):topics_directory_(topics_directory){} 
//convert all 16 uuid bytes into lowercase hexadecimal 
std::string PartionDirectoryManager::topic_id_to_string(const std::array<std::byte, 16>topic_id){
    std::ostringstream result; 
    //use hexadecimal output with two digits per byte 
    result << std::hex << std::setfill('0'); 
    for (const std::byte byte : topic_id) { 
        const auto value = std::to_integer<unsigned int>(byte); 
        result << std::setw(2)<<value;    
    
    } 
    return  result.str();
} 
//build the topic directory path 
std::filesystem::path PartionDirectoryManager::topic_directory(const TopicMetadata &topic) const{
    return topics_directory_ /topic_id_to_string(topic.topic_id);
} 
//build the partion directory path 
std::filesystem::path PartionDirectoryManager::partion_directory(const TopicMetadata &topic, std::int32_t particular_index) const{
    return topic_directory(topic) / partion_name(particular_index);
} 
//create file only if it does not exist 
void PartionDirectoryManager::create_file_if_missing(const std::filesystem::path &file_path){
    //preserve existing segment file 
    if(std::filesystem::exists(file_path)){
        if(!std::filesystem::is_regular_file(file_path)){
            throw std::runtime_error("excepted a regular storage file" +file_path.string());
        }  
        return;
    } 
    //create a binary file 
    std::ofstream output(
        file_path, 
        std::ios::binary | std::ios::out
    ); 
    if(!output){
        throw std::runtime_error("Could not create storage  file" + file_path.string());
    } 
    output.flush(); 
    if(!output){
        throw std::runtime_error("Failed while creating storage file"+file_path.string());
    }
} 
//create all partion directories and thier intial files 
void PartionDirectoryManager::create_topic_partion(const TopicMetadata &topic) const{
    //ensure topic storage root exists 
    const auto topic_path = topic_directory(topic); 
    std::filesystem::create_directory(topic_path); 
    //process each partion in tthe topic metadata 
    for (const auto &partion : topic.partitions) {
      //build this partion index 
      const auto partion_path = partion_directory(topic, partion.partion_index);  
      //create the partion directory if necessary 
      std::filesystem::create_directory(partion_path); 
      //create the intial log file 
      const auto log_path = partion_path / (std::string(INITIAL_SEGMENT_NAME) + ".log"); 
      create_file_if_missing(log_path); 
      //creeate the offset index file 
      const auto index_path = partion_path / (std::string(INITIAL_SEGMENT_NAME) + ".index"); 
      create_file_if_missing(index_path);   
      //create timestamp index 
      const auto time_index_path = partion_path / (std::string(INITIAL_SEGMENT_NAME) +".timeindex"); 
      create_file_if_missing(time_index_path);

    }   


}   
//delet all storage file belonging to this topic UUID 
void PartionDirectoryManager::delete_topic(const TopicMetadata &topic)const{
    const auto path = topic_directory(topic);  
    //if no directory  return 
    if(!std::filesystem::exists(path)){
        return;
    } 
    std::filesystem::remove_all(path);
}   
PartionDirectoryManager& partion_directory_manager(){
    static PartionDirectoryManager manager("data/topics");  
    return manager;
}