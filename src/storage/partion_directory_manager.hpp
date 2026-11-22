#pragma  once 
#include "metadata/topic.hpp"
#include <cstdint>
#include <filesystem> 
#include <string>

class PartionDirectoryManager{
 public: 
   explicit PartionDirectoryManager(const std::filesystem::path & topics_directory);  
   //create directory and intial segmnent files for evry partion 
   void create_topic_partion(const TopicMetadata & topic)const;  
   //delte all disk data belonging to a topic  
   void delete_topic(const TopicMetadata&topic)const; 
   //return the directory for a particular topic 
   std::filesystem::path topic_directory(const TopicMetadata&topic)const;   
   //return the directory for a particular partion 
   std::filesystem::path partion_directory(const TopicMetadata&topic, std::int32_t particular_index)const; 
   private: 
     std::filesystem::path topics_directory_; 
     //convert the 16 byte topic uuid into a 32 chracter hex string 
     static std::string topic_id_to_string(const std::array<std::byte, 16>topic_id); 
     //create a file only when it does 
     static void create_file_if_missing(const std::filesystem::path&file_path);
};  
PartionDirectoryManager & partion_directory_manager(); 