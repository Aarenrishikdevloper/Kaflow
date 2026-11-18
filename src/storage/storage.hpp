#pragma  once  
#include <filesystem>
class StorageManager{
 public: 
   //construct the storage manager with a data directory 
   explicit StorageManager(const std::filesystem::path&data_directory);   
   //intialize the storage directory 
   void intialize(); 
   //return the root storage directory 
   const std::filesystem::path&root_directory()const noexcept;  
   //return the topics directory  
   const std::filesystem::path&topics_directory()const noexcept;
   private:  
   std::filesystem::path root_directory_;  
   std::filesystem::path toics_directory_;  
   //file containing broker message 
   std::filesystem::path metadata_file_; 
    //create the intaila broker metadadata file 
    void create_metadat_file_missing();


};