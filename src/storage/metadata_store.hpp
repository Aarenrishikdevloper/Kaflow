#include "metadata/topic.hpp"
#include <filesystem>
#include <vector>
class MetadataStore{
  public: 
    explicit MetadataStore(const std::filesystem:: path&file_path); 
    //save a complete snapshot of all topics 
    void save_topics(const std::vector<TopicMetadata>&topics)const;    
    //load all topics from the meatdata file 
    std::vector<TopicMetadata> load_topics()const; 
    //check whether a metadat file exists  
    bool exists()const; 
    private: 
      std::filesystem::path file_path_;



}; 
MetadataStore& metadata_store();