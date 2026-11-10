#pragma once 
#include "topic.hpp"
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
class TopicManager{
    public:  
      //add a topic to the broker 
      void add_topic(TopicMetadata topic); 
      //find a topic by name 
      std::optional<TopicMetadata> find_topic(const std::string&topic_name) const; 
      private: 
        //protect shared topic metadata across client thread 
        mutable std::mutex  mutext_;  
        //store topic using their name as keys 
        std::unordered_map<std::string, TopicMetadata> topics_;  
      
}; 
  //acess the broker shared topic 
TopicManager& topic_manager();