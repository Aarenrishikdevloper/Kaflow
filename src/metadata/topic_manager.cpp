#include "topic_manager.hpp"    
#include "topic.hpp"
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
void TopicManager::add_topic(TopicMetadata topic){
    //lock the metadata before modyfying 
    std::lock_guard<std::mutex>lock(mutext_); 
    //save the topic name 
    const std::string name = topic.name; 
    //insert the topic 
    const auto[iterator, inserted]  = topics_.emplace(name, std::move(topic));   
    //do not replace an existing topic
    if(!inserted){
      throw std::runtime_error("Topics already exist");
    }
}  
//fina a topic by name 
std::optional<TopicMetadata> TopicManager::find_topic(const std::string &topic_name) const{
  //loc the mutex 
  std::lock_guard<std::mutex>lock(mutext_); 
  //search for the reueted topic 
  const auto iterator = topics_.find(topic_name); 
  //retun nullopt if the topic does not exist 
  if(iterator == topics_.end()){
    return  std::nullopt;
  } 
  return iterator->second;
}  
//return the shared topic manager 
TopicManager& topic_manager(){
  static TopicManager manager; 
  return manager;
}