#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages){}

LRUReplacer::~LRUReplacer() = default;

/**
 * TODO: Student Implement
 */
bool LRUReplacer::Victim(frame_id_t *frame_id) {
  if(lru_list_.size()==0) return false;
  *frame_id = lru_list_.back();
  lru_list_.pop_back();
  return true;
}
/**
 * TODO: Student Implement
 */
void LRUReplacer::Pin(frame_id_t frame_id) {
  for(auto it = lru_list_.begin();it!=lru_list_.end();it++){
    if(*it==frame_id) {lru_list_.erase(it);break;}
  }
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Unpin(frame_id_t frame_id) {
  for(auto it = lru_list_.begin();it!=lru_list_.end();it++){
    if(*it==frame_id) return;
  }
  lru_list_.push_front(frame_id);
}

/**
 * TODO: Student Implement
 */
size_t LRUReplacer::Size() {
  return lru_list_.size();
}