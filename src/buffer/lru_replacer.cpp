#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages){ max_pages_ = num_pages; }

LRUReplacer::~LRUReplacer() = default;

/**
 * TODO: Student Implement
 */
bool LRUReplacer::Victim(frame_id_t *frame_id) {
  if(lru_list_.empty()) {
    return false;
  }
  *frame_id = lru_list_.back();
  lru_list_.pop_back();
  lru_unpin_set_.erase(*frame_id);
  return true;
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Pin(frame_id_t frame_id) {
  if (Size() == max_pages_) {
    return;
  }else if(lru_unpin_set_.find(frame_id) != lru_unpin_set_.end() ) {
    lru_unpin_set_.erase(frame_id);
    lru_list_.remove(frame_id);
  }
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Unpin(frame_id_t frame_id) {
  if(Size() == max_pages_) {
    return;
  }else if(lru_unpin_set_.find(frame_id) == lru_unpin_set_.end()) {
    lru_unpin_set_.insert(frame_id);
    lru_list_.push_front(frame_id);
  }

}

/**
 * TODO: Student Implement
 */
size_t LRUReplacer::Size() {
  return lru_list_.size();
}