#include "buffer/buffer_pool_manager.h"

#include "glog/logging.h"
#include "page/bitmap_page.h"

static const char EMPTY_PAGE_DATA[PAGE_SIZE] = {0};

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager) {
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size_);
  for (size_t i = 0; i < pool_size_; i++) {
    free_list_.emplace_back(i);
  }
}

BufferPoolManager::~BufferPoolManager() {
  for (auto page : page_table_) {
    FlushPage(page.first);
  }
  delete[] pages_;
  delete replacer_;
}

/**
 * TODO: Student Implement
 */
Page *BufferPoolManager::FetchPage(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  if(page_id == INVALID_PAGE_ID) {
    return nullptr;
  }
  if(page_table_.find(page_id) != page_table_.end()) {
    auto find_page_id = page_table_.find(page_id);
    replacer_->Pin(find_page_id->second);
    return &pages_[find_page_id->second];
  }else {
    frame_id_t cache_page_frame_id;
    if(free_list_.empty()) {
      if(replacer_->Size() > 0) {
        replacer_->Victim(&cache_page_frame_id);
        if(pages_[cache_page_frame_id].IsDirty()) {
          FlushPage(page_id);
        }
        page_table_.erase(pages_[cache_page_frame_id].page_id_);
      }else {
        return nullptr;
      }
    }else {
      auto free_id = free_list_.begin();
      cache_page_frame_id = *free_id;
      free_list_.erase(free_id);
    }
    page_table_.emplace(page_id, cache_page_frame_id);
    disk_manager_->ReadPage(page_id, pages_[cache_page_frame_id].data_);
    pages_[cache_page_frame_id].page_id_ = page_id;
    pages_[cache_page_frame_id].is_dirty_ = false;
    if(pages_[cache_page_frame_id].pin_count_ == 0) {
      pages_[cache_page_frame_id].pin_count_  = 1;
      replacer_->Pin(cache_page_frame_id);
    }
    return &pages_[cache_page_frame_id];
  }
}

/**
 * TODO: Student Implement
 */
/*
Page *BufferPoolManager::NewPage(page_id_t &page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  frame_id_t cache_page_frame_id;
  if(free_list_.empty()) {
    if(replacer_->Size() > 0) {
      replacer_->Victim(&cache_page_frame_id);
      page_table_.erase(pages_[cache_page_frame_id].page_id_);
    }else {
      return nullptr;
    }
  }else {
    auto free_id = free_list_.begin();
    cache_page_frame_id = *free_id;
    free_list_.erase(free_id);
  }
  page_id_t new_page_id = AllocatePage();
  page_table_.emplace(new_page_id, cache_page_frame_id);
  pages_[cache_page_frame_id].ResetMemory();
  pages_[cache_page_frame_id].page_id_ = new_page_id;
  page_id = new_page_id;
  pages_[cache_page_frame_id].is_dirty_ = false;
  pages_[cache_page_frame_id].pin_count_ = 1;
  replacer_->Pin(cache_page_frame_id);
  return &pages_[cache_page_frame_id];
}
*/
Page *BufferPoolManager::NewPage(page_id_t &page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.

  // 缓冲区有空余的页
  if(!free_list_.empty())
  {
    frame_id_t frame_id = free_list_.front(); // 获取第一个空页的页帧
    free_list_.erase(free_list_.begin()); // 从空页链表中删除第一个空页

    page_id = AllocatePage(); //从磁盘中获取一个空的页(逻辑号)
    if(page_id == INVALID_PAGE_ID) // 磁盘没有空的页返回
      return nullptr;

    // 更新Page的信息
    // 注意此时不能解除空页固定，空页相当于被Pin住了(空页不能被替换)
    pages_[frame_id].ResetMemory(); // 清空Page的数据
    pages_[frame_id].page_id_ = page_id;
    pages_[frame_id].is_dirty_ = false;
    pages_[frame_id].pin_count_ = 0;
    page_table_.insert({page_id, frame_id});// 向page table中插入对应关系
    return &pages_[frame_id];
  }

  // 内存中没有空页
  frame_id_t frame_id;
  replacer_->Victim(&frame_id);  // 根据replacer策略获得一个替换的页R
  if(frame_id == INVALID_FRAME_ID) // 如果所有页都被Pin了，返回nullpter
    return nullptr;

  page_id = AllocatePage(); //从磁盘中获取一个空的页(逻辑号)
  if(page_id == INVALID_PAGE_ID) // 磁盘没有空的页返回
    return nullptr;

  page_id_t replace_page_id = pages_[frame_id].GetPageId(); // 获取替换页对应的逻辑页号

  // 替换页是脏的，需要重新写回磁盘
  if(pages_[frame_id].IsDirty())
    disk_manager_->WritePage(replace_page_id, pages_[frame_id].GetData());
  // 从page_table_中删除替换页的映射关系 ---> 替换页变成空页
  auto iter = page_table_.find(replace_page_id);
  page_table_.erase(iter);

  // 修改信息
  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].pin_count_ = 0;
  pages_[frame_id].ResetMemory();
  page_table_.insert({page_id, frame_id});// 向page table中插入对应关系
  return &pages_[frame_id];
}
/**
 * TODO: Student Implement
 */
bool BufferPoolManager::DeletePage(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  auto page = page_table_.find(page_id);
  if(page_table_.find(page_id) == page_table_.end()) {
    return true;
  }else if(pages_[page->second].pin_count_ != 0) {
    return false;
  }
  frame_id_t cache_page_frame_id = page->second;
  if(pages_[cache_page_frame_id].IsDirty()) {
    FlushPage(page_id);
  }
  page_table_.erase(page_id);
  pages_[cache_page_frame_id].pin_count_ = 0;
  pages_[cache_page_frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[cache_page_frame_id].is_dirty_ = false;
  free_list_.push_back(cache_page_frame_id);
  DeallocatePage(page_id);
  return true;
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  if(page_table_.find(page_id) == page_table_.end()) {
    return true;
  }else {
    auto page = page_table_.find(page_id);
    frame_id_t cache_page_frame_id = page->second;
    if(pages_[cache_page_frame_id].pin_count_ != 0) {
      pages_[cache_page_frame_id].pin_count_ = 0;
      if(is_dirty) {
        pages_[cache_page_frame_id].is_dirty_ = true;
      }
      replacer_->Unpin(cache_page_frame_id);
    }
    return true;
  }
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::FlushPage(page_id_t page_id) {
  if(page_table_.find(page_id) != page_table_.end()) {
    auto page = page_table_.find(page_id);
    disk_manager_->WritePage(page_id, pages_[page->second].data_);
  }
  return true;
}

page_id_t BufferPoolManager::AllocatePage() {
  int next_page_id = disk_manager_->AllocatePage();
  return next_page_id;
}

void BufferPoolManager::DeallocatePage(__attribute__((unused)) page_id_t page_id) {
  disk_manager_->DeAllocatePage(page_id);
}

bool BufferPoolManager::IsPageFree(page_id_t page_id) {
  return disk_manager_->IsPageFree(page_id);
}

// Only used for debug
bool BufferPoolManager::CheckAllUnpinned() {
  bool res = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ != 0) {
      res = false;
      LOG(ERROR) << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
    }
  }
  return res;
}