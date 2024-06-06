#include "storage/disk_manager.h"

#include <sys/stat.h>

#include <filesystem>
#include <stdexcept>

#include "glog/logging.h"
#include "page/bitmap_page.h"

DiskManager::DiskManager(const std::string &db_file) : file_name_(db_file) {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
  // directory or file does not exist
  if (!db_io_.is_open()) {
    db_io_.clear();
    // create a new file
    std::filesystem::path p = db_file;
    if (p.has_parent_path()) std::filesystem::create_directories(p.parent_path());
    db_io_.open(db_file, std::ios::binary | std::ios::trunc | std::ios::out);
    db_io_.close();
    // reopen with original mode
    db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
    if (!db_io_.is_open()) {
      throw std::exception();
    }
  }
  ReadPhysicalPage(META_PAGE_ID, meta_data_);
}

void DiskManager::Close() {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  WritePhysicalPage(META_PAGE_ID, meta_data_);
  if (!closed) {
    db_io_.close();
    closed = true;
  }
}

void DiskManager::ReadPage(page_id_t logical_page_id, char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  ReadPhysicalPage(MapPageId(logical_page_id), page_data);
}

void DiskManager::WritePage(page_id_t logical_page_id, const char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  WritePhysicalPage(MapPageId(logical_page_id), page_data);
}

/**
 * TODO: Student Implement
 */
page_id_t DiskManager::AllocatePage() {
  DiskFileMetaPage *metapage = reinterpret_cast<DiskFileMetaPage *>(GetMetaData());
  uint32_t num_extent = metapage->num_extents_;
  uint32_t allocated_extent_id = 0;
  for(allocated_extent_id = 0; allocated_extent_id < num_extent; allocated_extent_id++) {
    if(metapage->extent_used_page_[allocated_extent_id] < BitmapPage<4096>::GetMaxSupportedSize()) {
      break;
    }
  }
  uint32_t allocated_frame_id = allocated_extent_id * (BitmapPage<4096>::GetMaxSupportedSize() + 1) + 1;
  if(allocated_extent_id == num_extent) {
    auto new_extent = new BitmapPage<4090>();
    WritePhysicalPage(allocated_frame_id, reinterpret_cast<char *>(new_extent));
    metapage->num_extents_++;
    metapage->extent_used_page_[num_extent] = 0;
  }
  auto allocated_extent = new BitmapPage<4096>();
  ReadPhysicalPage(allocated_frame_id, reinterpret_cast<char *>(allocated_extent));
  uint32_t allocated_page_offset = -1;
  allocated_extent->AllocatePage(allocated_page_offset);
  metapage->num_allocated_pages_++;
  metapage->extent_used_page_[allocated_extent_id]++;
  WritePhysicalPage(allocated_frame_id, reinterpret_cast<char *>(allocated_extent));
  return allocated_extent_id * BitmapPage<4096>::GetMaxSupportedSize() + allocated_page_offset;
}

/**
 * TODO: Student Implement
 */
void DiskManager::DeAllocatePage(page_id_t logical_page_id) {
  DiskFileMetaPage *metapage = reinterpret_cast<DiskFileMetaPage *>(GetMetaData());
  uint32_t num_extent = metapage->num_extents_;
  uint32_t deallocated_id = logical_page_id / BitmapPage<4096>::GetMaxSupportedSize();
  if(deallocated_id > num_extent) {
    ASSERT(false, "Not implemented yet.");
  }
  uint32_t deallocated_frame_id = deallocated_id * (BitmapPage<4096>::GetMaxSupportedSize() + 1) + 1;
  uint32_t deallocated_page_offset = logical_page_id % BitmapPage<4096>::GetMaxSupportedSize();
  auto deallocated_extent = new BitmapPage<4096>();
  ReadPhysicalPage(deallocated_frame_id, reinterpret_cast<char *>(deallocated_extent));
  if(deallocated_extent == nullptr) {
    ASSERT(false, "It is null");
  }
  deallocated_extent->DeAllocatePage(deallocated_page_offset);
  metapage->num_allocated_pages_--;
  metapage->extent_used_page_[deallocated_id]--;
  WritePhysicalPage(deallocated_frame_id, reinterpret_cast<char *>(deallocated_extent));
}

/**
 * TODO: Student Implement
 */
bool DiskManager::IsPageFree(page_id_t logical_page_id) {
  DiskFileMetaPage *metapage = reinterpret_cast<DiskFileMetaPage *>(GetMetaData());
  uint32_t num_extent = metapage->num_extents_;
  uint32_t extent_id = logical_page_id / BitmapPage<4096>::GetMaxSupportedSize();
  if(extent_id < num_extent) {
    uint32_t extent_frame_id = extent_id * (BitmapPage<4096>::GetMaxSupportedSize() + 1) + 1;
    uint32_t extent_offset_id = logical_page_id % BitmapPage<4096>::GetMaxSupportedSize();
    auto extent = new BitmapPage<4096>();
    ReadPhysicalPage(extent_frame_id, reinterpret_cast<char *>(extent));
    if(extent != nullptr) {
      bool isnull = extent->IsPageFree(extent_offset_id);
      return isnull;
    }else {
      return false;
    }
  }
  return true;
}

/**
 * TODO: Student Implement
 */
page_id_t DiskManager::MapPageId(page_id_t logical_page_id) {
  uint32_t extent_page_number = (logical_page_id / BitmapPage<4096>::GetMaxSupportedSize() +1) + 1;
  return extent_page_number + logical_page_id;
  return 0;
}

int DiskManager::GetFileSize(const std::string &file_name) {
  struct stat stat_buf;
  int rc = stat(file_name.c_str(), &stat_buf);
  return rc == 0 ? stat_buf.st_size : -1;
}

void DiskManager::ReadPhysicalPage(page_id_t physical_page_id, char *page_data) {
  int offset = physical_page_id * PAGE_SIZE;
  // check if read beyond file length
  if (offset >= GetFileSize(file_name_)) {
#ifdef ENABLE_BPM_DEBUG
    LOG(INFO) << "Read less than a page" << std::endl;
#endif
    memset(page_data, 0, PAGE_SIZE);
  } else {
    // set read cursor to offset
    db_io_.seekp(offset);
    db_io_.read(page_data, PAGE_SIZE);
    // if file ends before reading PAGE_SIZE
    int read_count = db_io_.gcount();
    if (read_count < PAGE_SIZE) {
#ifdef ENABLE_BPM_DEBUG
      LOG(INFO) << "Read less than a page" << std::endl;
#endif
      memset(page_data + read_count, 0, PAGE_SIZE - read_count);
    }
  }
}

void DiskManager::WritePhysicalPage(page_id_t physical_page_id, const char *page_data) {
  size_t offset = static_cast<size_t>(physical_page_id) * PAGE_SIZE;
  // set write cursor to offset
  db_io_.seekp(offset);
  db_io_.write(page_data, PAGE_SIZE);
  // check for I/O error
  if (db_io_.bad()) {
    LOG(ERROR) << "I/O error while writing";
    return;
  }
  // needs to flush to keep disk file in sync
  db_io_.flush();
}