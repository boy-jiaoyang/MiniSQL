#include "page/bitmap_page.h"

#include "glog/logging.h"

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  if(page_allocated_ == MAX_CHARS * 8) {
    return false;
  }
  page_offset = next_free_page_;
  uint32_t byte_index = page_offset / 8;
  uint8_t bit_index = page_offset % 8;
  bytes[byte_index] ^= (1 << bit_index);
  for(int i = next_free_page_ + 1; i < MAX_CHARS * 8; i++) {
    if(IsPageFree(i)) {
      next_free_page_ = i;
      break;
    }
  }
  for (int i = next_free_page_ - 1; i >= 0; i--) {
    if(IsPageFree(i)) {
      next_free_page_ = i;
      break;
    }
  }
  page_allocated_++;
  return true;
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  if(IsPageFree(page_offset)) {
    return false;
  }
  uint32_t byte_index = page_offset / 8;
  uint8_t bit_index = page_offset % 8;
  bytes[byte_index] ^= (1 << bit_index);
  next_free_page_ = page_offset;
  page_allocated_--;
  return true;
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  return IsPageFreeLow(page_offset / 8, page_offset % 8);
}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  if(byte_index >= MAX_CHARS) {
    return false;
  }
  return ((bytes[byte_index] >> bit_index) & 1) ^ 1;
}

template class BitmapPage<64>;

template class BitmapPage<128>;

template class BitmapPage<256>;

template class BitmapPage<512>;

template class BitmapPage<1024>;

template class BitmapPage<2048>;

template class BitmapPage<4096>;