#include "page/bitmap_page.h"

#include "glog/logging.h"

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  for( int i = 0; i < MAX_CHARS; i++) {
    unsigned char test = 1;
    for(int j = 0; j < 8; j++) {
      if((bytes[i] & test) == 0) {
        page_offset = i * 8 + j;
        bytes[i] += test;
        page_allocated_++;
        return true;
      }else {
        test *= 2;
      }
    }
  }
  return false;
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  uint32_t bit = page_offset / 8;
  if(bit < MAX_CHARS) {
    int temp = page_offset % 8;
    unsigned char retest = 1;
    for(int i = 0; i < temp; i++) retest *= 2;
    if((bytes[bit] & retest) > 0) {
      bytes[bit] -= retest;
      page_allocated_--;
      return true;
    }
  }
  return false;
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