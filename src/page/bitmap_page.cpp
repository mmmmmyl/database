#include "page/bitmap_page.h"

#include "glog/logging.h"

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  if(page_allocated_==GetMaxSupportedSize()) return false;
  page_allocated_++;
  page_offset = next_free_page_;
  bytes[page_offset/8] |= 0x1 << (7-page_offset%8);
  for(uint32_t offset = 0;offset < GetMaxSupportedSize();offset++){
    if(IsPageFree(offset)) next_free_page_ = offset;
  }
  return true;
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  if(IsPageFree(page_offset)) return false;
  page_allocated_--;
  bytes[page_offset/8] &= ~(0x1 << (7-page_offset%8));
  next_free_page_ = page_offset;
  return true;
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  return IsPageFreeLow(page_offset / 8,page_offset % 8);
}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  unsigned char byte = bytes[byte_index];
  if((0x1 << (7-bit_index)) & byte) return false;
  else return true;
}

template class BitmapPage<64>;

template class BitmapPage<128>;

template class BitmapPage<256>;

template class BitmapPage<512>;

template class BitmapPage<1024>;

template class BitmapPage<2048>;

template class BitmapPage<4096>;