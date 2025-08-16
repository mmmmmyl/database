#include "index/index_iterator.h"

#include "index/basic_comparator.h"
#include "index/generic_key.h"

IndexIterator::IndexIterator() = default;

IndexIterator::IndexIterator(page_id_t page_id, BufferPoolManager *bpm, int index)
    : current_page_id(page_id), item_index(index), buffer_pool_manager(bpm) {
  if(current_page_id!=INVALID_PAGE_ID)page = reinterpret_cast<LeafPage *>(buffer_pool_manager->FetchPage(current_page_id)->GetData());
}

IndexIterator::~IndexIterator() {
  if (current_page_id != INVALID_PAGE_ID)
    buffer_pool_manager->UnpinPage(current_page_id, false);
}

/**
 * TODO: Student Implement
 */
std::pair<GenericKey *, RowId> IndexIterator::operator*() {
  if(current_page_id == INVALID_PAGE_ID || page == nullptr || item_index >= page->GetSize()) {
    return {nullptr, RowId()};
  }
  return page->GetItem(item_index);
}

/**
 * TODO: Student Implement
 */
IndexIterator &IndexIterator::operator++() {
  if(current_page_id == INVALID_PAGE_ID || page == nullptr) {
    return *this;
  }
  if(item_index == (page->GetSize() - 1)) {
    // Move to next page
    if (page->GetNextPageId() != INVALID_PAGE_ID) {
      buffer_pool_manager->UnpinPage(current_page_id, false);
      current_page_id = page->GetNextPageId();
      page = reinterpret_cast<LeafPage *>(buffer_pool_manager->FetchPage(current_page_id)->GetData());
      item_index = 0;
    } else {
      current_page_id = INVALID_PAGE_ID;
      page = nullptr;
      item_index = 0;
    }
  } else {
    item_index+=1;
  }
  return *this;
}

bool IndexIterator::operator==(const IndexIterator &itr) const {
  return current_page_id == itr.current_page_id && item_index == itr.item_index;
}

bool IndexIterator::operator!=(const IndexIterator &itr) const {
  return !(*this == itr);
}