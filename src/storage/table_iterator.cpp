#include "storage/table_iterator.h"

#include "common/macros.h"
#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
TableIterator::TableIterator(TableHeap *table_heap, RowId rid, Txn *txn, bool is_begin,
                        bool is_end) :table_heap_(table_heap), rid_(rid), txn_(txn){ is_begin_=is_begin; is_end_=is_end;}

// TableIterator::TableIterator(const TableHeap *table_heap, const RowId &rid, const Txn *txn, bool is_begin = false,
//                 bool is_end = false);

TableIterator::TableIterator(const TableIterator &other) {
  table_heap_ = other.table_heap_;
  rid_.Set(other.rid_.GetPageId(), other.rid_.GetSlotNum());
  txn_ = other.txn_;
  is_begin_ = other.is_begin_;
  is_end_ = other.is_end_;
}

TableIterator::~TableIterator() {

}

bool TableIterator::operator==(const TableIterator &itr) const {
  if (!(table_heap_ == itr.table_heap_)) return false;
  if (rid_ == itr.rid_) return true;
  if (is_begin_ == itr.is_begin_) return true;
  if (is_end_ == itr.is_end_) return true;
  return false;
}

bool TableIterator::operator!=(const TableIterator &itr) const {
  if (!(table_heap_ == itr.table_heap_)) return true;
  if (!(rid_ == itr.rid_)) return true;
  if (is_begin_ != itr.is_begin_) return true;
  if (is_end_ != itr.is_end_) return true;
  return false;
}

const Row &TableIterator::operator*() {
  ASSERT(false, "Not implemented yet.");
}

Row *TableIterator::operator->() {
  return nullptr;
}

TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept {
  ASSERT(false, "Not implemented yet.");
}

// ++iter
TableIterator &TableIterator::operator++() {
  auto page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(rid_.GetPageId()));
  if (page == nullptr) {
    return *this;
  }
  page->WLatch();
  RowId next_rid;
  bool flag = false;
  if (page->GetNextTupleRid(rid_, &next_rid)) {
    rid_ = next_rid;
  } else {
    while (page->GetNextPageId() != INVALID_PAGE_ID) {
      page->WUnlatch();
      page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(page->GetNextPageId()));
      if (page == nullptr) {
        return *this;
      }
      page->WLatch();
      if (page->GetFirstTupleRid(&next_rid)) {flag=true;break;}
      else {page->WUnlatch();}
    }
  }
  page->WUnlatch();
  if(flag) rid_ = next_rid;
  else{
    rid_.Set(INVALID_PAGE_ID, 0);
    is_end_ = true;
  }
  return *this;
}

// iter++
TableIterator TableIterator::operator++(int) { 
  TableIterator ret_val(*this);
  ++(*this);
  return ret_val;
 }
