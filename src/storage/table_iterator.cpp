#include "storage/table_iterator.h"

#include "common/macros.h"
#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
TableIterator::TableIterator(TableHeap *table_heap, RowId rid, Txn *txn) {
  table_page_id_ = rid.GetPageId();
  ASSERT(table_page_id_ != INVALID_PAGE_ID, "table_page_ is INVALID_PAGE_ID");
  slot_id_ = rid.GetSlotNum();
  ASSERT(slot_id_ != INVALID_LSN, "slot_id_ is INVALID_SLOT_NUM");
  table_heap_ = table_heap;
}

TableIterator::TableIterator(const TableIterator &other) {
  table_page_id_ = other.table_page_id_;
  slot_id_ = other.slot_id_;
  table_heap_ = other.table_heap_;
}

TableIterator::TableIterator() {
  table_page_id_ = INVALID_PAGE_ID;
  slot_id_ = INVALID_LSN;
  table_heap_ = nullptr;
}

TableIterator::~TableIterator() {}

bool TableIterator::operator==(const TableIterator &itr) const {
  return table_page_id_ == itr.table_page_id_ && slot_id_ == itr.slot_id_ && table_heap_ == itr.table_heap_;
}

bool TableIterator::operator!=(const TableIterator &itr) const {
  return table_page_id_ != itr.table_page_id_ || slot_id_ != itr.slot_id_ || table_heap_ != itr.table_heap_;
}

const Row &TableIterator::operator*() {
  //ASSERT(false, "Not implemented yet.");
  if (table_page_id_ == INVALID_PAGE_ID || slot_id_ == INVALID_LSN || table_heap_ == nullptr) {
    // throw exception shows that the iterator is invalid
    throw std::out_of_range("iterator is invalid");
  }
  Row *row = new Row(RowId(table_page_id_, slot_id_));
  auto *table_page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(table_page_id_));
  // if you need multiple threads, need to add lock here
  table_page->GetTuple(row, table_heap_->schema_, nullptr, table_heap_->lock_manager_);
  table_heap_->buffer_pool_manager_->UnpinPage(table_page_id_, false);
  return *row;
}

Row *TableIterator::operator->() {
  //return nullptr;
  if (table_page_id_ == INVALID_PAGE_ID || slot_id_ == INVALID_LSN || table_heap_ == nullptr) {
    // throw exception shows that the iterator is invalid
    throw std::out_of_range("iterator is invalid");
  }
  Row *row = new Row(RowId(table_page_id_, slot_id_));
  auto *table_page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(table_page_id_));
  // if you need multiple threads, need to add lock here
  table_page->GetTuple(row, table_heap_->schema_, nullptr, table_heap_->lock_manager_);
  table_heap_->buffer_pool_manager_->UnpinPage(table_page_id_, false);
  return row;
}

TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept {
  //ASSERT(false, "Not implemented yet.");
  table_page_id_ = itr.table_page_id_;
  slot_id_ = itr.slot_id_;
  table_heap_ = itr.table_heap_;
  return *this;
}

// ++iter
TableIterator &TableIterator::operator++() {
  if (table_page_id_ == INVALID_PAGE_ID || slot_id_ == INVALID_LSN || table_heap_ == nullptr) {
    // throw exception shows that the iterator is invalid
    throw std::out_of_range("iterator is invalid");
  }
  auto *table_page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(table_page_id_));
  RowId rid(table_page->GetPageId(), slot_id_);
  RowId next_rid;
  if (table_page->GetNextTupleRid(rid, &next_rid)) {
    table_page_id_ = next_rid.GetPageId();
    slot_id_ = next_rid.GetSlotNum();
  } else {
    table_page_id_ = INVALID_PAGE_ID;
    slot_id_ = INVALID_LSN;
    while ((table_page_id_ = table_page->GetNextPageId()) != INVALID_PAGE_ID) {
      table_heap_->buffer_pool_manager_->UnpinPage(table_page_id_, false);
      table_page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(table_page_id_));
      if(table_page->GetFirstTupleRid(&next_rid)) {
        slot_id_ = next_rid.GetSlotNum();
        break;
      }
    }
    if(slot_id_ == INVALID_PAGE_ID) {
      table_page_id_ == INVALID_PAGE_ID;
    }
  }
  return *this;
}

// iter++
TableIterator TableIterator::operator++(int) {
  auto temp = TableIterator(*this);
  ++(*this);
  return temp;
}

