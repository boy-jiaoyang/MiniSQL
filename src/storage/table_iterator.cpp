#include "storage/table_iterator.h"

#include "common/macros.h"
#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
TableIterator::TableIterator(TableHeap *table_heap, RowId rid, Txn *txn) {
  rid_ = rid;
  table_heap_ = table_heap;
  txn_ = txn;
}

TableIterator::TableIterator(TableHeap *table_heap, RowId &rid, Txn *txn, Row *row) {
  rid_ = rid;
  table_heap_ = table_heap;
  txn_ = txn;
  if (row) {
    row_ = new Row(*row);
  }
  else row_ = nullptr;
}

TableIterator::TableIterator(const TableIterator &other) {
  rid_ = other.rid_;
  table_heap_ = other.table_heap_;
  txn_ = other.txn_;
  if (other.row_) {
    row_ = new Row(*other.row_);
  }
  else row_ = nullptr;
}

TableIterator::~TableIterator() {
  if(!row_) delete row_;
}

bool TableIterator::operator==(const TableIterator &itr) const {
  return rid_ == itr.rid_ && table_heap_ == itr.table_heap_ && txn_ == itr.txn_;
}

bool TableIterator::operator!=(const TableIterator &itr) const {
  return !(rid_ == itr.rid_) || table_heap_ != itr.table_heap_ || txn_ != itr.txn_;
}

const Row &TableIterator::operator*() {
  //ASSERT(false, "Not implemented yet.");
  return *row_;
}

Row *TableIterator::operator->() {
  //return nullptr;
  return row_;
}

TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept {
  //ASSERT(false, "Not implemented yet.");
  if (itr.row_) {
    row_ = new Row(*itr.row_);
  }
  else row_ = nullptr;
  rid_ = itr.rid_;
  table_heap_ = itr.table_heap_;
  txn_ = itr.txn_;
  return *this;
}

// ++iter
TableIterator &TableIterator::operator++() {
  //ASSERT(false, "Not implemented yet.");
  //获取元组的内存位置
  ASSERT(row_ != nullptr, "Iterator is not null");
  page_id_t page_id = rid_.GetPageId();
  ASSERT(page_id != INVALID_PAGE_ID, "Invalid page id");
  auto page =reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(page_id));
  ASSERT(page_id == page->GetPageId(), "Page id errer");
  //获取下一个元组
  RowId next_rid;
  if (page->GetNextTupleRid(rid_, &next_rid)) {
    row_->GetFields().clear();
    rid_.Set(next_rid.GetPageId(), next_rid.GetSlotNum());
    row_->SetRowId(rid_);
    table_heap_->GetTuple(row_, nullptr);
    table_heap_->buffer_pool_manager_->UnpinPage(page_id, false);
    return *this;
  }
  //如果当前页没有下一个元组，则读取下一页
  page_id_t next_page_id = INVALID_PAGE_ID;
  while ((next_page_id = page->GetNextPageId()) != INVALID_PAGE_ID) {
    page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(next_page_id));
    if (page->GetFirstTupleRid(&next_rid)) {
      row_->GetFields().clear();
      rid_.Set(next_rid.GetPageId(), next_rid.GetSlotNum());
      row_->SetRowId(rid_);
      table_heap_->GetTuple(row_, nullptr);
      table_heap_->buffer_pool_manager_->UnpinPage(next_page_id, false);
      return *this;
    }
    table_heap_->buffer_pool_manager_->UnpinPage(next_page_id, false);
  }
  //如果没有下一页，则说明已经到达末尾
  rid_ = RowId(INVALID_PAGE_ID, 0);
  table_heap_->buffer_pool_manager_->UnpinPage(next_page_id, false);
  return *this;
}

// iter++
TableIterator TableIterator::operator++(int) {
  RowId rid = this->rid_;
  TableHeap *table_heap = this->table_heap_;
  Row *row = nullptr;
  Txn *txn = this->txn_;
  if(this->row_) {
    row = new Row(*this->row_);
  }else {
    row = this->row_;
  }
  ++(*this);
  return TableIterator(table_heap, rid, txn, row);
}

