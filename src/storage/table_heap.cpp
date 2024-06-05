#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
bool TableHeap::InsertTuple(Row &row, Txn *txn) {
    // Step1: Find the first page with enough space, if no page has enough space, create a new page.
    if (last_page_id_ != INVALID_PAGE_ID) {
        auto last_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(last_page_id_));
        if (last_page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_)) {
            buffer_pool_manager_->UnpinPage(last_page_id_, true);
            return true;
        }
        page_id_t new_page_id = INVALID_PAGE_ID;
        auto new_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(new_page_id));
        ASSERT(new_page_id != INVALID_PAGE_ID, "new page fail in Insert tuple");
        new_page->Init(new_page_id, last_page_id_, log_manager_, txn);
        new_page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
        last_page->SetNextPageId(new_page_id);
        buffer_pool_manager_->UnpinPage(last_page_id_, true);
        buffer_pool_manager_->UnpinPage(new_page_id, true);
        last_page_id_ = new_page_id;
        return true;
    } else {
        page_id_t traverse_page_id = first_page_id_;
        while (traverse_page_id != INVALID_PAGE_ID) {
            auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(traverse_page_id));
            if(page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_)) {
                buffer_pool_manager_->UnpinPage(traverse_page_id, true);
                return true;
            }
            buffer_pool_manager_->UnpinPage(traverse_page_id, false);
            traverse_page_id = page->GetNextPageId();
        }
        TablePage *new_page = nullptr;
        page_id_t new_page_id = INVALID_PAGE_ID;
        do {
            new_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(new_page_id));
        }while (new_page == nullptr);
        if(traverse_page_id == INVALID_PAGE_ID) {
            first_page_id_ = new_page_id;
            last_page_id_ = new_page_id;
        }
        new_page->Init(new_page_id, traverse_page_id, log_manager_, txn);
        new_page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
        auto rear_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(traverse_page_id));
        rear_page->SetNextPageId(new_page_id);
        buffer_pool_manager_->UnpinPage(traverse_page_id, true);
        buffer_pool_manager_->UnpinPage(new_page_id, true);
        return true;
    }
}

bool TableHeap::MarkDelete(const RowId &rid, Txn *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the recovery.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

/**
 * TODO: Student Implement
 */
bool TableHeap::UpdateTuple(Row &row, const RowId &rid, Txn *txn) {
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
    if (page == nullptr) {
        return false;
    }
    Row old_tuple = Row(rid);
    uint8_t error_code = page->UpdateTuple(row, &old_tuple, schema_, txn, lock_manager_, log_manager_);
    if(error_code == 0) {
        buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
        return true;
    }else if(error_code == 1) {
        buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
        return false;
    }
    MarkDelete(rid, txn);
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
    InsertTuple(row, txn);
    return true;
}

/**
 * TODO: Student Implement
 */
void TableHeap::ApplyDelete(const RowId &rid, Txn *txn) {
  // Step1: Find the page which contains the tuple.
  // Step2: Delete the tuple from the page.
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
    ASSERT(page != nullptr, "Page not found.");
    page->ApplyDelete(rid, txn, log_manager_);
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

void TableHeap::RollbackDelete(const RowId &rid, Txn *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback to delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

/**
 * TODO: Student Implement
 */
bool TableHeap::GetTuple(Row *row, Txn *txn) {
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(row->GetRowId().GetPageId()));
    if (page == nullptr) {
        return false;
    }
    page->GetTuple(row, schema_, txn, lock_manager_);
    buffer_pool_manager_->UnpinPage(row->GetRowId().GetPageId(), false);
    return true;
}

void TableHeap::DeleteTable(page_id_t page_id) {
  if (page_id != INVALID_PAGE_ID) {
    auto temp_table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));  // 删除table_heap
    if (temp_table_page->GetNextPageId() != INVALID_PAGE_ID)
      DeleteTable(temp_table_page->GetNextPageId());
    buffer_pool_manager_->UnpinPage(page_id, false);
    buffer_pool_manager_->DeletePage(page_id);
  } else {
    DeleteTable(first_page_id_);
  }
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::Begin(Txn *txn) {
    RowId first_row_id;
    if (first_page_id_ == INVALID_PAGE_ID) {
        return TableIterator();
    }
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
    while (page != nullptr && page->GetFirstTupleRid(&first_row_id) == false) {
        buffer_pool_manager_->UnpinPage(first_page_id_, false);
        page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page->GetNextPageId()));
    }
    if (page == nullptr) {
        return TableIterator();
    } else {
        auto temp = TableIterator(this, first_row_id, txn);
        buffer_pool_manager_->UnpinPage(first_page_id_, false);
        return temp;
    }
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::End() {
    return TableIterator();
}
