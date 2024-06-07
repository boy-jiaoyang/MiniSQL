#include "index/index_iterator.h"

#include "index/basic_comparator.h"
#include "index/generic_key.h"

IndexIterator::IndexIterator() : current_page_id(INVALID_PAGE_ID), item_index(-1), buffer_pool_manager(nullptr) {}

IndexIterator::IndexIterator(page_id_t page_id, BufferPoolManager *bpm, int index)
    : current_page_id(page_id), item_index(index), buffer_pool_manager(bpm) {
  page = reinterpret_cast<LeafPage *>(buffer_pool_manager->FetchPage(current_page_id)->GetData());
}

IndexIterator::~IndexIterator() {
  if (current_page_id != INVALID_PAGE_ID)
    buffer_pool_manager->UnpinPage(current_page_id, false);
}

std::pair<GenericKey *, RowId> IndexIterator::operator*() {
  return page->GetItem(item_index);
}

IndexIterator &IndexIterator::operator++() {//++
  if (item_index < page->GetSize() - 1) {//index is not the end
    item_index++;
  } else {
    //check if the next page exists
    page_id_t next_page_id = page->GetNextPageId();
    if (next_page_id == INVALID_PAGE_ID) {
      //reaches the end
      current_page_id = INVALID_PAGE_ID;
      item_index = -1;
      buffer_pool_manager->UnpinPage(page->GetPageId(), false);
      page = nullptr;
    } else {
      //not the end, move to the first element in the next page
      buffer_pool_manager->UnpinPage(page->GetPageId(), false);
      page = reinterpret_cast<LeafPage *>(buffer_pool_manager->FetchPage(next_page_id));
      current_page_id = next_page_id;
      item_index = 0;
    }
  }
  return *this;
}

bool IndexIterator::operator==(const IndexIterator &itr) const {
  return current_page_id == itr.current_page_id && item_index == itr.item_index;
}

bool IndexIterator::operator!=(const IndexIterator &itr) const {
  return !(*this == itr);
}