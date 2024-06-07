#include "page/b_plus_tree_internal_page.h"

#include "index/generic_key.h"

#define pairs_off (data_)
#define pair_size (GetKeySize() + sizeof(page_id_t))
#define key_off 0
#define val_off GetKeySize()

/**
 * TODO: Student Implement
 */
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 * 1.
 */
void InternalPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) {
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetKeyAt(0, nullptr);
  SetMaxSize(max_size);
  SetSize(0);
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetKeySize(key_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey *InternalPage::KeyAt(int index) {
  return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}
/*
void InternalPage::SetKeyAt(int index, GenericKey *key) {
  memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}
*/
void InternalPage::SetKeyAt(int index, GenericKey *key) {
    auto address = pairs_off + index * pair_size + key_off;
    if (key == nullptr)
        memset(pairs_off + index * pair_size + key_off, 0, GetKeySize());
    else
        memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}

page_id_t InternalPage::ValueAt(int index) const {
  return *reinterpret_cast<const page_id_t *>(pairs_off + index * pair_size + val_off);
}

void InternalPage::SetValueAt(int index, page_id_t value) {
  *reinterpret_cast<page_id_t *>(pairs_off + index * pair_size + val_off) = value;
}

int InternalPage::ValueIndex(const page_id_t &value) const {
  for (int i = 0; i < GetSize(); ++i) {
    if (ValueAt(i) == value)
      return i;
  }
  return -1;
}

void *InternalPage::PairPtrAt(int index) {
  return KeyAt(index);
}

void InternalPage::PairCopy(void *dest, void *src, int pair_num) {
  memcpy(dest, src, pair_num * (GetKeySize() + sizeof(page_id_t)));
}
/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 * 用了二分查找
 */
page_id_t InternalPage::Lookup(const GenericKey *key, const KeyManager &KM) {
  int left = 1, right = GetSize()-1;
  int find_index = 0;
  while(left <= right) {
    int mid = (left + right) / 2;
    if(KM.CompareKeys(KeyAt(mid), key) <= 0) {
      find_index = mid;
      left = mid + 1;
    }else {
      right = mid - 1;
    }
  }
  return ValueAt(find_index);
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
void InternalPage::PopulateNewRoot(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
  SetSize(2);
  SetKeyAt(0, nullptr);
  SetKeyAt(1, new_key);
  SetValueAt(0, old_value);
  SetValueAt(1, new_value);
}

/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
int InternalPage::InsertNodeAfter(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
  IncreaseSize(1);
  if(old_value == INVALID_PAGE_ID) {
    PairCopy(PairPtrAt(1), PairPtrAt(0), GetSize() - 1);
    ASSERT(0 < GetSize(), "index out of bound");
    SetKeyAt(0, new_key);
    SetValueAt(0, new_value);
    return GetSize();
  }
  int index = ValueIndex(old_value);
  if(index == -1) return -1;
  PairCopy(PairPtrAt(index + 2), PairPtrAt(index + 1), GetSize() - 1 - (index + 1));
  ASSERT(index + 1 < GetSize(), "index out of bound");
  SetKeyAt(index + 1, new_key);
  SetValueAt(index + 1, new_value);
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 * buffer_pool_manager 是干嘛的？传给CopyNFrom()用于Fetch数据页
 */
void InternalPage::MoveHalfTo(InternalPage *recipient, BufferPoolManager *buffer_pool_manager) {
  ASSERT(recipient->GetSize() == 0, "recipient not empty");
  int half = GetSize() / 2;
  recipient->CopyNFrom(PairPtrAt(half), GetSize() - half, buffer_pool_manager);
  SetSize(half);
  auto middle_key = recipient->KeyAt(0);
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 *
 */
void InternalPage::CopyNFrom(void *src, int size, BufferPoolManager *buffer_pool_manager) {
  int old_size = GetSize();
  ASSERT(old_size + size <= GetMaxSize(), "size out of bound");
  SetSize(size + old_size);
  PairCopy(PairPtrAt(old_size), src, size);
  for (int i = old_size; i < GetSize(); ++i) {
    page_id_t child_page_id = ValueAt(i);
    auto child_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager->FetchPage(child_page_id));
    ASSERT(child_page != nullptr, "fetch child page failed");
    child_page->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(child_page_id, true);
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
void InternalPage::Remove(int index) {
  PairCopy(PairPtrAt(index), PairPtrAt(index + 1), GetSize() - 1 - index);
  SetSize(GetSize() - 1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
page_id_t InternalPage::RemoveAndReturnOnlyChild() {
  auto only_value = ValueAt(0);
  SetSize(0);
  return only_value;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
void InternalPage::MoveAllTo(InternalPage *recipient, GenericKey *middle_key, BufferPoolManager *buffer_pool_manager) {
  int old_size = GetSize();
  int recipient_old_size = recipient->GetSize();
  recipient->CopyNFrom(PairPtrAt(0), old_size, buffer_pool_manager);
  recipient->SetKeyAt(recipient_old_size, middle_key);
  SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
void InternalPage::MoveFirstToEndOf(InternalPage *recipient, GenericKey *middle_key,
                                    BufferPoolManager *buffer_pool_manager) {
  recipient->CopyLastFrom(middle_key, ValueAt(0), buffer_pool_manager);
  Remove(0);
  auto new_middle_key = KeyAt(0);
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyLastFrom(GenericKey *key, const page_id_t value, BufferPoolManager *buffer_pool_manager) {
  int old_size = GetSize();
  IncreaseSize(1);
  SetKeyAt(old_size, key);//在末尾添加新的键值对
  SetValueAt(old_size, value);//设置新值
  page_id_t child_page_id = value;
  auto child_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager->FetchPage(child_page_id));//获取新值指向的子页面
  ASSERT(child_page != nullptr, "fetch child page failed");
  child_page->SetParentPageId(GetPageId());//更新该子页面的父页面ID为当前页面的ID
  buffer_pool_manager->UnpinPage(child_page_id, true);

}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
//将当前页面的最后一个键值对移动到另一个内部页面的最前面（包括pushback和pushfront）
void InternalPage::MoveLastToFrontOf(InternalPage *recipient, GenericKey *middle_key,
                                     BufferPoolManager *buffer_pool_manager) {
  int old_size = GetSize();
  recipient->SetKeyAt(0, middle_key);//更新recipient的第一个键为middle_key
  recipient->CopyFirstFrom(ValueAt(old_size - 1), buffer_pool_manager);//接收页面，添加到reci的前端
  //auto new_middle_key = KeyAt(old_size - 1);
  Remove(old_size - 1);//移除当前页面的最后一个键值对
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyFirstFrom(const page_id_t value, BufferPoolManager *buffer_pool_manager) {
  int old_size = GetSize();
  SetSize(old_size + 1);
  PairCopy(PairPtrAt(1), PairPtrAt(0), old_size);//所有键值后移一位
  //SetPairAt(0, nullptr, value);//设置新的第一键值对
  ASSERT(0 < GetSize(), "index out of bound");
  SetKeyAt(0, nullptr);
  SetValueAt(0, value);
  page_id_t child_page_id = value;
  auto child_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager->FetchPage(child_page_id));
  child_page->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(child_page_id, true);
}