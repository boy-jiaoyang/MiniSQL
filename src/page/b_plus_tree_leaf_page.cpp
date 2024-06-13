#include "page/b_plus_tree_leaf_page.h"

#include <algorithm>

#include "index/generic_key.h"
//ok
#define pairs_off (data_)
#define pair_size (GetKeySize() + sizeof(RowId))
#define key_off 0
#define val_off GetKeySize()
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * TODO: Student Implement
 */
/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 * 未初始化next_page_id
 */
void LeafPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) {
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetSize(0);
  SetPageType(IndexPageType::LEAF_PAGE);
  SetKeySize(key_size);
  SetNextPageId(INVALID_PAGE_ID);
}

/**
 * Helper methods to set/get next page id
 */
page_id_t LeafPage::GetNextPageId() const {
  return next_page_id_;
}

void LeafPage::SetNextPageId(page_id_t next_page_id) {
  next_page_id_ = next_page_id;
  if (next_page_id == 0) {
//    LOG(INFO) << "Fatal error: LeafPage SetNextPageId = 0";
  }
}

/**
 * TODO: Student Implement
 */
/**
 * Helper method to find the first index i so that pairs_[i].first >= key
 * NOTE: This method is only used when generating index iterator
 * 二分查找
 * add: if not found return -1(但是没有具体体现（？
 */
int LeafPage::KeyIndex(const GenericKey *key, const KeyManager &KM) {
  if(GetSize() == 0) {
    return 0;
  }
  int left = 0;
  int right = GetSize() - 1;
  int find_index = -1;  // 默认值为-1，如果没有找到合适的键
  while (left <= right) {
    int mid = left + (right - left) / 2;
    auto key_at_middle = KeyAt(mid);
    if (KM.CompareKeys(key_at_middle, key) > 0) {
      find_index = mid;
      right = mid - 1;
    } else if(KM.CompareKeys(key_at_middle, key) < 0){
      left = mid + 1;
    } else {
      find_index = mid;
      break;
    }
  }
  return find_index;
}


//一个可能会用到的函数（？
int LeafPage::KeyFind(const GenericKey *key, const KeyManager &KM) {
    int greater_or_equal_index = KeyIndex(key, KM);
    if (greater_or_equal_index == -1 || KM.CompareKeys(KeyAt(greater_or_equal_index), key) != 0) {
        return -1;
    } else {
        return greater_or_equal_index;
    }
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey *LeafPage::KeyAt(int index) {
  return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}

void LeafPage::SetKeyAt(int index, GenericKey *key) {
  memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}

RowId LeafPage::ValueAt(int index) const {
  return *reinterpret_cast<const RowId *>(pairs_off + index * pair_size + val_off);
}

void LeafPage::SetValueAt(int index, RowId value) {
  *reinterpret_cast<RowId *>(pairs_off + index * pair_size + val_off) = value;
}

void *LeafPage::PairPtrAt(int index) {
  return KeyAt(index);
}

void LeafPage::PairCopy(void *dest, void *src, int pair_num) {
  memcpy(dest, src, pair_num * (GetKeySize() + sizeof(RowId)));
}
/*
*void LeafPage::SetPairAt(int index, GenericKey *key, const RowId &value) {
    SetKeyAt(index, key);
    SetValueAt(index, value);
}*/

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a. array offset)
 */
std::pair<GenericKey *, RowId> LeafPage::GetItem(int index) {
  if (index < 0 || index >= GetSize()) {//index超出范围弹出
    return std::make_pair(nullptr, RowId(0));
  }
  GenericKey *key = KeyAt(index);
  RowId value = ValueAt(index);
  return std::make_pair(key, value);
  return {KeyAt(index), ValueAt(index)};
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return page size after insertion
 *
 * 1. Find the first index i so that pairs_[i].first >= key
 * 2. Insert the key & value pair at the index i
 * 3. Return the page size after insertion
 * 4. If the key already exists, return immediately
 * 5. If the page is full, return immediately
 * 6. If the key does not exist, insert the key & value pair at the index i
 */
int LeafPage::Insert(GenericKey *key, const RowId &value, const KeyManager &KM) {
  int index = KeyIndex(key, KM);
  if (index == -1) {
    SetKeyAt(GetSize(), key);
    SetValueAt(GetSize(), value);
    IncreaseSize(1);
    return GetSize();
  }
  PairCopy(PairPtrAt(index + 1), PairPtrAt(index), GetSize() - index);
  IncreaseSize(1);
  SetKeyAt(index, key);
  SetValueAt(index, value);
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 * 将当前叶节点的一半数据移动到一个新的、空的接收节点中
 *
 *
 */
void LeafPage::MoveHalfTo(LeafPage *recipient) {
  ASSERT(recipient->GetSize() == 0, "recipient is not empty");
  int half = GetSize() / 2;
  recipient->CopyNFrom(PairPtrAt(half), GetSize() - half);
  SetSize(half);
  recipient->SetNextPageId(GetNextPageId());
  SetNextPageId(recipient->GetPageId());
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
void LeafPage::CopyNFrom(void *src, int size) {
  int old_size = GetSize();
  ASSERT(size + GetSize() <= GetMaxSize(), "no enough space for copy n from");
  IncreaseSize(size);
  PairCopy(PairPtrAt(old_size), src, size);
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
bool LeafPage::Lookup(const GenericKey *key, RowId &value, const KeyManager &KM) {
  //int find_index = KeyFind(key, KM);
  //LOG(INFO)<<"Into Lookup";
  int find_index;
  int greater_or_equal_index = KeyIndex(key, KM);
  if (greater_or_equal_index == -1 || KM.CompareKeys(KeyAt(greater_or_equal_index), key) != 0) {
    //LOG(INFO)<<"RETURN FALSE";
    find_index = -1;
    return false;
  } else {
    //LOG(INFO)<<"RETURN TRUE";
    find_index = greater_or_equal_index;
    value = ValueAt(find_index);
    return true;
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * existed, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return  page size after deletion
 */
int LeafPage::RemoveAndDeleteRecord(const GenericKey *key, const KeyManager &KM) {
  int index = KeyIndex(key, KM);
  if(index != -1 && KM.CompareKeys(key, KeyAt(index)) == 0) {
    PairCopy(PairPtrAt(index), PairPtrAt(index + 1), GetSize() - index - 1);
    IncreaseSize(-1);
    return GetSize();
  }
  //无报错，不经过
  //LOG(ERROR) << "fail to find the key in RemoveAndDeleteRecord." << std::endl;
  return GetSize();
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 */
void LeafPage::MoveAllTo(LeafPage *recipient) {
  recipient->CopyNFrom(PairPtrAt(0), GetSize());
  recipient->SetNextPageId(GetNextPageId());
  SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 *
 */
void LeafPage::MoveFirstToEndOf(LeafPage *recipient) {
  if(GetSize() <= 0) {
    //    LOG(ERROR) << "No pair to be remove" << std::endl;
    return;
  }
  recipient->CopyLastFrom(KeyAt(0), ValueAt(0));
  PairCopy(PairPtrAt(0), PairPtrAt(1), GetSize() - 1);
  IncreaseSize(-1);
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
void LeafPage::CopyLastFrom(GenericKey *key, const RowId value) {
  SetKeyAt(GetSize(), key);
  SetValueAt(GetSize(), value);
  IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
void LeafPage::MoveLastToFrontOf(LeafPage *recipient) {
  recipient->CopyFirstFrom(KeyAt(GetSize() - 1), ValueAt(GetSize() - 1));
  PairCopy(PairPtrAt(GetSize() - 1), PairPtrAt(GetSize()), 0);
  SetSize(GetSize() - 1);
}

/*
 * Insert item at the front of my items. Move items accordingly.
 *
 */
void LeafPage::CopyFirstFrom(GenericKey *key, const RowId value) {
  PairCopy(KeyAt(1), KeyAt(0), GetSize());
  IncreaseSize(1);
  //SetPairAt(0, key, value);
  SetKeyAt(0, key);
  SetValueAt(0, value);
}