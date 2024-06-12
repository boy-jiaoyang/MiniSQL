#include "index/b_plus_tree.h"

#include <string>

#include "glog/logging.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/index_roots_page.h"

/**
 * TODO: Student Implement
 */
BPlusTree::BPlusTree(index_id_t index_id, BufferPoolManager *buffer_pool_manager, const KeyManager &KM,
                     int leaf_max_size, int internal_max_size)
    : index_id_(index_id),
      buffer_pool_manager_(buffer_pool_manager),
      processor_(KM),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      root_page_id_(INVALID_PAGE_ID){
  if(leaf_max_size_ == UNDEFINED_SIZE) {
    int DEFAULT_LEAF_MAX_SIZE = (PAGE_SIZE - LEAF_PAGE_HEADER_SIZE) / (KM.GetKeySize() + sizeof(RowId)) - 1;
    leaf_max_size_ = DEFAULT_LEAF_MAX_SIZE;
  }
  if(internal_max_size_ == UNDEFINED_SIZE) {
    int DEFAULT_INTERNAL_LEAF_MAX_SIZE = (PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / (KM.GetKeySize() + sizeof(page_id_t)) - 1;
    internal_max_size_ = DEFAULT_INTERNAL_LEAF_MAX_SIZE;
  }
  auto index_root_page = reinterpret_cast<IndexRootsPage *>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID));
  page_id_t root_page_id;
  if (index_root_page->GetRootId(index_id_, &root_page_id)) {
    root_page_id_ = root_page_id;
    buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, false);
  }
}



//destroy the b plus tree
void BPlusTree::DestroySubTree(page_id_t current_page_id) {
  auto current_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(current_page_id));
  if (current_page->IsLeafPage()) {
    buffer_pool_manager_->DeletePage(current_page_id);
  } else {
    auto internal_page = reinterpret_cast<BPlusTreeInternalPage *>(current_page);
    for (int i = 0; i < internal_page->GetSize(); i++) {
      DestroySubTree(internal_page->ValueAt(i));
    }
    buffer_pool_manager_->DeletePage(current_page_id);
  }
}

void BPlusTree::Destroy(page_id_t current_page_id) {
  auto index_root_page = reinterpret_cast<IndexRootsPage *>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID));
  page_id_t root_page_id;
  if (index_root_page->GetRootId(index_id_, &root_page_id)) {
    buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, false);
  } else {
    DestroySubTree(root_page_id);
    index_root_page->Delete(index_id_);
    buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
  }
  root_page_id_ = INVALID_PAGE_ID;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
bool BPlusTree::IsEmpty() const {
  if(root_page_id_ == INVALID_PAGE_ID) return true;
  return false;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 *
 *
 */
bool BPlusTree::GetValue(const GenericKey *key, std::vector<RowId> &result, Txn *transaction) {
  if(IsEmpty()) {
    //LOG(INFO)<<"EMPTY!";
    return false;
  } else {
    if(key==nullptr) {
      //LOG(INFO)<<"key blank";
    }
    auto *page = FindLeafPage(key, INVALID_PAGE_ID, false);
    if(page == nullptr) {
      //LOG(INFO)<<"page nullptr!";
      return false;
    }
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page->GetData());
    RowId val;
    bool Find = leaf->Lookup(key, val, processor_);
    if(Find) {
      result.push_back(val);
    }
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
    return Find;

    /*
    if(index == -1) {
      buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
      return false;
    } else {
      result.push_back(leaf_page->ValueAt(index));
      buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
      return true;
    }
    */
  }
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 *
 * 1. If the tree is empty, start a new tree.
 * 2. If the leaf page is not full, insert the key-value pair directly.
 * 3. If the leaf page is full, split the leaf page and insert the key-value pair.
 * 4. If the parent page is not full, insert the middle key and the new page id into the parent page.
 * 5. If the parent page is full, split the parent page and insert the middle key and the new page id into the parent page.
 * 6. Repeat step 4 until the root page is not full.
 * 7. If the root page is full, split the root page and create a new root page.
 * 8. Update the root page id in the header page.
 * 9. Unpin all the pages.
 * 10. Return true.
 *
 *
 */
bool BPlusTree::Insert(GenericKey *key, const RowId &value, Txn *transaction) {
  if (IsEmpty()) {
    StartNewTree(key, value);
    //LOG(INFO)<<"START";
    return true;
  } else {
    //LOG(INFO) << "BPlusTree::Insert() called" << std::endl;
    return InsertIntoLeaf(key, value, transaction);
  }
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 *
 * 1. Create a new leaf page.
 * 2. Insert the key-value pair into the leaf page.
 * 3. Update the root page id in the header page.
 * 4. Unpin the leaf page.
 * 5. Return.
 *
 */
void BPlusTree::StartNewTree(GenericKey *key, const RowId &value) {
  auto * page = buffer_pool_manager_->NewPage(root_page_id_);
  if(page == nullptr) {
    //    LOG(ERROR) << "out of memory" << std::endl;
  }
  auto * leaf = reinterpret_cast<LeafPage *>(page->GetData());
  leaf_max_size_ = 4064/(processor_.GetKeySize() + sizeof(value))-1;
  internal_max_size_ =  leaf_max_size_;
  if(internal_max_size_ < 2) {
    internal_max_size_ = 2, leaf_max_size_ = 2;
  }
  leaf->Init(root_page_id_, INVALID_PAGE_ID, processor_.GetKeySize(), leaf_max_size_);
  leaf->Insert(key, value, processor_);
  buffer_pool_manager_->UnpinPage(root_page_id_, true);
  UpdateRootPageId(1);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 *
 * 1. If the leaf page is not full, insert the key-value pair directly.
 * 2. If the leaf page is full, split the leaf page and insert the key-value pair.
 * 3. If the parent page is not full, insert the middle key and the new page id into the parent page.
 * 4. If the parent page is full, split the parent page and insert the middle key and the new page id into the parent page.
 * 5. Repeat step 4 until the root page is not full.
 * 6. If the root page is full, split the root page and create a new root page.
 * 7. Update the root page id in the header page.
 * 8. Unpin all the pages.
 * 9. Return true.
 *
 *
 */
bool BPlusTree::InsertIntoLeaf(GenericKey *key, const RowId &value, Txn *transaction) {
  RowId _value;
  auto * page = reinterpret_cast<LeafPage *>(FindLeafPage(key, INVALID_PAGE_ID,false)->GetData());
  if(page->Lookup(key, _value, processor_)) {
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    return false;
  } else {
    page->Insert(key, value, processor_);
    if(page->GetSize() >= page->GetMaxSize()) {
      auto *new_page = Split(page, transaction);
      new_page->SetNextPageId(page->GetNextPageId());
      page->SetNextPageId(new_page->GetPageId());
      InsertIntoParent(page, new_page->KeyAt(0), new_page, transaction);
      buffer_pool_manager_->UnpinPage(new_page->GetPageId(), true);
    }
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    return true;
  }
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 *
 * 1. Create a new page.
 * 2. Move the last half of the key-value pairs to the new page.
 * 3. Update the next page id of the new page.
 * 4. Update the next page id of the original page.
 * 5. Return the new page.
 * @return: newly created page
 *
 */
BPlusTreeInternalPage *BPlusTree::Split(InternalPage *node, Txn *transaction) {
  page_id_t new_page_id;
  auto new_page = reinterpret_cast<BPlusTreeInternalPage *>(buffer_pool_manager_->NewPage(new_page_id));
  if (new_page == nullptr) {
    LOG(FATAL) << "out of memory";
  }
  new_page->Init(new_page_id, node->GetParentPageId(), processor_.GetKeySize(), internal_max_size_);
  node->MoveHalfTo(new_page, buffer_pool_manager_);
  return new_page;
}
/*
 * 1.
 *
 */
BPlusTreeLeafPage *BPlusTree::Split(LeafPage *node, Txn *transaction) {
  page_id_t new_page_id;
  auto page = reinterpret_cast<BPlusTreeLeafPage *>(buffer_pool_manager_->NewPage(new_page_id));
  if (page == nullptr) {
    LOG(FATAL) << "out of memory";
    return nullptr;
  }
  BPlusTreeLeafPage *new_page = reinterpret_cast<LeafPage *>(page);
  new_page->Init(new_page_id, node->GetParentPageId(), node->GetKeySize(),node->GetMaxSize());
  node->MoveHalfTo(new_page);
  return new_page;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 *
 * 1. If the parent page is not full, insert the middle key and the new page id into the parent page.
 * 2. If the parent page is full, split the parent page and insert the middle key and the new page id into the parent page.
 * 3. Repeat step 2 until the root page is not full.
 * 4. If the root page is full, split the root page and create a new root page.
 * 5. Update the root page id in the header page.
 * 6. Unpin all the pages.
 * 7. Return.
 *
 */

void BPlusTree::InsertIntoParent(BPlusTreePage *old_node, GenericKey *key, BPlusTreePage *new_node, Txn *transaction) {
  // If the old node is the root, create a new root
  if (old_node->IsRootPage()) {
    // Create a new root page
    page_id_t new_root_page_id;
    auto root_page = reinterpret_cast<BPlusTreeInternalPage *>(buffer_pool_manager_->NewPage(new_root_page_id));
    if (root_page == nullptr) {
      LOG(FATAL) << "Out of memory";
    }
    // Initialize the new root page
    root_page->Init(new_root_page_id, INVALID_PAGE_ID, processor_.GetKeySize(), internal_max_size_);
    root_page->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    // Update the root page ID
    root_page_id_ = new_root_page_id;
    // Unpin the new root page
    buffer_pool_manager_->UnpinPage(new_root_page_id, true);
    // Update the parent page IDs of old_node and new_node
    old_node->SetParentPageId(new_root_page_id);
    new_node->SetParentPageId(new_root_page_id);
    return;
  }
  // Fetch the parent page
  auto parent_page = reinterpret_cast<BPlusTreeInternalPage *>(buffer_pool_manager_->FetchPage(old_node->GetParentPageId()));
  if (parent_page == nullptr) {
    LOG(FATAL) << "Parent page not found";
  }
  // Insert the key into the parent page if there's room
  if (parent_page->GetSize() < internal_max_size_) {
    parent_page->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
    return;
  }
  // If the parent page is full, split the parent page
  parent_page->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
  // Split the parent page
  GenericKey middle_key;
  auto new_internal_page = Split(parent_page, transaction);
  // Update the parent page IDs
  new_node->SetParentPageId(parent_page->GetParentPageId());
  new_internal_page->SetParentPageId(parent_page->GetParentPageId());
  // Recursively insert into the parent
  InsertIntoParent(parent_page, &middle_key, new_internal_page, transaction);
  // Unpin the pages
  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(new_internal_page->GetPageId(), true);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 *
 * 1. If the tree is empty, return immediately.
 * 2. If the leaf page is not empty, delete the key-value pair directly.
 * 3. If the leaf page is empty, delete the leaf page.
 * 4. If the leaf page is not full, return immediately.
 * 5. If the leaf page is full, coalesce or redistribute the leaf page.
 * 6. If the parent page is not full, return immediately.
 * 7. If the parent page is full, coalesce or redistribute the parent page.
 * 8. Repeat step 7 until the root page is not full.
 * 9. If the root page is full, coalesce or redistribute the root page.
 * 10. Update the root page id in the header page.
 * 11. Unpin all the pages.
 */
/*void BPlusTree::Remove(const GenericKey *key, Txn *transaction) {
  if (IsEmpty()) {
    return;
  }
  // 查找包含给定键的叶子页面
  auto leaf_page = reinterpret_cast<BPlusTreeLeafPage *>(FindLeafPage(key, false));
  if (leaf_page == nullptr) {
    return;
  }
  // 从叶子页面中删除键
  int old_size = leaf_page->GetSize();
  leaf_page->RemoveAndDeleteRecord(key, processor_);
  // 检查是否需要合并或重新分配
  if (leaf_page->GetSize() < leaf_page->GetMinSize()) {
    CoalesceOrRedistribute(leaf_page, transaction);
  }
  // 取消固定页面
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), old_size != leaf_page->GetSize());
}*/
void BPlusTree::Remove(const GenericKey *key, Txn *transaction) {
  if(IsEmpty()) return;
  auto * leaf = reinterpret_cast<LeafPage *>(FindLeafPage(key, INVALID_PAGE_ID,false)->GetData());
  int pre_size = leaf->GetSize();
  if(pre_size > leaf->RemoveAndDeleteRecord(key, processor_)) {
    CoalesceOrRedistribute(leaf, transaction);
  } else {//不进入
        //LOG(ERROR) << "Remove() : RemoveAndDeleteRecord() failed";无报错
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
    return;
  }
  buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
}

/* todo
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
/*
template <typename N>
bool BPlusTree::CoalesceOrRedistribute(N *&node, Txn *transaction) {
  LOG(INFO) << "CoalesceOrRedistribute() called";
  // 如果节点是根节点，则调整根节点
  if (node->IsRootPage()) {
    return AdjustRoot(node);
  }
  // 获取父节点
  page_id_t parent_id = node->GetParentPageId();
  auto * parent_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent_id) -> GetData());
  int index = parent_page->ValueIndex(node->GetPageId());
  // index 是需要接收节点的节点，所以如果 index 为 0，代表 index 是最左边的节点，需要从右边的节点接收
  // 1. 找到接收节点
  int recipient_index = index == 0 ? 1 : index - 1;
  page_id_t recipient_id = parent_page->ValueAt(recipient_index);
  auto * recipient_page = reinterpret_cast<N *>(buffer_pool_manager_->FetchPage(recipient_id)->GetData());
  // 2. 如果接收节点无法合并，则进行重新分配
  if (recipient_page->GetSize() + node->GetSize() >= node->GetMaxSize()) {
    Redistribute(recipient_page, node, index);
    buffer_pool_manager_->UnpinPage(recipient_page->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
    return false;
  } else {
    // 合并节点
    Coalesce(recipient_page, node, parent_page, index);
    buffer_pool_manager_->UnpinPage(recipient_page->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
    return true;
  }
}
*/
template <typename N>
bool BPlusTree::CoalesceOrRedistribute(N *&node, Txn *transaction) {
    //LOG(INFO) << "CoalesceOrRedistribute() called";
  bool _delete = false;
  if(node->IsRootPage()) {
    //LOG(INFO)<<"AdjustRoot CALLED";
    _delete = AdjustRoot(node);
  } else if (node->GetSize() >= node->GetMinSize()){
    //LOG(INFO)<<"RETURN ";
    return false;
  } else {
    //LOG(INFO)<<"WHEN else";
    page_id_t parent_id = node->GetParentPageId();
    auto * par = reinterpret_cast<InternalPage *>(
        buffer_pool_manager_->FetchPage(parent_id) -> GetData());
    int index = par->ValueIndex(node->GetPageId());
    int sib_index = index == 0 ? 1 : index - 1;
    page_id_t sibling_id = par->ValueAt(sib_index);
    auto * sibling = reinterpret_cast<N *>
        (buffer_pool_manager_->FetchPage(sibling_id)->GetData());
    if(node->GetSize() + sibling->GetSize() >= node->GetMaxSize()) {
      //LOG(INFO)<<"Redistribute called";
      Redistribute(sibling, node, index);
      buffer_pool_manager_->UnpinPage(par->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(sibling->GetPageId(), true);
    } else {
      //LOG(INFO)<<"Coalesce called";
      Coalesce(sibling, node, par, index);
      buffer_pool_manager_->UnpinPage(par->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(sibling->GetPageId(), true);
      _delete = 1;
    }
  }
  return _delete;
}


/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion happened
 */
bool BPlusTree::Coalesce(LeafPage *&neighbor_node, LeafPage *&node, InternalPage *&parent, int index,
                         Txn *transaction) {
  // 如果 index 为 0，代表 node 是最左边的节点，需要将 neighbor_node 的内容合并到 node
  int sib_index = index == 0 ? 1 : index - 1;
  if(index < sib_index) {
    neighbor_node->MoveAllTo(node);
    node->SetNextPageId(neighbor_node->GetNextPageId());
    //    buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), true);
    parent->Remove(sib_index);
  } else {
    node->MoveAllTo(neighbor_node);
    neighbor_node->SetNextPageId(node->GetNextPageId());
    //    buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), true);
    parent->Remove(index);
  }
  return CoalesceOrRedistribute(parent, transaction);
}

bool BPlusTree::Coalesce(InternalPage *&neighbor_node, InternalPage *&node, InternalPage *&parent, int index, Txn *transaction) {
  int sib_index = index == 0 ? 1 : index - 1;
  if(index < sib_index) {
    neighbor_node->MoveAllTo(node, parent->KeyAt(sib_index), buffer_pool_manager_);
    //    buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), true);
    parent->Remove(sib_index);
  } else {
    node->MoveAllTo(neighbor_node, parent->KeyAt(index), buffer_pool_manager_);
    //    buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), true);
    parent->Remove(index);
  }
  return CoalesceOrRedistribute(parent, transaction);
}


/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 *
 * 1. If index == 0, borrow the first element from the right neighbor node and move it to the end of the current node.
 * 2. If index != 0, borrow the last element from the left neighbor node and move it to the front of the current node.
 * 3. Update the parent node's key to reflect the change in the right neighbor.
 * 4. Unpin the parent page and mark it as dirty to reflect changes.
 */
void BPlusTree::Redistribute(LeafPage *neighbor_node, LeafPage *node, int index) {
  // Fetch the parent page of the node.
  auto *parent_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(node->GetParentPageId())->GetData());
  if (index == 0) {
    // Borrow the first element from the right neighbor node and move it to the end of the current node.
    neighbor_node->MoveFirstToEndOf(node);
    // Update the parent node's key to reflect the change in the right neighbor.
    parent_page->SetKeyAt(1, neighbor_node->KeyAt(0));
  } else {
    // Borrow the last element from the left neighbor node and move it to the front of the current node.
    neighbor_node->MoveLastToFrontOf(node);
    // Update the parent node's key to reflect the change in the current node.
    parent_page->SetKeyAt(index, node->KeyAt(0));
  }
  // Unpin the parent page and mark it as dirty to reflect changes.
  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
}

/*
 * redistribute InternalPage:
 *
 *
 */

void BPlusTree::Redistribute(InternalPage *neighbor_node, InternalPage *node, int index) {
  // Fetch the parent page.
  auto *parent_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(node->GetParentPageId())->GetData());
  if (index == 0) {
    // The current node is the leftmost child; borrow from the right neighbor.
    int middle_key_index = parent_page->ValueIndex(neighbor_node->GetPageId());
    auto old_middle_key = parent_page->KeyAt(middle_key_index);
    neighbor_node->MoveFirstToEndOf(node, parent_page->KeyAt(1), buffer_pool_manager_);
    parent_page->SetKeyAt(1, neighbor_node->KeyAt(0));
  } else {
    // The current node is not the leftmost child; borrow from the left neighbor.
    int middle_key_index = parent_page->ValueIndex(node->GetPageId());
    auto old_middle_key = parent_page->KeyAt(middle_key_index);
    neighbor_node->MoveLastToFrontOf(node, parent_page->KeyAt(index), buffer_pool_manager_);
    parent_page->SetKeyAt(index, node->KeyAt(0));
  }

  // Unpin the parent page and mark it as dirty to reflect changes.
  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
}


/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happened
 *
 * 1. If the old root node is empty, delete the root entry.
 * 2. If the old root node has only one child, update the root page id.
 * 3. If the old root node has more than one child, no adjustment is necessary.
 *
 */
/*
bool BPlusTree::AdjustRoot(BPlusTreePage *old_root_node) {
  // If the old root node is empty.
  if (old_root_node->GetSize() == 0 && old_root_node->IsLeafPage() ) {
    // Fetch the IndexRootsPage to delete the root entry.
    auto index_root_page = reinterpret_cast<IndexRootsPage *>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID));
    index_root_page->Delete(index_id_);
    buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
    // Update root_page_id to INVALID_PAGE_ID, as the tree is now empty.
    root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId();
    return true;
  }
  // If the old root node has only one child.
  else if (old_root_node->GetSize() == 1) {
    // Get the only child page id from the old root node.
    page_id_t new_root_page_id = reinterpret_cast<InternalPage *>(old_root_node)->RemoveAndReturnOnlyChild();
    root_page_id_ = new_root_page_id;
    // Fetch the new root page and update its parent page id.
    auto new_root_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_));
    new_root_page->SetParentPageId(INVALID_PAGE_ID);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    // Delete the old root page.
    buffer_pool_manager_->DeletePage(old_root_node->GetPageId());
    // Update the root page id in the IndexRootsPage.
    UpdateRootPageId();
    return true;
  }
  // If the old root node has more than one child, no adjustment is necessary.
  else {
    return false;
  }
}
*/
bool BPlusTree::AdjustRoot(BPlusTreePage *old_root_node) {
  //  LOG(INFO) << "AdjustRoot() called";
  if(old_root_node->IsLeafPage() && old_root_node->GetSize() == 0) {
    root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId(0);
    return true;
  } else if (!old_root_node->IsLeafPage() && old_root_node->GetSize() == 1) {
    auto root = reinterpret_cast<BPlusTree::InternalPage *>(old_root_node);
    auto * only_child = reinterpret_cast<BPlusTreePage *>
        (buffer_pool_manager_->FetchPage(root->ValueAt(0))->GetData());
    only_child->SetParentPageId(INVALID_PAGE_ID);
    root_page_id_ = only_child->GetPageId();
    UpdateRootPageId(0);
    buffer_pool_manager_->UnpinPage(only_child->GetPageId(), true);
    return true;
  }
  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the left most leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin() {
  // check noll tree
  if (IsEmpty()) {
    return IndexIterator();
  }

  // check leftmost node
  LeafPage *leftest_leaf_page = reinterpret_cast<LeafPage *>(FindLeafPage(nullptr, root_page_id_, true));

  // If the leftmost leaf page is empty, return an empty iterator.
  return IndexIterator(leftest_leaf_page->GetPageId(), buffer_pool_manager_, 0);
}


/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin(const GenericKey *key) {
  // 检查树是否为空
  if (IsEmpty()) {
    return IndexIterator();
  }
  // 查找包含给定键的叶页面
  auto leaf_page = reinterpret_cast<LeafPage *>(FindLeafPage(key, root_page_id_, false));
  if (leaf_page == nullptr) {
    return IndexIterator();
  }
  // 查找键在叶页面中的索引
  int index = leaf_page->KeyIndex(key, processor_);
  if (index == -1) {
    return IndexIterator();
  }
  // 返回指向给定键的迭代器
  return IndexIterator(leaf_page->GetPageId(), buffer_pool_manager_, index);
}


/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
IndexIterator BPlusTree::End() {
  return IndexIterator();
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * Note: the leaf page is pinned, you need to unpin it after use.
 *
 *
 *  @param key: the key to find
 */
/*
Page *BPlusTree::FindLeafPage(const GenericKey *key, page_id_t page_id, bool leftMost) {
  if(IsEmpty()) {
    return nullptr;
  } else {
    //start traversing from root page
    if(page_id == INVALID_PAGE_ID) page_id = root_page_id_;
    auto * start_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(page_id)->GetData());
    auto traversed_page = start_page;
    while(!traversed_page->IsLeafPage()) {
      auto internal_page = reinterpret_cast<InternalPage *>(traversed_page);
      page_id_t next_page_id;
      if(leftMost) {
        //check leftmost leaf node
        next_page_id = internal_page->ValueAt(0);
      } else {
        //check a particular leaf node
        next_page_id = internal_page->Lookup(key, processor_);
      }
      //cancel current page
      buffer_pool_manager_->UnpinPage(traversed_page->GetPageId(), false);
      traversed_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(next_page_id)->GetData());
    }
    return reinterpret_cast<Page *>(traversed_page);
  }

}
*/
Page *BPlusTree::FindLeafPage(const GenericKey *key, page_id_t page_id, bool leftMost) {
  if(page_id == INVALID_PAGE_ID) page_id = root_page_id_;
  auto * page = reinterpret_cast<BPlusTreePage *>
      (buffer_pool_manager_->FetchPage(page_id)->GetData());
  while(!page->IsLeafPage()) {
    auto inner = reinterpret_cast<InternalPage *>(page);
    page_id_t child_id = leftMost ? inner->ValueAt(0) : inner->Lookup(key, processor_);
    auto child_page = reinterpret_cast<BPlusTreePage *>
        (buffer_pool_manager_->FetchPage(child_id)->GetData());
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    page = child_page;
  }
  return reinterpret_cast<Page *>(page);
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, current_page_id> into header page instead of
 * updating it.
 */
void BPlusTree::UpdateRootPageId(int insert_record) {
  //get IndexRootPage
  auto index_root_page = reinterpret_cast<IndexRootsPage *>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID));
  if (insert_record != 0) {
    //insert a record <index_name, current_page_id> into header page
    index_root_page->Insert(index_id_, root_page_id_);
  } else {
    //update root page id in header page
    index_root_page->Update(index_id_, root_page_id_);
  }
  //unpin header page
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
}

/**
 * This method is used for debug only, You don't need to modify
 */
void BPlusTree::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out, Schema *schema) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId()
        << ",Parent=" << leaf->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      Row ans;
      processor_.DeserializeToKey(leaf->KeyAt(i), ans, schema);
      out << "<TD>" << ans.GetField(0)->toString() << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId()
        << ",Parent=" << inner->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        Row ans;
        processor_.DeserializeToKey(inner->KeyAt(i), ans, schema);
        out << ans.GetField(0)->toString();
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out, schema);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 */
void BPlusTree::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
      bpm->UnpinPage(internal->ValueAt(i), false);
    }
  }
}

bool BPlusTree::Check() {
  bool all_unpinned = buffer_pool_manager_->CheckAllUnpinned();
  if (!all_unpinned) {
    LOG(ERROR) << "problem in page unpin" << endl;
  }
  return all_unpinned;
}