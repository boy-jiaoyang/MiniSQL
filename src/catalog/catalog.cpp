#include "catalog/catalog.h"

void CatalogMeta::SerializeTo(char *buf) const {
  ASSERT(GetSerializedSize() <= PAGE_SIZE, "Failed to serialize catalog metadata to disk.");
  MACH_WRITE_UINT32(buf, CATALOG_METADATA_MAGIC_NUM);
  buf += 4;
  MACH_WRITE_UINT32(buf, table_meta_pages_.size());
  buf += 4;
  MACH_WRITE_UINT32(buf, index_meta_pages_.size());
  buf += 4;
  for (auto iter : table_meta_pages_) {
    MACH_WRITE_TO(table_id_t, buf, iter.first);
    buf += 4;
    MACH_WRITE_TO(page_id_t, buf, iter.second);
    buf += 4;
  }
  for (auto iter : index_meta_pages_) {
    MACH_WRITE_TO(index_id_t, buf, iter.first);
    buf += 4;
    MACH_WRITE_TO(page_id_t, buf, iter.second);
    buf += 4;
  }
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf) {
  // check valid
  uint32_t magic_num = MACH_READ_UINT32(buf);
  buf += 4;
  ASSERT(magic_num == CATALOG_METADATA_MAGIC_NUM, "Failed to deserialize catalog metadata from disk.");
  // get table and index nums
  uint32_t table_nums = MACH_READ_UINT32(buf);
  buf += 4;
  uint32_t index_nums = MACH_READ_UINT32(buf);
  buf += 4;
  // create metadata and read value
  CatalogMeta *meta = new CatalogMeta();
  for (uint32_t i = 0; i < table_nums; i++) {
    auto table_id = MACH_READ_FROM(table_id_t, buf);
    buf += 4;
    auto table_heap_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    meta->table_meta_pages_.emplace(table_id, table_heap_page_id);
  }
  for (uint32_t i = 0; i < index_nums; i++) {
    auto index_id = MACH_READ_FROM(index_id_t, buf);
    buf += 4;
    auto index_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    meta->index_meta_pages_.emplace(index_id, index_page_id);
  }
  return meta;
}

/**
 * TODO: Student Implement
 */
uint32_t CatalogMeta::GetSerializedSize() const {
  uint32_t size = 4 * 3 + 8 * (table_meta_pages_.size() + index_meta_pages_.size());
  return size;
}

CatalogMeta::CatalogMeta() {}

/**
 * TODO: Student Implement
 */
CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
    : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager), log_manager_(log_manager) {
//    ASSERT(false, "Not Implemented ");
  if(init) {
    catalog_meta_ = CatalogMeta::NewInstance();
    next_index_id_.store(0);
    next_table_id_.store(0);
  }else {
    Page *meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
    catalog_meta_ = CatalogMeta::DeserializeFrom(meta_page->GetData());
    next_index_id_.store(catalog_meta_->GetNextIndexId());
    next_table_id_.store(catalog_meta_->GetNextTableId());
    for(auto item : catalog_meta_->table_meta_pages_) {
      if(item.first >= next_table_id_) next_table_id_.store(item.first + 1);
      TableMetadata *old_table_metadata = nullptr;
      Page *old_meta_page = buffer_pool_manager_->FetchPage(item.second);
      //反序列化
      old_table_metadata->DeserializeFrom(old_meta_page->GetData(), old_table_metadata);
      table_names_[old_table_metadata->GetTableName()]= item.first;
      TableHeap *old_table_heap = nullptr;
      TableInfo *old_table_info = nullptr;
      old_table_heap = old_table_heap->Create(buffer_pool_manager_, old_table_metadata->GetFirstPageId(), old_table_metadata->GetSchema(), log_manager_, lock_manager_);
      old_table_info = old_table_info->Create();
      old_table_info->Init(old_table_metadata, old_table_heap);
      tables_.emplace(item.first, old_table_info);
    }
    for(auto item : catalog_meta_->index_meta_pages_) {
      if(item.first >= next_index_id_) next_index_id_.store(item.first + 1);
      IndexMetadata *old_index_metadata = nullptr;
      Page *old_index_page = buffer_pool_manager_->FetchPage(item.second);
      //反序列化
      old_index_metadata->DeserializeFrom(old_index_page->GetData(), old_index_metadata);
      //查表
      string old_table_name = tables_[old_index_metadata->GetTableId()]->GetTableName();
      string old_index_name = old_index_metadata->GetIndexName();
      index_names_[old_table_name][old_index_name] = item.first;
      IndexInfo *old_index_info = nullptr;
      old_index_info = old_index_info->Create();
      old_index_info->Init(old_index_metadata, tables_[old_index_metadata->GetTableId()], buffer_pool_manager_);
      indexes_.emplace(item.first, old_index_info);
    }
  }
}

CatalogManager::~CatalogManager() {
  FlushCatalogMetaPage();
  delete catalog_meta_;
  for (auto iter : tables_) {
    delete iter.second;
  }
  for (auto iter : indexes_) {
    delete iter.second;
  }
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema, Txn *txn, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  //return DB_FAILED;
  auto item = table_names_.find(table_name);
  if(item != table_names_.end()) return DB_TABLE_ALREADY_EXIST;

  table_id_t new_table_id = 0;
  new_table_id = catalog_meta_->GetNextTableId();
  table_names_[table_name] = new_table_id;

  page_id_t new_page_id = 0;
  Page *new_page = nullptr;
  new_page = buffer_pool_manager_->NewPage(new_page_id);


  TableHeap *new_table_heap = nullptr;
  TableSchema *new_schema = nullptr;
  new_schema = new_schema->DeepCopySchema(schema);
  new_table_heap = new_table_heap->Create(buffer_pool_manager_, new_schema, txn, log_manager_, lock_manager_);
  page_id_t new_heap_id = new_table_heap->GetFirstPageId();

  TableMetadata *new_table_metadata = nullptr;
  new_table_metadata = new_table_metadata->Create(new_table_id, table_name, new_heap_id, new_schema);
  //序列化
  new_table_metadata->SerializeTo(new_page->GetData());
  //建tableinfo
  table_info = table_info->Create();
  table_info->Init(new_table_metadata, new_table_heap);
  tables_.emplace(new_table_id, table_info);
  catalog_meta_->table_meta_pages_.emplace(new_table_id, new_page_id);

  FlushCatalogMetaPage();
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  //return DB_FAILED;
  auto item = table_names_.find(table_name);
  if(item == table_names_.end()) return DB_TABLE_NOT_EXIST;
  auto find_table_info = tables_.find(item->second);
  table_info = find_table_info->second;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  // ASSERT(false, "Not Implemented yet");
  //return DB_FAILED;
  if(tables_.size() == 0) return DB_FAILED;
  for(auto item = tables_.begin(); item != tables_.end(); item++) {
    tables.push_back(item->second);
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Txn *txn, IndexInfo *&index_info,
                                    const string &index_type) {
  // ASSERT(false, "Not Implemented yet");
  //return DB_FAILED;
  auto find_table = table_names_.find(table_name);
  if(find_table == table_names_.end()) return DB_TABLE_NOT_EXIST;
  auto find_index = index_names_.find(table_name);
  if(find_index != index_names_.end()) {
    auto find_index_name = find_index->second.find(index_name);
    if(find_index_name != find_index->second.end()) return DB_INDEX_ALREADY_EXIST;
  }
  //初始化
  table_id_t table_id = 0;
  TableSchema *table_schema = nullptr;
  TableInfo *table_info = nullptr;
  //获取信息
  table_id = find_table->second;
  table_info = tables_[table_id];
  table_schema = table_info->GetSchema();
  //构建索引映射
  std::vector<uint32_t> new_key_map;
  uint32_t column_index = 0;
  for(auto item : index_keys) {
    if(table_schema->GetColumnIndex(item, column_index) == DB_COLUMN_NAME_NOT_EXIST) {
      return DB_COLUMN_NAME_NOT_EXIST;
    }
    new_key_map.push_back(column_index);
  }
  //写入磁盘
  page_id_t new_page_id = 0;
  index_id_t index_id = 0;
  Page *new_page = buffer_pool_manager_->NewPage(new_page_id);
  index_id = catalog_meta_->GetNextIndexId();

  IndexMetadata *new_index_metadata = nullptr;
  new_index_metadata = new_index_metadata->Create(index_id, index_name, table_id, new_key_map);
  //序列化
  new_index_metadata->SerializeTo(new_page->GetData());
  //建indexinfo
  index_info = index_info->Create();
  index_info->Init(new_index_metadata, tables_[find_table->second], buffer_pool_manager_);
  index_names_[table_name][index_name] = index_id;
  indexes_.emplace(index_id, index_info);

  catalog_meta_->index_meta_pages_.emplace(index_id, new_page_id);
  FlushCatalogMetaPage();
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);

  //将表数据插回索引
  auto this_index = index_info->GetIndex();
  auto table_heap = table_info->GetTableHeap();
  vector<Field> fields;
  for(auto iter = table_heap->Begin(txn); iter != table_heap->End(); iter++) {
    fields.clear();
    for(auto item : new_key_map) {
      fields.push_back(*iter->GetField(item));
    }
    Row row(fields);
    RowId rid = iter->GetRowId();
    this_index->InsertEntry(row, rid, txn);
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  // ASSERT(false, "Not Implemented yet");
  //return DB_FAILED;
  auto find_table = table_names_.find(table_name);
  if(find_table == table_names_.end()) return DB_TABLE_NOT_EXIST;
  auto find_index = index_names_.find(table_name);
  if(find_index == index_names_.end()) return DB_INDEX_NOT_FOUND;
  auto find_index_name = find_index->second.find(index_name);
  if(find_index_name == find_index->second.end()) return DB_INDEX_NOT_FOUND;
  auto find_index_info = indexes_.find(find_index_name->second);
  index_info = find_index_info->second;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  // ASSERT(false, "Not Implemented yet");
  //return DB_FAILED;
  auto find_table = table_names_.find(table_name);
  if(find_table == table_names_.end()) return DB_TABLE_NOT_EXIST;
  auto find_index = index_names_.find(table_name);
  if(find_index == index_names_.end()) return DB_INDEX_NOT_FOUND;
  for(auto item = find_index->second.begin(); item != find_index->second.end(); item++) {
    IndexInfo *index_info = nullptr;
    GetIndex(table_name, item->first, index_info);
    indexes.push_back(index_info);
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropTable(const string &table_name) {
  // ASSERT(false, "Not Implemented yet");
  //return DB_FAILED;
  auto item = table_names_.find(table_name);
  if(item == table_names_.end()) return DB_TABLE_NOT_EXIST;
  table_id_t drop_table_id = table_names_[table_name];
  TableInfo *drop_table_info = tables_[drop_table_id];
  drop_table_info->~TableInfo();
  table_names_.erase(table_name);
  tables_.erase(drop_table_id);

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  // ASSERT(false, "Not Implemented yet");
  //return DB_FAILED;
  auto find_index = index_names_.find(table_name);
  if(find_index == index_names_.end()) return DB_INDEX_NOT_FOUND;
  auto find_index_name = find_index->second.find(index_name);
  if(find_index_name == find_index->second.end()) return DB_INDEX_NOT_FOUND;
  index_id_t drop_index_id = find_index_name->second;
  delete indexes_[drop_index_id];
  index_names_[table_name].erase(index_name);
  indexes_.erase(drop_index_id);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::FlushCatalogMetaPage() const {
  // ASSERT(false, "Not Implemented yet");
  //return DB_FAILED;
  Page *meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  catalog_meta_->SerializeTo(meta_page->GetData());
  buffer_pool_manager_->FlushPage(CATALOG_META_PAGE_ID);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  // ASSERT(false, "Not Implemented yet");
  //return DB_FAILED;
  auto find_table = tables_.find(table_id);
  if(find_table == tables_.end()) return DB_TABLE_NOT_EXIST;
  Page *meta_page = buffer_pool_manager_->FetchPage(page_id);
  auto old_page_id = catalog_meta_->table_meta_pages_[table_id];
  Page *old_meta_page = buffer_pool_manager_->FetchPage(old_page_id);
  memcpy(meta_page, old_meta_page, PAGE_SIZE);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  // ASSERT(false, "Not Implemented yet");
  //return DB_FAILED;
  auto find_index = indexes_.find(index_id);
  if(find_index == indexes_.end()) return DB_INDEX_NOT_FOUND;
  Page *meta_page = buffer_pool_manager_->FetchPage(page_id);
  auto old_page_id = catalog_meta_->index_meta_pages_[index_id];
  Page *old_meta_page = buffer_pool_manager_->FetchPage(old_page_id);
  memcpy(meta_page, old_meta_page, PAGE_SIZE);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  //return DB_FAILED;
  auto find_table = tables_.find(table_id);
  if(find_table == tables_.end()) return DB_TABLE_NOT_EXIST;
  table_info = find_table->second;
  return DB_SUCCESS;
}