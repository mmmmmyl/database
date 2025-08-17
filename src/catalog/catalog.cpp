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
  LOG(INFO)<<(const void*)buf;
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
  return 3 * sizeof(uint32_t) +
         table_meta_pages_.size() * (sizeof(table_id_t) + sizeof(page_id_t)) +
         index_meta_pages_.size() * (sizeof(index_id_t) + sizeof(page_id_t));
}

CatalogMeta::CatalogMeta() {}

/**
 * TODO: Student Implement
 */
CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
    : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager), log_manager_(log_manager) {
  if (init) {
    catalog_meta_ = CatalogMeta::NewInstance();
    next_table_id_ = 0;
    next_index_id_ = 0;
  }
  else{
    char* buf = reinterpret_cast<char *>(buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID)->GetData());
    catalog_meta_=CatalogMeta::DeserializeFrom(buf);
    if (catalog_meta_->GetSerializedSize() > PAGE_SIZE) {
      LOG(ERROR) << "Catalog metadata size exceeds page size.";
    }
    next_table_id_ = catalog_meta_->GetNextTableId();
    next_index_id_ = catalog_meta_->GetNextIndexId();
    for(auto &iter : catalog_meta_->table_meta_pages_){
      if (LoadTable(iter.first, iter.second) != DB_SUCCESS) {
        LOG(ERROR) << "Failed to load table with id: " << iter.first;
      }
      // LOG(INFO) << "Loaded table with id: " << iter.first;
    }
    for(auto &iter : catalog_meta_->index_meta_pages_){
      if (LoadIndex(iter.first, iter.second) != DB_SUCCESS) {
        LOG(ERROR) << "Failed to load index with id: " << iter.first;
      }
      // LOG(INFO)<< "Loaded index with id: " << iter.first;
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
  table_id_t table_id = next_table_id_++;
  table_names_[table_name] = table_id;
  table_info = TableInfo::Create();
  page_id_t table_meta_page_id;
  Page* table_meta_page = buffer_pool_manager_->NewPage(table_meta_page_id);
  LOG(INFO)<< "Create new page for table metadata, page id: " << table_meta_page_id;
  TableMetadata *table_meta = TableMetadata::Create(table_id, table_name, table_meta_page_id, TableSchema::DeepCopySchema(schema));
  TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_, table_meta->GetSchema(), txn, log_manager_, lock_manager_);
  table_info->Init(table_meta,table_heap);
  tables_[table_id] = table_info;
  catalog_meta_->table_meta_pages_[table_id] = table_meta_page_id;
  table_meta->SerializeTo(table_meta_page->GetData());
  buffer_pool_manager_->UnpinPage(table_meta_page_id, true);
  buffer_pool_manager_->FlushPage(table_meta_page_id);
  index_names_[table_name] = std::unordered_map<std::string, index_id_t>();
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  if(table_names_.find(table_name) == table_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  }
  if(table_names_.find(table_name) != table_names_.end() &&
     tables_.find(table_names_[table_name]) != tables_.end()) {
      table_info = tables_[table_names_[table_name]];
      return DB_SUCCESS;
  }
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  for (const auto &pair : tables_) {
    tables.emplace_back(pair.second);
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Txn *txn, IndexInfo *&index_info,
                                    const string &index_type) {
  if (index_names_.find(table_name) == index_names_.end()) return DB_TABLE_NOT_EXIST;
  if (index_names_[table_name].find(index_name) != index_names_[table_name].end()) return DB_INDEX_ALREADY_EXIST;
  
  dberr_t db_result;
  uint32_t pos;
  std::vector<uint32_t> key_map;
  for(auto &key : index_keys) {
    db_result = tables_[table_names_[table_name]]->GetSchema()->GetColumnIndex(key,pos);
    if(db_result == DB_COLUMN_NAME_NOT_EXIST) return DB_COLUMN_NAME_NOT_EXIST;
    key_map.push_back(pos);
  }
  if (key_map.empty()) return DB_COLUMN_NAME_NOT_EXIST;
  
  index_id_t index_id = next_index_id_++;
  index_names_[table_name][index_name] = index_id;
  index_info = IndexInfo::Create();
  page_id_t index_meta_page_id;
  Page* index_meta_page = buffer_pool_manager_->NewPage(index_meta_page_id);
  LOG(INFO)<< "Create new page for index metadata, page id: " << index_meta_page_id;
  IndexMetadata *index_meta = IndexMetadata::Create(index_id, index_name, table_names_[table_name], key_map);
  index_info->Init(index_meta,tables_[table_names_[table_name]], buffer_pool_manager_);
  indexes_[index_id] = index_info;
  catalog_meta_->index_meta_pages_[index_id] = index_meta_page_id;
  index_meta->SerializeTo(index_meta_page->GetData());
  buffer_pool_manager_->UnpinPage(index_meta_page_id, true);
  buffer_pool_manager_->FlushPage(index_meta_page_id);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  if (index_names_.find(table_name) == index_names_.end()) return DB_TABLE_NOT_EXIST;
  if (index_names_.at(table_name).find(index_name) == index_names_.at(table_name).end()) return DB_INDEX_NOT_FOUND;
  index_id_t index_id = index_names_.at(table_name).at(index_name);
  if (indexes_.find(index_id) == indexes_.end()) return DB_INDEX_NOT_FOUND;
  index_info = indexes_.at(index_id);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  if (index_names_.find(table_name) == index_names_.end()) return DB_TABLE_NOT_EXIST;
  for (const auto &pair : index_names_.at(table_name)) {
    indexes.emplace_back(indexes_.at(pair.second));
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropTable(const string &table_name) {
  if (table_names_.find(table_name) == table_names_.end()) return DB_TABLE_NOT_EXIST;
  table_id_t table_id = table_names_[table_name];
  if (tables_.find(table_id) == tables_.end()) return DB_TABLE_NOT_EXIST;
  //need to delete pages in table 
  catalog_meta_->table_meta_pages_.erase(table_id);
  table_names_.erase(table_name);
  delete tables_[table_id];
  tables_.erase(table_id);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  if (index_names_.find(table_name) == index_names_.end()) return DB_TABLE_NOT_EXIST;
  if (index_names_[table_name].find(index_name) == index_names_[table_name].end()) return DB_INDEX_NOT_FOUND;
  index_id_t index_id = index_names_[table_name][index_name];
  if (indexes_.find(index_id) == indexes_.end()) return DB_INDEX_NOT_FOUND;
  catalog_meta_->DeleteIndexMetaPage(buffer_pool_manager_, index_id);
  index_names_[table_name].erase(index_name);
  delete indexes_[index_id];
  indexes_.erase(index_id);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::FlushCatalogMetaPage() const {
  Page *meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  if (meta_page == nullptr) {
    LOG(ERROR) << "Failed to fetch catalog meta page.";
    return DB_FAILED;
  }
  catalog_meta_->SerializeTo(meta_page->GetData());
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
  buffer_pool_manager_->FlushPage(CATALOG_META_PAGE_ID);
  for(auto iter=catalog_meta_->GetTableMetaPages()->begin();iter!=catalog_meta_->GetTableMetaPages()->end();iter++) {
    buffer_pool_manager_->UnpinPage(iter->second, true);
    buffer_pool_manager_->FlushPage(iter->second);
  }
  for(auto iter=catalog_meta_->GetIndexMetaPages()->begin();iter!=catalog_meta_->GetIndexMetaPages()->end();iter++) {
    buffer_pool_manager_->UnpinPage(iter->second, true);
    buffer_pool_manager_->FlushPage(iter->second);
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  LOG(INFO) << "Loading table with id: " << table_id << " from page id: " << page_id;
  TableInfo *table_info = TableInfo::Create();
  Page *page = buffer_pool_manager_->FetchPage(page_id);
  TableMetadata *table_meta=nullptr;
  TableMetadata::DeserializeFrom(page->GetData(),table_meta);
  TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_, table_meta->GetSchema(), nullptr, log_manager_, lock_manager_);
  table_info->Init(table_meta, table_heap);
  tables_[table_id] = table_info;
  table_names_[table_meta->GetTableName()] = table_id;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  LOG(INFO) << "Loading index with id: " << index_id << " from page id: " << page_id;
  IndexInfo *index_info = IndexInfo::Create();
  Page *page = buffer_pool_manager_->FetchPage(page_id);
  IndexMetadata *index_meta=nullptr;
  IndexMetadata::DeserializeFrom(page->GetData(), index_meta);
  TableInfo *table_info;
  if (GetTable(index_meta->GetTableId(), table_info) != DB_SUCCESS) {
    LOG(ERROR) << "Failed to load table for index: " << index_meta->GetIndexName();
    return DB_FAILED;
  }
  index_info->Init(index_meta, table_info, buffer_pool_manager_);
  indexes_[index_id] = index_info;
  index_names_[table_info->GetTableName()][index_meta->GetIndexName()] = index_id;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  if (tables_.find(table_id) == tables_.end()) {
    return DB_TABLE_NOT_EXIST;
  }
  table_info = tables_[table_id];
  return DB_SUCCESS;
}