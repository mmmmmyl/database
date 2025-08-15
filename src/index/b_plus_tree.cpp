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
      internal_max_size_(internal_max_size) {
  if (leaf_max_size_ == UNDEFINED_SIZE) {
    leaf_max_size_ = (PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / (processor_.GetKeySize() + sizeof(RowId));
  }
  if (internal_max_size_ == UNDEFINED_SIZE) {
    internal_max_size_ = (PAGE_SIZE - LEAF_PAGE_HEADER_SIZE) / (processor_.GetKeySize() + sizeof(page_id_t));
  }
}

void BPlusTree::Destroy(page_id_t current_page_id) {
}

/*
 * Helper function to decide whether current b+tree is empty
 */
bool BPlusTree::IsEmpty() const {
  if(root_page_id_==INVALID_PAGE_ID) {
    return true;
  }
  BPlusTreePage *root_page = reinterpret_cast<BPlusTreePage*>(buffer_pool_manager_->FetchPage(root_page_id_));
  if (root_page == nullptr) {
    return true;
  }
  if (root_page->GetSize() == 0) {
    buffer_pool_manager_->UnpinPage(root_page_id_, false);
    return true;
  }
  buffer_pool_manager_->UnpinPage(root_page_id_, false);
  return false;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
bool BPlusTree::GetValue(const GenericKey *key, std::vector<RowId> &result, Txn *transaction) { 
  BPlusTreePage *root_page = reinterpret_cast<BPlusTreePage*>(buffer_pool_manager_->FetchPage(root_page_id_));
  BPlusTreeLeafPage *leaf_page = reinterpret_cast<BPlusTreeLeafPage*>(FindLeafPage(key, root_page_id_));
  if (leaf_page == nullptr) {
    LOG(ERROR) << "Leaf page not found for key: " << key;
    buffer_pool_manager_->UnpinPage(root_page_id_, false);
    return false;
  }
  RowId value;
  if (!leaf_page->Lookup(key, value, processor_)) {
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    buffer_pool_manager_->UnpinPage(root_page_id_, false);
    return false;
  }
  else{
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    buffer_pool_manager_->UnpinPage(root_page_id_, false);
    result.push_back(value);
    return true;
  }
  // RowId value = leaf_page->ValueAt(leaf_page->KeyIndex(key, processor_));
  // buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
  // buffer_pool_manager_->UnpinPage(root_page_id_, false);
  // result.push_back(value);
  // return true;
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
 */
bool BPlusTree::Insert(GenericKey *key, const RowId &value, Txn *transaction) { 
  if (IsEmpty()) {
    StartNewTree(key, value);
    return true;
  }
  BPlusTreeLeafPage *leaf_page = reinterpret_cast<BPlusTreeLeafPage*>(FindLeafPage(key, root_page_id_,false));
  //LOG(INFO) << "pin count after fetch leaf page: " << buffer_pool_manager_->pages_[2].GetPinCount();
  if (leaf_page->Insert(key, value, processor_) == -1) {
    //LOG(ERROR) << "Insert failed";
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    return false;
  }
  //LOG(INFO) << "pin count 1 " << buffer_pool_manager_->pages_[2].GetPinCount();
  if (leaf_page->GetSize() == leaf_page->GetMaxSize()) {
    LeafPage *new_leaf_page = Split(leaf_page, transaction);
    InsertIntoParent(leaf_page, new_leaf_page->KeyAt(0), new_leaf_page, transaction);
  }
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
  return true;
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
void BPlusTree::StartNewTree(GenericKey *key, const RowId &value) {
  page_id_t new_page_id;
  LeafPage *new_leaf_page = reinterpret_cast<LeafPage*>(buffer_pool_manager_->NewPage(new_page_id));
  if (new_leaf_page == nullptr) {
    throw std::runtime_error("Out of memory");
  }
  new_leaf_page->Init(new_page_id, INVALID_PAGE_ID, processor_.GetKeySize(), leaf_max_size_);
  new_leaf_page->Insert(key, value, processor_);
  root_page_id_ = new_page_id;
  buffer_pool_manager_->UnpinPage(new_page_id, true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::InsertIntoLeaf(GenericKey *key, const RowId &value, Txn *transaction) { 
  BPlusTreeLeafPage *leaf_page = reinterpret_cast<BPlusTreeLeafPage*>(FindLeafPage(key, root_page_id_));
  if (leaf_page->Insert(key, value, processor_) == -1) {
    return false;
  }
  if (leaf_page->GetSize() == leaf_page->GetMaxSize()) {
    LeafPage *new_leaf_page = Split(leaf_page, transaction);
    InsertIntoParent(leaf_page, new_leaf_page->KeyAt(0), new_leaf_page, transaction);
  }
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
BPlusTreeInternalPage *BPlusTree::Split(InternalPage *node, Txn *transaction) { 
  page_id_t new_page_id;
  InternalPage *new_internal_page = reinterpret_cast<InternalPage*>(buffer_pool_manager_->NewPage(new_page_id));
  if (new_internal_page == nullptr) {
    throw std::runtime_error("Out of memory");
  }
  new_internal_page->Init(new_page_id, node->GetParentPageId(), processor_.GetKeySize(), internal_max_size_);
  node->MoveHalfTo(new_internal_page, buffer_pool_manager_);
  buffer_pool_manager_->UnpinPage(new_page_id, true);
  return new_internal_page;
 }

BPlusTreeLeafPage *BPlusTree::Split(LeafPage *node, Txn *transaction) { 
  page_id_t new_page_id;
  LeafPage *new_leaf_page = reinterpret_cast<LeafPage*>(buffer_pool_manager_->NewPage(new_page_id));
  if (new_leaf_page == nullptr) {
    throw std::runtime_error("Out of memory");
  }
  new_leaf_page->Init(new_page_id, node->GetParentPageId(), processor_.GetKeySize(), leaf_max_size_);
  node->MoveHalfTo(new_leaf_page);
  buffer_pool_manager_->UnpinPage(new_page_id, true);
  return new_leaf_page;
 }

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
void BPlusTree::InsertIntoParent(BPlusTreePage *old_node, GenericKey *key, BPlusTreePage *new_node, Txn *transaction) {
  if(old_node->IsRootPage()){
    page_id_t new_root_page_id;
    InternalPage *new_root_page = reinterpret_cast<InternalPage*>(buffer_pool_manager_->NewPage(new_root_page_id));
    if (new_root_page == nullptr) {
      throw std::runtime_error("Out of memory");
    }
    new_root_page->Init(new_root_page_id, INVALID_PAGE_ID, processor_.GetKeySize(), internal_max_size_);
    root_page_id_ = new_root_page_id;
    new_root_page->SetPageType(IndexPageType::INTERNAL_PAGE);
    new_root_page->SetValueAt(0,old_node->GetPageId());
    new_root_page->SetSize(1);
    new_root_page->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    old_node->SetParentPageId(new_root_page_id);
    new_node->SetParentPageId(new_root_page_id);
    buffer_pool_manager_->UnpinPage(new_root_page_id, true);
    return;
  }
  BPlusTreeInternalPage* parent_page = reinterpret_cast<BPlusTreeInternalPage*>(buffer_pool_manager_->FetchPage(old_node->GetParentPageId()));
  if (parent_page->GetSize() == parent_page->GetMaxSize()) {
    InternalPage *new_internal_page = Split(parent_page, transaction);
    InsertIntoParent(parent_page, new_internal_page->KeyAt(0), new_internal_page, transaction);
  }
  else {
    parent_page->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
    new_node->SetParentPageId(parent_page->GetPageId());
  }
  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
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
 */
void BPlusTree::Remove(const GenericKey *key, Txn *transaction) {
  if (IsEmpty()) {
    return;
  }
  BPlusTreeLeafPage *leaf_page = reinterpret_cast<BPlusTreeLeafPage*>(FindLeafPage(key, root_page_id_));
  if (leaf_page->RemoveAndDeleteRecord(key, processor_) == -1) {
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    return;
  }
  LOG(INFO) << "page "<<leaf_page->GetPageId()<< " page size: "<<leaf_page->GetSize();
  if (leaf_page->GetSize() < leaf_page->GetMinSize() && leaf_page->IsRootPage() == false) {
    // LOG(INFO)<<"CoalesceOrRedistribute(leaf_page, transaction) for page "<<leaf_page->GetPageId();
    if(CoalesceOrRedistribute(leaf_page, transaction)){
      buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
     }
    // if(CoalesceOrRedistribute(leaf_page, transaction)){
    //   buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
    //   buffer_pool_manager_->DeletePage(leaf_page->GetPageId());
    // }
    // else {
    //   buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
    // }
    
  }
  else {
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
  }
}

/* todo
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
template <typename N>
bool BPlusTree::CoalesceOrRedistribute(N *&node, Txn *transaction) {
  if(node->IsRootPage()) return true;
  BPlusTreeInternalPage *parent_page = reinterpret_cast<BPlusTreeInternalPage*>(buffer_pool_manager_->FetchPage(node->GetParentPageId()));
  int index = parent_page->ValueIndex(node->GetPageId());

  // LOG(INFO)<<"Parent page "<<parent_page->GetPageId();
  // for(int i = 0; i < parent_page->GetSize(); i++) {
  //   LOG(INFO)<<" index "<<i<<" value "<<parent_page->ValueAt(i);
  // }

  BPlusTreePage *neighbor_node;
  bool coalesce_result;
  if(index == 0) neighbor_node = reinterpret_cast<BPlusTreePage*>(buffer_pool_manager_->FetchPage(parent_page->ValueAt(1)));
  else neighbor_node = reinterpret_cast<BPlusTreePage*>(buffer_pool_manager_->FetchPage(parent_page->ValueAt(index - 1)));
  if (neighbor_node->GetSize() + node->GetSize() >= node->GetMaxSize()) {
    LOG(INFO)<<"Redistribute for page "<<node->GetPageId()<<" and its neighbor "<<neighbor_node->GetPageId();
    if(neighbor_node->IsLeafPage()) Redistribute(reinterpret_cast<BPlusTreeLeafPage*>(neighbor_node), reinterpret_cast<BPlusTreeLeafPage*>(node), index);
    else Redistribute(reinterpret_cast<BPlusTreeInternalPage*>(neighbor_node), reinterpret_cast<BPlusTreeInternalPage*>(node), index);
    buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
    return false;
  } else {
    LOG(INFO)<<"Coalesce for page "<<node->GetPageId()<<" and its neighbor "<<neighbor_node->GetPageId();
    if(neighbor_node->IsLeafPage()) coalesce_result=Coalesce(reinterpret_cast<BPlusTreeLeafPage*&>(neighbor_node), reinterpret_cast<BPlusTreeLeafPage*&>(node), parent_page, index, transaction);
    else coalesce_result=Coalesce(reinterpret_cast<BPlusTreeInternalPage*&>(neighbor_node), reinterpret_cast<BPlusTreeInternalPage*&>(node), parent_page, index, transaction);
    //Coalesce(neighbor_node, node, parent_page, index, transaction);
    if(parent_page->IsRootPage() && parent_page->GetSize()==0)AdjustRoot(parent_page);
    buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
    if(coalesce_result) {
      buffer_pool_manager_->DeletePage(parent_page->GetPageId());
    }
    return true;
  }
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
  LeafPage *temp;
  if (index == 0) {
    neighbor_node->MoveAllTo(node);
    parent->Remove(1);
    temp=neighbor_node;neighbor_node = node;node=temp;
  }
  else {node->MoveAllTo(neighbor_node);parent->Remove(index);}
  buffer_pool_manager_->UnpinPage(node->GetPageId(), false);
  buffer_pool_manager_->DeletePage(node->GetPageId());
  if (parent->GetSize() < parent->GetMinSize() && parent->IsRootPage() == false) {
    return CoalesceOrRedistribute(parent, transaction);
  }
  LOG(INFO)<<"Parent page "<<parent->GetPageId();
  for(int i = 0; i < parent->GetSize(); i++) {
    LOG(INFO)<<" index "<<i<<" page "<<parent->ValueAt(i);
  }
  return false;
}

bool BPlusTree::Coalesce(InternalPage *&neighbor_node, InternalPage *&node, InternalPage *&parent, int index,
                         Txn *transaction) {
  node->MoveAllTo(neighbor_node, node->KeyAt(0), buffer_pool_manager_);
  buffer_pool_manager_->UnpinPage(node->GetPageId(), false);
  buffer_pool_manager_->DeletePage(node->GetPageId());
  parent->Remove(index);
  if (parent->GetSize() < parent->GetMinSize()) {
    return CoalesceOrRedistribute(parent, transaction);
  }
  return false;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
void BPlusTree::Redistribute(LeafPage *neighbor_node, LeafPage *node, int index) {
  if(index==0) {
    neighbor_node->MoveFirstToEndOf(node);
    InternalPage *parent_page = reinterpret_cast<InternalPage*>(buffer_pool_manager_->FetchPage(node->GetParentPageId()));
    parent_page->SetKeyAt(0, neighbor_node->KeyAt(0));
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
  }
  else {
    neighbor_node->MoveLastToFrontOf(node);
    InternalPage *parent_page = reinterpret_cast<InternalPage*>(buffer_pool_manager_->FetchPage(node->GetParentPageId()));
    parent_page->SetKeyAt(index, node->KeyAt(0));
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
  }
}
void BPlusTree::Redistribute(InternalPage *neighbor_node, InternalPage *node, int index) {
  if(index==0) neighbor_node->MoveFirstToEndOf(node, neighbor_node->KeyAt(0), buffer_pool_manager_);
  else neighbor_node->MoveLastToFrontOf(node, neighbor_node->KeyAt(neighbor_node->GetSize() - 1), buffer_pool_manager_);
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
 */
bool BPlusTree::AdjustRoot(BPlusTreePage *old_root_node) {
  if (old_root_node->GetSize() == 0) {
    BPlusTreeInternalPage *internal_page = reinterpret_cast<BPlusTreeInternalPage *>(old_root_node);
    page_id_t child_page_id = internal_page->RemoveAndReturnOnlyChild();
    buffer_pool_manager_->UnpinPage(old_root_node->GetPageId(), false);
    buffer_pool_manager_->DeletePage(old_root_node->GetPageId());
    root_page_id_ = child_page_id;
    BPlusTreePage *child_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(child_page_id));
    child_page->SetParentPageId(INVALID_PAGE_ID);
    buffer_pool_manager_->UnpinPage(child_page_id, true);
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
  return IndexIterator(root_page_id_, buffer_pool_manager_, 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin(const GenericKey *key) {
  BPlusTreeLeafPage *leaf_page = reinterpret_cast<BPlusTreeLeafPage*>(FindLeafPage(key, root_page_id_));
  return IndexIterator(leaf_page->GetPageId(), buffer_pool_manager_, leaf_page->KeyIndex(key, processor_));
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
IndexIterator BPlusTree::End() {
  return IndexIterator(INVALID_PAGE_ID, buffer_pool_manager_, 0);
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * Note: the leaf page is pinned, you need to unpin it after use.
 */
Page *BPlusTree::FindLeafPage(const GenericKey *key, page_id_t page_id, bool leftMost) {
  if(page_id == INVALID_PAGE_ID||page_id<=0) {
    LOG(ERROR) << "Invalid page id "<<page_id<< " for FindLeafPage";
    return nullptr;
  }
  BPlusTreePage *page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(page_id)->GetData());
  if (page->IsLeafPage()) {
    return reinterpret_cast<Page *>(page);
  }
  InternalPage *internal_page = reinterpret_cast<InternalPage *>(page);
  page_id_t child_page_id;
  if (leftMost) {
    child_page_id = internal_page->ValueAt(0);
  } else {
    child_page_id = internal_page->Lookup(key, processor_);
  }
  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  return FindLeafPage(key, child_page_id, leftMost);
}

/*
 * Update/Insert root page id in header page(where page_id = INDEX_ROOTS_PAGE_ID,
 * header_page isdefined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, current_page_id> into header page instead of
 * updating it.
 */
void BPlusTree::UpdateRootPageId(int insert_record) {
  ;
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