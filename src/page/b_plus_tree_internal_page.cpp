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
 */
void InternalPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) {
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetKeySize(key_size);
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetLSN(INVALID_LSN);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey *InternalPage::KeyAt(int index) {
  return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}

void InternalPage::SetKeyAt(int index, GenericKey *key) {
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
  int left,right;
  left = 0;
  right = GetSize()-1;
  while(left <= right){
    int mid = (left + right) / 2;
    if(KM.CompareKeys(KeyAt(mid),key) == 0){
      return ValueAt(mid);
    }else if(KM.CompareKeys(KeyAt(mid),key) < 0){
      left = mid + 1;
    }else{
      right = mid - 1;
    }
  }
  return INVALID_PAGE_ID;
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
  InternalPage* new_root = new InternalPage();
}

/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
int InternalPage::InsertNodeAfter(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
  for(auto i=0;i<GetSize();i++){
    if(ValueAt(i)==old_value){
      for(auto j=GetSize();j>i;j--){
        SetKeyAt(j,KeyAt(j-1));
        SetValueAt(j,ValueAt(j-1));
      }
      SetKeyAt(i,new_key);
      SetValueAt(i,new_value);
      IncreaseSize(1);
      return GetSize();
    }
  }
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 * buffer_pool_manager 是干嘛的？传给CopyNFrom()用于Fetch数据页
 */
void InternalPage::MoveHalfTo(InternalPage *recipient, BufferPoolManager *buffer_pool_manager) {
  int half = (GetSize() + 1) / 2;
  recipient->CopyNFrom(pairs_off + half * pair_size, GetSize() - half, buffer_pool_manager);
  SetSize(half);
  recipient->SetParentPageId(GetParentPageId());
  recipient->SetPageType(IndexPageType::INTERNAL_PAGE);
  recipient->SetSize(GetSize() - half);
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 *
 */
void InternalPage::CopyNFrom(void *src, int size, BufferPoolManager *buffer_pool_manager) {
  int old_size = GetSize();
  IncreaseSize(size);
  memcpy(pairs_off + old_size * pair_size, src, size * pair_size);
  for (int i = old_size; i < GetSize(); i++) {
    page_id_t child_page_id = ValueAt(i);
    BPlusTreePage *child_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager->FetchPage(child_page_id));
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
  for(auto i = index;i<GetSize()-1;i++){
    SetKeyAt(i,KeyAt(i+1));
    SetValueAt(i,ValueAt(i+1));
  }
  SetSize(GetSize()-1);
  SetKeyAt(GetSize(),nullptr);
  SetValueAt(GetSize(),INVALID_PAGE_ID);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
page_id_t InternalPage::RemoveAndReturnOnlyChild() {
  page_id_t child_page_id = ValueAt(0);
  SetSize(0);
  SetKeyAt(0, nullptr);
  SetValueAt(0, INVALID_PAGE_ID);
  return child_page_id;
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
  recipient->CopyLastFrom(middle_key, GetParentPageId(), buffer_pool_manager);
  // BPlusTreeInternalPage* parent_page = reinterpret_cast<BPlusTreeInternalPage*>(buffer_pool_manager->FetchPage(GetParentPageId()));
  // parent_page->Remove(ValueIndex(GetPageId()));
  for(auto i=0;i<GetSize();i++){
    recipient->CopyLastFrom(KeyAt(i),ValueAt(i),buffer_pool_manager);
  }

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
  InternalPage *parent_page = reinterpret_cast<InternalPage *>(buffer_pool_manager->FetchPage(GetParentPageId()));
  parent_page->SetKeyAt(ValueIndex(GetPageId()), middle_key);
  recipient->CopyLastFrom(KeyAt(0), ValueAt(0), buffer_pool_manager);
  for(auto i=0;i<GetSize()-1;i++){
    SetKeyAt(i,KeyAt(i+1));
    SetValueAt(i,ValueAt(i+1));
  }
  SetSize(GetSize()-1);
  SetKeyAt(GetSize(), nullptr);
  SetValueAt(GetSize(), INVALID_PAGE_ID);
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyLastFrom(GenericKey *key, const page_id_t value, BufferPoolManager *buffer_pool_manager) {
  SetKeyAt(GetSize(),key);
  SetValueAt(GetSize(),value);
  IncreaseSize(1);
  BPlusTreePage *child_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager->FetchPage(value));
  child_page->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(value, true);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
void InternalPage::MoveLastToFrontOf(InternalPage *recipient, GenericKey *middle_key,
                                     BufferPoolManager *buffer_pool_manager) {
  InternalPage *parent_page = reinterpret_cast<InternalPage *>(buffer_pool_manager->FetchPage(GetParentPageId()));
  parent_page->SetKeyAt(ValueIndex(GetPageId()), middle_key);
  recipient->CopyFirstFrom(ValueAt(GetSize()-1), buffer_pool_manager);
  SetSize(GetSize()-1);
  SetKeyAt(GetSize(), nullptr);
  SetValueAt(GetSize(), INVALID_PAGE_ID);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyFirstFrom(const page_id_t value, BufferPoolManager *buffer_pool_manager) {
  for(auto i=GetSize();i>0;i--){
    SetKeyAt(i,KeyAt(i-1));
    SetValueAt(i,ValueAt(i-1));
  }
  SetKeyAt(0, nullptr);
  SetValueAt(0, value);
  IncreaseSize(1);
  BPlusTreePage *child_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager->FetchPage(value));
  child_page->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(value, true);
}