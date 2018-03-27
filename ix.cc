#include "ix.h"
#include <cassert>
#include <iostream>
#include <stdlib.h>
#include <stack>
//#include <cstd::string>
std::shared_ptr<void> AttrtypePtrConversion(const AttrType& attr_type, const void* key);

IndexManager* IndexManager::_index_manager = 0;

IndexManager* IndexManager::instance()
{
    if(!_index_manager)
        _index_manager = new IndexManager();

    return _index_manager;
}

IndexManager::IndexManager()
{
    _pf_manager=PagedFileManager::instance();
}

IndexManager::~IndexManager()
{
}

RC IndexManager::createFile(const std::string &fileName)
{
    RC rc = 0;
    std::cout << fileName <<std::endl;
    rc = _pf_manager->createFile(fileName);
    IXFileHandle ixfileHandle;
    char page_buffer[PAGE_SIZE];
    rc += ixfileHandle.openFile(fileName);
    memset(page_buffer, 0, PAGE_SIZE);
    rc += ixfileHandle.file_handle_.appendPage(page_buffer);
    rc += _pf_manager->closeFile(ixfileHandle.file_handle_);
    return rc;
}

RC IndexManager::destroyFile(const std::string &fileName)
{
    return _pf_manager->destroyFile(fileName );
}

RC IndexManager::openFile(const std::string &fileName, IXFileHandle &ixfileHandle)
{
	if(ixfileHandle.counter_!=0)
		return -1;
    return ixfileHandle.openFile(fileName);
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
	return ixfileHandle.closeFile();
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{

    RC rc = 0;
    rc += readMataData(ixfileHandle, &this -> btree_);
    rc += this -> btree_.Insert(ixfileHandle, attribute, key, rid);
    rc += writeMataData(ixfileHandle, &this -> btree_);
    return rc;
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    RC rc = 0;
    rc += readMataData(ixfileHandle, &this -> btree_);
    rc += this -> btree_.Delete(ixfileHandle, attribute, key, rid);
    rc += writeMataData(ixfileHandle, &this -> btree_);
    return rc;
}

RC IndexManager::scan(IXFileHandle &ixfileHandle,
    const Attribute &attribute,
    const void      *lowKey,
    const void      *high_key,
    bool            lowKeyInclusive,
    bool            highKeyInclusive,
    IX_ScanIterator &ix_ScanIterator)
{
    RC rc = readMataData(ixfileHandle, &this -> btree_);
    if (rc != 0) 
        return rc;
    ix_ScanIterator.Init(this -> btree_, ixfileHandle, attribute, lowKey, high_key, lowKeyInclusive, highKeyInclusive);
    return 0;
}

RC IndexManager::writeMataData(IXFileHandle &ixfileHandle, const BtreeHandle *b_tree)
{
    char page_buffer[PAGE_SIZE];
    memset(page_buffer, 0, PAGE_SIZE);

    memcpy((char*) page_buffer , &b_tree->attr_type_, sizeof(int));

    memcpy((char*) page_buffer + 1*sizeof(int), &b_tree->attr_len_, sizeof(int));

    memcpy((char*) page_buffer + 2*sizeof(int), &b_tree->root_id_, sizeof(int));

    memcpy((char*) page_buffer + 3*sizeof(int), &b_tree->first_leaf_id_, sizeof(int));

    memcpy((char*) page_buffer + 4*sizeof(int), &b_tree->last_leaf_id_, sizeof(int));

    memcpy((char*) page_buffer + 5*sizeof(int), &b_tree->degree_, sizeof(int));

    if(ixfileHandle.file_handle_.writePage(0, page_buffer))
		return -1;
    return 0;
}

RC IndexManager::readMataData(IXFileHandle &ixfileHandle, BtreeHandle *b_tree){

    char page_buffer[PAGE_SIZE];
    memset(page_buffer, 0, PAGE_SIZE);
    if(ixfileHandle.file_handle_.readPage(0, page_buffer))
		return -1;

    memcpy(&b_tree->attr_type_, (char*) page_buffer, sizeof(int));

    memcpy(&b_tree->attr_len_, (char*) page_buffer + 1*sizeof(int), sizeof(int));

    memcpy(&b_tree->root_id_, (char*) page_buffer + 2*sizeof(int), sizeof(int));

    memcpy(&b_tree->first_leaf_id_, (char*) page_buffer + 3*sizeof(int), sizeof(int));

    memcpy(&b_tree->last_leaf_id_, (char*) page_buffer + 4*sizeof(int), sizeof(int));

    memcpy(&b_tree->degree_, (char*) page_buffer + 5*sizeof(int), sizeof(int));


    return 0;
}

void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const {
    this->btree_.printBtree(ixfileHandle, attribute);
}

IX_ScanIterator::IX_ScanIterator()
{
}

RC IX_ScanIterator::Init(BtreeHandle &bt,
                         IXFileHandle &ixfileHandle,
                         const Attribute &attribute,
                         const void        *lowKey,
                         const void        *high_key,
                         bool        low_key_inclusive,
                         bool        high_key_inclusive)
{
    AttrType attrType = TypeInt;
    char buffer[PAGE_SIZE];
    ixfileHandle.file_handle_.readPage(0, buffer);
    memcpy(&attrType, buffer, sizeof(int));
    if (attribute.type != attrType) 
        return -1;
    attr_type_ = attrType;

    this -> btree_ = bt;

    memcpy(&first_leaf_id_, buffer+3*sizeof(int), sizeof(int));
    
    this->ix_file_handle_ = &ixfileHandle;
    this->attr_ = attribute;
	cur_node_id_ = -1;
    idx_for_keys_ = 0;
    second_idx_ = 0;
    low_key_ = lowKey;
    high_key_ = high_key;
    if_low_key_ = low_key_inclusive;
    if_high_key_ = high_key_inclusive;

    if (low_key_ == NULL) {
        compare_low_key_eq_ = 1;
    }
    if (high_key_ == NULL) {
        compare_high_key_eq_ = -1;
    }
    
    return 0;
}

RC IX_ScanIterator::close() 
{
    this->ix_file_handle_ = NULL;
    return 0;
}

void IX_ScanIterator::CopyKey(void *key1, const void *key2, AttrType attrType)
{
    if(attrType==TypeInt) 
	{
        memcpy((char*)key1, (char*)key2, sizeof(int));
	}
	if(attrType==TypeReal) 
	{
        memcpy((char*)key1, (char*)key2, sizeof(float));
	}
	if(attrType==TypeVarChar) 
	{
			int len = 0;
            memcpy(&len, (char*)key2, sizeof(int));
            memcpy((char*)key1, &len, sizeof(int));
            memcpy((char*)key1+sizeof(int), (char*)key2+sizeof(int), len);
	}
        
}

RC IX_ScanIterator::ReadEveryNode(int node_id, BtreeNode &node)
{
    char page_buffer[PAGE_SIZE];
    memset(page_buffer, 0, PAGE_SIZE);
    (this->ix_file_handle_)->file_handle_.readPage(node_id, page_buffer);
    node.LoadNode(page_buffer);
    while (node.deleted_ == 1 && node.right_sibling_ != 0) 
	{
        memset(page_buffer, 0, PAGE_SIZE);
        (this->ix_file_handle_)->file_handle_.readPage(node.right_sibling_, page_buffer);
        node.LoadNode(page_buffer);
    }
    if (node.right_sibling_ == 0 && node.deleted_ == 1) 
	{
            return -1;
    }
    node.ReadBucket(*(this->ix_file_handle_));
    return 0;
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
	int first=1,second=1;
    if ((this->ix_file_handle_) == NULL) 
		return -1;
	//no cur_node_id_
    if (cur_node_id_ == -1) 
	{
        // find lowKey, if find, get id, if not, get nearest ID 
        if (low_key_) 
            cur_node_id_ = this -> btree_.Search(*(this->ix_file_handle_), low_key_);
        else 
            cur_node_id_ = first_leaf_id_;

        
    while(first)
	{

        if (0 != ReadEveryNode(cur_node_id_, cur_node_)) 
		{
            return -1;
        }
        cur_node_id_ = cur_node_.node_id_;
        
        // If_low_key exist, compare cur_node_.key with lowKey, then high_key
        while (idx_for_keys_ < cur_node_.keys_.size()) {
            if (low_key_) {
                compare_low_key_eq_ = BtreeNode::CompareKey(cur_node_.keys_[idx_for_keys_].get(), low_key_, attr_type_);
            }
            if (high_key_) {
                compare_high_key_eq_ = BtreeNode::CompareKey(cur_node_.keys_[idx_for_keys_].get(), high_key_, attr_type_);
            }
            // cur_node_.keys_[idx] compare with lowKey and highkey
            if (compare_low_key_eq_ == 1) 
			{

                if (compare_high_key_eq_ == 1)
				{
                    return -1;
                }
                else if (compare_high_key_eq_ == 0)
				{
                    if (!if_high_key_) 
					{
                        return -1;
                    }
                }
                // key
                CopyKey(key, cur_node_.keys_[idx_for_keys_].get(), attr_type_);
                // rid
                //cout<<"here size:"<<cur_node_.buckets_.size()<<std::endl;
                while (second_idx_ < cur_node_.buckets_[idx_for_keys_].size()) {
                    rid = cur_node_.buckets_[idx_for_keys_][second_idx_];
                    second_idx_ ++;
                    return 0;
                }
            }
            else if (compare_low_key_eq_ == 0) 
			{
    
                if (compare_high_key_eq_ == 1) 
				{
                    return -1;
                }
                else if (compare_high_key_eq_ == 0)
				{
                    if (!if_high_key_) 
					{
                        return -1;
                    }
                }
                if (if_low_key_) 
				{
                    CopyKey(key, cur_node_.keys_[idx_for_keys_].get(), attr_type_);
                    while (second_idx_ < cur_node_.buckets_[idx_for_keys_].size())
					{
                        rid = cur_node_.buckets_[idx_for_keys_][second_idx_];
                        second_idx_ ++;
                        return 0;
                    }
                }
                else 
				{
                    idx_for_keys_++;
                }
            }
            else
			{
                idx_for_keys_++;
            }
        }
        if (cur_node_.right_sibling_ != 0)
		{
			//go to next one
            cur_node_id_ = cur_node_.right_sibling_;

            idx_for_keys_ = 0;
            first=1;

        }
        else 
		{
			first=0;
            return -1;
        }

	}

    }
	// if not the first time
    else 
	{
    while(second)
    {
        //  compare cur_node_.key with high_key; if valid, read its RID and key
           
        while (idx_for_keys_ < cur_node_.keys_.size())
		{
            if (high_key_ && second_idx_ == 0)
			{
                compare_high_key_eq_ = BtreeNode::CompareKey(cur_node_.keys_[idx_for_keys_].get(), high_key_, attr_type_);
            }
            // cur_node_.keys_[idx] < high_key
            if (compare_high_key_eq_ == -1) {
                if (second_idx_ == 0) {
                    CopyKey(key, cur_node_.keys_[idx_for_keys_].get(), attr_type_);
                }
                while (second_idx_ < cur_node_.buckets_[idx_for_keys_].size()) {
                    rid = cur_node_.buckets_[idx_for_keys_][second_idx_];
                    second_idx_ ++;
                    return 0;
                }
                second_idx_ = 0;
                idx_for_keys_++;
            }
            else if (compare_high_key_eq_ == 0) {
                if (if_high_key_) {
                    // if in the middle of duplicate key's RID vector, just go directly to the position
                    if (second_idx_ == 0) {
                        CopyKey(key, cur_node_.keys_[idx_for_keys_].get(), attr_type_);
                    }
                    while (second_idx_ < cur_node_.buckets_[idx_for_keys_].size()) 
					{
                        rid = cur_node_.buckets_[idx_for_keys_][second_idx_];
                        second_idx_ ++;
                        return 0;
                    }
                    second_idx_ = 0;
                    idx_for_keys_++;
                }
                else 
                    return -1;
            }
            else 
                return -1;
        }
        if (cur_node_.right_sibling_ != 0) 
		{
            if (0 != ReadEveryNode(cur_node_.right_sibling_, cur_node_)) 
                return -1;
			 // update cur_node_id_ 
            cur_node_id_ = cur_node_.node_id_;
            idx_for_keys_ = 0;
            second=1;
        }
        else
        {
        	second=0;
            return -1;
        }
    }
    }
}

IXFileHandle::IXFileHandle()
{
	this -> counter_=0;
    this -> ixReadPageCounter = 0;
    this -> ixWritePageCounter = 0;
    this -> ixAppendPageCounter = 0;
}

IXFileHandle::~IXFileHandle()
{
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
	return file_handle_.collectCounterValues(readPageCount, writePageCount, appendPageCount);
}

RC IXFileHandle::readPage(PageNum pageNum, void *data)
{

	return file_handle_.readPage(pageNum, data) ;
}

RC IXFileHandle::writePage(PageNum pageNum, const void *data)
{
	return file_handle_.writePage(pageNum, data) ;
}

RC IXFileHandle::appendPage(const void *data)
{
	return file_handle_.appendPage(data) ;
}

RC IXFileHandle::openFile(const std::string &fileName)
{
    this -> counter_++;
	this -> filename_ = fileName;
	return file_handle_.openFile(fileName) ;
}

RC IXFileHandle::closeFile()
{
    this -> counter_--;    
	return file_handle_.closeFile();
}


BtreeNode::BtreeNode(){
}

BtreeNode::BtreeNode(char *data){
    LoadNode(data);
}

void BtreeNode::LoadNode(char *data){
    memcpy(this->node_data_, data, PAGE_SIZE * sizeof(char));

	unsigned node_id = 0;
    memcpy(&node_id, node_data_, sizeof(int));
    this->node_id_ = node_id;

	NodeType nodeType = Index;
    memcpy(&nodeType, node_data_ + sizeof(int), sizeof(int));
    this->node_type_ = nodeType;

	AttrType attrType = TypeInt;
    memcpy(&attrType, node_data_ + 2*sizeof(int), sizeof(int));
    this->attr_type_ = attrType;

	int attrLen = 0;
    memcpy(&attrLen, node_data_ + 3*sizeof(int), sizeof(int));
    this->attr_len_ = attrLen;

	int deleted = 0;
    memcpy(&deleted, node_data_ + 4*sizeof(int), sizeof(int));
    this->deleted_ = deleted;

	int degree = 0;
    memcpy(&degree, node_data_ + 5*sizeof(int), sizeof(int));
    this->degree_ = degree;

    this->keys_ = ReadKeys(this->attr_type_, 2 * this->degree_);


	int left_sibling = 0;
    memcpy(&left_sibling, node_data_ + 6*sizeof(int), sizeof(int));
    this->left_sibling_ = left_sibling;

	int right_sibling = 0;
    memcpy(&right_sibling, node_data_ + 7*sizeof(int), sizeof(int));
    this->right_sibling_ = right_sibling;

	std::vector<int> childList;
    int size = 0, child = 0;
    memcpy(&size, node_data_ + 8 * sizeof(int), sizeof(int));
    for (int i = 0; i < size; i++){
        memcpy(&child, node_data_ + (9 + i)*sizeof(int), sizeof(int));
        childList.push_back(child);
    }
    this->child_list_ = childList;
}

RC BtreeNode::CompareKey(const void *attrValue, const void *value, AttrType attrType){
    int ans = 0;
    if(attrType==TypeInt)
	{
		    int x = *(int *)attrValue;
            int y = *(int *)value;
            if (x > y) 
				ans = 1;
            else if (x == y) 
				ans = 0;
            else 
				ans = -1;

	}
	 if(attrType==TypeReal)
	{
		    float x = *(float *)attrValue;
            float y = *(float *)value;
            if (x > y) 
				ans = 1;
            else if (x == y) 
				ans = 0;
            else 
				ans = -1;

	}
    
     if(attrType==TypeVarChar) 
	 {
            unsigned xLen = (*(unsigned *)attrValue);
            char* x = (char *)attrValue + sizeof(int);
            unsigned yLen = *(unsigned *)value;
            char* y = (char *)value + sizeof(int);
            if (xLen == yLen && (strncmp(x, y, xLen) == 0))
            	ans = 0;
            else if (strncmp(x, y, xLen) > 0) 
            	ans = 1;
            else 
            	ans = -1;
    }
    return ans;
}

int BtreeNode::GetKeyIndex(const void* key){
    if (keys_.size() == 0)
        return 0;
    // binary Search is better
    int left = 0, right = (int)keys_.size()-1, cur;
    while (left != right){
        cur = (left + right) / 2;
        int eq = CompareKey(key, keys_[cur].get(), this->attr_type_);
        if (eq > 0) left = cur + 1;
        else right = cur;
    }
    if (CompareKey(key, keys_[left].get(), this->attr_type_) <= 0) 
        return left;
    else 
        return left + 1;
}



int BtreeNode::GetChildIndex(const void* key, int keyIndex){
    if (keyIndex == keys_.size()) return keyIndex;
    int eq = CompareKey(key, keys_[keyIndex].get(), this->attr_type_);

    if (eq == 0)
	{
        return keyIndex+1;
    }
    else
	{
        return keyIndex;
    }
}

int BtreeNode::GetLeafIndex(const void* key, int keyIndex){
    if (keyIndex == keys_.size())
        return -1;
    int eq = CompareKey(key, keys_[keyIndex].get(), this->attr_type_);
    if (eq != 0)
        return -1;
    return keyIndex;
}

RC BtreeNode::InsertIndex(std::shared_ptr<void> key, const int &childNodeID){
    RC rc = 0;
    int index = GetKeyIndex(key.get());
    
    keys_.insert(keys_.begin() + index, key);
    child_list_.insert(child_list_.begin() + index +1, childNodeID);
    return rc;
}

RC BtreeNode::InsertLeaf(std::shared_ptr<void> key, const RID &rid){
    RC rc = 0;
// <<<<<<< HEAD
    int index = GetKeyIndex(key.get());
    if (keys_.size() > 0 && index < keys_.size() && 0 == CompareKey(key.get(), keys_[index].get(), this->attr_type_))
	{
// =======
//     int index = GetKeyIndex(key.get());
//     //cout << "InsertLeaf getkey"<<index<<" "<<*(int *)key<<std::endl;
//     if (IsDuplicate(key.get(), index)){
// >>>>>>> a temp storage
        for (int i = 0; i < buckets_[index].size(); i++)
            if (buckets_[index][i].pageNum == rid.pageNum && buckets_[index][i].slotNum == rid.slotNum)
                return -1;
        buckets_[index].push_back(rid);
    }else{
        keys_.insert(keys_.begin() + index, key);
        Bucket bucket;
        bucket.push_back(rid);
        child_list_.insert(child_list_.begin() + index, 0);
        buckets_.insert(buckets_.begin() + index, bucket);
    }
    return rc;
}
RC BtreeNode::ReadBucket(IXFileHandle &ixfileHandle){
    RC rc = 0;
    char overflowPage_buffer[PAGE_SIZE];
    memset(overflowPage_buffer, 0, PAGE_SIZE);
    
    buckets_.clear();
    int offset = GetOverflowPageIDOffset();
    int overflowPageID = 0;
    int nextPageID = 0;
    int buckets_Size = 0;
    
    // read overflow bucket PageID
    memcpy(&overflowPageID, node_data_ + offset, sizeof(int));
    offset += sizeof(int);
    // read buckets_Size
    memcpy(&buckets_Size, node_data_ + offset, sizeof(int));
    offset += sizeof(int);

    buckets_.reserve(buckets_Size);
    for (int i = 0; i < buckets_Size; i++){
        RID rid;
        memcpy(&rid.pageNum, node_data_ + offset,  sizeof(int));
        offset += sizeof(int);
        memcpy(&rid.slotNum, node_data_ + offset, sizeof(int));
        offset += sizeof(int);
        Bucket bucket;
        bucket.push_back(rid);
        buckets_.push_back(bucket);
    }
    // read overflow bucket page
    if (overflowPageID != 0)
	{
        rc += ixfileHandle.file_handle_.readPage(overflowPageID, overflowPage_buffer);
        offset = 0;
        // read nextPageID
        memcpy(&nextPageID, overflowPage_buffer,  sizeof(int));
        offset += sizeof(int);
        for (int i = 0; i < buckets_Size; i++)
		{
            int size = 0;
            if (offset + sizeof(int) > PAGE_SIZE)
			{
                rc += ixfileHandle.file_handle_.readPage(nextPageID, overflowPage_buffer);
                memcpy(&nextPageID, overflowPage_buffer, sizeof(int));
                offset = sizeof(int);
            }
            memcpy(&size, overflowPage_buffer + offset,  sizeof(int));
            offset += sizeof(int);
            for(int j = 0; j < size; j++)
			{
                RID rid;
                if (offset + 2 * sizeof(int) > PAGE_SIZE)
				{
                    rc += ixfileHandle.file_handle_.readPage(nextPageID, overflowPage_buffer);
                    memcpy(&nextPageID, overflowPage_buffer, sizeof(int));
                    offset = sizeof(int);
                }
                memcpy(&rid.pageNum, overflowPage_buffer + offset,  sizeof(int));
                offset += sizeof(int);
                memcpy(&rid.slotNum, overflowPage_buffer + offset,  sizeof(int));
                offset += sizeof(int);
                buckets_[i].push_back(rid);
            }
        }
    }
    return rc;
}

RC BtreeNode::WriteBucket(IXFileHandle &ixfileHandle){ // here is the wrong place:!!!!!
    RC rc = 0;
    char overflowPage_buffer[PAGE_SIZE];
    memset(overflowPage_buffer, 0, PAGE_SIZE);
    
    int offset = GetOverflowPageIDOffset();
    int overflow = 0;
    int overflowPageID = 0;
    
    // read overflow bucket PageID
    memcpy(&overflowPageID, node_data_ + offset, sizeof(int));
    offset += sizeof(int);
    int buckets_Size = (int)buckets_.size();
    memcpy(node_data_ + offset, &buckets_Size, sizeof(int));
    offset += sizeof(int);
    for (int i = 0; i < buckets_Size; i++)
	{
        memcpy(node_data_ + offset, &buckets_[i][0].pageNum, sizeof(int));
        offset += sizeof(int);
        memcpy(node_data_ + offset, &buckets_[i][0].slotNum, sizeof(int));
        offset += sizeof(int);
        if (buckets_[i].size() > 1) overflow = 1;
    }
    // write overflow page
    if (overflow == 0)
	{
        memset(node_data_ + GetOverflowPageIDOffset(), 0, sizeof(int));        
    }
    else{
        offset = 0;
        if (overflowPageID == 0){
            // create the first overflow page, leave the first sizeof(int) for nextPageID, Init: pointer return to node_id
            memcpy(overflowPage_buffer,  &this->node_id_, sizeof(int));
            offset += sizeof(int);
            rc += ixfileHandle.file_handle_.appendPage(overflowPage_buffer);
            // update overflowPageID
            overflowPageID = ixfileHandle.file_handle_.getNumberOfPages() - 1;
            memcpy(node_data_ + GetOverflowPageIDOffset(), &overflowPageID, sizeof(int));
        }
        rc += ixfileHandle.file_handle_.readPage(overflowPageID, overflowPage_buffer);
        
        int nextPageID = this -> node_id_;
        memcpy(&nextPageID, overflowPage_buffer, sizeof(int));
        offset = sizeof(int);
        // write overflow page
        for (int i = 0; i < buckets_Size; i++)
		{
            if (offset + sizeof(int) > PAGE_SIZE)
			{
                if (nextPageID == this -> node_id_)
				{
                    rc += AddBucketPage(ixfileHandle, nextPageID);
                    memcpy(overflowPage_buffer, &nextPageID, sizeof(int));
                }
                rc += ixfileHandle.file_handle_.writePage(overflowPageID, overflowPage_buffer);
                
                overflowPageID = nextPageID;
                rc += ixfileHandle.file_handle_.readPage(overflowPageID, overflowPage_buffer);
                memcpy(&nextPageID, overflowPage_buffer, sizeof(int));
                offset = sizeof(int);
            }
            int size = (int)buckets_[i].size() - 1;
            memcpy(overflowPage_buffer + offset, &size, sizeof(int));
            offset += sizeof(int);
            for(int j = 1; j < size + 1; j++){
                if (offset + 2 * sizeof(int) > PAGE_SIZE)
				{
                    if (nextPageID == this -> node_id_)
					{
                        rc += AddBucketPage(ixfileHandle, nextPageID);
                        memcpy(overflowPage_buffer, &nextPageID, sizeof(int));
                    }
                    rc += ixfileHandle.file_handle_.writePage(overflowPageID, overflowPage_buffer);
                    
                    overflowPageID = nextPageID;
                    rc += ixfileHandle.file_handle_.readPage(overflowPageID, overflowPage_buffer);
                    memcpy(&nextPageID, overflowPage_buffer, sizeof(int));
                    offset = sizeof(int);
                }
                memcpy(overflowPage_buffer + offset, &buckets_[i][j].pageNum, sizeof(int));
                offset += sizeof(int);
                memcpy(overflowPage_buffer + offset, &buckets_[i][j].slotNum, sizeof(int));
                offset += sizeof(int);
            }
        }
        rc += ixfileHandle.file_handle_.writePage(overflowPageID, overflowPage_buffer);
    }
    return rc;
}

RC BtreeNode::AddBucketPage(IXFileHandle &ixfileHandle, int &nextPageID)
{
    RC rc = 0;
    char bucketData[PAGE_SIZE];
    memset(bucketData, 0, PAGE_SIZE);
    // first 4 byte for nextPageID
    memcpy(bucketData, &this -> node_id_, sizeof(int));
    rc += ixfileHandle.file_handle_.appendPage(bucketData);
    nextPageID = ixfileHandle.file_handle_.getNumberOfPages() - 1;
    return rc;
}

// Btree
BtreeHandle::BtreeHandle(): attr_type_(TypeInt), root_id_(0), first_leaf_id_(0), last_leaf_id_(1)
{
}

RC BtreeHandle::CreateNewNode(IXFileHandle &ixfileHandle, NodeType nodeType, BtreeNode &node)
{

    char buffer[PAGE_SIZE];
    memset(buffer, 0, PAGE_SIZE);
    memcpy(node.node_data_, buffer, PAGE_SIZE * sizeof(char));
    node.node_type_ = nodeType;
    node.degree_ = this->degree_;
    node.attr_type_ = this->attr_type_;
    node.attr_len_ = attr_len_;
    node.left_sibling_ = 0;
    node.right_sibling_ = 0;
	node.deleted_ = 0;

    if(ixfileHandle.file_handle_.appendPage(buffer))
		return -1;
    node.node_id_ = ixfileHandle.file_handle_.getNumberOfPages() - 1;
    
    this->last_leaf_id_ = node.node_id_;
    return 0;
}

RC BtreeHandle::ReadNode(IXFileHandle &ixfileHandle, int node_id, BtreeNode &node) const{
    char page_buffer[PAGE_SIZE];
    memset(page_buffer, 0, PAGE_SIZE);
    if(ixfileHandle.file_handle_.readPage(node_id, page_buffer))
		return -1;
    node.LoadNode(page_buffer);
    node.ReadBucket(ixfileHandle);
    return 0;
}

RC BtreeHandle::WriteNode(IXFileHandle &ixfileHandle, BtreeNode &node)
{
	memcpy(node.node_data_, &node.node_id_, sizeof(int));

	memcpy(node.node_data_ + sizeof(int), &node.node_type_, sizeof(int));

	memcpy(node.node_data_ + 2*sizeof(int), &node.attr_type_, sizeof(int));

	memcpy(node.node_data_ + 3 * sizeof(int), &attr_len_, sizeof(int));

	memcpy(node.node_data_ + 4 * sizeof(int), &node.deleted_, sizeof(int));

	memcpy(node.node_data_ + 5 * sizeof(int), &node.degree_, sizeof(int));

    node.WriteKeys(node.keys_, node.attr_type_, 2 * degree_);
    if (node.node_type_ == Leaf){

        node.WriteBucket(ixfileHandle);
		memcpy(node.node_data_ + 6 * sizeof(int), &node.left_sibling_, sizeof(int));

		memcpy(node.node_data_ + 7 * sizeof(int), &node.right_sibling_, sizeof(int));

    }

	int size = (int)node.child_list_.size();
    memcpy(node.node_data_ + 8 * sizeof(int), &size, sizeof(int));
    for (int i = 0; i < size; i++)
	{
        memcpy(node.node_data_ + (9 + i) * sizeof(int), &node.child_list_[i], sizeof(int));
    }
    if(ixfileHandle.file_handle_.writePage(node.node_id_, node.node_data_))
		return -1;
    return 0;
}

RC BtreeHandle::Insert(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid){
    RC rc = 0;
    // when empty, create new root
    if(root_id_ == NONE)
	{
        // set degree
        this->attr_type_ = attribute.type;
        attr_len_ = attribute.length;
        this->degree_ = GetOrder(attribute);
        
        BtreeNode root;
        rc += CreateNewNode(ixfileHandle, Leaf, root);

        std::shared_ptr<void> key_copy = AttrtypePtrConversion(attribute.type, key);
        root.InsertLeaf(key_copy, rid);
        rc += WriteNode(ixfileHandle, root);
        // set new info
        root_id_ = root.node_id_;
        first_leaf_id_ = root.node_id_;
    }
	else
	{
        int copyUpNodeID = -1;
        
        // check type
        if (this->attr_type_ != attribute.type) 
		{
            return -1;
        }
        else if (attribute.type == TypeVarChar && attribute.length != attr_len_) 
		{
                return -1;
        }
        
        // RecursiveInsert(ixfileHandle, root_id_, key, rid, copyUpNodeID, copyUpKey);
        LoopInsert(ixfileHandle, root_id_, key, rid);
        // free(copyUpKey);
    }
    return rc;
}

void BtreeHandle::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const {
    if (this->root_id_ == NONE){
        //std::cout << "null tree" << std::endl;
    }
    else{
        if (this->attr_type_ != attribute.type){
            //std::cout << "error type" << std::endl;
        }
        else{//print tree
            std::cout << "{" << std::endl;
            this -> RecursivePrint(ixfileHandle, this->root_id_);
            std::cout << "}" << std::endl;
        }
    }
}

RC BtreeHandle::RecursivePrint(IXFileHandle &ixfileHandle, int node_id) const{
    BtreeNode cur_node;
    ReadNode(ixfileHandle, node_id, cur_node);

    // generate keys_str for printing
    std::vector<std::string> keys_str;
    switch(cur_node.attr_type_)
	{
        case TypeVarChar:
            for(auto key_pointer : cur_node.keys_)
			{
                int var_char_len = 0;
                memcpy(&var_char_len, (int *)key_pointer.get(), sizeof(int));
                std::string key_str((char*)(key_pointer.get() + sizeof(int)), var_char_len);
                // memcpy((char *) dest, (char *)key_pointer, var_char_len + sizeof(int));
                keys_str.push_back(key_str);
            }
            break;
        case TypeReal:
            for(auto& key_pointer : cur_node.keys_)
			{
                float key_real;
                memcpy(&key_real, (float *) key_pointer.get(), sizeof(float));
                //std::string s = std::to_string(key_real);
                std::string key_str = std::to_string(key_real);
                keys_str.push_back(key_str);
            }
            break;
        case TypeInt:
            for(auto& key_pointer : cur_node.keys_)
			{
                int key_int;
                memcpy(&key_int, (int *) key_pointer.get(), sizeof(int));
                keys_str.push_back(std::to_string(key_int));
            }
            break;
    }

    //if(cur_node.attr_type_ == )
    // print keys_list
    if(cur_node.node_type_ == Leaf) {
        std::cout << "\"keys\":[";
        for(size_t i = 0; i < keys_str.size(); i++){
            std::cout << "\"" << keys_str[i] << ":";
            // print rid_lists for in key bucket
            std::cout << "[";
            for(auto key : cur_node.keys_){
                int key_index = cur_node.GetKeyIndex(key.get());
                int leaf_index = cur_node.GetLeafIndex(key.get(), key_index);
                for (size_t i = 0; i < cur_node.buckets_[leaf_index].size(); i++){
                        std::cout << "(" << cur_node.buckets_[leaf_index][i].pageNum
                                    << "," << cur_node.buckets_[leaf_index][i].slotNum << ")";
                        if(i != cur_node.buckets_[leaf_index].size() - 1)
                            std::cout << ",";
                }
            }
            std::cout << "]";
            std::cout << "\"";
            if (i != keys_str.size() - 1)
                std::cout << ",";
        }
        std::cout << "]," << std::endl;
    }
    else {
        std::cout << "\"keys\":[";
        for(size_t i = 0; i < keys_str.size(); i++){
            std::cout << "\"" << keys_str[i] << "\"";
            if (i != keys_str.size() - 1)
                std::cout << ",";
        }
        std::cout << "]," << std::endl;

        std::cout << "\"" << "children" << "\": [" << std::endl;
        for(size_t i = 0; i < cur_node.child_list_.size(); i++) {
            std::cout << "{";
            RecursivePrint(ixfileHandle, cur_node.child_list_[i]);
            std::cout << "}";
            if(i != cur_node.child_list_.size() - 1)
                std::cout << ",";
            std::cout << std::endl;
        }
        std::cout << "]";
    }
}

RC BtreeHandle::LoopInsert(IXFileHandle &ixfileHandle, int root_id, const void *key, const RID &rid){ //    , int &copyUpNodeID, void *copyUpKey){
    RC rc = 0;
    BtreeNode cur_node;
    int current_node_id = root_id;
    rc += ReadNode(ixfileHandle, current_node_id, cur_node);
    
    std::stack<int> node_id_stack;

    int copy_up_new_node_id = -1;
    std::shared_ptr<void> copy_up_new_key (malloc(PAGE_SIZE), free);
    // void *copy_up_new_key = malloc(PAGE_SIZE);

    while (cur_node.node_type_ != Leaf){
        int key_index = cur_node.GetKeyIndex(key);
        int child_index = cur_node.GetChildIndex(key, key_index);
        int child_node_id = cur_node.child_list_[child_index];

        // renew cur_node
        rc += ReadNode(ixfileHandle, child_node_id, cur_node);
        node_id_stack.push(current_node_id);
        current_node_id = child_node_id;
    }

    // work on leaf tree
    if (cur_node.node_type_ == Leaf){
        if (cur_node.deleted_ == 1) cur_node.deleted_ = 0;

        if (cur_node.keys_.size() == degree_ * 2) {
            // the leaf node is full, split the leaf node
            BtreeNode new_node;
            rc += CreateNewNode(ixfileHandle, Leaf, new_node);
            rc += SplitLeafNode(ixfileHandle, cur_node, new_node);
            int key_index = cur_node.GetKeyIndex(key);
            if (key_index < degree_){
                // new key belongs to the old node
                cur_node.InsertLeaf(AttrtypePtrConversion(this->attr_type_, key), rid);
            }else{
                // new key belongs to the new node
                new_node.InsertLeaf(AttrtypePtrConversion(this->attr_type_, key), rid);
            }
            rc += WriteNode(ixfileHandle, cur_node);
            rc += WriteNode(ixfileHandle, new_node);
            
            copy_up_new_node_id = new_node.node_id_;
            GetCopyUpKey(new_node.keys_[0].get(), copy_up_new_key.get(), this->attr_type_);
            
            // special case: if current node is root, create a new index node as root
            if (cur_node.node_id_ == root_id_) {
                // get index node
                BtreeNode indexNode;
                rc += CreateNewNode(ixfileHandle, Index, indexNode);
                indexNode.keys_.push_back(copy_up_new_key);
                indexNode.child_list_.push_back(cur_node.node_id_);
                indexNode.child_list_.push_back(new_node.node_id_);
                // update information of this -> btree_
                this->root_id_ = indexNode.node_id_;
                rc += WriteNode(ixfileHandle, indexNode);
                copy_up_new_node_id = -1;
            }
        // end of cur_node.keys_.size() == MAXNUM_KEY
        }

        else{
            cur_node.InsertLeaf(AttrtypePtrConversion(this->attr_type_, key), rid);
            rc += WriteNode(ixfileHandle, cur_node);
        }
    // end of cur_node.node_type_ == Leaf
    }


    // trace up using node_id_stack when split
    while( !node_id_stack.empty() ){
        current_node_id = node_id_stack.top();
        node_id_stack.pop();
        rc += ReadNode(ixfileHandle, current_node_id, cur_node);
        cur_node;

        if (copy_up_new_node_id != -1){
            if (cur_node.keys_.size() == degree_ * 2){
                // the index node is full, split the index node
                BtreeNode new_node;
                rc += CreateNewNode(ixfileHandle, Index, new_node);
                cur_node.InsertIndex(copy_up_new_key, copy_up_new_node_id);
                rc += SplitIndexNode(ixfileHandle, cur_node, new_node, copy_up_new_key.get());
                
                rc += WriteNode(ixfileHandle, cur_node);
                rc += WriteNode(ixfileHandle, new_node);
                
                //int keyIndex = cur_node.GetKeyIndex(key);
                //cout << "cur_node.keys_.size() == MAXNUM_KEY keyIndex = "<<keyIndex<<std::endl;
                
                copy_up_new_node_id = new_node.node_id_;
                
                // special case: if current node is root, create a new index node as root
                if (cur_node.node_id_ == this->root_id_) {
                    // get index node
                    BtreeNode index_node;
                    CreateNewNode(ixfileHandle, Index, index_node);
                    index_node.keys_.push_back(copy_up_new_key);
                    index_node.child_list_.push_back(cur_node.node_id_);
                    index_node.child_list_.push_back(new_node.node_id_);
                    // update information of this -> btree_
                    this->root_id_ = index_node.node_id_;
                    rc += WriteNode(ixfileHandle, index_node);
                    copy_up_new_node_id = -1;
                }
                // end of cur_node.keys_.size() == MAXNUM_KEY
            }else{
                cur_node.InsertIndex(copy_up_new_key, copy_up_new_node_id);
                rc += WriteNode(ixfileHandle, cur_node);
                copy_up_new_node_id = -1;
            // end of cur_node.keys_.size() != MAXNUM_KEY
            }
        }        
    }

    // free(copy_up_new_key);
    return rc;

}

RC BtreeHandle::Delete(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid){
    RC rc = 0;
	//not exist
    if(this->root_id_ == NONE) 
		return -1;
    else
	{
        // check type
        if (this->attr_type_ != attribute.type) 
            return -1;
        else if (attribute.type == TypeVarChar && attribute.length != attr_len_) 
                return -1;
        
        
        int node_id = this -> Search(ixfileHandle, key);
        
		// get node
        BtreeNode node;
        ReadNode(ixfileHandle, node_id, node);
        if(node.deleted_ == 1) 
			return -1;
        else
		{
            int keyIndex = node.GetKeyIndex(key);
            int leafIndex = node.GetLeafIndex(key, keyIndex);
            if (leafIndex >= 0) 
			{
                int flag = 0;
                for (int i = 0; i < node.buckets_[leafIndex].size(); i++)
                    if(node.buckets_[leafIndex][i].pageNum == rid.pageNum && node.buckets_[leafIndex][i].slotNum == rid.slotNum)
					{
                        // delete key and rid
                        node.buckets_[leafIndex].erase(node.buckets_[leafIndex].begin() + i);
                        flag = 1;
                    }
                // rid not exist
                if (flag== 0) 
					return -1;

                // if no rid
                if (node.buckets_[leafIndex].size() == 0){
                    node.keys_.erase(node.keys_.begin() + leafIndex);
                    node.child_list_.erase(node.child_list_.begin() + leafIndex);
                    node.buckets_.erase(node.buckets_.begin() + leafIndex);
                }
                // keys_ is empty
                if (node.keys_.size() == 0) 
					node.deleted_ = 1;
            }
			else
			{
                // keynot existed
                return -1;
            }
            rc += WriteNode(ixfileHandle, node);
        }
    }
    return rc;
}

int BtreeHandle::Search(IXFileHandle &ixfileHandle, const void *key){
    BtreeNode cur_node;
    int cur_node_id = this->root_id_;
    ReadNode(ixfileHandle, cur_node_id, cur_node);
    while(cur_node.node_type_ != Leaf){
        int keyIndex = cur_node.GetKeyIndex(key);
        int childIndex= cur_node.GetChildIndex(key, keyIndex);
        cur_node_id = cur_node.child_list_[childIndex];
        ReadNode(ixfileHandle, cur_node_id, cur_node);
    }
    return cur_node_id;
}

int BtreeHandle::RecursiveSearch(IXFileHandle &ixfileHandle, int node_id, const void *key){
    
	BtreeNode node;
    ReadNode(ixfileHandle, node_id, node);
    
    if (node.node_type_ == Leaf)
	{
        return node.node_id_;
    }
	else
	{
        // get the child
        int keyIndex = node.GetKeyIndex(key);
        int childIndex= node.GetChildIndex(key, keyIndex);
        int childNodeID = node.child_list_[childIndex];
        return RecursiveSearch(ixfileHandle, childNodeID, key);
    }
}

RC BtreeHandle::SplitLeafNode(IXFileHandle &ixfileHandle, BtreeNode &oldNode, BtreeNode &newNode){
    RC rc = 0;
    char page_buffer[PAGE_SIZE];
    memset(page_buffer, 0, PAGE_SIZE);
    
    for (int i = degree_; i < 2*degree_; i++) 
	{
        // move keys_ child_list_ buckets_ 
        newNode.keys_.push_back(oldNode.keys_[i]);
        newNode.child_list_.push_back(oldNode.child_list_[i]);
        newNode.buckets_.push_back(oldNode.buckets_[i]);
    }
    // delete in old node
    oldNode.keys_.erase(oldNode.keys_.begin() + degree_, oldNode.keys_.end());
    oldNode.child_list_.erase(oldNode.child_list_.begin() + degree_, oldNode.child_list_.end());
    oldNode.buckets_.erase(oldNode.buckets_.begin() + degree_, oldNode.buckets_.end());
    
    // update left_sibling_ and right_sibling_
    if (oldNode.right_sibling_ != 0)
	{
        BtreeNode nextnextNode;
        rc += ReadNode(ixfileHandle, oldNode.right_sibling_, nextnextNode);
        nextnextNode.left_sibling_ = newNode.node_id_;
        rc += WriteNode(ixfileHandle, nextnextNode);
    }
    newNode.right_sibling_ = oldNode.right_sibling_;
    oldNode.right_sibling_ = newNode.node_id_;
    newNode.left_sibling_ = oldNode.node_id_;
    return rc;
}

RC BtreeHandle::SplitIndexNode(IXFileHandle &ixfileHandle, BtreeNode &oldNode, BtreeNode &newNode, void *copyUpKey)
{
    RC rc = 0;
    char page_buffer[PAGE_SIZE];
    memset(page_buffer, 0, PAGE_SIZE);
    
    for (int i = degree_; i < 2*degree_; i++) {
        // move keys_ child_list_
        newNode.keys_.push_back(oldNode.keys_[i + 1]);
        newNode.child_list_.push_back(oldNode.child_list_[i + 1]);
    }
    newNode.child_list_.push_back(oldNode.child_list_[2 * degree_ + 1]);
    //node in the middle, up
    GetCopyUpKey(oldNode.keys_[degree_].get(), copyUpKey, this->attr_type_);
    // delete in old node
    oldNode.keys_.erase(oldNode.keys_.begin() + degree_, oldNode.keys_.end());
    oldNode.child_list_.erase(oldNode.child_list_.begin() + degree_ + 1, oldNode.child_list_.end());
    return rc;
}

RC BtreeHandle::GetCopyUpKey(void* source, void * dest, AttrType attrType)
{
    switch (attrType) 
	{
        case TypeInt:
            memcpy((char *) dest, (char *) source, sizeof(int));
            break;
        case TypeReal:
            memcpy((char *) dest, (char *) source, sizeof(float));
            break;
        case TypeVarChar:
            int varCharLen = 0;
            memcpy(&varCharLen, (char *)source, sizeof(int));
            memcpy((char *) dest, (char *) source, varCharLen + sizeof(int));
            break;
    }
    return 0;
}

int BtreeHandle::GetOrder(const Attribute &attribute){

    int degree = 0;
    switch (attribute.type) 
	{
        case TypeInt:
            degree = (PAGE_SIZE / sizeof(int) - 13) / 8;
            break;
        case TypeReal:
            degree = (PAGE_SIZE / sizeof(float) - 13) / 8;
            break;
        case TypeVarChar:
            degree = (PAGE_SIZE - 13 * sizeof(int)) / (2 * attribute.length * sizeof(char) + 8 * sizeof(int));
    }
 
    return degree;
}

std::shared_ptr<void> AttrtypePtrConversion(const AttrType& attr_type, const void* key){
    switch (attr_type) {
        case TypeInt:{
            std::shared_ptr<void> key_copy_int(malloc(4), free);
            // void *key = malloc(sizeof(int));
            memcpy(key_copy_int.get(), (int*)key, sizeof(int));
            return key_copy_int;
            break;
        }
        case TypeReal:{
            std::shared_ptr<void> key_copy_real(malloc(4), free);
            // void *key = malloc(sizeof(float));
            memcpy(key_copy_real.get(), (float*)key, sizeof(float));
            return key_copy_real;
            break;
        }
        case TypeVarChar:{
            unsigned varCharLen = 0;
            memcpy(&varCharLen, key, sizeof(int));
            std::shared_ptr<void> key_copy_varchar(malloc(varCharLen + sizeof(int)), free);
            //void *key = malloc(varCharLen + sizeof(int));
            memcpy(key_copy_varchar.get(), (char*)key, varCharLen + sizeof(int));
            return key_copy_varchar;
            break;
        }
    }
}

std::vector<std::shared_ptr<void>> BtreeNode::ReadKeys(AttrType attrType, int max_key_num){
    std::vector<std::shared_ptr<void>> temp_keys;
    int size = 0;
    int offset = (9 + max_key_num + 1) * sizeof(int);
    memcpy(&size, node_data_ + offset, sizeof(int));
    offset += sizeof(int);
    for (int i = 0; i < size; i++)
	{
        if (attrType == TypeVarChar)
		{
            unsigned varCharLen = 0;
            memcpy(&varCharLen, node_data_ + offset, sizeof(int));
            std::shared_ptr<void> key(malloc(varCharLen + sizeof(int)), free);
            //void *key = malloc(varCharLen + sizeof(int));
            memcpy(key.get(), (char*)node_data_ + offset, varCharLen + sizeof(int));
            offset += (varCharLen + sizeof(int));
            temp_keys.push_back(key);
            // free(key);
        }else if (attrType == TypeInt){
            std::shared_ptr<void> key(malloc(sizeof(int)), free);
            // void *key = malloc(sizeof(int));
            memcpy(key.get(), (char*)node_data_ + offset, sizeof(int));
            offset += sizeof(int);
            temp_keys.push_back(key);
            // free(key);
        }else if (attrType == TypeReal){
            std::shared_ptr<void> key(malloc(sizeof(float)), free);
            // void *key = malloc(sizeof(float));
            memcpy(key.get(), (char*)node_data_ + offset, sizeof(float));
            offset += sizeof(float);
            temp_keys.push_back(key);
            // free(key);
        }
    }
    return temp_keys;
}

void BtreeNode::WriteKeys(std::vector<std::shared_ptr<void>> &temp_keys, AttrType attrType, int maximumKeyNumber){
    int offset = (9 + maximumKeyNumber + 1) * sizeof(int);
    int size = (int)temp_keys.size();
    memcpy((char *)node_data_ + offset, &size, sizeof(int));
    offset += sizeof(int);
    for (int i = 0; i < size; i++){
        if (attrType == TypeVarChar){
            int varCharLen = *(int *)temp_keys[i].get();
            memcpy((char *)node_data_ + offset, (char *)temp_keys[i].get(), varCharLen + sizeof(int));
            offset += (varCharLen + sizeof(int));
        }else if (attrType == TypeInt){
            memcpy((char *)node_data_ + offset, temp_keys[i].get(), sizeof(int));
            offset += sizeof(int);
        }else if (attrType == TypeReal){
            memcpy((char *)node_data_ + offset, temp_keys[i].get(), sizeof(float));
            offset += sizeof(float);
        }
    }
}

int BtreeNode::GetOverflowPageIDOffset(){
    int offset = 0;
    switch (this->attr_type_) 
	{
        case TypeInt:
            offset = 44 + 16 * this->degree_;
            break;
        case TypeReal:
            offset = 44 + 16 * this->degree_;
            break;
        case TypeVarChar:
            offset = 44 + 2 * attr_len_ * this->degree_ + 16 * this->degree_;
			break;
    }
    return offset;
}
