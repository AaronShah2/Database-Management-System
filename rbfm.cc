#include "rbfm.h"
#include <cassert>
#include <cstring>
#include <sstream>
#include <cstdlib>
#include <iostream>
#include <cmath>

#include <string.h>

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager(){
    _pf_manager = PagedFileManager::instance();
}

RecordBasedFileManager::~RecordBasedFileManager()
{
}

RC RecordBasedFileManager::createFile(const std::string &fileName) {
    FileHandle file_handle;
    char hpHeader[PAGE_SIZE];
    _pf_manager->createFile(fileName);
    _pf_manager->openFile(fileName, file_handle);
    // wirte heap file header page
    // memset(hpHeader, 0, PAGE_SIZE);
    // file_handle.appendPage(hpHeader);
    _pf_manager->closeFile(file_handle);
    return SUCCESS;
}

RC RecordBasedFileManager::destroyFile(const std::string &fileName) {
    return _pf_manager->destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
    return _pf_manager->openFile(fileName,fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    return _pf_manager->closeFile(fileHandle);
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
    // read recordDescriptor and indicator in data to decode the data.

    // insert whole data.
    // find page with prior free space, cur -> front -> append. linear search.//matain RID
    // for insert and read, only deal with split and maintain header in page.
    // header: slot 

    // char headerPage_buffer[PAGE_SIZE];
    char page_buffer[PAGE_SIZE];
    // memset(headerPage_buffer, 0, sizeof(headerPage_buffer));
    memset(page_buffer, 0, sizeof(page_buffer));
    int page_num = -1;
    short slot_num = 0;
    
    // get length of the record
    short record_length = Record::getRecordLength(recordDescriptor, data);
    // find a page with free_space_size > record_length
    page_num = getFreePage(fileHandle, record_length);

    // read page[page_num] into page_buffer
    fileHandle.readPage((unsigned)page_num, page_buffer);
    // append record
    HFPage hfPage(page_buffer);
    Record record = hfPage.setRecord(record_length, slot_num);
    // write record
    record.writeRecord(recordDescriptor, data);
    // update free_space_size in heap file header
    // headerPage.setPageHeader(page_num, hfPage.free_space_);
    // write page
    assert(page_num != -1);
    fileHandle.writePage((unsigned)page_num, page_buffer);
    // fileHandle.writePage(0, headerPage_buffer);
    // write rid
    rid.pageNum = (unsigned)page_num;
    rid.slotNum = (unsigned)slot_num;
    // std::cout << "rid" << page_num << "--" << slot_num << std::endl;
    return 0;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
    RC rc = 0;
    char page_buffer[PAGE_SIZE];
    memset(page_buffer, 0, PAGE_SIZE);
    // get #page_num page -- O(1)
    fileHandle.readPage(rid.pageNum, page_buffer);
    HFPage hfPage(page_buffer);
    // get #slotNum record -- O(1)
    Record record;
    RID finalRid = rid;
    // check tombstone
    short a=hfPage.getLength(rid.slotNum);
    short recordoffset=hfPage.getOffset(finalRid.slotNum);

    if(a==0&&recordoffset==-1)
        return -1;

    while (hfPage.checkTombstone(finalRid.slotNum))
    {
        if((hfPage.getOffset(rid.slotNum))==-1)
            return -1;
        hfPage.getRID(finalRid.slotNum, finalRid);
        fileHandle.readPage(finalRid.pageNum, page_buffer);
        hfPage.load(page_buffer);
    }

    if(a>0)
    {
        record = hfPage.getRecord(finalRid.slotNum);
        // read record -- O(1)
        if (hfPage.getLength(finalRid.slotNum) > 0 && hfPage.getOffset(finalRid.slotNum) >= 0)
            rc += record.readRecord(recordDescriptor, data);
        else rc += 1;
        return rc;
    }
    else if(a==-2)
    {
        int addpage_num;
        short addoffset,addlen,addslot;
        memcpy(&addpage_num,page_buffer+recordoffset,sizeof(unsigned int));
        memcpy(&addslot,page_buffer+recordoffset+sizeof(unsigned int),sizeof(short));
        memcpy(&addlen,page_buffer+recordoffset+sizeof(unsigned int)+sizeof(short),sizeof(short));

        char addpage_buffer[PAGE_SIZE];
        memset(addpage_buffer, 0, PAGE_SIZE);
        fileHandle.readPage(addpage_num, addpage_buffer);
        HFPage addhfPage(addpage_buffer);
        addoffset=addhfPage.getOffset(addslot);
        Record addrecord(addpage_buffer+addoffset);
        addrecord.readRecord(recordDescriptor, data);

    }
    return 0;
}



RC RecordBasedFileManager::printRecord(const std::vector<Attribute> &recordDescriptor, const void *data)
{
    std::string attributename;
    const char* data1=(char *)data;
    const char* data2=(char *)data;
    int num,nullindicatorlen;
    num = recordDescriptor.size();
    nullindicatorlen=num/8+1;

    data1 = data1+nullindicatorlen;
    int tt=0;
    char a[nullindicatorlen*8];
    for(int k=0;k<nullindicatorlen;k++)
    {
    	char c=data2[k];
    	for(int j=0;j<8;j++)
    	{
    		char x=((c>>(7-j)) & 1)+'0';
    		a[tt++]=x;
    	}
    }

    for(int i=0;i<num;i++)
    {
    	attributename=recordDescriptor[i].name;
    	//std::cout  << attributename << ": " ;

    	if(a[i]=='1')
    		std::cout  << "NULL ";
    	else
    	{
        	if(recordDescriptor[i].type == TypeInt)
        	{
        		int contant;
        		memcpy(&contant,data1,sizeof(int));
        		data1=data1+sizeof(int);
        		std::cout  << contant << " ";
        	}
        	if(recordDescriptor[i].type == TypeReal)
        	{
        	    float contant;
        	    memcpy(&contant,data1,4);
        	    data1=data1+4;
        	    std::cout  << contant << " ";
        	}
        	if(recordDescriptor[i].type == TypeVarChar)
        	{
        		int len;
        		memcpy(&len,data1,sizeof(int));
        		data1=data1+sizeof(int);
        		char *var=new char[len + 1];
        		memcpy(var,data1,len);
        		data1=data1+len;
        		var[len]='\0';
        		std::cout  << var << " ";
                delete[] var;
        	}
    	}

    	std::cout << std::endl;
    }
	return 0;
}

int RecordBasedFileManager::getFreePage(FileHandle &fileHandle, short length){
    unsigned page_count = fileHandle.getNumberOfPages();
    void *page_buffer = malloc(PAGE_SIZE);
    // std::shared_ptr<void> page_buffer(malloc(PAGE_SIZE), free);

    for (unsigned i = 0; i < page_count; i++) {
        fileHandle.readPage(i, page_buffer);
        short free_space_size = 0;
        short free_offset = 0;
        short slot_count = 0;
        memcpy(&free_offset, page_buffer + (PAGE_SIZE - kFreeField), kFreeField);
        memcpy(&slot_count, page_buffer + (PAGE_SIZE - kFreeField - kSlotCntField), kSlotCntField);
        // std::cout << "unsigned: " << sizeof(unsigned) << std::endl;
        free_space_size = PAGE_SIZE - free_offset - 2 * (slot_count + 1) * kTwoBtyesSpace;
        // std::cout << "free_space_size: " << free_space_size << std::endl;

        if (free_space_size > (length + 2 * kTwoBtyesSpace)){
            free(page_buffer);
            //std::cout << "page_num : "<<page_num<<" freespace : "<<free_space_size<<std::endl;
            return i;
        }
    }

    free(page_buffer);
    
    void *new_page_buffer = malloc(PAGE_SIZE);
    short free_offset = 0;
    short slot_count = 0;
    memcpy(new_page_buffer + PAGE_SIZE - kFreeField, &free_offset, kFreeField);
    memcpy(new_page_buffer + PAGE_SIZE - kFreeField - kSlotCntField, 
        &slot_count, kSlotCntField);

    fileHandle.appendPage(new_page_buffer);
    free(new_page_buffer);
    return page_count;
}


RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid)
{
    char page_buffer[PAGE_SIZE];
    memset(page_buffer, 0, PAGE_SIZE);

    int page_num = rid.pageNum,slot_num=rid.slotNum;

    fileHandle.readPage((unsigned)page_num, page_buffer);

    HFPage hfPage(page_buffer);
    //short k=hfPage.getSlotCnt();
    if(slot_num>hfPage.getSlotCnt())
        return -1;
    if(page_num<0)
            return -1;
    short recordlen=hfPage.getLength(slot_num);
    short recordbegin=hfPage.getOffset(slot_num);
    short start;
    if(recordlen>0)
    {
        start=recordbegin+recordlen;

    }
    if(recordlen==-2)
    {
        recordlen=sizeof(unsigned int)+sizeof(short)+sizeof(short);
        start=recordbegin+sizeof(unsigned int)+sizeof(short)+sizeof(short);


        int addpage_num;
        short addslot,addlen,addoffset;
        memcpy(&addpage_num,page_buffer+recordbegin,sizeof(unsigned int));
        memcpy(&addslot,page_buffer+recordbegin+sizeof(unsigned int),sizeof(short));
        memcpy(&addlen,page_buffer+recordbegin+sizeof(unsigned int)+sizeof(short),sizeof(short));

        char addpage_buffer[PAGE_SIZE];
        memset(addpage_buffer, 0, PAGE_SIZE);
        fileHandle.readPage(addpage_num, addpage_buffer);
        HFPage addhfPage(addpage_buffer);

        addoffset=addhfPage.getOffset(addslot);

        short addstart=addoffset+addlen;
        short addend=addhfPage.getFreeOffset();

        short addneedtomovelen=addend-addstart;
        if(addneedtomovelen<0)
            return -1;
        char addmove[addneedtomovelen];
        memcpy(addmove,addpage_buffer+addstart,addneedtomovelen);
        memcpy(addpage_buffer+addoffset,addmove,addneedtomovelen);
        short i;
        short addSlotCnt=addhfPage.getSlotCnt();
        for(i=1;i<=addSlotCnt;i++)
        {

            short addoff=addhfPage.getOffset(i);

            if(addoff>=addstart)
            {
                short addnewoffset=addoff-addlen;
                addhfPage.setOffset(i,addnewoffset);
            }
        }
        addhfPage.setFreeOffset(addhfPage.getFreeOffset()-addlen);

        short a=-1,b=0;
        addhfPage.setOffset(addslot, a);
        addhfPage.setLength(addslot, b);

        fileHandle.writePage((unsigned)addpage_num, addpage_buffer);

    }

    if(slot_num<=hfPage.getSlotCnt())
      {
                    //start=hfPage.getOffset(slot_num+1);

                    short end=hfPage.getFreeOffset();
                    short needtomovelen=end-start;

                    char move[needtomovelen];
                    memcpy(move,page_buffer+start,needtomovelen);
                    memcpy(page_buffer+recordbegin,move,needtomovelen);

    }


    short a=-1,b=0;
    hfPage.setOffset(slot_num, a);
    hfPage.setLength(slot_num, b);
    short SlotCnt=hfPage.getSlotCnt();
    short i;
    for(i=1;i<=SlotCnt;i++)
    {

        short off=hfPage.getOffset(i);

        if(off>=start)
        {
            short newoffset=off-recordlen;

            hfPage.setOffset(i,newoffset);
        }
    }
    hfPage.setFreeOffset(hfPage.getFreeOffset()-recordlen);
    fileHandle.writePage((unsigned)page_num, page_buffer);

    return 0;
}


RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const void *data, const RID &rid)
{
    deleteRecord(fileHandle, recordDescriptor, rid);

    char page_buffer[PAGE_SIZE];
    memset(page_buffer, 0, PAGE_SIZE);

    int page_num = rid.pageNum,slot_num=rid.slotNum;

    fileHandle.readPage((unsigned)page_num, page_buffer);

    HFPage hfPage(page_buffer);

    short newrecord_length = Record::getRecordLength(recordDescriptor, data);
    short free_space=hfPage.free_space_;
    short freeoffset = hfPage.getFreeOffset();
    short recordoffset=freeoffset;
    if(newrecord_length<=free_space)
    {
        freeoffset += newrecord_length;
        hfPage.setFreeOffset(freeoffset);
        hfPage.setOffset(slot_num,recordoffset);
        hfPage.setLength(slot_num,newrecord_length);
        //did not maintain the  free_space_
        hfPage.free_space_=hfPage.free_space_-newrecord_length;
        Record record(page_buffer+recordoffset);
        record.writeRecord(recordDescriptor, data);

        fileHandle.writePage((unsigned)page_num, page_buffer);

        return 0;
    }
    if(newrecord_length>free_space)
    {
        short toolong=-2;
        freeoffset += 8;
        hfPage.setFreeOffset(freeoffset);
        hfPage.setOffset(slot_num,recordoffset);
        hfPage.setLength(slot_num,toolong);
        hfPage.free_space_=hfPage.free_space_-8;
        unsigned int newpage_num = getFreePage(fileHandle, newrecord_length);

        char newpage_buffer[PAGE_SIZE];
        memset(newpage_buffer, 0, PAGE_SIZE);
        fileHandle.readPage((unsigned)newpage_num, newpage_buffer);
        HFPage newhfPage(newpage_buffer);
        short start=newhfPage.getFreeOffset();

        newhfPage.setFreeOffset(start+newrecord_length+4);
        Record record(newpage_buffer+start);
        int slot_count_ = newhfPage.getSlotCnt()+1;
        memcpy(newpage_buffer + PAGE_SIZE - 2 * kTwoBtyesSpace, &slot_count_, kTwoBtyesSpace);


        int flag=0;
            for(short i=1;i<slot_count_;i++)
            {
                short off,len;
                //memcpy(&off,ptr_data_ + PAGE_SIZE - (2 * i + 2) * kTwoBtyesSpace, kTwoBtyesSpace);
                off=newhfPage.getOffset(i);
                //memcpy(&len,ptr_data_ + PAGE_SIZE - (2 * i + 1) * kTwoBtyesSpace, kTwoBtyesSpace);
                len=newhfPage.getLength(i);
                if(off==-1&&len==0)
                {
                    //memcpy(ptr_data_ + PAGE_SIZE - (2 * i + 2) * kTwoBtyesSpace, &offset, kTwoBtyesSpace);
                    newhfPage.setOffset(i,start);
                    //memcpy(ptr_data_ + PAGE_SIZE - (2 * i + 1) * kTwoBtyesSpace, &length, kTwoBtyesSpace);
                    newhfPage.setLength(i,-3);
                    flag=1;
                    slot_num=i;
                    newhfPage.free_space_ -= newrecord_length;
                    break;
                }
            }
            if(flag==0)
            {
                int ll=-3;
                memcpy(newpage_buffer + PAGE_SIZE - (2 * slot_count_ + 2) * kTwoBtyesSpace, &start, kTwoBtyesSpace);
                memcpy(newpage_buffer + PAGE_SIZE - (2 * slot_count_ + 1) * kTwoBtyesSpace, &ll, kTwoBtyesSpace);
                slot_num = slot_count_;
                newhfPage.free_space_ -= (newrecord_length + 2 * kTwoBtyesSpace);
            }


        record.writeRecord(recordDescriptor, data);
        fileHandle.writePage((unsigned)newpage_num, newpage_buffer);

        memcpy(page_buffer+recordoffset,&newpage_num,sizeof(unsigned int));
        memcpy(page_buffer+recordoffset+sizeof(unsigned int),&slot_num,sizeof(short));
        memcpy(page_buffer+recordoffset+sizeof(unsigned int)+sizeof(short),&newrecord_length,sizeof(short));
        fileHandle.writePage((unsigned)page_num, page_buffer);
    }

    return 0;
}
RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid, const std::string &attributeName, void *data) {
    RC rc = 0;
    char page_buffer[PAGE_SIZE];
    memset(page_buffer, 0, PAGE_SIZE);
    // get #page_num page -- O(1)
    fileHandle.readPage(rid.pageNum, page_buffer);
    HFPage hfPage(page_buffer);
    // get #slotNum record -- O(1)
    Record record;
    RID finalRid = rid;
    // check tombstone
    while (hfPage.checkTombstone(finalRid.slotNum)) {
        hfPage.getRID(finalRid.slotNum, finalRid);
        fileHandle.readPage(finalRid.pageNum, page_buffer);
        hfPage.load(page_buffer);
    }
    
    record = hfPage.getRecord(finalRid.slotNum);
    // read record -- O(1)
    char temp_data[50];
    short len = 0; // no use, only get length. but if <0, should have return false;
    if (hfPage.getLength(finalRid.slotNum) > 0 && hfPage.getOffset(finalRid.slotNum) >= 0){
        rc += record.readAttribute(recordDescriptor, attributeName, temp_data, len);
        memset(data, 0, (short)std::ceil((float)recordDescriptor.size() / 8));
        memcpy(data + (short)std::ceil((float)recordDescriptor.size() / 8), temp_data, len);
    }
    else rc += 1;
    return rc;
}


short RecordBasedFileManager::GetSlotCnt(FileHandle &fileHandle, const int page_id) {
    void *page_buffer = malloc(PAGE_SIZE);
    memset(page_buffer, 0, PAGE_SIZE);

    fileHandle.readPage(page_id, page_buffer);
    HFPage hfPage((char*)page_buffer);
    short return_cnt = hfPage.getSlotCnt();
    free(page_buffer);
    return return_cnt;
}

RC RecordBasedFileManager::scan(FileHandle &fileHandle,
    const std::vector<Attribute> &recordDescriptor,
    const std::string &conditionAttribute,
    const CompOp compOp,                  // comparision type such as "<" and "="
    const void *value,                    // used in the comparison
    const std::vector<std::string> &attributeNames, // a list of projected attributes
    RBFM_ScanIterator &rbfm_scanIterator){
	rbfm_scanIterator.init(fileHandle, recordDescriptor, conditionAttribute, compOp, value, attributeNames);
	return 0;
}

////// RBFM_ScanIterator /////

RBFM_ScanIterator::RBFM_ScanIterator()
{
}

RBFM_ScanIterator::~RBFM_ScanIterator()
{
}

RC RBFM_ScanIterator::init(FileHandle &fileHandle,
                        const std::vector<Attribute> &recordDescriptor,
                        const std::string &conditionAttribute,
                        const CompOp compOp,
                        const void *value,
                        const std::vector<std::string> &attributeNames){
    PagedFileManager::instance()->openFile(fileHandle.GetFileName().c_str(), this->file_handle_);
	// fileHandle : have to get slot cnt of every page;

    this->record_descriptor_ = recordDescriptor;
    this->condition_attribute_ = conditionAttribute;
    this->comp_op_ = compOp;
    this->value_ = (void*)value;
    this->attribute_names_ = attributeNames;
    this->cur_rid_.pageNum = 0;
    this->cur_rid_.slotNum = 1;

    if(this->attribute_names_.size() == 0){ // void projected_names_ represent all attrs will be projected
        for(auto attr : this->record_descriptor_){
            this->attribute_names_.push_back(attr.name);
        }
    }

    return 0;
}

bool RBFM_ScanIterator::IsComped(void *data){
    if (this -> comp_op_ == NO_OP)
        return true;

    AttrType comp_attr_type;
    FindAttrType(this->condition_attribute_, comp_attr_type);

    // need to switch types in compare
    // A.override comp function()
    // B.override operateor
    switch (comp_attr_type) {
        // set values, send them into overrided cmp function
        case TypeVarChar:
        {
            int data_str_len, condition_str_len;
            char data_str[50], condition_str[50]; // test_case don't have string more than 30. Easier than dynamic alloc.
            memcpy(&condition_str_len, this->value_, kFourBtyesSpace);// a better way is to store the condition value in iterator since init. But there'll be some issues there.
            //std::cout<<condition_str_len<<std::endl;
            memcpy(&data_str_len, data, kFourBtyesSpace);
            memcpy(condition_str, (char *)this->value_ + kFourBtyesSpace, condition_str_len);
            memcpy(data_str, (char *)data + kFourBtyesSpace, data_str_len);

            data_str[data_str_len] = '\0';
            condition_str[condition_str_len] = '\0';
            std::string d_s = std::string(data_str), c_s = std::string(condition_str);

            return Compare<std::string>(d_s, c_s, this->comp_op_);
            break;
        }
        case TypeInt:
        {
            int d_i, c_i;
            memcpy(&d_i, data, kFourBtyesSpace);
            memcpy(&c_i, this->value_, kFourBtyesSpace);
            return Compare<int>(d_i, c_i, this->comp_op_);
            break;
        }
        case TypeReal:
        {
            float d_f, c_f;
            memcpy(&d_f, data, kFourBtyesSpace);
            memcpy(&c_f, this->value_, kFourBtyesSpace);
            return Compare<float>(d_f, c_f, this->comp_op_);
            break;
        }
        default:
            break;
    }
    return false; //error
}

RC RBFM_ScanIterator::FindAttrType(std::string attr_name, AttrType& attr_type){
    for (auto& attr: this->record_descriptor_){
        if (attr.name == attr_name){
            attr_type = attr.type;
            return 0;
        }
    }
    return -1; // name not exist in descriptor;
}

RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data) {
    RecordBasedFileManager *rbf_manager = RecordBasedFileManager::instance();

    char page_buffer [PAGE_SIZE];
    memset(page_buffer, 0, PAGE_SIZE);

    unsigned page_num = this->file_handle_.getNumberOfPages();

    // for every page:
    for (; this->cur_rid_.pageNum < page_num; this->cur_rid_.pageNum++) {
        this->file_handle_.readPage(this->cur_rid_.pageNum, page_buffer);
        HFPage hf_page(page_buffer);

        while (this->cur_rid_.slotNum <= hf_page.getSlotCnt()){
            short record_len = hf_page.getLength((unsigned)this->cur_rid_.slotNum);
            if (record_len > 0 || record_len == -2) { // recursive tombstone not resolved
                // slot is existant.
                Record record;
                if (record_len == -2){ // slot is redirecdted to another 
                    short temp_offset = hf_page.getOffset((unsigned)this->cur_rid_.slotNum);

                    unsigned new_page_num;
                    short new_slot_num;
                    short new_slot_len;

                    memcpy(&new_page_num, page_buffer + temp_offset, sizeof(unsigned int));
                    memcpy(&new_slot_num, page_buffer + temp_offset + sizeof(unsigned int), sizeof(short));
                    memcpy(&new_slot_len, page_buffer + temp_offset + sizeof(unsigned int) + sizeof(short), sizeof(short));


                    char new_page_buffer[PAGE_SIZE];
                    memset(new_page_buffer, 0, PAGE_SIZE);
                    this->file_handle_.readPage(new_page_num, new_page_buffer);
                    HFPage new_hf_page(new_page_buffer);
                    record = new_hf_page.getRecord(new_slot_num);
                }
                else {
                    record = hf_page.getRecord(this->cur_rid_.slotNum);
                }

                char comp_value[50];
                short comp_attr_len = 0;

                // if  condition_attribute_ not existed or if no condition
                record.readAttribute(this->record_descriptor_, this->condition_attribute_, comp_value, comp_attr_len); //, comp_attr_len
                if (IsComped(comp_value)) {

                    bool *null_bools = new bool[this->attribute_names_.size()];
                    memset(null_bools, false, this->attribute_names_.size());

                    char eight_null_bits = 0;
                    std::vector<char> null_bits_vector;
                    rid = this->cur_rid_;
                    
                    // packing projected data;
                    unsigned offset = 0;
                    unsigned nullbits_len = (short)std::ceil((float)this->attribute_names_.size()/8);
                    offset += nullbits_len;
                    for (auto& attr_iter : this->attribute_names_){

                        short attr_len = 0;
                        // write an attr into data
                        /*cout<<this->record_descriptor_<<endl;
                        cout<<this->attr_iter<<endl;
                        cout<<this->data + offset<<endl;
                        attr_len*/
                        if (record.readAttribute(this->record_descriptor_, attr_iter, data + offset, attr_len) == -1) // , attr_len
                            std::cout << "illegel attr name" << std::endl;
                        if (attr_len == 0){
                            null_bools[&attr_iter - &(this->attribute_names_[0])] = true;
                            eight_null_bits ++;
                        }

                        if ((&attr_iter - &this->attribute_names_[0] + 1) % 8 == 0){
                            null_bits_vector.push_back(eight_null_bits);
                            eight_null_bits = 0;
                        }
                        else{
                            eight_null_bits *= 2;
                        }

                        offset += attr_len;
                    }
                    //std::cout << "nullbits" << (short)eight_null_bits;
                    null_bits_vector.push_back(eight_null_bits);
                    // writing nullbits: //nullbits_len = null_bits_vector.size() * sizeof(char)
                    memcpy(data, &null_bits_vector[0], nullbits_len);
                    this->cur_rid_.slotNum++;
                    delete[] null_bools;
                    return 0;
                }
            }
            this->cur_rid_.slotNum++;
        }
        this->cur_rid_.slotNum = 1;
    }
    return RBFM_EOF;  // end of scan, return -1;
}

RC RBFM_ScanIterator::close()
{
	RecordBasedFileManager::instance()->closeFile(file_handle_);
	return 0;
}

// -----------------------

// -----------------------
HFPage::HFPage(char *data){
    ptr_data_ = data;
    if (data != NULL){
        memcpy(&free_offset_, ptr_data_ + PAGE_SIZE - kTwoBtyesSpace, kTwoBtyesSpace);
        memcpy(&slot_count_, ptr_data_ + PAGE_SIZE - 2 * kTwoBtyesSpace, kTwoBtyesSpace);
    }else{
        free_offset_ = 0;
        slot_count_ = 0;
    }
    free_space_ = PAGE_SIZE - free_offset_ - 2 * (slot_count_ + 1) * kTwoBtyesSpace;
}

HFPage::~HFPage(){
    //free(ptr_data_);
}

void HFPage::load(char *data = NULL){
    ptr_data_ = data;
    if (data != NULL){
        memcpy(&free_offset_, ptr_data_ + PAGE_SIZE - kTwoBtyesSpace, kTwoBtyesSpace);
        memcpy(&slot_count_, ptr_data_ + PAGE_SIZE - 2 * kTwoBtyesSpace, kTwoBtyesSpace);
    }else{
        free_offset_ = 0;
        slot_count_ = 0;
    }
    free_space_ = PAGE_SIZE - free_offset_ - 2 * (slot_count_ + 1) * kTwoBtyesSpace;
}

Record HFPage::getRecord(unsigned i){
    // get offset
    short offset = 0;
    memcpy(&offset, ptr_data_ + PAGE_SIZE - (2 * i + 2) * kTwoBtyesSpace, kTwoBtyesSpace);
    // should always check offset >= 0
    // assert(offset >= 0);
    return Record(ptr_data_ + offset);
}

Record HFPage::setRecord(const short length, short& slot_num){
    if (free_space_ < (length + 2 * kTwoBtyesSpace)){
        //std::cout<<" not enough space in HFPage "<< free_space_ <<" "<< length << std::endl;
        return Record();
    }
    // enough space to append record, update slot_count_
    short offset = free_offset_;
    slot_count_ += 1;
    memcpy(ptr_data_ + PAGE_SIZE - 2 * kTwoBtyesSpace, &slot_count_, kTwoBtyesSpace);
    //slot_num = slot_count_;
    // write slot directory
    int flag=0;
    for(short i=1;i<getSlotCnt();i++)
    {
        short off,len;
        //memcpy(&off,ptr_data_ + PAGE_SIZE - (2 * i + 2) * kTwoBtyesSpace, kTwoBtyesSpace);
        off=getOffset(i);
        //memcpy(&len,ptr_data_ + PAGE_SIZE - (2 * i + 1) * kTwoBtyesSpace, kTwoBtyesSpace);
        len=getLength(i);
        if(off==-1&&len==0)
        {
            //memcpy(ptr_data_ + PAGE_SIZE - (2 * i + 2) * kTwoBtyesSpace, &offset, kTwoBtyesSpace);
            setOffset(i,offset);
            //memcpy(ptr_data_ + PAGE_SIZE - (2 * i + 1) * kTwoBtyesSpace, &length, kTwoBtyesSpace);
            setLength(i,length);
            flag=1;
            slot_num=i;
            free_space_ -= length;
            break;
        }
    }
    if(flag==0)
    {
        memcpy(ptr_data_ + PAGE_SIZE - (2 * slot_count_ + 2) * kTwoBtyesSpace, &offset, kTwoBtyesSpace);
        memcpy(ptr_data_ + PAGE_SIZE - (2 * slot_count_ + 1) * kTwoBtyesSpace, &length, kTwoBtyesSpace);
        slot_num = slot_count_;
        free_space_ -= (length + 2 * kTwoBtyesSpace);
    }



    // update free_offset_
    free_offset_ += length;
    memcpy(ptr_data_ + PAGE_SIZE - kTwoBtyesSpace, &free_offset_, kTwoBtyesSpace);
    // update free_space_
    //free_space_ -= (length + 2 * kTwoBtyesSpace);
    //std::cout << "slot : "<<slot_num<<" free_space_ "<<free_space_<<" ";
    // return record
    return Record(ptr_data_ + offset);
}
RC HFPage::setRID(const unsigned slotID, const RID rid){
    setOffset(slotID, -1 * rid.pageNum * PAGE_SIZE - 1 * rid.slotNum -1);
    return 0;
}

RC HFPage::getRID(const unsigned slotID, RID &rid){
    rid.pageNum = (-1 * getOffset(slotID) -1) / PAGE_SIZE;
    rid.slotNum = (-1 * getOffset(slotID) - 1) % PAGE_SIZE;
    return 0;
}

short HFPage::getLength(const unsigned slotID){
    short length = 0;
    memcpy(&length, ptr_data_ + PAGE_SIZE - (2 * slotID + 1) * kTwoBtyesSpace, kTwoBtyesSpace);
    return length;
}

void HFPage::setLength(const unsigned slotID, const short length){
    memcpy(ptr_data_ + PAGE_SIZE - (2 * slotID + 1) * kTwoBtyesSpace, &length, kTwoBtyesSpace);
}

short HFPage::getOffset(const unsigned slotID){
    short offset = 0;
    if ((short)slotID > getSlotCnt())
        return -2;
    else {
        memcpy(&offset, ptr_data_ + PAGE_SIZE - (2 * slotID + 2) * kTwoBtyesSpace, kTwoBtyesSpace);
        return offset;
    }
}

void HFPage::setOffset(const unsigned slotID, const short offset){
    memcpy(ptr_data_ + PAGE_SIZE - (2 * slotID + 2) * kTwoBtyesSpace, &offset, kTwoBtyesSpace);
}

short HFPage::getSlotCnt(){
    memcpy(&slot_count_, ptr_data_ + PAGE_SIZE - 2 * kTwoBtyesSpace, kTwoBtyesSpace);
    return slot_count_;
}

void HFPage::setSlotCnt(const short slot_count_){
    memcpy(ptr_data_ + PAGE_SIZE - 2 * kTwoBtyesSpace, &slot_count_, kTwoBtyesSpace);
}

short HFPage::getFreeOffset(){
    memcpy(&free_offset_, ptr_data_ + PAGE_SIZE - kTwoBtyesSpace, kTwoBtyesSpace);
    return free_offset_;
}

void HFPage::setFreeOffset(const short free_offset_){
    memcpy(ptr_data_ + PAGE_SIZE - kTwoBtyesSpace, &free_offset_, kTwoBtyesSpace);
}

bool HFPage::checkTombstone(const unsigned slotID){
    short offset = getOffset(slotID);
    //int length = getLength(slotID);
    if (offset < -1) return 1;
    else return 0;
}

/// -----------------------

/// -----------------------

Record::Record(char *data): ptr_data_(data){
}

Record::~Record(){
//    free(ptr_data_);
}

RC Record::writeRecord(const std::vector<Attribute> &recordDescriptor, const void *data){
    size_t attribute_num = recordDescriptor.size();
    short attribute_offset = (short)(attribute_num + 1) * kTwoBtyesSpace;
    short record_offset = 0;
    short offset = attribute_offset;
    // no need to call getcontentlength. can be computed using all the offset.
    // short record_length = getRecordContentLength(recordDescriptor, data);
    // memcpy(ptr_data_, &record_length, kTwoBtyesSpace);

    memcpy(ptr_data_ + offset, (char *)data, (short)std::ceil((float)recordDescriptor.size()/8));
    // byte temp;
    // memcpy(&temp, ptr_data_ + offset, (unsigned)std::ceil((float)recordDescriptor.size()/8));

    char* nullbits = (char*)malloc( 1 * (short)std::ceil((float)recordDescriptor.size() ) );
    memcpy(nullbits, (char *)ptr_data_ + offset, (short)std::ceil((float)recordDescriptor.size() / 8.0));

    std::vector<bool> bool_arr = CheckNullbits(recordDescriptor.size(), nullbits);

    offset += (short)std::ceil((float)recordDescriptor.size()/8);
    record_offset += (short)std::ceil((float)recordDescriptor.size()/8);

    //memcpy(ptr_data_, &attribute_num, sizeof(int));
    //memcpy(ptr_data_ + offset, data, length);
    for (size_t i = 0; i < attribute_num; i++) {
        memcpy(ptr_data_ + i * kTwoBtyesSpace, &offset, kTwoBtyesSpace);
        if (!bool_arr[i]) {
            if (recordDescriptor[i].type == TypeVarChar){
                unsigned var_char_len = 0;
                memcpy(&var_char_len, (char *)data + record_offset, kFourBtyesSpace);
                memcpy(ptr_data_ + offset, (char *)data + record_offset, kFourBtyesSpace + var_char_len);
                record_offset += kFourBtyesSpace + var_char_len;
                offset += kFourBtyesSpace + var_char_len;
            }else{
                memcpy(ptr_data_ + offset, (char *)data + record_offset, recordDescriptor[i].length);
                record_offset += recordDescriptor[i].length;
                offset += recordDescriptor[i].length;
            }
        }
    }
    memcpy(ptr_data_ + attribute_num * kTwoBtyesSpace, &offset, kTwoBtyesSpace);

    free(nullbits);
    return 0;
}


RC Record::readRecord(const std::vector<Attribute> &recordDescriptor, void *data){
    size_t attribute_num = recordDescriptor.size();

    short record_length = 0;
    memcpy(&record_length, ptr_data_ + attribute_num * kTwoBtyesSpace, kTwoBtyesSpace);
    short attribute_offset = (short)(attribute_num + 1) * kTwoBtyesSpace;
    // if(record_length == 0)
    // 	return -1;
    // if(record_length < attribute_offset){
    // 	std::cout<<"ERRRORRRRR Here" << std::endl;
    // 	assert(record_length > 0);
    // }
    short data_length = record_length - attribute_offset;

    memcpy(data, ptr_data_ + attribute_offset, data_length);

    return 0;
}

RC Record::readAttribute(const std::vector<Attribute> &recordDescriptor, const std::string attribute_name ,void *data, short& attr_len) { //,
    size_t attribute_num = recordDescriptor.size();

    int attr_index = -1;
    for (size_t i = 0; i < attribute_num; i++)
    {
    	//std::cout<<recordDescriptor[i].name<<std::endl;
    	//std::cout<<attribute_name<<std::endl;
        if(recordDescriptor[i].name == attribute_name) {
            attr_index = (int)i;
        }
    }
    if (attr_index < 0){
        // attr_name not exist in descriptor
        return -1;
    }

    short attr_begin = 0;
    short attr_end = 0;
//    memcpy(&attr_begin, ptr_data_, kTwoBtyesSpace);
//    memcpy(&attr_begin, ptr_data_ + , kTwoBtyesSpace);
//    memcpy(&attr_begin, ptr_data_ + attr_index* kTwoBtyesSpace, kTwoBtyesSpace);
    memcpy(&attr_begin, ptr_data_ + attr_index * kTwoBtyesSpace, kTwoBtyesSpace);
    // ERROR
    memcpy(&attr_end, ptr_data_ + (attr_index + 1) * kTwoBtyesSpace, kTwoBtyesSpace);

    attr_len = attr_end - attr_begin;

    if (attr_end <= attr_begin){
        if(attr_end < attr_begin){
            return -1;
        }
        else {
            data = NULL;
        }
    }
    else {
        memcpy(data, ptr_data_ + attr_begin, attr_end - attr_begin);
    }

    return 0;
}

// void Record::setRecordLength(const short length) {
//     memcpy(ptr_data_, &length, kTwoBtyesSpace);
//     return;
// }

// short Record::getRecordLength() {
//     short length = 0;
//     memcpy(&length, ptr_data_, kTwoBtyesSpace);
//     return length;
// }

short Record::getRecordLength(const std::vector<Attribute> &recordDescriptor, const void *data){
    short len = Record::getRecordContentLength(recordDescriptor, data) + (short)(recordDescriptor.size() + 1) * kTwoBtyesSpace;
    return len;
}

short Record::getRecordContentLength(const std::vector<Attribute> &recordDescriptor, const void *data){
    short len = 0;
    char* nullbits = (char*)malloc( 1 * (short)std::ceil((float)recordDescriptor.size() ) );
    memcpy(nullbits, (char*)data + len, (short)std::ceil((float)recordDescriptor.size() / 8.0));

    std::vector<bool> bool_arr = CheckNullbits(recordDescriptor.size(), nullbits);

    len += (short)std::ceil((float)recordDescriptor.size() / 8.0);
    for (size_t i = 0; i < recordDescriptor.size(); i++){
        if (!bool_arr[i]) {

            if (recordDescriptor[i].type == TypeVarChar){
                unsigned var_char_len = 0;
                memcpy(&var_char_len, (char *)data + len, kFourBtyesSpace);
                len += (kFourBtyesSpace + (short)var_char_len);
            }else
                len += kFourBtyesSpace;//recordDescriptor[i].length;
        }
    }
    free(nullbits);
    return len;
}


std::vector<bool> Record::CheckNullbits(int num, const char* nullbits){
    // char* nullbits;
    int len = std::ceil((float)num / 8.0);
    std::vector<bool> bool_arr;

    // char -> bool
    for (int i = 0; i < len; i ++)
    {
        for (int j = 0; j < 8; j ++)
        {
            bool_arr.push_back((nullbits[i] & (1 << j)) != 0);
        }
    }
    return bool_arr;

}
