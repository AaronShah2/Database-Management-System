#include "rm.h"

#include <algorithm>
#include <cstring>
#include <cassert>
#include <iostream>

char const* kRMCatalogName = "Tables";
char const* kRMColumnsName = "Columns";

char const* kCatalogAttrNames[] = {"table-id", "table-name", "file-name"};
const AttrType kCatalogAttrTypes[3] = {TypeInt, TypeVarChar, TypeVarChar};
const unsigned kCatalogAttrLens[3] = {4, 50, 50};

char const* kColumnAttrNames[] = {"table-id", "column-name", "column-type", "column-length", "column-position","if_index"};
const AttrType kColumnAttrTypes[6] = {TypeInt, TypeVarChar, TypeInt, TypeInt, TypeInt, TypeInt};
const unsigned kColumnAttrLens[6] = {4, 50, 4, 4, 4, 4};

RelationManager* RelationManager::instance() {
	static RelationManager _rm;
	return &_rm;
}

/*Important 0*/
RelationManager::RelationManager()
{
	rbf_manager_ = RecordBasedFileManager::instance();
    next_new_tbid_ = 1; // ?
    //only first time run db would call this, so no need to RetriveMetaInfo
    if (-1 == rbf_manager_->openFile(kRMCatalogName, file_handle_catalog_)){
        this -> createCatalog();
    }
    else{
        rbf_manager_->openFile(kRMColumnsName, file_handle_attr_);
        RetrieveCatalogInfo(); //?s
    }
}

RelationManager::~RelationManager()
{
}

// Important 2 //
RC RelationManager::createCatalog()
{
    rbf_manager_->createFile(kRMCatalogName);
    rbf_manager_->createFile(kRMColumnsName);
    
    rbf_manager_->openFile(kRMCatalogName, file_handle_catalog_);
    rbf_manager_->openFile(kRMColumnsName, file_handle_attr_);
    
    createTable(kRMCatalogName, GenerateCatalogDescriptor());
    createTable(kRMColumnsName, GenerateColumnsDescriptor());
#ifdef IAN_DEBUG
    for (auto &desp : this -> table_id_to_descriptor_){
        for(auto &attr : desp.second)
            std::cout << attr.name << std::endl;
    }
    std::cout << "CREATE" << std::endl; 
#endif

    return 0;
}

// retrieve 
// table_name_to_id_
// table_name_to_descriptor_
// into maps
// update next_next_new_tbid_
// scan every 
void RelationManager::RetrieveCatalogInfo() {
	RID rid_iter;
    char data_iter[PAGE_SIZE]; // reuse this memory in next 2 phase

    // retrieve name_to_id table // we hope it's one-on-one
    RM_ScanIterator rm_catalog_tb_iter;
    std::vector<std::string> catalog_project_attrs = {kCatalogAttrNames[1], kCatalogAttrNames[0]}; // table-name, table-id
    this -> scan(kRMCatalogName, "", NO_OP, NULL, catalog_project_attrs, rm_catalog_tb_iter);

    while (rm_catalog_tb_iter.getNextTuple(rid_iter, data_iter) != -1) {
        // get attr value from data stream // dirty code
        short offset = 0;
        char nullbits[1]; //only 2bits, that is 1 bytes
        memcpy(nullbits, data_iter, 1);
        offset += 1;
        std::vector<bool> bool_arr = Record::CheckNullbits(2, nullbits);
        assert(bool_arr[0] == 0 && bool_arr[1] == 0);

        short str_length;
        std::string table_name = ReadStringValue(data_iter + offset, str_length);
        offset += str_length;

        int table_id;
        memcpy(&table_id, data_iter + offset, kFourBtyesSpace);
        offset += kFourBtyesSpace;

        table_name_to_id_[table_name] = table_id;
        
        next_new_tbid_ = std::max(next_new_tbid_, (unsigned)table_id + 1);
    }

    rm_catalog_tb_iter.close();

    // retireve id_to_descriptor_table
    RM_ScanIterator rm_column_tb_iter;
    std::vector<std::string> column_project_attrs = {kColumnAttrNames[0], kColumnAttrNames[1],
                                                    kColumnAttrNames[2], kColumnAttrNames[3]}; // table-id, column-name, column-type, column-length
    this -> scan(kRMColumnsName, "", NO_OP, NULL, column_project_attrs, rm_column_tb_iter);

    while (rm_column_tb_iter.getNextTuple(rid_iter, data_iter) != -1) {
        // get attr value from data stream // dirty code
        short offset = 0;
        char nullbits[1]; //only 4 bits, that is 1 bytes
        memcpy(nullbits, data_iter, 1);
        offset += 1;
        std::vector<bool> bool_arr = Record::CheckNullbits(4, nullbits);
        assert(bool_arr[0] == 0 && bool_arr[1] == 0 && bool_arr[2] == 0 && bool_arr[3] == 0);

        int table_id;
        memcpy(&table_id, data_iter + offset, kFourBtyesSpace);
        offset += kFourBtyesSpace;

        short str_length;
        std::string column_name = ReadStringValue(data_iter + offset, str_length);
        offset += str_length;

        AttrType column_type;
        memcpy(&column_type, data_iter + offset, kFourBtyesSpace);
        offset += kFourBtyesSpace;

        unsigned column_length;
        memcpy(&column_length, data_iter + offset, kFourBtyesSpace);
        offset += kFourBtyesSpace;

        Attribute table_attr = {column_name, column_type, column_length};
        std::vector<Attribute> table_attr_vector = table_id_to_descriptor_[table_id]; // if no key, will automatically insert;
        table_attr_vector.push_back(table_attr); // assume attrs're stored and read in correct order. didn't implemented based on position attr;
        table_id_to_descriptor_[table_id] = table_attr_vector;
    }

#ifdef IAN_DEBUG
    for (auto &desp : this -> table_id_to_descriptor_){
    	for(auto &attr : desp.second)
    		std::cout << attr.name << std::endl;
    }
#endif
    
    rm_column_tb_iter.close();

    return;

}

std::vector<Attribute> RelationManager::GenerateCatalogDescriptor() {
    std::vector<Attribute> descriptor;
    for (int i = 0; i < 3; i++)
        descriptor.push_back({kCatalogAttrNames[i], kCatalogAttrTypes[i],
                               kCatalogAttrLens[i]});
    return descriptor;
}

std::vector<Attribute> RelationManager::GenerateColumnsDescriptor() {
    std::vector<Attribute> descriptor;
    for (int i = 0; i < 6; i++)
        descriptor.push_back({kColumnAttrNames[i], kColumnAttrTypes[i],
                               kColumnAttrLens[i]});
    return descriptor;    
}

int RelationManager::GenerateCatalogTuple(const std::string& table_name, char *data) {
    // alreay have cached table, now generate system tuple which will be stored into catalog files.
    assert(table_name_to_id_.find(table_name) != table_name_to_id_.end());
    int table_id = table_name_to_id_[table_name];
    std::string file_name = TableFileMapping(table_name); // get file_name according to table name. usually the same string.

    // table_id, table_name, file_name
    short offset = 0;
    char nullbits = 0;
    memcpy(data, &nullbits, 1);
    offset += 1;

    memcpy(data + offset, &table_id, kFourBtyesSpace);
    offset += kFourBtyesSpace;

    int str_len = table_name.length();
    memcpy(data + offset, &str_len, kFourBtyesSpace);
    offset += kFourBtyesSpace;
    memcpy(data + offset, table_name.c_str(), str_len);
    offset += str_len;

    str_len = file_name.length();
    memcpy(data + offset, &str_len, kFourBtyesSpace);
    offset += kFourBtyesSpace;
    memcpy(data + offset, file_name.c_str(), str_len);
    offset += str_len;

    return table_id;
}

int RelationManager::GenerateColumnsTuple(const std::string& table_name, const Attribute& attr, const int position, char *data) {
    assert(table_name_to_id_.find(table_name) != table_name_to_id_.end());
    int table_id = table_name_to_id_[table_name];
    // assert(table_id_to_descriptor_.find(table_id) != table_id_to_descriptor_.end());

    std::string column_name = attr.name;
    int column_type = attr.type;
    int column_length = attr.length;

    // table_id, column-name, column-type, column-length, column-position
    short offset = 0;
    char nullbits = 0;
    memcpy(data, &nullbits, 1); // 5 bits, all zeros
    offset += 1;

    memcpy(data + offset, &table_id, kFourBtyesSpace);
    offset += kFourBtyesSpace;

    int str_len = column_name.length();
    memcpy(data + offset, &str_len, kFourBtyesSpace);
    offset += kFourBtyesSpace;
    memcpy(data + offset, column_name.c_str(), str_len);
    offset += str_len;

    memcpy(data + offset, &column_type, kFourBtyesSpace);
    offset += kFourBtyesSpace;    

    memcpy(data + offset, &column_length, kFourBtyesSpace);
    offset += kFourBtyesSpace;

    memcpy(data + offset, &position, kFourBtyesSpace);
    offset += kFourBtyesSpace;

    int if_index=0;
    memcpy(data + offset, &if_index, kFourBtyesSpace);
    offset += kFourBtyesSpace;

    return table_id;
}

std::string RelationManager::TableFileMapping(const std::string& table_name){
    return table_name;
}

RC RelationManager::deleteCatalog()
{
    file_handle_attr_.closeFile();
    file_handle_catalog_.closeFile();

	// if(deleteTable(kRMCatalogName)!=0) // has restriction in delete system table
	// 	return -1;
	// if(deleteTable(kRMColumnsName)!=0)
	// 	return -1;
    if(rbf_manager_->destroyFile(TableFileMapping(kRMCatalogName))!=0) 
        return -1;
    if(rbf_manager_->destroyFile(TableFileMapping(kRMColumnsName))!=0)
        return -1;

    return 0;
}

/* Important 3 */
RC RelationManager::createTable(const std::string &tableName, const std::vector<Attribute> &attrs)
{
    // already exist:
	if (table_name_to_id_.find(tableName) != table_name_to_id_.end())
		return -1;

//	  unsigned table_id = table_name_to_id_[tableName];
//    if (table_id_to_descriptor_.find(table_id) != table_id_to_descriptor_.end())
//        return -1;
    char data[PAGE_SIZE];
    std::string table_file_name = TableFileMapping(tableName);
    RID rid;
    
    // if create catalog table, file already created in create catalog
    if (tableName != kRMColumnsName && tableName != kRMCatalogName)
    	rbf_manager_->createFile(table_file_name);

    // update cached data
    table_name_to_id_[tableName] = next_new_tbid_++;
    table_id_to_descriptor_[table_name_to_id_[tableName]] = attrs;

    // write tuple of table and column, into catalog files
    GenerateCatalogTuple(tableName, data); // ? need check if right format
    rbf_manager_->insertRecord(file_handle_catalog_, GenerateCatalogDescriptor(), data, rid);

    for (unsigned i = 0; i < attrs.size(); i++) {
        GenerateColumnsTuple(tableName, attrs[i], i + 1, data); // ? need check if right format , such as columnid, postion
        rbf_manager_->insertRecord(file_handle_attr_, GenerateColumnsDescriptor(), data, rid);// ? may use insertTuple. why using insertTuple would fail
    }
    return 0;
}

RC RelationManager::deleteTable(const std::string &tableName)
{
	std::string FileName;
	if(tableName != kRMCatalogName && tableName != kRMColumnsName)
		FileName = tableName;
	else
		return -1;

	if (table_name_to_id_.find(tableName) == table_name_to_id_.end())
		return -1;

	if(rbf_manager_->destroyFile(FileName) != 0)
	{
		return -1;
	}

	//delete column table

	RM_ScanIterator rmsi;
	RID rid;
	int tableID=table_name_to_id_[tableName];
	std::vector<std::string> projectedAttr;
	projectedAttr.push_back("column-name");
	scan(kRMColumnsName, "table-id", EQ_OP, &tableID, projectedAttr, rmsi);
	char deletedata[PAGE_SIZE];
    //FileHandle fileHandle;
    std::vector<Attribute> column_attrs = GenerateColumnsDescriptor();
    if(column_attrs.empty())
    {
        return -1;
    }
    /*if(rbf_manager_->openFile(kRMColumnsName, fileHandle) != 0)
    {
        return -1;
    }*/
    while(rmsi.getNextTuple(rid, deletedata) != RM_EOF)
    {
        if(rbf_manager_->deleteRecord(file_handle_attr_, column_attrs, rid) != 0)
        {
            return -1;
        }
    }
    table_id_to_descriptor_.erase(tableID);
    /*if(rbf_manager_->closeFile(fileHandle) != 0)
    {
        return -1;
    }*/
    rmsi.close();
	projectedAttr.clear();
	//delete table table
    std::vector<Attribute> catalog_attrs = GenerateCatalogDescriptor();
    scan(kRMCatalogName, "table-id", EQ_OP, &tableID, projectedAttr, rmsi);
        if(catalog_attrs.empty())
        {
            return -1;
        }
        /*if(rbf_manager_->openFile(kRMCatalogName, fileHandle) != 0)
        {
            return -1;
        }*/
        while(rmsi.getNextTuple(rid, deletedata) != RM_EOF)
        {
            if(rbf_manager_->deleteRecord(file_handle_catalog_, catalog_attrs, rid) != 0)
            {
                return -1;
            }
        }
        table_name_to_id_.erase(tableName);
        /*if(rbf_manager_->closeFile(fileHandle) != 0)
        {
            return -1;
        }	*/
        rmsi.close();


    return 0;
}

RC RelationManager::getAttributes(const std::string &tableName, std::vector<Attribute> &attrs)
{

    if(tableName == kRMCatalogName)
    {
        attrs = GenerateCatalogDescriptor();
        return 0;
    }

    if(tableName == kRMColumnsName)
    {
        attrs = GenerateColumnsDescriptor();
        return 0;
    }

    if(tableName != kRMCatalogName && tableName != kRMColumnsName)
    {
        int tableID=table_name_to_id_[tableName];
        if (table_id_to_descriptor_.find(tableID) != table_id_to_descriptor_.end())
            attrs = table_id_to_descriptor_[tableID];
        else
            return -1;
    }
    else
        return -1;

    return 0;
}


RC RelationManager::insertTuple(const std::string &tableName, const void *data, RID &rid)
{
    std::string FileName;
	if(tableName!=kRMCatalogName&&tableName != kRMColumnsName)
		FileName=tableName;
	else
		return -1;
	FileHandle fileHandle;
	if(rbf_manager_->openFile(FileName, fileHandle) != 0)
	{
	    return -1;
	}
	std::vector<Attribute> attrDescriptor;
    if(getAttributes(tableName, attrDescriptor) != 0)
    {
        return -1;
    }
    if(rbf_manager_->insertRecord(fileHandle, attrDescriptor, data, rid) != 0)
    {
        return -1;
    }
    if(rbf_manager_->closeFile(fileHandle) != 0)
    {
        return -1;
    }
    return 0;
}

RC RelationManager::deleteTuple(const std::string &tableName, const RID &rid)
{
    std::string FileName;
	if(tableName!=kRMCatalogName&&tableName != kRMColumnsName)
		FileName=tableName;
	else
		return -1;
	FileHandle fileHandle;
	if(rbf_manager_->openFile(FileName, fileHandle) != 0)
	{
	    return -1;
	}
	std::vector<Attribute> attrDescriptor;
    if(getAttributes(tableName, attrDescriptor) != 0)
    {
        return -1;
    }
    if(rbf_manager_->deleteRecord(fileHandle, attrDescriptor, rid) != 0)
    {
        return -1;
    }
    if(rbf_manager_->closeFile(fileHandle) != 0)
    {
        return -1;
    }
    return 0;
}

RC RelationManager::updateTuple(const std::string &tableName, const void *data, const RID &rid)
{
	std::string FileName;
	if(tableName!=kRMCatalogName&&tableName!=kRMColumnsName)
		FileName=tableName;
	else
		return -1;
	FileHandle fileHandle;
	if(rbf_manager_->openFile(FileName, fileHandle) != 0)
	{
	    return -1;
	}
	std::vector<Attribute> attrDescriptor;
    if(getAttributes(tableName, attrDescriptor) != 0)
    {
        return -1;
    }
    if(rbf_manager_->updateRecord(fileHandle, attrDescriptor, data, rid) != 0)
    {
        return -1;
    }
    if(rbf_manager_->closeFile(fileHandle) != 0)
    {
        return -1;
    }
    return 0;
}

RC RelationManager::readTuple(const std::string &tableName, const RID &rid, void *data)
{
	std::string FileName;
	if(tableName!=kRMCatalogName&&tableName!=kRMColumnsName)
		FileName=tableName;
	FileHandle fileHandle;
	if(rbf_manager_->openFile(FileName, fileHandle) != 0)
	{
	    return -1;
	}
	std::vector<Attribute> attrDescriptor;
    if(getAttributes(tableName, attrDescriptor) != 0)
    {
        return -1;
    }
    if(rbf_manager_->readRecord(fileHandle, attrDescriptor, rid, data) != 0)
    {
        return -1;
    }
    if(rbf_manager_->closeFile(fileHandle) != 0)
    {
        return -1;
    }
    return 0;

}

RC RelationManager::printTuple(const std::vector<Attribute> &attrs, const void *data)
{
    if(rbf_manager_->printRecord(attrs, data) != 0)
    {
        return -1;
    }
    return 0;
}

RC RelationManager::readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName, void *data)
{
	std::string FileName;
	//if(tableName!=kRMCatalogName&&tableName!=kRMColumnsName)
	FileName=tableName;
	FileHandle fileHandle;
	//std::cout<<tableName<<std::endl;
	//std::cout<<FileName<<std::endl;
	if(rbf_manager_->openFile(FileName, fileHandle) != 0)
	{
	    return -1;
	}
	std::vector<Attribute> attrDescriptor;
    if(getAttributes(tableName, attrDescriptor) != 0)
    {
        return -1;
    }
    if(rbf_manager_->readAttribute(fileHandle, attrDescriptor, rid, attributeName, data) != 0)
    {
        return -1;
    }
    if(rbf_manager_->closeFile(fileHandle) != 0)
    {
        return -1;
    }
    
    return 0;
}

RC RelationManager::scan(const std::string &tableName,
      const std::string &conditionAttribute,
      const CompOp compOp,
      const void *value,
      const std::vector<std::string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{
	std::string FileName=tableName;;

    FileHandle fileHandle;
    if(rbf_manager_->openFile(FileName, fileHandle) != 0)
    {
        return -1;
    }
    std::vector<Attribute> attr;
    if(tableName==kRMCatalogName)
    {
    	attr=GenerateCatalogDescriptor();
    }
    else if(tableName==kRMColumnsName)
    {
    	attr=GenerateColumnsDescriptor();
    }
    else if(getAttributes(tableName, attr) != 0)
    {
        return -1;
    }
    // RBFM_ScanIterator rbfmtmp ; //= new RBFM_ScanIterator();
    if(rbf_manager_->scan(fileHandle, attr, conditionAttribute, compOp, value, attributeNames, rm_ScanIterator.rbfm_scan_iterator_) != 0)
    {
        return -1;
    }

    // rm_ScanIterator.init(rbfmtmp);

    return 0;
}

RM_ScanIterator::RM_ScanIterator(){

}
RM_ScanIterator::~RM_ScanIterator(){

}

RC RM_ScanIterator::getNextTuple(RID &rid, void *data)
{
    if(rbfm_scan_iterator_.getNextRecord(rid, data) != RBFM_EOF)
    {
        return 0;
    } else
    {
        return RM_EOF;
    }
}

RC RM_ScanIterator::close()
{
    return rbfm_scan_iterator_.close();
        //delete rbfm_scan_iterator_;
}

//RC RM_ScanIterator::init(const RBFM_ScanIterator& rbfmtmp)
//{
//	this->rbfm_scan_iterator_ = rbfmtmp;
//	return 0;
//}

// Extra credit work
RC RelationManager::dropAttribute(const std::string &tableName, const std::string &attributeName)
{
    return -1;
}

// Extra credit work
RC RelationManager::addAttribute(const std::string &tableName, const Attribute &attr)
{
    return -1;
}

std::string ReadStringValue(const char* data, short& str_length) {
    int len;
    memcpy(&len, data, kFourBtyesSpace);

    char c_str[60] = {'\0'};
    memcpy(c_str, data + kFourBtyesSpace, len);

    str_length = kFourBtyesSpace + (short)len;

    return std::string(c_str);
}


RC RelationManager::createIndex(const std::string &tableName, const std::string &attributeName)
{
	if(changecolumnIndex(tableName,attributeName,1)!=0)
		return -1;

	std::string IndexFileName;
	GetIndexFileName(tableName,attributeName,IndexFileName);

	RC rc = 0;

	    rc = _pf_manager->createFile(IndexFileName);
	    IXFileHandle ixfileHandle;
	    char page_buffer[PAGE_SIZE];
	    rc += ixfileHandle.openFile(IndexFileName);
	    memset(page_buffer, 0, PAGE_SIZE);
	    rc += ixfileHandle.file_handle_.appendPage(page_buffer);
	    rc += _pf_manager->closeFile(ixfileHandle.file_handle_);
	    if(rc!=0)
	    	return -1;

	//if(ix_manager_->createFile(IndexFileName)!=0)
		//return -1;

	std::vector<Attribute> attr;
	if(getAttributes(tableName, attr) != 0)
	{
		return -1;
	}



	return 0;
}


RC RelationManager::changecolumnIndex( const std::string &tableName, const std::string &attributeName,int index)
{

		RM_ScanIterator scaniterator;
		std::vector<std::string> attrName;
	    attrName.push_back("table-id");
	    //attrName.push_back("table-name");
	    //attrName.push_back("column-name");

	    int len=tableName.size();
	    char value[len+4];
	    memcpy(value,&len,sizeof(int));
	    memcpy(value+sizeof(int), tableName.c_str(),len);

		if(scan(kRMCatalogName, "table-name", EQ_OP, &value, attrName, scaniterator)!=0)
			return -1;

		IXFileHandle ixfileHandle1;
		if(ix_manager_->openFile(kRMCatalogName,ixfileHandle1)!=0)
				return -1;
		RID rid;
	    char data[PAGE_SIZE]; // reuse this memory in next 2 phase
	    int tableid;
		while(scaniterator.getNextTuple(rid, data) != RM_EOF)
		{

		     /*std::string attributeTofind="table-id";
		     if(readAttribute(kRMCatalogName, rid, attributeTofind, tableid)!=0)
		    	 return -1;*/
			 tableid = *(int *)((char *)data+1);
			 //std::cout << tableid <<std::endl;
		     break;
		}

	RM_ScanIterator rmsi;
	std::vector<std::string> attrNames;
    attrNames.push_back("table-id");
    //attrNames.push_back("column-name");
    //attrNames.push_back("column-type");
    //attrNames.push_back("column-length");
    //attrNames.push_back("column-position");
    //attrNames.push_back("if_index");

    int attlen=attributeName.size();
    char attvalue[len+4];
    memcpy(attvalue,&attlen,sizeof(int));
    memcpy(attvalue+sizeof(int),attributeName.c_str(),attlen);

	if(scan(kRMColumnsName, "column-name", EQ_OP, &attvalue, attrNames, rmsi)!=0)
		return -1;

	IXFileHandle ixfileHandle;
	if(ix_manager_->openFile(kRMColumnsName,ixfileHandle)!=0)
			return -1;
	RID rid_iter;
    char data_iter[PAGE_SIZE];
    std::vector<Attribute> recordDescriptor_iter=GenerateColumnsDescriptor();
    int returnid;
    int indexnow;
    char recorddata[PAGE_SIZE];
	while(rmsi.getNextTuple(rid_iter, data_iter) != RM_EOF)
	{

		returnid = *(int *)((char *)data_iter+1);

		//std::cout << returnid <<std::endl;

		if(returnid==tableid)
		{

		 Record record;

		 char page_buffer[PAGE_SIZE];
		 ixfileHandle.file_handle_.readPage(rid.pageNum, page_buffer);
		 HFPage hfPage(page_buffer);

		 record = hfPage.getRecord(rid_iter.slotNum);

		 record.readRecord(recordDescriptor_iter, recorddata);
	     //int len=record.getRecordLength(recordDescriptor_iter, record);
	     //std::cout << len <<std::endl;


	     void *returnedData = malloc(10);
	     std::string attributeTofind="if_index";

	     //std::cout << kRMColumnsName <<std::endl;

	     if(readAttribute(kRMColumnsName, rid_iter, attributeTofind, returnedData)!=0)
	    	 return -1;
	     indexnow=*(int *)((char *)returnedData+1);

	     //std::cout<<indexnow<<std::endl;

	     free(returnedData);
	     break;
		}

	}
	     if(index==1)
	     {
	    	 if(indexnow==0)
	    	 {
	    	 	 memcpy(recorddata+len-4,&index,sizeof(int));
	    	 	 updateTuple(kRMColumnsName,recorddata, rid_iter);
	    	  }
	    	 else
	    	 {

	    		 return -1;
	    	 }

	     }
	     if(index==0)
	     {
	     	 if(indexnow==1)
	     	 {
	     	    memcpy(recorddata+len-4,&index,sizeof(int));

	     	    updateTuple(kRMColumnsName,recorddata, rid_iter);
	     	 }
	     	 else
	     	    return -1;
	    }

	return 0;
}

RC RelationManager::destroyIndex(const std::string &tableName, const std::string &attributeName)
{
	if(changecolumnIndex(tableName,attributeName,1)!=0)
		return -1;

	std::string IndexFileName;
	GetIndexFileName(tableName,attributeName,IndexFileName);

	if(_pf_manager->destroyFile(IndexFileName)!=0)
		return -1;



	return 0;
}

RC RelationManager::GetIndexFileName(const std::string &tableName,const std::string &attributeName, std::string &IndexFileName)
{
	if(tableName==""||attributeName=="")
		return -1;
	else if(tableName==kRMCatalogName||tableName==kRMColumnsName)
			return -1;
	else
	{
		IndexFileName=tableName+"-"+attributeName;
	}
	return 0;
}


RC RelationManager::indexScan(const std::string &tableName,
                      const std::string &attributeName,
                      const void *lowKey,
                      const void *highKey,
                      bool lowKeyInclusive,
                      bool highKeyInclusive,
                      RM_IndexScanIterator &rm_IndexScanIterator)
{
		std::string IndexFileName;
		GetIndexFileName(tableName,attributeName,IndexFileName);

		IXFileHandle ixfileHandle;
	    if(rbf_manager_->openFile(IndexFileName, ixfileHandle.file_handle_) != 0)
	    {
	        return -1;
	    }

	    std::vector<Attribute> attrs;
	    if(getAttributes(tableName, attrs)!= 0)
	    	return -1;
	    Attribute attr;
	    for (int i=0; i < attrs.size(); ++i)
	    {
	       if (attrs[i].name.compare(attributeName) == 0)
	       {
	            attr = attrs[i];
	       }
	    }


	    if(ix_manager_->scan(ixfileHandle, attr, lowKey, highKey, lowKeyInclusive, highKeyInclusive, rm_IndexScanIterator.ix_scan_iterator_)!= 0)
	    	return -1;

	    return 0;
}

RM_IndexScanIterator::RM_IndexScanIterator() {
}

RM_IndexScanIterator::~RM_IndexScanIterator() {
}

// "key" follows the same format as in IndexManager::insertEntry()
RC RM_IndexScanIterator::getNextEntry(RID &rid, void *key)
{
	 if(ix_scan_iterator_.getNextEntry(rid, key) !=IX_EOF)
	        return 0;
	 else
		 return RM_EOF;
}

RC RM_IndexScanIterator::close() {
	return ix_scan_iterator_.close();
}
