#ifndef _rbfm_h_
#define _rbfm_h_

#include <string>
#include <vector>
#include <climits>
#include <memory>
#include "../rbf/pfm.h"

#define kFreeField 2
#define kSlotCntField 2
#define kSlotLenField 2
#define kSlotOffsetField 2
#define kFourBtyesSpace 4
#define kTwoBtyesSpace 2

//using namespace std;

class Record;
class HFPage;
// Record ID
typedef struct
{
  unsigned pageNum;    // page number
  unsigned slotNum;    // slot number in the page
} RID;

// Attribute
typedef enum { TypeInt = 0, TypeReal, TypeVarChar } AttrType;

typedef unsigned AttrLength;

struct Attribute {
    std::string name;     // attribute name
    AttrType type;     // attribute type
    AttrLength length; // attribute length
};

// Comparison Operator (NOT needed for part 1 of the project)
typedef enum { EQ_OP = 0, // no condition// = 
           LT_OP,      // <
           LE_OP,      // <=
           GT_OP,      // >
           GE_OP,      // >=
           NE_OP,      // !=
           NO_OP       // no condition
} CompOp;


/********************************************************************************
The scan iterator is NOT required to be implemented for the part 1 of the project 
********************************************************************************/

# define RBFM_EOF (-1)  // end of a scan operator

// RBFM_ScanIterator is an iterator to go through records
// The way to use it is like the following:
//  RBFM_ScanIterator rbfmScanIterator;
//  rbfm.open(..., rbfmScanIterator);
//  while (rbfmScanIterator(rid, data) != RBFM_EOF) {
//    process the data;
//  }
//  rbfmScanIterator.close();

class RBFM_ScanIterator {
public:
  RBFM_ScanIterator();

  ~RBFM_ScanIterator();

  // RBFM_ScanIterator& operator=(const RBFM_ScanIterator& rbfm_iter);

  // Never keep the results in the memory. When getNextRecord() is called, 
  // a satisfying record needs to be fetched from the file.
  // "data" follows the same format as RecordBasedFileManager::insertRecord().
  RC getNextRecord(RID &rid, void *data);

  RC close();

  RC init(FileHandle &fileHandle,
          const std::vector<Attribute> &recordDescriptor,
          const std::string &conditionAttribute,
          const CompOp compOp,
          const void *value,
          const std::vector<std::string> &attributeNames);

  bool IsComped(void *data);

  RC FindAttrType(std::string attr_name, AttrType& attr_type);

private:
  FileHandle file_handle_;
  std::vector<Attribute> record_descriptor_;
  std::string condition_attribute_;
  CompOp comp_op_;                  // comparision type such as "<" and "="
  void *value_;                    // used in the comparison
  std::vector<std::string> attribute_names_; // a list of projected attributes
  RID cur_rid_;
};

class RecordBasedFileManager
{
public:
  static RecordBasedFileManager* instance();

  RC createFile(const std::string &fileName);
  
  RC destroyFile(const std::string &fileName);
  
  RC openFile(const std::string &fileName, FileHandle &fileHandle);
  
  RC closeFile(FileHandle &fileHandle);

  //  Format of the data passed into the function is the following:
  //  [n byte-null-indicators for y fields] [actual value for the first field] [actual value for the second field] ...
  //  1) For y fields, there is n-byte-null-indicators in the beginning of each record.
  //     The value n can be calculated as: ceil(y / 8). (e.g., 5 fields => ceil(5 / 8) = 1. 12 fields => ceil(12 / 8) = 2.)
  //     Each bit represents whether each field value is null or not.
  //     If k-th bit from the left is set to 1, k-th field value is null. We do not include anything in the actual data part.
  //     If k-th bit from the left is set to 0, k-th field contains non-null values.
  //     If there are more than 8 fields, then you need to find the corresponding byte first, 
  //     then find a corresponding bit inside that byte.
  //  2) Actual data is a concatenation of values of the attributes.
  //  3) For Int and Real: use 4 bytes to store the value;
  //     For Varchar: use 4 bytes to store the length of characters, then store the actual characters.
  //  !!! The same format is used for updateRecord(), the returned data of readRecord(), and readAttribute().
  // For example, refer to the Q8 of Project 1 wiki page.
  RC insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const void *data, RID &rid);

  RC readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid, void *data);
  
  // This method will be mainly used for debugging/testing. 
  // The format is as follows:
  // field1-name: field1-value  field2-name: field2-value ... \n
  // (e.g., age: 24  height: 6.1  salary: 9000
  //        age: NULL  height: 7.5  salary: 7500)
  RC printRecord(const std::vector<Attribute> &recordDescriptor, const void *data);

/******************************************************************************************************************************************************************
IMPORTANT, PLEASE READ: All methods below this comment (other than the constructor and destructor) are NOT required to be implemented for the part 1 of the project
******************************************************************************************************************************************************************/
  RC deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid);

  // Assume the RID does not change after an update
  RC updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const void *data, const RID &rid);

  RC readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid, const std::string &attributeName, void *data);

  // Scan returns an iterator to allow the caller to go through the results one by one. 
  RC scan(FileHandle &fileHandle,
      const std::vector<Attribute> &recordDescriptor,
      const std::string &conditionAttribute,
      const CompOp compOp,                  // comparision type such as "<" and "="
      const void *value,                    // used in the comparison
      const std::vector<std::string> &attributeNames, // a list of projected attributes
      RBFM_ScanIterator &rbfm_scanIterator);

  short GetSlotCnt(FileHandle &fileHandle, const int page_id);
private:
  int getFreePage(FileHandle &fileHandle, short length);

protected:
  RecordBasedFileManager();
  ~RecordBasedFileManager();

private:
  static RecordBasedFileManager *_rbf_manager;
  PagedFileManager *_pf_manager;
};

// Record: variable length
class Record{
public:
    char *ptr_data_; //points to beginning of header

    Record(char *data = NULL);
    ~Record();

    RC writeRecord(const std::vector<Attribute> &recordDescriptor, const void *data);
    RC readRecord(const std::vector<Attribute> &recordDescriptor, void *data);
    RC readAttribute(const std::vector<Attribute> &recordDescriptor, const std::string attribute_name, void *data, short& attr_len);

    // void setRecordLength(const short length);
    // short getRecordLength();
    static short getRecordLength(const std::vector<Attribute> &descriptor, const void *data);
    static short getRecordContentLength(const std::vector<Attribute> &descriptor, const void *data);
    static std::vector<bool> CheckNullbits(const int num, const char* nullbits);
};

// Heap File Page: variable length
class HFPage{
public:
    char *ptr_data_;
    short free_space_;
    short free_offset_;    // offset of the pointer to free space
    short slot_count_;       // #slots
    //SlotDirectory slotDirectory;
    
    HFPage(char *data = NULL);
    ~HFPage();

    void load(char *data);
    
    Record getRecord(unsigned i); // return ith record, i start from 1
    Record setRecord(const short length, short& slotID);  // append record
    void deleteRecord(const short length, const unsigned slotID);  // delete record
    Record updateRecord(const short length, const unsigned slotID);  // append record
    
    short getLength(const unsigned slotID);
    void setLength(const unsigned slotID, const short length);
    short getOffset(const unsigned slotID);
    void setOffset(const unsigned slotID, const short Offset);
    
    RC getRID(const unsigned slotID, RID &rid);
    RC setRID(const unsigned slotID, const RID rid);
    bool checkTombstone(const unsigned slotID);
    
    short getSlotCnt();
    void setSlotCnt(const short slot_count_);
    short getFreeOffset();
    void setFreeOffset(const short free_offset_);
};

template <typename T>
bool Compare(const T data_value, const T condition_value, const CompOp compare_op){
    // Question : how to compare string??   
    switch (compare_op) {
        case(NO_OP):
            return true;

        case(EQ_OP): // ==
            return (data_value == condition_value)?  true : false;

        case(LT_OP): // <
            return (data_value < condition_value)?  true : false;

        case(LE_OP): // <=
            return (data_value <= condition_value)?  true : false;

        case(GT_OP): // >
            return (data_value > condition_value)?  true : false;

        case(GE_OP): // >=
            return (data_value >= condition_value)?  true : false;

        case(NE_OP): // !=
            return (data_value != condition_value)?  true : false;

        default:
            // std::iostream::cout<< "comp_op not appropriate." << std::endl;
            break;
    }
    return false; // no corret return before, something went wrong
}

#endif
