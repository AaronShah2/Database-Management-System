
#ifndef _ix_h_
#define _ix_h_

#define NO_CERR

#include <vector>
#include <string>
#include <cstring>
#include "../rbf/rbfm.h"
#include "../rbf/pfm.h"
#include <memory>

//using namespace std;

# define IX_EOF (-1)  // end of the index scan
# define NONE 0

typedef enum { Index = 0, Leaf } NodeType;
typedef enum { Left = 0, Right } SiblingDirection;
typedef std::vector<RID> Bucket;

class BtreeNode;
class IXFileHandle;
class IX_ScanIterator;
class IndexManager;

class BtreeHandle{
public:
    BtreeHandle();

    RC CreateNewNode(IXFileHandle &ix_file_handle, NodeType nodeType, BtreeNode &node);

    RC ReadNode(IXFileHandle &ix_file_handle, int nodeID, BtreeNode &node) const;

    RC WriteNode(IXFileHandle &ix_file_handle, BtreeNode &node);
    
    RC Insert(IXFileHandle &ix_file_handle, const Attribute &attribute, const void *key, const RID &rid);

    RC Delete(IXFileHandle &ix_file_handle, const Attribute &attribute, const void *key, const RID &rid);
    
    void printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const;
    
    int Search(IXFileHandle &ix_file_handle, const void *key);

private:
    RC LoopInsert(IXFileHandle &ixfileHandle, int root_id, const void *key, const RID &rid);
    // RC RecursiveInsert(IXFileHandle &ix_file_handle, int nodeID, const void *key, const RID &rid, int &newChildNodeID, void *newKey);

    int RecursiveSearch(IXFileHandle &ix_file_handle, int nodeID, const void *key);
    
    RC RecursivePrint(IXFileHandle &ixfileHandle, int node_id) const;
    
    RC SplitLeafNode(IXFileHandle &ix_file_handle, BtreeNode &oldNode, BtreeNode &newNode);

    RC SplitIndexNode(IXFileHandle &ix_file_handle, BtreeNode &oldNode, BtreeNode &newNode, void *copyUpKey);

    RC GetCopyUpKey(void* source, void * dest, AttrType attrType);

    int GetOrder(const Attribute &attribute);
public:
    AttrType attr_type_;
    int attr_len_;
    int root_id_;
    int first_leaf_id_;
    int last_leaf_id_;
    int degree_;
};

class BtreeNode{
public:    
    BtreeNode();

    BtreeNode(char *data);

    void LoadNode(char *data);
    
    static RC CompareKey(const void *attrValue, const void *value, AttrType attrType);
    
    //RC InsertIndex(const void* key, const int &childNodeID);
    RC InsertIndex(std::shared_ptr<void> key, const int &childNodeID);

    //RC InsertLeaf(const void* key, const RID &rid);
    RC InsertLeaf(std::shared_ptr<void> key, const RID &rid);
    // -----
    int GetKeyIndex(const void* key);
    
    int GetChildIndex(const void* key, int keyIndex);
    
    int GetLeafIndex(const void* key, int keyIndex);
    
    int GetOverflowPageIDOffset();
    // -----
    RC ReadBucket(IXFileHandle &ix_file_handle);

    RC WriteBucket(IXFileHandle &ix_file_handle);

    RC AddBucketPage(IXFileHandle &ix_file_handle, int &nextPageID);

    std::vector<std::shared_ptr<void>> ReadKeys(AttrType attrType, int maximumKeyNumber);
    void WriteKeys(std::vector<std::shared_ptr<void>> &keys, AttrType attrType, int maximumKeyNumber);

public:
    char node_data_[PAGE_SIZE];

    int node_id_;

    NodeType node_type_;
    AttrType attr_type_;
    int attr_len_;

    int deleted_;
    int degree_;

    //std::vector<void *> keys_;
    std::vector<std::shared_ptr<void>> keys_;
    std::vector<int> child_list_;

    int left_sibling_;
    int right_sibling_;
    std::vector<Bucket> buckets_;

};

class IndexManager {

    public:
        static IndexManager* instance();

        // Create an index file.
        RC createFile(const std::string &fileName);

        // Delete an index file.
        RC destroyFile(const std::string &fileName);

        // Open an index and return an ix_file_handle.
        RC openFile(const std::string &fileName, IXFileHandle &ix_file_handle);

        // Close an ix_file_handle for an index.
        RC closeFile(IXFileHandle &ix_file_handle);

        // Insert an entry into the given index that is indicated by the given ix_file_handle.
        RC insertEntry(IXFileHandle &ix_file_handle, const Attribute &attribute, const void *key, const RID &rid);

        // Delete an entry from the given index that is indicated by the given ix_file_handle.
        RC deleteEntry(IXFileHandle &ix_file_handle, const Attribute &attribute, const void *key, const RID &rid);

        // Initialize and IX_ScanIterator to support a range search
        RC scan(IXFileHandle &ix_file_handle,
                const Attribute &attribute,
                const void *lowKey,
                const void *highKey,
                bool lowKeyInclusive,
                bool highKeyInclusive,
                IX_ScanIterator &ix_ScanIterator);

        RC writeMataData(IXFileHandle &ix_file_handle, const BtreeHandle *btree);

        RC readMataData(IXFileHandle &ix_file_handle, BtreeHandle *btree);

        // Print the B+ tree in pre-order (in a JSON record format)
        void printBtree(IXFileHandle &ix_file_handle, const Attribute &attribute) const;

    protected:
        IndexManager();
        ~IndexManager();

    private:
        static IndexManager *_index_manager;
        PagedFileManager *_pf_manager;
    
    BtreeHandle btree_;
//  FileHandle ix__ixsifile_handle_;
};

class IX_ScanIterator {
public:    
    IX_ScanIterator();                              // Constructor
//    ~IX_ScanIterator();                             // Destructor

    RC Init(BtreeHandle &bt,
            IXFileHandle &ix_file_handle,
            const Attribute &attribute,
            const void *lowKey,
            const void *highKey,
            bool lowKeyInclusive,
            bool highKeyInclusive);
    
    RC getNextEntry(RID &rid, void *key);           // Get next matching entry

    RC close();

private:

    void CopyKey(void *dest, const void *src, AttrType attrType);

    RC ReadEveryNode(int nodeID, BtreeNode &node);

public:
    int first_leaf_id_;
    IXFileHandle *ix_file_handle_;
    BtreeHandle btree_;
    BtreeNode cur_node_;
    Attribute attr_;
    const void *low_key_;
    const void *high_key_;
    bool if_low_key_;
    bool if_high_key_;
    int cur_node_id_;
    AttrType attr_type_;
    int idx_for_keys_;
    int second_idx_;

    int compare_low_key_eq_;  
    int compare_high_key_eq_; 
};

class IXFileHandle {
public:
    // Constructor
    IXFileHandle();

    // Destructor
    ~IXFileHandle();

    RC readPage(PageNum pageNum, void *data);

    RC writePage(PageNum pageNum, const void *data);
    
    RC appendPage(const void *data);
    
    RC openFile(const std::string &fileName);
    
    RC closeFile();
	// Put the current counter values of associated PF FileHandles into variables
	RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);

public:
    // variables to keep counter for each operation
    unsigned ixReadPageCounter;
    unsigned ixWritePageCounter;
    unsigned ixAppendPageCounter;

	FileHandle file_handle_;
	std::string filename_;
	int counter_;
};

#endif
