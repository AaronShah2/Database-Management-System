#ifndef _pfm_h_
#define _pfm_h_

typedef unsigned PageNum;
typedef int RC;
typedef char byte;

#define PAGE_SIZE 4096
#define SUCCESS 0
#define FAIL -1

#include <string>
#include <climits>
#include <fstream>

// using namespace std;

class FileHandle;

class PagedFileManager
{
public:
    static PagedFileManager* instance();                                  // Access to the _pf_manager instance

    RC createFile    (const std::string &fileName);                            // Create a new file
    RC destroyFile   (const std::string &fileName);                            // Destroy a file
    RC openFile      (const std::string &fileName, FileHandle &fileHandle);    // Open a file
    RC closeFile     (FileHandle &fileHandle);                            // Close a file

protected:
    PagedFileManager();                                                   // Constructor
    ~PagedFileManager();                                                  // Destructor

private:
    static PagedFileManager *_pf_manager;
};


class FileHandle
{
public:
    // variables to keep the counter for each operation
    unsigned readPageCounter;
    unsigned writePageCounter;
    unsigned appendPageCounter;
    
    FileHandle();                                                         // Default constructor
    ~FileHandle();                                                        // Destructor
    
    FileHandle(const std::string &fileName);

    RC readPage(PageNum pageNum, void *data);                             // Get a specific page
    RC writePage(PageNum pageNum, const void *data);                      // Write a specific page
    RC appendPage(const void *data);                                      // Append a specific page
    unsigned getNumberOfPages();                                          // Get the number of pages in the file
    RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);  // Put the current counter values into variables

    RC openFile (const std::string &fileName);
    RC closeFile();

    std::string GetFileName();
    
private:
    std::fstream file_;
    std::string file_name_;
};

struct FileHead
{
    unsigned number_of_pages;
    unsigned read_page_counter;
    unsigned write_page_counter;
    unsigned append_page_counter;
};
#endif
