#include "pfm.h"
#include <cstdio> 
#include <iostream>
PagedFileManager* PagedFileManager::_pf_manager = 0;

PagedFileManager* PagedFileManager::instance()
{
    if(!_pf_manager)
        _pf_manager = new PagedFileManager();

    return _pf_manager;
}


PagedFileManager::PagedFileManager()
{
}


PagedFileManager::~PagedFileManager()
{
}


RC PagedFileManager::createFile(const std::string &fileName) {
    // need to add code to deal with recreate->fail
    /*
    ...
    */
	std::ifstream open_test(fileName);
	if (open_test)
	   return FAIL;
	else {
		if(std::ofstream(fileName).good())
			return SUCCESS;
//		Untracked files:

		return FAIL;
	}
}

RC PagedFileManager::destroyFile(const std::string &fileName) {
    if(remove(fileName.c_str()) == 0)
        return SUCCESS;
    return FAIL;
}

RC PagedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
    if(fileHandle.openFile(fileName) == SUCCESS)
        return SUCCESS;
    return FAIL;
}


RC PagedFileManager::closeFile(FileHandle &fileHandle) {
    if(fileHandle.closeFile() == SUCCESS)
        return SUCCESS;
    return FAIL;
}


FileHandle::FileHandle() {
    readPageCounter = 0;
    writePageCounter = 0;
    appendPageCounter = 0;
}

FileHandle::FileHandle(const std::string &fileName){
    file_name_ = fileName;

    // read Counters from filehead
    FileHead file_head;
    file_.seekg(0, std::ios_base::beg);
    file_.read(reinterpret_cast<char*>(&file_head), sizeof(FileHead));

    readPageCounter = file_head.read_page_counter;
    writePageCounter = file_head.write_page_counter;
    appendPageCounter = file_head.append_page_counter;
    /* 
    ... 
    */
}

FileHandle::~FileHandle()
{
}

RC FileHandle::readPage(PageNum pageNum, void *data){
    if (pageNum < 0 || pageNum >= getNumberOfPages()) {
        return FAIL;
    }
    file_.seekg((pageNum + 1) * PAGE_SIZE, std::ios_base::beg);
    file_.read((char*)data, PAGE_SIZE); // need the convert???
    if (file_.good()) {
        readPageCounter++;

        FileHead file_head;
        file_head.read_page_counter = readPageCounter;
        file_head.write_page_counter = writePageCounter;
        file_head.append_page_counter = appendPageCounter;
        file_head.number_of_pages = this->getNumberOfPages();

        file_.seekg(0, std::ios_base::beg);
        file_.write(reinterpret_cast<char*>(&file_head), sizeof(file_head));
        if(file_.good())
            return SUCCESS;
        return FAIL;
    }
    return FAIL;
}


RC FileHandle::writePage(PageNum pageNum, const void *data){
    if (pageNum < 0 || pageNum >= getNumberOfPages()) {
        return FAIL;
    }
    file_.seekp((pageNum + 1) * PAGE_SIZE, std::ios_base::beg);
    file_.write((const char*)data, PAGE_SIZE);
    if (file_.good()) {
        writePageCounter++;

        FileHead file_head;
        file_head.read_page_counter = readPageCounter;
        file_head.write_page_counter = writePageCounter;
        file_head.append_page_counter = appendPageCounter;
        file_head.number_of_pages = this->getNumberOfPages();

        file_.seekg(0, std::ios_base::beg);
        file_.write(reinterpret_cast<char*>(&file_head), sizeof(file_head));
        if(file_.good())
            return SUCCESS;
        return FAIL;
    }
    return FAIL;
}


RC FileHandle::appendPage(const void *data) {
    file_.seekp(0, std::ios_base::end);
    file_.write((const char*)data, PAGE_SIZE);
    if (file_.good()) {
        // renew and store the file header
        appendPageCounter++;

        FileHead file_head;
        file_head.read_page_counter = readPageCounter;
        file_head.write_page_counter = writePageCounter;
        file_head.append_page_counter = appendPageCounter;
        file_head.number_of_pages = this->getNumberOfPages();

        file_.seekg(0, std::ios_base::beg);
        file_.write(reinterpret_cast<char*>(&file_head), sizeof(file_head));
        if(file_.good())
            return SUCCESS;
        return FAIL;
    }
    return FAIL;
}


unsigned FileHandle::getNumberOfPages() {
    auto current_pos = std::ios_base::cur;
    file_.seekg(0, std::ios_base::end);
    auto bytesnumber_of_file = file_.tellg();
    file_.seekg(current_pos);
    return bytesnumber_of_file < 4096 ? 0 : bytesnumber_of_file / PAGE_SIZE - 1;
}


RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
    readPageCount = readPageCounter;
    writePageCount = writePageCounter;
    appendPageCount = appendPageCounter;

    // file.seekg(0, std::ios_base::beg);
    // file.seekg(0, std::ios_base::end);
    // std::cout<<file.tellg()<<std::endl;
    // std::cout<<file.width()<<std::endl;

    // // store into disk filehead
    // FileHead file_head;
    // file_head.read_page_counter = readPageCounter;
    // file_head.write_page_counter = writePageCounter;
    // file_head.append_page_counter = appendPageCounter;
    // file_head.number_of_pages = this->getNumberOfPages();

    // file.seekg(0, std::ios_base::beg);
    // file.write(reinterpret_cast<char*>(&file_head), sizeof(file_head));
    // /*
    // ...
    // */
    // std::cout<<file.rdstate()<<std::endl;
    // std::cout<<file.width()<<std::endl;

    if(file_.good())
        return SUCCESS;
    return FAIL;
}

RC FileHandle::openFile(const std::string &fileName) {
    file_.open(fileName.c_str(), std::fstream::in | std::fstream::out | std::fstream::binary);
    if (file_.is_open()){
        // read Counters from filehead
        FileHead file_head;
        file_.seekg(0, std::ios_base::beg);
        file_.read(reinterpret_cast<char*>(&file_head), sizeof(FileHead));
        if(!file_.good()){
        	file_.clear();
            //initialize file header
        	file_head.read_page_counter = 0;
        	file_head.write_page_counter = 0;
        	file_head.append_page_counter = 0;
        	file_head.number_of_pages = 0;
            file_.seekg(0, std::ios_base::beg);
            file_.write(reinterpret_cast<char*>(&file_head), sizeof(file_head));
            byte* align_buffer = new byte[PAGE_SIZE - sizeof(file_head)]{0};
            file_.write(align_buffer, PAGE_SIZE - sizeof(file_head)); // Syscall param writev(vector[...]) points to uninitialised byte(s)

            delete[] align_buffer;
        }
        this -> file_name_ = fileName;

        readPageCounter = file_head.read_page_counter;
        writePageCounter = file_head.write_page_counter;
        appendPageCounter = file_head.append_page_counter;

        // file.seekg(0, std::ios_base::end);
        // std::cout<<file.tellg()<<std::endl;

        return SUCCESS;
    }
    return FAIL;
}

RC FileHandle::closeFile() {
    // when should we store the counters into filehead?
    file_.close();
    return file_.is_open() ? FAIL : SUCCESS;
}

std::string FileHandle::GetFileName(){
    return this -> file_name_;
}
