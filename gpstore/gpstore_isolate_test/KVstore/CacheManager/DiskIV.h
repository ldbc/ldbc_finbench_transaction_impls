/*=======================================================================
* File name: DiskIV.h
* Author: Zhengren Wang
* Mail: zr-wang@outlook.com
* Last Modified: 2023/4/13 
* Description:
* Definition of class DiskIV
* ======================================================================*/

#include "DiskManager.h"
namespace test_zrwang
{

class DiskIV{
private:
    DiskManager* diskManager;
    map<unsigned,unsigned> key2Idx;
    string IVFile_path;
    FILE* IVFile;
public:
    DiskIV(string& _filename, string &_mode, unsigned _keynum = 0);
    DiskIV(DiskIV&& o);
    ~DiskIV();
    unsigned write(unsigned key, const char *_str, const unsigned long _len);
    bool read(unsigned key, char *&_str, unsigned long &_len);
    bool remove(unsigned key);
};
    
}