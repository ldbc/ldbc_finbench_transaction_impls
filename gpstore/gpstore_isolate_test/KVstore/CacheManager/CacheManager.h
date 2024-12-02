/*=======================================================================
* File name: CacheManager.h
* Author: Zhengren Wang
* Mail: zr-wang@outlook.com
* Last Modified: 2023/4/13 
* Description:
* Definition of class CacheManager
* ======================================================================*/

#include "DiskIV.h"

namespace test_zrwang
{
    class CacheManager;
    struct CompactStr
    {
        shared_ptr<char[]> value = nullptr;
        unsigned len = 0;
        CompactStr(shared_ptr<char[]> v, unsigned l):value(std::move(v)), len(l){}
        CompactStr(char* v, unsigned l):len(l)
        {
            value.reset(v);
        }
        CompactStr()=default;
    };
    class CacheItem{
        friend class CacheManager;
    private:
        bool dirtyFlag;
        bool pinFlag;
        unsigned key;
        CacheItem* next;
        CacheItem* prev;
        
        char *value;
        unsigned long length;
    public:
        CacheItem()
        : dirtyFlag(false),pinFlag(false),key(0),next(nullptr),prev(nullptr),value(nullptr),length(0)
        {
        }

        CacheItem(CacheItem&& item)
        : value(item.value), length(item.length)
        {
            item.value = nullptr;
            item.length = 0;
        }

        char at(int pos) const
        {
            return value[pos];
        }

        char operator[](int pos) const
        {
            return value[pos];
        }

        unsigned len() const
        {
            return length;
        }

        ~CacheItem()
        {
            if(value) delete[] value;
        }
    };

    class CacheManager {
    private:
        mutex cache_lock;
        unsigned long long cacheLimit;
        unsigned long long cacheSize;
        map<unsigned,CacheItem*> key2Item;
        unique_ptr<DiskIV> diskIV;
        CacheItem* head;
        bool cleanRoom(unsigned budget);

    public:
        CacheManager(unsigned long long _cacheLimit, unique_ptr<DiskIV> _diskIV);
        CompactStr GetCacheItem(unsigned key);
        void SetCacheItem(unsigned key, char *str, unsigned long len);
        bool Delete(unsigned key);
        bool Flush();
        bool KeepInCache(unsigned key);
    };
}