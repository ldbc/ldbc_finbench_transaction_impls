/*=======================================================================
* File name: IVCache.cpp
* Author: Xinyi Ye
* Mail:
* Last Modified:
* Description:
* ======================================================================*/

#include <utility>

#include "IVBlockManager.h"
#include "../../DiskIV/DiskIV.h"
#include "lru_cache.h"
#include "sharded_cache.h"

using tsLRUHandle = threadsafe_lrucache::LRUHandle;
using tsLRUCache = threadsafe_lrucache::LRUCache;

class CacheItem;
#define num_shard_bits_ 2  /// 2^2 = 4
#define num_in_mem_shards 48

extern bool start_set_cache_item;
extern double sf_value;

class InMemoryItemTable
{
    mutex mut;
    vector<pair<CompactStr,bool>>  id2str; /// bool: dirty bit
public:
    int item_table_idx = -1;

    InMemoryItemTable()
    {
        unsigned max_node_num;
        max_node_num = 10000;
        id2str.resize(max_node_num / num_in_mem_shards + 1, {{}, false});
    }

    CompactStr Get(unsigned key)
    {
        unique_lock<mutex> l(mut);
        return id2str[key / num_in_mem_shards].first;
    }

    /**
     *
     * @param key
     * @param value
     * @param len
     * @return delta mem usage
     */
    int Set(unsigned int key, const shared_ptr<char[]>& value, unsigned int len)
    {
        unsigned idx = key / num_in_mem_shards;
        assert(idx < id2str.size());
        unique_lock<mutex> l(mut);
        if(id2str[idx].first.value ==  nullptr){
            bool dirty_bit = false;
            if(start_set_cache_item)dirty_bit = true;
            id2str[idx] = {{value, len}, dirty_bit};

            return -1;
        }
        else
        {
            id2str[idx] = {{value, len},true};
            return -1;
        }
    }



    /**
     *
     * @param key
     * @return true: success. false: the value is already nullptr / not-exist.
     */
    bool Del(unsigned int key)
    {
        unique_lock<mutex> l(mut);
        unsigned idx = key / num_in_mem_shards;
        assert(idx < id2str.size());
        if(id2str[idx].first.value == nullptr){
            return false;
        }
        else
        {
            unsigned pre_len = id2str[idx].first.len;
            if(pre_len == 0)
            {
                assert(id2str[idx].first.value == nullptr);
                id2str[idx] = {{nullptr,0}, true};
                return false;
            }
            id2str[idx] = {{nullptr,0}, true};
            return true;
        }
    }

    void Flush(DiskIV& disk_iv)
    {
        /// we do not need lock here. CacheManager::global_rw_lock ensures that nothing else is done in cache_manager when flushing
        int id = item_table_idx;
        for(auto& [str, dirty_bit]: id2str)
        {
            // cout<<"flush pinned ids, id = "<<id<<endl;
            if(dirty_bit)  /// dirty
            {
                // cout<<"dirty, really flush, id = "<<id<<endl;
                disk_iv.WriteToDisk(id, str.value.get(), str.len);
                dirty_bit = false;
            }
            id+=num_in_mem_shards;
        }
    }
};

class KeepInMemoryPart
{
    mutex cap_usa_mut; /// protect usage & capacity only
    uint64_t capacity = 1024*1024*1024*(unsigned)512;
    uint64_t usage = 0;
    InMemoryItemTable in_memory_item_tables[num_in_mem_shards];
public:
    KeepInMemoryPart()
    {
        for(int i = 0;i<num_in_mem_shards;i++)
        {
            in_memory_item_tables[i].item_table_idx = i;
        }
    }

    InMemoryItemTable& GetShard(unsigned key)
    {
        unsigned num_shard = key % num_in_mem_shards;
        return in_memory_item_tables[num_shard];
    }
public:
    uint64_t SetAndGetCapacity()
    {
        unique_lock<mutex> l(cap_usa_mut);
        capacity = usage * 2;
        return capacity;
    }

    CompactStr Get(unsigned key)
    {
        return GetShard(key).Get(key);
    }

    /**
     *
     * @param key
     * @param value
     * @param len
     * @return 0: do not need to add capacity; >0: have added capacity to be return_value.
     */
    unsigned Set(unsigned key, const shared_ptr<char[]>& value, unsigned len)
    {
        int delta_usage = GetShard(key).Set(key, value, len);
        unique_lock<mutex> l(cap_usa_mut);
        usage += delta_usage;
        if(usage > capacity) {
            capacity = usage*3/2;
            return capacity;
        }
        else return 0;
    }

    /**
     *
     * @param key
     * @return true: success. false: the value is already nullptr / not-exist.
     */
    bool Del(unsigned int key)
    {
        return GetShard(key).Del(key);
    }


    void Flush(DiskIV& disk_iv) {
        /// we do not need lock here. CacheManager::global_rw_lock ensures that nothing else is done in cache_manager when flushing
        for(auto& shard: in_memory_item_tables)
        {
            shard.Flush(disk_iv);
        }
    }
};

/**
 * @brief a cache that interact with disk.
 */
class CacheManager {
private:

    unique_ptr<DiskIV> disk_iv;

    unordered_set<unsigned> kept_in_memory_ids;
    KeepInMemoryPart keep_in_memory_part;
    tsLRUCache* lru_cache_pt = nullptr;

    uint64_t buffer_size = 0;

    shared_mutex global_rw_lock; ///< writer: flush / keepincache; reader: others. todo: change this to writer-preferred rwlock.
    atomic<bool> warmup_end = false;
    once_flag init_flag;

    void LRUCacheInit();
public:

    CacheManager(uint64_t buffer_size, unique_ptr<DiskIV> disk_iv_);
    ~CacheManager();

    /**
     * 若不在cache则加载，然后返回值。若key不存在，返回nullptr
     * @param key
     * @return
     */
    CompactStr GetCacheItem(unsigned key);

    /**
     * @note char *str的访问权限从此归cache所有，caller不能再访问str. 如果caller想获得str，需要再调用一次GetCacheItem函数
     * @note 若在cache则删除cache内容，然后添加新值【dirtyFlag置1】
     * @param key
     * @param str
     * @param len
     */
    void SetCacheItem(unsigned key, char *str, unsigned long len);

    /**
     * @brief 删除key。删除对应的磁盘块（store）、内存块（value）（如果有的话）。
     * @param key
     * @return true: SUCCESS. false: the key does not exist
     */
    bool Delete(unsigned key);


    /**
     * @brief write all "dirty" things from cache to disk to persist them. Items in memory are not changed.
     * @note use a global lock on the cache to flush the whole cache. this operation is rare so that this will not affect performance.
     * @return always true (I do not know why this cannot be void. but too lazy to communicate with many colleagues to change it.)
     */
    bool Flush();

    /**
     * @note should be called by only 1 thread at the very beginning of the code.
     * @brief make one item always in cache.
     * @param key
     * @return TRUE: success；FALSE: key不存在
     */
    bool KeepInCache(unsigned key);

    friend class CacheItem;


    void TestWarmupEnd();

/**
 *
 * @param key the shard that need to be evict is key's shard.
 * @return need evict again
 */
    bool Evict(unsigned int key);

    void EvictUsingShardId(unsigned int shard_num);

    int Get4Bytes(unsigned int key);

    unsigned Get4Bytes(unsigned int key, unsigned int offset);

    void PrintInfo();
};