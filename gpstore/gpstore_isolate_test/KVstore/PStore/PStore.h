/*=============================================================================
# Filename: PStore.h
# Author:
# Mail:
# Last Modified:
# Description:
=============================================================================*/

#ifndef GPSTORE_PSTORE_H
#define GPSTORE_PSTORE_H

# include <string>
#include "../../Util/Transaction.h"
#include  "../rocksdb_api/rocksdb_api.h"


class PStore {
private:
    string rocksdb_name;
    string filename;
    string dir_path;
    unsigned key_size_ = 0;
    StoreApi *rocks_db;

    unsigned long long MAX_BUFFER_SIZE;
public:
    void GetRocksdbCache(int& cache_miss, int& cache_hit);
    PStore(std::string _dir_path, string _filename, unsigned long long _buffer_size, unsigned key_size);
    ~PStore();
    bool getvalue(const char *_key, char *& _value, unsigned &_len, unsigned _key_len = 0);
    void setvalue(const std::vector<unsigned long long > &key_set, const std::vector<std::pair<char*, unsigned>> &val_set);
    bool setvalue(const char *_key, const char * _value, unsigned _len, unsigned _key_len = 0);
    bool delvalue(const char *_key, unsigned _key_len = 0);

    void getValueNumber(std::unordered_map<string, unsigned> &m, int element_len);

    /**
     * @brief Get first key and value for range seek
     *
     * @param _key_lower_bound Min key of range, no lower_bound when only has prefix, cannot be nullptr, must have prefix at least
     * @param _key_lower_len Length of min key, no lower_bound when equal to prefix_len
     * @param _key_upper_bound Max key of range, no upper_bound when nullptr
     * @param _key_upper_len Length of max key, no upper_bound when 0
     * @param _key First key of this search or nullptr when not exist
     * @param key_len Length of result key
     * @param _val First value of this search or nullptr when not exist
     * @param _val_len Length of first value
     * @param prefix_len Length of same prefix of min/max key, now is property_id
     * @return Pointer of auxiliary structure for range seek, now is iterator of rocks_db
     *         "nullptr" means don't exist
     */
    void* RangeSeekFirst(const char* _key_lower_bound, unsigned _key_lower_len, const char* _key_upper_bound, unsigned _key_upper_len,
                         const char* &_key, size_t &key_len, const char* &_val, size_t &_val_len, unsigned prefix_len);


    /**
     * @brief Get first key and value for range seek
     *
     * @param _key_lower_bound Min key of range, no lower_bound when nullptr
     * @param _key_lower_len Length of min key, no lower_bound when 0
     * @param _key_upper_bound Max key of range, must exist. equal to max(prefix+0xfff...) when seek for max key.
     * @param _key_upper_len Length of max key
     * @param _key Last key of this search or nullptr when not exist
     * @param key_len Length of result key
     * @param _val Last value of this search or nullptr when not exist
     * @param _val_len Length of Last value
     * @param prefix_len Length of same prefix of min/max key, now is property_id
     * @return Pointer of auxiliary structure for range seek, now is iterator of rocks_db
     *         "nullptr" means don't exist
     */
    void* RangeSeekLast(const char* _key_lower_bound, unsigned _key_lower_len, const char* _key_upper_bound, unsigned _key_upper_len,
                         const char* &_key, size_t &key_len, const char* &_val, size_t &_val_len, unsigned prefix_len);

    /**
     * @brief Get next iterator for range seek
     *
     * @param ptr Pointer of auxiliary structure for range seek, now is iterator of rocks_db
     * @param _key_lower_bound Min key of range, no lower_bound when only has prefix, cannot be nullptr, must have prefix at least
     * @param _key_lower_len Length of min key, no lower_bound when equal to prefix_len
     * @param _key_upper_bound Max key of range, no upper_bound when nullptr
     * @param _key_upper_len Length of max key, no upper_bound when 0
     * @param _key Next key of this search or nullptr when not exist
     * @param key_len Length of result key
     * @param _val Next value of this search or nullptr when not exist
     * @param _val_len Length of next value
     * @param prefix_len Length of same prefix of min/max key, now is property_id
     * @return Pointer of auxiliary structure for range seek, now is iterator of rocks_db
     *         "nullptr" means don't exist
     */
    static void* RangeSeekNext(void* ptr, const char* _key_lower_bound, unsigned _key_lower_len, const char* _key_upper_bound, unsigned _key_upper_len,
                               const char* &_key, size_t &key_len, const char* &_val, size_t &_val_len, unsigned prefix_len);

    /**
     * @brief add k-v pair to write batch
     *
     * @param key[in]
     * @param key_len
     * @param val[in]
     * @param val_len
     */
    void Write2Batch(const char *key, unsigned key_len, const char * val, unsigned val_len) {
        rocks_db->Write2Batch(key, key_len, val, val_len);
    }

    /**
     * @brief flush write_batch to rocksdb
     *
     */
    void BatchFlush() {
        rocks_db->BatchFlush();
    }

    /**
     * @brief clear all values
     */
    void Clear() {
      rocks_db->Clear();
    }

/*     //lock(growing)
    int TryExclusiveLatch(char *_key, unsigned long key_len, shared_ptr<Transaction> txn, bool has_read = false);
    //unlock(commit)
    bool ReleaseLatch(char *_key, unsigned long key_len, shared_ptr<Transaction> txn, bool is_exclusive);

    //abort
    //clean invalid version(release exclusive latch along) and release exclusive lock
    bool Rollback(char *_key, unsigned long key_len, shared_ptr<Transaction> txn, bool has_read); */
};


#endif //GPSTORE_PSTORE_H