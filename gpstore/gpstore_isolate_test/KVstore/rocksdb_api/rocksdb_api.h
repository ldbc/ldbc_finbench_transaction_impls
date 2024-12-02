#ifndef __ROCKSDB_API__
#define __ROCKSDB_API__

/**
 * @file rocksdb_api.h
 * @author victbr (yunfan.xiong@stu.pku.edu.cn)
 * @brief interface of rocksdb api in c style
 * @version 0.1
 * @date 2023-04-13
 *
 * @copyright Copyright (c) 2023
 *
 */

// 关于本文件自定义的三个类的关系：
// 一个数据库对应一个`StoreApiImpl`（一个`StoreApiImpl`中包含一个`rocksdb::DB * db_`）
// 一个数据库的每个属性索引对应一个`TxnPStore`：对应一个`StoreApi`，也对应`db_`的一个列族（`rocksdb::ColumnFamilyHandle*`），还对应一个`std::unique_ptr<WriteBatchRep>`（用于批量写）
//    不同属性索引用不同列族是需要的，因为不同索引间存在相同key的情况
// 每个包含一个`WriteBatchRep`包含一个`std::unique_ptr<rocksdb::WriteBatch>`

/* **************************************************************** */
/*                                                                  */
/*                            rocksdb                               */
/*                                                                  */
/* **************************************************************** */
#ifndef USE_LMDB

#include <unistd.h>

#include <cstdio>
#include <string>
#include <map>
#include <unordered_map>
#include <thread>
#include <vector>
#include <mutex>

// #include "rocksdb/c.h"
// #include "rocksdb/db.h"
// #include "rocksdb/options.h"
// #include "rocksdb/slice.h"

// using ROCKSDB_NAMESPACE::DB;
// using ROCKSDB_NAMESPACE::Options;
// using ROCKSDB_NAMESPACE::PinnableSlice;
// using ROCKSDB_NAMESPACE::ReadOptions;
// using ROCKSDB_NAMESPACE::Status;
// using ROCKSDB_NAMESPACE::WriteBatch;
// using ROCKSDB_NAMESPACE::WriteOptions;

/// using char* to handle key and value
// typedef std::string KeyType;
// typedef std::string ValueType;

namespace rocksdb {
class DB;
class Iterator;
class WriteBatch;
class ColumnFamilyHandle;
class Options;
}

class WriteBatchRep {
 public:
  /**
   * @brief construct a new object
   *
   */
  WriteBatchRep(rocksdb::DB *db, rocksdb::ColumnFamilyHandle *handle);

  /**
   * @brief add key-value to write_batch and execute insertion when there are over 1000 k-v pairs in batch
   *
   * @param key
   * @param key_len
   * @param value
   * @param val_len
   */
  void Put(const char* key, unsigned key_len, const char* value,
           unsigned val_len);

  /**
   * @brief execute insertion manually
   *
   */
  void Flush();

  /**
   * @brief destroy the Store Api object and execute remaining insertion before
   *
   */
  ~WriteBatchRep();

 private:
  rocksdb::DB *db_;
  rocksdb::ColumnFamilyHandle *handle_;
  std::mutex mutex_write_;
  std::mutex mutex_flush_;
  std::unique_ptr<rocksdb::WriteBatch> write_batch_;
  std::unique_ptr<std::thread> flush_thread_ = nullptr;
  int count_ = 0; // the num of k-v pairs in batch
  const static int threshold_ = 200000;
};

class StoreApiImpl {
 public:
  explicit StoreApiImpl(const std::string &kDBPath);

  /**
   * @brief open column family
   * 
   * @param cf_name name of column family
   * @param key_size fixed key len; when key_size==0, key len is variable
   * @return column_id
  */
  unsigned Open(std::string cf_name, unsigned key_size);

  /**
   * @brief close column family
   * 
   * @param cf_name name of column family
   * @param is_last_open_column return whether is the last cf to close or not
  */
  void Close(unsigned column_id, bool &is_last_open_column);
  void Put(unsigned column_id, const char *key, unsigned key_len, const char *value, unsigned val_len);
  void Get(unsigned column_id, const char *key, unsigned key_len, char *&value, unsigned &val_len);
  void Del(unsigned column_id, const char *key, unsigned key_len);
  void Clear(unsigned column_id, const std::string &cf_name);


  rocksdb::Iterator* RangeSeekFirst(unsigned column_id, const char* lower_bound, unsigned l_len, const char* upper_bound, unsigned u_len,
                                    const char* &key, size_t &key_len, unsigned prefix_len = 0) const;
  rocksdb::Iterator* RangeSeekLast(unsigned column_id, const char* lower_bound, unsigned l_len, const char* upper_bound, unsigned u_len,
                                   const char* &key, size_t &key_len, unsigned prefix_len = 0) const;
  void getValueNumber(unsigned column_id, std::unordered_map<std::string, unsigned> &m, int element_len);


  void Write2Batch(unsigned column_id, const char* key, unsigned key_len, const char* value, unsigned val_len);
  void BatchFlush(unsigned column_id);

  void GetRocksdbCache(int& cache_miss, int& cache_hit);
  void Print(int& cache_miss, int& cache_hit);

  ~StoreApiImpl();

  rocksdb::DB *db_;

 private:
  std::string dir_path_;
  std::map<std::string, unsigned> cf_name2id_; // default db excluded
  std::map<std::string, unsigned> cf_name2prefix_size_; // default db excluded
  std::unique_ptr<rocksdb::Options> options_;
  std::vector<rocksdb::ColumnFamilyHandle*> handles_;
  std::vector<std::unique_ptr<WriteBatchRep>> write_batchs_;
  unsigned open_column_num_{0}; // default column not included
  unsigned open_api_num_{0}; // default column not included
  const static unsigned invalid_column_id_ = 0xffffffff;
  const static bool statistic = false;
};

/**
 * @brief please execute constructor and destructor concurrently, don't mix them with other functions cause we don't protect
 */
class StoreApi{
 public:
  /**
   * @brief Construct a new Store Api object,
   *
   * @param kDBPath path to save the database
   * @param cf_name name of column family
   * @param key_size fixed key len; when key_size==0, key len is variable
   */
  StoreApi(const std::string &kDBPath, const std::string &cf_name, unsigned key_size);

  /**
   * @brief set key-value
   *
   * @param key[in]
   * @param key_len
   * @param value[in]
   * @param val_len
   */
  void Put(const char *key, unsigned key_len, const char *value, unsigned val_len);

  /**
   * @brief get value
   *
   * @param key[int]
   * @param key_len
   * @param value[out]
   * @param len[out]
   *
   * @note allocate memmory in this function, but free the memory by caller.
   */
  void Get(const char *key, unsigned key_len, char *&value, unsigned &val_len);

  /**
   * @brief del key
   *
   * @param key[in]
   * @param key_len
   */
  void Del(const char *key, unsigned key_len);

  /**
   * @brief clear all
   */
  void Clear();

  /**
   * @brief : Get first iterator for range seek
   *
   * @param lower_bound Min key of range, no lower_bound when only has prefix, cannot be nullptr, must have prefix at least
   * @param l_len Length of min key, no lower_bound when equal to prefix_len
   * @param upper_bound Max key of range, no upper_bound when nullptr
   * @param u_len Length of max key, no upper_bound when 0
   * @param key : Result key of this search
   * @param key_len : Length of result key
   * @return : first iterator of rocksdb in range or nullptr when not find
   */
  rocksdb::Iterator* RangeSeekFirst(const char* lower_bound, unsigned l_len, const char* upper_bound, unsigned u_len,
                                    const char* &key, size_t &key_len, unsigned prefix_len = 0) const;

  /**
   * @brief : Get last iterator for range seek
   *
   * @param lower_bound Min key of range, no lower_bound when nullptr
   * @param l_len Length of min key, no lower_bound when 0
   * @param upper_bound Max key of range, must exist. equal to max(prefix+0xfff...) when seek for max key.
   * @param u_len Length of max key
   * @param key : Result key of this search
   * @param key_len : Length of result key
   * @return : first iterator of rocksdb in range or nullptr when not find
   */
  rocksdb::Iterator* RangeSeekLast(const char* lower_bound, unsigned l_len, const char* upper_bound, unsigned u_len,
                                   const char* &key, size_t &key_len, unsigned prefix_len = 0) const;

  /**
   * @brief : Get next iterator for range seek
   * @param iterator : Last iterator
   * @param upper_bound : Max key of range, no upper_bound when only has prefix, cannot be nullptr, must have prefix at least
   * @param u_len : Length of max key, no upper_bound when equal to prefix_len
   * @param key : Result key of this search
   * @param key_len : Length of result key
   * @return : next iterator of rocksdb in range or nullptr when not find
   */
  static rocksdb::Iterator* RangeSeekNext(rocksdb::Iterator* iterator, const char* upper_bound, unsigned u_len,
                                          const char* &key, size_t &key_len, unsigned prefix_len = 0);

  /**
   * @brief get element number of value when load database
   *
   */
  void getValueNumber(std::unordered_map<std::string, unsigned> &m, int element_len);

  /**
   * @brief add k-v pair to write batch
   *
   * @param key[in]
   * @param key_len
   * @param value[in]
   * @param val_len
   */
  void Write2Batch(const char* key, unsigned key_len, const char* value, unsigned val_len);

  /**
   * @brief flush write_batch to rocksdb
   *
   */
  void BatchFlush();

  /**
   * @brief get rocksdb cache info
   *
   * @param cache_miss
   * @param cache_hit
   */
  void GetRocksdbCache(int& cache_miss, int& cache_hit);

  /**
   * @brief print statistic information about rocksdb
   *
   */
  void Print(int& cache_miss, int& cache_hit);

  /**
   * @brief get the db pointer
   * 
  */
  std::shared_ptr<StoreApiImpl> GetPtr() {
    return db_impl_;
  }

  /**
   * @brief Destroy the Store Api object
   *
   */
  ~StoreApi();
 private:
  inline static std::map<std::string, std::shared_ptr<StoreApiImpl>> db_name2impl_{};
  std::shared_ptr<StoreApiImpl> db_impl_;
  std::string dir_path_;
  std::string cf_name_;
  unsigned cf_id_;
};

#endif // #ifndef USE_LMDB

/* **************************************************************** */
/*                                                                  */
/*                             lmdb                                 */
/*                                                                  */
/* **************************************************************** */
#ifdef USE_LMDB

#include <unistd.h>
#include <assert.h>
#include <cstdio>
#include <string>
#include <cstring>
#include <map>
#include <unordered_map>
#include <thread>
#include <vector>
#include <mutex>
#include <iostream>
#include <experimental/filesystem>
#include "lmdb.h"



class WriteBatchRep {
public:
    WriteBatchRep(MDB_env* env, MDB_dbi dbi);
    void Put(const char* key, unsigned key_len, const char* value, unsigned val_len);
    void Flush();
    ~WriteBatchRep();

private:
    MDB_env* env_;
    MDB_dbi dbi_;
    std::mutex mutex_write_;
    std::mutex mutex_flush_;
    std::unique_ptr<std::thread> flush_thread_ = nullptr; // TODO: use flush_thread_ to batch write
    int count_ = 0; // the num of k-v pairs in batch
    const static int threshold_ = 200000;
    // tmp store for batch write
    std::vector<MDB_val> keyset_;
    std::vector<MDB_val> valueset_;
};

class StoreApiImpl {
public:
    explicit StoreApiImpl(const std::string& kDBPath);
    unsigned Open(std::string db_name);
    void Close(unsigned db_id, bool& is_last_open_column);
    void Put(unsigned db_id,const char* key, unsigned key_len, const char* value, unsigned val_len);
    void Put(unsigned db_id, const std::vector<unsigned long long > &key_set, const std::vector<std::pair<char*, unsigned>> &val_set);
    void Get(unsigned db_id,const char* key, unsigned key_len, char*& value, unsigned& val_len);
    void Del(unsigned db_id,const char* key, unsigned key_len);
    void Clear(unsigned db_id);
    
    MDB_cursor* RangeSeekFirst(const char* lower_bound, unsigned l_len,
                                     const char* upper_bound, unsigned u_len, const char*& key, size_t& key_len, unsigned prefix_len = 0) const;
    MDB_cursor* RangeSeekLast(const char* lower_bound, unsigned l_len,
                                    const char* upper_bound, unsigned u_len, const char*& key, size_t& key_len, unsigned prefix_len = 0) const;
    void getValueNumber(std::unordered_map<std::string, unsigned>& m, int element_len);
    void Write2Batch(unsigned db_id,const char* key, unsigned key_len, const char* value, unsigned val_len);
    void BatchFlush(unsigned db_id);
    void GetRocksdbCache(int& cache_miss, int& cache_hit);
    void Print(int& cache_miss, int& cache_hit);
    ~StoreApiImpl();

    MDB_env* env_;
    MDB_txn* txn_;
    MDB_dbi default_dbi_;
    std::map<unsigned, MDB_dbi> id2db;
    std::map<unsigned, std::string> id2dbname;
    std::string dir_path_;
    std::map<std::string, unsigned> db_name2id_;
    std::vector<std::unique_ptr<WriteBatchRep>> write_batchs_;
    unsigned open_db_num_{0};
};


class StoreApi {
public:
    StoreApi(const std::string& kDBPath,const std::string &db_name = "just_for_test");

    void Put(const char* key, unsigned key_len, const char* value, unsigned val_len);
    void Put(const std::vector<unsigned long long > &key_set, const std::vector<std::pair<char*, unsigned>> &val_set);
    void Get(const char* key, unsigned key_len, char*& value, unsigned& val_len);
    void Del(const char* key, unsigned key_len);
    void Clear();
    MDB_cursor* RangeSeekFirst(const char* lower_bound, unsigned l_len, const char* upper_bound,
                                      unsigned u_len, const char*& key, size_t& key_len, unsigned prefix_len = 0) const;
    MDB_cursor* RangeSeekLast(const char* lower_bound, unsigned l_len, const char* upper_bound,
                                     unsigned u_len, const char*& key, size_t& key_len, unsigned prefix_len = 0) const;
    static MDB_cursor* RangeSeekNext(MDB_cursor* cursor, const char* upper_bound,
                                           unsigned u_len, const char*& key, size_t& key_len, unsigned prefix_len = 0);
    void getValueNumber(std::unordered_map<std::string, unsigned>& m, int element_len);
    void Write2Batch(const char* key, unsigned key_len, const char* value, unsigned val_len);
    void BatchFlush();
    void GetRocksdbCache(int& cache_miss, int& cache_hit);
    void Print(int& cache_miss, int& cache_hit);
    std::shared_ptr<StoreApiImpl> GetPtr() ;
    ~StoreApi();

private:
    inline static std::map<std::string, std::shared_ptr<StoreApiImpl>> db_name2impl_{};
    std::shared_ptr<StoreApiImpl> db_impl_;
    std::string dir_path_;
    // std::string cf_name_;
    unsigned db_id_;
};

#endif // #ifdef USE_LMDB

#endif  // __ROCKSDB_API__