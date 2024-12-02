//
// Created by yxy on 2023/4/1.
//

#ifndef GPSTORE_KVSTORE_DISKIV_DISKIV_H_
#define GPSTORE_KVSTORE_DISKIV_DISKIV_H_

#define DISKIV_MAX_KEY_NUM 1<<31

#define DiskIV_ASSERT(CONDITION)                                       \
        if (CONDITION) { } else {                               \
                cout << "ASSERT failed in DiskIV: "<< #CONDITION << endl;  \
        }

#include <shared_mutex>
#include <iostream>
#include <thread>
#include <chrono>
#include "../CacheManager/include/IVBlockManager.h"


/**
 * @brief IV: index to value.
 */
struct IVEntry_
{
private:
  unsigned block_id = 1<<31;  ///< 0 stands for "not exist". the first bit: dirty bit, showing whether we need to write the block id into file when flush.
public:
  std::mutex mut;

  IVEntry_() = default;

    /**
   * @brief copy constructor
   */
  void Copy(const IVEntry_ &);

  [[nodiscard]] unsigned GetBlockId() const
  {
      return block_id & (~ (1 << 31));
  }
  [[nodiscard]] bool GetBlockIdDirty() const
  {
    return block_id >> 31;
  }
  void SetBlockId(unsigned bid)
  {
      assert(bid >> 31 == 0);
      block_id = bid;
      block_id |= 1 << 31;
      assert(GetBlockIdDirty());
  }
  void SetBlockIdClean()
  {
      assert(block_id >> 31);
      block_id &= ~(1 << 31);
      assert(!GetBlockIdDirty());
  }
};


/**
 * @brief stores entry_vec, between CacheManager and IVBlockManager.
 * @note for Lingxin Zhang: you can refer to KVstore/IVArray/IVArray.cpp.
 */
class DiskIV
{

  std::unique_ptr<IVEntry_[]> entry_vec = nullptr;
  unsigned entry_vec_size = 0;

  std::unique_ptr<IVBlockManager> block_manager = nullptr;
  string entry_file_name;
  std::shared_mutex diskiv_mut;

  std::FILE *entry_file;

  /**
   * @brief double the size of entry_vec.
   *    We use write lock to prevent inconsistency.
   *    The efficiency will not be affected too much
   *    because write operation is rare(5%) in the whole database,
   *    so that this function is rarely called.
   */
  void Expand();

 public:

  /**
   * @note when mode == open: load entry_vec from disk; when mode == build: init it as all "not exist".
   * @param mode "build" / "open". build: an empty database; open: open from filename.
   * @param entry_file_name_ the name of the file storing entry_vec. (the file is empty when mode == "build").
   * @param bm
   * @param entry_num used to decide the size of entry_vec. It is only useful when mode == "build".
   */
  DiskIV(const string & mode, string entry_file_name_, unique_ptr<IVBlockManager> bm, unsigned entry_num);

  /**
   * @note move constructor
   * */
   DiskIV(DiskIV && src) noexcept;

  ~DiskIV() = default;   ///< all ptrs are stored using unique_ptr. As a result, we do not need to write deconstructor.

  /**
   * @brief alloc a new piece of memory, read the value of id from disk to the piece of memory.
   * @param id
   * @return a piece of newly alloced memory and its length.
   *    (nullptr, 0) means that the input id does not exist.
   */
  pair<char*, unsigned > ReadFromDisk(unsigned id);

  /**
   * @brief write "str" to disk.
   *    If id used to have value stored in disk, we replace the old value with str
   *    ( i.e., delete the old value in the disk and write the new value to disk).
   * @param id
   * @param str
   * @param len the length of str
   */
  void WriteToDisk(unsigned id, const char* str, unsigned len);

  /**
   * @brief delete the corresponding value to disk and set the IVEntry to 0 ("not exist").
   * @param id
   * @return true: succeed. false: id does not exist.
   */
//  bool Delete(unsigned id);

  /**
   * @brief save entry vector's size and content to the disk. the file name is entry_file_name.
   */
  void SaveEntryVec();

  /**
   * @brief Check if id has a value
   */
  bool CheckExist(unsigned int id);

  unsigned Read4Bytes(unsigned int id, unsigned offset);
};


#endif //GPSTORE_KVSTORE_DISKIV_DISKIV_H_