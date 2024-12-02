#ifndef IV_ARRAY_H
#define IV_ARRAY_H

#include <memory>
#include <shared_mutex>
#include <vector>
#include <future>

#include "../IVEntryHandler/p_adj_list.h"
#include "../DiskIV/DiskIV.h"

// class DiskIV {
//  public:
//   std::pair<char *, unsigned> ReadFromDisk(unsigned int id);
//   void WriteToDisk(unsigned int id, const char *str, unsigned int len);
// };

class POXTableWithLock {
 public:
  std::shared_ptr<POXTable> ptr_;
  std::shared_mutex mtx_;
  bool dirty_bit_;

  POXTableWithLock() : dirty_bit_(false) {}
};

class IVArray {
 public:
  std::shared_ptr<std::vector<POXTableWithLock>>
      data_;  // shared_ptr for expanding
  std::shared_mutex mtx_;
  std::unique_ptr<DiskIV> disk_iv_;

  IVArray(std::unique_ptr<DiskIV> disk_iv);
  ~IVArray();
  size_t getNodeNum() const { return data_->size(); }
  size_t getEdgeNum() const;
  void getp2edgeNum(std::unordered_map<unsigned, unsigned> &p2edgeNum);

  /**
   * interface for kvstore
   */
  void Set(unsigned s, const std::shared_ptr<POXTable>& pox_table);
  void GetOldPOXTable(unsigned s, std::shared_ptr<POXTable> &pox_table);
  void GetCopiedPOXTable(unsigned s, std::shared_ptr<POXTable> &pox_table);

  /**
   * interface for disk
   */
  void Flush();
  void Load(unsigned size, unsigned num_threads = 10);
};

#endif 