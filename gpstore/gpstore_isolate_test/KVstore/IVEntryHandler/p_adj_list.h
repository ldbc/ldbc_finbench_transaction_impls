#ifndef P_ADJ_LIST_H
#define P_ADJ_LIST_H

#include <memory>
#include <vector>
#include <string.h>

class OXList;

class POXTable
{
public:
  std::vector<std::shared_ptr<OXList>> data_;

  void Insert(unsigned p, unsigned xwidth,
              const std::vector<std::pair<unsigned, const long long *>> &ox_list);
  void Delete(unsigned p, unsigned o);
  void Modify(unsigned p, unsigned o, unsigned x_id, long long new_x_val);
  void Get(unsigned p, unsigned *&o_list, long long *&xs_list, unsigned &len,
           unsigned &xwidth);
  void GetOList(unsigned p, unsigned *&o_list, unsigned &len);
  void Copy(const POXTable &old) {
    data_.resize(old.data_.size());
    for(int i= 0; i < data_.size(); i++) {
      data_[i] = old.data_[i];
    }
  }

  char *Serialize(unsigned &len);
  void Deserialize(const char *data, unsigned len);

  POXTable() {
//    data_.resize(p_number);
  }

 private:
  const int p_number = 20;
};

class OXList
{

public:
  unsigned p_;
  unsigned xwidth_;
  unsigned len_;
  unsigned *o_list_;
  long long *xs_list_;

//  OXList(unsigned _p, unsigned _xwidth, int static_num = 0) : p_(_p), xwidth_(_xwidth), len_(0) {
//    if(static_num) {
//      o_list_ = new unsigned[static_num];
//      xs_list_ = new long long[static_num * _xwidth];
//      capacity_ = static_num;
//    }
//    else {
//      o_list_ = new unsigned[initial_ox_number_];
//      xs_list_ = new long long[initial_ox_number_ * _xwidth];
//      capacity_ = initial_ox_number_;
//    }
//  }
  OXList(unsigned _p, unsigned _xwidth) : p_(_p), xwidth_(_xwidth), len_(0), o_list_(nullptr), xs_list_(nullptr) {}
  OXList() {}
  ~OXList() {
    if(o_list_ != nullptr) delete[] o_list_;
    if(xs_list_ != nullptr )delete[] xs_list_;
  }

  void CopyAndInsert(const std::vector<std::pair<unsigned, const long long *>> &ox_list, const std::shared_ptr<OXList>& _old = nullptr);
  void CopyAndDelete(unsigned o, const std::shared_ptr<OXList>& _old = nullptr);
  void CopyAndModify(const std::shared_ptr<OXList>& _old, unsigned o, unsigned x_id, long long new_x_val);

 private :

  /**
   * todo
   * Used for future rwlock
   */
  int capacity_;
  void Insert(const std::vector<std::pair<unsigned, const long long *>> &ox_list);
  void multiply() {
    capacity_ <<= 1;
    auto new_o_list = new unsigned[capacity_];
    auto new_xs_list = new long long[capacity_ * xwidth_];

    memcpy(new_o_list, o_list_, len_ * sizeof(unsigned));
    memcpy(new_xs_list, xs_list_, len_ * xwidth_ * sizeof(long long));

    delete[] o_list_;
    delete[] xs_list_;
  }
  /**
   * rwlock predefine end
   */



  void operator = (const OXList &old) {
    p_ = old.p_;
    xwidth_ = old.xwidth_;
    len_ = old.len_;

    //Used for future rwlock
    capacity_ = old.capacity_;
  }
  const int initial_ox_number_ = 100;
};

#endif 
