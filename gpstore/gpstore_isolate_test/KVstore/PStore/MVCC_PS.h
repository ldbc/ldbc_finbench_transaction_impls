/*=======================================================================
 * File name: MVCC_PS.h
 * Author:
 * Mail:
 * Last Modified:
 * Description:
 * =======================================================================*/


#pragma once

#include <iostream>

#include "../../Util/Util.h"
#include "../../Util/SpinLock.h"
#include "../../Util/VersionList.h"
#include "PStore.h"

#define initial_size 10000

enum PSTYPE {NOTXN, DELTA, COVER};
enum GetLengthType {LOAD, RUN};


template <class T>
class Dstr{
private:
    char* val;
    unsigned len;
    T* ptr;
public:
    Dstr(const char* _val, unsigned _len)
    {
        val = (char*)_val;
        len = _len;
        ptr = (T*)val;
    }

    Dstr(){}

    Dstr& operator=(Dstr && dstr)
    {
        len = dstr.len;
        val = dstr.val;
        dstr.val = nullptr;
        ptr = dstr.ptr;
        dstr.ptr = nullptr;
        return *this;
    }

    T & operator [] (int id)
    {
        return *(ptr+id);
    }
};


template <class T>
class MVCC_PS
{
public:
    typedef std::set<T> VDataSetz;


    void GetRocksdbCache(int& cache_miss, int& cache_hit);

    /**
     * @brief Construction of MVCC_PS
     * @param _dir_path Path of file to save
     * @param _pstore_name Name of index
     * @param _buffer_size Buffer size in memory, not used in MVCC_PS
     * @param _type Multiple version type, NOTXN/DELTA/COVER
     * @param detail_type Specific type of Index, shift from string using map
     * @param _get_value_num Does this index need to calculate element number? need correct update and only used in DELTA type
     * @param _is_range_index Does this index need to range seek? need correct update and only used in DELTA type
     * @param _prefix_len Constrain that length of prefix must to be same when range seek.
     */
    MVCC_PS(string _dir_path, string _pstore_name, unsigned _buffer_size, PSTYPE _type,
            string detail_type, unsigned key_size = 0, int _get_value_num = 0, bool _is_range_index = 0, int _prefix_len = 0);

    ~MVCC_PS();

    /* delta get ptr */
    VersionList<T>* GetDeltaPtr(string key, bool read_only, shared_ptr<Transaction> txn = nullptr);
    CoverVersionList<T>* CGetDeltaPtr(string key, bool read_only, shared_ptr<Transaction> txn = nullptr);
    /* notxn and cover get ptr */

    /* delta read */
    bool ReadDeltaVersion(const char* _key, unsigned _key_len, char* &val, unsigned &_len,
                          bool &latched, shared_ptr<Transaction> txn = nullptr, const char* tmp = nullptr, unsigned tmp_len = 0);
    /* notxn and cover read */
    bool ReadCoverVersion(const char* _key, unsigned _key_len, char* &val, unsigned &_len, bool &latched, shared_ptr<Transaction> txn = nullptr);

    /**
     * @brief Range Seek
     *
     * @param _key_lower_bound Min key of range
     * @param _klen_lower_bound Length of min key
     * @param _key_upper_bound Max key of range
     * @param _klen_upper_bound Length of max key
     * @param _value vector of answer
     * @param txn transaction pointer
     * @return success of failed maybe caused by ABORT or DATA corruption
     */
    bool ReadDeltaVersion_PrefixSearch(const char* _key_lower_bound, unsigned _klen_lower_bound,
                                       const char* _key_upper_bound, unsigned _klen_upper_bound, std::vector<T> &_value, shared_ptr<Transaction> txn = nullptr);

    /**
     * @brief Get Statistical information about min/max key for range_seek_index
     *
     * @param _key_lower_bound Min key of range
     * @param _klen_lower_bound Length of min key
     * @param _key_upper_bound Max key of range
     * @param _klen_upper_bound Length of max key
     * @param min_key Min key exist, nullptr means no key
     * @param minl Length of exist min key
     * @param max_key Max key exist, nullptr means no key
     * @param maxl Length of exist max key
     *
     * @return success of failed maybe caused by ABORT or DATA corruption
     */
    bool GetExtremeValue(const char* _key_lower_bound, unsigned _klen_lower_bound, const char* _key_upper_bound, unsigned _klen_upper_bound,
                         const char* &min_key, size_t &minl, const char* &max_key, size_t &maxl);

    /* delta write */
    bool WriteDeltaVersion(const char* _key, unsigned _key_len, const VDataSetz &AddSet, const VDataSetz &DelSet, shared_ptr<Transaction> txn = nullptr);
    /* notxn and cover write */
    bool WriteCoverVersion(const char* _key, unsigned _key_len, const char* val, unsigned _len, shared_ptr<Transaction> txn = nullptr);
    /* notxn batch write */
    void Write2Batch(const char *key, unsigned key_len, const char *val, unsigned val_len) {
        pstore->Write2Batch(key, key_len, val, val_len);
    }
    void BatchWrite(const std::vector<unsigned long long > &key_set, const std::vector<std::pair<char*, unsigned>> &val_set) {
        pstore->setvalue(key_set, val_set);
    }
    void BatchFlush() {
        pstore->BatchFlush();
    }

    bool getValueNumber(GetLengthType _type);
    bool getValueNumber(const char* key, unsigned key_len, unsigned &num, GetLengthType _type, shared_ptr<Transaction> txn = nullptr);

/* 	//MVCC
	//read
	bool search(unsigned _key, char *& _str, unsigned long & _len, VDataSet& AddSet, VDataSet& DelSet, shared_ptr<Transaction> txn, bool &latched, bool is_firstread = false);
	//write
	bool remove(unsigned _key, VDataSet& delta, shared_ptr<Transaction> txn);
	bool insert(unsigned _key, VDataSet& delta, shared_ptr<Transaction> txn); */

    /* support for read/write set */
    enum class Operation{ReadFind, ReadInsert, WriteFind, WriteInsert};
    bool ReadWriteSetOp(string &key, Operation op, shared_ptr<Transaction> txn, int ptr = -1);

    /**
     * @brief Release lock and apply personal version when COMMIT
     * @param txn Txn pointer calling COMMIT
     * @return success of failed
     */
    bool ReleaseAllLocks(std::shared_ptr<Transaction> txn);


    /* lock for vector */
    void ArraySharedLock(){ VersionArrayLock.lockShared(); }
    void ArrayExclusiveLock(){ VersionArrayLock.lockExclusive(); };
    void ArrayUnlock(){VersionArrayLock.unlock();}

    //abort
    //clean invalid version(release exclusive latch along) and release exclusive lock
    bool RollBack(shared_ptr<Transaction> txn);

    //garbage clean

    /**
     * @brief Clean version data and reset base_version for all key in this MVCC_PS when checkpoint
     *
     * @param PDirtyKey Dirtykey of all PStore, get own by individual
     * @return success of failed
     */
    bool Vacuum(vector<ProValSet> &PDirtyKey);

    /**
     * @brief Clean version data and reset base_version for specific key in this MVCC_PS when checkpoint
     *
     * @param _key Dirtykey
     * @param txn Clean transaction
     * @return success of failed
     */
    bool CleanDirtyKey(string _key, shared_ptr<Transaction> txn = nullptr);


    /**
     * @brief clear all values, only use when transaction is closed.
     */
    void Clear() {
        pstore->Clear();
    }

private:
/*  type = 1 notxn : property 2 id / id 2 property
	type = 2 cover : eid+propertyid 2 value+versionhead
	type = 3 delta : eid 2 spo+propertylist / spo 2 eidlist
*/

    const int use_ptr = 0;

    int T_len;
    PSTYPE type;

    /* alloc version head ptr */
    int head_ptr_num;
    mutex lock_ptr;

    /* lock for vector */
    Latch VersionArrayLock;


    /* check write set */
    string write_set_type;
    int type_int;
    int individual;

    PStore* pstore;


    string pstore_name;
    string dir_path;

    bool is_range_index;
    int prefix_len;

    unsigned MAX_BUFFER_SIZE;

    vector<VersionList<T>> v_array;
    vector<CoverVersionList<T>> cv_array;

    bool get_value_num;
    std::unordered_map<string, unsigned> value_num;
    std::unordered_map<string, VersionList<T>*> ptr_map;
    std::unordered_map<string, CoverVersionList<T>*> cptr_map;

    void merge(const VDataSetz &AddSet, const VDataSetz &DelSet, const char *tmp, unsigned len, char* &values, unsigned &values_len);
};
