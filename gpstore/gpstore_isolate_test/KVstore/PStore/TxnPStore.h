/*=============================================================================
# Filename: TxnPStore.h
# Author: Faye Wang
# Mail: jchang6996@gmail.com
# Last Modified: 2023/04/27
# Description: a Key-Value Index for property
=============================================================================*/

#ifndef GPSTORE_TXNPSTORE_H
#define GPSTORE_TXNPSTORE_H

#include <string>
#include "../../Util/Transaction.h"
#include "MVCC_PS.h"

template<class T>
class TxnPStore {
public:
    /* get rocksdb cache info */
    void GetRocksdbCache(int& cache_miss, int& cache_hit);

    /* load/build file */
    TxnPStore(std::string _dir_path, std::string _name, PSTYPE _type, string detail_type, unsigned key_size = 0, 
              unsigned long long _buffer_size = 0, bool _get_value_num = 0, bool _is_range_index = 0, int _prefix_len = 0);
    ~TxnPStore() {delete mvcc;}

    /* notxn and cover read */
    bool ReadCoverVersion(const char *_key, unsigned _key_len, char *&_value, unsigned &_value_len, shared_ptr<Transaction> txn = nullptr);
    /* delta read */
    bool ReadDeltaVersion(const char *_key, unsigned _key_len, char *&_value, unsigned &_value_len, shared_ptr<Transaction> txn = nullptr);
    /* prefix read */
    bool ReadDeltaVersion_PrefixSearch(const char* _key_lower_bound, unsigned _klen_lower_bound, const char* _key_upper_bound, unsigned _klen_upper_bound, \
    std::vector<T> &_value, shared_ptr<Transaction> txn = nullptr);

    /* delta write */
    bool WriteDeltaVersion(const char *_key, unsigned _key_len, const typename MVCC_PS<T>::VDataSetz &AddSet, const typename MVCC_PS<T>::VDataSetz &DelSet, shared_ptr<Transaction> txn = nullptr);
    /* notxn and cover write */
    bool WriteCoverVersion(const char *_key, unsigned _key_len, const char *_value, unsigned _value_len, shared_ptr<Transaction> txn = nullptr);
    void Write2Batch(const char *key, unsigned key_len, const char *val, unsigned val_len) {
        mvcc->Write2Batch(key, key_len, val, val_len);
    }
    void BatchWrite(const std::vector<unsigned long long > &key_set, const std::vector<std::pair<char*, unsigned>> &val_set) {
        mvcc->BatchWrite(key_set, val_set);
    }
    void BatchFlush() {
        mvcc->BatchFlush();
    }
    bool getValueNumber(GetLengthType _type);
    bool getValueNumber(const char* key, unsigned key_len, unsigned &num, GetLengthType _type, shared_ptr<Transaction> txn = nullptr);

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

    /**
     * @brief Release lock and apply personal version when COMMIT
     *
     * @param txn Txn pointer calling COMMIT
     * @return success of failed
     */
    bool ReleaseAllLocks(shared_ptr<Transaction> txn = nullptr);

    /**
     * @brief Invalid lock and when ABORT
     *
     * @param txn Txn pointer calling ABORT
     * @return success of failed
     */
    bool RollBack(shared_ptr<Transaction> txn = nullptr);

    /**
     * @brief Clean version data when checkpoint
     *
     * @param PDirtyKey Dirtykey of all PStore
     * @return success of failed
     */
    bool Vacuum(vector<ProValSet> &PDirtyKey);

    /**
     * @brief clear all values, only use when transaction is closed.
     */
    void Clear() {
        mvcc->Clear();
    }

private:
    MVCC_PS<T> *mvcc;
};



#endif //GPSTORE_TXNPSTORE_H