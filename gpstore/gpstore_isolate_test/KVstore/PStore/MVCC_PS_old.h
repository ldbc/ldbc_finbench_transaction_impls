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

class VersionHead
{

};


template <class T>
class Dstr{
private:
    char* val;
    unsigned len;
    T* ptr;
public:
    Dstr(char* _val, unsigned _len)
    {
        val = _val;
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

    MVCC_PS(string _dir_path, string _pstore_name, unsigned _buffer_size, PSTYPE _type, int detail_type, int _get_value_num = 0);

    ~MVCC_PS();

    /* delta get ptr */
    unsigned GetDeltaPtr(const char* _key, unsigned _key_len, char* &val, unsigned &_len, bool read_only, shared_ptr<Transaction> txn = nullptr, bool ptr_only = 0);
    /* notxn and cover get ptr */

    /* delta read */
    bool ReadDeltaVersion(const char* _key, unsigned _key_len, char* &val, unsigned &_len, bool &latched, shared_ptr<Transaction> txn = nullptr);
    /* notxn and cover read */
    bool ReadCoverVersion(const char* _key, unsigned _key_len, char* &val, unsigned &_len, bool &latched, shared_ptr<Transaction> txn = nullptr);


    /* delta write */
    bool WriteDeltaVersion(const char* _key, unsigned _key_len, VDataSetz &AddSet, VDataSetz &DelSet, shared_ptr<Transaction> txn = nullptr);
    /* notxn and cover write */
    bool WriteCoverVersion(const char* _key, unsigned _key_len, const char* val, unsigned &_len, shared_ptr<Transaction> txn = nullptr);

    bool getValueNumber(GetLengthType _type);
    bool getValueNumber(TYPE_LABEL_ID _labelID, unsigned &num, GetLengthType _type, shared_ptr<Transaction> txn = nullptr);

/* 	//MVCC
	//read
	bool search(unsigned _key, char *& _str, unsigned long & _len, VDataSet& AddSet, VDataSet& DelSet, shared_ptr<Transaction> txn, bool &latched, bool is_firstread = false);
	//write
	bool remove(unsigned _key, VDataSet& delta, shared_ptr<Transaction> txn);
	bool insert(unsigned _key, VDataSet& delta, shared_ptr<Transaction> txn); */

    /* support for read/write set */
    enum class Operation{ReadFind, ReadInsert, WriteFind, WriteInsert};
    bool ReadWriteSetOp(const char *key, Operation op, shared_ptr<Transaction> txn, int ptr = -1);


    /* transaction commit */
    bool ReleaseAllLocks(std::shared_ptr<Transaction> txn);


    /* lock for vector */
    void ArraySharedLock(){ VersionArrayLock.lockShared(); }
    void ArrayExclusiveLock(){ VersionArrayLock.lockExclusive(); };
    void ArrayUnlock(){VersionArrayLock.unlock();}

    //abort
    //clean invalid version(release exclusive latch along) and release exclusive lock
    bool Rollback(char* _key, shared_ptr<Transaction> txn);

    //garbage clean
    bool CleanDirtyKey(char* _key) ;

private:
/*  type = 1 notxn : property 2 id / id 2 property
	type = 2 cover : eid+propertyid 2 value+versionhead
	type = 3 delta : eid 2 spo+propertylist / spo 2 eidlist
*/

    const int use_ptr = 1;

    int T_len;
    PSTYPE type;

    /* alloc version head ptr */
    int head_ptr_num;
    mutex lock_ptr;

    /* lock for vector */
    Latch VersionArrayLock;


    /* check write set */
    int write_set_type;
    Transaction::IDType short_type;
    Transaction::ProType long_type;

    PStore* pstore;


    string pstore_name;
    string dir_path;

    unsigned MAX_BUFFER_SIZE;

    vector<VersionList<T>> v_array;

    bool get_value_num;
    std::map<unsigned, unsigned> value_num;
    std::unordered_map<string, unsigned> ptr_map;

    void merge(VDataSetz &AddSet, VDataSetz &DelSet, char *tmp, unsigned len, char* &values, unsigned &values_len);
};


template class Dstr<unsigned>;
template class MVCC_PS<unsigned>;
// for spo2eid
template class Dstr<unsigned long long>;
template class MVCC_PS<unsigned long long>;
//for others
template class Dstr<char>;
template class MVCC_PS<char>;


template<class T>
bool MVCC_PS<T>::getValueNumber(GetLengthType _type) {
    if(_type == LOAD) {
        pstore->getValueNumber(value_num, T_len);
        return true;
    }
    return false;
}

//todo: add txn_ptr
template<class T>
bool MVCC_PS<T>::getValueNumber(TYPE_LABEL_ID _labelID, unsigned &num, GetLengthType _type, shared_ptr<Transaction> txn) {
    num = 0;
    if(txn == nullptr) {
        auto it = value_num.find(_labelID);
        if(it != value_num.end()) {
            num = it->second;
        }
        return true;
    }

    unsigned tmp_len = 0;
    unsigned ptr = GetDeltaPtr(_key, _key_len, tmp, tmp_len, 0, txn);

    bool IS_SI = txn->GetIsolationLevelType() == IsolationLevelType::SERIALIZABLE;

    /* get first read */
    bool is_firstread = false;
    if(IS_SI)
        is_firstread = !ReadWriteSetOp(_key, Operation::ReadFind, txn, ptr) && !ReadWriteSetOp(_key, Operation::WriteFind, txn, ptr);

    /* read version */
    VDataSetz AddSet, DelSet;

    ArraySharedLock();
    bool ret = v_array[ptr].ReadVersion(AddSet, DelSet, txn, latched, is_firstread);
    ArrayUnlock();

    bool is_empty = AddSet.size() == 0 && DelSet.size() == 0;

    //read failed, query abort
    if(ret == false) {
        val = NULL;
        _len = 0;
//        txn->SetState(TransactionState::ABORTED);
        assert(latched == false);
        return false;
    }
    //special condition : empty
    if(is_empty && tmp_len == 4) {
        val = NULL;
        _len = 0;
        return true;
    }

    merge(AddSet, DelSet, tmp, tmp_len, val, _len);

    if(is_firstread)
        ReadWriteSetOp(_key, Operation::ReadInsert, txn, ptr);

    _len-=4;

    if(_len == 0 && val != nullptr) {
        delete val;
        val = nullptr;
    }

    return true;
}


template <class T>
bool MVCC_PS<T>::ReleaseAllLocks(std::shared_ptr<Transaction> txn)
{
    if(use_ptr) {
        auto &WriteSet = txn->Get_WriteSet();
        auto &Wset = WriteSet[(unsigned) Transaction::IDType::NOTXN];

        int ret1 = true;
        //shared Latch
        if (txn->GetIsolationLevelType() == IsolationLevelType::SERIALIZABLE) {
            auto &ReadSet = txn->Get_ReadSet();
            auto &Rset = ReadSet[(unsigned) Transaction::IDType::NOTXN
            ];
            //check the released key
            for (auto &it: Rset) {
                if (Wset.find(it) == Wset.end()) {
                    ArraySharedLock();
                    if (v_array[it].UnLatch(txn, VersionList<T>::LatchType::SHARED) == false) {
                        ret1 = false;//release shared latch
                    }
                    ArrayUnlock();
                }
            }
        }

        //get all key with exclusive latched
        //exclusive Latch

        bool ret2 = true;
        //release exclusive latch
        for (auto &it: Wset) {
            ArraySharedLock();
            if (v_array[it].UnLatch(txn, VersionList<T>::LatchType::EXCLUSIVE) == false) {
                ret2 = false;
            }
            ArrayUnlock();
        }
        assert(ret1 && ret2);
        return ret1 && ret2;
    }
    return 0;
}

template <class T>
bool MVCC_PS<T>::ReadWriteSetOp(const char *key, Operation op, shared_ptr<Transaction> txn, int ptr) {

    bool ret = false;

    /* use head ptr as id in read/write set */
    if(use_ptr) {
        switch(op) {
            case Operation::ReadFind :
                ret = txn->ReadSetFind(ptr, Transaction::IDType::NOTXN);
                break;
            case Operation::ReadInsert :
                txn->ReadSetInsert(ptr, Transaction::IDType::NOTXN);
                break;
            case Operation::WriteFind :
                ret = txn->WriteSetFind(ptr, Transaction::IDType::NOTXN);
                break;
            case Operation::WriteInsert :
                txn->WriteSetInsert(ptr, Transaction::IDType::NOTXN);
                break;
        }
        return ret;
    }


    if(!write_set_type) {
        TYPE_EDGE_ID id;
        if (short_type == Transaction::IDType::EDGE) {
            TYPE_EDGE_ID id2;
            memcpy(&id2, key, sizeof(TYPE_EDGE_ID));
            id = id2;
        }

        else {
            TYPE_ENTITY_LITERAL_ID id2;
            memcpy(&id2, key, sizeof(TYPE_ENTITY_LITERAL_ID));
            id = id2;
        }

        switch(op) {
            case Operation::ReadFind :
                ret = txn->ReadSetFind(id, short_type);
                break;
            case Operation::ReadInsert :
                txn->ReadSetInsert(id, short_type);
                break;
            case Operation::WriteFind :
                ret = txn->WriteSetFind(id, short_type);
                break;
            case Operation::WriteInsert :
                txn->WriteSetInsert(id, short_type);
                break;
        }
    }

    else {
        TYPE_EDGE_ID id1;
        TYPE_PROPERTY_ID id2;
        memcpy(&id1, key, sizeof(TYPE_EDGE_ID));
        memcpy(&id2, key + sizeof(TYPE_EDGE_ID), sizeof(TYPE_PROPERTY_ID));

        switch(op) {
            case Operation::ReadFind :
                ret = txn->EPReadSetFind(pair<TYPE_EDGE_ID, TYPE_PROPERTY_ID>(id1, id2), long_type);
                break;
            case Operation::ReadInsert :
                txn->EPReadSetInsert(pair<TYPE_EDGE_ID, TYPE_PROPERTY_ID>(id1, id2), long_type);
                break;
            case Operation::WriteFind :
                ret = txn->EPWriteSetFind(pair<TYPE_EDGE_ID, TYPE_PROPERTY_ID>(id1, id2), long_type);
                break;
            case Operation::WriteInsert :
                txn->EPWriteSetInsert(pair<TYPE_EDGE_ID, TYPE_PROPERTY_ID>(id1, id2), long_type);
                break;
        }
    }

    return ret;
}

template <class T>
MVCC_PS<T>::MVCC_PS(string _dir_path, string _pstore_name, unsigned _buffer_size, PSTYPE _type, int detail_type, int _get_value_num)
{
    dir_path = _dir_path;
    pstore_name = _pstore_name;
    type = _type;
    get_value_num = _get_value_num;

    if(type != NOTXN)
        v_array.reserve(initial_size);

    if(detail_type == 3 || detail_type == 4) {
        short_type = Transaction::IDType(detail_type);
        write_set_type = 0;
    }

    if(detail_type > 4) {
        long_type = Transaction::ProType(detail_type - 5);
        write_set_type = 1;
    }

    MAX_BUFFER_SIZE = _buffer_size;
    T_len = sizeof(T);

    Util::create_dir(dir_path+'/'+pstore_name);
    pstore = new PStore(dir_path, pstore_name, _buffer_size);
}

template <class T>
MVCC_PS<T>::~MVCC_PS()
{
    delete pstore;
}

template <class T>
void MVCC_PS<T>::merge(VDataSetz &AddSet, VDataSetz &DelSet, char *tmp, unsigned len, char *&values, unsigned &values_len)
{
    /// "len" mean length of char array, "n" mean number of element(eid/pid)
    int add_n = AddSet.size(), tmp_n, _values_n, _values_len;
    char* _values = nullptr;
    Dstr<T> Dtmp, Dvalues;

    unsigned offset = 0;
    if(pstore_name == "eid2values") {
        offset = sizeof(TYPE_ENTITY_LITERAL_ID) * 2 + sizeof(TYPE_PREDICATE_ID);
    }

    tmp_n = (len - offset - 4) / T_len;
    _values_n = tmp_n + add_n;
    _values_len = _values_n * T_len + offset;
    _values = new char[_values_len + 4];  //+4 for head_ptr
    std::memcpy(_values, tmp, offset);
    Dvalues = Dstr<T>(_values + offset, _values_len - offset);
    Dtmp = Dstr<T>(tmp + offset, len - offset);

    int v_offset = 0;

    auto add_it = AddSet.begin(), del_it = DelSet.begin();
    int tmp_offset = 0;

    bool del_end = DelSet.empty();


    /// Lambda for has eid/pid been delete
    auto exedel = [&](T x)
    {
        if(!del_end && x == *del_it)
        {
            del_it ++;
            del_end = (del_it == DelSet.end());
            _values_n --;
        }
        else Dvalues[v_offset++]  = x;
    };

    while(tmp_offset < tmp_n && add_it != AddSet.end())
    {
        T xt = Dtmp[tmp_offset], xa = *add_it;
        T x = std::min(xt, xa);

        exedel(x);

        if(xt == x)tmp_offset ++;
        if(xa == x)add_it ++;

        _values_n -= (xt == xa);
    }

    while(tmp_offset < tmp_n)
    {
        exedel(Dtmp[tmp_offset]);
        tmp_offset ++;
    }

    while(add_it != AddSet.end())
    {
        exedel(*add_it);
        add_it ++;
    }

    values_len = _values_n * T_len + offset + 4;
    memcpy(_values + values_len - 4, tmp + len - 4, 4);

    values = _values;
}


template <class T>
unsigned MVCC_PS<T>::GetDeltaPtr(const char* _key, unsigned _key_len, char* &val, unsigned &_len, bool read_only, shared_ptr<Transaction> txn, bool ptr_only)
{
    pstore->getvalue(_key, val, _len, _key_len);
    int head_ptr = 0;

    /* don't need insert head_ptr */
    if(read_only) {
        if(_len != 0) {
            memcpy(&head_ptr, val + _len - 4, 4);
        }
        return head_ptr;
    }

    if(_len) {
        memcpy(&head_ptr, val + _len - 4, 4);
        if(head_ptr) return head_ptr
    }

    /* alloc ptr */
    {
        lock_ptr.lock();

        pstore->getvalue(_key, val, _len, _key_len);

        /* head_ptr write by other threads && has ptr*/
        if(_len) {
            memcpy(&head_ptr, val + _len - 4, 4);
            if(head_ptr) {
                lock_ptr.unlock();
                return head_ptr;
            }
        }

        /* alloc new head ptr id */
        int ret = -1;
        if(txn != nullptr) {
            ret = head_ptr_num++;

            if(v_array.size() == v_array.capacity()) {
                ArrayExclusiveLock();
//                cout << "resize from " << v_array.capacity();
                v_array.push_back(VersionList<T>());
//                cout << " to " << v_array.capacity() << endl;
                ArrayUnlock();
            }
            else {
                v_array.push_back(VersionList<T>());
            }

        }

        val = new char[4];
        _len = 4;
        memcpy(val, &ret, 4);
        pstore->setvalue(_key, val, _len, _key_len);
        lock_ptr.unlock();
        return ret;
    }

    memcpy(&head_ptr, val + _len - 4, 4);
    return head_ptr;
}

template <class T>
bool MVCC_PS<T>::ReadDeltaVersion(const char* _key, unsigned _key_len, char* &val, unsigned &_len, bool &latched, shared_ptr<Transaction> txn)
{
    if(txn == nullptr)
    {
        GetDeltaPtr(_key, _key_len, val, _len, 1, txn);
//        cout << "read " << pstore_name << " " << _len << endl;
        if(_len)_len -= 4;
        if(_len == 0 && val != nullptr) {
            delete val;
            val = nullptr;
        }
        return true;
    }

    char *tmp;
    unsigned tmp_len = 0;
    unsigned ptr = GetDeltaPtr(_key, _key_len, tmp, tmp_len, 0, txn);

    bool IS_SI = txn->GetIsolationLevelType() == IsolationLevelType::SERIALIZABLE;

    /* get first read */
    bool is_firstread = false;
    if(IS_SI)
        is_firstread = !ReadWriteSetOp(_key, Operation::ReadFind, txn, ptr) && !ReadWriteSetOp(_key, Operation::WriteFind, txn, ptr);

    /* read version */
    VDataSetz AddSet, DelSet;

    ArraySharedLock();
    bool ret = v_array[ptr].ReadVersion(AddSet, DelSet, txn, latched, is_firstread);
    ArrayUnlock();

    bool is_empty = AddSet.size() == 0 && DelSet.size() == 0;

    //read failed, query abort
    if(ret == false) {
        val = NULL;
        _len = 0;
//        txn->SetState(TransactionState::ABORTED);
        assert(latched == false);
        return false;
    }
    //special condition : empty
    if(is_empty && tmp_len == 4) {
        val = NULL;
        _len = 0;
        return true;
    }

    merge(AddSet, DelSet, tmp, tmp_len, val, _len);

    if(is_firstread)
        ReadWriteSetOp(_key, Operation::ReadInsert, txn, ptr);

    _len-=4;

    if(_len == 0 && val != nullptr) {
        delete val;
        val = nullptr;
    }

    return true;
}


template <class T>
bool MVCC_PS<T>::ReadCoverVersion(const char* _key, unsigned _key_len, char* &val, unsigned &_len, bool &latched, shared_ptr<Transaction> txn)
{
    if(txn == nullptr)
    {
        GetDeltaPtr(_key, _key_len, val, _len, 1, txn);
        /// @bug
        if(_len)_len -= 4;
        if(_len == 0 && val != nullptr) {
            delete val;
            val = nullptr;
        }
        return true;
    }
    return false;
}



template <class T>
bool MVCC_PS<T>::WriteDeltaVersion(const char* _key, unsigned _key_len, VDataSetz &AddSet, VDataSetz &DelSet, shared_ptr<Transaction> txn)
{
    if(txn == nullptr)
    {

        if(get_value_num) {
            int del = AddSet.size() - DelSet.size();
            if(value_num.find(*(unsigned*)_key) == value_num.end()) {
                value_num[*(unsigned*)_key] = del;
            }
            else value_num[*(unsigned*)_key] += del;
        }
        char* tmp;
        char* values;
        unsigned tmp_len = 0, values_len = 0;

        GetDeltaPtr(_key, _key_len, tmp, tmp_len, 0, txn);

        merge(AddSet, DelSet, tmp, tmp_len, values, values_len);

//        cout << "write " << pstore_name << " " << values_len << " " << AddSet.size() << " " << DelSet.size() << endl;

        //std::cout << values_len << " " << "after merge : ";
        //for(int j = 0; j < values_len; j++)printf("%d ", values[j]);
        //std::cout << std::endl;

        bool ret = pstore->setvalue(_key, values, values_len, _key_len);

        delete [] tmp;
        delete [] values;

        return ret;
    }

    char *tmp;
    unsigned tmp_len = 0;
    unsigned ptr = GetDeltaPtr(_key, _key_len, tmp, tmp_len, 0, txn);

    bool IS_SI = txn->GetIsolationLevelType() == IsolationLevelType::SERIALIZABLE;

    /* get first read */
    bool has_read = false;
    if(IS_SI)
        has_read = ReadWriteSetOp(_key, Operation::ReadFind, txn, ptr);


    /* get write lock */
    if(!ReadWriteSetOp(_key, Operation::WriteFind, txn, ptr)) {
        ArraySharedLock();
        bool ret = v_array[ptr].GetExclusiveLatch(txn, has_read);
        if (!ret) {
            ArrayUnlock();
            return false;
        }
    }
    else ArraySharedLock();

    /* write lock */
    v_array[ptr].WriteVersion(AddSet, DelSet, txn);
    ArrayUnlock();

    ReadWriteSetOp(_key, Operation::WriteInsert, txn, ptr);

    return true;
}

template <class T>
bool MVCC_PS<T>::WriteCoverVersion(const char* _key, unsigned _key_len, const char* val, unsigned &_len, shared_ptr<Transaction> txn)
{
    /* Lambda for cat val and head_ptr */
    auto CatHeadPtr = [&](const char* val, unsigned &_len, char* &val_with_ptr, unsigned &_len_with_ptr, int head_ptr)
    {
        _len_with_ptr = _len + 4;
        val_with_ptr = new char[_len_with_ptr];

        memcpy(val_with_ptr, val, _len);

        /* set head ptr*/
        memcpy(val_with_ptr + _len, &head_ptr, 4);
    };



    if(txn == nullptr)
    {
//        unsigned int _len_with_ptr = _len;
//        if(type != PSTYPE::DELTA)
//            _len_with_ptr += 4;
//        char* val_with_ptr = new char[_len_with_ptr];
//
//        /* set head ptr = -1 */
//        memcpy(val_with_ptr, val, _len);
//        if(type != PSTYPE::DELTA)
//            memset(val_with_ptr + _len, 0xff, 4);

        unsigned _len_with_ptr;
        char* val_with_ptr;

        CatHeadPtr(val, _len, val_with_ptr, _len_with_ptr, -1);
        bool ret = pstore->setvalue(_key, val_with_ptr, _len_with_ptr, _key_len);

        delete [] val_with_ptr;
        return ret;
    }
    return false;
}