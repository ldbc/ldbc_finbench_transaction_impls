//
// Created by mst on 5/12/23.
//

#ifndef GPSTORE_VERSIONLIST_H
#define GPSTORE_VERSIONLIST_H

#include "Version.h"
#include "GraphLock.h"
#include "Transaction.h"


template <class T>
class VersionList
{
private:
    GLatch glatch;
    Latch rwLatch;

public:

    vector<shared_ptr<Version<T>>> vList;
    enum class LatchType { SHARED, EXCLUSIVE};

    typedef set<T> VDataSetz;
    typedef vector<T> VDataArrayz;
    typedef T VDataz;

    VersionList()
    {
        vList.reserve(5);
        shared_ptr<Version<T>> p = make_shared<Version<T>>(0, INVALID_ID);
        vList.push_back(p); //dummy version [0, INF)
    }
    VersionList(VersionList<T> const &other) {
//        vList = other.vList;
//        glatch = other.glatch;
//        rwLatch = std::move(other.rwLatch);
        vList.reserve(5);
        shared_ptr<Version<T>> p = make_shared<Version<T>>(0, INVALID_ID);
        vList.push_back(p); //dummy version [0, INF)
    }
    // VersionList& operator=(VersionList& v){

    // }
    // VersionList& operator=(VersionList&& v){

    // }
    // VersionList(VersionList& v){

    // }
    // VersionList(VersionList&& v){

    // }

    //get exclusive lock before update
    int GetExclusiveLatch(shared_ptr<Transaction> txn, bool has_read);
    bool InvalidExlusiveLatch(shared_ptr<Transaction> txn, bool has_read);

    bool UnLatch(shared_ptr<Transaction> txn, LatchType latch_type); //commit

    bool ReadVersion(unsigned& num, VDataSetz &AddSet, VDataSetz &DelSet, shared_ptr<Transaction> txn, bool &latched, bool first_read = false, bool get_num = false); //read
    int  WriteVersion(const VDataSetz &AddSet, const VDataSetz &DelSet, shared_ptr<Transaction> txn);

    void Print();

    ///@todo clean up
    void CleanAllVersion();


private:
    bool checkVersion(TYPE_TXN_ID TID, bool IS_RC);
    int checkheadVersion(TYPE_TXN_ID TID);
    void getProperVersion(TYPE_TXN_ID TID, unsigned& num, VDataSetz &addset, VDataSetz &delset, bool get_num);
    void getLatestVersion(TYPE_TXN_ID TID, unsigned& num, VDataSetz &addset, VDataSetz &delset, bool get_num);
};

template <class T>
void VersionList<T>::CleanAllVersion() {
    rwLatch.lockExclusive();
    vList.reserve(5);
    shared_ptr<Version<T>> p = make_shared<Version<T>>(0, INVALID_ID);
    vList.push_back(p); //dummy version [0, INF)
    rwLatch.unlock();
}

template <class T>
bool VersionList<T>::checkVersion(TYPE_TXN_ID TID, bool IS_RC)
{
    int n = vList.size();
    auto latest = vList[n-1];
    if(latest->get_begin_ts() == INVALID_TS ){
        if(latest->get_end_ts() == TID) return true; //owned lock
        else return false; // locked
    }
    if(!IS_RC && TID < latest->get_begin_ts()) return false; //old write
    return true;
}
template <class T>
int VersionList<T>::GetExclusiveLatch(shared_ptr<Transaction> txn, bool has_read)
{
    auto TID = txn->GetTID();
    shared_ptr<Version<T>> new_version = make_shared<Version<T>>(INVALID_TS, TID); //[-1, TID]

    rwLatch.lockExclusive();
    if(txn->GetIsolationLevelType() == IsolationLevelType::READ_COMMITTED)
    {
        //check the timestamp(lock)
        if(!checkVersion(TID, true)){
            rwLatch.unlock();
            return 0;
        }
    }
    else if(txn->GetIsolationLevelType() == IsolationLevelType::SNAPSHOT)
    {
        //check the timestamp(lock)
        if(!checkVersion(TID, false)){
            rwLatch.unlock();
            return 0;
        }
    }
    else if(txn->GetIsolationLevelType() == IsolationLevelType::SERIALIZABLE)
    {
        if(!checkVersion(TID, false)){
            rwLatch.unlock();
            return 0;
        }
        if(has_read){
            if(glatch.tryupgradelatch(TID) == false){
                rwLatch.unlock();
                return 0;
            }
        }
        else{
            //cerr << "get exclusive latch here" << endl;
            if(glatch.tryexclusivelatch(TID, true) == false){
                rwLatch.unlock();
                return 0;
            }
        }
    }
    else
    {
        cerr << "undefined IsolationLevelType!" << endl;
        rwLatch.unlock();
        return 0;
    }
    vList.push_back(new_version);
    //assert(glatch.get_TID() == TID);
    rwLatch.unlock();
    return 1;
}
template <class T>
bool VersionList<T>::InvalidExlusiveLatch(shared_ptr<Transaction> txn, bool has_read)
{
    bool IS_SR = txn->GetIsolationLevelType() == IsolationLevelType::SERIALIZABLE;
    auto TID = txn->GetTID();
    rwLatch.lockExclusive();
    if(IS_SR)
    {
        //SR only
        if(has_read)
        {
            if(glatch.trydowngradelatch(TID) == false) {
                assert(false);
                rwLatch.unlock();
                return false;
            }
        }
        else
        {
            if(glatch.unlatch(TID, true, IS_SR) == false) {
                cerr << "exclusive unlatch failed!" << endl;
                rwLatch.unlock();
                return false;
            }
        }
    }
    //assert(vList.back()->get_end_ts() == TID);// recheck
    vList.pop_back();
    rwLatch.unlock();
    return true;
}

template <class T>
bool VersionList<T>::UnLatch(shared_ptr<Transaction> txn, LatchType latch_type)
{
    bool IS_SR = (txn->GetIsolationLevelType() == IsolationLevelType::SERIALIZABLE);
    auto TID = txn->GetTID();
    if(IS_SR && latch_type == LatchType::SHARED)
    {
        bool ret = glatch.unlatch(TID, false, IS_SR);
        return ret;
    }
    else if(latch_type == LatchType::EXCLUSIVE)
    {
        auto cid = txn->GetCommitID();

        rwLatch.lockExclusive();
        int k = vList.size();
        if(IS_SR){
            if(glatch.unlatch(TID, true, IS_SR) == false){
                //assert(glatch.get_TID() == TID);
                rwLatch.unlock();
                return false;
            }
        }
        //update the version info(unlock)
        //assert(vList[k-1]->get_end_ts() == TID);
//        cout << "cid..................." << cid << endl;
        vList[k-1]->set_begin_ts(cid);
        vList[k-1]->set_end_ts(INVALID_TS);
        vList[k-2]->set_end_ts(cid);
        rwLatch.unlock();
        return true;
    }
    else
    {
        cerr << "error latch type !" << endl;
        return false;
    }
}

template <class T>
int VersionList<T>::checkheadVersion(TYPE_TXN_ID TID)
{
    int n = vList.size();
    int ret = 0;
    if((vList[n-1]->get_begin_ts() == INVALID_TS && TID > vList[n-1]->get_end_ts())) ret = -1; //try reading not owned uncommitted version, abort
    if(vList[n-1]->get_begin_ts() <= TID) ret = 1; //try aquiring the lock(maybe failed because new verison is not added in list yet)
    return ret;
}
template <class T>
void VersionList<T>::getProperVersion(TYPE_TXN_ID TID, unsigned& num, VDataSetz &addset, VDataSetz &delset, bool get_num)
{
    int n = vList.size();
    for(int k = 0; k < n; k++){
        if(TID < vList[k]->get_begin_ts()){
            break;
        }
        if(!get_num)
            vList[k]->get_version(addset, delset);
        else
            num += vList[k]->get_number();
    }
    if(vList[n-1]->get_begin_ts() == INVALID_TS && TID == vList[n-1]->get_end_ts()){
        if(!get_num)
            vList[n-1]->get_version(addset, delset); //read private version
        else
            vList[n-1]->get_number();
    }
}
template <class T>
void VersionList<T>::getLatestVersion(TYPE_TXN_ID TID, unsigned& num, VDataSetz &addset, VDataSetz &delset, bool get_num)
{
    rwLatch.lockShared();
    unsigned n = vList.size();
    for(unsigned i = 0; i < n - 1; i++)
    {
        if(!get_num)
            vList[i]->get_version(addset, delset);
        else
            vList[i]->get_number();
//        cout << "begin_ts" << vList[i]->get_begin_ts() << "  end_ts" << vList[i]->get_end_ts() << endl;
    }
    if((vList[n-1]->get_begin_ts() == INVALID_TS && vList[n-1]->get_end_ts() == TID) || (vList[n-1]->get_end_ts() == INVALID_TS)) //private version or committed version
    {
        if(!get_num)
            vList[n-1]->get_version(addset, delset);
        else
            vList[n-1]->get_number();
//        cout << "begin_ts" << vList[n-1]->get_begin_ts() << "  end_ts" << vList[n-1]->get_end_ts() << endl;
    }
    rwLatch.unlock();
}

template <class T>
bool VersionList<T>::ReadVersion(unsigned &num, VDataSetz &AddSet, VDataSetz &DelSet, shared_ptr<Transaction> txn, bool &latched, bool first_read, bool get_num)
{
    if(txn->GetIsolationLevelType() == IsolationLevelType::READ_COMMITTED)
    {
        getLatestVersion(txn->GetTID(), num, AddSet, DelSet, get_num); //get latest committed version or owned uncommitted version
    }
    else if (txn->GetIsolationLevelType() == IsolationLevelType::SNAPSHOT)
    {
        rwLatch.lockShared();
        getProperVersion(txn->GetTID(), num, AddSet, DelSet, get_num); //get version according to timestamp
        rwLatch.unlock();
    }
    else if (txn->GetIsolationLevelType() == IsolationLevelType::SERIALIZABLE)
    {
        rwLatch.lockShared();
        int ret = checkheadVersion(txn->GetTID());
        if(ret == -1){
            rwLatch.unlock();
            assert(latched == false);
            return false;
        }
        else if(ret == 1 && first_read){
            latched = glatch.trysharedlatch(txn->GetTID());
            if(!latched){
                rwLatch.unlock();
                assert(latched == false);
                return false;
            }
        }
        getProperVersion(txn->GetTID(), num, AddSet, DelSet, get_num);
        rwLatch.unlock();
    }
    else //not defined
    {
        assert(false);
    }
    return true;
}

template <class T>
int VersionList<T>::WriteVersion(const VDataSetz &AddSet, const VDataSetz &DelSet, shared_ptr<Transaction> txn)
{
    //private version no need any lock here
    rwLatch.lockShared();
    //assert(glatch.get_TID() == txn->GetTID());
    //assert(vList.size() != 0);
    for(auto it: AddSet)
        vList.back()->add(it);
    for(auto it: DelSet)
        vList.back()->remove(it);
    rwLatch.unlock();
    return 1;
}

template <class T>
void VersionList<T>::Print()
{
    int n = vList.size();
    cout << "has " << n << " version:\n";
    for(int i=0; i<n; i++)
    {
        cout << "   version" << i << ":\n";
        vList[i]->print_data();
//        auto addset = vList[i].get_add_set(), delset = vList[i].get_del_set();

    }
}


template <class T>
class CoverVersionList
{
private:
    GLatch glatch;
    Latch rwLatch;

public:

    vector<TYPE_TXN_ID> time_stamp;
    TYPE_TXN_ID _TID;
    vector<char*> datum;
    vector<unsigned> len;
    char* datum_own;
    unsigned len_own;
    enum class LatchType { SHARED, EXCLUSIVE};

    CoverVersionList() {_TID = INVALID_TS;  time_stamp.push_back(0);  datum.push_back(nullptr);  len.push_back(0);  datum_own = nullptr;}
    CoverVersionList(CoverVersionList<T> const &other) {_TID = INVALID_TS;  time_stamp.push_back(0);  datum.push_back(nullptr);  len.push_back(0);  datum_own = nullptr;}
    //get exclusive lock before update
    int GetExclusiveLatch(shared_ptr<Transaction> txn, bool has_read);
    bool InvalidExlusiveLatch(shared_ptr<Transaction> txn, bool has_read);

    bool UnLatch(shared_ptr<Transaction> txn, LatchType latch_type); //commit

    bool ReadVersion(char*& val, unsigned &val_len, shared_ptr<Transaction> txn, bool &latched, bool first_read = false ); //read
    int  WriteVersion(const char* val, unsigned val_len, shared_ptr<Transaction> txn);

    void Print();

    ///@todo clean up
    void CleanAllVersion();


private:
    bool checkVersion(TYPE_TXN_ID TID, bool IS_RC);
    int checkheadVersion(TYPE_TXN_ID TID);
    void getProperVersion(char*& val, unsigned &val_len, TYPE_TXN_ID TID);
    void getLatestVersion(char*& val, unsigned &val_len, TYPE_TXN_ID TID);
    void Copy(char*& val, unsigned &val_len, unsigned version) {
        if(version == -1) {
            val_len = len_own;
            val = new char[val_len];
            memcpy(val, datum_own, val_len);
        }

        else {
            assert(version < time_stamp.size());
            if(version == 0)return void(val = nullptr);
            val_len = len[version];
            val = new char[val_len];
            memcpy(val, datum[version], val_len);
        }
    }
};

template <class T>
void CoverVersionList<T>::CleanAllVersion() {
    rwLatch.lockExclusive();
    _TID = INVALID_TS;
    time_stamp.push_back(0);
    datum.push_back(nullptr);
    len.push_back(0);
    datum_own = nullptr;
    rwLatch.unlock();
}

template <class T>
bool CoverVersionList<T>::checkVersion(TYPE_TXN_ID TID, bool IS_RC) {
    if(_TID != INVALID_TS && _TID != TID) return false;  // other own lock
    if(!IS_RC && TID < (*time_stamp.end())) return false; //old write
    return true;
}

template <class T>
int CoverVersionList<T>::GetExclusiveLatch(shared_ptr<Transaction> txn, bool has_read)
{
    auto TID = txn->GetTID();

    rwLatch.lockExclusive();
    if(txn->GetIsolationLevelType() == IsolationLevelType::READ_COMMITTED)
    {
        //check the timestamp(lock)
        if(!checkVersion(TID, true)){
            rwLatch.unlock();
            return 0;
        }
    }
    else if(txn->GetIsolationLevelType() == IsolationLevelType::SNAPSHOT)
    {
        //check the timestamp(lock)
        if(!checkVersion(TID, false)){
            rwLatch.unlock();
            return 0;
        }
    }
    else if(txn->GetIsolationLevelType() == IsolationLevelType::SERIALIZABLE)
    {
        if(!checkVersion(TID, false)){
            rwLatch.unlock();
            return 0;
        }
        if(has_read){
            if(glatch.tryupgradelatch(TID) == false){
                rwLatch.unlock();
                return 0;
            }
        }
        else{
            if(glatch.tryexclusivelatch(TID, true) == false){
                rwLatch.unlock();
                return 0;
            }
        }
    }
    else
    {
        cerr << "undefined IsolationLevelType!" << endl;
        rwLatch.unlock();
        return 0;
    }
    _TID = TID;
    rwLatch.unlock();
    return 1;
}
template <class T>
bool CoverVersionList<T>::InvalidExlusiveLatch(shared_ptr<Transaction> txn, bool has_read)
{
    bool IS_SR = txn->GetIsolationLevelType() == IsolationLevelType::SERIALIZABLE;
    auto TID = txn->GetTID();
    rwLatch.lockExclusive();
    if(IS_SR)
    {
        //SR only
        if(has_read)
        {
            if(glatch.trydowngradelatch(TID) == false) {
                assert(false);
                rwLatch.unlock();
                return false;
            }
        }
        else
        {
            if(glatch.unlatch(TID, true, IS_SR) == false) {
                cerr << "exclusive unlatch failed!" << endl;
                rwLatch.unlock();
                return false;
            }
        }
    }
    //assert(vList.back()->get_end_ts() == TID);// recheck
    _TID = INVALID_TS;
    rwLatch.unlock();
    return true;
}

template <class T>
bool CoverVersionList<T>::UnLatch(shared_ptr<Transaction> txn, LatchType latch_type)
{
    bool IS_SR = (txn->GetIsolationLevelType() == IsolationLevelType::SERIALIZABLE);
    auto TID = txn->GetTID();
    if(IS_SR && latch_type == LatchType::SHARED)
    {
        bool ret = glatch.unlatch(TID, false, IS_SR);
        return ret;
    }
    else if(latch_type == LatchType::EXCLUSIVE)
    {
        auto cid = txn->GetCommitID();

        rwLatch.lockExclusive();
        if(IS_SR){
            if(glatch.unlatch(TID, true, IS_SR) == false){
                //assert(glatch.get_TID() == TID);
                rwLatch.unlock();
                return false;
            }
        }
        //update the version info(unlock)
        //assert(vList[k-1]->get_end_ts() == TID);
//        cout << "cid..................." << cid << endl;
        time_stamp.push_back(cid);
        datum.push_back(datum_own);
        len.push_back(len_own);
        datum_own = nullptr;
        rwLatch.unlock();
        _TID = INVALID_TS;
        return true;
    }
    else
    {
        cerr << "error latch type !" << endl;
        return false;
    }
}


template <class T>
int CoverVersionList<T>::checkheadVersion(TYPE_TXN_ID TID)
{
    int n = time_stamp.size();
    int ret = 0;
    if(_TID != INVALID_TS && TID > _TID) return -1; //try reading not owned uncommitted version, abort
    if(_TID != INVALID_TS && TID <= _TID) return 0; // read old-version
    if(time_stamp[n-1] <= TID) return 1; //try aquiring the lock(maybe failed because new verison is not added in list yet)
    return ret;
}
template <class T>
void CoverVersionList<T>::getProperVersion(char* &val, unsigned &val_len, TYPE_TXN_ID TID)
{
    if(_TID == TID)
        return void(Copy(val, val_len, -1));
    auto first = upper_bound(time_stamp.begin(), time_stamp.end(), TID);
    --first;
//    return void(Copy(val, val_len, datum.size()));
    return void(Copy(val, val_len, (int)(first - time_stamp.begin())));
}
template <class T>
void CoverVersionList<T>::getLatestVersion(char* &val, unsigned &val_len, TYPE_TXN_ID TID)
{
    rwLatch.lockShared();
    if(_TID == TID) {
        Copy(val, val_len, -1);
        rwLatch.unlock();
        return;
    }

    Copy(val, val_len, datum.size()-1);
    rwLatch.unlock();
}


template <class T>
bool CoverVersionList<T>::ReadVersion(char* &val, unsigned &val_len, shared_ptr<Transaction> txn, bool &latched, bool first_read)
{
    if(txn->GetIsolationLevelType() == IsolationLevelType::READ_COMMITTED)
    {
        getLatestVersion(val, val_len, txn->GetTID()); //get latest committed version or owned uncommitted version
    }
    else if (txn->GetIsolationLevelType() == IsolationLevelType::SNAPSHOT)
    {
        rwLatch.lockShared();
        getProperVersion(val, val_len, txn->GetTID()); //get version according to timestamp
        rwLatch.unlock();
    }
    else if (txn->GetIsolationLevelType() == IsolationLevelType::SERIALIZABLE)
    {
        rwLatch.lockShared();
        int ret = checkheadVersion(txn->GetTID());
        if(ret == -1){
            rwLatch.unlock();
            assert(latched == false);
            return false;
        }
        else if(ret == 1 && first_read) {
            latched = glatch.trysharedlatch(txn->GetTID());
            if(!latched){
                rwLatch.unlock();
                assert(latched == false);
                return false;
            }
        }
        getProperVersion(val, val_len, txn->GetTID());
        rwLatch.unlock();
    }
    else //not defined
    {
        assert(false);
    }
    return true;
}

template <class T>
int CoverVersionList<T>::WriteVersion(const char* val, unsigned val_len, shared_ptr<Transaction> txn)
{
    //private version no need any lock here
    rwLatch.lockShared();
    //assert(glatch.get_TID() == txn->GetTID());
    //assert(vList.size() != 0);
    len_own = val_len;
    if(datum_own != nullptr) {
        delete datum_own;
        datum_own = nullptr;
    }

    datum_own = new char[val_len];
    memcpy(datum_own, val, val_len);
    rwLatch.unlock();
    return 1;
}


#endif //GPSTORE_VERSIONLIST_H
