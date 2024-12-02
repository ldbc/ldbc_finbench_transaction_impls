/*=======================================================================
 * File name: MVCC_IV.h
 * Author:
 * Mail:
 * Last Modified:
 * Description:
 * =======================================================================*/
#ifndef INDEXVALUE_MVCC_IV_H
#define INDEXVALUE_MVCC_IV_H

#include "Util/Util.h"
#include "Util/Transaction.h"
#include "Util/Version.h"
#include "Util/SpinLock.h"
#include "../CacheManager/include/CacheManager.h"
#include "Util/VersionList.h"
#include "Util/Transaction.h"
#include "Util/Latch.h"

#include <map>

#define IVMAXKEYNUM 1 << 31
#define IVMIN(a, b) a < b ? a : b


typedef std::pair<unsigned,unsigned> VersionValueType;


class MVCC_IV
{
private:
    // sid oid when type == 0 1; pid when type == 2
    unsigned type;
	Transaction::IDType txn_type;
	unsigned T_len = sizeof(VersionValueType);

	std::vector<shared_ptr<VersionList<VersionValueType>>> versionLists;
	public:
	CacheManager *cacheManager;
	private:

	//the ownership of _tmp is transferred(_tmp will be deleted at last)
	//actual len of values is not always equal to values_len
	void Insert_so2values(const std::vector<unsigned>& _pidsoidlist, unsigned* _tmp,  unsigned long _len, unsigned*& values, unsigned long& values_len) const;
	/**
	 * @param _len number of unsigned in _tmp
	 * @param values_len number of unsigned in values
	*/
	void Insert_so2xvalues(const VXXDataSet& _pidsoidlist, unsigned xwidth, unsigned* _tmp,  unsigned long _len, unsigned*& values, unsigned long& values_len) const;
	void Remove_so2values(const std::vector<unsigned>& _pidsoidlist, unsigned* _tmp,  unsigned long _len, unsigned*& values, unsigned long& values_len) const;
	void Insert_p2values(const std::vector<unsigned>& _sidoidlist, unsigned* _tmp,  unsigned long _len, unsigned*& values, unsigned long& values_len) const;
	void Remove_p2values(const std::vector<unsigned>& _sidoidlist, unsigned* _tmp,  unsigned long _len, unsigned*& values, unsigned long& values_len) const;
	
	void merge(VDataSet &AddSet, VDataSet &DelSet, char *tmp, unsigned long len, char *&values, unsigned long &values_len);


	Latch versionListsLock;
	inline void VersionListsSharedLock(){ versionListsLock.lockShared();}
	inline void VersionListsExclusiveLock(){versionListsLock.lockExclusive();};
	inline void VersionListsUnlock(){versionListsLock.unlock();}

	/**
	 * @brief Get the Has Read object
	 * @details use Transaction's ReadSetFind, by this->type 
	 * @param _key 
	 * @param txn 
	 * @return true 
	 * @return false 
	 */
	bool GetHasRead(unsigned _key, shared_ptr<Transaction> txn){
		assert(txn != nullptr);
		return txn->ReadSetFind(_key, txn_type) || txn->WriteSetFind(_key, txn_type);
	}

public:
	MVCC_IV();
	MVCC_IV(unsigned _type, std::string _dir_path, std::string _filename, std::string mode, unsigned long long buffer_size, unsigned _key_num = 0);
	~MVCC_IV();
	enum class LatchType { SHARED, EXCLUSIVE};

	//flush
	bool Save();

	void PinCache(unsigned _key);
	bool RemoveKey(unsigned _key);


    bool ReadDeltaVersion(unsigned _key, char *&_str, unsigned long &_len, shared_ptr<Transaction> txn);
    bool WriteDeltaVersion(unsigned _key, VDataSet &AddSet, VDataSet &DelSet, shared_ptr<Transaction> txn);

    bool WriteXDeltaVersion(unsigned _key, VXXDataSet &AddSet, unsigned xwidth, VDataSet &DelSet, shared_ptr<Transaction> txn);
	bool UpdateXValues(TYPE_ENTITY_LITERAL_ID _src_id, TYPE_PREDICATE_ID _pid, TYPE_ENTITY_LITERAL_ID _dst_id, unsigned _xid, long long _xvalue, shared_ptr<Transaction> txn = nullptr);
	
	//lock(growing)
	bool TryExclusiveLatch(unsigned _key, shared_ptr<Transaction> txn);
	//unlock(commit)
	bool ReleaseLatch(unsigned _key, shared_ptr<Transaction> txn, LatchType type);
	
	//abort
	//clean invalid version(release exclusive latch along) and release exclusive lock
	bool Rollback(unsigned _key, shared_ptr<Transaction> txn);
	
	/**
	 * @brief remove the `_key` version list. 
	 * 
	 * @param _key 
	 * @return true 
	 * @return false version list doesn't exist.
	 */
	bool CleanDirtyKey(unsigned _key);

	// return pair number of given key and only use it while building database
	unsigned GetPairNum(unsigned _key);

	// return cache size of given key and only use it while building database
    unsigned GetListSize(unsigned _key);



};

#endif //INDEXVALUE_MVCC_IV_H
