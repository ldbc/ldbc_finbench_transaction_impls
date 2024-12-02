/*=======================================================================
 * File name: TxnIV.h
 * Author: Faye Wang
 * Mail: jchang6996@gmail.com
 * Last Modified: 2023-04-01
 * Description:
 * a Key-Value Index for ID-Value pair
 * =======================================================================*/
#ifndef GPSTORE_KVSTORE_TXNIV_TXNIV_H_
#define GPSTORE_KVSTORE_TXNIV_TXNIV_H_

#include "MVCC_IV.h"
#include "../../Util/Version.h"
#include "../../Util/Transaction.h"

class TxnIV {
	friend class KVstore;
protected:
	MVCC_IV* mvcc;
public:
	TxnIV(){mvcc = nullptr;}

	// sid/oid when type == 0/1; pid when type == 2
	TxnIV(unsigned _type, std::string _dir_path, std::string _filename, std::string _mode, unsigned long long _buffer_size, unsigned _key_num = 0);
	~TxnIV();

	//flush
	bool Save();

	void PinCache(unsigned _key);
	bool RemoveKey(unsigned _key);

	//read
	bool Search(unsigned _key, char* &_val, unsigned long &_len, shared_ptr<Transaction> txn = nullptr);
	//write
	bool Modify(unsigned _key, VDataSet &AddSet, VDataSet &DelSet, shared_ptr<Transaction> txn = nullptr);
	bool Modify(unsigned _key, VXXDataSet &AddSet, unsigned xwidth, VDataSet &DelSet, shared_ptr<Transaction> txn = nullptr);
	bool UpdateXValues(TYPE_ENTITY_LITERAL_ID _src_id, TYPE_PREDICATE_ID _pid, TYPE_ENTITY_LITERAL_ID _dst_id, unsigned _xid, long long _xvalue, shared_ptr<Transaction> txn = nullptr);

	//lock(growing)
	bool TryExclusiveLatch(unsigned _key, shared_ptr<Transaction> txn);
	//unlock(commit)
	bool ReleaseLatch(unsigned _key, shared_ptr<Transaction> txn, MVCC_IV::LatchType type);

	//abort
	//clean invalid version(release exclusive latch along) and release exclusive lock
	bool Rollback(unsigned _key, shared_ptr<Transaction> txn);

	//garbage clean; No Transaction should be running!
	bool CleanDirtyKey(unsigned _key) ;

	// return pair number of given key and only use it while building database
	unsigned long GetPairNum(unsigned _key);

	// return cache size of given key and only use it while building database
	unsigned long GetListSize(unsigned _key);

};

#endif //GPSTORE_KVSTORE_TXNIV_TXNIV_H_

