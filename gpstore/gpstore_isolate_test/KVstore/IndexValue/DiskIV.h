/*=======================================================================
 * File name: IVArray.h
 * Author: Zongyue Qin
 * Mail: qinzongyue@pku.edu.cn
 * Last Modified: 2018-02-09
 * Description:
 * a Key-Value Index for ID-Value pair in form of Array
 * =======================================================================*/

#include "../../Util/Util.h"
#include "../../Util/SpinLock.h"
#include "IVEntry.h"
#include "../CacheManager/CacheManager.h"

using namespace std;

#define IVMAXKEYNUM 1 << 31
#define IVMIN(a, b) a < b ? a : b

class IVArray
{
private:

/// 由LRU决定换入换出
/* 	// stores at most 1 billion keys
	static const unsigned int MAX_KEY_NUM = 1 << 31;
	static const unsigned int SET_KEY_NUM = 1 << 10; // minimum initial keys num
	static const unsigned int SET_KEY_INC = SET_KEY_NUM; // minimum keys num inc
	static const unsigned int SEG_LEN = 1 << 8; 
	unsigned long long MAX_CACHE_SIZE; */
/// end

private:

//todo IVEntry -> 偏移量数组
	unsigned* store;
	// IVEntry* array;
/// LRU相关属性
	// IVEntry *cache_head;
	// int cache_tail_id;
/// end
	FILE* IVfile; // file that records index-store
	string IVfile_name;
	string filename;
	string dir_path;
	IVBlockManager *BM;
	unsigned CurEntryNum; // how many entries are available
	bool CurEntryNumChange;

/// 由LRU决定换入换出
/* 	//Cache 
	unsigned CurCacheSize;
	//map <unsigned, long> index_time_map;
	//multimap <long, unsigned> time_index_map;

	bool AddInCache(unsigned _key, char *_str, unsigned long _len);
	bool SwapOut();
	bool UpdateTime(unsigned _key, bool HasLock = false);

	void RemoveFromLRUQueue(unsigned _key); */
/// end
	
	//mutex CacheLock;

/// 由LRU层负责并发
//todo 存疑
	// spinlock CacheLock;
/// end
	Latch ArrayLock;
	//spinrwlock ArrayLock;
	inline void ArraySharedLock(){ ArrayLock.lockShared();}
	inline void ArrayExclusiveLock(){ArrayLock.lockExclusive();};
	inline void ArrayUnlock(){ArrayLock.unlock();}
	//inline void ArrayUnlock(){ArrayLock.unlock(false);}
	//inline void ArraySharedUnLock(){ArrayLock.unlock(false);}
	//inline void ArrayExclusiveUnLock(){ArrayLock.unlock(true);}
public:
	IVArray();
	IVArray(string _dir_path, string _filename, string mode, unsigned long long buffer_size, unsigned _key_num = 0);
	~IVArray();

//todo 实现叶老师的需要的接口
	unsigned int Getlen(unsigned int id);
	void Set(unsigned int id, const char *_str, const unsigned long _len);
	bool Get(unsigned int id, char *_str);
	bool Delete(unsigned int id);

/// 挪到txnIVArray
/* 	bool search(unsigned _key, char *& _str, unsigned long & _len);
	bool modify(unsigned _key, char *_str, unsigned long _len);
	bool remove(unsigned _key);
	bool insert(unsigned _key, char *_str, unsigned long _len);
	bool save();
	void PinCache(unsigned _key);
	
	//MVCC
	//read 
	bool search(unsigned _key, char *& _str, unsigned long & _len, VDataSet& AddSet, VDataSet& DelSet, shared_ptr<Transaction> txn, bool &latched, bool is_firstread = false);
	//write
	bool remove(unsigned _key, VDataSet& delta, shared_ptr<Transaction> txn);
	bool insert(unsigned _key, VDataSet& delta, shared_ptr<Transaction> txn); */
/// end

/// 挪到MVCC_IVArray层
	//lock(growing)
/* 	int TryExclusiveLatch(unsigned _key, shared_ptr<Transaction> txn, bool has_read = false);
	//unlock(commit)
	bool ReleaseLatch(unsigned _key, shared_ptr<Transaction> txn, IVEntry::LatchType type);
	
	//abort
	//clean invalid version(release exclusive latch along) and release exclusive lock
	bool Rollback(unsigned _key, shared_ptr<Transaction> txn, bool has_read );
	
	//garbage clean
	bool CleanDirtyKey(unsigned _key) ; */
/// end
};
