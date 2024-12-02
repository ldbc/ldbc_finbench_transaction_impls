/*=============================================================================
# Filename:		KVstore.h
# Author: Bookug Lobert
# Mail: 1181955272@qq.com
# Last Modified:	2018-02-09 14:23
# Description: Modified by Wang Libo
=============================================================================*/

#ifndef _KVSTORE_KVSTORE_H
#define _KVSTORE_KVSTORE_H

#include "../Util/Util.h"
#include "../Util/VList.h"
#include "Tree.h"
// #include "../Trie/Trie.h"
#include "ISArray/ISArray.h"
#include "PStore/TxnPStore.h"
#include "../Util/Value.h"
#include "IndexValue/TxnIV.h"
#include "IVArray/iv_array.h"

//TODO: is it needed to keep a length in Bstr?? especially for IVTree?
//add a length: sizeof bstr from 8 to 16(4 -> 8 for alignment)
//add a \0 in tail: only add 1 char
//QUERY: but to count the length each time maybe very costly?
//No, because triple num is stored in char* now!!!! we do not need to save it again
//
//QUERY: but to implement vlist, we need a unsigned flag
//What is more, we need to store the string in disk, how can we store it if without the length? 
//unsigned type stored as chars, maybe will have '\0'
//In memory, we do not know when the oidlist ends if without the original length (butthe triple num will answer this!)
//
//TODO: entity_border in s2values list is not needed!!! not waste memory here

//STRUCT:
//1. s2xx
//Triple Num   Pre Num   Entity Num   p1 offset1  p2 offset2  ...  pn offsetn (olist-p1) (olist-p2) ... (olist-pn)
//2. o2xx
//Triple Num   Pre Num   p1 offset1  p2 offset2 ... pn offsetn (slist-p1) (slist-p2) ... (slist-pn)
//3. p2xx
//Triple Num   (sid list)  (oid list) (not sorted, matched with sid one by one)
class PreProcess;

class KVstore
{
public:
	static const int READ_WRITE_MODE = 1;	//Open a B tree, which must exist
	static const int CREATE_MODE = 2;		//Build a new B tree and delete existing ones (if any)


    void GetRocksdbCache(int& cache_miss, int& cache_hit);

	//===============================================================================


	KVstore(std::string _store_path = ".");
	~KVstore();
	void flush();
	void release();
	void open(int _mode);

	std::string getStringByID(TYPE_ENTITY_LITERAL_ID _id) const;
	TYPE_ENTITY_LITERAL_ID getIDByString(std::string _str);

	// functions to load Vlist's cache
	// NOTICE: only use it when building database
	void AddIntoSubCache(TYPE_ENTITY_LITERAL_ID _entity_id);
	void AddIntoObjCache(TYPE_ENTITY_LITERAL_ID _entity_literal_id);
	void AddIntoPreCache(TYPE_PREDICATE_ID _pre_id);
    int PinSPOXCache(TYPE_ENTITY_LITERAL_ID id);
	unsigned long GetPreListSize(TYPE_PREDICATE_ID _pre_id);
	unsigned long GetSubListSize(TYPE_ENTITY_LITERAL_ID _sub_id);
	unsigned long GetObjListSize(TYPE_ENTITY_LITERAL_ID _obj_id);
	unsigned long GetPrePairNum(TYPE_PREDICATE_ID _pre_id);
	unsigned long GetSubPairNum(TYPE_ENTITY_LITERAL_ID _sub_id);
	unsigned long GetObjPairNum(TYPE_ENTITY_LITERAL_ID _obj_id);

	// NOTICE: use it after building database
	unsigned long getPreListSize(TYPE_PREDICATE_ID _pre_id);
	unsigned long getSubListSize(TYPE_ENTITY_LITERAL_ID _sub_id);
	unsigned long getObjListSize(TYPE_ENTITY_LITERAL_ID _obj_id);

	bool existThisTriple(TYPE_ENTITY_LITERAL_ID _sub_id, TYPE_PREDICATE_ID _pre_id, TYPE_ENTITY_LITERAL_ID _obj_id);


		//===============================================================================

	//including IN-neighbor & OUT-neighbor
	unsigned getEntityDegree(TYPE_ENTITY_LITERAL_ID _entity_id) const;
	unsigned getEntityInDegree(TYPE_ENTITY_LITERAL_ID _entity_id) const;
	unsigned getEntityOutDegree(TYPE_ENTITY_LITERAL_ID _entity_id) const;

	unsigned getLiteralDegree(TYPE_ENTITY_LITERAL_ID _literal_id) const;
	unsigned getPredicateDegree(TYPE_PREDICATE_ID _predicate_id) const;

	unsigned getSubjectPredicateDegree(TYPE_ENTITY_LITERAL_ID _subid, TYPE_PREDICATE_ID _preid) const;
	unsigned getObjectPredicateDegree(TYPE_ENTITY_LITERAL_ID _objid, TYPE_PREDICATE_ID _preid) const;

	//===============================================================================
	//Before calling these functions, we are sure that the triples doesn't exist.
	//batch update(no implementation)
	bool updateTupleslist_insert(const vector<ID_TUPLE> &Triples, shared_ptr<Transaction> txn);

	bool updateTupleXslist_insert(const vector<ID_XX_TUPLE> &Triples, unsigned xwidth, shared_ptr<Transaction> txn);/* for sub/objID2xvalues [SPOX]*/

	bool updateTupleslist_remove(vector<ID_TUPLE> &Triples, shared_ptr<Transaction> txn);

	//basic update
	bool updateInsert_s2values(TYPE_ENTITY_LITERAL_ID _sub_id, TYPE_PREDICATE_ID _pre_id, TYPE_ENTITY_LITERAL_ID _obj_id, shared_ptr<Transaction> txn = nullptr);
	bool updateRemove_s2values(TYPE_ENTITY_LITERAL_ID _sub_id, TYPE_PREDICATE_ID _pre_id, TYPE_ENTITY_LITERAL_ID _obj_id, shared_ptr<Transaction> txn = nullptr);
	bool updateInsert_o2values(TYPE_ENTITY_LITERAL_ID _sub_id, TYPE_PREDICATE_ID _pre_id, TYPE_ENTITY_LITERAL_ID _obj_id, shared_ptr<Transaction> txn = nullptr);
	bool updateRemove_o2values(TYPE_ENTITY_LITERAL_ID _sub_id, TYPE_PREDICATE_ID _pre_id, TYPE_ENTITY_LITERAL_ID _obj_id, shared_ptr<Transaction> txn = nullptr);
	bool updateInsert_p2values(TYPE_ENTITY_LITERAL_ID _sub_id, TYPE_PREDICATE_ID _pre_id, TYPE_ENTITY_LITERAL_ID _obj_id, shared_ptr<Transaction> txn = nullptr);
	bool updateRemove_p2values(TYPE_ENTITY_LITERAL_ID _sub_id, TYPE_PREDICATE_ID _pre_id, TYPE_ENTITY_LITERAL_ID _obj_id, shared_ptr<Transaction> txn = nullptr);

	//batch update
	unsigned updateInsert_s2values(TYPE_ENTITY_LITERAL_ID _subid, const std::vector<unsigned>& _pidoidlist);
	unsigned updateRemove_s2values(TYPE_ENTITY_LITERAL_ID _subid, const std::vector<unsigned>& _pidoidlist);
	unsigned updateInsert_o2values(TYPE_ENTITY_LITERAL_ID _objid, const std::vector<unsigned>& _pidsidlist);
	unsigned updateRemove_o2values(TYPE_ENTITY_LITERAL_ID _objid, const std::vector<unsigned>& _pidsidlist);
	unsigned updateInsert_p2values(TYPE_PREDICATE_ID _preid, const std::vector<unsigned>& _sidoidlist);
	unsigned updateRemove_p2values(TYPE_PREDICATE_ID _preid, const std::vector<unsigned>& _sidoidlist);

	bool updateTupleslist_insert(TYPE_ENTITY_LITERAL_ID _sub_id, TYPE_PREDICATE_ID _pre_id, TYPE_ENTITY_LITERAL_ID _obj_id, shared_ptr<Transaction> txn = nullptr);
	bool updateTupleslist_remove(TYPE_ENTITY_LITERAL_ID _sub_id, TYPE_PREDICATE_ID _pre_id, TYPE_ENTITY_LITERAL_ID _obj_id, shared_ptr<Transaction> txn = nullptr);

	

	//Growing Locking
	//called before update operation
	bool GetExclusiveLock(TYPE_ENTITY_LITERAL_ID _sub_id, TYPE_PREDICATE_ID _pre_id, TYPE_ENTITY_LITERAL_ID _obj_id, shared_ptr<Transaction> txn);
	bool GetExclusiveLocks(std::vector<TYPE_ENTITY_LITERAL_ID>& sids, std::vector<TYPE_ENTITY_LITERAL_ID>& oids, std::vector<TYPE_PREDICATE_ID>& pids, std::shared_ptr<Transaction> txn);

	//Shrinking(commit)
	bool ReleaseAllLocks(std::shared_ptr<Transaction> txn) const;

	//Shrinking(Abort)
	bool ReleaseExclusiveLock(TYPE_ENTITY_LITERAL_ID _sub_id, TYPE_PREDICATE_ID _pre_id, TYPE_ENTITY_LITERAL_ID _obj_id, std::shared_ptr<Transaction> txn);
	bool TransactionInvalid(std::shared_ptr<Transaction> txn);

	//garbage clean
	//No Transaction should be running!
	void IVArrayVacuum(std::vector<unsigned>& sub_ids , std::vector<unsigned>& obj_ids, std::vector<unsigned>& obj_literal_ids, std::vector<unsigned>& pre_ids) ;

	//===============================================================================

	//for entity2id
	bool open_entity2id(int _mode);
	bool close_entity2id();
    void clear_entity2id();
	bool subIDByEntity(std::string _entity);
	TYPE_ENTITY_LITERAL_ID getIDByEntity(std::string _entity) const;
	bool setIDByEntity(std::string _entity, TYPE_ENTITY_LITERAL_ID _id);

	//for id2entity
	bool open_id2entity(int _mode);
	bool close_id2entity();
    void clear_id2entity();
	bool subEntityByID(TYPE_ENTITY_LITERAL_ID _id);
	std::string getEntityByID(TYPE_ENTITY_LITERAL_ID _id, bool needUnCompress = false) const;
	bool setEntityByID(TYPE_ENTITY_LITERAL_ID _id, std::string _entity);

	//for predicate2id
	bool open_predicate2id(int _mode);
	bool close_predicate2id();
    void clear_predicate2id();
	bool subIDByPredicate(std::string _predicate);
	TYPE_PREDICATE_ID getIDByPredicate(std::string _predicate) const;
	bool setIDByPredicate(std::string _predicate, TYPE_PREDICATE_ID _id);

	//for id2predicate
	bool open_id2predicate(int _mode);
	bool close_id2predicate();
    void clear_id2predicate();
	bool subPredicateByID(TYPE_PREDICATE_ID _id);
	std::string getPredicateByID(TYPE_PREDICATE_ID _id, bool needUnCompress = false) const;
	bool setPredicateByID(TYPE_PREDICATE_ID _id, std::string _predicate);

	//for literal2id
	bool open_literal2id(int _mode);
	bool close_literal2id();
    void clear_literal2id();
	bool subIDByLiteral(std::string _literal);
	TYPE_ENTITY_LITERAL_ID getIDByLiteral(std::string _literal) const;
	bool setIDByLiteral(std::string _literal, TYPE_ENTITY_LITERAL_ID _id);

	//for id2literal
	bool open_id2literal(int _mode);
	bool close_id2literal();
    void clear_id2literal();
	bool subLiteralByID(TYPE_ENTITY_LITERAL_ID _id);
	std::string getLiteralByID(TYPE_ENTITY_LITERAL_ID _id, bool needUnCompress = false) const;
	bool setLiteralByID(TYPE_ENTITY_LITERAL_ID _id, std::string _literal);

	//===============================================================================

	//for subID2values
	bool open_subID2values(int _mode, TYPE_ENTITY_LITERAL_ID _entity_num = 0);
	bool close_subID2values();
    void clear_subID2values();
	bool build_subID2values(ID_TUPLE* _p_id_tuples, TYPE_TRIPLE_NUM _triples_num, TYPE_ENTITY_LITERAL_ID total_entity_num);
	bool getpreIDlistBysubID(TYPE_ENTITY_LITERAL_ID _subid, unsigned*& _preidlist, unsigned& _list_len, bool _no_duplicate = false, shared_ptr<Transaction> txn = nullptr) const;
	bool getobjIDlistBysubID(TYPE_ENTITY_LITERAL_ID _subid, unsigned*& _objidlist, unsigned& _list_len, bool _no_duplicate = false, shared_ptr<Transaction> txn = nullptr) const;
	bool getobjIDlistBysubIDpreID(TYPE_ENTITY_LITERAL_ID _subid, TYPE_PREDICATE_ID _preid, unsigned*& _objidlist, unsigned& _list_len, bool _no_duplicate = false, shared_ptr<Transaction> txn = nullptr) const;
	bool getpreIDobjIDlistBysubID(TYPE_ENTITY_LITERAL_ID _subid, unsigned*& _preid_objidlist, unsigned& _list_len, bool _no_duplicate = false, shared_ptr<Transaction> txn = nullptr) const;

	/**
	 * for subID2xvalues [SPOX]
	 */ 
	bool open_subID2xvalues(int _mode, TYPE_ENTITY_LITERAL_ID _entity_num = 0);
	bool close_subID2xvalues();	
	/**
	 * @brief Given subject id and predicate id, return the object id list and edge properties. Also return the num of neigbors(objects), and the number of properties per edge.
	 * @param _subid The subject id
	 * @param _preid The predicate id
	 * @param _objidlist After the call, will be filled with all objects
	 * @param _x_list After the call, will be filled with edge properties
	 * @param xwidth Num of properties on each edge
	 * @param _no_duplicate Not used
	 * @param txn Transaction
	*/
	bool getobjIDxvaluesBysubIDpreID(TYPE_ENTITY_LITERAL_ID _subid, TYPE_PREDICATE_ID _preid, unsigned*& _objidlist, long long* & _x_list, unsigned& xwidth, unsigned& _list_len, bool _no_duplicate = false, shared_ptr<Transaction> txn = nullptr) const;

	/**
	 * for s2pox_iv_ [SPOX]
	 * 
	*/
	bool open_s2pox_iv(int _mode, TYPE_ENTITY_LITERAL_ID _entity_num = 0);
	bool close_s2pox_iv();
	bool getObjIDListBySubIDPreIDIV(TYPE_ENTITY_LITERAL_ID _subid, TYPE_PREDICATE_ID _preid, unsigned*& _objidlist, unsigned& _list_len, shared_ptr<Transaction> txn = nullptr) const;
	bool getObjIDXValuesBySubIDPreIDIV(TYPE_ENTITY_LITERAL_ID _subid, TYPE_PREDICATE_ID _preid, unsigned*& _objidlist, long long* & _x_list, unsigned& xwidth, unsigned& _list_len, shared_ptr<Transaction> txn = nullptr) const;

	//for objID2values
	bool open_objID2values(int _mode, TYPE_ENTITY_LITERAL_ID _entitynum = 0, TYPE_ENTITY_LITERAL_ID _literal_num = 0);
	bool close_objID2values();
    void clear_objID2values();
	bool build_objID2values(ID_TUPLE* _p_id_tuples, TYPE_TRIPLE_NUM _triples_num, TYPE_ENTITY_LITERAL_ID total_entity_num, TYPE_ENTITY_LITERAL_ID total_literal_num);
	bool getpreIDlistByobjID(TYPE_ENTITY_LITERAL_ID _objid, unsigned*& _preidlist, unsigned& _list_len, bool _no_duplicate = false, shared_ptr<Transaction> txn = nullptr) const;
	bool getsubIDlistByobjID(TYPE_ENTITY_LITERAL_ID _objid, unsigned*& _subidlist, unsigned& _list_len, bool _no_duplicate = false, shared_ptr<Transaction> txn = nullptr) const;
	bool getsubIDlistByobjIDpreID(TYPE_ENTITY_LITERAL_ID _objid, TYPE_PREDICATE_ID _preid, unsigned*& _subidlist, unsigned& _list_len, bool _no_duplicate = false, shared_ptr<Transaction> txn = nullptr) const;
	bool getpreIDsubIDlistByobjID(TYPE_ENTITY_LITERAL_ID _objid, unsigned*& _preid_subidlist, unsigned& _list_len, bool _no_duplicate = false, shared_ptr<Transaction> txn = nullptr) const;
	
	/**
	 * for objID2xvalues [SPOX]
	*/
	bool open_objID2xvalues(int _mode, TYPE_ENTITY_LITERAL_ID _entitynum = 0);
	bool close_objID2xvalues();
	/**
	 * @brief Given object id and predicate id, return the subject id list and edge properties. Also return the num of neigbors(subjects), and the number of properties per edge.
	 * @param _objid The object id
	 * @param _preid The predicate id
	 * @param _subidlist After the call, will be filled with all subjects
	 * @param _x_list After the call, will be filled with edge properties
	 * @param xwidth Num of properties on each edge
	 * @param _no_duplicate Not used
	 * @param txn Transaction
	*/
	bool getsubIDxvaluesByobjIDpreID(TYPE_ENTITY_LITERAL_ID _objid, TYPE_PREDICATE_ID _preid, unsigned*& _subidlist, long long* &_x_list, unsigned& xwidth, unsigned& _list_len, bool _no_duplicate = false, shared_ptr<Transaction> txn = nullptr) const;
	bool update_xvalues(TYPE_ENTITY_LITERAL_ID _sid, TYPE_PREDICATE_ID _pid, TYPE_ENTITY_LITERAL_ID _oid, unsigned _xid, long long _xvalue, shared_ptr<Transaction> txn = nullptr);

	/**
	 * for o2psx_iv_ [SPOX]
	 * 
	*/
	bool open_o2psx_iv(int _mode, TYPE_ENTITY_LITERAL_ID _entity_num = 0);
	bool close_o2psx_iv();
	bool getSubIDListByObjIDPreIDIV(TYPE_ENTITY_LITERAL_ID _objid, TYPE_PREDICATE_ID _preid, unsigned*& _subidlist, unsigned& _list_len, shared_ptr<Transaction> txn = nullptr) const;
	bool getSubIDXValuesByObjIDPreIDIV(TYPE_ENTITY_LITERAL_ID _objid, TYPE_PREDICATE_ID _preid, unsigned*& _subidlist, long long* &_x_list, unsigned& xwidth, unsigned& _list_len, shared_ptr<Transaction> txn = nullptr) const;
	
	/* update functions for s2pox_iv, o2psx_iv */
    bool insert_spox_iv_one(const vector<ID_XX_TUPLE> &Triples, unsigned xwidth, bool is_s2pox, shared_ptr<Transaction> txn);  // used for build
	bool insert_spox_iv(const vector<ID_XX_TUPLE> &Triples, unsigned xwidth, shared_ptr<Transaction> txn);
  bool delete_spox_iv(TYPE_ENTITY_LITERAL_ID _sid, TYPE_PREDICATE_ID _pid, TYPE_ENTITY_LITERAL_ID _oid, shared_ptr<Transaction> txn = nullptr);
	bool modify_spox_iv(TYPE_ENTITY_LITERAL_ID _sid, TYPE_PREDICATE_ID _pid, TYPE_ENTITY_LITERAL_ID _oid, unsigned _xid, long long _xvalue, shared_ptr<Transaction> txn = nullptr);
  bool load_spox_iv(TYPE_ENTITY_LITERAL_ID limit_id_entity, unsigned num_threads);

	//for preID2values
	bool open_preID2values(int _mode, TYPE_PREDICATE_ID _pre_num = 0);
	bool close_preID2values();
  void clear_preID2values();
	bool build_preID2values(ID_TUPLE* _p_id_tuples, TYPE_TRIPLE_NUM _triples_num, TYPE_PREDICATE_ID total_pre_num);
	bool getsubIDlistBypreID(TYPE_PREDICATE_ID _preid, unsigned*& _subidlist, unsigned& _list_len, bool _no_duplicate = false, shared_ptr<Transaction> txn = nullptr) const;
	bool getobjIDlistBypreID(TYPE_PREDICATE_ID _preid, unsigned*& _objidlist, unsigned& _list_len, bool _no_duplicate = false, shared_ptr<Transaction> txn = nullptr) const;
	bool getsubIDobjIDlistBypreID(TYPE_PREDICATE_ID _preid, unsigned*& _subid_objidlist, unsigned& _list_len, bool _no_duplicate = false, shared_ptr<Transaction> txn = nullptr) const;


	//for so2p
	bool getpreIDlistBysubIDobjID(TYPE_ENTITY_LITERAL_ID _subID, TYPE_ENTITY_LITERAL_ID _objID, unsigned*& _preidlist, unsigned& _list_len, bool _no_duplicate = false, shared_ptr<Transaction> txn = nullptr) const;

	// bool load_trie(int _mode);

	// Trie *getTrie();

	void set_if_single_thread(bool _single);

    //for str2propID
    bool open_str2propID();
    bool close_str2propID();
    void clear_str2propID();
	TYPE_PROPERTY_ID getpropIDBypropStr(const std::string &_propStr) const;
    bool setpropIDBypropStr(const std::string &_propStr, TYPE_PROPERTY_ID _id);
    bool subpropIDBypropStr(const std::string &_propStr);

    //for propID2str
    bool open_propID2str();
    bool close_propID2str();
    void clear_propID2str();
    std::string getpropStrBypropID(TYPE_PROPERTY_ID _id) const;
    bool setpropStrBypropID(TYPE_PROPERTY_ID _id, const std::string &_propStr);
    bool subpropStrBypropID(TYPE_PROPERTY_ID _id);

    // for spo2eid
    bool open_spo2eid();
    bool close_spo2eid();
    void clear_spo2eid();
    bool getedgeIDlistByidTuple(ID_TUPLE* _id_tuple, TYPE_EDGE_ID *&_edgeIDlist, unsigned &_len, shared_ptr<Transaction> txn = nullptr, bool _no_duplicate = false) const;
    bool getedgeIDlistByidTuple(TYPE_ENTITY_LITERAL_ID s_id, TYPE_ENTITY_LITERAL_ID o_id, const std::string& p_str,
                                TYPE_EDGE_ID *&_edgeIDlist, unsigned &_len, shared_ptr<Transaction> txn = nullptr, bool _no_duplicate = false) const;
    bool getedgeIDlistByidTuple(TYPE_ENTITY_LITERAL_ID s_id, TYPE_ENTITY_LITERAL_ID o_id, TYPE_PREDICATE_ID pre_id,
                                TYPE_EDGE_ID *&_edgeIDlist, unsigned &_len, shared_ptr<Transaction> txn = nullptr, bool _no_duplicate = false) const;
    TYPE_EDGE_ID getedgeIDByidTuple(TYPE_ENTITY_LITERAL_ID s_id, TYPE_ENTITY_LITERAL_ID o_id, TYPE_PREDICATE_ID p_id, unsigned e_index, shared_ptr<Transaction> txn = nullptr) const;
    void SetEdgeIdWithBatch(const ID_TUPLE &id_tuple, const std::vector<TYPE_EDGE_ID> &edge_id_list);
    bool updateInsert_edgeIDlistByidTuple(const ID_TUPLE* _id_tuple, const TYPE_EDGE_ID *_edgeIDlist, unsigned _len, shared_ptr<Transaction> txn = nullptr);
    bool updateRemove_edgeIDlistByidTuple(ID_TUPLE* _id_tuple, TYPE_EDGE_ID *_edgeIDlist, unsigned _len, shared_ptr<Transaction> txn = nullptr);

    // for eid2spo
    bool open_eid2values();
    bool close_eid2values();
    void clear_eid2values();
    ID_TUPLE* getidTupleByedgeID(TYPE_EDGE_ID _edgeID) const;
    bool setidTupleByedgeID(TYPE_EDGE_ID _edgeID, ID_TUPLE* _id_tuple, shared_ptr<Transaction> txn = nullptr);
    bool subidTupleByedgeID(TYPE_EDGE_ID _edgeID);

    // for eid2propID
    bool getpropIDlistByedgeID(TYPE_EDGE_ID _edgeID, TYPE_PROPERTY_ID *&_propIDlist, unsigned &_len, shared_ptr<Transaction> txn = nullptr, bool _no_duplicate = false) const;
    void SetEdgePropIdWithBatch(TYPE_EDGE_ID edge_id, const ID_TUPLE &id_tuple, const std::vector<TYPE_PROPERTY_ID > &prop_id_list); // nontxn
    bool updateInsert_propIDlistByedgeID(TYPE_EDGE_ID _edgeID, TYPE_PROPERTY_ID *_propIDlist, unsigned _len, shared_ptr<Transaction> txn = nullptr);
    bool updateRemove_propIDlistByedgeID(TYPE_EDGE_ID _edgeID, TYPE_PROPERTY_ID *_propIDlist, unsigned _len, shared_ptr<Transaction> txn = nullptr);

    // for eidpropID2value
    bool open_eidpropID2value();
    bool close_eidpropID2value();
    void clear_eidpropID2value();
    bool getedgeValueByid(TYPE_EDGE_ID _edgeID, TYPE_PROPERTY_ID _propID, GPStore::Value *&_value, shared_ptr<Transaction> txn = nullptr) const;
    void SetEdgeValueWithBatch(TYPE_EDGE_ID edge_id, TYPE_PROPERTY_ID prop_id, const GPStore::Value *value); // nontxn
    bool setedgeValueByid(TYPE_EDGE_ID _edgeID, TYPE_PROPERTY_ID _propID, const GPStore::Value *_value, shared_ptr<Transaction> txn = nullptr);
    bool subedgeValueByid(TYPE_EDGE_ID _edgeID, TYPE_PROPERTY_ID _propID, shared_ptr<Transaction> txn = nullptr);

    // for vid2propID
    bool open_vid2propID();
    bool close_vid2propID();
    void clear_vid2propID();
    bool getpropIDlistBynodeID(TYPE_ENTITY_LITERAL_ID _nodeID, TYPE_PROPERTY_ID *&_propIDlist, unsigned &_len, shared_ptr<Transaction> txn = nullptr, bool _no_duplicate = false) const;
    void SetNodePropIdWithBatch(TYPE_ENTITY_LITERAL_ID node_id, const std::vector<TYPE_PROPERTY_ID > &prop_id_list); // nontxn
    bool updateInsert_propIDlistBynodeID(TYPE_ENTITY_LITERAL_ID _nodeID, TYPE_PROPERTY_ID *_propIDlist, unsigned _len, shared_ptr<Transaction> txn = nullptr);
    bool updateRemove_propIDlistBynodeID(TYPE_ENTITY_LITERAL_ID _nodeID, TYPE_PROPERTY_ID *_propIDlist, unsigned _len, shared_ptr<Transaction> txn = nullptr);

    // for vidpropID2value
    bool open_vidpropID2value();
    bool close_vidpropID2value();
    void clear_vidpropID2value();
    bool getnodeValueByid(TYPE_ENTITY_LITERAL_ID _nodeID, TYPE_PROPERTY_ID _propID, GPStore::Value *&_value, shared_ptr<Transaction> txn = nullptr) const;
    bool getNodeValueByidAndCompare(TYPE_ENTITY_LITERAL_ID _nodeID, TYPE_PROPERTY_ID _propID, const char *_value, unsigned len, shared_ptr<Transaction> txn) const;
    void SetNodeValueWithBatch(TYPE_ENTITY_LITERAL_ID node_id, TYPE_PROPERTY_ID prop_id, const GPStore::Value *value); // nontxn
    void SetNodeValueWithBatch(const std::vector<unsigned long long > &key_set, const std::vector<std::pair<char*, unsigned>> &val_set); //nontxn
	bool setnodeValueByid(TYPE_ENTITY_LITERAL_ID _nodeID, TYPE_PROPERTY_ID _propID, const GPStore::Value *_value, shared_ptr<Transaction> txn = nullptr);
    bool subnodeValueByid(TYPE_ENTITY_LITERAL_ID _nodeID, TYPE_PROPERTY_ID _propID, shared_ptr<Transaction> txn = nullptr);

	// for nodeID2labelID
	bool open_nodeID2labelID();
	bool close_nodeID2labelID();
    void clear_nodeID2labelID();
	bool getlabelIDlistBynodeID(TYPE_ENTITY_LITERAL_ID _nodeID, TYPE_LABEL_ID *&_labelIDlist, unsigned &_len, shared_ptr<Transaction> txn = nullptr) const;
    void SetNodeLabelWithBatch(TYPE_ENTITY_LITERAL_ID node_id, const std::vector<TYPE_LABEL_ID> &label_id_list);
	bool updateInsert_labelIDlistBynodeID(TYPE_ENTITY_LITERAL_ID _nodeID, const std::set<TYPE_LABEL_ID> &_labelIDlist, shared_ptr<Transaction> txn = nullptr);
	bool updateRemove_labelIDlistBynodeID(TYPE_ENTITY_LITERAL_ID _nodeID, const std::set<TYPE_LABEL_ID> &_labelIDlist, shared_ptr<Transaction> txn = nullptr);

	// for labelID2nodeID
	bool open_labelID2nodeID();
	bool close_labelID2nodeID();
    void clear_labelID2nodeID();
	bool getnodeIDlistBylabelID(TYPE_LABEL_ID _labelID, TYPE_ENTITY_LITERAL_ID *&_nodeIDlist, unsigned &_len, shared_ptr<Transaction> txn = nullptr) const;
	bool updateInsert_nodeIDlistBylabelID(TYPE_LABEL_ID _labelID, const std::set<TYPE_ENTITY_LITERAL_ID> &_nodeIDlist, shared_ptr<Transaction> txn = nullptr);
	bool updateRemove_nodeIDlistBylabelID(TYPE_LABEL_ID _labelID, const std::set<TYPE_ENTITY_LITERAL_ID> &_nodeIDlist, shared_ptr<Transaction> txn = nullptr);
    bool getnodeNumBylabelID(TYPE_LABEL_ID _labelID, unsigned &num, shared_ptr<Transaction> txn = nullptr);

	// for prop2nodeID
	bool open_prop2nodeID();
	bool close_prop2nodeID();
    void clear_prop2nodeID();
	bool build_index(TYPE_PROPERTY_ID _propID, TYPE_ENTITY_LITERAL_ID limitID_entity, TYPE_ENTITY_LITERAL_ID limitID_literal);
	void SetProp2NodeWithBatch(TYPE_LABEL_ID label_id, TYPE_PROPERTY_ID prop_id, const GPStore::Value *value, const std::vector<TYPE_ENTITY_LITERAL_ID> &node_ids);
	bool is_propID_indexed(TYPE_PROPERTY_ID _propID) const;
	void set_propID_indexed(TYPE_PROPERTY_ID _propID);
	std::vector<TYPE_PROPERTY_ID> get_propID_indexed() const;
	bool find_index(TYPE_LABEL_ID _labelID, TYPE_PROPERTY_ID _propID, const GPStore::Value *_value, TYPE_ENTITY_LITERAL_ID *&_nodeIDlist, unsigned int &_len, shared_ptr<Transaction> txn = nullptr) const;

	// for propRange2nodeID [only for value_type == INT LONG STRING]
	bool open_propRange2nodeID();
	bool close_propRange2nodeID();
    void clear_propRange2nodeID();
	bool build_prefix_index(TYPE_PROPERTY_ID _propID, TYPE_ENTITY_LITERAL_ID limitID_entity, TYPE_ENTITY_LITERAL_ID limitID_literal);
	bool is_propID_prefix_indexed(TYPE_PROPERTY_ID _propID);
	bool getnodeIDlistByPropRange(TYPE_PROPERTY_ID _propID, const GPStore::Value *_lower_bound, const GPStore::Value *_upper_bound, vector<TYPE_ENTITY_LITERAL_ID> &_nodeIDlist, shared_ptr<Transaction> txn = nullptr);
    std::pair<long long, long long> getPropRange(TYPE_PROPERTY_ID _propID);

	//for S2POX
    bool open_s2pox();
    bool close_s2pox();
    void clear_s2pox();

	//for O2PSX
    bool open_o2psx();
    bool close_o2psx();
    void clear_o2psx();

    //No Transaction should be running!
    /**
     * @brief Garbage Collection for KVStore
     * @param sub_ids Dirtykey of sub
     * @param obj_ids Dirtykey of obj
     * @param obj_literal_ids Dirtykey of obj_literal
     * @param pre_ids Dirtykey of pre
     * @param PDirtyKeys Dirtykey of PStore index
     */
    void Vacuum(std::vector<unsigned>& sub_ids, std::vector<unsigned>& obj_ids, std::vector<unsigned>& obj_literal_ids,
                std::vector<unsigned>& pre_ids, vector<ProValSet> &PDirtyKeys) ;


    void FlushNodeBatch();
    void FlushEdgeBatch();
private:
	std::string store_path;

	std::string dictionary_store_path;

	// Trie *trie;

	SITree* entity2id;
	//ISTree* id2entity;
	ISArray* id2entity;
	static std::string s_entity2id;
	static std::string s_id2entity;
	static unsigned short buffer_entity2id_build;
	static unsigned short buffer_id2entity_build;
	static unsigned short buffer_entity2id_query;
	static unsigned short buffer_id2entity_query;

	SITree* predicate2id;
	//ISTree* id2predicate;
	ISArray* id2predicate;
	static std::string s_predicate2id;
	static std::string s_id2predicate;
	static unsigned short buffer_predicate2id_build;
	static unsigned short buffer_id2predicate_build;
	static unsigned short buffer_predicate2id_query;
	static unsigned short buffer_id2predicate_query;

	SITree* literal2id;
	//ISTree* id2literal;
	ISArray* id2literal;
	static std::string s_literal2id;
	static std::string s_id2literal;
	static unsigned short buffer_literal2id_build;
	static unsigned short buffer_id2literal_build;
	static unsigned short buffer_literal2id_query;
	static unsigned short buffer_id2literal_query;

//	IVTree* subID2values;
//	IVTree* objID2values;
//	IVTree* preID2values;

	/**
	 * for sp2ox, and op2sx
	*/
	TxnIV* subID2xvalues;
	TxnIV* objID2xvalues;
	IVArray *s2pox_iv_;
	IVArray *o2psx_iv_;

	TxnIV* subID2values;
	TxnIV* objID2values;
	TxnIV* objID2values_literal;
	TxnIV* preID2values;
	static std::string s_sID2values;
	static std::string s_oID2values;
	static std::string s_pID2values;
	static std::string s_sID2xvalues;
	static std::string s_oID2xvalues;
	static std::string s_s2pox_iv;
	static std::string s_o2psx_iv;
	static unsigned short buffer_sID2values_build;
	static unsigned short buffer_oID2values_build;
	static unsigned short buffer_pID2values_build;
	static unsigned short buffer_sID2xvalues_build;
	static unsigned short buffer_oID2xvalues_build;
	static unsigned short buffer_sID2values_query;
	static unsigned short buffer_oID2values_query;
	static unsigned short buffer_pID2values_query;
	static unsigned short buffer_sID2xvalues_query;
	static unsigned short buffer_oID2xvalues_query;

// Everything about PStore
    TxnPStore<char>* str2propID;
	TxnPStore<char>* propID2str;
	TxnPStore<unsigned>* vid2propID;
	TxnPStore<char>* vidpropID2value;
	TxnPStore<unsigned long long>* spo2eid;
	TxnPStore<unsigned>* eid2values;
	TxnPStore<char>* eidpropID2value;
	TxnPStore<unsigned> *nodeID2labelID;
	TxnPStore<unsigned> *labelID2nodeID;
	TxnPStore<unsigned> *prop2nodeID; // [label_id+prop]2nodeID without prefix search
	TxnPStore<unsigned> *propRange2nodeID; //prop2nodeID with prefix search
	std::unique_ptr<PStore> s2pox_;
	std::unique_ptr<PStore> o2psx_;

    std::map<unsigned, unsigned> Label_node_num;
	std::unordered_set<TYPE_PROPERTY_ID> propID_indexed;
	std::unordered_set<TYPE_PROPERTY_ID> propID_prefix_indexed;
    std::map<TYPE_PROPERTY_ID, std::pair<long long, long long> > mp_propID2valueRange;

    //===============================================================================

	bool open(SITree* & _p_btree, std::string _tree_name, int _mode, unsigned long long _buffer_size);
	//bool open(ISTree* & _p_btree, std::string _tree_name, int _mode, unsigned long long _buffer_size);
	bool open(ISArray* & _array, std::string _name, int _mode, unsigned long long _buffer_size, unsigned _key_num = 0);
	//bool open(IVTree* & _p_btree, std::string _tree_name, int _mode, unsigned long long _buffer_size);
	bool open(TxnIV *&_txnIV, std::string _name, int _mode, unsigned long long _buffer_size, unsigned _key_num = 0);
	bool open(IVArray *&iv_array, std::string _name, int _mode, unsigned _key_num = 0);

	template<class T> bool close(TxnPStore<T>* &_txnPStore){
		if(_txnPStore == nullptr)
			return true;

		delete _txnPStore;
		_txnPStore = nullptr;

		return true;
	}

	void flush(SITree* _p_btree);
	//void flush(ISTree* _p_btree);
	void flush(ISArray* _array);
	//void flush(IVTree* _p_btree);
	void flush(TxnIV* _txnIV);

	bool addValueByKey(SITree* _p_btree, char* _key, unsigned _klen, unsigned _val);
	//bool addValueByKey(ISTree* _p_btree, unsigned _key, char* _val, unsigned _vlen);
	bool addValueByKey(ISArray* _array, unsigned _key, char* _val, unsigned _vlen);
	//bool addValueByKey(IVTree* _p_btree, unsigned _key, char* _val, unsigned _vlen);

	bool setValueByKey(SITree* _p_btree, char* _key, unsigned _klen, unsigned _val);
	//bool setValueByKey(ISTree* _p_btree, unsigned _key, char* _val, unsigned _vlen);
	bool setValueByKey(ISArray* _array, unsigned _key, char* _val, unsigned _vlen);
//	bool setValueByKey(IVTree* _p_btree, unsigned _key, char* _val, unsigned _vlen);

	bool getValueByKey(SITree* _p_btree, const char* _key, unsigned _klen, unsigned* _val) const;
	//bool getValueByKey(ISTree* _p_btree, unsigned _key, char*& _val, unsigned& _vlen) const;
	bool getValueByKey(ISArray* _array, unsigned _key, char*& _val, unsigned& _vlen) const;
//	bool getValueByKey(IVTree* _p_btree, unsigned _key, char*& _val, unsigned& _vlen) const;


	TYPE_ENTITY_LITERAL_ID getIDByStr(SITree* _p_btree, const char* _key, unsigned _klen) const;

	bool removeKey(SITree* _p_btree, const char* _key, unsigned _klen);
	//bool removeKey(ISTree* _p_btree, unsigned _key);
	bool removeKey(ISArray* _array, unsigned _key);
//	bool removeKey(IVTree* _p_btree, unsigned _key);
	bool removeKey(TxnIV* _txnIV, unsigned _key);

	static std::vector<unsigned> intersect(const unsigned* _list1, const unsigned* _list2, unsigned _len1, unsigned _len2);
	static unsigned binarySearch(unsigned key, const unsigned* _list, unsigned _list_len, int step = 1);
	static bool isEntity(TYPE_ENTITY_LITERAL_ID id);

	/*
	===================
	|MVCC SubFunctions|
	===================
	*/

	//read
	template<class T>
	bool getValueByKey(TxnPStore<T>* _txnPStore, const char *_key, unsigned _key_len, char *&_value, unsigned &_value_len, shared_ptr<Transaction> txn = nullptr) const
	{
		if(sizeof(T) == 1) {	//for nontxn & cover version
			return _txnPStore->ReadCoverVersion(_key, _key_len, _value, _value_len, txn);
		} else	//for delta version
			return _txnPStore->ReadDeltaVersion(_key, _key_len, _value, _value_len, txn);
	}
	bool getValueByKey(TxnIV* _txnIV, unsigned _key, char*& _val, unsigned long & _vlen, shared_ptr<Transaction> txn = nullptr) const;

	//write
    bool setValueByKey(TxnIV* _txnIV, unsigned _key, char*& _val, unsigned long & _vlen, shared_ptr<Transaction> txn = nullptr) const;
    template<class T> bool setValueByKey(TxnPStore<T>* _txnPStore, const char *_key, unsigned _key_len, const char *_value, unsigned _value_len, shared_ptr<Transaction> txn = nullptr)
	{
		return _txnPStore->WriteCoverVersion(_key, _key_len, _value, _value_len, txn);
	}
	public:
	tuple<PStore*, PStore*, CacheManager*, CacheManager*> GetIndexes()
	{
		return {s2pox_.get(), o2psx_.get(), subID2values->mvcc->cacheManager, objID2values->mvcc->cacheManager};
	}
	tuple<CacheManager*, CacheManager*, CacheManager*, CacheManager*> GetIndexesNOLMDB()
	{
		return {subID2xvalues->mvcc->cacheManager, objID2xvalues->mvcc->cacheManager, subID2values->mvcc->cacheManager, objID2values->mvcc->cacheManager};
	}
    tuple<IVArray*, IVArray*> GetIndexesNewS2POX() const
    {
      return {s2pox_iv_, o2psx_iv_};
    }
private:

	template<class T> bool insert_values(TxnPStore<T>* _txnPStore, const char *_key, unsigned _key_len, const typename MVCC_PS<T>::VDataSetz &addset, shared_ptr<Transaction> txn = nullptr){
		const typename MVCC_PS<T>::VDataSetz delset;
		return _txnPStore->WriteDeltaVersion(_key, _key_len, addset, delset, txn);
	}

	template<class T> bool remove_values(TxnPStore<T>* _txnPStore, const char *_key, unsigned _key_len, const typename MVCC_PS<T>::VDataSetz &delset, shared_ptr<Transaction> txn = nullptr)
	{
		const typename MVCC_PS<T>::VDataSetz addset;
		return _txnPStore->WriteDeltaVersion(_key, _key_len, addset, delset, txn);
	}
	bool insert_values(TxnIV* _txnIV, unsigned _key, VDataSet &addset, shared_ptr<Transaction> txn = nullptr);
	bool insert_xvalues(TxnIV* _txnIV, unsigned _key, VXXDataSet &addset, unsigned xwidth, shared_ptr<Transaction> txn = nullptr);
	bool remove_values(TxnIV* _txnIV, unsigned _key, VDataSet &delset, shared_ptr<Transaction> txn = nullptr);

	// SPOX In LMDB
	bool insert_xvalues(std::unique_ptr<PStore>& _so2xvalues, unsigned _key, VXXDataSet &addset, unsigned xwidth, shared_ptr<Transaction> txn = nullptr, bool is_batch_insert = false);
	static void merge_addset(const VXXDataSet &_pidsoidlist, unsigned xwidth,
								unsigned *_tmp, unsigned long _len, 
								unsigned *&values, unsigned long &values_len);
public:
	bool update_xvalues(std::unique_ptr<PStore>& _so2xvalues, TYPE_ENTITY_LITERAL_ID _src_id, TYPE_PREDICATE_ID _pid, TYPE_ENTITY_LITERAL_ID _dst_id, unsigned _xid, long long _xvalue, shared_ptr<Transaction> txn = nullptr);

  /**
   *
   * @param _value value in GPStore::Value format
   * @param _str value in string format
   * @param _len value len
   * @return conversion successful or not
   */
  bool value2str(const GPStore::Value *_value, char *&_str, unsigned &_len) const;
  
  // INTEGER: 32-bit; LONG: 64-bit; FLOAT: 64-bit
  enum value_type: char{
	INTEGER, FLOAT, LONG, STRING, BOOLEAN, LIST_INT, LIST_LONG,LIST_FLOAT, LIST_STR, LIST_BOOL, EMPTY_LIST
  };

private:
    void clear_values(TxnIV *txnIV);
    void clear_values(ISArray *array);
    void clear_values(SITree *btree);

	//abort
	bool invalid_values(TxnIV *_txnIV, unsigned _key, shared_ptr<Transaction> txn);

	//locks and latches operation
	//bool try_exclusive_lock(TYPE_ENTITY_LITERAL_ID _sub_id, TYPE_PREDICATE_ID _pre_id, TYPE_ENTITY_LITERAL_ID _obj_id, shared_ptr<Transaction> txn);
	bool try_exclusive_locks(vector<TYPE_ENTITY_LITERAL_ID>& sids, vector<TYPE_ENTITY_LITERAL_ID>& oids, vector<TYPE_PREDICATE_ID>& pids, shared_ptr<Transaction> txn);
	bool try_exclusive_lock(TYPE_ENTITY_LITERAL_ID _sub_id, TYPE_PREDICATE_ID _pre_id, TYPE_ENTITY_LITERAL_ID _obj_id, shared_ptr<Transaction> txn = nullptr);
	bool get_exclusive_latch(TxnIV *_txnIV, unsigned _key, shared_ptr<Transaction> txn) const;

	bool release_exclusive_latch(TxnIV* _txnIV, unsigned _key, shared_ptr<Transaction> txn) const;
	bool release_shared_latch(TxnIV* _txnIV, unsigned _key, shared_ptr<Transaction> txn) const;

	//Garbage Collection
    /**
     * @brief Clean version data and reset base_version for key
     *
     * @param _txnIV Pointer of TxnIV, s2po/p2so/o2sp
     * @param _key id
     * @return success or failed
     */
	bool clean_dirty_key(TxnIV* _txnIV, unsigned _key);

    //garbage clean

    /**
     * @brief Garbage Collection for IndexValue
     *
     * @param _txnIV Pointer of TxnIV, s2po/p2so/o2sp
     * @param ids Dirtykey wait to clean
     * @param txn Pointer of txn, only equal to clean_txn
     */
    void IndexValue_vacuum(TxnIV* _txnIV, vector<unsigned>& ids, shared_ptr<Transaction> txn);

	// transfer string to value
	bool str2value(char* _str, unsigned _len, GPStore::Value *&_value) const;

	// modify index while set & sub nodeValueByid
	bool remove_index(TYPE_ENTITY_LITERAL_ID _nodeID, TYPE_PROPERTY_ID _propID, shared_ptr<Transaction> txn = nullptr);
	bool remove_index(TYPE_ENTITY_LITERAL_ID _nodeID, TYPE_PROPERTY_ID _propID, const char *_prop_value, const unsigned _prop_vlen, shared_ptr<Transaction> txn = nullptr);
	bool insert_index(TYPE_ENTITY_LITERAL_ID _nodeID, TYPE_PROPERTY_ID _propID, const char *_prop_value, const unsigned _prop_vlen, shared_ptr<Transaction> txn = nullptr);
	// modify index while set & sub nodeValueByid for prefix index
	bool insert_prefix_index(TYPE_ENTITY_LITERAL_ID _nodeID, TYPE_PROPERTY_ID _propID, const char *_prop_value, const unsigned _prop_vlen, shared_ptr<Transaction> txn = nullptr);
	bool remove_prefix_index(TYPE_ENTITY_LITERAL_ID _nodeID, TYPE_PROPERTY_ID _propID, shared_ptr<Transaction> txn = nullptr);
	bool remove_prefix_index(TYPE_ENTITY_LITERAL_ID _nodeID, TYPE_PROPERTY_ID _propID, const char *_prop_value, const unsigned _prop_vlen, shared_ptr<Transaction> txn = nullptr);
    std::pair<long long, long long> getPropExtremeValue(TYPE_PROPERTY_ID _propID, GPStore::Value::Type _value_type);

    friend class PreProcess;
};

#endif //_KVSTORE_KVSTORE_H

