/*=============================================================================
# Filename: Database.h
# Author: Bookug Lobert
# Mail: 1181955272@qq.com
# Last Modified: 2015-10-23 14:20
# Description: originally written by liyouhuan, modified by zengli and chenjiaqi
=============================================================================*/
#pragma once
#ifndef _DATABASE_DATABASE_H
#define _DATABASE_DATABASE_H

#include "../Query/SPARQLquery.h"
#include "../StringIndex/StringIndex.h"
#include "../Parser/RDFParser.h"
#include "../Parser/SPARQL/SPARQLParser.h"
#include "../Query/GeneralEvaluation.h"
#include "../Server/Socket.h"
#include "../PQuery/PTempResult.h"
#include "../PQuery/Node.h"
#include "../PQuery/Edge.h"
#include "CSR.h"

#include "Util/Value.h"
// #include "Database/PropertyUtils.h"

class Database
{
public:
	// static const bool only_sub2idpre2id = true;
	// static const int internal = 100 * 1000;

	// void test();
	// void test_build_sig();
	// void test_join();
	// void printIDlist(int _i, int* _list, int _len, std::string _log);
	// void printPairList(int _i, int* _list, int _len, std::string _log);

	// when encode EntitySig, one way uses STRING-hash, the other one uses ID-hash
	// depending on this->encode_mode
	static const int STRING_MODE = 1;
	static const int ID_MODE = 2;
	CSR *csr;
	Database();
	Database(std::string _name);
	void release(FILE *fp0);
	~Database();

	bool save();
	bool load(Socket &socket, bool loadCSR = false);
	bool load(bool loadCSR = false);
    void LoadEntity(int id);
    void LoadLiteral(int id);
    void LoadPredicate(int id);
    void LoadEntityProp(int id);
    bool ExecFuncInterval(int num_threads, int max_id_plus_1, void (Database::*func)(int));
    void MultiThreadExec(int num_threads, int start_id, int end_id, void (Database::*func)(int));
	bool unload();
	void clear();
	void export_db(FILE *fp);
	int query_cypher(const std::string &_query, PTempResult &temp_result, shared_ptr<Transaction> txn = nullptr);
	int query_cypher(const std::string &_query, std::string& _ret_msg, shared_ptr<Transaction> txn = nullptr);
	int query(const string _query, ResultSet &_result_set, FILE *_fp = stdout, bool update_flag = true, bool export_flag = false, shared_ptr<Transaction> txn = nullptr);
	// 1. if subject of _triple doesn't exist,
	// then assign a new subid, and insert a new SigEntry
	// 2. assign new tuple_id to tuple, if predicate or object doesn't exist before too;
	// 3. if subject exist, update SigEntry, and update spo, ops... etc. if needed

    void InitEmptyDB();
    void BuildEmptyKVstore();
    bool BuildEmptyDB();
	bool build(const string &_rdf_file, Socket &socket);
	bool build(const string &_rdf_file);
	// interfaces to insert/delete from given rdf file
	bool insert(std::string _rdf_file, bool _is_restore = false, shared_ptr<Transaction> txn = nullptr);
	bool remove(std::string _rdf_file, bool _is_restore = false, shared_ptr<Transaction> txn = nullptr);

	unsigned batch_insert(std::string _rdf_file, bool _is_restore = false, shared_ptr<Transaction> txn = nullptr);
	unsigned batch_remove(std::string _rdf_file, bool _is_restore = false, shared_ptr<Transaction> txn = nullptr);

	bool backup();
	bool restore();

	// name of this DB
	string getName();
	// get infos
	TYPE_TRIPLE_NUM getTripleNum();
	TYPE_ENTITY_LITERAL_ID getEntityNum();
	TYPE_ENTITY_LITERAL_ID getLiteralNum();
	TYPE_ENTITY_LITERAL_ID getSubNum();
	TYPE_PREDICATE_ID getPreNum();

	// root Path of this DB + sixTuplesFile
	string getSixTuplesFile();

	// root Path of this DB + signatureBFile
	//  string getSignatureBFile();

	// root Path of this DB + DBInfoFile
	string getDBInfoFile();

	bool loadDBInfoFile();

	bool loadStatisticsInfoFile();
	void loadPProcedureInfo();

	unordered_map<string, unsigned long long> getStatisticsInfo();

	void setTypePredicateName(string &names);

	bool checkIsTypePredicate(string &predicate);

	// id tuples file
	string getIDTuplesFile();

	KVstore *getKVstore();
	StringIndex *getStringIndex();
	QueryCache *getQueryCache();
	TYPE_TRIPLE_NUM *getpre2num();
	TYPE_TRIPLE_NUM *getpre2sub();
	TYPE_TRIPLE_NUM *getpre2obj();
	TYPE_ENTITY_LITERAL_ID &getlimitID_literal();
	TYPE_ENTITY_LITERAL_ID &getlimitID_entity();
	TYPE_PREDICATE_ID &getlimitID_predicate();
	// mutex& get_query_parse_lock();

	// MVCC
	void TransactionRollback(shared_ptr<Transaction> txn);
	void TransactionCommit(shared_ptr<Transaction> txn);

    /**
     * @brief Clean version data when checkpoint
     *
     * @param sub_ids Dirtykey of sub
     * @param obj_ids Dirtykey of obj
     * @param obj_literal_ids Dirtykey of obj_literal
     * @param pre_ids Dirtykey of pre
     * @param EPDirtyKeys Dirtykey of Pstore index
     */
	void VersionClean(vector<unsigned> &sub_ids, vector<unsigned> &obj_ids, vector<unsigned> &obj_literal_ids,
                      vector<unsigned> &pre_ids, vector<ProValSet> &EPDirtyKeys);
	std::string CreateJson(int StatusCode, std::string StatusMsg, std::string ResponseBody);

	// all entity id
	const BlockInfo* getfreelist_entity()const {return freelist_entity;}
	TYPE_ENTITY_LITERAL_ID getentity_num() const {return entity_num;}

private:
	string name;
	string store_path;
	bool is_active;
	atomic<TYPE_TRIPLE_NUM> triples_num;
	TYPE_ENTITY_LITERAL_ID entity_num;
	atomic<TYPE_ENTITY_LITERAL_ID> sub_num;
	// BETTER: add object num
	TYPE_PREDICATE_ID pre_num;
	TYPE_ENTITY_LITERAL_ID literal_num;

	int encode_mode;
	bool if_loaded;

	// locks
	mutex query_parse_lock;
	// for read/write, we should use rwlock to improve parallism
	pthread_rwlock_t update_lock;
	// just for debug a block of code
	mutex debug_lock;
	// for getFinalResult
	mutex getFinalResult_lock;
	// for allocEntityID
	mutex allocEntityID_lock;
	// for allocLiteralID
	mutex allocLiteralID_lock;
	// for allocPredicateID
	mutex allocPredicateID_lock;
	// for log file
	mutex log_lock;

public:
	KVstore *kvstore;
private:
	StringIndex *stringindex;
	Join *join;

	enum class UPDATE_TYPE
	{
		SUBJECT_INSERT,
		SUBJECT_REMOVE,
		PREDICATE_INSERT,
		PREDICATE_REMOVE,
		OBJECT_INSERT,
		OBJECT_REMOVE
	};
	// metadata of this database: sub_num, pre_num, obj_num, literal_num, etc.

	// metadata of this database: sub_num, pre_num, obj_num, literal_num, etc.
	string db_info_file;

	// this Statistics data  of catalog entities this database
	string statistics_info_file;

	// this type predicate name
	string type_predicate_name;

	// six tuples: <sub pre obj sid pid oid>
	string six_tuples_file;

	// B means binary
	string signature_binary_file;

	// id tuples file
	string id_tuples_file;
	string update_log;
	string update_log_since_backup;

	// pre2num mapping
	TYPE_TRIPLE_NUM *pre2num;
	// pre2subnum mapping
	TYPE_TRIPLE_NUM *pre2sub;
	// pre2objnum mapping
	TYPE_TRIPLE_NUM *pre2obj;
	// valid: check from minNumPID to maxNumPID
	TYPE_PREDICATE_ID maxNumPID, minNumPID;
	void setPre2Num();
	void setPreMap();

	// TODO: set the buffer capacity as dynamic according to the current memory usage
	// string buffer
	Buffer *entity_buffer;
	// unsigned offset; //maybe let id start from an offset
	unsigned entity_buffer_size;
	Buffer *literal_buffer;
	unsigned literal_buffer_size;

	QueryCache *query_cache;

	// the unordered_map for store the statistic data of entitys
	unordered_map<string, unsigned long long> umap;

	// Trie *trie;

	void setStringBuffer();
	void warmUp();
	// BETTER:add a predicate buffer for ?p query
	// However, I think this is not necessary because ?p is rare and the p2xx tree is small enough

	void query_stringIndex(int id);
	void print_data_count();
	// used for multiple threads
	//  void load_vstree(unsigned _vstree_size);
	void load_entity2id(int _mode);
	void load_id2entity(int _mode);
	void load_literal2id(int _mode);
	void load_id2literal(int _mode);
	void load_predicate2id(int _mode);
	void load_id2predicate(int _mode);
	void load_sub2values(int _mode);
	void load_obj2values(int _mode);
	void load_pre2values(int _mode);

	// functions used to build cache
	void load_cache();
	void get_important_preID();
	std::vector<TYPE_PREDICATE_ID> important_preID;
	void load_important_sub2values();
	void load_important_obj2values();
	void load_candidate_pre2values();
	void build_CacheOfPre2values();
	void build_CacheOfSub2values();
	void build_CacheOfObj2values();
	void get_important_subID();
	void get_important_objID();
	void get_candidate_preID();
	std::priority_queue<KEY_SIZE_VALUE> candidate_preID;
	std::priority_queue<KEY_SIZE_VALUE> important_subID;
	std::priority_queue<KEY_SIZE_VALUE> important_objID;

	// triple num per group for insert/delete
	// can not be too high, otherwise the heap will over
	//  static const int GROUP_SIZE = 1000;
	// manage the ID allocate and garbage
	static const TYPE_ENTITY_LITERAL_ID START_ID_NUM = 0;
	// static const int START_ID_NUM = 1000;
	/////////////////////////////////////////////////////////////////////////////////
	// NOTICE:error if >= LITERAL_FIRST_ID
	string free_id_file_entity;			   // the first is limitID, then free id list
	TYPE_ENTITY_LITERAL_ID limitID_entity; // the current maxium ID num(maybe not used so much)
	BlockInfo *freelist_entity;			   // free id list, reuse BlockInfo for Storage class
public:
	TYPE_ENTITY_LITERAL_ID allocEntityID();
	/**
   * @brief allocate a new id for property.
   *
   * @return TYPE_PROPERTY_ID
   *
   * @details allocate a new id.
   * 	check if there is enough space for new item.
   * 	add the new id into freelist.
   */
  TYPE_PROPERTY_ID allocPropertyID();

    /**
     * @brief allocate id for edge
     *
     * @return TYPE_EDGE_ID
     *
     * @details allocate a new id.
     * 	check if there is enough space for new item.
     * 	add the new id into freelist.
     */
    TYPE_EDGE_ID allocEdgeID();
private:
    void freeEntityID(TYPE_ENTITY_LITERAL_ID _id);
	/////////////////////////////////////////////////////////////////////////////////
	// NOTICE:error if >= 2*LITERAL_FIRST_ID
	string free_id_file_literal;
	TYPE_ENTITY_LITERAL_ID limitID_literal;
	BlockInfo *freelist_literal;
public:
	TYPE_ENTITY_LITERAL_ID allocLiteralID();
	void freeLiteralID(TYPE_ENTITY_LITERAL_ID _id);
private:
	/////////////////////////////////////////////////////////////////////////////////
	// NOTICE:error if >= 2*LITERAL_FIRST_ID
	string free_id_file_predicate;
	TYPE_PREDICATE_ID limitID_predicate;
	BlockInfo *freelist_predicate;
	TYPE_PREDICATE_ID allocPredicateID();
	void freePredicateID(TYPE_PREDICATE_ID _id);
	/////////////////////////////////////////////////////////////////////////////////



  /// allocate property id and edge id. @author victbr

  /// @todo don't know why we need it. just imitate the old code.  
  TYPE_PROPERTY_ID property_num;
  TYPE_EDGE_ID edge_num;
	

  mutex allocPropertyID_lock;
  string free_id_file_property;
public:
  TYPE_PROPERTY_ID limitID_property;
private:
  BlockInfo *freelist_property;

  /**
   * @brief free the given property id. 
   * 
   * @param id property id. 
   */
  void freePropertyID(TYPE_PROPERTY_ID id);

  mutex allocEdgeID_lock;
  string free_id_file_edge;
  TYPE_EDGE_ID limitID_edge;
  BlockInfo *freelist_edge;

  /**
   * @brief free the given edge id. 
   * 
   * @param id 
   */
  void freeEdgeID(TYPE_EDGE_ID id);

  ///



	void initIDinfo();	// initialize the members
	void resetIDinfo(); // reset the id info for build
	void readIDinfo();	// read and build the free list
	void writeIDinfo(); // write and empty the free list
	void saveIDinfo();	// write and empty the free list

	bool saveDBInfoFile();

	bool saveStatisticsInfoFile();

	string getStorePath();

	// encode relative signature data of all Basic Graph Query, who union together into SPARQLquery
	//  void buildSparqlSignature(SPARQLquery & _sparql_q);

	// encode Triple into Subject EntityBitSet
	//  bool encodeTriple2SubEntityBitSet(EntityBitSet& _bitset, const Triple* _p_triple);
	// NOTICE: the encodeTriple with Triple* is invalid now(not enocde the linkage of neighbor-predicate)
	//  bool encodeTriple2SubEntityBitSet(EntityBitSet& _bitset, TYPE_PREDICATE_ID _pre_id, TYPE_ENTITY_LITERAL_ID _obj_id);
	// encode Triple into Object EntityBitSet
	//  bool encodeTriple2ObjEntityBitSet(EntityBitSet& _bitset, const Triple* _p_triple);
	//  bool encodeTriple2ObjEntityBitSet(EntityBitSet& _bitset, TYPE_PREDICATE_ID _pre_id, TYPE_ENTITY_LITERAL_ID _sub_id);

	// bool calculateEntityBitSet(TYPE_ENTITY_LITERAL_ID _entity_id, EntityBitSet & _bitset);

	// check whether the relative 3-tuples exist
	// usually, through sp2olist
	bool exist_triple(TYPE_ENTITY_LITERAL_ID _sub_id, TYPE_PREDICATE_ID _pre_id, TYPE_ENTITY_LITERAL_ID _obj_id, shared_ptr<Transaction> txn = nullptr);
	bool exist_triple(const TripleWithObjType &_triple, shared_ptr<Transaction> txn = nullptr);

	//* _rdf_file denotes the path of the RDF file, where stores the rdf data
	//* there are many step in this function, each one responds to an sub-function
	//* 1. map sub2id and pre2id
	//* 2. map literal2id and encode RDF data into signature,
	//*    storing in binary file: this->getSignatureBFile(), the order responds to subID
	//*    also, store six_tuples in file: this->getSixTuplesFile()
	//* 3. build: subID2objIDlist, <subIDpreID>2objIDlist subID2<preIDobjID>list
	//* 4. build: objID2subIDlist, <objIDpreID>2subIDlist objID2<preIDsubID>list
	// encodeRDF_new invoke new rdfParser to solve task 1 & 2 in one time scan.
	bool encodeRDF_new(const string _rdf_file);
	// add param to store the parse error tuple
	bool encodeRDF_new(const string _rdf_file, const string _error_log);
	void readIDTuples(ID_TUPLE *&_p_id_tuples);
	void build_s2xx(ID_TUPLE *);
	void build_o2xx(ID_TUPLE *);
	void build_p2xx(ID_TUPLE *);

	// insert and delete, notice that modify is not needed here
	// we can read from file or use sparql syntax
	bool insertTriple(const TripleWithObjType &_triple, vector<unsigned> *_vertices = NULL, vector<unsigned> *_predicates = NULL, shared_ptr<Transaction> txn = nullptr);
	bool removeTriple(const TripleWithObjType &_triple, vector<unsigned> *_vertices = NULL, vector<unsigned> *_predicates = NULL, shared_ptr<Transaction> txn = nullptr);
	// NOTICE:one by one is too costly, sort and insert/delete at a time will be better
	unsigned insert(const TripleWithObjType *_triples, TYPE_TRIPLE_NUM _triple_num, bool _is_restore = false, shared_ptr<Transaction> txn = nullptr);
	// bool insert(const vector<TripleWithObjType>& _triples, vector<int>& _vertices, vector<int>& _predicates);
	unsigned remove(const TripleWithObjType *_triples, TYPE_TRIPLE_NUM _triple_num, bool _is_restore = false, shared_ptr<Transaction> txn = nullptr);

	unsigned batch_insert(const TripleWithObjType *_triples, TYPE_TRIPLE_NUM _triple_num, bool _is_restore = false, shared_ptr<Transaction> txn = nullptr);
	unsigned batch_remove(const TripleWithObjType *_triples, TYPE_TRIPLE_NUM _triple_num, bool _is_restore = false, shared_ptr<Transaction> txn = nullptr);

	void sub_batch_update(vector<ID_TUPLE> id_tuples, TYPE_TRIPLE_NUM _triple_num, unsigned &update_num, UPDATE_TYPE type, shared_ptr<Transaction> txn = nullptr);
	static void run_batch_update(vector<ID_TUPLE> id_tuples, TYPE_TRIPLE_NUM _triple_num, unsigned &update_num, UPDATE_TYPE type, shared_ptr<Transaction> txn = nullptr);

	bool sub2id_pre2id_obj2id_RDFintoSignature(const string _rdf_file);
	bool sub2id_pre2id_obj2id_RDFintoSignature(const string _rdf_file, const string _error_log);
	// bool literal2id_RDFintoSignature(const string _rdf_file, int** _p_id_tuples, TYPE_TRIPLE_NUM _id_tuples_max);

	bool objIDIsEntityID(TYPE_ENTITY_LITERAL_ID _id);

	//* join on the vector of CandidateList, available after retrieve from the VSTREE
	//* and store the resut in _result_set

	// bool join(vector<int*>& _result_list, int _var_id, int _pre_id, int _var_id2, const char _edge_type, int _var_num, bool shouldAddLiteral, IDList& _can_list);

	// bool select(vector<int*>& _result_list, int _var_id, int _pre_id, int _var_id2, const char _edge_type, int _var_num);

	// get the final string result_set from SPARQLquery
	bool getFinalResult(SPARQLquery &_sparql_q, ResultSet &_result_set);

	static int read_update_log(const string _path, multiset<string> &_i, multiset<string> &_r);
	bool restore_update(multiset<string> &_i, multiset<string> &_r);
	void clear_update_log();
	bool write_update_log(const TripleWithObjType *_triples, TYPE_TRIPLE_NUM _triple_num, int type, shared_ptr<Transaction> txn);



	/// insert/remove methods for building. modified from 
	/// bool insertTriple(const TripleWithObjType &_triple, vector<unsigned> *_vertices = NULL, vector<unsigned> *_predicates = NULL, shared_ptr<Transaction> txn = nullptr);
	/// bool removeTriple(const TripleWithObjType &_triple, vector<unsigned> *_vertices = NULL, vector<unsigned> *_predicates = NULL, shared_ptr<Transaction> txn = nullptr);
public:
	bool AddNode(const std::set<TYPE_LABEL_ID>& node_label_id, std::vector<TYPE_PROPERTY_ID> props_id, std::vector<GPStore::Value> values, TYPE_ENTITY_LITERAL_ID& new_node_vid, std::shared_ptr<Transaction> txn = nullptr);

	bool AddEdge(TYPE_ENTITY_LITERAL_ID src, TYPE_ENTITY_LITERAL_ID dst, TYPE_PREDICATE_ID edge_type, std::vector<GPStore::int_64> edge_props = {}, std::shared_ptr<Transaction> txn = nullptr);

	bool EraseEdge(TYPE_ENTITY_LITERAL_ID node_1, TYPE_ENTITY_LITERAL_ID node_2, char edge_dir, const std::string &edge_type, std::shared_ptr<Transaction> txn = nullptr);

	bool EraseNode(TYPE_ENTITY_LITERAL_ID node_id, std::shared_ptr<Transaction> txn = nullptr);

	/**
	 * @deprecated Please dont use it 
	 * @brief insert node by node name, and its labels.
	 * 
	 * @param node node name
	 * @param labels node's labels
	 * @param txn transaction
	 * @return node id
	 */
	unsigned InsertNode(std::string node, std::vector<std::string> labels, std::shared_ptr<Transaction> txn = nullptr);

    unsigned FindOrAllocNodeId(const std::string &node);
	TYPE_PREDICATE_ID FindOrAllocPreId(const std::string &predicate);
	TYPE_PROPERTY_ID FindOrAllocPropId(const std::string &prop_name);
	std::vector<TYPE_LABEL_ID > FindOrAllocLabelId(const std::vector<std::string> &labels);

	TYPE_PROPERTY_ID FindOrAllocPropertyID(const std::string &prop_str);

    bool AccumulateNodeLabels(TYPE_ENTITY_LITERAL_ID node_id, const std::vector<std::string> &labels,
                        std::vector<ID_TUPLE> &spo_label_tuple, std::shared_ptr<Transaction> txn = nullptr);

    bool InsertNodeLabels(TYPE_ENTITY_LITERAL_ID node_id, const std::vector<std::string> &labels, std::map<TYPE_LABEL_ID, std::set<TYPE_ENTITY_LITERAL_ID> >& label2node_list, std::shared_ptr<Transaction> txn = nullptr);
	std::vector<TYPE_LABEL_ID> InsertNodeLabels_new_version(TYPE_ENTITY_LITERAL_ID node_id, const std::vector<TYPE_LABEL_ID > &labels,
									  						const std::vector<std::string> &add_labels,
									  						std::map<TYPE_LABEL_ID, std::set<TYPE_ENTITY_LITERAL_ID>> &label2node_list);

	/**
	 * @brief add/del properties for a given node
	 * 
	 * @param node_id node id
	 * @param add_prop k-v pair of properties if add properties
	 * @param delete_prop k-v pair of properties if del properties
	 * @param txn 
	 * 
	 * @note insert only or delete only
	 */
	void UpdateNodeProp(TYPE_ENTITY_LITERAL_ID node_id, const std::map<std::string, GPStore::Value> &add_prop, const std::vector<std::string> &delete_prop, std::shared_ptr<Transaction> txn = nullptr);

	/**
	 * @brief link two node with given predicate
	 * 
	 * @param source_id source node id
	 * @param target_id 
	 * @param predicate predicate name
	 * @param txn 
	 * @return TYPE_EDGE_ID 
	 */
	TYPE_EDGE_ID InsertEdge(TYPE_ENTITY_LITERAL_ID source_id, TYPE_ENTITY_LITERAL_ID target_id, const std::string &predicate, std::shared_ptr<Transaction> txn = nullptr);
    TYPE_EDGE_ID InsertEdge(std::string source_name, std::string target_name, std::string predicate, std::shared_ptr<Transaction> txn = nullptr);

    std::pair<TYPE_PREDICATE_ID , TYPE_EDGE_ID> AllocPredicateIdAndEdgeId(TYPE_ENTITY_LITERAL_ID source_id, TYPE_ENTITY_LITERAL_ID target_id, std::string predicate, std::shared_ptr<Transaction> txn = nullptr);

	/**
	 * @brief add/del properties for edge
	 * 
	 * @param edge_id 
	 * @param add_prop k-v pair of properties if add properties
	 * @param delete_prop k-v pair of properties if delete properties
	 * @param txn 
	 */
	void UpdateEdgeProp(TYPE_EDGE_ID edge_id, const std::map<std::string, GPStore::Value> &add_prop, const std::vector<std::string> &delete_prop, std::shared_ptr<Transaction> txn = nullptr);
	
	/**
	 * @brief remove given node
	 * 
	 * @param node_id
	 * @param detach_delete delete the node and its related edges if True, only delete isolated node if False. If fail to delete, return false.
	 * @param txn 
	 * @return true 
	 * @return false 
	 */
	bool DeleteNode(TYPE_ENTITY_LITERAL_ID node_id, bool detach_delete, std::shared_ptr<Transaction> txn = nullptr);

	/**
	 * @brief remove given edge
	 * 
	 * @param edge_id 
	 * @param txn 
	 * @return true 
	 * @return false 
	 */
	bool DeleteEdge(TYPE_EDGE_ID edge_id, std::shared_ptr<Transaction> txn = nullptr);
	
	/**
	 * @brief add labels for given node
	 * 
	 * @param node_id 
	 * @param labels 
	 * @param txn 
	 * 
	 * @details materialize labels as nodes. 
	 */
	void AddNodeLabel(TYPE_ENTITY_LITERAL_ID node_id, std::vector<std::string> labels, std::shared_ptr<Transaction> txn = nullptr);

	/**
	 * @brief remove labels for given node
	 * 
	 * @param node_id 
	 * @param labels 
	 * @param txn 
	 * 
	 * @details materialize labels as nodes. 
	 */
	void RemoveNodeLabel(TYPE_ENTITY_LITERAL_ID node_id, std::vector<std::string> labels, std::shared_ptr<Transaction> txn = nullptr);

    void SetStringIndex();

	void testExtraProp();
	///
};

#endif //_DATABASE_DATABASE_H
