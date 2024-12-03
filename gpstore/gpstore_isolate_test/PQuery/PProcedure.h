#ifndef PQUERY_PPROCEDURE_H
#define PQUERY_PPROCEDURE_H

#include "../KVstore/KVstore.h"
#include "../Util/Value.h"
#include "PTempResult.h"
#include "Node.h"
#include "Edge.h"
#include "../tools/tsl/hopscotch_map.h"
#include "PShortestPath.h"
#include "../Database/Database.h"

enum TruncationOrderType {
    TIMESTAMP_DESCENDING, TIMESTAMP_ASCENDING, AMOUNT_ASCENDING, AMOUNT_DESCENDING
};
struct EdgeInfoList
{
    TYPE_ENTITY_LITERAL_ID* dst_ids = nullptr; 
    unsigned dst_num = 0;
    long long* edge_props = nullptr; 
    unsigned edge_prop_num = 0;
    void Reset()
    {
        delete[] dst_ids;
        delete[] edge_props;
        dst_ids = nullptr; 
        edge_props = nullptr; 
        dst_num = 0;
        edge_prop_num = 0;
    }
    ~EdgeInfoList()
    {
        Reset();
    }
};
inline double LL2double(long long a)
{
    union U
    {
        long long l;
        double d;
    }u;
    u.l = a;
    return u.d;
}

class PProcedure {
public:
    static unsigned n;

    static TYPE_PREDICATE_ID comment_has_creator_pre_id;
    static TYPE_PREDICATE_ID comment_has_tag_pre_id;
    static TYPE_PREDICATE_ID comment_is_located_in_pre_id;
    static TYPE_PREDICATE_ID container_of_pre_id;
    static TYPE_PREDICATE_ID forum_has_tag_pre_id;
    static TYPE_PREDICATE_ID has_interest_pre_id;
    static TYPE_PREDICATE_ID has_member_pre_id;
    static TYPE_PREDICATE_ID has_moderator_pre_id;
    static TYPE_PREDICATE_ID has_type_pre_id;
    static TYPE_PREDICATE_ID is_part_of_pre_id;
    static TYPE_PREDICATE_ID is_subclass_of_pre_id;
    static TYPE_PREDICATE_ID knows_pre_id;
    static TYPE_PREDICATE_ID likes_pre_id;
    static TYPE_PREDICATE_ID organisation_is_located_in_pre_id;
    static TYPE_PREDICATE_ID person_is_located_in_pre_id;
    static TYPE_PREDICATE_ID post_has_creator_pre_id;
    static TYPE_PREDICATE_ID post_has_tag_pre_id;
    static TYPE_PREDICATE_ID post_is_located_in_pre_id;
    static TYPE_PREDICATE_ID reply_of_comment_pre_id;
    static TYPE_PREDICATE_ID reply_of_post_pre_id;
    static TYPE_PREDICATE_ID study_at_pre_id;
    static TYPE_PREDICATE_ID work_at_pre_id;

    static TYPE_PROPERTY_ID commenthascreator_creationdate_prop_id;
    static TYPE_PROPERTY_ID comment_creator_prop_id;
    static TYPE_PROPERTY_ID comment_replyofcomment_prop_id;
    static TYPE_PROPERTY_ID comment_replyofpost_prop_id;
    static TYPE_PROPERTY_ID person_place_prop_id;
    static TYPE_PROPERTY_ID posthascreator_creationdate_prop_id;
    static TYPE_PROPERTY_ID post_container_prop_id;
    static TYPE_PROPERTY_ID post_creator_prop_id;
    static TYPE_PROPERTY_ID replyof_creationdate_prop_id;
    static TYPE_PROPERTY_ID birthday_prop_id;
    static TYPE_PROPERTY_ID browserused_prop_id;
    static TYPE_PROPERTY_ID content_prop_id;
    static TYPE_PROPERTY_ID creationdate_prop_id;
    static TYPE_PROPERTY_ID email_prop_id;
    static TYPE_PROPERTY_ID firstname_prop_id;
    static TYPE_PROPERTY_ID gender_prop_id;
    static TYPE_PROPERTY_ID id_prop_id;
    static TYPE_PROPERTY_ID imagefile_prop_id;
    static TYPE_PROPERTY_ID language_prop_id;
    static TYPE_PROPERTY_ID lastname_prop_id;
    static TYPE_PROPERTY_ID length_prop_id;
    static TYPE_PROPERTY_ID locationip_prop_id;
    static TYPE_PROPERTY_ID name_prop_id;
    static TYPE_PROPERTY_ID speaks_prop_id;
    static TYPE_PROPERTY_ID title_prop_id;
    static TYPE_PROPERTY_ID workfrom_prop_id;
    static TYPE_PROPERTY_ID organisation_place_prop_id;

    static TYPE_ENTITY_LITERAL_ID comment_label_id;
    static TYPE_ENTITY_LITERAL_ID country_label_id;
    static TYPE_ENTITY_LITERAL_ID forum_label_id;
    static TYPE_ENTITY_LITERAL_ID message_label_id;
    static TYPE_ENTITY_LITERAL_ID person_label_id;
    static TYPE_ENTITY_LITERAL_ID post_label_id;
    static TYPE_ENTITY_LITERAL_ID tagclass_label_id;
    static TYPE_ENTITY_LITERAL_ID tag_label_id;
    static TYPE_ENTITY_LITERAL_ID city_label_id;
    static TYPE_ENTITY_LITERAL_ID university_label_id;
    static TYPE_ENTITY_LITERAL_ID company_label_id;

    static void ic1(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
        const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn = nullptr);
    static void ic2(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
        const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn = nullptr);
    static void ic3(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
        const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn = nullptr);
    static void ic4(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
        const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn = nullptr);
    static void ic5(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
        const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn = nullptr);
    static void ic6(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
        const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn = nullptr);
    static void ic7(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
        const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn = nullptr);
    static void ic8(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
        const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn = nullptr);
    static void ic9(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
        const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn = nullptr);
    static void ic10(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
        const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn = nullptr);
    static void ic11(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
        const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn = nullptr);
    static void ic12(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
        const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn = nullptr);
    static void ic13(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
        const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn = nullptr);
    static void ic14_tugraph(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
        const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn = nullptr);
    static void ic14(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
        const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn = nullptr);
    static void is1(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
        const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn = nullptr);
    static void is2(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
        const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn = nullptr);
    static void is3(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
        const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn = nullptr);
    static void is4(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
        const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn = nullptr);
    static void is5(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
        const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn = nullptr);
    static void is6(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
        const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn = nullptr);
    static void is7(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
        const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn = nullptr);
    static void iu1(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
        const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn = nullptr);
    static void iu2(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
        const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn = nullptr);
    static void iu3(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
        const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn = nullptr);
    static void iu4(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
        const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn = nullptr);
    static void iu5(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
        const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn = nullptr);
    static void iu6(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
        const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn = nullptr);
    static void iu7(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
        const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn = nullptr);
    static double GetIC14KnowsWeight(const KVstore * kvstore, TYPE_ENTITY_LITERAL_ID person1_vid, TYPE_ENTITY_LITERAL_ID person2_vid,
                                     std::unordered_map<GPStore::uint_32, std::vector<GPStore::uint_32>> &postCache,
                                     std::unordered_map<GPStore::uint_32, std::vector<GPStore::uint_32>> &commentCache,
                                     std::unordered_map<GPStore::uint_32, std::unordered_map<GPStore::uint_32, unsigned>> &commReplyOfCache);
    static void iu8(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
        const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn = nullptr);
    typedef void (*ProcPtr)(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
        const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn);
    static ProcPtr getProcPtrByName(const std::string &procName) {
        auto it = functionMap.find(procName);
        if (it == functionMap.end())
            return nullptr;
        return it->second;
    }

    static void test(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
        const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn = nullptr);
private:
    static std::unordered_map<std::string, ProcPtr> functionMap;
    static int IC14getKnowsShortestPathLen(int src, int dst, const KVstore *kvstore);
    static int GetShortestPathLen(GPStore::uint_32 src, GPStore::uint_32 dst, bool directed, \
        const std::vector<TYPE_PREDICATE_ID> &pred_set, std::vector<GPStore::uint_32> &result, const KVstore *kvstore);
    static void IC14kHopEnumeratePath(int src, int dst, int k, std::vector<std::vector<int>> &result, const KVstore *kvstore);
    static int IC14bc_dfs(int src, int dst, int &retBudget, int k, std::vector<int> &s, std::unordered_map<int, int> &bar, \
        std::vector<std::vector<int>> &result, const KVstore *kvstore);
    static void EnumerateAllShortestPaths(const KVstore *kvstore, GPStore::int_64 start_vid, GPStore::int_64 end_vid, std::unique_ptr<PTempResult> &new_result);
    static void EnumeratePartialPaths(const KVstore *kvstore, const tsl::hopscotch_map<int64_t, int>& hop_info,
                           std::vector<std::pair<double, std::vector<int64_t> > >& paths, const int64_t person_vid,
                           const int depth, const double path_weight, std::vector<int64_t>& path, const bool reverse, TYPE_PREDICATE_ID knowsPred);
    static void getPostComments(unsigned vid, std::vector<GPStore::uint_32> &postVec, std::vector<GPStore::uint_32> &commentVec, const KVstore *kvstore, std::shared_ptr<Transaction> txn=nullptr);
    static void getCommReplyOf(const std::vector<GPStore::uint_32> &commentVec, std::unordered_map<GPStore::uint_32, unsigned> &commReplyOf, const KVstore *kvstore, std::shared_ptr<Transaction> txn=nullptr);
    static void getNeighbor(GPStore::uint_32 src, char edge_dir, const std::vector<TYPE_PREDICATE_ID> &pred_set, std::vector<GPStore::uint_32> &neighbors, const KVstore *kvstore, std::shared_ptr<Transaction> txn=nullptr);
    static void ProcessFriendMessages(unsigned friend_node_id, GPStore::Value &max_date, unsigned limit_num, const KVstore *kvstore, std::shared_ptr<Transaction> txn,
                                      std::set<std::tuple<GPStore::Value, GPStore::Value, GPStore::Value, GPStore::Value, GPStore::Value, GPStore::Value>>& candidates);
    static void GetPersonLocatedInCountry(Node& country_node, const KVstore *kvstore, std::shared_ptr<Transaction> txn, std::set<unsigned>& person_id_set);
    static void GetMessagesInIC3(Node& country_node, GPStore::Value& start_date, GPStore::Value& duration, const KVstore* kvstore, std::shared_ptr<Transaction> txn, std::vector<std::pair<unsigned, unsigned>>& msg_list);
    static void ProcessFriendPost(unsigned friend_node_id, GPStore::Value& start_time, GPStore::Value& end_time, const KVstore* kvstore, std::shared_ptr<Transaction> txn, std::map<uint32_t, std::pair<long long, uint32_t>>& tag_stats);
    static void ProcessPersonPosts(Node& person, std::pair<std::set<GPStore::int_64>,\
            std::vector<std::tuple<GPStore::int_64, GPStore::int_64>> > & post_counts,
                                   const GPStore::int_64 min_date);
    static void ProcessPersonCommentsIC12(Node& person, std::set<std::tuple<int32_t, int64_t, std::vector<int64_t>, int64_t>> &candidates,
        const tsl::hopscotch_map<TYPE_ENTITY_LITERAL_ID, std::string> &tagInfo, size_t limit_results);
    template <class T>
    static std::vector<T> ForEachVertexEdge(const KVstore *kv_store, std::shared_ptr<Transaction> txn, const std::vector<TYPE_ENTITY_LITERAL_ID> &vertices,
                                     const std::vector<TYPE_EDGE_ID> &edges, std::function<T(const KVstore *, Node &, Edge &)> work,
                                     size_t parallel_factor = 8)
    {
        int num = (int)vertices.size(), stride = int(num/parallel_factor);
        std::vector<T> results(num);
        std::vector<std::thread> workers;

        for(int i = 0; i < parallel_factor; ++i){
            int start_idx = i * stride, end_idx = (i + 1)*stride;
            if(i == parallel_factor - 1){
                end_idx = num;
            }
            workers.emplace_back([kv_store, &txn, &vertices, &edges, start_idx, end_idx, &results, work](){
                for(int i = start_idx; i < end_idx; ++i){
                    Node node(vertices[i], kv_store, txn);
                    Edge edge(edges[i], kv_store, txn);
                    results[i] = work(kv_store, node, edge);
                }
            });
        }

        for(int i = 0; i < parallel_factor; ++i){
            workers[i].join();
        }

        return results;
    }

    template <class T>
    static T ForEachVertex(const KVstore *kv_store, std::shared_ptr<Transaction> txn, const std::vector<TYPE_ENTITY_LITERAL_ID> &vertices,
                           std::function<void(Node&, T &)>work, std::function<void(const T &, T &)> reduce,
                           size_t parallel_factor = 8){
        T results;
        std::mutex result_lock;
        std::vector<std::thread> workers;
        int num = (int)vertices.size(), stride = int(num/parallel_factor);

        for(int i = 0; i < parallel_factor; ++i){
            int start_idx = i * stride, end_idx = (i + 1)*stride;
            if(i == parallel_factor - 1){
                end_idx = num;
            }
            workers.emplace_back([kv_store, &txn, &vertices, start_idx, end_idx, &results, work, reduce, &result_lock](){
                for(int i = start_idx; i < end_idx; ++i){
                    T local_results;
                    Node node(vertices[i], kv_store, txn);
                    work(node, local_results);
                    result_lock.lock();
                    reduce(local_results, results);
                    result_lock.unlock();
                }
            });
        }

        for(int i = 0; i < parallel_factor; ++i){
            workers[i].join();
        }

        return results;
    }

    // FinBench
    static void tcr1(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
        const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn = nullptr);
    static void tcr2(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
        const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn = nullptr);
    static void tcr3(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
        const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn = nullptr);
    static void tcr4(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
        const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn = nullptr);
    static void tcr5(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
        const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn = nullptr);
    static void tcr6(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
        const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn = nullptr);
    static void tcr7(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
        const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn = nullptr);
    static void tcr8(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
        const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn = nullptr);
    static void tcr9(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
        const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn = nullptr);
    static void tcr10(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
        const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn = nullptr);
    static void tcr11(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
        const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn = nullptr);
    static void tcr12(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
        const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn = nullptr);
    static void tsr1(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
        const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn = nullptr);
    static void tsr2(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
        const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn = nullptr);
    static void tsr3(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
        const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn = nullptr);
    static void tsr4(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
        const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn = nullptr);
    static void tsr5(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
        const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn = nullptr);
    static void tsr6(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
        const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn = nullptr);
    static void tw1(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
        const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn = nullptr);
    static void tw2(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
        const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn = nullptr);
    static void tw3(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
        const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn = nullptr);
    static void tw4(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
        const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn = nullptr);
    static void tw5(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
        const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn = nullptr);
    static void tw6(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
        const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn = nullptr);
    static void tw7(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
        const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn = nullptr);
    static void tw8(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
        const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn = nullptr);
    static void tw9(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
        const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn = nullptr);
    static void tw10(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
        const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn = nullptr);
    static void tw11(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
        const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn = nullptr);
    static void tw12(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
        const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn = nullptr);
    static void tw13(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
        const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn = nullptr);
    static void tw14(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
        const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn = nullptr);
    static void tw15(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
        const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn = nullptr);
    static void tw16(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
        const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn = nullptr);
    static void tw17(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
        const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn = nullptr);
    static void tw18(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
        const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn = nullptr);
    static void tw19(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
        const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn = nullptr);
    static void trw1(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
        const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn = nullptr);
    static void trw2(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
        const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn = nullptr);
    static void trw3(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
        const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn = nullptr);
};
int Truncate(TYPE_ENTITY_LITERAL_ID* neighborList , unsigned neighbor_len ,long long* prop_list , unsigned prop_len,TruncationOrderType truncationOrder,int truncationLimit );
int Truncate(EdgeInfoList l,TruncationOrderType truncationOrder,int truncationLimit);
int truncationReorder(TYPE_ENTITY_LITERAL_ID* neighborList, unsigned neighbor_len, long long* prop_list, unsigned prop_len, int prop_idx, int truncationLimit, bool asc);
void quickSortUnion(TYPE_ENTITY_LITERAL_ID* neighborList, long long* prop_list, unsigned prop_len, int prop_idx, bool asc, int left, int right);
void BFSWithPathTsOrder(const KVstore *kvstore, Node& accountNode,std::shared_ptr<Transaction> txn,int truncationLimit,TruncationOrderType& truncationOrder, unordered_map<long long, pair<double, double>>& ans, double startTime, double endTime);

#endif