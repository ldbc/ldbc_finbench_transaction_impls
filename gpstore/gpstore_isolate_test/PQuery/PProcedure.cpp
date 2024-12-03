#include "PProcedure.h"

#include <memory>
#include <set>
#include "Node.h"
#include "Edge.h"
#include "../Query/IDList.h"
#include "../tools/tsl/hopscotch_set.h"

using namespace std;

TYPE_PREDICATE_ID PProcedure::comment_has_creator_pre_id = -1;
TYPE_PREDICATE_ID PProcedure::comment_has_tag_pre_id = -1;
TYPE_PREDICATE_ID PProcedure::comment_is_located_in_pre_id = -1;
TYPE_PREDICATE_ID PProcedure::container_of_pre_id = -1;
TYPE_PREDICATE_ID PProcedure::forum_has_tag_pre_id = -1;
TYPE_PREDICATE_ID PProcedure::has_interest_pre_id = -1;
TYPE_PREDICATE_ID PProcedure::has_member_pre_id = -1;
TYPE_PREDICATE_ID PProcedure::has_moderator_pre_id = -1;
TYPE_PREDICATE_ID PProcedure::has_type_pre_id = -1;
TYPE_PREDICATE_ID PProcedure::is_part_of_pre_id = -1;
TYPE_PREDICATE_ID PProcedure::is_subclass_of_pre_id = -1;
TYPE_PREDICATE_ID PProcedure::knows_pre_id = -1;
TYPE_PREDICATE_ID PProcedure::likes_pre_id = -1;
TYPE_PREDICATE_ID PProcedure::organisation_is_located_in_pre_id = -1;
TYPE_PREDICATE_ID PProcedure::person_is_located_in_pre_id = -1;
TYPE_PREDICATE_ID PProcedure::post_has_creator_pre_id = -1;
TYPE_PREDICATE_ID PProcedure::post_has_tag_pre_id = -1;
TYPE_PREDICATE_ID PProcedure::post_is_located_in_pre_id = -1;
TYPE_PREDICATE_ID PProcedure::reply_of_comment_pre_id = -1;
TYPE_PREDICATE_ID PProcedure::reply_of_post_pre_id = -1;
TYPE_PREDICATE_ID PProcedure::study_at_pre_id = -1;
TYPE_PREDICATE_ID PProcedure::work_at_pre_id = -1;

TYPE_PROPERTY_ID PProcedure::commenthascreator_creationdate_prop_id = -1;
TYPE_PROPERTY_ID PProcedure::comment_creator_prop_id = -1;
TYPE_PROPERTY_ID PProcedure::comment_replyofcomment_prop_id = -1;
TYPE_PROPERTY_ID PProcedure::comment_replyofpost_prop_id = -1;
TYPE_PROPERTY_ID PProcedure::person_place_prop_id = -1;
TYPE_PROPERTY_ID PProcedure::posthascreator_creationdate_prop_id = -1;
TYPE_PROPERTY_ID PProcedure::post_container_prop_id = -1;
TYPE_PROPERTY_ID PProcedure::post_creator_prop_id = -1;
TYPE_PROPERTY_ID PProcedure::replyof_creationdate_prop_id = -1;
TYPE_PROPERTY_ID PProcedure::birthday_prop_id = -1;
TYPE_PROPERTY_ID PProcedure::browserused_prop_id = -1;
TYPE_PROPERTY_ID PProcedure::content_prop_id = -1;
TYPE_PROPERTY_ID PProcedure::creationdate_prop_id = -1;
TYPE_PROPERTY_ID PProcedure::email_prop_id = -1;
TYPE_PROPERTY_ID PProcedure::firstname_prop_id = -1;
TYPE_PROPERTY_ID PProcedure::gender_prop_id = -1;
TYPE_PROPERTY_ID PProcedure::id_prop_id = -1;
TYPE_PROPERTY_ID PProcedure::imagefile_prop_id = -1;
TYPE_PROPERTY_ID PProcedure::language_prop_id = -1;
TYPE_PROPERTY_ID PProcedure::lastname_prop_id = -1;
TYPE_PROPERTY_ID PProcedure::length_prop_id = -1;
TYPE_PROPERTY_ID PProcedure::locationip_prop_id = -1;
TYPE_PROPERTY_ID PProcedure::name_prop_id = -1;
TYPE_PROPERTY_ID PProcedure::speaks_prop_id = -1;
TYPE_PROPERTY_ID PProcedure::title_prop_id = -1;
TYPE_PROPERTY_ID PProcedure::workfrom_prop_id = -1;
TYPE_PROPERTY_ID PProcedure::organisation_place_prop_id = -1;

TYPE_ENTITY_LITERAL_ID PProcedure::comment_label_id = -1;
TYPE_ENTITY_LITERAL_ID PProcedure::country_label_id = -1;
TYPE_ENTITY_LITERAL_ID PProcedure::forum_label_id = -1;
TYPE_ENTITY_LITERAL_ID PProcedure::message_label_id = -1;
TYPE_ENTITY_LITERAL_ID PProcedure::person_label_id = -1;
TYPE_ENTITY_LITERAL_ID PProcedure::post_label_id = -1;
TYPE_ENTITY_LITERAL_ID PProcedure::tagclass_label_id = -1;
TYPE_ENTITY_LITERAL_ID PProcedure::tag_label_id = -1;
TYPE_ENTITY_LITERAL_ID PProcedure::city_label_id = -1;
TYPE_ENTITY_LITERAL_ID PProcedure::university_label_id = -1;
TYPE_ENTITY_LITERAL_ID PProcedure::company_label_id = -1;

#ifdef DEBUG_PG
static std::map<std::string, std::pair<unsigned, unsigned>> CostRecorder;
#define START_TIMING(s) CostRecorder[s].first = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
#define END_TIMING(s) CostRecorder[s].second = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
#define OUTPUT_TIMING() for (auto it: CostRecorder) { std::cout <<"[GPSTORE] " << it.first << ": " << it.second.second - it.second.first << "us" << std::endl; }
#else
#define START_TIMING(s)
#define END_TIMING(s)
#define OUTPUT_TIMING()
#endif

const unsigned LIMIT_NUM = 20;

std::unordered_map<std::string, PProcedure::ProcPtr> PProcedure::functionMap = {
    {"ic1", &PProcedure::ic1}, {"ic2", &PProcedure::ic2}, {"ic3", &PProcedure::ic3}, {"ic4", &PProcedure::ic4},
    {"ic5", &PProcedure::ic5}, {"ic6", &PProcedure::ic6}, {"ic7", &PProcedure::ic7}, {"ic8", &PProcedure::ic8},
    {"ic9", &PProcedure::ic9}, {"ic10", &PProcedure::ic10}, {"ic11", &PProcedure::ic11}, {"ic12", &PProcedure::ic12},
    {"ic13", &PProcedure::ic13}, {"ic14", &PProcedure::ic14}, {"is1", &PProcedure::is1}, {"is2", &PProcedure::is2},
    {"is3", &PProcedure::is3}, {"is4", &PProcedure::is4}, {"is5", &PProcedure::is5}, {"is6", &PProcedure::is6},
    {"is7", &PProcedure::is7}, {"iu1", &PProcedure::iu1}, {"iu2", &PProcedure::iu2}, {"iu3", &PProcedure::iu3},
    {"iu4", &PProcedure::iu4}, {"iu5", &PProcedure::iu5}, {"iu6", &PProcedure::iu6}, {"iu7", &PProcedure::iu7},
    {"iu8", &PProcedure::iu8}, {"test", &PProcedure::test},
    {"tcr1", &PProcedure::tcr1}, {"tcr2", &PProcedure::tcr2}, {"tcr3", &PProcedure::tcr3}, {"tcr4", &PProcedure::tcr4},
    {"tcr5", &PProcedure::tcr5}, {"tcr6", &PProcedure::tcr6}, {"tcr7", &PProcedure::tcr7}, {"tcr8", &PProcedure::tcr8},
    {"tcr9", &PProcedure::tcr9}, {"tcr10", &PProcedure::tcr10}, {"tcr11", &PProcedure::tcr11}, {"tcr12", &PProcedure::tcr12},
    {"tsr1", &PProcedure::tsr1}, {"tsr2", &PProcedure::tsr2}, {"tsr3", &PProcedure::tsr3}, {"tsr4", &PProcedure::tsr4},
    {"tsr5", &PProcedure::tsr5}, {"tsr6", &PProcedure::tsr6},
    {"tw1", &PProcedure::tw1}, {"tw2", &PProcedure::tw2}, {"tw3", &PProcedure::tw3}, {"tw4", &PProcedure::tw4},
    {"tw5", &PProcedure::tw5}, {"tw6", &PProcedure::tw6}, {"tw7", &PProcedure::tw7}, {"tw8", &PProcedure::tw8},
    {"tw9", &PProcedure::tw9}, {"tw10", &PProcedure::tw10}, {"tw11", &PProcedure::tw11}, {"tw12", &PProcedure::tw12},
    {"tw13", &PProcedure::tw13}, {"tw14", &PProcedure::tw14}, {"tw15", &PProcedure::tw15}, {"tw16", &PProcedure::tw16},
    {"tw17", &PProcedure::tw17}, {"tw18", &PProcedure::tw1}, {"tw19", &PProcedure::tw19},
    {"trw1", &PProcedure::trw1}, {"trw2", &PProcedure::trw2}, {"trw3", &PProcedure::trw3}
};

void quickSortUnion(TYPE_ENTITY_LITERAL_ID* neighborList, long long* prop_list, unsigned prop_len, int prop_idx, bool asc, int left, int right) {
    if (left >= right) return;

    int pivot = prop_list[prop_len * right + prop_idx];
    int i = left - 1;

    for (int j = left; j < right; ++j) {
        if ((asc && prop_list[prop_len * j + prop_idx] <= pivot) || (!asc && prop_list[prop_len * j + prop_idx] >= pivot)) {
            ++i;
            // Swap prop_list
            for (int k = 0; k < prop_len; ++k)
                std::swap(prop_list[i * prop_len + k], prop_list[j * prop_len + k]);
            // Swap neighborList
            std::swap(neighborList[i], neighborList[j]);
        }
    }

    // Swap pivot
    for (int k = 0; k < prop_len; ++k)
        std::swap(prop_list[(i + 1) * prop_len + k], prop_list[right * prop_len + k]);
    std::swap(neighborList[i + 1], neighborList[right]);

    quickSortUnion(neighborList, prop_list, prop_len, prop_idx, asc, left, i);
    quickSortUnion(neighborList, prop_list, prop_len, prop_idx, asc, i + 2, right);
}

int truncationReorder(TYPE_ENTITY_LITERAL_ID* neighborList, unsigned neighbor_len, long long* prop_list, unsigned prop_len, int prop_idx, int truncationLimit, bool asc) {
    if (neighbor_len > truncationLimit) {
        quickSortUnion(neighborList, prop_list, prop_len, prop_idx, asc, 0, neighbor_len - 1);
        return truncationLimit;
    }
    return neighbor_len;
}

void PProcedure::EnumeratePartialPaths(const KVstore *kvstore, const tsl::hopscotch_map<int64_t, int>& hop_info,
std::vector<std::pair<double, std::vector<int64_t> > >& paths, const int64_t person_vid,
const int depth, const double path_weight, std::vector<int64_t>& path, const bool reverse, TYPE_PREDICATE_ID knowsPred) {
    // TODO: implement according to tugraph
    path.emplace_back(person_vid);
    TYPE_ENTITY_LITERAL_ID *friends = nullptr;
    GPStore::int_64 *x_list = nullptr;
    unsigned friends_num = 0, xwidth = 0;
    if (depth != 0) {
        kvstore->getobjIDxvaluesBysubIDpreID(person_vid, knowsPred, friends, x_list, xwidth, friends_num, false, nullptr);
        for (unsigned i = 0; i < friends_num; i++) {
            int64_t friend_vid = friends[i];
            auto it = hop_info.find(friend_vid);
            if (it != hop_info.end() && it->second == depth - 1) {
                double weight;
                memcpy(&weight, x_list + i * xwidth + 1, sizeof(double));   // Handle the problem because of the double -> int cast
                EnumeratePartialPaths(kvstore, hop_info, paths, friend_vid, depth - 1, path_weight + weight, path, reverse, knowsPred);
            }
        }
        delete []friends;
        delete []x_list;
        kvstore->getsubIDxvaluesByobjIDpreID(person_vid, knowsPred, friends, x_list, xwidth, friends_num, false, nullptr);
        for (unsigned i = 0; i < friends_num; i++) {
            int64_t friend_vid = friends[i];
            auto it = hop_info.find(friend_vid);
            if (it != hop_info.end() && it->second == depth - 1) {
                double weight;
                memcpy(&weight, x_list + i * xwidth + 1, sizeof(double));   // Handle the problem because of the double -> int cast
                EnumeratePartialPaths(kvstore, hop_info, paths, friend_vid, depth - 1, path_weight + weight, path, reverse, knowsPred);
            }
        }
        delete []friends;
        delete []x_list;
    } else {
        if (reverse) {
            paths.emplace_back();
            paths.back().first = path_weight;
            paths.back().second.insert(paths.back().second.end(), path.rbegin(), path.rend());
        } else {
            paths.emplace_back(path_weight, path);
        }
    }
    path.pop_back();
}

void PProcedure::EnumerateAllShortestPaths(const KVstore *kvstore, GPStore::int_64 start_vid, GPStore::int_64 end_vid, std::unique_ptr<PTempResult> &new_result) {
    TYPE_PREDICATE_ID knowsPred = knows_pre_id;
    tsl::hopscotch_map<int64_t, int> parent({{start_vid, 0}});
    tsl::hopscotch_map<int64_t, int> child({{end_vid, 0}});
    std::vector<int64_t> forward_q({start_vid});
    std::vector<int64_t> backward_q({end_vid});
    int fhop = 0;
    int bhop = 0;
    std::vector<std::tuple<int64_t, int64_t, double> > hits;
    TYPE_ENTITY_LITERAL_ID *friends = nullptr;
    GPStore::int_64 *x_list = nullptr;
    unsigned friends_num = 0, xwidth = 0;
    for (int hop = 0; !forward_q.empty() && !backward_q.empty() && hits.empty(); hop++) {
        std::vector<int64_t> next_q;
        if (forward_q.size() <= backward_q.size()) {
            fhop++;
            for (int64_t person_vid : forward_q) {
                kvstore->getobjIDxvaluesBysubIDpreID(person_vid, knowsPred, friends, x_list, xwidth, friends_num, false, nullptr);
                for (unsigned i = 0; i < friends_num; i++) {
                    int64_t friend_vid = friends[i];
                    double weight;
                    memcpy(&weight, x_list + i * xwidth + 1, sizeof(double));   // Handle the problem because of the double -> int cast
                    // double weight = x_list[i * xwidth + 1];
                    if (child.find(friend_vid) != child.end()) {
                        hits.emplace_back(person_vid, friend_vid, weight);
                    } else {
                        auto it = parent.find(friend_vid);
                        if (it == parent.end()) {
                            parent.emplace_hint(it, friend_vid, fhop);
                            next_q.emplace_back(friend_vid);
                        }
                    }
                }
                delete []friends;
                delete []x_list;
                kvstore->getsubIDxvaluesByobjIDpreID(person_vid, knowsPred, friends, x_list, xwidth, friends_num, false, nullptr);
                for (unsigned i = 0; i < friends_num; i++) {
                    int64_t friend_vid = friends[i];
                    double weight;
                    memcpy(&weight, x_list + i * xwidth + 1, sizeof(double));   // Handle the problem because of the double -> int cast
                    // double weight = x_list[i * xwidth + 1];
                    if (child.find(friend_vid) != child.end()) {
                        hits.emplace_back(person_vid, friend_vid, weight);
                    } else {
                        auto it = parent.find(friend_vid);
                        if (it == parent.end()) {
                            parent.emplace_hint(it, friend_vid, fhop);
                            next_q.emplace_back(friend_vid);
                        }
                    }
                }
                delete []friends;
                delete []x_list;
            }
            std::sort(next_q.begin(), next_q.end());
            forward_q.swap(next_q);
        } else {
            bhop++;
            for (int64_t person_vid : backward_q) {
                kvstore->getobjIDxvaluesBysubIDpreID(person_vid, knowsPred, friends, x_list, xwidth, friends_num, false, nullptr);
                for (unsigned i = 0; i < friends_num; i++) {
                    int64_t friend_vid = friends[i];
                    double weight;
                    memcpy(&weight, x_list + i * xwidth + 1, sizeof(double));   // Handle the problem because of the double -> int cast
                    // double weight = x_list[i * xwidth + 1];
                    if (parent.find(friend_vid) != parent.end()) {
                        hits.emplace_back(friend_vid, person_vid, weight);
                    } else {
                        auto it = child.find(friend_vid);
                        if (it == child.end()) {
                            child.emplace_hint(it, friend_vid, bhop);
                            next_q.emplace_back(friend_vid);
                        }
                    }
                }
                delete []friends;
                delete []x_list;

                kvstore->getsubIDxvaluesByobjIDpreID(person_vid, knowsPred, friends, x_list, xwidth, friends_num, false, nullptr);
                for (unsigned i = 0; i < friends_num; i++) {
                    int64_t friend_vid = friends[i];
                    double weight;
                    memcpy(&weight, x_list + i * xwidth + 1, sizeof(double));   // Handle the problem because of the double -> int cast
                    // double weight = x_list[i * xwidth + 1];
                    if (parent.find(friend_vid) != parent.end()) {
                        hits.emplace_back(friend_vid, person_vid, weight);
                    } else {
                        auto it = child.find(friend_vid);
                        if (it == child.end()) {
                            child.emplace_hint(it, friend_vid, bhop);
                            next_q.emplace_back(friend_vid);
                        }
                    }
                }
                delete []friends;
                delete []x_list;
            }
            std::sort(next_q.begin(), next_q.end());
            backward_q.swap(next_q);
        }
    }
    if (hits.empty())
        return;
    std::vector<std::pair<double, std::vector<int64_t> > > paths;
    for (auto& tup : hits) {
        std::vector<std::pair<double, std::vector<int64_t> > > fpaths;
        std::vector<std::pair<double, std::vector<int64_t> > > bpaths;
        std::vector<int64_t> path;
        int64_t src = std::get<0>(tup);
        int64_t dst = std::get<1>(tup);
        EnumeratePartialPaths(kvstore, parent, fpaths, src, parent[src], 0.0, path, true, knowsPred);
        EnumeratePartialPaths(kvstore, child, bpaths, dst, child[dst], 0.0, path, false, knowsPred);
        for (auto& fpath : fpaths) {
            for (auto& bpath : bpaths) {
                paths.emplace_back(fpath.first + std::get<2>(tup) + bpath.first, fpath.second);
                paths.back().second.insert(paths.back().second.end(), bpath.second.begin(), bpath.second.end());
            }
        }
    }
    tsl::hopscotch_map<int64_t, int64_t> person_info;
    for (auto it = paths.rbegin(); it != paths.rend(); it++)
        it->first = -it->first;
    std::sort(paths.begin(), paths.end());
    
    TYPE_PROPERTY_ID nodeIdProp = id_prop_id;
    for (auto it = paths.begin(); it != paths.end(); it++) {
        new_result->rows_.emplace_back();
        new_result->rows_.back().values_.resize(2);
        new_result->rows_.back().values_[0].type_ = GPStore::Value::LIST;
        size_t sz = it->second.size();
        new_result->rows_.back().values_[0].data_.List = new std::vector<GPStore::Value*>(sz, nullptr);
        for (size_t i = 0; i < sz; i++)
            kvstore->getnodeValueByid(it->second[i], nodeIdProp, new_result->rows_.back().values_[0].data_.List->at(i));
        new_result->rows_.back().values_[1] = -it->first;
    }
}

void PProcedure::ic14_tugraph(const vector<GPStore::Value> &args, unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
    if (args.size() != 2)
        return;
    GPStore::int_64 src = args[0].data_.Long, dst = args[1].data_.Long;
    if (src == dst)
        return;
    Node src_node(person_label_id, id_prop_id, &args[0], kvstore, txn);
    Node dst_node(person_label_id, id_prop_id, &args[1], kvstore, txn);
    EnumerateAllShortestPaths(kvstore, src_node.node_id_, dst_node.node_id_, new_result);
}

/**
 * @brief Stored procedure for the IC14-o query ("Trusted connection paths (old version)") of LDBC-SNB Interactive workload.
 * Given a pair of persons, first compute the shortest path length between them (where each edge is labeled by "KNOWS") by calling `IC14getKnowsShortestPathLen`, then enumerate all the shortest path between them by calling `IC14kHopEnumeratePath`, and finally compute their weights according to IC14’s rule.
 * 
 * @param args The arguments of the stored procedure
 * @param new_result The result of the stored procedure
 * @param kvstore Pointer to the KVStore instance
 * @param db Pointer to the Database instance
 * @param txn Pointer to the Transaction instance
 * @remark IC14存储过程
 */
void PProcedure::ic14(const vector<GPStore::Value> &args, unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
    if (args.size() != 2)
        return;
    if (args[0].data_.Long == args[1].data_.Long)
        return;
    Node src_node(person_label_id, id_prop_id, &args[0], kvstore, txn);
    Node dst_node(person_label_id, id_prop_id, &args[1], kvstore, txn);
    int spLen = IC14getKnowsShortestPathLen(src_node.node_id_, dst_node.node_id_, kvstore);
    if (spLen == -1)
        return;
    vector<vector<int>> allSp;
    IC14kHopEnumeratePath(src_node.node_id_, dst_node.node_id_, spLen, allSp, kvstore);    // fix the pred_set as KNOWS

    // Report weights
    double totalWeight = 0;
    TYPE_PROPERTY_ID nodeIdProp = id_prop_id;
    TYPE_ENTITY_LITERAL_ID *friends = nullptr;
    GPStore::int_64 *x_list = nullptr;
    unsigned friends_num = 0, xwidth = 0;
	for (auto &oneSp : allSp) {
        new_result->rows_.emplace_back();
        new_result->rows_.back().values_.resize(2);
        new_result->rows_.back().values_[0].type_ = GPStore::Value::LIST;
        new_result->rows_.back().values_[0].data_.List = new std::vector<GPStore::Value*>(spLen + 1);
		totalWeight = 0;
        size_t j = 0;
        for (size_t i = 0; i < spLen; i++) {
            // Read the materialized KNOWS_WEIGHT
            double curWeight = 0;
            bool foundNei = false;
            kvstore->getobjIDxvaluesBysubIDpreID(oneSp[i], knows_pre_id, friends, x_list, xwidth, friends_num, false, nullptr);
            for (unsigned k = 0; k < friends_num; ++k) {
                if (friends[k] == oneSp[i + 1]) {
                    // curWeight = x_list[k * xwidth + 1];
                    // Handle the problem because of the double -> int cast
                    foundNei = true;
                    memcpy(&curWeight, x_list + k * xwidth + 1, sizeof(double));
                    break;
                }
            }
            totalWeight += curWeight;
            delete []friends;
            delete []x_list;
            if (!foundNei) {
                kvstore->getsubIDxvaluesByobjIDpreID(oneSp[i], knows_pre_id, friends, x_list, xwidth, friends_num, false, nullptr);
                for (unsigned k = 0; k < friends_num; ++k) {
                    if (friends[k] == oneSp[i + 1]) {
                        // curWeight = x_list[k * xwidth + 1];
                        // Handle the problem because of the double -> int cast
                        memcpy(&curWeight, x_list + k * xwidth + 1, sizeof(double));
                        break;
                    }
                }
                totalWeight += curWeight;
                delete []friends;
                delete []x_list;
            }
            kvstore->getnodeValueByid(oneSp[i], nodeIdProp, new_result->rows_.back().values_[0].data_.List->at(i));
        }
        kvstore->getnodeValueByid(oneSp.back(), nodeIdProp, new_result->rows_.back().values_[0].data_.List->back());
        new_result->rows_.back().values_[1] = totalWeight;
	}
    std::sort(new_result->rows_.begin(), new_result->rows_.end(), [](const PTempResult::Record &l, const PTempResult::Record &r) {
        return l.values_[1].data_.Float > r.values_[1].data_.Float;
    });
    cout << endl;
}

/**
 * @brief A helper function for the IC14-o query ("Trusted connection paths (old version)") of LDBC-SNB Interactive workload.
 * Computes the shortest path length (where each edge is labeled by “KNOWS”) between a given pair of persons using bidirectional breadth-first search.
 * 
 * @param src The source person vertex.
 * @param dst The destination person vertex.
 * @param kvstore Pointer to the KVStore instance
 * @return int The shortest path length between the source and destination persons, where each edge is labeled by “KNOWS”, or -1 if no path exists.
 * @remark IC14帮助函数-获得最短路径长度
 */
int PProcedure::IC14getKnowsShortestPathLen(int src, int dst, const KVstore *kvstore) {
    map<int, int> dis_u, dis_v; // store the distance to u and v
    queue<int> q_u, q_v;
    q_u.push(src);
	q_v.push(dst);
	dis_u[src] = 0;
	dis_v[dst] = 0;
    while (!q_u.empty() || !q_v.empty()) {
        queue<int> new_q_u;
        queue<int> new_q_v;
        if(!q_u.empty() && q_u.size() <= q_v.size() || q_v.empty()) {
            while (!q_u.empty()) {
                int temp_u = q_u.front();
                q_u.pop();
                Node node_u(temp_u, kvstore, nullptr);
                TYPE_ENTITY_LITERAL_ID* neighborList = nullptr;
                unsigned len = 0;
                node_u.GetLinkedNodes(knows_pre_id, neighborList, len, Util::EDGE_OUT);
                for (unsigned i = 0; i < len; i++) {
                    int t = neighborList[i];
                    if (dis_v.find(t) != dis_v.end())
                        return dis_u[temp_u] + 1 + dis_v[t];
                    if (dis_u.find(t) != dis_u.end())
                        continue; // Already in
                    new_q_u.push(t);
                    dis_u[t] = dis_u[temp_u] + 1;
                }
                delete []neighborList;
                node_u.GetLinkedNodes(knows_pre_id, neighborList, len, Util::EDGE_IN);
                for (unsigned i = 0; i < len; i++) {
                    int t = neighborList[i];
                    if (dis_v.find(t) != dis_v.end())
                        return dis_u[temp_u] + 1 + dis_v[t];
                    if (dis_u.find(t) != dis_u.end())
                        continue; // Already in
                    new_q_u.push(t);
                    dis_u[t] = dis_u[temp_u] + 1;
                }
                delete []neighborList;
            }
            q_u.swap(new_q_u);
        } else {
            while (!q_v.empty()) {
                int temp_v = q_v.front();
                q_v.pop();
                Node node_v(temp_v, kvstore, nullptr);
                TYPE_ENTITY_LITERAL_ID* neighborList = nullptr;
                unsigned len = 0;
                node_v.GetLinkedNodes(knows_pre_id, neighborList, len, Util::EDGE_OUT);
                for (unsigned i = 0; i < len; i++) {
                    int t = neighborList[i];
                    if (dis_u.find(t) != dis_u.end())
                        return dis_v[temp_v] + 1 + dis_u[t];
                    if (dis_v.find(t) != dis_v.end())
                        continue; // Already in
                    new_q_v.push(t);
                    dis_v[t] = dis_v[temp_v] + 1;
                }
                delete []neighborList;
                node_v.GetLinkedNodes(knows_pre_id, neighborList, len, Util::EDGE_IN);
                for (unsigned i = 0; i < len; i++) {
                    int t = neighborList[i];
                    if (dis_u.find(t) != dis_u.end())
                        return dis_v[temp_v] + 1 + dis_u[t];
                    if (dis_v.find(t) != dis_v.end())
                        continue; // Already in
                    new_q_v.push(t);
                    dis_v[t] = dis_v[temp_v] + 1;
                }
                delete []neighborList;
            }
            q_v.swap(new_q_v);
        }
    }
    return -1;  // no route from u to v
}

int PProcedure::GetShortestPathLen(GPStore::uint_32 src, GPStore::uint_32 dst, bool directed, \
const std::vector<TYPE_PREDICATE_ID> &pred_set, std::vector<GPStore::uint_32> &result, const KVstore *kvstore) {
    // cout << "BFS2" << endl;

#ifdef DEBUG_PG
    std::chrono::microseconds t_count{0},all{0};
    auto start = std::chrono::steady_clock::now();
#endif
    map<GPStore::uint_32, int> dis_u, dis_v; // store the distance to u and v
    queue<GPStore::uint_32> q_u, q_v;
    q_u.push(src);
    q_v.push(dst);
    dis_u[src] = 0;
    dis_v[dst] = 0;
    bool flag = 0;
    map<int, int>::iterator it;
    int meet_node = 0;
    vector<GPStore::uint_32> neighbors;
    while (flag == 0 && (!q_u.empty() || !q_v.empty()))
    {
        queue<GPStore::uint_32> new_q_u;
        queue<GPStore::uint_32> new_q_v;
        if(!q_u.empty() && q_u.size() <= q_v.size() || q_v.empty())
        {
            while (!q_u.empty() && flag == 0) {
                int temp_u = q_u.front();
                q_u.pop();

#ifdef DEBUG_PG
                auto t1 = std::chrono::steady_clock::now();
#endif
                neighbors.clear();
                Node temp_u_node(temp_u, kvstore, nullptr);
                temp_u_node.GetNeighbors(Util::EDGE_OUT, pred_set, neighbors);
#ifdef DEBUG_PG
                t_count += std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - t1);
#endif

                for (const auto &t : neighbors) {
                    if (dis_v.find(t) != dis_v.end()) {
                        flag = 1;
                        meet_node = t;
                        dis_u[t] = dis_u[temp_u] + 1;
#ifdef DEBUG_PG
                        all = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start);

    cout << "[GPSTORE]get nbr: " << t_count.count() << " us" << endl;
    cout << "[GPSTORE]all: " << all.count() << " us" << endl;
#endif
                        return dis_u[t]+dis_v[t];
                        break;
                    }
                    if (dis_u.find(t) != dis_u.end())
                        continue; // Already in
                    new_q_u.push(t);
                    dis_u[t] = dis_u[temp_u] + 1;
                }
                if (flag)
                    break;
                if (directed)
                    continue;

#ifdef DEBUG_PG
                t1 = std::chrono::steady_clock::now();
#endif
                neighbors.clear();
                temp_u_node = Node(temp_u, kvstore, nullptr);
                temp_u_node.GetNeighbors(Util::EDGE_IN, pred_set, neighbors);
#ifdef DEBUG_PG
                t_count += std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - t1);
#endif
                for (const auto &t : neighbors) {
                    if (dis_v.find(t) != dis_v.end()) {
                        flag = 1;
                        meet_node = t;
                        dis_u[t] = dis_u[temp_u] + 1;
#ifdef DEBUG_PG
                        all = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start);

    cout << "[GPSTORE]get nbr: " << t_count.count() << " us" << endl;
    cout << "[GPSTORE]all: " << all.count() << " us" << endl;
#endif
                        return dis_u[t]+dis_v[t];
                        break;
                    }
                    if (dis_u.find(t) != dis_u.end())
                        continue; // Already in
                    new_q_u.push(t);
                    dis_u[t] = dis_u[temp_u] + 1;
                }
                if (flag)
                    break;
            }
            q_u.swap(new_q_u);
        }
        else
        {
            while (!q_v.empty() && flag == 0) {
                int temp_v = q_v.front();
                q_v.pop();
#ifdef DEBUG_PG
                auto t1 = std::chrono::steady_clock::now();
#endif
                neighbors.clear();
                Node temp_v_node(temp_v, kvstore, nullptr);
                temp_v_node.GetNeighbors(Util::EDGE_IN, pred_set, neighbors);
#ifdef DEBUG_PG
                t_count += std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - t1);
#endif
                for (const auto &t : neighbors) {
                    if (dis_u.find(t) != dis_u.end()) {
                        flag = 1;
                        meet_node = t;
                        dis_v[t] = dis_v[temp_v] + 1;
#ifdef DEBUG_PG
                        all = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start);

    cout << "[GPSTORE]get nbr: " << t_count.count() << " us" << endl;
    cout << "[GPSTORE]all: " << all.count() << " us" << endl;
#endif
                        return dis_u[t]+dis_v[t];
                        break;
                    }
                    if (dis_v.find(t) != dis_v.end())
                        continue; // Already in
                    new_q_v.push(t);
                    dis_v[t] = dis_v[temp_v] + 1;
                }
                if (flag)
                    break;
                if (directed)
                    continue;

#ifdef DEBUG_PG
                t1 = std::chrono::steady_clock::now();
#endif
                neighbors.clear();
                temp_v_node = Node(temp_v, kvstore, nullptr);
                temp_v_node.GetNeighbors(Util::EDGE_OUT, pred_set, neighbors);
#ifdef DEBUG_PG
                t_count += std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - t1);
#endif
                for (const auto &t : neighbors) {
                    if (dis_u.find(t) != dis_u.end()) {
                        flag = 1;
                        meet_node = t;
                        dis_v[t] = dis_v[temp_v] + 1;
#ifdef DEBUG_PG
                        all = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start);

    cout << "[GPSTORE]get nbr: " << t_count.count() << " us" << endl;
    cout << "[GPSTORE]all: " << all.count() << " us" << endl;
#endif
                        return dis_u[t]+dis_v[t];
                        break;
                    }
                    if (dis_v.find(t) != dis_v.end())
                        continue; // Already in
                    new_q_v.push(t);
                    dis_v[t] = dis_v[temp_v] + 1;
                }
                if (flag)
                    break;
            }
            q_v.swap(new_q_v);
        }



    }

    // get the route through the distance.
#ifdef DEBUG_PG
    all = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start);

    cout << "[GPSTORE]get nbr: " << t_count.count() << " us" << endl;
    cout << "[GPSTORE]all: " << all.count() << " us" << endl;
#endif
    return -1; // no route from u to v

    // cout << "*************you are wrong!!!**********" << endl;



}

// fix pred = KNOWS
/**
 * @brief A helper function for the IC14-o query ("Trusted connection paths (old version)") of LDBC-SNB Interactive workload.
 * Enumerates all the k-hop paths (where each edge is labeled by “KNOWS”) between a given pair of nodes using the BC-DFS algorithm (You Peng, Ying Zhang, Xuemin Lin, Wenjie Zhang, Lu Qin, and Jingren Zhou. 2019. Towards bridging theory and practice: hop-constrained st simple path enumeration. Proceedings of the VLDB Endowment 13, 4 (2019)).
 * 
 * @param src The source person vertex.
 * @param dst The destination person vertex.
 * @param k The hop constraint (i.e., the maximum path length) k.
 * @param result The return value that stores all the k-hop paths.
 * @param kvstore Pointer to the KVStore instance.
 * @remark IC14帮助函数-枚举k跳路径
 */
void PProcedure::IC14kHopEnumeratePath(int src, int dst, int k, std::vector<std::vector<int>> &result, const KVstore *kvstore) {
	vector<int> s;
    unordered_map<int, int> bar;
	// Use reverse kBFS from destination to refine bar
	queue<int> q;
	q.push(dst);
    bar[dst] = 0;
    TYPE_ENTITY_LITERAL_ID* neighborList = nullptr;
    unsigned len = 0;
	while (!q.empty()) {
		int cur = q.front();
		q.pop();
		if (bar.find(cur) != bar.end() && bar[cur] >= k)
			continue;
        Node cur_node(cur, kvstore, nullptr);
        cur_node.GetLinkedNodes(knows_pre_id, neighborList, len, Util::EDGE_OUT);
        for (unsigned i = 0; i < len; i++) {
            int to = neighborList[i];
            if ((bar.find(to) == bar.end() || bar[to] == 0) && to != dst) {
                bar[to] = bar[cur] + 1;
                q.push(to);
            }
        }
        delete []neighborList;
        cur_node.GetLinkedNodes(knows_pre_id, neighborList, len, Util::EDGE_IN);
        for (unsigned i = 0; i < len; i++) {
            int to = neighborList[i];
            if ((bar.find(to) == bar.end() || bar[to] == 0) && to != dst) {
                bar[to] = bar[cur] + 1;
                q.push(to);
            }
        }
        delete []neighborList;
	}

	s.emplace_back(src);	// (vertex_id, pred_id that precedes it)
	int retBudget = -1;
    result.clear();
	IC14bc_dfs(src, dst, retBudget, k, s, bar, result, kvstore);
}

// fix pred = KNOWS
int PProcedure::IC14bc_dfs(int src, int dst, int &retBudget, int k, std::vector<int> &s, \
std::unordered_map<int, int> &bar, std::vector<std::vector<int>> &result, const KVstore *kvstore) {
    int f = k + 1;
    TYPE_ENTITY_LITERAL_ID* neighborList = nullptr;
    unsigned len = 0;
	if (src == dst) {
		result.emplace_back(s);
		s.pop_back();
		if (retBudget > 0)
			retBudget--;
		return 0;
	}
	else if ((int)s.size() - 1 < k) {
        Node src_node(src, kvstore, nullptr);
        src_node.GetLinkedNodes(knows_pre_id, neighborList, len, Util::EDGE_OUT);
        for (size_t i = 0; i < len; i++) {
            int to = neighborList[i];
            bool stacked = false;
            for (int cur : s) {
                if (cur == to) {
                    stacked = true;
                    break;
                }
            }
            if (stacked)
                continue;
            if (bar.find(to) != bar.end() && (int)s.size() + bar[to] <= k) {
                if (retBudget != 0) {
                    s.emplace_back(to);
                    int next_f = IC14bc_dfs(to, dst, retBudget, k, s, bar, result, kvstore);
                    if (next_f != k + 1 && f < next_f + 1)
                        f = next_f + 1;
                }
            }
        }
        delete []neighborList;
        src_node.GetLinkedNodes(knows_pre_id, neighborList, len, Util::EDGE_IN);
        for (size_t i = 0; i < len; i++) {
            GPStore::uint_32 to = neighborList[i];
            bool stacked = false;
            for (int cur : s) {
                if (cur == to) {
                    stacked = true;
                    break;
                }
            }
            if (stacked)
                continue;
            if (bar.find(to) != bar.end() && (int)s.size() + bar[to] <= k) {
                if (retBudget != 0) {
                    s.emplace_back(to);
                    int next_f = IC14bc_dfs(to, dst, retBudget, k, s, bar, result, kvstore);
                    if (next_f != k + 1 && f < next_f + 1)
                        f = next_f + 1;
                }
            }
        }
	}

	// The following pruning not suitable for labeled multigraphs
	// if (f == k + 1)
	// 	bar[uid] = k - s.size() + 2;
	// else
	// 	updateBarrier(uid, directed, pred_set, bar, f);

	s.pop_back();
	return f;
}

void PProcedure::getPostComments(unsigned dst, std::vector<GPStore::uint_32> &postVec, std::vector<GPStore::uint_32> &commentVec,
const KVstore *kvstore, std::shared_ptr<Transaction> txn) {
    Node personNode = Node(dst, kvstore, txn);
    TYPE_ENTITY_LITERAL_ID* neighborList = nullptr;
    unsigned len = 0;
    personNode.GetLinkedNodes(post_has_creator_pre_id, neighborList, len, Util::EDGE_IN);
    postVec.assign(neighborList, neighborList + len);
    delete[] neighborList;
    personNode.GetLinkedNodes(comment_has_creator_pre_id, neighborList, len, Util::EDGE_IN);
    commentVec.assign(neighborList, neighborList + len);
    delete[] neighborList;
}

void PProcedure::getCommReplyOf(const std::vector<GPStore::uint_32> &commentVec, std::unordered_map<GPStore::uint_32, unsigned> &commReplyOf,
const KVstore *kvstore, std::shared_ptr<Transaction> txn) {
    TYPE_ENTITY_LITERAL_ID* neighborList = nullptr;
    unsigned len = 0;
    for (const GPStore::uint_32 &comm : commentVec) {
        Node commentNode = Node(comm, kvstore, txn);
        commentNode.GetLinkedNodes(reply_of_post_pre_id, neighborList, len, Util::EDGE_OUT);
        for (unsigned i = 0; i < len; i++) {
            if (commReplyOf.find(neighborList[i]) == commReplyOf.end())
                commReplyOf[neighborList[i]] = 1;
            else
                commReplyOf[neighborList[i]]++;
        }
        delete[] neighborList;
        commentNode.GetLinkedNodes(reply_of_comment_pre_id, neighborList, len, Util::EDGE_OUT);
        for (unsigned i = 0; i < len; i++) {
            if (commReplyOf.find(neighborList[i]) == commReplyOf.end())
                commReplyOf[neighborList[i]] = 1;
            else
                commReplyOf[neighborList[i]]++;
        }
        delete[] neighborList;
	}
}

/**
 * @brief Stored procedure for the IC1 query ("transitive friends with certain name") of LDBC-SNB Interactive workload.
 * Starting from a given person, find his/her friends with a given first name by at most 3 steps via the "knows" relationships.
 * Return friends, including the distances (1..3), and the workplaces and places of study of friends.
 * @param args The arguments of the stored procedure
 * @param new_result The result of the stored procedure
 * @param kvstore Pointer to the KVStore instance
 * @param db Pointer to the Database instance
 * @param txn Pointer to the Transaction instance
 * @remark IC1存储过程
 */
void PProcedure::ic1(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result,
                     const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
  string first_name = args[1].toString();
  const char* first_name_char = first_name.data();
  unsigned first_name_size = first_name.size();
  Node person_node(person_label_id, id_prop_id, &args[0], kvstore, txn);
  if (person_node.node_id_ == -1)
    return;
  std::set<std::tuple<int, std::string, long long, unsigned> > candidates;
  TYPE_ENTITY_LITERAL_ID start_vid = person_node.node_id_;
  std::vector<TYPE_ENTITY_LITERAL_ID> curr_frontier({start_vid});
  tsl::hopscotch_set<TYPE_ENTITY_LITERAL_ID> visited({start_vid});

#ifdef DEBUG_PG
  auto t1 = std::chrono::steady_clock::now();
#endif
  for (int distance = 0; distance <= 3; distance++) {
#ifdef DEBUG_PG
    cout << "[GPSTORE] distance = " << distance << ", size = " << curr_frontier.size() << endl;
    std::chrono::microseconds t_count{0};
    std::chrono::microseconds t1_count{0};
#endif
    std::vector<TYPE_ENTITY_LITERAL_ID > next_frontier;
    for (const auto& vid : curr_frontier) {
#ifdef DEBUG_PG
      auto t1_ = std::chrono::steady_clock::now();
#endif
      Node froniter_person(vid, kvstore, txn);
      bool flag = vid == start_vid || !kvstore->getNodeValueByidAndCompare(vid, firstname_prop_id, first_name_char, first_name_size, txn);
#ifdef DEBUG_PG
      t_count += std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - t1_);
#endif
      if (flag) continue;
#ifdef DEBUG_PG
      auto t2_ = std::chrono::steady_clock::now();
#endif
//      if (vid == start_vid || froniter_person[first_name_id]->toString() != first_name) continue;
      std::string last_name = froniter_person[lastname_prop_id]->toString();
      long long person_id = froniter_person[id_prop_id]->toLLong();
      auto tup = std::make_tuple(distance, last_name, person_id, vid);
      if (candidates.size() >= LIMIT_NUM) {
        auto& candidate = *candidates.rbegin();
        if (tup > candidate) continue;
      }
      candidates.emplace(std::move(tup));
      if (candidates.size() > LIMIT_NUM) {
        candidates.erase(--candidates.end());
      }
#ifdef DEBUG_PG
      t1_count += std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - t2_);
#endif
    }
#ifdef DEBUG_PG
    cout << "[GPSTORE] \t check firstname, used " << t_count.count() << "us." << endl;
    cout << "[GPSTORE] \t write candidate, used " << t1_count.count() << "us." << endl;
#endif
    if (candidates.size() >= LIMIT_NUM || distance == 3) break;
#ifdef DEBUG_PG
    auto t2_ = std::chrono::steady_clock::now();
#endif
    for (auto vid : curr_frontier) {
      Node froniter_person(vid, kvstore, txn);
      TYPE_ENTITY_LITERAL_ID* friends_list = nullptr; unsigned list_len;
      froniter_person.GetLinkedNodes(knows_pre_id, friends_list, list_len, Util::EDGE_OUT, false);
      for (unsigned friend_index = 0; friend_index < list_len; ++friend_index) {
        TYPE_ENTITY_LITERAL_ID friend_vid = friends_list[friend_index];
        if (visited.find(friend_vid) == visited.end()) {
          visited.emplace(friend_vid);
          next_frontier.emplace_back(friend_vid);
        }
      }
      delete[] friends_list;
      froniter_person.GetLinkedNodes(knows_pre_id, friends_list, list_len, Util::EDGE_IN, false);
      for (unsigned friend_index = 0; friend_index < list_len; ++friend_index) {
        TYPE_ENTITY_LITERAL_ID friend_vid = friends_list[friend_index];
        if (visited.find(friend_vid) == visited.end()) {
          visited.emplace(friend_vid);
          next_frontier.emplace_back(friend_vid);
        }
      }
      delete[] friends_list;
    }
    std::sort(next_frontier.begin(), next_frontier.end());
    curr_frontier.swap(next_frontier);
#ifdef DEBUG_PG
    cout << "[GPSTORE] \t expend frontier, used " << std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - t2_).count() << "us." << endl;
#endif
  }
#ifdef DEBUG_PG
  auto t2 = std::chrono::steady_clock::now();
  cout << "[GPSTORE] 1-3 hop time: " << std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count() << "us." << endl;
  auto t3 = std::chrono::steady_clock::now();
#endif

  for (const auto & tup : candidates) {
    unsigned vid = std::get<3>(tup);
    Node person(vid, kvstore, txn);

    new_result->rows_.emplace_back();
    new_result->rows_.back().values_.reserve(13);
    new_result->rows_.back().values_.emplace_back(std::get<2>(tup));
    new_result->rows_.back().values_.emplace_back(std::get<1>(tup));
    new_result->rows_.back().values_.emplace_back(std::get<0>(tup));
    new_result->rows_.back().values_.emplace_back(*person[birthday_prop_id]);
    new_result->rows_.back().values_.emplace_back(*person[creationdate_prop_id]);
    new_result->rows_.back().values_.emplace_back(*person[gender_prop_id]);
    new_result->rows_.back().values_.emplace_back(*person[browserused_prop_id]);
    new_result->rows_.back().values_.emplace_back(*person[locationip_prop_id]);
    new_result->rows_.back().values_.emplace_back(*person[email_prop_id]);
    new_result->rows_.back().values_.emplace_back(*person[speaks_prop_id]);
    new_result->rows_.back().values_.emplace_back(*Node(person[person_place_prop_id]->toLLong(), kvstore, txn)[name_prop_id]);

    unsigned* list = nullptr; unsigned list_len = 0;
    long long* prop_list = nullptr; unsigned prop_len = 0;

    new_result->rows_.back().values_.emplace_back(GPStore::Value::Type::LIST);
    person.GetLinkedNodesWithEdgeProps(study_at_pre_id, list, prop_list, prop_len, list_len, Util::EDGE_OUT);
    for (unsigned i = 0; i < list_len; ++i) {
      Node university(list[i], kvstore, txn);
      Node location_city(university[organisation_place_prop_id]->toLLong(), kvstore, txn);
      vector<GPStore::Value *> university_prop_vec{university[name_prop_id], new GPStore::Value(prop_list[i]), location_city[name_prop_id]};
      new_result->rows_.back().values_.back().data_.List->emplace_back(new GPStore::Value(university_prop_vec, true));
    }
    delete[] list; delete[] prop_list;

    new_result->rows_.back().values_.emplace_back(GPStore::Value::Type::LIST);
    person.GetLinkedNodesWithEdgeProps(work_at_pre_id, list, prop_list, prop_len, list_len, Util::EDGE_OUT);
    for (unsigned i = 0; i < list_len; ++i) {
      Node company(list[i], kvstore, txn);
      Node location_country(company[organisation_place_prop_id]->toLLong(), kvstore, txn);
      vector<GPStore::Value *> university_prop_vec{company[name_prop_id], new GPStore::Value(prop_list[i]), location_country[name_prop_id]};
      new_result->rows_.back().values_.back().data_.List->emplace_back(new GPStore::Value(university_prop_vec, true));
    }
    delete[] list; delete[] prop_list;
  }
#ifdef DEBUG_PG
  cout << "[GPSTORE] optional university and company, used " << std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - t3).count() << "us." << endl;
#endif
}

/**
 * @brief get the message information of a friend
 * @param friend_node_id the id of the friend
 * @param max_date the maximum date of the message
 * @param limit_num the maximum number of messages
 * @param kvstore the pointer to the KVStore instance
 * @param txn the pointer to the Transaction instance
 * @param candidates return value, the message information of the friend
 */
void PProcedure::ProcessFriendMessages(
    unsigned int friend_node_id, GPStore::Value &max_date, unsigned limit_num,
    const KVstore *kvstore, std::shared_ptr<Transaction> txn,
    std::set<std::tuple<GPStore::Value, GPStore::Value, GPStore::Value,
                        GPStore::Value, GPStore::Value, GPStore::Value>>
        &candidates) {
#define MESSAGE_CREATION_DATE 0
#define MESSAGE_ID 1
#define FRIEND_ID 2
#define FRIEND_FIRSTNAME 3
#define FRIEND_LASTNAME 4
#define MESSAGE_CONTENT 5
    Node friend_node = Node(friend_node_id, kvstore, txn);
    unsigned *message_list = nullptr, message_list_len = 0;
    long long *creation_date_list = nullptr;
    unsigned creation_data_width = 0;
    START_TIMING("ProcessFriendMessages" + to_string(friend_node_id));
    friend_node.GetLinkedNodesWithEdgeProps(post_has_creator_pre_id, message_list, creation_date_list, creation_data_width, message_list_len, Util::EDGE_IN);
    assert(message_list == nullptr || creation_data_width == 1);
    for (unsigned i = 0; i < message_list_len; ++i) {
        GPStore::Value message_creation_date = GPStore::Value(creation_date_list[i]);
        if (max_date < message_creation_date) continue;
        Node message_node = Node(message_list[i], kvstore, txn);
        auto message_content = message_node[content_prop_id];
        if (*message_content == GPStore::Value::NO_VALUE) {
            message_content = message_node[imagefile_prop_id];
        }
        GPStore::Value minus_message_creation_date = GPStore::Value(0LL - message_creation_date.toLLong());
        candidates.insert(std::make_tuple(minus_message_creation_date, 
                                          message_node[id_prop_id],
                                          friend_node[id_prop_id],
                                          friend_node[firstname_prop_id],
                                          friend_node[lastname_prop_id],
                                          message_content));
        if (candidates.size() > limit_num) {
            candidates.erase(--candidates.end());
        }
    }
    delete [] message_list;
    delete [] creation_date_list;
    friend_node.GetLinkedNodesWithEdgeProps(comment_has_creator_pre_id, message_list, creation_date_list, creation_data_width, message_list_len, Util::EDGE_IN);
    assert(message_list == nullptr || creation_data_width == 1);
    for (unsigned i = 0; i < message_list_len; ++i) {
        GPStore::Value message_creation_date = GPStore::Value(creation_date_list[i]);
        if (max_date < message_creation_date) continue;
        Node message_node = Node(message_list[i], kvstore, txn);
        auto message_content = message_node[content_prop_id];
        if (*message_content == GPStore::Value::NO_VALUE) {
            message_content = message_node[imagefile_prop_id];
        }
        GPStore::Value minus_message_creation_date = GPStore::Value(0LL - message_creation_date.toLLong());
        candidates.insert(std::make_tuple(minus_message_creation_date, 
                                          message_node[id_prop_id],
                                          friend_node[id_prop_id],
                                          friend_node[firstname_prop_id],
                                          friend_node[lastname_prop_id],
                                          message_content));
        if (candidates.size() > limit_num) {
            candidates.erase(--candidates.end());
        }
    }
    delete [] message_list;
    delete [] creation_date_list;
    END_TIMING("ProcessFriendMessages" + to_string(friend_node_id));
}

/**
 * @brief Stored procedure for the IC2 query ("Recent messages by your friends")
 * of LDBC-SNB Interactive workload. Given a start Person with ID $personId,
 * find the most recent Messages from all of that Person’s friends (friend
 * nodes). Only consider Messages created before the given $maxDate (excluding
 * that day).
 * @param args The arguments of the stored procedure
 * @param new_result The result of the stored procedure
 * @param kvstore Pointer to the KVStore instance
 * @param db Pointer to the Database instance
 * @param txn Pointer to the Transaction instance
 * @remark IC2存储过程
 */
void PProcedure::ic2(const std::vector<GPStore::Value> &args,
                     std::unique_ptr<PTempResult> &new_result,
                     const KVstore *kvstore, Database *db,
                     std::shared_ptr<Transaction> txn) {
  auto max_date = args[1];
  auto limit = 20;
  Node person_node = Node(person_label_id, id_prop_id, &args[0], kvstore, txn);
  if (person_node.node_id_ == -1) {
    std::cout << "Person not found" << std::endl;
    return;
  }
  std::set<std::tuple<GPStore::Value, GPStore::Value, GPStore::Value,
                      GPStore::Value, GPStore::Value, GPStore::Value>>
      candidates;
  unsigned *know_list = nullptr, know_list_len = 0;
  START_TIMING("get friend list");
  person_node.GetLinkedNodes(knows_pre_id, know_list, know_list_len,
                             Util::EDGE_IN);
  END_TIMING("get friend list");
  for (unsigned i = 0; i < know_list_len; ++i) {
    PProcedure::ProcessFriendMessages(know_list[i], max_date, limit, kvstore,
                                      txn, candidates);
  }
  delete[] know_list;
  person_node.GetLinkedNodes(knows_pre_id, know_list, know_list_len,
                             Util::EDGE_OUT);
  for (unsigned i = 0; i < know_list_len; ++i) {
    PProcedure::ProcessFriendMessages(know_list[i], max_date, limit, kvstore,
                                      txn, candidates);
  }
  delete[] know_list;
  for (auto it : candidates) {
    new_result->rows_.emplace_back();
    new_result->rows_.back().values_.reserve(6);
    new_result->rows_.back().values_.push_back(std::get<FRIEND_ID>(it));
    new_result->rows_.back().values_.push_back(std::get<FRIEND_FIRSTNAME>(it));
    new_result->rows_.back().values_.push_back(std::get<FRIEND_LASTNAME>(it));
    new_result->rows_.back().values_.push_back(std::get<MESSAGE_ID>(it));
    new_result->rows_.back().values_.push_back(std::get<MESSAGE_CONTENT>(it));
    GPStore::Value minus_message_creation_date =
        GPStore::Value(0LL - std::get<MESSAGE_CREATION_DATE>(it).toLLong());
    new_result->rows_.back().values_.push_back(minus_message_creation_date);
  }
  OUTPUT_TIMING();
#undef MESSAGE_CONTENT
#undef FRIEND_LASTNAME
#undef FRIEND_FIRSTNAME
#undef FRIEND_ID
#undef MESSAGE_ID
#undef MESSAGE_CREATION_DATE
}

/**
 * @brief get the person list located in specific country
 * @param country_node the node of the country
 * @param kvstore the pointer to the KVStore instance
 * @param txn the pointer to the Transaction instance
 * @param person_id_set return value, the set of person id
*/
void PProcedure::GetPersonLocatedInCountry(Node& country_node, const KVstore *kvstore, std::shared_ptr<Transaction> txn, std::set<unsigned>& person_id_set) {
    unsigned *city_list = nullptr, city_list_len;
    country_node.GetLinkedNodes(is_part_of_pre_id, city_list, city_list_len, Util::EDGE_IN);
    for (unsigned i = 0; i < city_list_len; ++i) {
        Node city_node = Node(city_list[i], kvstore, txn);
        unsigned *person_list = nullptr, person_list_len = 0;
        city_node.GetLinkedNodes(person_is_located_in_pre_id, person_list, person_list_len, Util::EDGE_IN);
        for (unsigned j = 0; j < person_list_len; ++j) {
            person_id_set.insert(person_list[j]);
        }
        delete [] person_list;
    }
    delete [] city_list;
}

/**
 * @brief get the messages in specific country within the given duration
 * @param country_node the node of the country
 * @param start_date the start date of the duration
 * @param duration the duration of the messages
 * @param kvstore the pointer to the KVStore instance
 * @param txn the pointer to the Transaction instance
 * @param msg_list return value, the list of message id and message type(0: post, 1: comment)
*/
void PProcedure::GetMessagesInIC3(
    Node &country_node, GPStore::Value &start_date, GPStore::Value &duration,
    const KVstore *kvstore, std::shared_ptr<Transaction> txn,
    std::vector<std::pair<unsigned, unsigned>> &msg_list) {
  unsigned *id_list = nullptr, len = 0, creation_date_width = 0;
  long long *creation_date_list = nullptr;
  country_node.GetLinkedNodesWithEdgeProps(
      post_is_located_in_pre_id, id_list, creation_date_list,
      creation_date_width, len, Util::EDGE_IN);
  assert(creation_date_list == nullptr || creation_date_width == 1);
  msg_list.reserve(len);
  for (unsigned i = 0; i < len; ++i) {
    unsigned *label_id_list = nullptr, label_id_list_len = 0;
    kvstore->getlabelIDlistBynodeID(id_list[i], label_id_list,
                                    label_id_list_len, txn);
    GPStore::Value creation_date = creation_date_list[i];
    if ((creation_date < start_date) ||
        !(creation_date <
          GPStore::Value(start_date.toLLong() + duration.toLLong())))
      continue;
    msg_list.push_back(std::make_pair(id_list[i], 0));
  }
  delete[] id_list;
  delete[] creation_date_list;
  country_node.GetLinkedNodesWithEdgeProps(
      comment_is_located_in_pre_id, id_list, creation_date_list,
      creation_date_width, len, Util::EDGE_IN);
  for (unsigned i = 0; i < len; ++i) {
    unsigned *label_id_list = nullptr, label_id_list_len = 0;
    kvstore->getlabelIDlistBynodeID(id_list[i], label_id_list,
                                    label_id_list_len, txn);
    GPStore::Value creation_date = creation_date_list[i];
    if ((creation_date < start_date) ||
        !(creation_date <
          GPStore::Value(start_date.toLLong() + duration.toLLong())))
      continue;
    msg_list.push_back(std::make_pair(id_list[i], 1));
  }
  delete[] id_list;
  delete[] creation_date_list;
}

/**
 * @brief Stored procedure for the IC3 query ("Friends and friends of friends
 * that have been to given countries") of LDBC-SNB Interactive workload. Given a
 * start Person with ID $personId, find Persons that are their friends and
 * friends of friends (excluding the start Person) that have made Posts /
 * Comments in both of the given Countries (named $countryXName and
 * $countryYName), within [$startDate, $startDate + $durationDays) (closedopen
 * interval). Only Persons that are foreign to these Countries are considered,
 * that is Persons whose location Country is neither named $countryXName nor
 * $countryYName.
 * @param args The arguments of the stored procedure
 * @param new_result The result of the stored procedure
 * @param kvstore Pointer to the KVStore instance
 * @param db Pointer to the Database instance
 * @param txn Pointer to the Transaction instance
 * @remark IC3存储过程
 */
void PProcedure::ic3(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
    auto person_id = args[0];
    auto x_country = args[1];
    auto y_country = args[2];
    auto start_date = args[3];
    auto duration = args[4];
    Node x_country_node = Node(country_label_id, name_prop_id, &x_country, kvstore, txn);
    if (x_country_node.node_id_ == -1)
        return;
    Node y_country_node = Node(country_label_id, name_prop_id, &y_country, kvstore, txn);
    if (y_country_node.node_id_ == -1)
        return;
    std::set<unsigned> xy_person_list; // the person located in country x or country y
    std::vector<std::pair<unsigned, unsigned>> x_msg_list, y_msg_list; /* message_id, message_type(0: post, 1: comment) */
    START_TIMING("GetPersonLocatedInCountry");
    GetPersonLocatedInCountry(x_country_node, kvstore, txn, xy_person_list);
    GetPersonLocatedInCountry(y_country_node, kvstore, txn, xy_person_list);
    END_TIMING("GetPersonLocatedInCountry");
    START_TIMING("GetMessagesInIC3");
    GetMessagesInIC3(x_country_node, start_date, duration, kvstore, txn, x_msg_list);
    GetMessagesInIC3(y_country_node, start_date, duration, kvstore, txn, y_msg_list);
    END_TIMING("GetMessagesInIC3");

    // output x_msg_list and y_msg_list
    // std::cout << "x_msg_list:\n";
    // for (auto& item: x_msg_list) {
    //     auto msg_node_id = item.first;
    //     auto msg_type = item.second; // 0: post, 1: comment
    //     auto msg_node = Node(msg_node_id, kvstore, txn);
    //     std::cout << "msg_id: " << msg_node_id << ", msg_type: " << msg_type << "\n";
    // }
    // std::cout << "y_msg_list:\n";
    // for (auto& item: y_msg_list) {
    //     auto msg_node_id = item.first;
    //     auto msg_type = item.second; // 0: post, 1: comment
    //     auto msg_node = Node(msg_node_id, kvstore, txn);
    //     std::cout << "msg_id: " << msg_node_id << ", msg_type: " << msg_type << "\n";
    // }

    START_TIMING("GET_TWO_HOP_FRIENDS");
    std::map<unsigned, std::pair<int32_t, int32_t> > person_infos;
    Node person_node = Node(person_label_id, id_prop_id, &person_id, kvstore, txn);
    if (person_node.node_id_ == -1)
        return;
    std::vector<unsigned> cur_frontier({person_node.node_id_});
    for(int i = 0; i < 2; ++i) {
        std::vector<unsigned> nxt_frontier;
        for (auto & node_id: cur_frontier) {
            unsigned *friend_list = nullptr, friend_list_len;
            auto node = Node(node_id, kvstore, txn);
            node.GetLinkedNodes(knows_pre_id, friend_list, friend_list_len, Util::EDGE_OUT);
            for (unsigned j = 0; j < friend_list_len; ++j) {
                if (xy_person_list.find(friend_list[j]) != xy_person_list.end()) continue;
                Node friend_node = Node(friend_list[j], kvstore, txn);
                nxt_frontier.push_back(friend_list[j]);
                person_infos.insert(std::make_pair(friend_list[j], std::make_pair(0, 0)));
            }
            delete [] friend_list;
            node.GetLinkedNodes(knows_pre_id, friend_list, friend_list_len, Util::EDGE_IN);
            for (unsigned j = 0; j < friend_list_len; ++j) {
                if (xy_person_list.find(friend_list[j]) != xy_person_list.end()) continue;
                Node friend_node = Node(friend_list[j], kvstore, txn);
                nxt_frontier.push_back(friend_list[j]);
                person_infos.insert(std::make_pair(friend_list[j], std::make_pair(0, 0)));
            }
            delete [] friend_list;
        }
        cur_frontier.swap(nxt_frontier);
    }
    END_TIMING("GET_TWO_HOP_FRIENDS");
    START_TIMING("FRIEND_MSG_COUNT");
    for (auto& item: x_msg_list) {
        auto msg_node_id = item.first;
        auto msg_type = item.second; // 0: post, 1: comment
        unsigned create_person_id = 0;
        auto msg_node = Node(msg_node_id, kvstore, txn);
        if (msg_type == 0) {
            create_person_id = msg_node[post_creator_prop_id]->toLLong(); 
        }
        else {
            create_person_id = msg_node[comment_creator_prop_id]->toLLong();
        }
        if (person_infos.find(create_person_id) != person_infos.end()) {
            person_infos[create_person_id].first++;
        }
    }
    for (auto& item: y_msg_list) {
        auto msg_node_id = item.first;
        auto msg_type = item.second; // 0: post, 1: comment
        unsigned create_person_id = 0;
        auto msg_node = Node(msg_node_id, kvstore, txn);
        if (msg_type == 0) {
            create_person_id = msg_node[post_creator_prop_id]->toLLong();
        }
        else {
            create_person_id = msg_node[comment_creator_prop_id]->toLLong();
        }
        if (person_infos.find(create_person_id) != person_infos.end()) {
            person_infos[create_person_id].second++;
        }
    }
    END_TIMING("FRIEND_MSG_COUNT");
    START_TIMING("WRITE_RESULT");
    std::set<std::tuple<GPStore::Value, GPStore::Value, GPStore::Value, GPStore::Value, GPStore::Value, GPStore::Value>> candidates;
    for (auto & it: person_infos) {
        auto friend_node = Node(it.first, kvstore, txn);
        if (it.second.first == 0 || it.second.second == 0) continue;
        candidates.insert(std::make_tuple(-it.second.first - it.second.second, 
        friend_node[id_prop_id], 
        friend_node[firstname_prop_id], 
        friend_node[lastname_prop_id], 
        it.second.first, 
        it.second.second));
        if (candidates.size() > 20) {
            candidates.erase(--candidates.end());
        }
    }
    for (auto tup: candidates) {
        new_result->rows_.emplace_back();
        new_result->rows_.reserve(6);
        new_result->rows_.back().values_.push_back(get<1>(tup));
        new_result->rows_.back().values_.push_back(get<2>(tup));
        new_result->rows_.back().values_.push_back(get<3>(tup));
        new_result->rows_.back().values_.push_back(get<4>(tup));
        new_result->rows_.back().values_.push_back(get<5>(tup));
        new_result->rows_.back().values_.push_back(GPStore::Value(-get<0>(tup).toLLong()));
    }
    END_TIMING("WRITE_RESULT");
    OUTPUT_TIMING();
}

/**
 * @brief get the tag list of the post node which is created by the friend node
 * and the creation date is within the given time interval
 * @param friend_node_id the id of the friend node
 * @param start_time the start time of the time interval
 * @param end_time the end time of the time interval
 * @param kvstore the pointer to the KVStore instance
 * @param txn the pointer to the Transaction instance
 * @param tag_stats the statistics of the tags
 */
void PProcedure::ProcessFriendPost(unsigned friend_node_id, GPStore::Value& start_time, GPStore::Value& end_time, const KVstore* kvstore, std::shared_ptr<Transaction> txn, std::map<uint32_t, std::pair<long long, uint32_t>>& tag_stats) {
    Node friend_node = Node(friend_node_id, kvstore, txn);
    unsigned *post_list = nullptr, post_list_len = 0, creation_date_width = 0;
    long long *creation_date_list = nullptr;
    START_TIMING("ProcessFriendPost" + to_string(friend_node_id));
    friend_node.GetLinkedNodesWithEdgeProps(post_has_creator_pre_id, post_list, creation_date_list, creation_date_width, post_list_len, Util::EDGE_IN);
    assert(post_list == nullptr || creation_date_width == 1);
#ifdef DEBUG_PG
    std::cout << "post_list_len = " << post_list_len << "\n";
#endif 
    for (unsigned i = 0; i < post_list_len; ++i) {
        auto creation_date = creation_date_list[i];
        if (!(creation_date < end_time.toLLong())) continue;
        Node post_node = Node(post_list[i], kvstore, txn);
        unsigned *tag_list = nullptr, tag_list_len = 0;
        post_node.GetLinkedNodes(post_has_tag_pre_id, tag_list, tag_list_len, Util::EDGE_OUT);
        for (unsigned j = 0; j < tag_list_len; ++j) {
            auto iter = tag_stats.find(tag_list[j]);
            if (iter == tag_stats.end()) {
                tag_stats.insert(std::make_pair(tag_list[j], 
                                 std::make_pair(creation_date, 1)));
            }
            else {
                iter->second.first = std::min(iter->second.first, creation_date);
                iter->second.second++;
            }
        }
        delete [] tag_list;
    }
    delete [] creation_date_list;
    delete [] post_list;
    END_TIMING("ProcessFriendPost" + to_string(friend_node_id));
}

/**
 * @brief Stored procedure for the IC4 query ("New topics") of LDBC-SNB
 * Interactive workload. Given a start Person with ID $personId, find Tags that
 * are attached to Posts that were created by that Person’s friends. Only
 * include Tags that were attached to friends’ Posts created within a given time
 * interval [$startDate, $startDate + $durationDays) (closed-open) and that were
 * never attached to friends’ Posts created before this interval.
 * @param args The arguments of the stored procedure
 * @param new_result The result of the stored procedure
 * @param kvstore Pointer to the KVStore instance
 * @param db Pointer to the Database instance
 * @param txn Pointer to the Transaction instance
 * @remark IC4存储过程
 */
void PProcedure::ic4(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
    auto person_id = args[0];
    auto start_time = args[1];
    auto duration_time = args[2];
    auto end_time = GPStore::Value(start_time.toLLong() + duration_time.toLLong());
    Node person_node = Node(person_label_id, id_prop_id, &person_id, kvstore, txn);
    if (person_node.node_id_ == -1)
        return;
    unsigned *friend_list = nullptr, list_len = 0;
    std::map<uint32_t, std::pair<long long, uint32_t>> tag_stats; // pair(earliest time, count)
    person_node.GetLinkedNodes(knows_pre_id, friend_list, list_len, Util::EDGE_IN);
    for (unsigned i = 0; i < list_len; ++i) {
        ProcessFriendPost(friend_list[i], start_time, end_time, kvstore, txn, tag_stats);
    }
    delete [] friend_list;
    person_node.GetLinkedNodes(knows_pre_id, friend_list, list_len, Util::EDGE_OUT);
    for (unsigned i = 0; i < list_len; ++i) {
        ProcessFriendPost(friend_list[i], start_time, end_time, kvstore, txn, tag_stats);
    }
    delete [] friend_list;
    // output tag_stats
    // for (auto& it: tag_stats) {
    //     std::cout << "tag_id: " << it.first << ", earliest_time: " << it.second.first << ", count: " << it.second.second << "\n";
    // }
    struct cmp {
        bool operator()(const std::pair<std::string, uint32_t> &a, const std::pair<std::string, uint32_t> &b) const {
            return a.second > b.second || (a.second == b.second && a.first < b.first);
        }
    };
    std::set<std::pair<std::string, uint32_t>, cmp> candidates;
    for (const auto& it: tag_stats) {
        if (it.second.first < start_time.toLLong()) continue;
        Node tag_node = Node(it.first, kvstore, txn);
        auto pair = std::make_pair(tag_node[name_prop_id]->toString(), it.second.second);
        candidates.insert(pair);
        if (candidates.size() > 10) {
            candidates.erase(--candidates.end());
        }
    }
    for (auto tup: candidates) {
        new_result->rows_.emplace_back();
        new_result->rows_.back().values_.push_back(tup.first);
        new_result->rows_.back().values_.push_back((int)tup.second);
    }
    OUTPUT_TIMING();
}

void PProcedure::ProcessPersonPosts(Node& person, std::pair<std::set<GPStore::int_64>,\
        std::vector<std::tuple<GPStore::int_64, GPStore::int_64>> > & post_counts,
        const GPStore::int_64 min_date){
    const KVstore *kvstore = person.kv_store_;
    auto &txn = person.txn_;

    TYPE_ENTITY_LITERAL_ID *forum_list = nullptr;
    GPStore::int_64 *x_list = nullptr;
    unsigned forum_num = 0, xwidth = 0;

    // Schema File: hasMember have 3 prop: joinDate; POST_COUNT; HASMEMBER_FORUMID 
    if(kvstore->getsubIDxvaluesByobjIDpreID(person.node_id_, has_member_pre_id, forum_list, x_list, xwidth, forum_num, false, nullptr) && forum_num != 0){
        for(unsigned i = 0; i < forum_num; ++i){
            GPStore::int_64 join_date = x_list[i*xwidth];
            if(join_date <= min_date) continue;

            auto forum_vid = forum_list[i];
            GPStore::int_64 num_posts = x_list[i*xwidth+1];
            GPStore::int_64 forum_id = x_list[i*xwidth+2];
            if(num_posts == 0){
                auto &tail = post_counts.first;
                if(tail.size() < 20 /*limit*/ || *tail.rbegin() > forum_id){
                    tail.emplace(forum_id);
                    if(tail.size() > 20) tail.erase(--tail.end());
                }
            }
            else {
                post_counts.second.emplace_back(forum_id, num_posts);
            }
        }
        delete[] forum_list;
        delete[] x_list;
    }
}


/**
 * @brief Stored procedure for the IC5 query ("New groups") of LDBC-SNB Interactive workload.
 * Given a start Person with ID $personId and their friends within a certain join date range, it then 
 * counts the posts created by each friend in these forums and returns the forum title along with the
 * post count, ordered by post count in descending order and forum id in ascending order.
 * @param args The arguments of the stored procedure
 * @param new_result The result of the stored procedure
 * @param kvstore Pointer to the KVStore instance
 * @param db Pointer to the Database instance
 * @param txn Pointer to the Transaction instance
 * @remark IC5存储过程
 */
void PProcedure::ic5(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
#ifdef DEBUG_PG
    std::chrono::microseconds k_hop_cost(0), person_post_count_cost(0), top_k_cost(0), candidate_to_result_cost(0), total_cost(0);
    auto start = std::chrono::steady_clock::now();
#endif
    constexpr size_t limit_results = 20;

    TYPE_ENTITY_LITERAL_ID* list_ptr = nullptr;
    unsigned list_len;

#ifdef DEBUG_PG
    auto k_hop_begin = std::chrono::steady_clock::now();
#endif
    Node person_node(person_label_id, id_prop_id, &args[0], kvstore, txn);
    if (person_node.node_id_ == -1)
        return;
    GPStore::int_64 min_date = args[1].data_.Long;

    tsl::hopscotch_set<TYPE_ENTITY_LITERAL_ID> visited({person_node.node_id_});
    std::vector<TYPE_ENTITY_LITERAL_ID> curr_frontier({person_node.node_id_});
    std::vector<TYPE_ENTITY_LITERAL_ID> friends;
    for (int hop = 0; hop <= 2; hop++) {
        std::vector<TYPE_ENTITY_LITERAL_ID > next_frontier;
        for (auto vid : curr_frontier) {
            if(hop < 2){
                Node person(vid, kvstore, txn);
                person.GetLinkedNodes(knows_pre_id, list_ptr, list_len, Util::EDGE_OUT);
                for (unsigned index = 0; index < list_len; ++index) {
                    TYPE_ENTITY_LITERAL_ID friend_vid = list_ptr[index];
                    if (visited.find(friend_vid) == visited.end()) {
                        visited.emplace(friend_vid);
                        next_frontier.emplace_back(friend_vid);
                    }
                }
                delete[] list_ptr;
                person.GetLinkedNodes(knows_pre_id, list_ptr, list_len, Util::EDGE_IN);
                for (unsigned index = 0; index < list_len; ++index) {
                    TYPE_ENTITY_LITERAL_ID friend_vid = list_ptr[index];
                    if (visited.find(friend_vid) == visited.end()) {
                        visited.emplace(friend_vid);
                        next_frontier.emplace_back(friend_vid);
                    }
                }
                delete[] list_ptr;
            }
            if(hop == 0) continue;
            friends.emplace_back(vid);
        }
        std::sort(next_frontier.begin(), next_frontier.end());
        curr_frontier.swap(next_frontier);
    }

#ifdef DEBUG_PG
    auto k_hop_end = std::chrono::steady_clock::now();
    k_hop_cost =  std::chrono::duration_cast<std::chrono::microseconds>(k_hop_end - k_hop_begin);
    auto person_post_cnt_begin =std::chrono::steady_clock::now();
#endif
    // result_type.first: Top-20 ID ASC forums that #(posts by friends) = 0.
    // result_type.second: vector of <forum, num of posts from friends.>  note: forum use external id.
    using result_type = std::pair<std::set<GPStore::int_64>, std::vector<std::tuple<GPStore::int_64, GPStore::int_64>>>;
    auto post_counts = ForEachVertex<result_type>(
            kvstore, txn, friends,
            [min_date](Node& person, result_type& post_counts){
                PProcedure::ProcessPersonPosts(person, post_counts, min_date);
            },
            [](const result_type& local, result_type& res){
                res.first.insert(local.first.begin(), local.first.end());
                res.second.insert(res.second.end(), local.second.begin(), local.second.end());
            } );

#ifdef DEBUG_PG
    auto person_post_cnt_end =std::chrono::steady_clock::now();
    person_post_count_cost = std::chrono::duration_cast<std::chrono::microseconds>(person_post_cnt_end - person_post_cnt_begin);
    auto top_k_begin = std::chrono::steady_clock::now();
#endif
    // Select Candidates : Forums with non-zero post count.
    std::set<std::tuple<GPStore::int_64, GPStore::int_64 >> candidates;
    if(!post_counts.second.empty()){
        std::sort(post_counts.second.begin(), post_counts.second.end());
        GPStore::int_64 last_forum = std::get<0>(post_counts.second[0]);
        GPStore::int_64 post_count = std::get<1>(post_counts.second[0]);
        for(size_t i = 1; i < post_counts.second.size(); ++i){
            auto curr_forum = std::get<0>(post_counts.second[i]);
            if (curr_forum == last_forum) {
                post_count += std::get<1>(post_counts.second[i]);
                continue;
            }
            GPStore::int_64 forum_id = last_forum;
            auto tup = std::make_tuple(0 - post_count, forum_id);
            if (candidates.size() < limit_results || *candidates.rbegin() > tup) {
                candidates.emplace(tup);
                if (candidates.size() > limit_results){
                    candidates.erase(--candidates.end());
                }
            }
            last_forum = curr_forum;
            post_count = std::get<1>(post_counts.second[i]);
        }
        GPStore::int_64 forum_id = last_forum;
        auto tup = std::make_tuple(0 - post_count, forum_id);
        if (candidates.size() < limit_results || *candidates.rbegin() > tup) {
            candidates.emplace(tup);
            if (candidates.size() > limit_results){
                candidates.erase(--candidates.end());
            }
        }
    }

#ifdef DEBUG_PG
    auto top_k_end = std::chrono::steady_clock::now();
    top_k_cost = std::chrono::duration_cast<std::chrono::microseconds>(top_k_end - top_k_begin);

    auto candidate_to_result_begin = std::chrono::steady_clock::now();
#endif
    // output results.
    for(auto& tup : candidates){
        if(new_result->getSize() == limit_results) break;
        new_result->rows_.emplace_back();
        new_result->rows_.back().values_.reserve(2);
        GPStore::Value forum_id_val(GPStore::int_64(std::get<1>(tup)));
        Node forum(forum_label_id, id_prop_id, &forum_id_val, kvstore, txn);
        new_result->rows_.back().values_.emplace_back(forum[title_prop_id]);
        new_result->rows_.back().values_.emplace_back(GPStore::int_64(0 - std::get<0>(tup)));
    }

    if(new_result->getSize() < limit_results){
        tsl::hopscotch_set<GPStore::int_64> candidates_forum_ids;
        for (auto tuple_item:candidates) {
            candidates_forum_ids.emplace(std::get<1>(tuple_item));
        }
        for (auto forum_id : post_counts.first) {
            if (candidates_forum_ids.find(forum_id) != candidates_forum_ids.end()) continue;
            new_result->rows_.emplace_back();
            GPStore::Value forum_id_val(forum_id);
            Node forum(forum_label_id, id_prop_id, &forum_id_val, kvstore, txn);
            new_result->rows_.back().values_.emplace_back(forum[title_prop_id]);
            new_result->rows_.back().values_.emplace_back(GPStore::int_64(0));
            if(new_result->getSize() == limit_results) break;
        }
    }
#ifdef DEBUG_PG
    auto candidate_to_result_end = std::chrono::steady_clock::now();
    candidate_to_result_cost = std::chrono::duration_cast<std::chrono::microseconds>(candidate_to_result_end - candidate_to_result_begin);

    auto end_ = std::chrono::steady_clock::now();
    total_cost = std::chrono::duration_cast<std::chrono::microseconds>(end_ - start);
    std::cout << "[GPSTORE]Select 1,2 Hop Friends Cost: " << k_hop_cost.count() << " us." << std::endl;
    std::cout << "[GPSTORE]Process Friend-Post Count Cost: " << person_post_count_cost.count() << " us." << std::endl;
    std::cout << "[GPSTORE]Select Top-K Candidate Cost: " << top_k_cost.count() << " us." << std::endl;
    std::cout << "[GPSTORE]Convert Candidate To Output [id2vid, getprop] Cost:" << candidate_to_result_cost.count() << " us." << std::endl;
    std::cout << "[GPSTORE]Total Cost: " << total_cost.count() << " us." << std::endl;
#endif
}



/**
 * @brief Stored procedure for the IC6 query ("Tag co-occurrence") of LDBC-SNB Interactive workload.
 * This Cypher query retrieves information about posts related to a specific tag made by friends of 
 * a person within 1 to 2 degrees of separation. It then counts the posts by each friend that are 
 * related to the known tag but not exclusively. The query returns the tag name and the post count, 
 * ordered by post count in descending order and tag name in ascending order.
 * @param args The arguments of the stored procedure
 * @param new_result The result of the stored procedure
 * @param kvstore Pointer to the KVStore instance
 * @param db Pointer to the Database instance
 * @param txn Pointer to the Transaction instance
 * @remark IC6存储过程
 */
void PProcedure::ic6(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
#ifdef DEBUG_PG
    std::chrono::microseconds k_hop_cost(0), get_post_with_tag_cost(0), tag_post_count(0), top_k_cost(0), total_cost(0);
    auto start = std::chrono::steady_clock::now();
#endif
    constexpr size_t limit_results = 10;

    TYPE_ENTITY_LITERAL_ID *node_list = nullptr, *creator_list = nullptr, *post_list = nullptr;
    unsigned node_len = 0, creator_len = 0, post_len = 0;

#ifdef DEBUG_PG
    auto k_hop_begin = std::chrono::steady_clock::now();
#endif
    Node person_start(person_label_id, id_prop_id, &args[0], kvstore, txn);
    if (person_start.node_id_ == -1)
        return;
    auto person_start_vid = person_start.node_id_;
    tsl::hopscotch_set<TYPE_ENTITY_LITERAL_ID> visited({person_start_vid});
    std::vector<TYPE_ENTITY_LITERAL_ID> curr_frontier({person_start_vid});
    for(int hop = 0; hop < 2; ++hop){
        std::vector<TYPE_ENTITY_LITERAL_ID> next_frontier;
        for(auto person_vid : curr_frontier){
            Node person(person_vid, kvstore, txn);
            person.GetLinkedNodes(knows_pre_id, node_list, node_len, Util::EDGE_IN);
            for(int i = 0; i < node_len; ++i){
                TYPE_ENTITY_LITERAL_ID friend_vid = node_list[i];
                if(visited.find(friend_vid) == visited.end()){
                    visited.emplace(friend_vid);
                    if(hop < 1) next_frontier.emplace_back(friend_vid);
                }
            }
            delete []node_list;
            person.GetLinkedNodes(knows_pre_id, node_list, node_len, Util::EDGE_OUT);
            for(int i = 0; i < node_len; ++i){
                TYPE_ENTITY_LITERAL_ID friend_vid = node_list[i];
                if(visited.find(friend_vid) == visited.end()){
                    visited.emplace(friend_vid);
                    if(hop < 1) next_frontier.emplace_back(friend_vid);
                }
            }
            delete []node_list;
        }
        std::sort(next_frontier.begin(), next_frontier.end());
        curr_frontier.swap(next_frontier);
    }
    visited.erase(person_start_vid);

#ifdef DEBUG_PG
    auto k_hop_end = std::chrono::steady_clock::now();
    k_hop_cost = std::chrono::duration_cast<std::chrono::microseconds>(k_hop_end - k_hop_begin);
    auto post_with_tag_begin = std::chrono::steady_clock::now();
#endif
    Node tag_start(tag_label_id, name_prop_id, &args[1], kvstore, txn);
    if (tag_start.node_id_ == -1)
        return;
    TYPE_ENTITY_LITERAL_ID start_tag_vid = tag_start.node_id_;
    tsl::hopscotch_map<TYPE_ENTITY_LITERAL_ID, GPStore::int_64> post_counts;
    // Get all posts with the tag
    tag_start.GetLinkedNodes(post_has_tag_pre_id, post_list, post_len, Util::EDGE_IN);

#ifdef DEBUG_PG
    auto post_with_tag_end = std::chrono::steady_clock::now();
    get_post_with_tag_cost = std::chrono::duration_cast<std::chrono::microseconds>(post_with_tag_end - post_with_tag_begin);

    auto post_tag_count_begin = std::chrono::steady_clock::now();
#endif
    for(int i = 0; i < post_len; ++i){
        TYPE_ENTITY_LITERAL_ID post_vid = post_list[i];
        Node post(post_vid, kvstore, txn);
        post.GetLinkedNodes(post_has_creator_pre_id, creator_list, creator_len, Util::EDGE_OUT);
        auto creator_vid = creator_list[0];
        delete []creator_list;
        if(visited.find(creator_vid) == visited.end()) continue;

        TYPE_ENTITY_LITERAL_ID *tag_list = nullptr;
        unsigned tag_len = 0;
        post.GetLinkedNodes(post_has_tag_pre_id, tag_list, tag_len, Util::EDGE_OUT);
        for(int j = 0; j < tag_len; ++j){
            auto tag_vid = tag_list[j];
            if(tag_vid == start_tag_vid) continue;
            auto it = post_counts.find(tag_vid);
            if(it != post_counts.end()){
                it.value() += 1;
            } else {
                post_counts.emplace(tag_vid, 1);
            }
        }
        delete []tag_list;
    }
    delete []post_list;

#ifdef DEBUG_PG
    auto post_tag_count_end = std::chrono::steady_clock::now();
    tag_post_count =  std::chrono::duration_cast<std::chrono::microseconds>(post_tag_count_end - post_tag_count_begin);
    auto top_k_begin = std::chrono::steady_clock::now();
#endif
    std::set<std::pair<GPStore::int_64, std::string>> candidates;
    for(auto it = post_counts.begin(); it != post_counts.end(); ++it){
        auto tag_vid = it->first;
        std::string tag_name;
        GPStore::int_64 post_count = it->second;
        if(candidates.size() >= limit_results){
            auto &candidate = *candidates.rbegin();
            if(0 - post_count > candidate.first) continue;
            if(0 - post_count == candidate.first){
                Node tag(tag_vid, kvstore, txn);
                tag_name = *tag[name_prop_id]->data_.String;
                if(tag_name > candidate.second) continue;
            }
        }
        if(tag_name.empty()){
            Node tag(tag_vid, kvstore, txn);
            tag_name = *tag[name_prop_id]->data_.String;
        }
        candidates.emplace(0 - post_count, tag_name);
        if(candidates.size() > limit_results){
            candidates.erase(--candidates.end());
        }
    }

#ifdef DEBUG_PG
    auto top_k_end = std::chrono::steady_clock::now();
    top_k_cost = std::chrono::duration_cast<std::chrono::microseconds>(top_k_end - top_k_begin);
#endif
    for(auto& tup : candidates){
        new_result->rows_.emplace_back();
        new_result->rows_.back().values_.emplace_back(tup.second);
        new_result->rows_.back().values_.emplace_back(GPStore::int_64(0 - tup.first));
    }

#ifdef DEBUG_PG
    auto end_ =std::chrono::steady_clock::now();
    total_cost = std::chrono::duration_cast<std::chrono::microseconds>(end_ - start);

    std::cout << "[GPSTORE]Select 1,2 Hop Friends Cost: " << k_hop_cost.count() << " us." << std::endl;
    std::cout << "[GPSTORE]Get Posts With $TAG Cost: " << get_post_with_tag_cost.count() << " us." << std::endl;
    std::cout << "[GPSTORE]Count Tags of Posts published by Friends Cost: " << tag_post_count.count() << " us." << std::endl;
    std::cout << "[GPSTORE]Select Top-K Candidate And Load Properties Cost:" << top_k_cost.count() << " us." << std::endl;
    std::cout << "[GPSTORE]Total Cost: " << total_cost.count() << " us." << std::endl;
#endif
}

void ProcessMessageLikes(Node &message, long long message_id, long long message_creation_date,
                         const std::string &message_content, Node &other_person,
                         tsl::hopscotch_map<TYPE_ENTITY_LITERAL_ID, long long> &person_id_map,
                         tsl::hopscotch_map<long long, std::pair<long long, long long> > &candidates_index,
                         std::map<std::pair<long long, long long>, std::tuple<long long, long long, std::string, int> > &candidates,
                         TYPE_PREDICATE_ID likes_pre_id, TYPE_PROPERTY_ID creation_date_prop_id, TYPE_PROPERTY_ID id_prop_id,
                         const KVstore* kvstore, std::shared_ptr<Transaction> txn
#ifdef DEBUG_PG
                         , unsigned &friends_num_sum, std::chrono::microseconds &t_count2, std::chrono::microseconds &t_count3
#endif
                         ) {
  TYPE_ENTITY_LITERAL_ID *person_friends = nullptr;
  unsigned friends_num = 0;
  long long *creation_date_list = nullptr;
  unsigned creation_data_width = 0;
#ifdef DEBUG_PG
  auto start_time = std::chrono::steady_clock::now();
#endif
//   message.GetLinkedNodes(likes_pre_id, person_friends, friends_num, Util::EDGE_IN);
  message.GetLinkedNodesWithEdgeProps(likes_pre_id, person_friends, creation_date_list, creation_data_width, friends_num, Util::EDGE_IN);
//  assert(creation_date_list == nullptr || creation_data_width == 1);
#ifdef DEBUG_PG
  t_count2 += std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start_time);
  friends_num_sum += friends_num;
  start_time = std::chrono::steady_clock::now();
#endif
  for (unsigned j = 0; j < friends_num; ++j) {
    auto person_vid = person_friends[j];
    // Edge message_likes(person_vid, message.node_id_, likes_pre_id, 0, kvstore, txn);
    // long long like_creation_date = message_likes[creation_date_prop_id]->toLLong();
    long long like_creation_date = creation_date_list[j];
    auto it = candidates_index.find(person_vid);
    if (it != candidates_index.end()) {
      auto &key = it.value();
      if (like_creation_date < 0 - key.first) {
        continue;
      }
      if (like_creation_date == 0 - key.first) {
        auto cit = candidates.find(key);
        long long old_message_id = std::get<1>(cit->second);
        if (message_id > old_message_id) {
          continue;
        }
      }
      candidates.erase(key);
      key.first = 0 - like_creation_date;
      candidates.emplace(key, std::make_tuple(person_vid, message_id, message_content,
                         (like_creation_date - message_creation_date) / 1000 / 60));
    } else {
      long long person_id;
      auto pit = person_id_map.find(person_vid);
      if (pit != person_id_map.end()) {
        person_id = pit->second;
      } else {
        other_person.Goto(person_vid);
        person_id = other_person[id_prop_id]->toLLong();
        person_id_map[person_vid] = person_id;
      }
      auto key = std::make_pair(0 - like_creation_date, person_id);
      if (candidates.size() >= LIMIT_NUM && candidates.lower_bound(key) == candidates.end()) {
        continue;
      }
      candidates.emplace(key, std::make_tuple(person_vid, message_id, message_content,
                         (like_creation_date - message_creation_date) / 1000 / 60));
      candidates_index.emplace(person_vid, key);
      if (candidates.size() > LIMIT_NUM) {
        auto cit = --candidates.end();
        candidates_index.erase(candidates_index.find(std::get<0>(cit->second)));
        candidates.erase(cit);
      }
    }
  }
#ifdef DEBUG_PG
  t_count3 += std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start_time);
#endif
  delete []person_friends;
  delete []creation_date_list;
}

void PProcedure::ic7(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
  Node person(person_label_id, id_prop_id, &args[0], kvstore, txn);
  if (person.node_id_ == -1)
    return;
  TYPE_ENTITY_LITERAL_ID *person_friends = nullptr;
  unsigned friends_num = 0;
  tsl::hopscotch_set<TYPE_ENTITY_LITERAL_ID > friends;
  tsl::hopscotch_map<TYPE_ENTITY_LITERAL_ID, long long> person_id_map;
#ifdef DEBUG_PG
  auto start_time = std::chrono::steady_clock::now();
#endif
  person.GetLinkedNodes(knows_pre_id, person_friends, friends_num, Util::EDGE_IN);
#ifdef DEBUG_PG
  std::cout << "[GPSTORE][person <-knows<opt>- friend][op2s]: " << (std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start_time)).count() << " us" << std::endl;
  start_time = std::chrono::steady_clock::now();
#endif
  for (unsigned i = 0; i < friends_num; ++i) {
    auto friend_vid = person_friends[i];
    Node person_friend(friend_vid, kvstore, txn);
    friends.emplace(friend_vid);
    person_id_map[friend_vid] = person_friend[id_prop_id]->toLLong();
  }
  delete []person_friends;
#ifdef DEBUG_PG
  std::cout << "[GPSTORE][get " <<  friends_num << " friend.id][vidprop2value]: " << (std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start_time)).count() << " us" << std::endl;
  start_time = std::chrono::steady_clock::now();
#endif
  person_friends = nullptr;
  friends_num = 0;
  person.GetLinkedNodes(knows_pre_id, person_friends, friends_num, Util::EDGE_OUT);
#ifdef DEBUG_PG
  std::cout << "[GPSTORE][person -knows<opt>-> friend][sp2o]: " << (std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start_time)).count() << " us" << std::endl;
  start_time = std::chrono::steady_clock::now();
#endif
  for (unsigned i = 0; i < friends_num; ++i) {
    auto friend_vid = person_friends[i];
    Node person_friend(friend_vid, kvstore, txn);
    friends.emplace(friend_vid);
    person_id_map[friend_vid] = person_friend[id_prop_id]->toLLong();
  }
  delete []person_friends;
#ifdef DEBUG_PG
  std::cout << "[GPSTORE][get " <<  friends_num << " friend.id][vidprop2value]: " << (std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start_time)).count() << " us" << std::endl;
#endif

  tsl::hopscotch_map<long long, std::pair<long long, long long> > candidates_index;
  std::map<std::pair<long long, long long>, std::tuple<long long, long long, std::string, int> > candidates;
  Node other_person(kvstore, txn);
  // process person <- POST_HAS_CREATOR - Post
  TYPE_ENTITY_LITERAL_ID *messages_list = nullptr;
  unsigned messages_num = 0;
  long long *creation_date_list = nullptr;
  unsigned creation_data_width = 0; 
#ifdef DEBUG_PG
  start_time = std::chrono::steady_clock::now();
#endif
  person.GetLinkedNodesWithEdgeProps(post_has_creator_pre_id, messages_list, creation_date_list, creation_data_width, messages_num, Util::EDGE_IN);
//  person.GetLinkedNodes(post_has_creator_pre_id, messages_list, messages_num, Util::EDGE_IN);
#ifdef DEBUG_PG
  std::cout << "[GPSTORE][person <-hasCreator- post][op2s]: " << (std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start_time)).count() << " us" << std::endl;
  std::chrono::microseconds t_count1{0}, t_count2{0}, t_count3{0};
  unsigned friends_num_sum = 0;
#endif
  for (unsigned i = 0; i < messages_num; ++i) {
#ifdef DEBUG_PG
    start_time = std::chrono::steady_clock::now();
#endif
    Node message(messages_list[i], kvstore, txn);
    long long message_id = message[id_prop_id]->data_.Long;
    long long message_creation_date = creation_date_list[i];
//    long long message_creation_date = message[creation_date_prop_id]->data_.Long;
    std::string message_content;
    if (!message[content_prop_id]->isNull()) {
      message_content = message[content_prop_id]->toString();
    } else if (!message[imagefile_prop_id]->isNull()) {
      message_content = message[imagefile_prop_id]->toString();
    }
#ifdef DEBUG_PG
    t_count1 += std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start_time);
#endif
    ProcessMessageLikes(message, message_id, message_creation_date, message_content, other_person, person_id_map, candidates_index,
                        candidates, likes_pre_id, creationdate_prop_id, id_prop_id, kvstore, txn
#ifdef DEBUG_PG
                        , friends_num_sum, t_count2, t_count3
#endif
    );
  }
  delete []messages_list;
  delete []creation_date_list;
#ifdef DEBUG_PG
  std::cout << "[GPSTORE][get " <<  messages_num << " message.id, creationDate, content]: " << t_count1.count() << " us" << std::endl;
  std::cout << "[GPSTORE][message <-likes- friend]: " << t_count2.count() << " us" << std::endl;
  std::cout << "[GPSTORE][filter " <<  friends_num_sum << " person]: " << t_count3.count() << " us" << std::endl;
  start_time = std::chrono::steady_clock::now();
#endif
  // process person <- POST_HAS_CREATOR - comment
  messages_list = nullptr;
  messages_num = 0;
  creation_date_list = nullptr;
  creation_data_width = 0;
  person.GetLinkedNodesWithEdgeProps(comment_has_creator_pre_id, messages_list, creation_date_list, creation_data_width, messages_num, Util::EDGE_IN);
//  person.GetLinkedNodes(comment_has_creator_pre_id, messages_list, messages_num, Util::EDGE_IN);
#ifdef DEBUG_PG
  std::cout << "[GPSTORE][person <-hasCreator- Comment][op2s]: " << (std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start_time)).count() << " us" << std::endl;
  t_count1 = t_count2 = t_count3 = std::chrono::microseconds::zero();
  friends_num_sum = 0;
#endif
  for (unsigned i = 0; i < messages_num; ++i) {
#ifdef DEBUG_PG
    start_time = std::chrono::steady_clock::now();
#endif
    Node message(messages_list[i], kvstore, txn);
    long long message_id = message[id_prop_id]->data_.Long;
    long long message_creation_date = creation_date_list[i];
//    long long message_creation_date = message[creationdate_prop_id]->data_.Long;
    std::string message_content = message[content_prop_id]->toString();
#ifdef DEBUG_PG
    t_count1 += std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start_time);
#endif
    ProcessMessageLikes(message, message_id, message_creation_date, message_content, other_person, person_id_map, candidates_index,
                        candidates, likes_pre_id, creationdate_prop_id, id_prop_id, kvstore, txn
#ifdef DEBUG_PG
        , friends_num_sum, t_count2, t_count3
#endif
    );
  }
  delete []messages_list;
  delete []creation_date_list;
#ifdef DEBUG_PG
  std::cout << "[GPSTORE][get " <<  messages_num << " message.id, creationDate, content]: " << t_count1.count() << " us" << std::endl;
  std::cout << "[GPSTORE][message <-likes- friend]: " << t_count2.count() << " us" << std::endl;
  std::cout << "[GPSTORE][filter " <<  friends_num_sum << " person]: " << t_count3.count() << " us" << std::endl;
  start_time = std::chrono::steady_clock::now();
#endif

  for (const auto &candidate: candidates) {
    new_result->rows_.emplace_back();
    new_result->rows_.reserve(8);
    auto person_vid = std::get<0>(candidate.second);
    Node other_person(person_vid, kvstore, txn);
    new_result->rows_.back().values_.emplace_back(candidate.first.second);
    new_result->rows_.back().values_.emplace_back(*other_person[firstname_prop_id]);
    new_result->rows_.back().values_.emplace_back(*other_person[lastname_prop_id]);
    new_result->rows_.back().values_.emplace_back(0 - candidate.first.first);
    new_result->rows_.back().values_.emplace_back(std::get<1>(candidate.second));
    new_result->rows_.back().values_.emplace_back(std::get<2>(candidate.second));
    new_result->rows_.back().values_.emplace_back(std::get<3>(candidate.second));
    bool isNew = friends.find(person_vid) == friends.end();
    new_result->rows_.back().values_.emplace_back(isNew);
  }
#ifdef DEBUG_PG
  std::cout << "[GPSTORE][write_result]: " << (std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start_time)).count() << " us" << std::endl;
#endif
}

/**
 * @brief Stored procedure for the IC8 query ("Recent replies") of LDBC-SNB Interactive workload.
 * `Description of the query`: Given a start Person with ID $personId, find the most recent Comments that are replies to Messages of the start Person. 
 * Only consider direct (single-hop) replies, not the transitive (multi-hop) ones. Return the reply Comments, and the Person that created each reply Comment.
 * `Query plan`: Starting from a given person, get messages created by it, then get its reply comments and their creators and check if these creators.
 * 
 * @param args The arguments of the stored procedure
 * @param new_result The result of the stored procedure
 * @param kvstore Pointer to the KVStore instance
 * @param db Pointer to the Database instance
 * @param txn Pointer to the Transaction instance
 * @remark IC8存储过程
 */
void PProcedure::ic8(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
    const int retry_times = 5;
    const size_t limit_results = 20;

#ifdef DEBUG_PG
    auto start_time = std::chrono::steady_clock::now();
#endif
    Node person(person_label_id, id_prop_id, &args[0], kvstore, txn);
    if (person.node_id_ == -1)
        return;

    unsigned int post_list_len = 0, comment_list_len = 0;
    TYPE_ENTITY_LITERAL_ID* post_list = nullptr, * comment_list = nullptr;
    person.GetLinkedNodes(comment_has_creator_pre_id, comment_list, comment_list_len, Util::EDGE_IN);
    person.GetLinkedNodes(post_has_creator_pre_id, post_list, post_list_len, Util::EDGE_IN);
    std::vector<TYPE_ENTITY_LITERAL_ID> comments(comment_list_len), posts(post_list_len);
    for(unsigned i=0; i<comment_list_len; ++i)
        comments[i]=comment_list[i];
    for(unsigned i=0; i<post_list_len; ++i)
        posts[i]=post_list[i];
    delete[] comment_list;
    delete[] post_list;
#ifdef DEBUG_PG
    auto end_time = std::chrono::steady_clock::now();
    std::cout << "[GPSTORE][person<-hasCreator-message]: " << (std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time)).count() << " us" << std::endl;
    start_time = std::chrono::steady_clock::now();
#endif

    using result_type = std::set<std::tuple<GPStore::int_64, GPStore::int_64, std::string, TYPE_ENTITY_LITERAL_ID>>;
#ifdef DEBUG_PG
    std::chrono::microseconds query_creator_time = std::chrono::microseconds::zero();
#endif
    
    // comment creation_date descending + comment id ascending 
    // creation_date instead of -creation_date could be wrong: when update happens we remove candidates.begin()
    // consider: candidates.begin() a candidates.begin()+1 b, if a.creation_date==b.creation_date then a.comment_id<b.comment_id, when update happens we will remove a, however we ought to remove b
    auto process_message = [kvstore, txn, retry_times, limit_results
    ](Node&message, TYPE_PREDICATE_ID reply_of_pre_id, result_type& candidates
#ifdef DEBUG_PG
    ,std::chrono::microseconds& query_creator_time
#endif
    ){
#ifdef DEBUG_PG
        std::chrono::microseconds t = std::chrono::microseconds::zero();
#endif
        TYPE_ENTITY_LITERAL_ID*node_list = nullptr, *creator_list = nullptr;
        unsigned int node_list_len = 0, creator_list_len = 0;
        unsigned int xwidth = 0;
        GPStore::int_64 *x_list = nullptr;
        int retry_cnt=0;
        do{
            kvstore->getsubIDxvaluesByobjIDpreID(message.node_id_, reply_of_pre_id, node_list, x_list, xwidth, node_list_len, false, nullptr);
            ++retry_cnt;
        }while(node_list_len==0 && retry_cnt<retry_times);

        for(unsigned j = 0; j < node_list_len; ++j){
            Node comment(node_list[j], kvstore, txn);
            GPStore::int_64 comment_id = -1;
            GPStore::int_64 creation_date;
            creation_date = x_list[j*xwidth]; // only one property on REPLY_OF_POST or REPLY_OF_COMMENT
            if (candidates.size() >= limit_results) {
                auto& candidate = *candidates.rbegin();
                if (0 - creation_date > std::get<0>(candidate)) continue;
                if (creation_date == std::get<0>(candidate)) {
                    comment_id = (comment[id_prop_id])->toLLong();
                    if (comment_id > std::get<1>(candidate)) continue;
                }
            }
            if (comment_id == -1) 
            {
                comment_id = (comment[id_prop_id])->toLLong();
            }

#ifdef DEBUG_PG
            auto t1 = std::chrono::steady_clock::now();
#endif
            TYPE_ENTITY_LITERAL_ID creator;
            retry_cnt=0;
            do{
                comment.GetLinkedNodes(comment_has_creator_pre_id, creator_list, creator_list_len, Util::EDGE_OUT);
                ++retry_cnt;
            }while(creator_list_len<1 && retry_cnt<retry_times); // this loop is necessary when worker_num > 1
            if(creator_list_len)
            {
                creator = creator_list[0];
                delete[] creator_list;
#ifdef DEBUG_PG
                auto t2 = std::chrono::steady_clock::now();
                t+=std::chrono::duration_cast<std::chrono::microseconds>(t2-t1);
#endif
                GPStore::Value* cp=comment[content_prop_id];
                candidates.emplace(0 - creation_date, comment_id, cp->toString(), creator);
            }
            else
                candidates.emplace(0 - creation_date, comment_id, "", (TYPE_ENTITY_LITERAL_ID)-1ull);
            if(candidates.size() > limit_results){
                candidates.erase(--candidates.end());
            }
        }
        if(node_list_len)
            delete[] node_list;
#ifdef DEBUG_PG
        query_creator_time += t;
#endif
    };
    

    auto merge_result = [&](const result_type& local, result_type& res){
        for (auto& r : local) res.emplace(r);
    };
    auto candidates_comment = ForEachVertex<result_type>(
        kvstore, txn, comments,
        [&](Node& message, result_type& local){
            process_message(message, reply_of_comment_pre_id, local
#ifdef DEBUG_PG
            ,std::ref(query_creator_time) 
#endif
            );
        },
        merge_result
    );
    auto candidates = ForEachVertex<result_type>(
        kvstore, txn, posts,
        [&](Node& message, result_type& local){
            process_message(message, reply_of_post_pre_id, local
#ifdef DEBUG_PG
            ,std::ref(query_creator_time) 
#endif
            );
        },
        merge_result
    );
    merge_result(candidates_comment, candidates);

#ifdef DEBUG_PG
    end_time = std::chrono::steady_clock::now();
    long long second_step = (std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time)).count();
    long long second_step_sum = 0, substep_average_time;
    std::cout << "[GPSTORE][message<-replyOf-message (include topk select)]: " << second_step << " us" << std::endl;
    substep_average_time = query_creator_time.count()/worker_num;
    second_step_sum += substep_average_time;
    std::cout << "[GPSTORE]\t[query_creator_time][average per thread]: " << substep_average_time << " us" << std::endl;
    std::cout << "[GPSTORE]\t[rest][average per thread]: " << second_step-second_step_sum << " us" << std::endl;
    
    start_time = std::chrono::steady_clock::now();
#endif
    int res_size = std::min(candidates.size(), limit_results); 
    int res_count = 0;
    for (auto& tup : candidates) {
      new_result->rows_.emplace_back();
      new_result->rows_.reserve(6);
      TYPE_ENTITY_LITERAL_ID person_vid = std::get<3>(tup);
      Node person(person_vid, kvstore, txn);
      new_result->rows_.back().values_.emplace_back(*person[id_prop_id]);
      new_result->rows_.back().values_.emplace_back(*person[firstname_prop_id]);
      new_result->rows_.back().values_.emplace_back(*person[lastname_prop_id]);
      new_result->rows_.back().values_.emplace_back(0 - std::get<0>(tup));
      new_result->rows_.back().values_.emplace_back(std::get<1>(tup));
      new_result->rows_.back().values_.emplace_back(std::get<2>(tup));
      if (++res_count == res_size) break;
    }
#ifdef DEBUG_PG
    end_time = std::chrono::steady_clock::now();
    std::cout << "[GPSTORE][write result]: " << (std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time)).count() << " us" << std::endl;
#endif
}

void PProcedure::ic9(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
  long long max_date = args[1].data_.Long;

  Node person(person_label_id, id_prop_id, &args[0], kvstore, txn);
  if (person.node_id_ == -1)
    return;
  std::set<std::tuple<long long, long long, TYPE_ENTITY_LITERAL_ID, int >> candidates;
  tsl::hopscotch_set<TYPE_ENTITY_LITERAL_ID > visited{person.node_id_};
  std::vector<TYPE_ENTITY_LITERAL_ID > curr_frontier{person.node_id_};
  TYPE_ENTITY_LITERAL_ID *person_friends;
  unsigned friends_num;
  TYPE_ENTITY_LITERAL_ID *messages_list;
  unsigned message_num;
  long long *creation_date_list = nullptr;
  unsigned creation_data_width = 0;
#ifdef DEBUG_PG
  std::chrono::microseconds t_count1{0}, t_count2{0}, t_count3{0}, all{0};
  int friends_num_sum = 0, messages_num_sum = 0;
  auto start_time = std::chrono::steady_clock::now();
#endif
  for (int hop = 0; hop <= 2; ++hop) {
    std::vector<TYPE_ENTITY_LITERAL_ID > next_frontier;
    for (auto vid : curr_frontier) {
      Node other_person(vid, kvstore, txn);
      if (hop < 2) {
#ifdef DEBUG_PG
        start_time = std::chrono::steady_clock::now();
#endif
        person_friends = nullptr;
        friends_num = 0;
        other_person.GetLinkedNodes(knows_pre_id, person_friends, friends_num, Util::EDGE_OUT);
        for (unsigned i = 0; i < friends_num; ++i) {
          if (visited.find(person_friends[i]) == visited.end()) {
            visited.emplace(person_friends[i]);
            next_frontier.emplace_back(person_friends[i]);
          }
        }
        delete []person_friends;
#ifdef DEBUG_PG
        friends_num_sum += friends_num;
#endif
        person_friends = nullptr;
        friends_num = 0;
        other_person.GetLinkedNodes(knows_pre_id, person_friends, friends_num, Util::EDGE_IN);
        for (unsigned i = 0; i < friends_num; ++i) {
          if (visited.find(person_friends[i]) == visited.end()) {
            visited.emplace(person_friends[i]);
            next_frontier.emplace_back(person_friends[i]);
          }
        }
        delete []person_friends;
#ifdef DEBUG_PG
        t_count1 += std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start_time);
        friends_num_sum += friends_num;
#endif
      }
      if (hop == 0) {
        continue;
      }

      // Process otherPerson <-hasCreator- post
#ifdef DEBUG_PG
      start_time = std::chrono::steady_clock::now();
#endif
      messages_list = nullptr;
      message_num = 0;
      creation_date_list = nullptr;
      creation_data_width = 0;
      // other_person.GetLinkedNodes(post_has_creator_pre_id, messages_list, message_num, Util::EDGE_IN);
      other_person.GetLinkedNodesWithEdgeProps(post_has_creator_pre_id, messages_list, creation_date_list, creation_data_width, message_num, Util::EDGE_IN);
      // assert(creation_date_list == nullptr || creation_data_width == 1);
#ifdef DEBUG_PG
      t_count2 += std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start_time);
      messages_num_sum += message_num;
      start_time = std::chrono::steady_clock::now();
#endif
      for (unsigned i = 0; i < message_num; ++i) {
        // Node friend_message(messages_list[i], kvstore, txn);
        // long long creation_date = friend_message[creationdate_prop_id]->data_.Long;
        long long creation_date = creation_date_list[i];
        long long message_id = -1;
        if (creation_date >= max_date) {
          continue;
        }
        if (candidates.size() >= LIMIT_NUM) {
          auto &candidate = *candidates.rbegin();
          if (0 - creation_date > std::get<0>(candidate)) {
            continue;
          }
          if (0 - creation_date == std::get<0>(candidate)) {
            Node friend_message(messages_list[i], kvstore, txn);
            message_id = friend_message[id_prop_id]->data_.Long;
            if (message_id > std::get<1>(candidate)) {
              continue;
            }
          }
        }
        if (message_id == -1) {
          Node friend_message(messages_list[i], kvstore, txn);
          message_id = friend_message[id_prop_id]->data_.Long;
        }
        candidates.emplace(0-creation_date, message_id, other_person.node_id_, static_cast<int>(messages_list[i]));
        if (candidates.size() > LIMIT_NUM) {
          candidates.erase(--candidates.end());
        }
      }
      delete []messages_list;
      delete []creation_date_list;
#ifdef DEBUG_PG
      t_count3 += std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start_time);
#endif
      // Process otherPerson <-hasCreator- comment
#ifdef DEBUG_PG
      start_time = std::chrono::steady_clock::now();
#endif
      messages_list = nullptr;
      message_num = 0;
      creation_date_list = nullptr;
      creation_data_width = 0;
      // other_person.GetLinkedNodes(comment_has_creator_pre_id, messages_list, message_num, Util::EDGE_IN);
      other_person.GetLinkedNodesWithEdgeProps(comment_has_creator_pre_id, messages_list, creation_date_list, creation_data_width, message_num, Util::EDGE_IN);
      // assert(creation_date_list == nullptr || creation_data_width == 1);
#ifdef DEBUG_PG
      t_count2 += std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start_time);
      messages_num_sum += message_num;
      start_time = std::chrono::steady_clock::now();
#endif
      for (unsigned i = 0; i < message_num; ++i) {
        // Node friend_message(messages_list[i], kvstore, txn);
        // long long creation_date = friend_message[creationdate_prop_id]->data_.Long;
        long long creation_date = creation_date_list[i];
        long long message_id = -1;
        if (creation_date >= max_date) {
          continue;
        }
        if (candidates.size() >= LIMIT_NUM) {
          auto &candidate = *candidates.rbegin();
          if (0 - creation_date > std::get<0>(candidate)) {
            continue;
          }
          if (0 - creation_date == std::get<0>(candidate)) {
            Node friend_message(messages_list[i], kvstore, txn);
            message_id = friend_message[id_prop_id]->data_.Long;
            if (message_id > std::get<1>(candidate)) {
              continue;
            }
          }
        }
        if (message_id == -1) {
          Node friend_message(messages_list[i], kvstore, txn);
          message_id = friend_message[id_prop_id]->data_.Long;
        }
        candidates.emplace(0-creation_date, message_id, other_person.node_id_, 0 - static_cast<int>(messages_list[i]));
        if (candidates.size() > LIMIT_NUM) {
          candidates.erase(--candidates.end());
        }
      }
      delete []messages_list;
      delete []creation_date_list;
#ifdef DEBUG_PG
      t_count3 += std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start_time);
#endif
    }
#ifdef DEBUG_PG
    if (hop == 2) {
        continue;
    }
    std::cout << "[GPSTORE][person -knows- otherPerson]: " << t_count1.count() << " us; hop=" << hop << "; friends_num_sum=" << friends_num_sum << std::endl;
    friends_num_sum = 0;
    t_count1 = std::chrono::microseconds::zero();;
#endif
    std::sort(next_frontier.begin(), next_frontier.end());
    curr_frontier.swap(next_frontier);
  }
#ifdef DEBUG_PG
  std::cout << "[GPSTORE][otherPerson <-hasCreator- message]: " << t_count2.count() << " us" << std::endl;
  std::cout << "[GPSTORE][filter " <<  messages_num_sum << " messages]: " << t_count3.count() << " us" << std::endl;
  start_time = std::chrono::steady_clock::now();
#endif

  for (const auto &candidate: candidates) {
    Node other_person(std::get<2>(candidate), kvstore, txn);

    new_result->rows_.emplace_back();
    new_result->rows_.back().values_.reserve(6);
    new_result->rows_.back().values_.emplace_back(*other_person[id_prop_id]);
    new_result->rows_.back().values_.emplace_back(*other_person[firstname_prop_id]);
    new_result->rows_.back().values_.emplace_back(*other_person[lastname_prop_id]);
    new_result->rows_.back().values_.emplace_back(std::get<1>(candidate));
    if (std::get<3>(candidate) >= 0) {
      Node message(std::get<3>(candidate), kvstore, txn);
      auto content = message[content_prop_id];
      new_result->rows_.back().values_.emplace_back(content->isNull() ? *message[imagefile_prop_id] : *content);
    } else {
      Node message(0 - std::get<3>(candidate), kvstore, txn);
      new_result->rows_.back().values_.emplace_back(*message[content_prop_id]);
    }
    new_result->rows_.back().values_.emplace_back(0 - std::get<0>(candidate));
  }
#ifdef DEBUG_PG
  std::cout << "[GPSTORE][write_result]: " << (std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start_time)).count() << " us" << std::endl;
#endif
}

/**
 * @brief Stored procedure for the IC10 query ("Friend recommendation") of LDBC-SNB Interactive workload.
 * `Query plan`: Starting from a given person, find its 2-hop friends (friends of friends); 
 * collect tags that the start person is interested in;
 * then calculate the similarity between each friend and the start person by examing the posts created by friend:
 * common = number of Posts created by friend, such that the Post has a Tag that the start person is interested in
 * uncommon = number of Posts created by friend, such that the Post has no Tag that the start person is interested in
 * commonInterestScore = common - uncommon
 * 
 * @param args The arguments of the stored procedure
 * @param new_result The result of the stored procedure
 * @param kvstore Pointer to the KVStore instance
 * @param db Pointer to the Database instance
 * @param txn Pointer to the Transaction instance
 * @remark IC10存储过程
 */
void PProcedure::ic10(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
    const size_t limit_results = 10;
    const int retry_times = 5;

    tsl::hopscotch_set<TYPE_LABEL_ID > interested_tags;
    Node person_node(person_label_id, id_prop_id, &args[0], kvstore, txn);
    if (person_node.node_id_ == -1)
        return;

    int32_t month = args[1].toInt();
    int32_t next_month = month % 12 + 1;

    TYPE_ENTITY_LITERAL_ID* list_ptr = nullptr; unsigned list_len;
    person_node.GetLinkedNodes(has_interest_pre_id, list_ptr, list_len, Util::EDGE_OUT);
    for (unsigned index = 0; index < list_len; ++index) {
        interested_tags.emplace(list_ptr[index]);
    }
    tsl::hopscotch_set<TYPE_ENTITY_LITERAL_ID> visited({person_node.node_id_});
    std::vector<TYPE_ENTITY_LITERAL_ID > curr_frontier({person_node.node_id_});
    for (int hop = 0; hop < 2; hop++) {
        std::vector<TYPE_ENTITY_LITERAL_ID > next_frontier;
        for (auto vid : curr_frontier) {
            Node person(vid, kvstore, txn);
            person.GetLinkedNodes(knows_pre_id, list_ptr, list_len, Util::EDGE_OUT);
            for (unsigned index = 0; index < list_len; ++index) {
                TYPE_ENTITY_LITERAL_ID friend_vid = list_ptr[index];
                if (visited.find(friend_vid) == visited.end()) {
                    visited.emplace(friend_vid);
                    next_frontier.emplace_back(friend_vid);
                }
            }
            delete[] list_ptr;

            person.GetLinkedNodes(knows_pre_id, list_ptr, list_len, Util::EDGE_IN);
            for (unsigned index = 0; index < list_len; ++index) {
                TYPE_ENTITY_LITERAL_ID friend_vid = list_ptr[index];
                if (visited.find(friend_vid) == visited.end()) {
                    visited.emplace(friend_vid);
                    next_frontier.emplace_back(friend_vid);
                }
            }
            delete[] list_ptr;
        }
        std::sort(next_frontier.begin(), next_frontier.end());
        curr_frontier.swap(next_frontier);
    }

    std::sort(curr_frontier.begin(), curr_frontier.end());

    using result_type = std::set<std::tuple<GPStore::int_32, GPStore::int_64, TYPE_ENTITY_LITERAL_ID>>; // score, fof id, TYPE_ENTITY_LITERAL_ID of fof
    auto process_foaf = [limit_results, kvstore, txn, month, next_month, &interested_tags](Node&foaf, result_type& result){
        TYPE_ENTITY_LITERAL_ID foaf_vid = foaf.node_id_;
        TYPE_ENTITY_LITERAL_ID* list_ptr = nullptr; unsigned list_len;

        // ! below commented part is not threadsafe
        // GPStore::Value birthday(GPStore::Value::DATE_TIME);
        // GPStore::Value* bp=foaf[birthday_prop_id];
        // birthday.SetDatetime(bp->toLLong(), 0);
        // auto month_day = birthday.getMonthDay();

        GPStore::Value* bp=foaf[birthday_prop_id];
        std::pair<GPStore::uint_16, GPStore::uint_16> month_day;
        {
            std::chrono::milliseconds timestamp(bp->toLLong());
            std::time_t timestampSec = std::chrono::duration_cast<std::chrono::seconds>(timestamp).count();
            std::tm localTime;
            localtime_r(&timestampSec, &localTime);

            month_day.first = localTime.tm_mon + 1;
            if(localTime.tm_hour >= 8){
                month_day.second = localTime.tm_mday;
            }
            else{
                month_day.second = localTime.tm_mday-1;
            }
        }

        bool ok = (month_day.first == month && month_day.second >= 21) ||
                (month_day.first == next_month && month_day.second < 22);
        if (!ok) return;
        // // DEBUG
        // std::cout << "pass birthday check: \033[34;1m" << (foaf["id"])->toString() << "\033[0m" << std::endl; // blue in cmd
        // [32 green
        GPStore::int_32 score = 0;
        foaf.GetLinkedNodes(post_has_creator_pre_id, list_ptr, list_len, Util::EDGE_IN);
        TYPE_ENTITY_LITERAL_ID *tag_list = nullptr; 
        unsigned tag_list_len;
        
        for (unsigned index = 0; index < list_len; ++index) {
            Node post(list_ptr[index], kvstore, txn);
            post.GetLinkedNodes(post_has_tag_pre_id, tag_list, tag_list_len, Util::EDGE_OUT);
            ok = false;
            for (unsigned tag_index = 0; tag_index < tag_list_len; ++tag_index) {
                if (interested_tags.find(tag_list[tag_index]) != interested_tags.end()) {
                    ok = true;
                    break;
                }
            }
            delete[] tag_list;

            score += (ok ? +1 : -1);
        }

        delete[] list_ptr;

        int64_t person_id = -1;
        if (result.size() >= limit_results) {
            auto &cand = *result.rbegin();
            if (0 - score > std::get<0>(cand)) return;
            if (0 - score == std::get<0>(cand)) {
                person_id = foaf[id_prop_id]->toLLong();
                if (person_id > std::get<1>(cand)) return;
            }
        }
        if (person_id == -1) 
        {
            person_id = foaf[id_prop_id]->toLLong();
        }
        result.emplace(0 - score, person_id, foaf_vid);
        if (result.size() > limit_results) {
            result.erase(--result.end());
        }
    };

    auto candidates = ForEachVertex<result_type>(
        kvstore, txn, curr_frontier,
        [&](Node& foaf, result_type& local){
            process_foaf(foaf, local);
        },
        [&](const result_type& local, result_type& res){
            for (auto& r : local) res.emplace(r);
        }
    );

    int res_size = std::min(candidates.size(), limit_results);
    int res_count = 0;
    for (auto &tup : candidates) {
        new_result->rows_.emplace_back();
        new_result->rows_.reserve(6);
        new_result->rows_.back().values_.emplace_back(std::get<1>(tup));
        Node person(std::get<2>(tup), kvstore, txn);
        new_result->rows_.back().values_.emplace_back(*person[firstname_prop_id]);
        new_result->rows_.back().values_.emplace_back(*person[lastname_prop_id]);
        new_result->rows_.back().values_.emplace_back(0 - std::get<0>(tup));
        new_result->rows_.back().values_.emplace_back(*person[gender_prop_id]);
        TYPE_ENTITY_LITERAL_ID place_id;
        list_len = 0;
        list_ptr = nullptr;
        int retry_cnt = 0;
        do{
            person.GetLinkedNodes(person_is_located_in_pre_id, list_ptr, list_len, Util::EDGE_OUT);
            ++retry_cnt;
        }while(list_len<1 && retry_cnt<retry_times);
        if(list_len)
        {
            place_id = list_ptr[0];
            delete[] list_ptr;
            Node place(place_id, kvstore, txn);
            new_result->rows_.back().values_.emplace_back(*place[name_prop_id]);
        }
        else
            new_result->rows_.back().values_.emplace_back(GPStore::Value::NO_VALUE);
        if (++res_count == res_size) break;
    }
}

/**
 * @brief Stored procedure for the IC11 query ("job referral") of LDBC-SNB Interactive workload.
 * Starting from a given person, find that Person’s friends and friends of friends (excluding start Person)
 * who started working in some Company in a given Country with name $countryName, before a given date ($workFromYear).
 * @param args The arguments of the stored procedure
 * @param new_result The result of the stored procedure
 * @param kvstore Pointer to the KVStore instance
 * @param db Pointer to the Database instance
 * @param txn Pointer to the Transaction instance
 * @remark IC11存储过程
 */
void PProcedure::ic11(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result,
                      const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
  constexpr size_t limit_results = 10;
  tsl::hopscotch_set<TYPE_ENTITY_LITERAL_ID > visited;
  Node person_node(person_label_id, id_prop_id, &args[0], kvstore, txn);
  if (person_node.node_id_ == -1)
    return;

  TYPE_ENTITY_LITERAL_ID* list_ptr = nullptr; unsigned list_len;
  std::vector<TYPE_ENTITY_LITERAL_ID > curr_frontier({person_node.node_id_});
  for (int hop = 0; hop < 2; hop++) {
    std::vector<TYPE_ENTITY_LITERAL_ID > next_frontier;
    for (auto vid : curr_frontier) {
      Node person(vid, kvstore, txn);
      person.GetLinkedNodes(knows_pre_id, list_ptr, list_len, Util::EDGE_OUT);
      for (unsigned index = 0; index < list_len; ++index) {
        TYPE_ENTITY_LITERAL_ID friend_vid = list_ptr[index];
        if (visited.find(friend_vid) == visited.end()) {
          visited.emplace(friend_vid);
          next_frontier.emplace_back(friend_vid);
        }
      }
      delete[] list_ptr;
      person.GetLinkedNodes(knows_pre_id, list_ptr, list_len, Util::EDGE_IN);
      for (unsigned index = 0; index < list_len; ++index) {
        TYPE_ENTITY_LITERAL_ID friend_vid = list_ptr[index];
        if (visited.find(friend_vid) == visited.end()) {
          visited.emplace(friend_vid);
          next_frontier.emplace_back(friend_vid);
        }
      }
      delete[] list_ptr;
    }
    if (hop == 1) break;
    std::sort(next_frontier.begin(), next_frontier.end());
    curr_frontier.swap(next_frontier);
  }
  visited.erase(person_node.node_id_);

  TYPE_ENTITY_LITERAL_ID* country_list = nullptr; unsigned country_list_len = 0;
  kvstore->find_index(country_label_id, name_prop_id, &args[1], country_list, country_list_len, txn);
  if (!country_list_len) return;
  std::vector<std::tuple<int32_t, int64_t, std::string, std::string, std::string>> result;
  Node country(country_list[0], kvstore, txn);
  delete[] country_list;
  country.GetLinkedNodes(organisation_is_located_in_pre_id, list_ptr, list_len, Util::EDGE_IN);

  for (unsigned index = 0; index < list_len; ++index) {
    unsigned company_node_id = list_ptr[index];
    Node company(company_node_id, kvstore, txn);
    TYPE_ENTITY_LITERAL_ID *person_list = nullptr; unsigned person_list_len;
    long long* prop_list; unsigned prop_len = 0;
    company.GetLinkedNodesWithEdgeProps(work_at_pre_id, person_list, prop_list, prop_len, person_list_len, Util::EDGE_IN);
//    company.GetLinkedNodes(work_at_pre_id, person_list, person_list_len, Util::EDGE_IN);
    for (unsigned person_index = 0; person_index < person_list_len; ++person_index) {
      if (!visited.contains(person_list[person_index])) continue;
      if (prop_list[person_index] < args[2].toLLong()) {
        Node person0(person_list[person_index], kvstore, txn);
        auto person_id0 = person0[id_prop_id]->toLLong();
        auto person_first_name = person0[firstname_prop_id]->toString();
        auto person_last_name = person0[lastname_prop_id]->toString();
        result.emplace_back(0 - prop_list[person_index], 0 - person_id0, company[name_prop_id]->toString(),
                            person_first_name, person_last_name);
      }
    }
    delete[] person_list; delete[] prop_list;
  }
  delete[] list_ptr;
  std::sort(result.begin(), result.end(),
            std::greater<std::tuple<int32_t, int64_t, std::string, std::string, std::string>>());
  unsigned result_size = result.size() > limit_results ? limit_results : result.size();
  for (int i = 0; i < result_size; i++) {
    const auto &tup = result[i];
    new_result->rows_.emplace_back();
    new_result->rows_.back().values_.reserve(5);
    new_result->rows_.back().values_.emplace_back((long long) (0 - std::get<1>(tup)));
    new_result->rows_.back().values_.emplace_back(std::get<3>(tup));
    new_result->rows_.back().values_.emplace_back(std::get<4>(tup));
    new_result->rows_.back().values_.emplace_back(std::get<2>(tup));
    new_result->rows_.back().values_.emplace_back(0 - std::get<0>(tup));
  }
}
/**
 * @brief Stored procedure for the IC12 query ("Expert Search") of LDBC-SNB Interactive workload.
 * Starting from a given tag class, find and record all its descendant tags by the “isSubclassOf” edges using BFS. Starting from a given person, get all their friends, then get the comments they created that replies to a post with a tag in the aforementioned descendant tag set by invoking `ProcessPersonCommentsIC12` in parallel.
 * 
 * @param args The arguments of the stored procedure
 * @param new_result The result of the stored procedure
 * @param kvstore Pointer to the KVStore instance
 * @param db Pointer to the Database instance
 * @param txn Pointer to the Transaction instance
 * @remark IC12存储过程
 */
void PProcedure::ic12(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
    // Parse args
    constexpr size_t limit_results = 20;
    if (args.size() != 2)
        return;
    const auto &startPersonId = args[0], &tagClassName = args[1];

    // Get tags from tagClass by BFS
    #ifdef DEBUG_PG
    auto start_time = std::chrono::steady_clock::now();
    #endif
    Node tagClassNode = Node(tagclass_label_id, name_prop_id, &tagClassName, kvstore, txn);
    TYPE_ENTITY_LITERAL_ID tagClassNodeId = tagClassNode.node_id_;
    if (tagClassNodeId == -1)
        return;
    Node personNode(person_label_id, id_prop_id, &args[0], kvstore, txn);
    if (personNode.node_id_ == -1)
        return;
    tsl::hopscotch_set<TYPE_ENTITY_LITERAL_ID> visited({tagClassNodeId});
    tsl::hopscotch_map<TYPE_ENTITY_LITERAL_ID, std::string> tagInfo;
    std::vector<TYPE_ENTITY_LITERAL_ID> currFrontier({tagClassNodeId});
    while (!currFrontier.empty()) {
        std::vector<TYPE_ENTITY_LITERAL_ID> nextFrontier;
        for (auto vid : currFrontier) {
            Node curNode(vid, kvstore, txn);
            TYPE_ENTITY_LITERAL_ID *tagList = nullptr;
            unsigned tagListLen = 0;
            curNode.GetLinkedNodes(has_type_pre_id, tagList, tagListLen, Util::EDGE_IN);
            for (unsigned index = 0; index < tagListLen; ++index) {
                TYPE_ENTITY_LITERAL_ID tagId = tagList[index];
                if (tagInfo.find(tagId) == tagInfo.end()) {
                    Node tagNode(tagId, kvstore, txn);
                    tagInfo.emplace(tagId, *tagNode[name_prop_id]->data_.String);
                }
            }
            delete[] tagList;
            curNode.GetLinkedNodes(is_subclass_of_pre_id, tagList, tagListLen, Util::EDGE_IN);
            for (unsigned index = 0; index < tagListLen; ++index) {
                TYPE_ENTITY_LITERAL_ID tagClassId = tagList[index];
                if (visited.find(tagClassId) == visited.end()) {
                    visited.emplace(tagClassId);
                    nextFrontier.emplace_back(tagClassId);
                }
            }
            delete[] tagList;
        }
        currFrontier.swap(nextFrontier);
    }
    #ifdef DEBUG_PG
    auto end_time = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
    std::cout << "[GPSTORE]tagClass BFS: " << elapsed.count() << " us" << std::endl;
    #endif

    // Find all friends of the starting person
    #ifdef DEBUG_PG
    start_time = std::chrono::steady_clock::now();
    #endif
    std::vector<TYPE_ENTITY_LITERAL_ID> friends;
    TYPE_ENTITY_LITERAL_ID *node_list = nullptr;
    unsigned node_list_len = 0;
    personNode.GetLinkedNodes(knows_pre_id, node_list, node_list_len, Util::EDGE_IN);
    for(int i = 0; i < node_list_len; ++i)
        friends.push_back(node_list[i]);
    delete []node_list;
    personNode.GetLinkedNodes(knows_pre_id, node_list, node_list_len, Util::EDGE_OUT);
    for(int i = 0; i < node_list_len; ++i)
        friends.push_back(node_list[i]);
    delete []node_list;
    #ifdef DEBUG_PG
    end_time = std::chrono::steady_clock::now();
    elapsed = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
    std::cout << "[GPSTORE]Find friends: " << elapsed.count() << " us" << std::endl;
    #endif

    // ProcessPersonComments in parallel for all friends
    #ifdef DEBUG_PG
    start_time = std::chrono::steady_clock::now();
    #endif
    using result_type = std::set<std::tuple<int32_t, int64_t, std::vector<int64_t>, int64_t>>;
    auto candidates = ForEachVertex<result_type>(
        kvstore, txn, friends,
        [&](Node& person, result_type& local){
            PProcedure::ProcessPersonCommentsIC12(person, local, tagInfo, limit_results);
        },
        [&](const result_type& local, result_type& res){
            for (auto& r : local) res.emplace(r);
        }
    );
    #ifdef DEBUG_PG
    end_time = std::chrono::steady_clock::now();
    elapsed = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
    std::cout << "[GPSTORE]ProcessPersonComments: " << elapsed.count() << " us" << std::endl;
    #endif

    // Write new_result
    #ifdef DEBUG_PG
    start_time = std::chrono::steady_clock::now();
    #endif
    for (const auto &tup : candidates) {
        if(new_result->getSize() == limit_results) break;
        new_result->rows_.emplace_back();
        TYPE_ENTITY_LITERAL_ID friendId = std::get<3>(tup);
        Node friendNode(friendId, kvstore, txn);
        new_result->rows_.back().values_.emplace_back(GPStore::int_64(std::get<1>(tup)));
        new_result->rows_.back().values_.emplace_back(*friendNode[firstname_prop_id]);
        new_result->rows_.back().values_.emplace_back(*friendNode[lastname_prop_id]);
        new_result->rows_.back().values_.emplace_back(GPStore::Value::LIST);
        new_result->rows_.back().values_.back().data_.List = new std::vector<GPStore::Value *>();
        auto &tagList = std::get<2>(tup);
        for (auto tagVid : tagList) {
            GPStore::Value *tagValue = new GPStore::Value(tagInfo[tagVid].c_str());
            new_result->rows_.back().values_.back().data_.List->emplace_back(tagValue);
        }
        new_result->rows_.back().values_.emplace_back(0 - std::get<0>(tup));
    }
    #ifdef DEBUG_PG
    end_time = std::chrono::steady_clock::now();
    elapsed = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
    std::cout << "[GPSTORE][write_result]: " << elapsed.count() << " us" << std::endl;
    #endif
}
/**
 * @brief A helper function for the IC12 query ("Expert Search") of LDBC-SNB Interactive workload.
 * Given person and a set of tags, count how many comments created by this person reply to posts that have tags in the tag set, and put this information into the candidate set if the number of such posts could be among the top-k.
 * 
 * @param person The given person node
 * @param candidates The return candidate set, where each candidate consists of a person, the number of comments satisfying the constraint, and the tags of the posts replied by the comments in the given tag set
 * @param tagInfo The given set of tags
 * @param limit_results The maximum size of the candidate set (from the LIMIT clause in the query)
 * @remark IC12帮助函数-处理用户评论
 */
void PProcedure::ProcessPersonCommentsIC12(Node& person, std::set<std::tuple<int32_t, int64_t, std::vector<int64_t>, int64_t>> &candidates,
const tsl::hopscotch_map<TYPE_ENTITY_LITERAL_ID, std::string> &tagInfo, size_t limit_results) {
    int32_t count = 0;
    tsl::hopscotch_set<int64_t> tag_set;
    TYPE_ENTITY_LITERAL_ID *node_list = nullptr;
    unsigned node_list_len = 0;
    person.GetLinkedNodes(comment_has_creator_pre_id, node_list, node_list_len, Util::EDGE_IN);
    for(int i = 0; i < node_list_len; ++i) {
        Node commentNode(node_list[i], person.kv_store_, person.txn_);
        TYPE_ENTITY_LITERAL_ID *node_list_i = nullptr;
        unsigned node_list_len_i = 0;
        commentNode.GetLinkedNodes(reply_of_post_pre_id, node_list_i, node_list_len_i, Util::EDGE_OUT);
        if (node_list_len_i == 0) {
            delete[] node_list_i;
            continue;
        }
        // Filter out none-post nodes
        Node postNode(node_list_i[0], person.kv_store_, person.txn_);
        postNode.GetLinkedNodes(post_has_tag_pre_id, node_list_i, node_list_len_i, Util::EDGE_OUT);
        if (node_list_len_i == 0) {
            delete[] node_list_i;
            continue;
        }
        bool ok = false;
        for (int j = 0; j < node_list_len_i; j++) {
            TYPE_ENTITY_LITERAL_ID tag_vid = node_list_i[j];
            if (tagInfo.find(tag_vid) != tagInfo.end()) {
                tag_set.emplace(tag_vid);
                ok = true;
            }
        }
        if (ok) count++;
        delete []node_list_i;   // TODO: remove frequent new/delete?
    }
    delete []node_list;
    if (count == 0) return;
    int64_t person_id = -1;
    if (candidates.size() >= limit_results) {
        auto& candidate = *candidates.rbegin();
        if (0 - count > std::get<0>(candidate)) return;
        if (0 - count == std::get<0>(candidate)) {
            person_id = person[id_prop_id]->data_.Long;
            if (person_id > std::get<1>(candidate)) return;
        }
    } else {
        person_id = person[id_prop_id]->data_.Long;
    }
    std::vector<int64_t> tag_list;
    for (auto tag_vid : tag_set) {
        tag_list.emplace_back(tag_vid);
    }
    candidates.emplace(0 - count, person_id, tag_list, person.node_id_);
    if (candidates.size() > limit_results) {
        candidates.erase(--candidates.end());
    }
}

/**
 * @brief Stored procedure for the IC13 query ("Single shortest path") of LDBC-SNB Interactive workload.
 * Given two Persons with IDs $person1Id and $person2Id, find the shortest path between these two Persons in the subgraph induced by the knows edges. Return the length of this path
 * 
 * @param args The arguments of the stored procedure
 * @param new_result The result of the stored procedure
 * @param kvstore Pointer to the KVStore instance
 * @param db Pointer to the Database instance
 * @param txn Pointer to the Transaction instance
 * @remark IC13存储过程
 */
void PProcedure::ic13(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
    if (args.size() != 2)
        return;
    if (args[0].data_.Long == args[1].data_.Long)
        return;
    Node src_node(person_label_id, id_prop_id, &args[0], kvstore, txn);
    Node dst_node(person_label_id, id_prop_id, &args[1], kvstore, txn);

    TYPE_PREDICATE_ID knowsPred = knows_pre_id;
    vector<GPStore::uint_32> sp;
    vector<TYPE_PREDICATE_ID> pred_set({knowsPred});
    int len = GetShortestPathLen(src_node.node_id_, dst_node.node_id_, false, pred_set, sp, kvstore);
    new_result->rows_.emplace_back();
    new_result->rows_.back().values_.resize(1);
    new_result->rows_.back().values_[0] = (int)(len);
}

/**
 * @brief Stored procedure for the IS1 query ("profile of a person") of LDBC-SNB Interactive workload.
 * Given a start Person with ID $personId, retrieve his/her first name, last name, birthday, IP address, browser, and city of residence
 * @param args The arguments of the stored procedure
 * @param new_result The result of the stored procedure
 * @param kvstore Pointer to the KVStore instance
 * @param db Pointer to the Database instance
 * @param txn Pointer to the Transaction instance
 * @remark IS1存储过程
 */
void PProcedure::is1(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result,
                     const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
  Node person_node(person_label_id, id_prop_id, &args[0], kvstore, txn);
  if (person_node.node_id_ == -1)
    return;
  new_result->rows_.emplace_back();
  new_result->rows_.back().values_.reserve(8);
  new_result->rows_.back().values_.emplace_back(*person_node[firstname_prop_id]);
  new_result->rows_.back().values_.emplace_back(*person_node[lastname_prop_id]);
  new_result->rows_.back().values_.emplace_back(*person_node[birthday_prop_id]);
  new_result->rows_.back().values_.emplace_back(*person_node[locationip_prop_id]);
  new_result->rows_.back().values_.emplace_back(*person_node[browserused_prop_id]);
  Node city_node(person_node[person_place_prop_id]->toLLong(), kvstore, txn);
  new_result->rows_.back().values_.emplace_back(*city_node[id_prop_id]);
  new_result->rows_.back().values_.emplace_back(*person_node[gender_prop_id]);
  new_result->rows_.back().values_.emplace_back(*person_node[creationdate_prop_id]);
}

/**
 * @brief Stored procedure for the IS2 query ("Recent messages of a person") of LDBC-SNB Interactive workload.
 * This Cypher query retrieves the latest 10 messages created by a specific person, along with additional information 
 * about the related posts and creators of those posts. It orders the messages by creation date in descending order 
 * and message id in ascending order. Then, it finds posts that are replies to these messages, retrieves details 
 * about the posts and their creators, and returns specific information about the messages, posts, and creators.
 * The final result is sorted by message creation date in descending order and message id in ascending order.
 * @param args The arguments of the stored procedure
 * @param new_result The result of the stored procedure
 * @param kvstore Pointer to the KVStore instance
 * @param db Pointer to the Database instance
 * @param txn Pointer to the Transaction instance
 * @remark IS2存储过程
 */
void PProcedure::is2(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
#ifdef DEBUG_PG
    auto start = std::chrono::steady_clock::now();
#endif

    constexpr size_t limit_messages = 10;

    unsigned int node_list_len = 0, xwidth = 0;
    GPStore::int_64 *x_list = nullptr;
    TYPE_ENTITY_LITERAL_ID* node_list = nullptr;

    unsigned int label_list_len = 0;
    TYPE_LABEL_ID *label_list = nullptr;

    Node person(person_label_id, id_prop_id, &args[0], kvstore, txn);
    if (person.node_id_ == -1)
        return;

    std::set<std::pair<GPStore::int_64, TYPE_ENTITY_LITERAL_ID>> candidates;
    if(kvstore->getsubIDxvaluesByobjIDpreID(person.node_id_, post_has_creator_pre_id, node_list, x_list, xwidth, node_list_len, false, nullptr) && node_list_len != 0){
        for(unsigned i = 0; i < node_list_len; ++i){
            GPStore::int_64 creation_date = x_list[i*xwidth];
            TYPE_ENTITY_LITERAL_ID post_vid = node_list[i];
            if(candidates.size() < limit_messages || creation_date > candidates.begin()->first){
                candidates.emplace(creation_date, post_vid);
                if(candidates.size() > limit_messages){
                    candidates.erase(candidates.begin());
                }
            }
        }
        delete[] node_list;
        delete[] x_list;
    }

    if(kvstore->getsubIDxvaluesByobjIDpreID(person.node_id_, comment_has_creator_pre_id, node_list, x_list, xwidth, node_list_len, false, nullptr) && node_list_len != 0){
        for(unsigned i = 0; i < node_list_len; ++i){
            GPStore::int_64 creation_date = x_list[i*xwidth];
            TYPE_ENTITY_LITERAL_ID comment_vid = node_list[i];
            if(candidates.size() < limit_messages || creation_date > candidates.begin()->first){
                candidates.emplace(creation_date, comment_vid);
                if(candidates.size() > limit_messages){
                    candidates.erase(candidates.begin());
                }
            }
        }
        delete[] node_list;
        delete[] x_list;
    }
 

    for(auto it = candidates.rbegin(); it != candidates.rend(); ++it){
        TYPE_ENTITY_LITERAL_ID message_vid = it->second;
        std::unique_ptr<Node> message_ptr(new Node(message_vid, kvstore, txn));
        auto &message = *message_ptr;
        GPStore::Value message_id_val = *message[id_prop_id], message_content_val;
        if(!message[imagefile_prop_id]->isNull()){
            message_content_val = *message[imagefile_prop_id];
        }
        else {
            message_content_val = *message[content_prop_id];
        }
        kvstore->getlabelIDlistBynodeID(message_vid, label_list, label_list_len, txn);

        bool is_comment = false;
        for(unsigned j = 0; j < label_list_len; ++j){
            is_comment |= label_list[j] == comment_label_id;
        }
        delete []label_list;

        if(is_comment){
            while(true){
                message_ptr->GetLinkedNodes(reply_of_comment_pre_id, node_list, node_list_len, Util::EDGE_OUT);
                if(node_list_len == 0){
                    break;
                }
                message_ptr = std::make_unique<Node>(node_list[0], kvstore, txn);
                delete []node_list;
            }

            message_ptr->GetLinkedNodes(reply_of_post_pre_id, node_list, node_list_len, Util::EDGE_OUT);
            if (!node_list)
                continue;
            message_ptr = std::make_unique<Node>(node_list[0], kvstore, txn);
            delete []node_list;

            auto &post = *message_ptr;

            post.GetLinkedNodes(post_has_creator_pre_id, node_list, node_list_len, Util::EDGE_OUT);
            if (!node_list)
                continue;
            // Insert new result for real
            new_result->rows_.emplace_back();
            new_result->rows_.reserve(7);
            new_result->rows_.back().values_.emplace_back(message_id_val);
            new_result->rows_.back().values_.emplace_back(message_content_val);
            new_result->rows_.back().values_.emplace_back(it->first);
            new_result->rows_.back().values_.emplace_back(*post[id_prop_id]);
            TYPE_ENTITY_LITERAL_ID author_vid = node_list[0];
            delete []node_list;
            Node author(author_vid, kvstore, txn);
            new_result->rows_.back().values_.emplace_back(*author[id_prop_id]);
            new_result->rows_.back().values_.emplace_back(*author[firstname_prop_id]);
            new_result->rows_.back().values_.emplace_back(*author[lastname_prop_id]);
        }
        else /* POST */{
            message.GetLinkedNodes(post_has_creator_pre_id, node_list, node_list_len, Util::EDGE_OUT);
            if (!node_list)
                continue;
            // Insert new result for real
            new_result->rows_.emplace_back();
            new_result->rows_.reserve(7);
            new_result->rows_.back().values_.emplace_back(message_id_val);
            new_result->rows_.back().values_.emplace_back(message_content_val);
            new_result->rows_.back().values_.emplace_back(it->first);
            new_result->rows_.back().values_.emplace_back(message_id_val);
            TYPE_ENTITY_LITERAL_ID author_vid = node_list[0];
            delete []node_list;
            Node author(author_vid, kvstore, txn);
            new_result->rows_.back().values_.emplace_back(*author[id_prop_id]);
            new_result->rows_.back().values_.emplace_back(*author[firstname_prop_id]);
            new_result->rows_.back().values_.emplace_back(*author[lastname_prop_id]);
        }
    }
#ifdef DEBUG_PG
    auto total_cost = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start);
    std::cout << "[GPStore IS2] " << total_cost.count() << " us." << std::endl;
#endif
}


/**
 * @brief Stored procedure for the IS2 query ("Friends of a person") of LDBC-SNB Interactive workload.
 * Given a start Person with ID $personId, retrieve all of their friends, and the date at which they became friends.
 * @param args The arguments of the stored procedure
 * @param new_result The result of the stored procedure
 * @param kvstore Pointer to the KVStore instance
 * @param db Pointer to the Database instance
 * @param txn Pointer to the Transaction instance
 * @remark IS3存储过程
 */
void PProcedure::is3(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
#ifdef DEBUG_PG
    auto start = std::chrono::steady_clock::now();
#endif
    Node person(person_label_id, id_prop_id, &args[0], kvstore, txn);
    if (person.node_id_ == -1)
        return;
    TYPE_ENTITY_LITERAL_ID person_vid = person.node_id_;

    TYPE_ENTITY_LITERAL_ID *friends = nullptr;
    GPStore::int_64 *x_list = nullptr;
    unsigned friends_num = 0, xwidth = 0;

    using result_type = std::tuple<GPStore::int_64, GPStore::int_64, std::string, std::string>;
    std::vector<result_type> candidates;

    if(kvstore->getsubIDxvaluesByobjIDpreID(person_vid, knows_pre_id, friends, x_list, xwidth, friends_num, false, nullptr) && friends_num != 0){
        for(unsigned i = 0; i < friends_num; ++i){
            auto friend_vid = friends[i];
            GPStore::int_64 creation_date = x_list[i * xwidth];
            Node friend_i(friend_vid, kvstore, txn);
            candidates.emplace_back(
                0LL - creation_date,
                friend_i[id_prop_id]->data_.Long,
                *(friend_i[firstname_prop_id]->data_.String),
                *(friend_i[lastname_prop_id]->data_.String)
            );
        }
        delete[] friends;
        delete[] x_list;
    }

    if(kvstore->getobjIDxvaluesBysubIDpreID(person_vid, knows_pre_id, friends, x_list, xwidth, friends_num, false, nullptr) && friends_num != 0){
        for(unsigned i = 0; i < friends_num; ++i){
            auto friend_vid = friends[i];
            GPStore::int_64 creation_date = x_list[i * xwidth];
            Node friend_i(friend_vid, kvstore, txn);
            candidates.emplace_back(
                0LL - creation_date,
                friend_i[id_prop_id]->data_.Long,
                *(friend_i[firstname_prop_id]->data_.String),
                *(friend_i[lastname_prop_id]->data_.String)
            );
        }
        delete[] friends;
        delete[] x_list;
    }

    std::sort(candidates.begin(), candidates.end());
    new_result->rows_.reserve(candidates.size());

    for(auto &tup : candidates){
        new_result->rows_.emplace_back();
        new_result->rows_.back().values_.reserve(4);
        new_result->rows_.back().values_.emplace_back(GPStore::int_64(std::get<1>(tup)));
        new_result->rows_.back().values_.emplace_back(std::get<2>(tup));
        new_result->rows_.back().values_.emplace_back(std::get<3>(tup));
        new_result->rows_.back().values_.emplace_back(GPStore::int_64(0 - std::get<0>(tup)));
    }
#ifdef DEBUG_PG
    auto total_cost = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start);
    std::cout << "[GPStore IS3] " << total_cost.count() << " us." << std::endl;
#endif
}


void PProcedure::is4(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
    Node msg_node(message_label_id, id_prop_id, &args[0], kvstore, txn);
    if (msg_node.node_id_ == -1)
        return;
    // printf("%d\n",msg_node.node_id_);
    new_result->rows_.emplace_back();
    new_result->rows_.back().values_.reserve(2);

    new_result->rows_.back().values_.emplace_back(*msg_node[creationdate_prop_id]);
    GPStore::Value *content_ptr = msg_node[content_prop_id];
    if(!content_ptr->isNull()){
        new_result->rows_.back().values_.emplace_back(*content_ptr);
    } else {
        new_result->rows_.back().values_.emplace_back(*msg_node[imagefile_prop_id]);
    }

}

/**
 * @brief Stored procedure for the IS5 query ("Creator of a message") of LDBC-SNB Interactive workload.
 * Given a Message with ID $messageId, retrieve its author.
 * 
 * @param args The arguments of the stored procedure
 * @param new_result The result of the stored procedure
 * @param kvstore Pointer to the KVStore instance
 * @param db Pointer to the Database instance
 * @param txn Pointer to the Transaction instance
 * @remark IS5存储过程
 */
void PProcedure::is5(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {

#ifdef DEBUG_PG
    std::chrono::microseconds t_count{0},all{0};
    auto start = std::chrono::steady_clock::now();
#endif
    Node msg_node(message_label_id, id_prop_id, &args[0], kvstore, txn);
    if (msg_node.node_id_ == -1)
        return;
    new_result->rows_.emplace_back();
    new_result->rows_.back().values_.reserve(3);
    unsigned *creator_list = nullptr; unsigned creator_list_len = 0;
//  Assumption : the place where person "IS_LOCATED_IN" is a "city", so there is no need to check the node label
//  Assumption : a person only lives in one city
#ifdef DEBUG_PG
    auto t1 = std::chrono::steady_clock::now();
#endif
//    msg_node.GetLinkedNodes("HAS_CREATOR", creator_list, creator_list_len, Util::EDGE_OUT);
//    cout<<"yxy: "<<creator_list_len<<endl;
    msg_node.GetLinkedNodes(post_has_creator_pre_id, creator_list, creator_list_len, Util::EDGE_OUT);
//    cout<<"yxy: "<<creator_list_len<<endl;
    if(creator_list_len == 0)
    {
        msg_node.GetLinkedNodes(comment_has_creator_pre_id, creator_list, creator_list_len, Util::EDGE_OUT);
//        cout<<"yxy: "<<creator_list_len<<endl;
    }

#ifdef DEBUG_PG
    t_count = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - t1);
#endif
    Node creator_node(creator_list[0], kvstore, txn);
    new_result->rows_.back().values_.emplace_back(*creator_node[id_prop_id]);
    new_result->rows_.back().values_.emplace_back(*creator_node[firstname_prop_id]);
    new_result->rows_.back().values_.emplace_back(*creator_node[lastname_prop_id]);
    delete[] creator_list;
#ifdef DEBUG_PG
    all = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start);

    cout<<"time: get edge; all: "<<t_count.count()<<' '<<all.count()<<endl;
#endif
}

/**
 * @brief Stored procedure for the IS6 query ("Forum of a message") of LDBC-SNB
 * Interactive workload. Given a Message with ID $messageId, retrieve the Forum
 * that contains it and the Person that moderates that Forum. Since Comments are
 * not directly contained in Forums, for Comments, return the Forum containing
 * the original Post in the thread which the Comment is replying to.
 * @param args The arguments of the stored procedure
 * @param new_result The result of the stored procedure
 * @param kvstore Pointer to the KVStore instance
 * @param db Pointer to the Database instance
 * @param txn Pointer to the Transaction instance
 * @remark IS6存储过程
 */
void PProcedure::is6(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
    Node msg_node(message_label_id, id_prop_id, &args[0], kvstore, txn);
    if (msg_node.node_id_ == -1)
        return;
    for(;;) {
        unsigned *reply_comment_list = nullptr, reply_comment_list_len = 0;
        msg_node.GetLinkedNodes(reply_of_comment_pre_id, reply_comment_list, reply_comment_list_len, Util::EDGE_OUT);
        if (reply_comment_list_len == 1){
            Node new_node = Node(reply_comment_list[0], kvstore, txn);
            msg_node = new_node;
        }
        else if (reply_comment_list_len == 0) {
            unsigned *reply_post_list = nullptr, reply_post_list_len = 0;
            msg_node.GetLinkedNodes(reply_of_post_pre_id, reply_post_list, reply_post_list_len, Util::EDGE_OUT);
            if (reply_post_list_len == 1) {
                Node new_node = Node(reply_post_list[0], kvstore, txn);
                msg_node = new_node;
                delete [] reply_post_list;
                break;
            }
            else if (reply_post_list_len == 0) {
                break;
            }
            else {
                fprintf(stderr, "error in is6: reply_post_list_len = %d, comment_id = %llu", reply_post_list_len, msg_node[id_prop_id]->toLLong());
                exit(-1);
            }
        }
        else {
            fprintf(stderr, "error in is6: reply_comment_list_len = %d\n", reply_comment_list_len);
            exit(-1);
        }
        delete [] reply_comment_list;
    }
    auto & post_node = msg_node;
    unsigned *forum_list = nullptr, forum_list_len = 0;
    post_node.GetLinkedNodes(container_of_pre_id, forum_list, forum_list_len, Util::EDGE_IN);
    for (unsigned i = 0; i < forum_list_len; ++i) {
        Node forum_node = Node(forum_list[i], kvstore, txn);
        unsigned *moderator_list = nullptr, moderator_list_len;
        forum_node.GetLinkedNodes(has_moderator_pre_id, moderator_list, moderator_list_len, Util::EDGE_OUT);
        for (unsigned j = 0; j < moderator_list_len; ++j) {
            Node moderator_node = Node(moderator_list[j], kvstore, txn);
            new_result->rows_.emplace_back();
            new_result->rows_.back().values_.emplace_back(forum_node[id_prop_id]);
            new_result->rows_.back().values_.emplace_back(forum_node[title_prop_id]);
            new_result->rows_.back().values_.emplace_back(moderator_node[id_prop_id]);
            new_result->rows_.back().values_.emplace_back(moderator_node[firstname_prop_id]);
            new_result->rows_.back().values_.emplace_back(moderator_node[lastname_prop_id]);
        }
        delete [] moderator_list;
    }
    delete [] forum_list;
}
/**
 * @brief Stored procedure for the IS7 query ("Replies of a message") of LDBC-SNB Interactive workload.
 * Starting from a given message, get its creator and their friends, then get its reply comments and their creators and check if these creators are in those friends.
 * 
 * @param args The arguments of the stored procedure
 * @param new_result The result of the stored procedure
 * @param kvstore Pointer to the KVStore instance
 * @param db Pointer to the Database instance
 * @param txn Pointer to the Transaction instance
 * @remark IS7存储过程
 */
void PProcedure::is7(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
    // Parse args
    const auto &startMessageId = args[0];
    #ifdef DEBUG_PG
    auto start_time = std::chrono::steady_clock::now();
    #endif
    Node messageNode = Node(message_label_id, id_prop_id, &args[0], kvstore, txn);
    if (messageNode.node_id_ == -1) {
        cout << "Cannot find node" << endl;
        return;
    }
    #ifdef DEBUG_PG
    auto end_time = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
    std::cout << "[GPSTORE]find creator: " << elapsed.count() << " us" << std::endl;
    #endif

    // Find message creator and their friends
    #ifdef DEBUG_PG
    start_time = std::chrono::steady_clock::now();
    #endif
    tsl::hopscotch_set<int64_t> message_creator_friends;
    TYPE_ENTITY_LITERAL_ID *node_list = nullptr; unsigned node_list_len = 0;
    bool srcIsComment = false;
    messageNode.GetLinkedNodes(post_has_creator_pre_id, node_list, node_list_len, Util::EDGE_OUT);
    if (node_list_len == 0) {
        messageNode.GetLinkedNodes(comment_has_creator_pre_id, node_list, node_list_len, Util::EDGE_OUT);
        srcIsComment = true;
    }
    Node creatorNode(node_list[0], kvstore, txn);
    delete []node_list;
    creatorNode.GetLinkedNodes(knows_pre_id, node_list, node_list_len, Util::EDGE_IN);
    for(int i = 0; i < node_list_len; ++i)
        message_creator_friends.emplace(node_list[i]);
    delete []node_list;
    creatorNode.GetLinkedNodes(knows_pre_id, node_list, node_list_len, Util::EDGE_OUT);
    for(int i = 0; i < node_list_len; ++i)
        message_creator_friends.emplace(node_list[i]);
    delete []node_list;
    #ifdef DEBUG_PG
    end_time = std::chrono::steady_clock::now();
    elapsed = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
    std::cout << "[GPSTORE]find friends: " << elapsed.count() << " us" << std::endl;
    #endif

    // Find message's reply comments and creator, check if in friends
    #ifdef DEBUG_PG
    start_time = std::chrono::steady_clock::now();
    #endif
    std::vector<std::tuple<int64_t, int64_t, int64_t, std::string, std::string, std::string, bool> > comments;
    TYPE_PREDICATE_ID pre_reply_of_id = reply_of_post_pre_id;
    if (srcIsComment) pre_reply_of_id = reply_of_comment_pre_id;
    long long *x_list = nullptr; unsigned xwidth = 0;
    kvstore->getsubIDxvaluesByobjIDpreID(messageNode.node_id_, pre_reply_of_id, node_list, x_list, xwidth, node_list_len);
    for(int i = 0; i < node_list_len; ++i) {
        Node commentNode(node_list[i], kvstore, txn);
     TYPE_ENTITY_LITERAL_ID *node_list_i = nullptr; unsigned node_list_len_i = 0;
        commentNode.GetLinkedNodes(comment_has_creator_pre_id, node_list_i, node_list_len_i, Util::EDGE_OUT);  // creator_list_len == 1
        Node curCreatorNode(node_list_i[0], kvstore, txn);
        bool knows = message_creator_friends.find(curCreatorNode.node_id_) != message_creator_friends.end();
        comments.emplace_back(0 - x_list[i], curCreatorNode[id_prop_id]->data_.Long, commentNode[id_prop_id]->data_.Long, 
            *(commentNode[content_prop_id]->data_.String), *(curCreatorNode[firstname_prop_id]->data_.String), *(curCreatorNode[lastname_prop_id]->data_.String), knows);
        delete []node_list_i;
    }
    delete []node_list;
    delete []x_list;
    #ifdef DEBUG_PG
    end_time = std::chrono::steady_clock::now();
    elapsed = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
    std::cout << "[GPSTORE]check if friend: " << elapsed.count() << " us" << std::endl;
    #endif
    #ifdef DEBUG_PG
    start_time = std::chrono::steady_clock::now();
    #endif
    std::sort(comments.begin(), comments.end());
    #ifdef DEBUG_PG
    end_time = std::chrono::steady_clock::now();
    elapsed = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
    std::cout << "[GPSTORE]order: " << elapsed.count() << " us" << std::endl;
    #endif

    // Write new_result
    #ifdef DEBUG_PG
    start_time = std::chrono::steady_clock::now();
    #endif
    for (const auto &tup : comments) {
        new_result->rows_.emplace_back();
        new_result->rows_.back().values_.reserve(7);
        new_result->rows_.back().values_.emplace_back(GPStore::int_64(std::get<2>(tup)));
        new_result->rows_.back().values_.emplace_back(std::get<3>(tup));
        new_result->rows_.back().values_.emplace_back(GPStore::int_64(0 - std::get<0>(tup)));
        new_result->rows_.back().values_.emplace_back(GPStore::int_64(std::get<1>(tup)));
        new_result->rows_.back().values_.emplace_back(std::get<4>(tup));
        new_result->rows_.back().values_.emplace_back(std::get<5>(tup));
        new_result->rows_.back().values_.emplace_back(std::get<6>(tup));
    }
    #ifdef DEBUG_PG
    end_time = std::chrono::steady_clock::now();
    elapsed = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
    std::cout << "[GPSTORE][write_result]: " << elapsed.count() << " us" << std::endl;
    #endif
}

/**
 * @brief Stored procedure for the IU1 query ("Add person") of LDBC-SNB
 * Interactive workload. Add a Person node, connected to the network by 4
 * possible edge types.
 * @param args The arguments of the stored procedure
 * @param new_result The result of the stored procedure
 * @param kvstore Pointer to the KVStore instance
 * @param db Pointer to the Database instance
 * @param txn Pointer to the Transaction instance
 * @remark IU1存储过程
 */
void PProcedure::iu1(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn){
    auto person_id = args[0];
    auto person_first_name = args[1];
    auto person_last_name = args[2];
    auto gender = args[3];
    auto birthday = args[4];
    auto creation_date = args[5];
    auto location_ip = args[6];
    auto browser_used = args[7];
    auto city_id = args[8];
    auto languanges = args[9];
    auto emails = args[10];
    auto tag_ids = args[11];
    auto study_at = args[12];
    auto work_at = args[13];


    // insert new node
    unsigned node_id;
    db->AddNode({person_label_id}, 
                {
                    id_prop_id,
                    firstname_prop_id,
                    lastname_prop_id,
                    gender_prop_id,
                    birthday_prop_id,
                    creationdate_prop_id,
                    locationip_prop_id,
                    browserused_prop_id,
                    speaks_prop_id,
                    email_prop_id,
                }, 
                {
                    person_id,
                    person_first_name,
                    person_last_name,
                    gender,
                    birthday,
                    creation_date,
                    location_ip,
                    browser_used,
                    languanges,
                    emails
                }, node_id, txn);

    // insert person-city edge
    Node city_node(city_label_id, id_prop_id, &city_id, kvstore, txn);
    assert(city_node.node_id_ != -1);
    db->AddEdge(node_id, city_node.node_id_, person_is_located_in_pre_id, {}, txn);

    // insert person-tag edge
    assert(tag_ids.type_ == GPStore::Value::LIST);
    for (auto &tag_id : *tag_ids.data_.List) {
        Node tag_node(tag_label_id, id_prop_id, tag_id, kvstore, txn);
        assert(tag_node.node_id_ != -1);
        db->AddEdge(node_id, tag_node.node_id_, has_interest_pre_id, {}, txn);
    }

    // insert person-study_at edge
    assert(study_at.type_ == GPStore::Value::LIST);
    for (auto &it : *study_at.data_.List) {
        auto &univ_id = it->data_.List->at(0);
        auto &class_year = it->data_.List->at(1)->data_.Int;
        Node univ_node(university_label_id, id_prop_id, univ_id, kvstore, txn);
        assert(univ_node.node_id_ != -1);
        db->AddEdge(node_id, univ_node.node_id_, study_at_pre_id, {class_year}, txn);
    }

    // insert person-work_at edge
    assert(work_at.type_ == GPStore::Value::LIST);
    for (auto &it : *work_at.data_.List) {
        auto &comp_id = it->data_.List->at(0);
        auto &work_from = it->data_.List->at(1)->data_.Int;
        Node comp_node(company_label_id, id_prop_id, comp_id, kvstore, txn);
        assert(comp_node.node_id_ != -1);
        db->AddEdge(node_id, comp_node.node_id_, work_at_pre_id, {work_from}, txn);
    }
}

/**
 * @brief Stored procedure for the IU2 query ("Add like to post") of LDBC-SNB
 * Interactive workload. Add a likes edge to a Post.
 * @param args The arguments of the stored procedure
 * @param new_result The result of the stored procedure
 * @param kvstore Pointer to the KVStore instance
 * @param db Pointer to the Database instance
 * @param txn Pointer to the Transaction instance
 * @remark IU2存储过程
 */
void PProcedure::iu2(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn){
    auto& person_id = args[0];
    auto& post_id = args[1];
    auto& creation_date = args[2];

    // insert edge between person and post
    Node person_node(person_label_id, id_prop_id, &person_id, kvstore, txn);
    Node post_node(post_label_id, id_prop_id, &post_id, kvstore, txn);
    db->AddEdge(person_node.node_id_, post_node.node_id_, post_has_creator_pre_id, {creation_date.data_.Long}, txn);
}

/**
 * @brief Stored procedure for the IU3 query ("Add like to comment") of LDBC-SNB
 * Interactive workload. Add a likes edge to a Comment.
 * @param args The arguments of the stored procedure
 * @param new_result The result of the stored procedure
 * @param kvstore Pointer to the KVStore instance
 * @param db Pointer to the Database instance
 * @param txn Pointer to the Transaction instance
 * @remark IU3存储过程
 */
void PProcedure::iu3(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn){
    auto& person_id = args[0];
    auto& comment_id = args[1];
    auto& creation_date = args[2];

    // insert edge between person and comment
    Node person_node(person_label_id, id_prop_id, &person_id, kvstore, txn);
    Node comment_node(comment_label_id, id_prop_id, &comment_id, kvstore, txn);
    db->AddEdge(person_node.node_id_, comment_node.node_id_, comment_has_creator_pre_id, {creation_date.data_.Long}, txn);
}


/**
 * @brief Stored procedure for the IU4 query ("Add forum") of LDBC-SNB
 * Add a Forum node, connected to the network by 2 possible edge types.
 * @param args The arguments of the stored procedure
 * @param new_result The result of the stored procedure
 * @param kvstore Pointer to the KVStore instance
 * @param db Pointer to the Database instance
 * @param txn Pointer to the Transaction instance
 * @remark IU4存储过程
 */
void PProcedure::iu4(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn){
//    throw std::runtime_error("IU4 not implemented");

    auto forum_id = args[0];
    auto forum_title = args[1];
    auto creation_date = args[2];
    auto person_id = args[3];
    Node person_node(person_label_id, id_prop_id, &person_id, kvstore, txn);
    int person_vid = person_node.node_id_;
    assert(person_vid != -1);

    unsigned forum_node_id = 0;
    db->AddNode({forum_label_id},
                {
                    id_prop_id,
                    title_prop_id,
                    creationdate_prop_id,
                    // moderatorid_prop_id
                },
                {
                        forum_id,
                        forum_title,
                        creation_date,
                        // person_vid,
                }, forum_node_id, txn);

//    cout<<"forum node id: "<<forum_node_id<<endl;

    db->AddEdge(forum_node_id, person_vid, has_moderator_pre_id, {}, txn);

    // insert person-tag edge
    auto  tag_ids = args[4];
    assert(tag_ids.type_ == GPStore::Value::LIST);
    for (auto &tag_id : *tag_ids.data_.List) {
        Node tag_node(tag_label_id, id_prop_id, tag_id, kvstore, txn);
        assert(tag_node.node_id_ != -1);
        db->AddEdge(forum_node_id, tag_node.node_id_, forum_has_tag_pre_id, {}, txn);
    }
}

/**
 * @brief Stored procedure for the IU5 query ("Add forum membership") of LDBC-SNB
 * Add a Forum membership edge (hasMember) to a Person.
 * @param args The arguments of the stored procedure
 * @param new_result The result of the stored procedure
 * @param kvstore Pointer to the KVStore instance
 * @param db Pointer to the Database instance
 * @param txn Pointer to the Transaction instance
 * @remark IU5存储过程
 */
void PProcedure::iu5(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn){
//    throw std::runtime_error("IU5 not implemented");

    auto& person_id = args[0];
    auto& forum_id = args[1];
    auto& creation_date = args[2];

    // insert edge between person and post
    Node person_node(person_label_id, id_prop_id, &person_id, kvstore, txn);
    Node forum_node(forum_label_id, id_prop_id, &forum_id, kvstore, txn);
    assert(person_node.node_id_ != -1);
    assert(forum_node.node_id_ != -1);
    int num_posts = 0;
    vector<GPStore::uint_32> postVec;
    TYPE_ENTITY_LITERAL_ID* neighborList = nullptr;
    unsigned len = 0;
    person_node.GetLinkedNodes(post_has_creator_pre_id, neighborList, len, Util::EDGE_IN);
    postVec.assign(neighborList, neighborList + len);
    delete[] neighborList;

    for (auto post_vid: postVec) {

        Node post_node(post_vid, kvstore,txn);
        assert(post_node.node_id_ != -1);
        auto container_vid = post_node[post_container_prop_id]->data_.Long;
        if (container_vid == forum_node.node_id_) num_posts++;
    }

    db->AddEdge(forum_node.node_id_, person_node.node_id_, has_member_pre_id, {creation_date.data_.Long,num_posts,forum_node[id_prop_id]->data_.Long }, txn);

}

/**
 * @brief Stored procedure for the IU6 query ("Add post") of LDBC-SNB
 * Add a Post node connected to the network by 4 possible edge types (hasCreator, containerOf, isLocatedIn, hasTag)
 * @param args The arguments of the stored procedure
 * @param new_result The result of the stored procedure
 * @param kvstore Pointer to the KVStore instance
 * @param db Pointer to the Database instance
 * @param txn Pointer to the Transaction instance
 * @remark IU6存储过程
 */
void PProcedure::iu6(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn){
//    throw std::runtime_error("IU6 not implemented");

    auto post_id = args[0];
    auto image_file = args[1];
    auto creation_date = args[2];
    auto location_ip = args[3];
    auto browser_used = args[4];
    auto language = args[5];
    auto content = args[6];
    auto length = args[7];
    auto person_id = args[8];
    auto forum_id = args[9];
    auto country_id = args[10];
    auto tag_ids = args[11];

    Node person_node(person_label_id, id_prop_id, &person_id, kvstore, txn);
    int person_vid = person_node.node_id_;
    assert(person_vid != -1);

    Node forum_node(forum_label_id, id_prop_id, &forum_id, kvstore, txn);
    int forum_vid = forum_node.node_id_;
    assert(forum_vid != -1);

    Node country_node(country_label_id, id_prop_id, &country_id, kvstore, txn);
    int place_vid = country_node.node_id_;
    assert(place_vid != -1);

    unsigned post_node_id;
    db->AddNode({message_label_id, post_label_id},
                {
                    id_prop_id,
                    creationdate_prop_id,
                    locationip_prop_id,
                    browserused_prop_id,
                    language_prop_id,
                    length_prop_id,
                    post_creator_prop_id,
                    post_container_prop_id,
                    // POST_PLACE not used in gpstore, but used in tugraph.
                    imagefile_prop_id,
                    content_prop_id
                },
                {
                        post_id,
                        creation_date,
                        location_ip,
                        browser_used,
                        language,
                        length,
                        person_vid,
                        forum_vid,
                        // place_vid,
                        image_file,
                        content,
                }, post_node_id, txn);

    db->AddEdge(post_node_id, person_vid, post_has_creator_pre_id, {creation_date.data_.Long}, txn);
    db->AddEdge(post_node_id, place_vid, post_is_located_in_pre_id, {creation_date.data_.Long}, txn);
    db->AddEdge(forum_vid, post_node_id, container_of_pre_id, {}, txn);

    assert(tag_ids.type_ == GPStore::Value::LIST);
    for (auto &tag_id : *tag_ids.data_.List) {
        Node tag_node(tag_label_id, id_prop_id, tag_id, kvstore, txn);
        assert(tag_node.node_id_ != -1);
        db->AddEdge(post_node_id, tag_node.node_id_, post_has_tag_pre_id, {}, txn);
    }

    unsigned* obj_id_list = nullptr;
    long long* prop_list = nullptr;
    unsigned prop_width = 0;
    unsigned obj_num = 0;
    kvstore->getobjIDxvaluesBysubIDpreID(forum_vid, has_member_pre_id, obj_id_list, prop_list, prop_width, obj_num);
    int num_posts = -1;
    for(int i = 0;i<obj_num;i++)
    {
        if(obj_id_list[i] == person_vid)
        {
            num_posts = prop_list[i*prop_width+1];
        }
    }
    if(num_posts != -1)
    {
        db->kvstore->update_xvalues(forum_vid, has_member_pre_id, person_vid, 1/* post count's edge prop offset: 1. */, num_posts+1, txn);
    }

    /// this 2 lines: in tugraph, but i think it is useless.
//    auto person = txn.GetVertexIterator(person_vid);
//    person.SetField(PERSON_CREATIONDATE, person[PERSON_CREATIONDATE]);

}

/**
 * @brief Stored procedure for the IU7 query ("add comment") of LDBC-SNB Interactive workload.
 * Add a Comment node replying to a Post/Comment, connected to the network by 4 possible edge types (replyOf, hasCreator, isLocatedIn, hasTag).
 * @param args The arguments of the stored procedure
 * @param new_result The result of the stored procedure
 * @param kvstore Pointer to the KVStore instance
 * @param db Pointer to the Database instance
 * @param txn Pointer to the Transaction instance
 * @remark IU7存储过程
 */
void PProcedure::iu7(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn){
    auto &commentid = args[0];
    auto &creationdate = args[1];
    auto &ip_addr = args[2];
    auto &browser = args[3];
    auto &content = args[4];
    auto &length = args[5];
    auto &authorpersonid = args[6];
    auto &countryid = args[7];
    auto &replytopostid = args[8];
    auto &replytocommentid = args[9];
    auto &tagids = args[10];

    Node person_node(person_label_id, id_prop_id, &authorpersonid, kvstore, txn);
    assert(person_node.node_id_ != -1);
    Node post_node;
    Node comment_node;
    if (replytopostid.data_.Long != -1) {
        post_node = Node(post_label_id, id_prop_id, &replytopostid, kvstore, txn);
        if (post_node.node_id_ == -1) {
          return;
        }
    }
    if (replytocommentid.data_.Long != -1) {
        comment_node = Node(comment_label_id, id_prop_id, &replytocommentid, kvstore, txn);
        if (comment_node.node_id_ == -1) {
          return;
        }
    }

    unsigned node_id;
    db->AddNode({message_label_id, comment_label_id}, {
                        id_prop_id, creationdate_prop_id, locationip_prop_id, browserused_prop_id, content_prop_id,
                        length_prop_id, comment_creator_prop_id,
                        (replytopostid.data_.Long != -1 ? comment_replyofpost_prop_id : comment_replyofcomment_prop_id)},
                {
                        commentid,
                        creationdate,
                        ip_addr,
                        browser,
                        content,
                        length,
                        (GPStore::int_64)person_node.node_id_,
                        (replytopostid.data_.Long != -1 ? (GPStore::int_64)post_node.node_id_ : (GPStore::int_64)comment_node.node_id_)
                        }, node_id, txn);

    db->AddEdge(node_id, person_node.node_id_, comment_has_creator_pre_id,
                {creationdate.data_.Long}, txn);

    Node country_node(country_label_id, id_prop_id, &countryid, kvstore, txn);
    assert(country_node.node_id_ != -1);

    db->AddEdge(node_id, country_node.node_id_, comment_is_located_in_pre_id,
                {creationdate.data_.Long}, txn);

    if (replytopostid.data_.Long != -1) {
        db->AddEdge(node_id, post_node.node_id_, reply_of_post_pre_id,
                    {creationdate.data_.Long}, txn);
        unsigned* knows_person_ids = nullptr; unsigned knows_person_num;
        unsigned post_creator_person_id = post_node[post_creator_prop_id]->toLLong();
        person_node.GetLinkedNodes(knows_pre_id, knows_person_ids, knows_person_num, Util::EDGE_OUT);
        if (find(knows_person_ids, knows_person_ids+knows_person_num, post_creator_person_id) != knows_person_ids+knows_person_num) {
            Node person2_node(post_creator_person_id, kvstore, txn);
            unordered_map<GPStore::uint_32, vector<GPStore::uint_32>> postCache, commentCache;
            unordered_map<GPStore::uint_32, unordered_map<GPStore::uint_32, unsigned>> commReplyOfCache;
            double know_weight = GetIC14KnowsWeight(kvstore, person_node.node_id_, person2_node.node_id_, postCache, commentCache, commReplyOfCache);
            long long know_weight_ll = 0;
            memcpy(&know_weight_ll, &know_weight, sizeof(double));
            db->kvstore->modify_spox_iv(person_node.node_id_, knows_pre_id, post_creator_person_id, 1, know_weight_ll, txn);
        }
        delete[] knows_person_ids;

        person_node.GetLinkedNodes(knows_pre_id, knows_person_ids, knows_person_num, Util::EDGE_IN);
        if (find(knows_person_ids, knows_person_ids+knows_person_num, post_creator_person_id) != knows_person_ids+knows_person_num) {
            Node person2_node(post_creator_person_id, kvstore, txn);
            unordered_map<GPStore::uint_32, vector<GPStore::uint_32>> postCache, commentCache;
            unordered_map<GPStore::uint_32, unordered_map<GPStore::uint_32, unsigned>> commReplyOfCache;
            double know_weight = GetIC14KnowsWeight(kvstore, person2_node.node_id_, person_node.node_id_, postCache, commentCache, commReplyOfCache);
            long long know_weight_ll = 0;
            memcpy(&know_weight_ll, &know_weight, sizeof(double));
            db->kvstore->modify_spox_iv(post_creator_person_id, knows_pre_id, person_node.node_id_, 1, know_weight_ll, txn);
        }
        delete[] knows_person_ids;
    }
    if (replytocommentid.data_.Long != -1) {
        db->AddEdge(node_id, comment_node.node_id_, reply_of_comment_pre_id,
                    {creationdate.data_.Long}, txn);
        unsigned* knows_person_ids = nullptr; unsigned knows_person_num;
        unsigned comment_creator_person_id = comment_node[comment_creator_prop_id]->toLLong();
        person_node.GetLinkedNodes(knows_pre_id, knows_person_ids, knows_person_num, Util::EDGE_OUT);
        if (find(knows_person_ids, knows_person_ids+knows_person_num, comment_creator_person_id) != knows_person_ids+knows_person_num) {
            Node person2_node(comment_creator_person_id, kvstore, txn);
            unordered_map<GPStore::uint_32, vector<GPStore::uint_32>> postCache, commentCache;
            unordered_map<GPStore::uint_32, unordered_map<GPStore::uint_32, unsigned>> commReplyOfCache;
            double know_weight = GetIC14KnowsWeight(kvstore, person_node.node_id_, person2_node.node_id_, postCache, commentCache, commReplyOfCache);
            long long know_weight_ll = 0;
            memcpy(&know_weight_ll, &know_weight, sizeof(double));
            db->kvstore->modify_spox_iv(person_node.node_id_, knows_pre_id, comment_creator_person_id, 1, know_weight_ll, txn);
        }
        delete[] knows_person_ids;

        person_node.GetLinkedNodes(knows_pre_id, knows_person_ids, knows_person_num, Util::EDGE_IN);
        if (find(knows_person_ids, knows_person_ids+knows_person_num, comment_creator_person_id) != knows_person_ids+knows_person_num) {
            Node person2_node(comment_creator_person_id, kvstore, txn);
            unordered_map<GPStore::uint_32, vector<GPStore::uint_32>> postCache, commentCache;
            unordered_map<GPStore::uint_32, unordered_map<GPStore::uint_32, unsigned>> commReplyOfCache;
            double know_weight = GetIC14KnowsWeight(kvstore, person2_node.node_id_, person_node.node_id_, postCache, commentCache, commReplyOfCache);
            long long know_weight_ll = 0;
            memcpy(&know_weight_ll, &know_weight, sizeof(double));
            db->kvstore->modify_spox_iv(comment_creator_person_id, knows_pre_id, person_node.node_id_, 1, know_weight_ll, txn);
        }
        delete[] knows_person_ids;
    }

    for (auto &tag_id : *tagids.data_.List) {
        Node tag_node(tag_label_id, id_prop_id, tag_id, kvstore, txn);
        assert(tag_node.node_id_ != -1);
        db->AddEdge(node_id, tag_node.node_id_, comment_has_tag_pre_id, {}, txn);
    }
}

/**
 * @brief A helper function for building a database, which calculates the weight of the "KNOWS" edge between two persons according to IC14's definition for materialization.
 * Get the posts and comments created by one person and count how many comments created by the other person reply to them. For each comment-post pair, add 1 to the weight; for each comment-comment pair, add 0.5 to the weight.
 * 
 * @param kvstore Pointer to the KVStore instance
 * @param person1_vid The vertex ID of the first person
 * @param person2_vid The vertex ID of the second person
 * @param postCache The mapping from person vertex ID to each person's posts
 * @param commentCache The mapping from person vertex ID to each person's comments
 * @param commReplyOfCache The mapping from person vertex ID to the posts / comments that each person's comments reply to
 * @return double The weight of the KNOWS edge between the two given persons
 */
double PProcedure::GetIC14KnowsWeight(const KVstore *kvstore, TYPE_ENTITY_LITERAL_ID person1_vid,
                                      TYPE_ENTITY_LITERAL_ID person2_vid,
                                      std::unordered_map<GPStore::uint_32, std::vector<GPStore::uint_32>> &postCache,
                                      std::unordered_map<GPStore::uint_32, std::vector<GPStore::uint_32>> &commentCache,
                                      std::unordered_map<GPStore::uint_32, std::unordered_map<GPStore::uint_32, unsigned>> &commReplyOfCache) {
    if (postCache.find(person1_vid) == postCache.end()) {
        getPostComments(person1_vid, postCache[person1_vid], commentCache[person1_vid], kvstore, nullptr);
        getCommReplyOf(commentCache[person1_vid], commReplyOfCache[person1_vid], kvstore, nullptr);
    }
    if (postCache.find(person2_vid) == postCache.end()) {
        getPostComments(person2_vid, postCache[person2_vid], commentCache[person2_vid], kvstore, nullptr);
        getCommReplyOf(commentCache[person2_vid], commReplyOfCache[person2_vid], kvstore, nullptr);
    }
    size_t comm2post = 0, comm2comm = 0;
    std::unordered_map<GPStore::uint_32, unsigned>::iterator it;
    for (GPStore::uint_32 rp : postCache[person2_vid]) {
        it = commReplyOfCache[person1_vid].find(rp);
        if (it != commReplyOfCache[person1_vid].end())
            comm2post += it->second;
    }
    for (GPStore::uint_32 rc : commentCache[person2_vid]) {
        it = commReplyOfCache[person1_vid].find(rc);
        if (it != commReplyOfCache[person1_vid].end())
            comm2comm += it->second;
    }
    for (GPStore::uint_32 lp : postCache[person1_vid]) {
        it = commReplyOfCache[person2_vid].find(lp);
        if (it != commReplyOfCache[person2_vid].end())
            comm2post += it->second;
    }
    for (GPStore::uint_32 lc : commentCache[person1_vid]) {
        it = commReplyOfCache[person2_vid].find(lc);
        if (it != commReplyOfCache[person2_vid].end())
            comm2comm += it->second;
    }
    return double(comm2post) + double(comm2comm) * .5;
}

/**
 * @brief Stored procedure for the IU8 query ("add friendship") of LDBC-SNB Interactive workload.
 * Add a friendship edge (knows) between two Persons.
 * @param args The arguments of the stored procedure
 * @param new_result The result of the stored procedure
 * @param kvstore Pointer to the KVStore instance
 * @param db Pointer to the Database instance
 * @param txn Pointer to the Transaction instance
 * @remark IU8存储过程
 */
void PProcedure::iu8(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn){
    auto& person1id = args[0];
    auto& person2id = args[1];
    auto& creationdate = args[2];
    Node person1_node(person_label_id, id_prop_id, &person1id, kvstore, txn);
    Node person2_node(person_label_id, id_prop_id, &person2id, kvstore, txn);
    assert(person1_node.node_id_ != -1);
    assert(person2_node.node_id_ != -1);
    unordered_map<GPStore::uint_32, vector<GPStore::uint_32>> postCache, commentCache;
    unordered_map<GPStore::uint_32, unordered_map<GPStore::uint_32, unsigned>> commReplyOfCache;
    double know_weight = GetIC14KnowsWeight(kvstore, person1_node.node_id_, person2_node.node_id_, postCache, commentCache, commReplyOfCache);
    long long know_weight_ll = 0;
    memcpy(&know_weight_ll, &know_weight, sizeof(double));
    db->AddEdge(person1_node.node_id_, person2_node.node_id_, knows_pre_id,
                {creationdate.data_.Long, know_weight_ll}, txn);
}

void PProcedure::test(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result,
                     const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
  std::tuple<IVArray*, IVArray*> ivarray_pair = kvstore->GetIndexesNewS2POX();
  size_t s_num = std::get<0>(ivarray_pair)->getNodeNum();
  std::cout << "s_num: " << s_num << std::endl;
  size_t edge_num = std::get<0>(ivarray_pair)->getEdgeNum();
  std::cout << "edge_num: " << edge_num << std::endl;
  std::unordered_map<unsigned, unsigned> p2edge_cnt;
  std::get<0>(ivarray_pair)->getp2edgeNum(p2edge_cnt);
    for (auto &it : p2edge_cnt) {
        std::cout << "p2edge: " << kvstore->getPredicateByID(it.first) << " " << it.second << std::endl;
    }

    GPStore::Value account_id(4620132191874070283LL);
    Node account_node("Account", "id", &account_id, kvstore, txn);
    cout<<account_node.node_id_<<endl;
    cout << account_node["id"]->toString() << endl;
    cout << account_node["createTime"]->toString() << endl;
    cout << account_node["isBlocked"]->toString() << endl;
    cout << account_node["accountType"]->toString() << endl;
    cout << account_node["nickname"]->toString() << endl;
    cout << account_node["phonenum"]->toString() << endl;
    cout << account_node["email"]->toString() << endl;
    cout << account_node["freqLoginType"]->toString() << endl;
    cout << account_node["lastLoginTime"]->toString() << endl;
    cout << account_node["accountLevel"]->toString() << endl;
    TYPE_ENTITY_LITERAL_ID* neighborList = nullptr;
    unsigned neighbor_len = 0;
    long long* prop_list = nullptr; unsigned prop_len = 0;
    account_node.GetLinkedNodesWithEdgeProps("ACCOUNT_REPAY_LOAN", neighborList, prop_list, prop_len, neighbor_len, Util::EDGE_OUT);
    cout << "neighbor_len: " << neighbor_len << endl;
    cout << "prop_len: " << prop_len << endl << endl;
    for (int i = 0; i < neighbor_len; ++i) {
        cout << "[" << i << "] amount = " << *(double*)(&(prop_list[i * prop_len])) << ", createTime = " << *(double*)(&(prop_list[i * prop_len + 1])) << endl;
        Node node(neighborList[i], kvstore, txn);
        cout << "id = " << node["id"]->toString() << ", createTime = " << node["createTime"]->toString() << endl;
        cout << "loanAmount = " << node["loanAmount"]->toString() << ", balance = " << node["balance"]->toString() << endl;
        cout << "loanUsage = " << node["loanUsage"]->toString() << ", interestRate = " << node["interestRate"]->toString() << endl << endl;
    }
    delete[] neighborList;
    delete[] prop_list;

    GPStore::Value loan_id(3942848697212711LL);
    Node loan_node("Loan", "id", &loan_id, kvstore, txn);
    loan_node.GetLinkedNodesWithEdgeProps("ACCOUNT_REPAY_LOAN", neighborList, prop_list, prop_len, neighbor_len, Util::EDGE_IN);
    cout << "neighbor_len: " << neighbor_len << endl;
    cout << "prop_len: " << prop_len << endl << endl;
    delete[] neighborList;
    delete[] prop_list;
}
struct CompareTuplesTCR1 {
    bool operator()(const tuple<long long, int, long long, string>& a, const tuple<long long, int, long long, string>& b) {
        // Compare based on the first three integers in ascending order
        if (get<0>(a) != get<0>(b)) return get<0>(a) > get<0>(b);
        if (get<1>(a) != get<1>(b)) return get<1>(a) > get<1>(b);
        return get<2>(a) > get<2>(b);
    }
};

void PProcedure::tcr1(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
    Node accountNode("Account", "id", &args[0], kvstore, txn);
    if (accountNode.node_id_ == -1)
        return;
    double startTime = args[1].data_.Long, endTime = args[2].data_.Long;
    int truncationLimit = args[3].data_.Int;
    TruncationOrderType truncationOrder = static_cast<TruncationOrderType>(args[4].data_.Int);
    
    priority_queue<tuple<long long, int, long long, string>, vector<tuple<long long, int, long long, string>>, CompareTuplesTCR1> pq;
    unordered_map<long long, double> id2earliest;
    queue<pair<long long, int>> frontier;
    frontier.push(make_pair(accountNode.node_id_, 0));
    id2earliest[accountNode.node_id_] = -1;
    // Stop at 3 hops
    bool asc = true; int prop_idx = -1;
    if (truncationOrder == TruncationOrderType::TIMESTAMP_ASCENDING) {
        asc = true;
        prop_idx = 1;
    } else if (truncationOrder == TruncationOrderType::TIMESTAMP_DESCENDING) {
        asc = false;
        prop_idx = 1;
    } else if (truncationOrder == TruncationOrderType::AMOUNT_ASCENDING) {
        asc = true;
        prop_idx = 0;
    } else if (truncationOrder == TruncationOrderType::AMOUNT_DESCENDING) {
        asc = false;
        prop_idx = 0;
    }
    while (!frontier.empty()) {
        long long curId = frontier.front().first;
        int curDistance = frontier.front().second;
        frontier.pop();
        if (curDistance == 3)
            continue;
        Node curNode(curId, kvstore, txn);
        TYPE_ENTITY_LITERAL_ID* neighborList = nullptr; unsigned neighbor_len = 0;
        long long* prop_list = nullptr; unsigned prop_len = 0;
        curNode.GetLinkedNodesWithEdgeProps("ACCOUNT_TRANSFER_ACCOUNT", neighborList, prop_list, prop_len, neighbor_len, Util::EDGE_OUT);
        int numValidNeighbor = truncationReorder(neighborList, neighbor_len, prop_list, prop_len, prop_idx, truncationLimit, asc);
        for (unsigned i = 0; i < numValidNeighbor; ++i) {
            // If the edge's timestamp <= the earliest timestamp that can reach the current node, skip this edge
            double edgeTime = 0;
            memcpy(&edgeTime, &prop_list[i * prop_len + 1], sizeof(double));
            if (edgeTime >= endTime || edgeTime <= startTime || edgeTime <= id2earliest[curId])
                continue;
            unsigned neighbor_id = neighborList[i];
            if (id2earliest.find(neighbor_id) == id2earliest.end()) {
                id2earliest[neighbor_id] = edgeTime;
                frontier.push(make_pair(neighbor_id, curDistance + 1));
                // Check medium here
                TYPE_ENTITY_LITERAL_ID* inner_neighborList = nullptr; unsigned inner_neighbor_len = 0;
                Node neighbor(neighbor_id, kvstore, txn);
                neighbor.GetLinkedNodes("MEDIUM_SIGN_INACCOUNT", inner_neighborList, inner_neighbor_len, Util::EDGE_IN);
                for (unsigned j = 0; j < inner_neighbor_len; ++j) {
                    Node medium(inner_neighborList[j], kvstore, txn);
                    if (medium["isBlocked"]->data_.Boolean) {
                        tuple<long long, int, long long, string> t = make_tuple(neighbor_id, curDistance + 1, medium["id"]->data_.Long, *(medium["mediumType"]->data_.String));
                        pq.push(t);
                        break;
                    }
                }
                delete[] inner_neighborList;
            } else if (edgeTime < id2earliest[neighbor_id]) {
                id2earliest[neighbor_id] = edgeTime;
                frontier.push(make_pair(neighbor_id, curDistance + 1)); // Reexplore the node if the earliest reach time is updated
            }
        }
        delete[] neighborList;
        delete[] prop_list;
    }

    new_result->rows_.reserve(pq.size());
    while (!pq.empty()) {
        auto t = pq.top();
        pq.pop();
        new_result->rows_.emplace_back();
        new_result->rows_.back().values_.reserve(4);
        new_result->rows_.back().values_.emplace_back(get<0>(t));
        new_result->rows_.back().values_.emplace_back(get<1>(t));
        new_result->rows_.back().values_.emplace_back(get<2>(t));
        new_result->rows_.back().values_.emplace_back(get<3>(t));
        cout << get<0>(t) << " " << get<1>(t) << " " << get<2>(t) << " " << get<3>(t) << endl;
    }
}

void BFSWithPathTsOrder(const KVstore *kvstore, Node& accountNode,std::shared_ptr<Transaction> txn,int truncationLimit,TruncationOrderType& truncationOrder, unordered_map<long long, pair<double, double>>& ans, double startTime, double endTime)
{
    unordered_map<long long, double> id2latest;
    queue<pair<long long, int>> frontier;
    frontier.push(make_pair(accountNode.node_id_, 0));
    id2latest[accountNode.node_id_] = -1;
    // Stop at 3 hops
    while (!frontier.empty()) 
    {
        long long curId = frontier.front().first;
        int curDistance = frontier.front().second;
        frontier.pop();
        if (curDistance == 3)
            continue;
        Node curNode(curId, kvstore, txn);
        TYPE_ENTITY_LITERAL_ID* neighborList = nullptr; unsigned neighbor_len = 0;
        long long* prop_list = nullptr; unsigned prop_len = 0;
        curNode.GetLinkedNodesWithEdgeProps("ACCOUNT_TRANSFER_ACCOUNT", neighborList, prop_list, prop_len, neighbor_len, Util::EDGE_IN);
        bool asc = true; int prop_idx = -1;
        if (truncationOrder == TruncationOrderType::TIMESTAMP_ASCENDING) {
            asc = true;
            prop_idx = 1;
        } else if (truncationOrder == TruncationOrderType::TIMESTAMP_DESCENDING) {
            asc = false;
            prop_idx = 1;
        } else if (truncationOrder == TruncationOrderType::AMOUNT_ASCENDING) {
            asc = true;
            prop_idx = 0;
        } else if (truncationOrder == TruncationOrderType::AMOUNT_DESCENDING) {
            asc = false;
            prop_idx = 0;
        }
        int numValidNeighbor = truncationReorder(neighborList, neighbor_len, prop_list, prop_len, prop_idx, truncationLimit, asc);
        for (unsigned i = 0; i < numValidNeighbor; ++i) 
        {
            // If the edge's timestamp <= the earliest timestamp that can reach the current node, skip this edge
            double edgeTime = LL2double(prop_list[i * prop_len + 1]);
            if (edgeTime >= id2latest[curId])
                continue;
            if(edgeTime >= endTime || edgeTime <= startTime) continue;
            unsigned neighbor_id = neighborList[i];
            if (id2latest.find(neighbor_id) == id2latest.end()) {
                id2latest[neighbor_id] = edgeTime;
                frontier.push(make_pair(neighbor_id, curDistance + 1));
                // Check loan here
                if(!ans.count(neighbor_id))
                {
                    TYPE_ENTITY_LITERAL_ID* loan_list = nullptr; unsigned nbr_loan_num = 0;
                    Node neighbor(neighbor_id, kvstore, txn);
                    neighbor.GetLinkedNodes("LOAN_DEPOSIT_ACCOUNT", loan_list, nbr_loan_num, Util::EDGE_IN);
                    double amount_sum = 0;
                    double balance_sum=0;
                    for (unsigned j = 0; j < nbr_loan_num; ++j) {
                        Node loan(loan_list[j], kvstore, txn);
                        amount_sum += loan["loanAmount"]->data_.Float;
                        balance_sum += loan["balance"]->data_.Float;
                        
                    }
                    ans.insert({neighbor_id,{amount_sum, balance_sum}});
                    delete[] loan_list;
                }
                
                else if (edgeTime > id2latest[neighbor_id]) {
                    id2latest[neighbor_id] = edgeTime;
                    frontier.push(make_pair(neighbor_id, curDistance + 1)); // Reexplore the node if the earliest reach time is updated
                }
            }
            delete[] neighborList;
            delete[] prop_list;
        }
    }
}

void PProcedure::tcr2(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
    Node person("Person", "id", &args[0], kvstore, txn);
    if (person.node_id_ == -1)
        return;
    double startTime = args[1].data_.Long, endTime = args[2].data_.Long;
    int truncationLimit = args[3].data_.Int;
    TruncationOrderType truncationOrder = static_cast<TruncationOrderType>(args[4].data_.Int);
    

    unordered_map<long long, pair<double, double>> ans;

    TYPE_ENTITY_LITERAL_ID* account_list = nullptr; unsigned account_num = 0;
    person.GetLinkedNodes("PERSON_OWN_ACCOUNT", account_list, account_num, Util::EDGE_OUT);

    if(account_num)
    {
        for(int i = 0;i<account_num;i++)
        {
            Node accountNode(account_list[i], kvstore, txn); 
            BFSWithPathTsOrder(kvstore, accountNode, txn, truncationLimit, truncationOrder, ans,startTime, endTime);
        }
    }
    
    vector<tuple< double,long long, double>> ans_vec;
    for(auto i: ans)
    {
        ans_vec.emplace_back(- i.second.first, i.first, i.second.second);
    }
    sort(ans_vec.begin(), ans_vec.end());


    new_result->rows_.reserve(ans_vec.size());
    for(auto t: ans_vec) {
        
        new_result->rows_.emplace_back();
        new_result->rows_.back().values_.reserve(3);
        new_result->rows_.back().values_.emplace_back(get<1>(t));
        new_result->rows_.back().values_.emplace_back(-get<0>(t));
        new_result->rows_.back().values_.emplace_back(get<2>(t));
        
        cout << get<1>(t) << " " << -get<0>(t) << " " << get<2>(t) << endl;
    }
}
void PProcedure::tcr3(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
    Node src_account("Account", "id", &args[0], kvstore, txn);
    Node dst_account("Account", "id", &args[1], kvstore, txn);
    if (src_account.node_id_ == -1 || dst_account.node_id_ == -1)
        return;
    int dst_id = dst_account.node_id_;
    double startTime = args[2].data_.Long, endTime = args[3].data_.Long;
    
    
    queue<pair<int,int>> q;
    q.push({src_account.node_id_,0});
    long long ans = -1;
    while(!q.empty())
    {
        int path_len = q.front().second;
        Node node (q.front().first,kvstore, txn);
        q.pop();

        TYPE_ENTITY_LITERAL_ID* neighborList = nullptr; unsigned neighbor_len = 0;
        long long* prop_list = nullptr; unsigned prop_len = 0;
        node.GetLinkedNodesWithEdgeProps("ACCOUNT_TRANSFER_ACCOUNT", neighborList, prop_list, prop_len, neighbor_len, Util::EDGE_OUT);
        for (unsigned i = 0; i < neighbor_len; ++i) 
        {
            // If the edge's timestamp <= the earliest timestamp that can reach the current node, skip this edge
            double edgeTime = LL2double(prop_list[i * prop_len + 1]);
            if(edgeTime >= endTime || edgeTime <= startTime) continue;
            unsigned neighbor_id = neighborList[i];
            if(neighbor_id == dst_id)
            {
                ans = path_len +1;
                break;
            }
            q.push({neighbor_id,path_len+1});
        }
        if(ans != -1) break;

    }
    new_result->rows_.emplace_back();
    new_result->rows_.back().values_.reserve(1);
    new_result->rows_.back().values_.emplace_back(ans);
    
    cout << ans << endl;

}


struct CompareTcr4Tuples {
    bool operator()(const tuple<int, long long, double, double, long long, double,double>& a, const tuple<int, long long, double, double, long long, double,double>& b) {
        // Compare based on the first three integers in ascending order
        if (get<2>(a) != get<2>(b)) return get<2>(a) > get<2>(b);
        if (get<5>(a) != get<5>(b)) return get<5>(a) > get<5>(b);


        return get<0>(a) < get<0>(b);
    }
};
void PProcedure::tcr4(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
    Node src_account("Account", "id", &args[0], kvstore, txn);
    Node dst_account("Account", "id", &args[1], kvstore, txn);
    if (src_account.node_id_ == -1 || dst_account.node_id_ == -1)
        return;
    int dst_id = dst_account.node_id_;
    double startTime = args[2].data_.Long, endTime = args[3].data_.Long;

    TYPE_ENTITY_LITERAL_ID* neighborList_src = nullptr; unsigned neighbor_len_src = 0;
    long long* prop_list_src = nullptr; unsigned prop_len_src = 0;
    src_account.GetLinkedNodesWithEdgeProps("ACCOUNT_TRANSFER_ACCOUNT", neighborList_src, prop_list_src, prop_len_src, neighbor_len_src, Util::EDGE_OUT);
    bool valid = false;
    for(int i = 0;i<neighbor_len_src;i++){
        double edgeTime = prop_list_src[i * prop_len_src + 1];
        if(edgeTime >= endTime || edgeTime <= startTime) continue;
        if(neighborList_src[i] == dst_id) valid = true;
    }
    if(!valid)
    {
        cout<<"empty result"<<endl;
        return;
    }

    TYPE_ENTITY_LITERAL_ID* neighborList_dst = nullptr; unsigned neighbor_len_dst = 0;
    long long* prop_list_dst = nullptr; unsigned prop_len_dst = 0;
    dst_account.GetLinkedNodesWithEdgeProps("ACCOUNT_TRANSFER_ACCOUNT", neighborList_dst, prop_list_dst, prop_len_dst, neighbor_len_dst, Util::EDGE_OUT);
    unordered_map<int,tuple<long long,double, double>> other_id_dst;  /// in tuple: int: num_edges; 2 doubles: sum of account & max of account
    for(int i = 0;i<neighbor_len_dst;i++)
    {
        double edgeTime = prop_list_dst[i * prop_len_dst + 1];
        if(edgeTime >= endTime || edgeTime <= startTime) continue;
        int other_id = neighborList_dst[i];
        if(!other_id_dst.count(other_id))
        {
            other_id_dst.insert({other_id,{0,0,0}});
        }
        get<0>(other_id_dst[other_id])++;
        get<1>(other_id_dst[other_id]) += LL2double(prop_list_dst[i * prop_len_dst]);
        get<2>(other_id_dst[other_id]) = max(LL2double(prop_list_dst[i * prop_len_dst]),get<2>(other_id_dst[other_id]));
        
    }


    delete[] prop_list_src;
    prop_list_src = nullptr;
    delete[] neighborList_src;
    neighborList_src = nullptr;
    neighbor_len_src = prop_len_src = 0;
    
    unordered_map<int,tuple<long long , double, double>> other_id_src;  /// in tuple: int: num_edges; 2 doubles: sum of account & max of account
    src_account.GetLinkedNodesWithEdgeProps("ACCOUNT_TRANSFER_ACCOUNT", neighborList_src, prop_list_src, prop_len_src, neighbor_len_src, Util::EDGE_IN);
    for(int i = 0;i<neighbor_len_src;i++)
    {
        double edgeTime = prop_list_src[i * prop_len_src + 1];
        if(edgeTime >= endTime || edgeTime <= startTime) continue;
        int other_id = neighborList_src[i];
        if(other_id_dst.count(other_id))
        {
            if(!other_id_src.count(other_id))
            {
                other_id_src.insert({other_id,{0,0,0}});
            }
            get<0>(other_id_src[other_id])++;
            get<1>(other_id_src[other_id]) += LL2double(prop_list_src[i * prop_len_src]);
            get<2>(other_id_src[other_id]) = max(LL2double(prop_list_src[i * prop_len_src]),get<2>(other_id_src[other_id]));
        
        }
    }
    vector<tuple<int, long long, double, double, long long, double,double>> ans;
    for(auto& [other_id, info2]: other_id_src)
    {
        long long num_edge2 = get<0>(info2);
        double sum_edge2amount = get<1>(info2);
        double max_edge2amount = get<2>(info2);
        auto& info3 = other_id_dst[other_id];
        long long num_edge3 = get<0>(info3);
        double sum_edge3amount = get<1>(info3);
        double max_edge3amount = get<2>(info3);
        ans.emplace_back(other_id, num_edge2,sum_edge2amount, max_edge2amount, num_edge3, sum_edge3amount, max_edge3amount);

    }
    
    sort(ans.begin(), ans.end(), CompareTcr4Tuples());

    new_result->rows_.reserve(ans.size());
    for(auto&t:  ans)
    {
        new_result->rows_.emplace_back();
        new_result->rows_.back().values_.reserve(7);
        new_result->rows_.back().values_.emplace_back(get<0>(t));
        new_result->rows_.back().values_.emplace_back(get<1>(t));
        new_result->rows_.back().values_.emplace_back(get<2>(t));
        new_result->rows_.back().values_.emplace_back(get<3>(t));
        new_result->rows_.back().values_.emplace_back(get<4>(t));
        new_result->rows_.back().values_.emplace_back(get<5>(t));
        new_result->rows_.back().values_.emplace_back(get<6>(t));
        
        cout << get<0>(t) << " " << get<1>(t) << " " << get<2>(t) << " " << get<3>(t) << get<4>(t) << " " << get<5>(t) << " " << get<6>(t) <<endl;
    }
    
}
int Truncate(TYPE_ENTITY_LITERAL_ID* neighborList , unsigned neighbor_len ,long long* prop_list , unsigned prop_len,TruncationOrderType truncationOrder,int truncationLimit )
{
    bool asc = true; int prop_idx = -1;
    if (truncationOrder == TruncationOrderType::TIMESTAMP_ASCENDING) {
        asc = true;
        prop_idx = 1;
    } else if (truncationOrder == TruncationOrderType::TIMESTAMP_DESCENDING) {
        asc = false;
        prop_idx = 1;
    } else if (truncationOrder == TruncationOrderType::AMOUNT_ASCENDING) {
        asc = true;
        prop_idx = 0;
    } else if (truncationOrder == TruncationOrderType::AMOUNT_DESCENDING) {
        asc = false;
        prop_idx = 0;
    }
    int numValidNeighbor = truncationReorder(neighborList, neighbor_len, prop_list, prop_len, prop_idx, truncationLimit, asc);
      return   numValidNeighbor;
}
int Truncate(EdgeInfoList l,TruncationOrderType truncationOrder,int truncationLimit)
{
    return Truncate(l.dst_ids, l.dst_num, l.edge_props, l.edge_prop_num, truncationOrder, truncationLimit);
         
}

void PProcedure::tcr5(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
    Node person("Person", "id", &args[0], kvstore, txn);
    if (person.node_id_ == -1)
        return;
    double startTime = args[1].data_.Long, endTime = args[2].data_.Long;
    int truncationLimit = args[3].data_.Int;
    TruncationOrderType truncationOrder = static_cast<TruncationOrderType>(args[4].data_.Int);
    

    
    struct HashPairInt
    {
        ssize_t operator()(pair<int,int> p) const
        {
            long long combined = static_cast<long long>(p.first) | (static_cast<long long>(p.second) << 32);
            // 使用 std::hash<long long> 进行哈希计算
            return std::hash<long long>{}(combined);
        }
        
    };
    struct PathSameSrc
    {
        int start_id = -1;
        unordered_map<int,double> len1path;  /// double: create time
        unordered_map<pair<int,int>,double,HashPairInt> len2path;
        set<tuple<int,int,int>> len3path;
    };
    

    EdgeInfoList person_own_accounts;
    person.GetLinkedNodes("PERSON_OWN_ACCOUNT", person_own_accounts.dst_ids, person_own_accounts.dst_num, Util::EDGE_OUT);
    int num_valid_nbr_own = Truncate(person_own_accounts, truncationOrder, truncationLimit);

    vector<PathSameSrc> whole_paths;
    for(int i = 0;i<num_valid_nbr_own; i++)
    {
        PathSameSrc paths;
        paths.start_id = person_own_accounts.dst_ids[i];
        Node start_acc(paths.start_id, kvstore, txn);
        EdgeInfoList l;
        start_acc.GetLinkedNodesWithEdgeProps("ACCOUNT_TRANSFER_ACCOUNT", l.dst_ids, l.edge_props, l.edge_prop_num, l.dst_num, Util::EDGE_OUT);
        int num_valid_nbr = Truncate(l, truncationOrder, truncationLimit);
        for(int j = 0;j<num_valid_nbr; j++)
        {
            double edgeTime = LL2double(l.edge_props[j * l.edge_prop_num + 1]);
            if(edgeTime >= endTime || edgeTime <= startTime) continue;

            if(!paths.len1path.count(l.dst_ids[j]))
            {
                paths.len1path.insert({l.dst_ids[j],edgeTime});
            }
            else
            {
                if(paths.len1path[l.dst_ids[j]] > edgeTime)
                {
                    paths.len1path[l.dst_ids[j]] = edgeTime;
                }
            }
        }

        for(auto& [path_mid, mid_create_time]: paths.len1path)
        {
            Node mid_acc(path_mid, kvstore, txn);
            EdgeInfoList l;
            mid_acc.GetLinkedNodes("ACCOUNT_TRANSFER_ACCOUNT", l.dst_ids, l.dst_num, Util::EDGE_OUT);
            int num_valid_nbr = Truncate(l, truncationOrder, truncationLimit);
            for(int j = 0;j<num_valid_nbr; j++)
            {
                double edgeTime = LL2double(l.edge_props[j * l.edge_prop_num + 1]);
                if(edgeTime >= endTime || edgeTime <= startTime) continue;
                if(mid_create_time >= edgeTime) continue;

                if(paths.len2path.count({path_mid, l.dst_ids[j]}))
                {
                    paths.len2path.insert({{path_mid, l.dst_ids[j]},edgeTime});
                }
                else
                {
                    if(paths.len2path[{path_mid, l.dst_ids[j]}] > edgeTime)
                    {
                        paths.len2path[{path_mid, l.dst_ids[j]}] = edgeTime;
                    }
                }
            }
        }
        for(auto& [path_mid, mid_create_time]: paths.len2path)
        {
            Node mid_acc(path_mid.second, kvstore, txn);
            EdgeInfoList l;
            mid_acc.GetLinkedNodes("ACCOUNT_TRANSFER_ACCOUNT", l.dst_ids, l.dst_num, Util::EDGE_OUT);
            int num_valid_nbr = Truncate(l, truncationOrder, truncationLimit);
            for(int j = 0;j<num_valid_nbr; j++)
            {
                double edgeTime = LL2double(l.edge_props[j * l.edge_prop_num + 1]);
                if(edgeTime >= endTime || edgeTime <= startTime) continue;
                if(mid_create_time >= edgeTime) continue;

                paths.len3path.insert({path_mid.first, path_mid.second, l.dst_ids[j]});
            }
        }
        whole_paths.emplace_back(paths);

    }


    vector<vector<int>> ans;
    for(int path_len = 3; path_len>=1; path_len--)
    {
        for(auto& path_same_src: whole_paths)
        {
            switch (path_len)
            {
                case 1:
                    for(auto& [len1path,_]: path_same_src.len1path)
                    {
                        ans.emplace_back();
                        ans.back().emplace_back(path_same_src.start_id);
                        ans.back().emplace_back(len1path);
                    }
                    break;
                case 2:
                    for(auto& [len2path,_]: path_same_src.len2path)
                    {
                        ans.emplace_back();
                        ans.back().emplace_back(path_same_src.start_id);
                        ans.back().emplace_back(len2path.first);
                        ans.back().emplace_back(len2path.second);
                    }
                    break;
                case 3:
                    for(auto& [acc1,acc2, acc3]: path_same_src.len3path)
                    {
                        ans.emplace_back();
                        ans.back().emplace_back(path_same_src.start_id);
                        ans.back().emplace_back(acc1);
                        ans.back().emplace_back(acc2);
                        ans.back().emplace_back(acc3);
                    }
                    break;
                default:
                    assert(0);
            }
        }
    }

    new_result->rows_.reserve(ans.size());
    for(auto&path:  ans)
    {
        new_result->rows_.emplace_back();
        for(auto& i:path)
        {
            new_result->rows_.back().values_.emplace_back(i);
            cout<<i<<' ';
        }
        cout<<endl;
        
        
    }


}

void PProcedure::tcr6(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
    Node dst_account("Account", "id", &args[0], kvstore, txn);
    if (dst_account.node_id_ == -1)
        return;
    double threshold1 = args[1].data_.Float;
    double threshold2 = args[2].data_.Float;
    double startTime = args[3].data_.Long, endTime = args[4].data_.Long;
    int truncationLimit = args[5].data_.Int;
    TruncationOrderType truncationOrder = static_cast<TruncationOrderType>(args[6].data_.Int);

    
    
    TYPE_ENTITY_LITERAL_ID* neighborList = nullptr; unsigned neighbor_len = 0;
    long long* prop_list = nullptr; unsigned prop_len = 0;
    dst_account.GetLinkedNodesWithEdgeProps("ACCOUNT_WITHDRAW_ACCOUNT", neighborList, prop_list, prop_len, neighbor_len, Util::EDGE_IN);
    int numValidNeighbor = Truncate(neighborList, neighbor_len, prop_list, prop_len, truncationOrder, truncationLimit);
        
    unordered_map<int,double> mid_id_to_edge2amount;
    for (unsigned i = 0; i < numValidNeighbor; ++i) 
    {
        // If the edge's timestamp <= the earliest timestamp that can reach the current node, skip this edge
        double edgeTime = LL2double(prop_list[i * prop_len + 1]);
        if(edgeTime >= endTime || edgeTime <= startTime) continue;
        double amount = LL2double(prop_list[i * prop_len]);
        if(amount <= threshold2) continue;
        
        unsigned mid_id = neighborList[i];
        if(mid_id_to_edge2amount.count(mid_id))
        {
            mid_id_to_edge2amount.insert({mid_id,0});
        }
        mid_id_to_edge2amount[mid_id]+=amount;
    }

    vector<tuple<double, int, double>> ans; /// -sumedge2amount, midid, sumedge1amount
    for(auto& [mid_id, edge2amount]:mid_id_to_edge2amount)
    {
        delete[] neighborList;
        neighborList = nullptr;
        neighbor_len = 0;
        delete[] prop_list;
        prop_list = nullptr;
        prop_len = 0;
        Node mid_account(mid_id, kvstore, txn);
        mid_account.GetLinkedNodesWithEdgeProps("ACCOUNT_TRANSFER_ACCOUNT", neighborList, prop_list, prop_len, neighbor_len, Util::EDGE_IN);
        
        int numValidNeighbor =  Truncate(neighborList, neighbor_len, prop_list, prop_len, truncationOrder, truncationLimit);
            
        if(numValidNeighbor<=3)continue;
        int valid_edge1_cnt = 0;
        double amount_sum = 0;
        for (unsigned i = 0; i < numValidNeighbor; ++i) 
        {
            // If the edge's timestamp <= the earliest timestamp that can reach the current node, skip this edge
            double edgeTime = LL2double(prop_list[i * prop_len + 1]);
            if(edgeTime >= endTime || edgeTime <= startTime) continue;
            double amount = LL2double(prop_list[i * prop_len]);
            if(amount <= threshold1) continue;
            
            unsigned neighbor_id = neighborList[i];
            valid_edge1_cnt ++;
            amount_sum+=amount;
        }
        if(valid_edge1_cnt >3) 
        {
            ans.emplace_back(-edge2amount, mid_id, amount_sum);
        }
    }
    sort(ans.begin(), ans.end());
    new_result->rows_.reserve(ans.size());
    for(auto&t:  ans)
    {
        new_result->rows_.emplace_back();
        new_result->rows_.back().values_.reserve(3);
        new_result->rows_.back().values_.emplace_back(get<1>(t));
        new_result->rows_.back().values_.emplace_back(-get<0>(t));
        new_result->rows_.back().values_.emplace_back(get<2>(t));
        
        cout << get<0>(t) << " " << get<1>(t) << " " << get<2>(t)  <<endl;
    }


}
void PProcedure::tcr7(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
    Node mid_account("Account", "id", &args[0], kvstore, txn);
    if (mid_account.node_id_ == -1)
        return;
    double threshold = args[1].data_.Float;
    double startTime = args[2].data_.Long, endTime = args[3].data_.Long;
    int truncationLimit = args[4].data_.Int;
    TruncationOrderType truncationOrder = static_cast<TruncationOrderType>(args[5].data_.Int);

    bool asc = true; int prop_idx = -1;
    
    TYPE_ENTITY_LITERAL_ID* neighborList = nullptr; unsigned neighbor_len = 0;
    long long* prop_list = nullptr; unsigned prop_len = 0;
    mid_account.GetLinkedNodesWithEdgeProps("ACCOUNT_TRANSFER_ACCOUNT", neighborList, prop_list, prop_len, neighbor_len, Util::EDGE_OUT);
     if (truncationOrder == TruncationOrderType::TIMESTAMP_ASCENDING) {
        asc = true;
        prop_idx = 1;
    } else if (truncationOrder == TruncationOrderType::TIMESTAMP_DESCENDING) {
        asc = false;
        prop_idx = 1;
    } else if (truncationOrder == TruncationOrderType::AMOUNT_ASCENDING) {
        asc = true;
        prop_idx = 0;
    } else if (truncationOrder == TruncationOrderType::AMOUNT_DESCENDING) {
        asc = false;
        prop_idx = 0;
    }
    int numValidNeighbor = truncationReorder(neighborList, neighbor_len, prop_list, prop_len, prop_idx, truncationLimit, asc);
        
    int cntsrc=0, cntdst=0;
    double amountsrc=0, amountdst =0;
    for (unsigned i = 0; i < numValidNeighbor; ++i) 
    {
        // If the edge's timestamp <= the earliest timestamp that can reach the current node, skip this edge
        double edgeTime = LL2double(prop_list[i * prop_len + 1]);
        if(edgeTime >= endTime || edgeTime <= startTime) continue;
        double amount = LL2double(prop_list[i * prop_len]);
        if(amount <= threshold) continue;
        
        unsigned neighbor_id = neighborList[i];
        cntdst++;
        amountdst += amount;
        
    }

    delete[] neighborList;
    neighborList = nullptr;
    neighbor_len = 0;
    delete[] prop_list;
    prop_list = nullptr;
    prop_len = 0;
    mid_account.GetLinkedNodesWithEdgeProps("ACCOUNT_TRANSFER_ACCOUNT", neighborList, prop_list, prop_len, neighbor_len, Util::EDGE_IN);
    if (truncationOrder == TruncationOrderType::TIMESTAMP_ASCENDING)
    {
        asc = true;
        prop_idx = 1;
    }
    else if (truncationOrder == TruncationOrderType::TIMESTAMP_DESCENDING)
    {
        asc = false;
        prop_idx = 1;
    }
    else if (truncationOrder == TruncationOrderType::AMOUNT_ASCENDING)
    {
        asc = true;
        prop_idx = 0;
    }
    else if (truncationOrder == TruncationOrderType::AMOUNT_DESCENDING)
    {
        asc = false;
        prop_idx = 0;
    }
    numValidNeighbor = truncationReorder(neighborList, neighbor_len, prop_list, prop_len, prop_idx, truncationLimit, asc);

    for (unsigned i = 0; i < numValidNeighbor; ++i) 
    {
        // If the edge's timestamp <= the earliest timestamp that can reach the current node, skip this edge
        double edgeTime = LL2double(prop_list[i * prop_len + 1]);
        if(edgeTime >= endTime || edgeTime <= startTime) continue;
        double amount = LL2double(prop_list[i * prop_len]);
        if(amount <= threshold) continue;
        
        unsigned neighbor_id = neighborList[i];
        cntsrc++;
        amountsrc += amount;
        
    }
    float ratio;
    if(cntdst == 0) ratio = -1;
    else ratio = amountsrc/amountdst;

    new_result->rows_.emplace_back();
    new_result->rows_.back().values_.reserve(1);
    new_result->rows_.back().values_.emplace_back(cntsrc); 
    new_result->rows_.back().values_.emplace_back(cntdst);
    new_result->rows_.back().values_.emplace_back(ratio);
    cout << cntsrc<<' '<<cntdst<<' '<<ratio << endl;

}

struct CompareTuplesTCR8 {
    bool operator()(const tuple<long long, float, int>& a, const tuple<long long, float, int>& b) {
        // Compare based on the first three integers in ascending order
        if (get<2>(a) != get<0>(b)) return get<2>(a) < get<0>(b);
        if (get<1>(a) != get<1>(b)) return get<1>(a) < get<1>(b);
        return get<0>(a) > get<0>(b);
    }
};

void PProcedure::tcr8(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
    Node loan("Loan", "id", &args[0], kvstore, txn);
    if (loan.node_id_ == -1)
        return;
    float loanAmount = loan["loanAmount"]->data_.Float;
    double threshold = args[1].data_.Float, startTime = args[2].data_.Long, endTime = args[3].data_.Long;
    int truncationLimit = args[4].data_.Int;
    TruncationOrderType truncationOrder = static_cast<TruncationOrderType>(args[5].data_.Int);
    TYPE_ENTITY_LITERAL_ID* neighborList = nullptr; unsigned neighbor_len = 0;
    long long* prop_list = nullptr; unsigned prop_len = 0;
    loan.GetLinkedNodesWithEdgeProps("LOAN_DEPOSIT_ACCOUNT", neighborList, prop_list, prop_len, neighbor_len, Util::EDGE_OUT);
    if (neighbor_len == 0) {
        delete[] neighborList;
        delete[] prop_list;
        return;
    }
    Node curAccount(neighborList[0], kvstore, txn);
    queue<pair<long long, int>> frontier;
    frontier.push(make_pair(curAccount.node_id_, 0));
    unordered_map<unsigned, double> account2upstream;
    curAccount.GetLinkedNodesWithEdgeProps("ACCOUNT_TRANSFER_ACCOUNT", neighborList, prop_list, prop_len, neighbor_len, Util::EDGE_IN);
    double curUpstream = 0;
    for (unsigned i = 0; i < neighbor_len; ++i)
        curUpstream += LL2double(prop_list[i * prop_len]);
    account2upstream[curAccount.node_id_] = curUpstream;
    delete[] neighborList;
    delete[] prop_list;
    priority_queue<tuple<long long, float, int>, vector<tuple<long long, float, int>>, CompareTuplesTCR8> pq;
    // Stop at 3 hops
    while (!frontier.empty()) {
        long long curId = frontier.front().first;
        int curDistance = frontier.front().second;
        frontier.pop();
        if (curDistance == 3)
            continue;
        Node curNode(curId, kvstore, txn);

        // Get transfer and withdraw edges, concatenate them for truncation
        TYPE_ENTITY_LITERAL_ID* neighborList = nullptr; unsigned neighbor_len = 0;
        long long* prop_list = nullptr; unsigned prop_len = 0;
        curNode.GetLinkedNodesWithEdgeProps("ACCOUNT_TRANSFER_ACCOUNT", neighborList, prop_list, prop_len, neighbor_len, Util::EDGE_OUT);   // prop_len == 3
        TYPE_ENTITY_LITERAL_ID* neighborList_other = nullptr; unsigned neighbor_len_other = 0;
        long long* prop_list_other = nullptr; unsigned prop_len_other = 0;
        curNode.GetLinkedNodesWithEdgeProps("ACCOUNT_WITHDRAW_ACCOUNT", neighborList_other, prop_list_other, prop_len_other, neighbor_len_other, Util::EDGE_OUT);   // prop_len_other == 2
        unsigned neighbor_len_total = neighbor_len + neighbor_len_other;
        if (neighbor_len_total == 0) {
            delete[] neighborList;
            delete[] prop_list;
            delete[] neighborList_other;
            delete[] prop_list_other;
            continue;
        }

        TYPE_ENTITY_LITERAL_ID* neighborList_total = new TYPE_ENTITY_LITERAL_ID[neighbor_len_total];
        long long* prop_list_total = new long long[neighbor_len_total * 2];
        memcpy(neighborList_total, neighborList, neighbor_len * sizeof(TYPE_ENTITY_LITERAL_ID));
        memcpy(neighborList_total + neighbor_len, neighborList_other, neighbor_len_other * sizeof(TYPE_ENTITY_LITERAL_ID));
        memcpy(prop_list_total, prop_list, neighbor_len * prop_len * sizeof(long long));
        size_t offset = neighbor_len * 2;
        for (size_t i = 0; i < neighbor_len_other; ++i) {
            prop_list_total[neighbor_len * 2 + i * 2] = prop_list_other[i * 3];
            prop_list_total[neighbor_len * 2 + i * 2 + 1] = prop_list_other[i * 3 + 1];
        }
        delete [] neighborList;
        delete [] prop_list;
        delete [] neighborList_other;
        delete [] prop_list_other;

        prop_len = 2;
        bool asc = true; int prop_idx = -1;
        if (truncationOrder == TruncationOrderType::TIMESTAMP_ASCENDING) {
            asc = true;
            prop_idx = 1;
        } else if (truncationOrder == TruncationOrderType::TIMESTAMP_DESCENDING) {
            asc = false;
            prop_idx = 1;
        } else if (truncationOrder == TruncationOrderType::AMOUNT_ASCENDING) {
            asc = true;
            prop_idx = 0;
        } else if (truncationOrder == TruncationOrderType::AMOUNT_DESCENDING) {
            asc = false;
            prop_idx = 0;
        }
        int numValidNeighbor = truncationReorder(neighborList_total, neighbor_len_total, prop_list_total, 2, prop_idx, truncationLimit, asc);

        // Get current node's upstream amount
        auto it = account2upstream.find(curId);
        double curUpstream = it->second;
        if (curUpstream < 0)
            continue;
        it->second = -1;    // Mark visited
        double thresholdAmount = curUpstream * threshold;

        for (unsigned i = 0; i < numValidNeighbor; ++i) {
            // If the edge's timestamp <= the earliest timestamp that can reach the current node, skip this edge
            double edgeTime = *((double *)(&prop_list_total[i * prop_len + 1]));
            double edgeAmount = *((double *)(&prop_list_total[i * prop_len]));
            if (edgeTime >= endTime || edgeTime <= startTime || edgeAmount <= thresholdAmount)
                continue;
            unsigned neighbor_id = neighborList_total[i];
            frontier.push(make_pair(neighbor_id, curDistance + 1));
            it = account2upstream.find(neighbor_id);
            double neighborUpstream = 0;
            Node neighborNode(neighbor_id, kvstore, txn);
            if (it == account2upstream.end()) {
                neighborNode.GetLinkedNodesWithEdgeProps("ACCOUNT_TRANSFER_ACCOUNT", neighborList, prop_list, prop_len, neighbor_len, Util::EDGE_IN);
                for (unsigned i = 0; i < neighbor_len; ++i)
                    neighborUpstream += *((double *)(prop_list[i * prop_len]));
                account2upstream[neighbor_id] = neighborUpstream;
                delete[] neighborList;
                delete[] prop_list;
            } else
                neighborUpstream = it->second;
            float ratio = float(std::round(1000. * neighborUpstream / loanAmount)) / 1000.;
            pq.push(make_tuple(neighborNode["id"]->data_.Long, ratio, curDistance + 1));
        }
        delete[] neighborList_total;
        delete[] prop_list_total;
    }

    new_result->rows_.reserve(pq.size());
    while (!pq.empty()) {
        auto t = pq.top();
        pq.pop();
        new_result->rows_.emplace_back();
        new_result->rows_.back().values_.reserve(3);
        new_result->rows_.back().values_.emplace_back(get<0>(t));
        new_result->rows_.back().values_.emplace_back(get<1>(t));
        new_result->rows_.back().values_.emplace_back(get<2>(t));
        cout << get<0>(t) << " " << get<1>(t) << " " << get<2>(t) << endl;
    }
}

void PProcedure::tcr9(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
    Node account("Account", "id", &args[0], kvstore, txn);
    if (account.node_id_ == -1)
        return;
    double threshold = args[1].data_.Float, startTime = args[2].data_.Long, endTime = args[3].data_.Long;
    int truncationLimit = args[4].data_.Int;
    TruncationOrderType truncationOrder = static_cast<TruncationOrderType>(args[5].data_.Int);

    // edge 1
    float edge1sum = 0;
    TYPE_ENTITY_LITERAL_ID* neighborList = nullptr; unsigned neighbor_len = 0;
    long long* prop_list = nullptr; unsigned prop_len = 0;
    account.GetLinkedNodesWithEdgeProps("LOAN_DEPOSIT_ACCOUNT", neighborList, prop_list, prop_len, neighbor_len, Util::EDGE_IN);
    int numValidNeighbor = Truncate(neighborList, neighbor_len, prop_list, prop_len, truncationOrder, truncationLimit);
    for (int i = 0; i < numValidNeighbor; ++i) {
        double edgeTime = LL2double(prop_list[i * prop_len + 1]);
        if (edgeTime >= endTime || edgeTime <= startTime) continue;
        double amount = LL2double(prop_list[i * prop_len]);
        if (amount <= threshold) continue;
        edge1sum += amount;
    }
    delete [] neighborList;
    delete [] prop_list;

    // edge 2
    float edge2sum = 0;
    account.GetLinkedNodesWithEdgeProps("ACCOUNT_REPAY_LOAN", neighborList, prop_list, prop_len, neighbor_len, Util::EDGE_OUT);
    numValidNeighbor = Truncate(neighborList, neighbor_len, prop_list, prop_len, truncationOrder, truncationLimit);
    for (int i = 0; i < numValidNeighbor; ++i) {
        double edgeTime = LL2double(prop_list[i * prop_len + 1]);
        if (edgeTime >= endTime || edgeTime <= startTime) continue;
        double amount = LL2double(prop_list[i * prop_len]);
        if (amount <= threshold) continue;
        edge2sum += amount;
    }
    delete [] neighborList;
    delete [] prop_list;

    // edge 3
    float edge3sum = 0;
    account.GetLinkedNodesWithEdgeProps("ACCOUNT_TRANSFER_ACCOUNT", neighborList, prop_list, prop_len, neighbor_len, Util::EDGE_IN);
    numValidNeighbor = Truncate(neighborList, neighbor_len, prop_list, prop_len, truncationOrder, truncationLimit);
    for (int i = 0; i < numValidNeighbor; ++i) {
        double edgeTime = LL2double(prop_list[i * prop_len + 1]);
        if (edgeTime >= endTime || edgeTime <= startTime) continue;
        double amount = LL2double(prop_list[i * prop_len]);
        if (amount <= threshold) continue;
        edge3sum += amount;
    }
    delete [] neighborList;
    delete [] prop_list;

    // edge 4
    float edge4sum = 0;
    account.GetLinkedNodesWithEdgeProps("ACCOUNT_TRANSFER_ACCOUNT", neighborList, prop_list, prop_len, neighbor_len, Util::EDGE_OUT);
    numValidNeighbor = Truncate(neighborList, neighbor_len, prop_list, prop_len, truncationOrder, truncationLimit);
    for (int i = 0; i < numValidNeighbor; ++i) {
        double edgeTime = LL2double(prop_list[i * prop_len + 1]);
        if (edgeTime >= endTime || edgeTime <= startTime) continue;
        double amount = LL2double(prop_list[i * prop_len]);
        if (amount <= threshold) continue;
        edge4sum += amount;
    }
    delete [] neighborList;
    delete [] prop_list;

    float ratioRepay = edge2sum == 0 ? -1 : edge1sum / edge2sum;
    float ratioDeposit = edge4sum == 0 ? -1 : edge1sum / edge4sum;
    float ratioTransfer = edge4sum == 0 ? -1 : edge3sum / edge4sum;
    new_result->rows_.emplace_back();
    new_result->rows_.back().values_.reserve(3);
    new_result->rows_.back().values_.emplace_back(ratioRepay);
    new_result->rows_.back().values_.emplace_back(ratioDeposit);
    new_result->rows_.back().values_.emplace_back(ratioTransfer);
    cout << ratioRepay << " " << ratioDeposit << " " << ratioTransfer << endl;
}

void PProcedure::tcr10(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
    Node p1("Person", "id", &args[0], kvstore, txn), p2("Person", "id", &args[1], kvstore, txn);
    if (p1.node_id_ == -1)
        return;
    double startTime = args[2].data_.Long, endTime = args[3].data_.Long;
    unordered_set<int> p1Invest;
    TYPE_ENTITY_LITERAL_ID* neighborList = nullptr; unsigned neighbor_len = 0;
    long long* prop_list = nullptr; unsigned prop_len = 0;
    int p1InvestNum = 0, p2InvestNum = 0, intersectNum = 0;
    p1.GetLinkedNodesWithEdgeProps("PERSON_INVEST_COMPANY", neighborList, prop_list, prop_len, neighbor_len, Util::EDGE_OUT);
    for (int i = 0; i < neighbor_len; ++i) {
        double edgeTime = LL2double(prop_list[i * prop_len + 1]);
        if (edgeTime >= startTime && edgeTime <= endTime)
            p1Invest.insert(neighborList[i]);
    }
    p1InvestNum = neighbor_len;
    delete[] neighborList;
    delete[] prop_list;
    p2.GetLinkedNodesWithEdgeProps("PERSON_INVEST_COMPANY", neighborList, prop_list, prop_len, neighbor_len, Util::EDGE_OUT);
    p2InvestNum = neighbor_len;
    for (int i = 0; i < neighbor_len; ++i) {
        if (p1Invest.find(neighborList[i]) != p1Invest.end())
            ++intersectNum;
    }
    float jaccard = 0;
    if (p1InvestNum > 0 || p2InvestNum > 0)
        jaccard = float(intersectNum) / float(p1InvestNum + p2InvestNum - intersectNum);
    new_result->rows_.emplace_back();
    new_result->rows_.back().values_.reserve(1);
    new_result->rows_.back().values_.emplace_back(jaccard);
    cout << jaccard << endl;
}
void PProcedure::tcr11(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
    Node src("Person", "id", &args[0], kvstore, txn);
    if (src.node_id_ == -1)
        return;
    double startTime = args[1].data_.Long, endTime = args[2].data_.Long;
    int truncationLimit = args[3].data_.Int;
    TruncationOrderType truncationOrder = static_cast<TruncationOrderType>(args[4].data_.Int);

    bool asc = true; int prop_idx = -1;
    if (truncationOrder == TruncationOrderType::TIMESTAMP_ASCENDING) {
        asc = true;
    } else if (truncationOrder == TruncationOrderType::TIMESTAMP_DESCENDING) {
        asc = false;
    } else if (truncationOrder == TruncationOrderType::AMOUNT_ASCENDING)
        assert(false);

    double sumLoanAmount = 0;
    int numLoans = 0;
    std::unordered_set<TYPE_ENTITY_LITERAL_ID> dst_set, src_set{src.node_id_}, visited;
    while (!src_set.empty()) {
        for(auto vid : src_set) {
            auto v = GPStore::Value((int)vid);
            Node cur("Person", "id", &v, kvstore, txn);
            TYPE_ENTITY_LITERAL_ID *neighborList = nullptr;  unsigned neighbor_len = 0;
            long long *prop_list = nullptr;  unsigned prop_len = 0;
            cur.GetLinkedNodesWithEdgeProps("PERSON_GUARANTEE_PERSON", neighborList, prop_list, prop_len, neighbor_len, Util::EDGE_OUT);
            int numValidNeighbor = truncationReorder(neighborList, neighbor_len, prop_list, prop_len, 0, truncationLimit, asc);

            for(int i = 0; i < numValidNeighbor; ++i) {
                double edge_time = GPStore::Value(prop_list[i * prop_len + 1]).data_.Float;
                if(edge_time > startTime && edge_time < endTime &&
                    visited.find(neighborList[i]) == visited.end()) {
                    visited.insert(neighborList[i]);
                    dst_set.insert(neighborList[i]);
                }
            }
            delete[] neighborList;
            delete[] prop_list;
        }
        swap(src_set, dst_set);
        dst_set.clear();
    }

    for (auto vid : visited) {
        auto v = GPStore::Value((int)vid);
        Node cur("Person", "id", &v, kvstore, txn);
        TYPE_ENTITY_LITERAL_ID *neighborList = nullptr;  unsigned neighbor_len = 0;
        long long *prop_list = nullptr;  unsigned prop_len = 0;
        cur.GetLinkedNodesWithEdgeProps("PERSON_APPLY_LOAN", neighborList, prop_list, prop_len, neighbor_len, Util::EDGE_OUT);
        int numValidNeighbor = truncationReorder(neighborList, neighbor_len, prop_list, prop_len, 1, truncationLimit, asc);
        numLoans += numValidNeighbor;
        for(int i = 0; i < numValidNeighbor; ++i)
            sumLoanAmount += GPStore::Value(prop_list[i * prop_len]).data_.Float;
        delete []neighborList;
        delete []prop_list;
    }

    new_result->rows_.emplace_back();
    new_result->rows_.back().values_.reserve(2);
    new_result->rows_.back().values_.emplace_back(sumLoanAmount);
    new_result->rows_.back().values_.emplace_back(numLoans);
    cout << sumLoanAmount << " " << numLoans << endl;
}
void PProcedure::tcr12(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
    Node src("Person", "id", &args[0], kvstore, txn);
    if (src.node_id_ == -1)
        return;
    double startTime = args[1].data_.Long, endTime = args[2].data_.Long;
    int truncationLimit = args[3].data_.Int;
    TruncationOrderType truncationOrder = static_cast<TruncationOrderType>(args[4].data_.Int);

    TYPE_ENTITY_LITERAL_ID *neighborList = nullptr;  unsigned neighbor_len = 0;
    long long *prop_list = nullptr;  unsigned prop_len = 0;
    src.GetLinkedNodesWithEdgeProps("PERSON_OWN_ACCOUNT", neighborList, prop_list, prop_len, neighbor_len, Util::EDGE_OUT);
    unordered_map<int, double> compAccount2amount;
    for (unsigned i = 0; i < neighbor_len; i++) {
        Node account(neighborList[i], kvstore, txn);
        TYPE_ENTITY_LITERAL_ID *neighborList2 = nullptr;  unsigned neighbor_len2 = 0;
        long long *prop_list2 = nullptr;  unsigned prop_len2 = 0;
        account.GetLinkedNodesWithEdgeProps("ACCOUNT_TRANSFER_ACCOUNT", neighborList2, prop_list2, prop_len2, neighbor_len2, Util::EDGE_OUT);
        int numValidNeighbor = Truncate(neighborList2, neighbor_len2, prop_list2, prop_len2, truncationOrder, truncationLimit);
        for (int j = 0; j < numValidNeighbor; j++) {
            double edgeTime = LL2double(prop_list2[j * prop_len2 + 1]);
            if (edgeTime >= startTime && edgeTime <= endTime) {
                double amount = LL2double(prop_list2[j * prop_len2]);
                auto it = compAccount2amount.find(neighborList2[j]);
                if (it == compAccount2amount.end())
                    compAccount2amount[neighborList2[j]] = amount;
                else
                    it->second += amount;
            }
        }
    }

    priority_queue<pair<double, long long>, vector<pair<double, long long>>, greater<pair<double, long long>>> pq;
    for (auto &it : compAccount2amount) {
        Node cur(it.first, kvstore, txn);
        pq.push(make_pair(it.second, cur["id"]->data_.Long));
    }
    while (!pq.empty()) {
        auto &p = pq.top();
        new_result->rows_.emplace_back();
        new_result->rows_.back().values_.reserve(2);
        new_result->rows_.back().values_.emplace_back(p.second);
        new_result->rows_.back().values_.emplace_back(p.first);
        cout << p.second << " " << p.first << endl;
        pq.pop();
    }
}
void PProcedure::tsr1(const std::vector<GPStore::Value> &args,
                      std::unique_ptr<PTempResult> &new_result,
                      const KVstore *kvstore, Database *db,
                      std::shared_ptr<Transaction> txn) {
  Node account("Account", "id", &args[0], kvstore, txn);
  if (account.node_id_ != -1) {
    new_result->rows_.emplace_back();
    new_result->rows_.back().values_.reserve(3);
    new_result->rows_.back().values_.emplace_back(*account["createTime"]);
    new_result->rows_.back().values_.emplace_back(*account["isBlocked"]);
    new_result->rows_.back().values_.emplace_back(*account["accountType"]);
  }
}
void PProcedure::tsr2(const std::vector<GPStore::Value> &args,
                      std::unique_ptr<PTempResult> &new_result,
                      const KVstore *kvstore, Database *db,
                      std::shared_ptr<Transaction> txn) {
  Node account("Account", "id", &args[0], kvstore, txn);
  if (account.node_id_ == -1)
    return;
  double start_time = args[1].data_.Long, end_time = args[2].data_.Long;
  TYPE_ENTITY_LITERAL_ID *neighbor_list = nullptr;
  unsigned neighbor_len = 0;
  long long *prop_list = nullptr;
  unsigned prop_len = 0;
  auto calculate_stats = [&]() -> tuple<double, double, long long> {
    double sum_amount = 0.0, max_amount = -1;
    long long transfer_count = 0;
    for (int i = 0; i < neighbor_len; ++i) {
      double prop[2];  // prop[0] is amount, prop[1] is createTime
      memcpy(&prop, prop_list + i * prop_len, sizeof(double) << 1);
      if (prop[1] > start_time && prop[1] < end_time) {
        sum_amount += prop[0];
        max_amount = max(max_amount, prop[0]);
        transfer_count += 1;
      }
    }
    sum_amount = std::round(sum_amount * 1000) / 1000;
    max_amount = std::round(max_amount * 1000) / 1000;
    return {sum_amount, max_amount, transfer_count};
  };
  // transfer-outs
  account.GetLinkedNodesWithEdgeProps("ACCOUNT_TRANSFER_ACCOUNT", neighbor_list,
                                      prop_list, prop_len, neighbor_len,
                                      Util::EDGE_OUT);
  auto res1 = calculate_stats();
  new_result->rows_.emplace_back();
  new_result->rows_.back().values_.reserve(6);
  new_result->rows_.back().values_.emplace_back(std::get<0>(res1));
  new_result->rows_.back().values_.emplace_back(std::get<1>(res1));
  new_result->rows_.back().values_.emplace_back(std::get<2>(res1));
  delete neighbor_list;
  delete prop_list;
  neighbor_len = prop_len = 0;
  // transfer-ins
  account.GetLinkedNodesWithEdgeProps("ACCOUNT_TRANSFER_ACCOUNT", neighbor_list,
                                      prop_list, prop_len, neighbor_len,
                                      Util::EDGE_IN);
  auto res2 = calculate_stats();
  delete neighbor_list;
  delete prop_list;
  new_result->rows_.back().values_.emplace_back(std::get<0>(res2));
  new_result->rows_.back().values_.emplace_back(std::get<1>(res2));
  new_result->rows_.back().values_.emplace_back(std::get<2>(res2));
}
void PProcedure::tsr3(const std::vector<GPStore::Value> &args,
                      std::unique_ptr<PTempResult> &new_result,
                      const KVstore *kvstore, Database *db,
                      std::shared_ptr<Transaction> txn) {
  Node account("Account", "id", &args[0], kvstore, txn);
  if (account.node_id_ == -1)
    return;
  double threshold = args[1].data_.Float, start_time = args[2].data_.Long,
         end_time = args[3].data_.Long;
  TYPE_ENTITY_LITERAL_ID *neighbor_list = nullptr;
  unsigned neighbor_len = 0;
  long long *prop_list = nullptr;
  unsigned prop_len = 0;
  account.GetLinkedNodesWithEdgeProps("ACCOUNT_TRANSFER_ACCOUNT", neighbor_list,
                                      prop_list, prop_len, neighbor_len,
                                      Util::EDGE_IN);
  double valid_neighbor_cnt = 0.0;
  for (int i = 0; i < neighbor_len; ++i) {
    double prop[2];  // prop[0] is amount, prop[1] is createTime
    memcpy(&prop, prop_list + i * prop_len, sizeof(double) << 1);
    if (prop[1] > start_time && prop[1] < end_time && prop[0] > threshold) {
      valid_neighbor_cnt += 1;
    }
  }
  delete neighbor_list;
  delete prop_list;
  double ratio =
      neighbor_len == 0
          ? -1.0
          : std::round(valid_neighbor_cnt / neighbor_len * 1000) / 1000;
  new_result->rows_.emplace_back();
  new_result->rows_.back().values_.emplace_back(ratio);
}
void PProcedure::tsr4(const std::vector<GPStore::Value> &args,
                      std::unique_ptr<PTempResult> &new_result,
                      const KVstore *kvstore, Database *db,
                      std::shared_ptr<Transaction> txn) {
  Node account("Account", "id", &args[0], kvstore, txn);
  if (account.node_id_ == -1)
    return;
  double threshold = args[1].data_.Float, start_time = args[2].data_.Long,
         end_time = args[3].data_.Long;
  TYPE_ENTITY_LITERAL_ID *neighbor_list = nullptr;
  unsigned neighbor_len = 0;
  long long *prop_list = nullptr;
  unsigned prop_len = 0;
  account.GetLinkedNodesWithEdgeProps("ACCOUNT_TRANSFER_ACCOUNT", neighbor_list,
                                      prop_list, prop_len, neighbor_len,
                                      Util::EDGE_OUT);
  std::unordered_map<TYPE_ENTITY_LITERAL_ID, std::pair<int, double>> res_mp;
  for (int i = 0; i < neighbor_len; ++i) {
    double prop[2];  // prop[0] is amount, prop[1] is createTime
    memcpy(&prop, prop_list + i * prop_len, sizeof(double) << 1);
    if (prop[1] > start_time && prop[1] < end_time && prop[0] > threshold) {
      if (res_mp.count(neighbor_list[i]) == 0) {
        res_mp.emplace(neighbor_list[i], std::pair<int, double>(0, 0.0));
      }
      res_mp[neighbor_list[i]].first += 1;
      res_mp[neighbor_list[i]].second += prop[0];
    }
  }
  delete neighbor_list;
  delete prop_list;
  auto cmp = [](const std::tuple<long long, int, double> &a,
                const std::tuple<long long, int, double> &b) {
    if (std::get<2>(a) != std::get<2>(b)) {
      return std::get<2>(a) < std::get<2>(b);
    }
    return std::get<0>(a) > std::get<0>(b);
  };
  std::priority_queue<std::tuple<long long, int, double>,
                      std::vector<std::tuple<long long, int, double>>,
                      decltype(cmp)>
      que(cmp);
  TYPE_PREDICATE_ID local_id_prop_id = kvstore->getpropIDBypropStr("id");
  for (const auto &item : res_mp) {
    GPStore::Value *val_ptr = nullptr;
    assert(
        kvstore->getnodeValueByid(item.first, local_id_prop_id, val_ptr, txn));
    que.emplace(val_ptr->data_.Long, item.second.first,
                std::round(item.second.second * 1000) / 1000);
    delete val_ptr;
  }
  new_result->rows_.reserve(que.size());
  while (!que.empty()) {
    new_result->rows_.emplace_back();
    new_result->rows_.back().values_.reserve(3);
    new_result->rows_.back().values_.emplace_back(std::get<0>(que.top()));
    new_result->rows_.back().values_.emplace_back(std::get<1>(que.top()));
    new_result->rows_.back().values_.emplace_back(std::get<2>(que.top()));
    que.pop();
  }
}
void PProcedure::tsr5(const std::vector<GPStore::Value> &args,
                      std::unique_ptr<PTempResult> &new_result,
                      const KVstore *kvstore, Database *db,
                      std::shared_ptr<Transaction> txn) {
  Node account("Account", "id", &args[0], kvstore, txn);
  if (account.node_id_ == -1)
    return;
  double threshold = args[1].data_.Float, start_time = args[2].data_.Long,
         end_time = args[3].data_.Long;
  TYPE_ENTITY_LITERAL_ID *neighbor_list = nullptr;
  unsigned neighbor_len = 0;
  long long *prop_list = nullptr;
  unsigned prop_len = 0;
  account.GetLinkedNodesWithEdgeProps("ACCOUNT_TRANSFER_ACCOUNT", neighbor_list,
                                      prop_list, prop_len, neighbor_len,
                                      Util::EDGE_IN);
  std::unordered_map<TYPE_ENTITY_LITERAL_ID, std::pair<int, double>> res_mp;
  for (int i = 0; i < neighbor_len; ++i) {
    double prop[2];  // prop[0] is amount, prop[1] is createTime
    memcpy(&prop, prop_list + i * prop_len, sizeof(double) << 1);
    if (prop[1] > start_time && prop[1] < end_time && prop[0] > threshold) {
      if (res_mp.count(neighbor_list[i]) == 0) {
        res_mp.emplace(neighbor_list[i], std::pair<int, double>(0, 0.0));
      }
      res_mp[neighbor_list[i]].first += 1;
      res_mp[neighbor_list[i]].second += prop[0];
    }
  }
  delete neighbor_list;
  delete prop_list;
  auto cmp = [](const std::tuple<long long, int, double> &a,
                const std::tuple<long long, int, double> &b) {
    if (std::get<2>(a) != std::get<2>(b)) {
      return std::get<2>(a) < std::get<2>(b);
    }
    return std::get<0>(a) > std::get<0>(b);
  };
  std::priority_queue<std::tuple<long long, int, double>,
                      std::vector<std::tuple<long long, int, double>>,
                      decltype(cmp)>
      que(cmp);
  TYPE_PREDICATE_ID local_id_prop_id = kvstore->getpropIDBypropStr("id");
  for (const auto &item : res_mp) {
    GPStore::Value *val_ptr = nullptr;
    assert(
        kvstore->getnodeValueByid(item.first, local_id_prop_id, val_ptr, txn));
    que.emplace(val_ptr->data_.Long, item.second.first,
                std::round(item.second.second * 1000) / 1000);
    delete val_ptr;
  }
  new_result->rows_.reserve(que.size());
  while (!que.empty()) {
    new_result->rows_.emplace_back();
    new_result->rows_.back().values_.reserve(3);
    new_result->rows_.back().values_.emplace_back(std::get<0>(que.top()));
    new_result->rows_.back().values_.emplace_back(std::get<1>(que.top()));
    new_result->rows_.back().values_.emplace_back(std::get<2>(que.top()));
    que.pop();
  }
}
void PProcedure::tsr6(const std::vector<GPStore::Value> &args,
                      std::unique_ptr<PTempResult> &new_result,
                      const KVstore *kvstore, Database *db,
                      std::shared_ptr<Transaction> txn) {
  Node account("Account", "id", &args[0], kvstore, txn);
  if (account.node_id_ == -1)
    return;
  double start_time = args[1].data_.Long, end_time = args[2].data_.Long;
  TYPE_PREDICATE_ID local_transfer_pre_id =
      kvstore->getIDByPredicate("ACCOUNT_TRANSFER_ACCOUNT");
  TYPE_ENTITY_LITERAL_ID *neighbor_list = nullptr;
  unsigned neighbor_len = 0;
  long long *prop_list = nullptr;
  unsigned prop_len = 0;
  // one-hop
  std::unordered_set<TYPE_ENTITY_LITERAL_ID> valid_neigh_st;
  account.GetLinkedNodesWithEdgeProps(local_transfer_pre_id, neighbor_list,
                                      prop_list, prop_len, neighbor_len,
                                      Util::EDGE_IN);
  for (int i = 0; i < neighbor_len; ++i) {
    double create_time;
    memcpy(&create_time, prop_list + i * prop_len + 1, sizeof(double));
    if (create_time > start_time && create_time < end_time) {
      valid_neigh_st.insert(neighbor_list[i]);
    }
  }
  delete neighbor_list;
  delete prop_list;
  // two-hop
  std::set<long long> res_id_st;
  std::unordered_set<TYPE_ENTITY_LITERAL_ID> candidate_st;
  for (auto valid_neigh : valid_neigh_st) {
    neighbor_len = prop_len = 0;
    kvstore->getobjIDxvaluesBysubIDpreID(valid_neigh, local_transfer_pre_id,
                                         neighbor_list, prop_list, prop_len,
                                         neighbor_len, false, txn);
    for (int i = 0; i < neighbor_len; ++i) {
      if (candidate_st.count(neighbor_list[i])) {
        continue;
      }
      double create_time;
      memcpy(&create_time, prop_list + i * prop_len + 1, sizeof(double));
      if (create_time > start_time && create_time < end_time) {
        candidate_st.insert(neighbor_list[i]);
        Node candidate_node(neighbor_list[i], kvstore, txn);
        if (candidate_node["isBlocked"]->data_.Boolean) {
          res_id_st.insert(candidate_node["id"]->data_.Long);
        }
      }
    }
    delete neighbor_list;
    delete prop_list;
  }
  new_result->rows_.reserve(res_id_st.size());
  for (auto res_id : res_id_st) {
    new_result->rows_.emplace_back();
    new_result->rows_.back().values_.emplace_back(res_id);
  }
}
void PProcedure::tw1(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
  auto& person_id = args[0];
  auto& person_name = args[1];
  auto& is_blocked = args[2];

  unsigned node_id;
  db->AddNode({kvstore->getIDByLiteral("Person")},
              {
                  kvstore->getpropIDBypropStr("id"), kvstore->getpropIDBypropStr("personName"),
                  kvstore->getpropIDBypropStr("isBlocked"), kvstore->getpropIDBypropStr("createTime"),
                  kvstore->getpropIDBypropStr("gender"), kvstore->getpropIDBypropStr("birthday"),
                  kvstore->getpropIDBypropStr("country"), kvstore->getpropIDBypropStr("city")
              },
              {
                  person_id, person_name, is_blocked,
                  {}, {}, {}, {}, {}
              }, node_id, txn);
}

void PProcedure::tw2(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
  auto& company_id = args[0];
  auto& company_name = args[1];
  auto& is_blocked = args[2];

  unsigned node_id;
  db->AddNode({kvstore->getIDByLiteral("Company")},
              {
                  kvstore->getpropIDBypropStr("id"), kvstore->getpropIDBypropStr("CompanyName"),
                  kvstore->getpropIDBypropStr("isBlocked"), kvstore->getpropIDBypropStr("createTime"),
                  kvstore->getpropIDBypropStr("country"), kvstore->getpropIDBypropStr("city"), kvstore->getpropIDBypropStr("business"),
                  kvstore->getpropIDBypropStr("description"), kvstore->getpropIDBypropStr("url")
              },
              {
                  company_id, company_name, is_blocked,
                  {}, {}, {}, {}, {}, {}
              }, node_id, txn);
}

void PProcedure::tw3(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
  auto& medium_id = args[0];
  auto& medium_type = args[1];
  auto& is_blocked = args[2];

  unsigned node_id;
  db->AddNode({kvstore->getIDByLiteral("Medium")},
              {
                  kvstore->getpropIDBypropStr("id"), kvstore->getpropIDBypropStr("mediumType"),
                  kvstore->getpropIDBypropStr("isBlocked"),kvstore->getpropIDBypropStr("createTime"),
                  kvstore->getpropIDBypropStr("lastLoginTime"), kvstore->getpropIDBypropStr("riskLevel")

              },
              {
                  medium_id, medium_type,is_blocked,
                  {}, {}, {}
              }, node_id, txn);
}

void PProcedure::tw4(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
  auto& person_id = args[0];
  auto& account_id = args[1];
  auto& time = args[2];
  auto& account_blocked = args[3];
  auto& account_type = args[4];

  unsigned node_id;
  db->AddNode({kvstore->getIDByLiteral("Account")},
              {
                  kvstore->getpropIDBypropStr("id"), kvstore->getpropIDBypropStr("createTime"),
                  kvstore->getpropIDBypropStr("isBlocked"), kvstore->getpropIDBypropStr("accountType"),
                  kvstore->getpropIDBypropStr("nickname"), kvstore->getpropIDBypropStr("phonenum"),
                  kvstore->getpropIDBypropStr("email"), kvstore->getpropIDBypropStr("freqLoginType"),
                  kvstore->getpropIDBypropStr("lastLoginTime"), kvstore->getpropIDBypropStr("accountLevel")
              },
              {
                  account_id, time, account_blocked, account_type,
                  {}, {}, {}, {}, {}, {}
              }, node_id, txn);
  Node person_node("Person", "id", &person_id, kvstore, txn);
  assert(person_node.node_id_ != -1);
  db->AddEdge(person_node.node_id_, node_id, kvstore->getIDByPredicate("PERSON_OWN_ACCOUNT"), {}, txn);
}

void PProcedure::tw5(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
  auto& company_id = args[0];
  auto& account_id = args[1];
  auto& time = args[2];
  auto& account_blocked = args[3];
  auto& account_type = args[4];

  unsigned node_id;
  db->AddNode({kvstore->getIDByLiteral("Account")},
              {
                  kvstore->getpropIDBypropStr("id"), kvstore->getpropIDBypropStr("createTime"),
                  kvstore->getpropIDBypropStr("isBlocked"), kvstore->getpropIDBypropStr("accountType"),
                  kvstore->getpropIDBypropStr("nickname"), kvstore->getpropIDBypropStr("phonenum"),
                  kvstore->getpropIDBypropStr("email"), kvstore->getpropIDBypropStr("freqLoginType"),
                  kvstore->getpropIDBypropStr("lastLoginTime"), kvstore->getpropIDBypropStr("accountLevel")
              },
              {
                  account_id, time, account_blocked, account_type,
                  {}, {}, {}, {}, {}, {}
              }, node_id, txn);
  Node company_node("Company", "id", &company_id, kvstore, txn);
  assert(company_node.node_id_ != -1);
  db->AddEdge(company_node.node_id_, node_id, kvstore->getIDByPredicate("COMPANY_OWN_ACCOUNT"), {}, txn);
}

void PProcedure::tw6(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
  auto& person_id = args[0];
  auto& loan_id = args[1];
  auto& loan_amount = args[2];
  auto& balance = args[3];
  auto& time = args[4];

  unsigned node_id;
  db->AddNode({kvstore->getIDByLiteral("Loan")},
              {
                  kvstore->getpropIDBypropStr("id"), kvstore->getpropIDBypropStr("loanAmount"),
                  kvstore->getpropIDBypropStr("balance"), kvstore->getpropIDBypropStr("createTime"),
                  kvstore->getpropIDBypropStr("loanUsage"), kvstore->getpropIDBypropStr("interestRate")
              },
              {
                  loan_id, loan_amount, balance, {}, {}, {}
              }, node_id, txn);
  Node person_node("Person", "id", &person_id, kvstore, txn);
  assert(person_node.node_id_ != -1);
  db->AddEdge(person_node.node_id_, node_id, kvstore->getIDByPredicate("PERSON_APPLY_LOAN"), {time.data_.Long}, txn);
}

void PProcedure::tw7(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
    auto& company_id = args[0];
    auto& loan_id = args[1];
    auto& loan_amount = args[2];
    auto& balance = args[3];
    auto& time = args[4];

    unsigned node_id;
    db->AddNode({kvstore->getIDByLiteral("Loan")},
                {
                    kvstore->getpropIDBypropStr("id"), kvstore->getpropIDBypropStr("loanAmount"),
                    kvstore->getpropIDBypropStr("balance"), kvstore->getpropIDBypropStr("createTime"),
                    kvstore->getpropIDBypropStr("loanUsage"), kvstore->getpropIDBypropStr("interestRate")
                },
                {
                    loan_id, loan_amount, balance, {}, {}, {}
                }, node_id, txn);

    Node company_node("Company", "id", &company_id, kvstore, txn);
    assert(company_node.node_id_ != -1);
    db->AddEdge(company_node.node_id_, node_id, kvstore->getIDByPredicate("COMPANY_APPLY_LOAN"), {time.data_.Long}, txn);
}
void PProcedure::tw8(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
    auto& person_id = args[0];
    auto& company_id = args[1];
    auto& time = args[2];
    auto& ratio = args[3];

    Node person_node("Person", "id", &person_id, kvstore, txn);
    Node company_node("Company", "id", &company_id, kvstore, txn);
    assert(person_node.node_id_ != -1);
    assert(company_node.node_id_ != -1);

    long long ratio_ll;
    memcpy(&ratio_ll, &(ratio.data_.Float), sizeof(long long));

    db->AddEdge(person_node.node_id_, company_node.node_id_, kvstore->getIDByPredicate("PERSON_INVEST_COMPANY"),
                {time.data_.Long, ratio_ll}, txn);

}
void PProcedure::tw9(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
    auto& company_id1 = args[0];
    auto& company_id2 = args[1];
    auto& time = args[2];
    auto& ratio = args[3];

    Node company_node1("Company", "id", &company_id1, kvstore, txn);
    Node company_node2("Company", "id", &company_id2, kvstore, txn);
    assert(company_node1.node_id_ != -1);
    assert(company_node2.node_id_ != -1);

    long long ratio_ll;
    memcpy(&ratio_ll, &(ratio.data_.Float), sizeof(long long));

    db->AddEdge(company_node1.node_id_, company_node2.node_id_, kvstore->getIDByPredicate("COMPANY_INVEST_COMPANY"),
                {time.data_.Long, ratio_ll}, txn);

}
void PProcedure::tw10(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
    auto& person_id1 = args[0];
    auto& person_id2 = args[1];
    auto& time = args[2];

    Node person_node1("Person", "id", &person_id1, kvstore, txn);
    Node person_node2("Person", "id", &person_id2, kvstore, txn);
    assert(person_node1.node_id_ != -1);
    assert(person_node2.node_id_ != -1);

    db->AddEdge(person_node1.node_id_, person_node2.node_id_, kvstore->getIDByPredicate("PERSON_GUARANTEE_PERSON"),
                {time.data_.Long}, txn);

}
void PProcedure::tw11(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
    auto& company_id1 = args[0];
    auto& company_id2 = args[1];
    auto& time = args[2];

    Node company_node1("Company", "id", &company_id1, kvstore, txn);
    Node company_node2("Company", "id", &company_id2, kvstore, txn);
    assert(company_node1.node_id_ != -1);
    assert(company_node2.node_id_ != -1);

    db->AddEdge(company_node1.node_id_, company_node2.node_id_, kvstore->getIDByPredicate("COMPANY_GUARANTEE_COMPANY"),
                {time.data_.Long}, txn);

}
void PProcedure::tw12(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
    auto& account_id1 = args[0];
    auto& account_id2 = args[1];
    auto& time = args[2];
    auto& amount = args[3];

    Node account_node1("Account", "id", &account_id1, kvstore, txn);
    Node account_node2("Account", "id", &account_id2, kvstore, txn);
    assert(account_node1.node_id_ != -1);
    assert(account_node2.node_id_ != -1);

    long long ratio_ll;
    memcpy(&ratio_ll, &(amount.data_.Float), sizeof(long long));

    db->AddEdge(account_node1.node_id_, account_node2.node_id_, kvstore->getIDByPredicate("ACCOUNT_TRANSFER_ACCOUNT"),
                {time.data_.Long, ratio_ll}, txn);

}
void PProcedure::tw13(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
  Node account_1("Account", "id", &args[0], kvstore, txn);
  Node account_2("Account", "id", &args[1], kvstore, txn);
  assert(account_1.node_id_ != -1);
  assert(account_2.node_id_ != -1);
  TYPE_PREDICATE_ID withdraw_pre_id = kvstore->getIDByPredicate("ACCOUNT_WITHDRAW_ACCOUNT");
  db->AddEdge(account_1.node_id_, account_2.node_id_, withdraw_pre_id, {args[3].data_.Long, args[2].data_.Long}, txn);
}
void PProcedure::tw14(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
  Node account("Account", "id", &args[0], kvstore, txn);
  Node loan("Loan", "id", &args[1], kvstore, txn);
  assert(account.node_id_ != -1);
  assert(loan.node_id_ != -1);
  TYPE_PREDICATE_ID repay_pre_id = kvstore->getIDByPredicate("ACCOUNT_REPAY_LOAN");
  db->AddEdge(account.node_id_, loan.node_id_, repay_pre_id, {args[3].data_.Long, args[2].data_.Long}, txn);
}
void PProcedure::tw15(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
  Node loan("Loan", "id", &args[0], kvstore, txn);
  Node account("Account", "id", &args[1], kvstore, txn);
  assert(loan.node_id_ != -1);
  assert(account.node_id_ != -1);
  TYPE_PREDICATE_ID deposit_pre_id = kvstore->getIDByPredicate("LOAN_DEPOSIT_ACCOUNT");
  db->AddEdge(loan.node_id_, account.node_id_, deposit_pre_id, {args[3].data_.Long, args[2].data_.Long}, txn);
}
void PProcedure::tw16(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
  Node medium("Medium", "id", &args[0], kvstore, txn);
  Node account("Account", "id", &args[1], kvstore, txn);
  assert(medium.node_id_ != -1);
  assert(account.node_id_ != -1);
  TYPE_PREDICATE_ID signIn_pre_id = kvstore->getIDByPredicate("MEDIUM_SIGN_INACCOUNT");
  db->AddEdge(medium.node_id_, account.node_id_, signIn_pre_id, {args[2].data_.Long}, txn);
}

/**
 * @brief Given an id, remove the Account, and remove the related edges
 * including own, transfer, withdraw, repay, deposit, signIn. Remove the
 * connected Loan vertex in cascade.
 */
void PProcedure::tw17(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
    auto account_id = args[0];
    Node account_node("Account", "id", &account_id, kvstore, txn);
    if (account_node.node_id_ == -1)
        return;
    // remove edge person (or company) -own-> account
    unsigned *node_list, len;
    account_node.GetLinkedNodes("PERSON_OWN_ACCOUNT", node_list, len,
                                Util::EDGE_IN);
    if (!len) {
      account_node.GetLinkedNodes("COMPANY_OWN_ACCOUNT", node_list, len,
                                  Util::EDGE_IN);
      assert(len == 1);  // only one person or company owns the account
      auto node_id = node_list[0];
      db->EraseEdge(node_id, account_node.node_id_, Util::EDGE_OUT,
                    "COMPANY_OWN_ACCOUNT", txn);
    }
    else {
        assert(len == 1);  // only one person or company owns the account
        auto node_id = node_list[0];
        db->EraseEdge(node_id, account_node.node_id_, Util::EDGE_OUT,
                      "PERSON_OWN_ACCOUNT", txn);
    }

    // remove edge account <-transfer, withdraw-> account
    std::vector<std::string> edge_labels = {"ACCOUNT_TRANSFER_ACCOUNT",
                                            "ACCOUNT_WITHDRAW_ACCOUNT"};
    std::vector<char> edge_directions = {Util::EDGE_OUT, Util::EDGE_IN};
    for (auto edge_label: edge_labels) {
        for (int d = 0; d < 2; ++d) {
            account_node.GetLinkedNodes(edge_label, node_list, len,
                                        edge_directions[d]);
            for (int i = 0; i < len; ++i) {
                db->EraseEdge(account_node.node_id_, node_list[i],
                            edge_directions[d], edge_label, txn);
            }
        }
    }

    // remove edge Medium -signIn-> account
    account_node.GetLinkedNodes("MEDIUM_SIGN_INACCOUNT", node_list, len,
                                Util::EDGE_IN);
    for (int i = 0; i < len; ++i) {
        db->EraseEdge(node_list[i], account_node.node_id_, Util::EDGE_OUT,
                    "MEDIUM_SIGN_INACCOUNT", txn);
    }

    // remove loan
    std::set<int> loan_set;
    account_node.GetLinkedNodes("ACCOUNT_REPAY_LOAN", node_list, len,
                                Util::EDGE_OUT);
    for (int i = 0; i < len; ++i) {
        loan_set.insert(node_list[i]);
        db->EraseEdge(account_node.node_id_, node_list[i], Util::EDGE_OUT,
                    "ACCOUNT_REPAY_LOAN", txn);
    }
    account_node.GetLinkedNodes("LOAN_DEPOSIT_ACCOUNT", node_list, len,
                                Util::EDGE_IN);
    for (int i = 0; i < len; ++i) {
        loan_set.insert(node_list[i]);
        db->EraseEdge(node_list[i], account_node.node_id_, Util::EDGE_OUT,
                    "LOAN_DEPOSIT_ACCOUNT", txn);
    }
    for (auto load_node_id: loan_set) {
        Node load_node(load_node_id, kvstore, txn);
        node_list = nullptr;
        len = 0;
        load_node.GetLinkedNodes("PERSON_APPLY_LOAN", node_list, len,
                                Util::EDGE_IN);
        if (!len) {
            load_node.GetLinkedNodes("COMPANY_APPLY_LOAN", node_list, len,
                                    Util::EDGE_IN);
            assert(len == 1);  // only one person or company owns the account
            auto node_id = node_list[0];
            db->EraseEdge(node_id, load_node.node_id_, Util::EDGE_OUT,
                        "COMPANY_APPLY_LOAN", txn);
        }
        else {
            assert(len == 1);  // only one person or company owns the account
            auto node_id = node_list[0];
            db->EraseEdge(node_id, load_node.node_id_, Util::EDGE_OUT,
                        "PERSON_APPLY_LOAN", txn);
        }
        db->EraseNode(load_node_id, txn);
    }

    // remove account
    db->EraseNode(account_node.node_id_, txn);
}

void PProcedure::tw18(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
    auto account_id= args[0];
    Node account_node("Account", "id", &account_id, kvstore, txn); // @todo transform "Account" to account_label_id
    if (account_node.node_id_ == -1)
        return;
    auto isBlocked_prop_id = kvstore->getpropIDBypropStr("isBlocked");
    auto value_true = GPStore::Value(true);
    db->getKVstore()->setnodeValueByid(account_node.node_id_, isBlocked_prop_id, &value_true, txn);
}

void PProcedure::tw19(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
    auto person_id = args[0];
    Node person_node(person_label_id, id_prop_id, &person_id, kvstore, txn);
    if (person_node.node_id_ == -1)
        return;
    auto isBlocked_prop_id = kvstore->getpropIDBypropStr("isBlocked");
    auto value_true = GPStore::Value(true);
    db->getKVstore()->setnodeValueByid(person_node.node_id_, isBlocked_prop_id, &value_true, txn);
}

void PProcedure::trw1(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
  Node src("Account", "id", &args[0], kvstore, txn);
  Node dst("Account", "id", &args[1], kvstore, txn);
  if (src.node_id_ == -1 || dst.node_id_ == -1)
    return;
  if (src["isBlocked"]->data_.Boolean || dst["isBlocked"]->data_.Boolean)
    return;

  TYPE_ENTITY_LITERAL_ID* in_neighborList = nullptr; unsigned in_neighbor_len = 0;
  long long* in_prop_list = nullptr; unsigned in_prop_len = 0;
  dst.GetLinkedNodesWithEdgeProps("ACCOUNT_TRANSFER_ACCOUNT", in_neighborList,
                                  in_prop_list, in_prop_len, in_neighbor_len, Util::EDGE_OUT);

  TYPE_ENTITY_LITERAL_ID* out_neighborList = nullptr; unsigned out_neighbor_len = 0;
  long long* out_prop_list = nullptr; unsigned out_prop_len = 0;
  src.GetLinkedNodesWithEdgeProps("ACCOUNT_TRANSFER_ACCOUNT", out_neighborList,
                                  out_prop_list, out_prop_len, out_neighbor_len, Util::EDGE_IN);


  bool has_same_account = false;
  for(unsigned i = 0; i < in_neighbor_len; ++i) {
    double edge_time_in = GPStore::Value(in_prop_list[i * in_prop_len + 1]).data_.Float;
    if(edge_time_in <= args[4].data_.Long || edge_time_in >= args[5].data_.Long) continue;
    for(unsigned j = 0; j < out_neighbor_len; ++j) {
      double edge_time_out = GPStore::Value(out_prop_list[j * out_prop_len + 1]).data_.Float;
      if(edge_time_out > args[4].data_.Long && edge_time_out < args[5].data_.Long &&
         in_prop_list[i] == out_prop_list[j]) {
        has_same_account = true;
        break;
      }
    }

    if(has_same_account) break;
  }

  if(has_same_account) {
    auto value_true = GPStore::Value(true);
    db->getKVstore()->setnodeValueByid(src.node_id_, kvstore->getpropIDBypropStr("isBlocked"),
                                       &value_true, txn);
    db->getKVstore()->setnodeValueByid(dst.node_id_, kvstore->getpropIDBypropStr("isBlocked"),
                                       &value_true, txn);
    return;
  }

  db->AddEdge(src.node_id_, dst.node_id_, kvstore->getIDByPredicate("ACCOUNT_TRANSFER_ACCOUNT"),
              {args[4].data_.Long, args[3].data_.Long, 0}, txn);
}
void PProcedure::trw2(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
  Node src("Account", "id", &args[0], kvstore, txn);
  Node dst("Account", "id", &args[1], kvstore, txn);
  if (src.node_id_ == -1 || dst.node_id_ == -1)
    return;
  if (src["isBlocked"]->data_.Boolean || dst["isBlocked"]->data_.Boolean)
    return;

  double start_time = args[5].data_.Long, end_time = args[6].data_.Long;
  double amountThreshold = args[4].data_.Float;
  int truncationLimit = args[8].data_.Int;
  TruncationOrderType truncationOrder = static_cast<TruncationOrderType>(args[9].data_.Int);
  bool asc = true; int prop_idx = -1;
  if (truncationOrder == TruncationOrderType::TIMESTAMP_ASCENDING) {
      asc = true;
      prop_idx = 1;
    } else if (truncationOrder == TruncationOrderType::TIMESTAMP_DESCENDING) {
      asc = false;
      prop_idx = 1;
    } else if (truncationOrder == TruncationOrderType::AMOUNT_ASCENDING) {
      asc = true;
      prop_idx = 0;
    } else if (truncationOrder == TruncationOrderType::AMOUNT_DESCENDING) {
      asc = false;
      prop_idx = 0;
    }


  TYPE_ENTITY_LITERAL_ID* in_neighborList = nullptr; unsigned in_neighbor_len = 0;
  long long* in_prop_list = nullptr; unsigned in_prop_len = 0;
  dst.GetLinkedNodesWithEdgeProps("ACCOUNT_TRANSFER_ACCOUNT", in_neighborList,
                                  in_prop_list, in_prop_len, in_neighbor_len, Util::EDGE_OUT);
  int in_numValidNeighbor = truncationReorder(in_neighborList, in_neighbor_len,
                                              in_prop_list, in_prop_len, prop_idx, truncationLimit, asc);


  TYPE_ENTITY_LITERAL_ID* out_neighborList = nullptr; unsigned out_neighbor_len = 0;
  long long* out_prop_list = nullptr; unsigned out_prop_len = 0;
  dst.GetLinkedNodesWithEdgeProps("ACCOUNT_TRANSFER_ACCOUNT", out_neighborList,
                                  out_prop_list, out_prop_len, out_neighbor_len, Util::EDGE_IN);
  int out_numValidNeighbor = truncationReorder(out_neighborList, out_neighbor_len,
                                               out_prop_list, out_prop_len, prop_idx, truncationLimit, asc);

  double in_sum = 0, out_sum = 0;

  for(unsigned i = 0; i < in_numValidNeighbor; ++i) {
    double edge_time_in = GPStore::Value(in_prop_list[i * in_prop_len + 1]).data_.Float;
    double amount_in = GPStore::Value(in_prop_list[i * in_prop_len]).data_.Float;
    if (edge_time_in > start_time && edge_time_in < end_time &&
        amount_in > amountThreshold) {
      in_sum += GPStore::Value(in_prop_list[i * in_prop_len]).data_.Float;
    }
  }
  for(unsigned j = 0; j < out_numValidNeighbor; ++j) {
    double edge_time_out = GPStore::Value(out_prop_list[j * out_prop_len + 1]).data_.Float;
    double amount_out = GPStore::Value(out_prop_list[j * out_prop_len]).data_.Float;
    if (edge_time_out > start_time && edge_time_out < end_time &&
        amount_out > amountThreshold) {
      out_sum += GPStore::Value(out_prop_list[j * out_prop_len]).data_.Float;
    }
  }

  if(in_sum / out_sum <= args[7].data_.Float) {
    auto value_true = GPStore::Value(true);
    db->getKVstore()->setnodeValueByid(src.node_id_, kvstore->getpropIDBypropStr("isBlocked"),
                                       &value_true, txn);
    db->getKVstore()->setnodeValueByid(dst.node_id_, kvstore->getpropIDBypropStr("isBlocked"),
                                       &value_true, txn);
    return;
  }

  db->AddEdge(src.node_id_, dst.node_id_, kvstore->getIDByPredicate("ACCOUNT_TRANSFER_ACCOUNT"),
              {args[3].data_.Long, args[2].data_.Long, 0}, txn);
}

void PProcedure::trw3(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
  Node src("Person", "id", &args[0], kvstore, txn);
  Node dst("Person", "id", &args[1], kvstore, txn);
  if (src.node_id_ == -1 || dst.node_id_ == -1)
    return;
  if (src["isBlocked"]->data_.Boolean || dst["isBlocked"]->data_.Boolean)
    return;

  double start_time = args[4].data_.Long, end_time = args[5].data_.Long;
  int truncationLimit = args[6].data_.Int;
  TruncationOrderType truncationOrder = static_cast<TruncationOrderType>(args[7].data_.Int);

  bool asc = true; int prop_idx = -1;
  if (truncationOrder == TruncationOrderType::TIMESTAMP_ASCENDING) {
    asc = true;
  } else if (truncationOrder == TruncationOrderType::TIMESTAMP_DESCENDING) {
    asc = false;
  } else if (truncationOrder == TruncationOrderType::AMOUNT_ASCENDING)
    assert(false);


  auto sumLoanAmount = 0;
  std::unordered_set<TYPE_ENTITY_LITERAL_ID> dst_set, src_set{src.node_id_}, visited;
  while(!src_set.empty()) {
    for(auto vid : src_set) {
      auto v = GPStore::Value((int)vid);
      Node cur("Person", "id", &v, kvstore, txn);
      TYPE_ENTITY_LITERAL_ID *neighborList = nullptr;  unsigned neighbor_len = 0;
      long long *prop_list = nullptr;  unsigned prop_len = 0;
      cur.GetLinkedNodesWithEdgeProps("PERSON_GUARANTEE_PERSON", neighborList, prop_list, prop_len, neighbor_len, Util::EDGE_OUT);
      int numValidNeighbor = truncationReorder(neighborList, neighbor_len, prop_list, prop_len, 0, truncationLimit, asc);

      for(int i = 0; i < numValidNeighbor; ++i) {
        double edge_time = GPStore::Value(prop_list[i * prop_len + 1]).data_.Float;
        if(edge_time > start_time && edge_time < end_time &&
           visited.find(neighborList[i]) == visited.end()) {
          visited.insert(neighborList[i]);
          dst_set.insert(neighborList[i]);
        }
      }
    }

    swap(src_set, dst_set);
    dst_set.clear();
  }

  for(auto vid : visited) {
    auto v = GPStore::Value((int)vid);
    Node cur("Person", "id", &v, kvstore, txn);
    TYPE_ENTITY_LITERAL_ID *neighborList = nullptr;  unsigned neighbor_len = 0;
    long long *prop_list = nullptr;  unsigned prop_len = 0;
    cur.GetLinkedNodesWithEdgeProps("PERSON_APPLY_LOAN", neighborList, prop_list, prop_len, neighbor_len, Util::EDGE_OUT);
    int numValidNeighbor = truncationReorder(neighborList, neighbor_len, prop_list, prop_len, 1, truncationLimit, asc);

    for(int i = 0; i < numValidNeighbor; ++i) {
      sumLoanAmount += GPStore::Value(prop_list[i * prop_len]).data_.Float;
      if(sumLoanAmount > args[3].data_.Float) {
        break;
      }
    }
  }


  if(sumLoanAmount > args[3].data_.Float) {
    auto value_true = GPStore::Value(true);
    db->getKVstore()->setnodeValueByid(src.node_id_, kvstore->getpropIDBypropStr("isBlocked"),
                                       &value_true, txn);
    db->getKVstore()->setnodeValueByid(dst.node_id_, kvstore->getpropIDBypropStr("isBlocked"),
                                       &value_true, txn);
    return;
  }

  db->AddEdge(src.node_id_, dst.node_id_, kvstore->getIDByPredicate("PERSON_GUARANTEE_PERSON"),
              {args[2].data_.Long}, txn);
}