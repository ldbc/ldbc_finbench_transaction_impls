#include "../PQuery/PProcedure.h"
using namespace std;

extern "C" void tcr10(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
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