#include "../PQuery/PProcedure.h"
using namespace std;

extern "C" void tcr9(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
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
        double edgeTime = prop_list[i * prop_len + 1];
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
        double edgeTime = prop_list[i * prop_len + 1];
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
        double edgeTime = prop_list[i * prop_len + 1];
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
        double edgeTime = prop_list[i * prop_len + 1];
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
    // Round to 3 decimal places
    ratioRepay = float(std::round(1000. * ratioRepay)) / 1000.;
    ratioDeposit = float(std::round(1000. * ratioDeposit)) / 1000.;
    ratioTransfer = float(std::round(1000. * ratioTransfer)) / 1000.;
    new_result->rows_.emplace_back();
    new_result->rows_.back().values_.reserve(3);
    new_result->rows_.back().values_.emplace_back(ratioRepay);
    new_result->rows_.back().values_.emplace_back(ratioDeposit);
    new_result->rows_.back().values_.emplace_back(ratioTransfer);
    cout << ratioRepay << " " << ratioDeposit << " " << ratioTransfer << endl;
}