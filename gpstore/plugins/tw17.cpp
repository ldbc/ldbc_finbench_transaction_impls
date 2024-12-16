#include "../PQuery/PProcedure.h"
using namespace std;

extern "C" void tw17(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
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