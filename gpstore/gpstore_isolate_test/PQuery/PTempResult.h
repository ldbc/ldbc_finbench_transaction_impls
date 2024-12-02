#ifndef PQUERY_PTEMPRESULT_H
#define PQUERY_PTEMPRESULT_H

#include <vector>
#include <map>
#include <string>
#include <unordered_map>
#include "../Value/Expression.h"
#include "../Util/Value.h"
#include "PVarset.h"
#include "../Util/Util.h"
// #include "../Value/PCalculator.h"
#include "../KVstore/KVstore.h"

class PGeneralEvaluation;

/**
 * @brief A temporary result set for a query engine.
 *
 * This class represents a temporary result set for a query engine. It contains a header and a list of records.
 * The header stores information about the columns, such as variable sets and column IDs.
 * The records store the actual data.
 *
 * The class provides various methods for manipulating the result set, such as sorting, joining, and projecting.
 * It also provides methods for evaluating aggregation expressions and generating rows.
 *
 */
class PTempResult {
public:
    /**
     * @brief Represents a header for a temporary result set, containing information about columns, variable sets, and column IDs.
     *
     * This class provides a way to manage the metadata of a temporary result set, including the variable sets, column IDs, and variable names.
     */
    class Header{
    public:
        PVarset<unsigned> spo_var_; // spo variable set 结点变量集
        PVarset<unsigned> edge_var_;  // edge variable set 边变量集
        PVarset<unsigned> other_var_; // other variable set 其他变量集
        PVarset<std::pair<unsigned, TYPE_PROPERTY_ID>> prop_;   // property variable set 属性变量集
        // Varset视作无序集合，不保证与Record的列对应，需要额外维护map记录对应关系
        std::map<unsigned, unsigned> spo_var_id2col_; // spo variable id to column id map 结点变量id到列id映射
        std::map<unsigned, unsigned> edge_var_id2col_;  // edge variable id to column id map 边变量id到列id映射
        std::map<unsigned, unsigned> value_var_id2col_; // value variable id to column id map 其他变量id到列id映射
        std::map<std::pair<unsigned,TYPE_PROPERTY_ID >, unsigned > var_propid2col_; // property variable id to column id map 属性变量到列id映射

        std::shared_ptr<std::vector<std::string>> id2var_name_;     // variable id to variable name map 变量id到变量名映射

        Header() = default;
        Header(const Header & that);
        Header& operator=(const Header & that);
        ~Header() = default;
        void clear();
        void swap(Header& that);
        void initColumnInfoByVarset();
        void setVarset(const std::vector<unsigned > & spo_var, const std::vector<unsigned > & edge_var,
                       const std::vector<unsigned > & other_var,const std::vector<std::pair<unsigned,unsigned>> & prop);
    };
    /**
     * @brief Represents a record in a temporary result set.
     *
     * This class provides a way to store the data of a single row in a temporary result set.
    */
    class Record{
    public:
        std::vector<TYPE_ENTITY_LITERAL_ID > spo_id_; // spo id list 结点id列表
        std::vector<TYPE_EDGE_ID> edge_id_; // edge id list 边id列表
        std::vector<GPStore::Value> values_;  // value list 值列表
        Record() = default;
        Record(const Record& that);
        Record& operator=(const Record& that);
        ~Record() = default;
        void swap(Record& that);
    };

    std::vector<Record> rows_;  // record list 记录列表
    Header head_; // head 表头

    PTempResult() = default;
    PTempResult(const PTempResult& that);
    PTempResult& operator=(const PTempResult& that);
    ~PTempResult() = default;

    unsigned getSize() const;
    PVarset<unsigned > getAllVarset() const;
    void clear();
    void swap(PTempResult &that);
    void print(KVstore * _kvstore = nullptr) const;

    static int compareRow(const Record& x, const std::vector<unsigned > & x_cols,
                          const Record& y, const std::vector<unsigned > & y_cols,
                          const std::vector<bool> & asc);

    static int compareRow(const Record& x, const std::vector<unsigned > & x_cols,
                          const Record& y, const std::vector<unsigned > & y_cols);

    void sort(int l, int r, const std::vector<unsigned> & this_cols);
    void stableSort(int l, int r, const std::vector<unsigned> & this_cols);

    void sort(int l, int r, const std::vector<const GPStore::Expression *> & exps,
              const std::vector<bool> &asc, KVstore * _kvstore,
              const std::unordered_map<std::string, GPStore::Value> * params, const PGeneralEvaluation *general_evaluation = nullptr);

    void sort(int l, int r, function<bool(const Record&, const Record&)> compare_func);
    void sort(int l, int r, const std::vector<unsigned> & this_cols, const std::vector<bool> &asc);


    int findLeftBounder(const Record& x, const std::vector<unsigned >& x_cols, const std::vector<unsigned >& this_cols) const;
    int findRightBounder(const Record& x, const std::vector<unsigned >& x_cols, const std::vector<unsigned >& this_cols) const;

    void extend(const GPStore::Expression * exp, KVstore * _kvstore, const std::unordered_map<std::string, GPStore::Value> * params, const PGeneralEvaluation *general_evaluation = nullptr);

    std::vector<unsigned > projectionView(const std::vector<const GPStore::Expression *> & exps, KVstore * _kvstore,
                                          const std::unordered_map<std::string, GPStore::Value> * params, const PGeneralEvaluation *general_evaluation = nullptr);

    void doJoin(PTempResult &x, PTempResult &r, const std::vector<unsigned >& x_cols, const std::vector<unsigned >& this_cols);
    void doJoinWithFilter(PTempResult &x, PTempResult  &r, const std::vector<unsigned >& x_cols, const std::vector<unsigned >& this_cols,
                          const GPStore::Expression * filter, KVstore * _kvstore,
                          const std::unordered_map<std::string, GPStore::Value> * params,
                          const PGeneralEvaluation *general_evaluation = nullptr);

    void doCartesianProduct(PTempResult &x, PTempResult &r);
    void doUnion(PTempResult &r);
    void doEqualUnion(PTempResult &x, PTempResult &r);
    void doLeftOuterJoin(PTempResult &x, PTempResult &r, const std::vector<unsigned >& x_cols,
                         const std::vector<unsigned >& this_cols);
    void doFilter(PTempResult &r, const GPStore::Expression * filter, KVstore * _kvstore,
                  const std::unordered_map<std::string, GPStore::Value> * params, const PGeneralEvaluation *general_evaluation = nullptr);
    void doProjection(const std::vector<const GPStore::Expression *> & proj_exps, const std::vector<unsigned > & alias,
                      const std::vector<unsigned > & keep_vars,
                      KVstore * _kvstore, const std::unordered_map<std::string, GPStore::Value> * params, 
                      const PGeneralEvaluation *general_evaluation = nullptr);
    void doDistinct(PTempResult &r, const std::vector<unsigned > & dis_cols, const std::vector<unsigned > already_order);
    void doUnwind(PTempResult &r, const GPStore::Expression * exp,  unsigned var_id, KVstore * _kvstore,
                  const std::unordered_map<std::string, GPStore::Value> * params, const PGeneralEvaluation *general_evaluation = nullptr);
    std::vector<unsigned > doGroupBy(const std::vector<unsigned > & grouping_keys);
    void doAggregation(PTempResult &r, const std::vector<unsigned > & groups, const std::vector<const GPStore::Expression *> & aggr_val,
                       std::vector<unsigned > aggr_id,  KVstore * _kvstore,
                       const std::unordered_map<std::string, GPStore::Value> * params, const PGeneralEvaluation *general_evaluation = nullptr);
    void doLimit(PTempResult &r, unsigned limit = INVALID, unsigned skip = 0);


    static std::vector<int> mapTo(const PVarset<unsigned > &from, const std::map<unsigned ,unsigned > & id2col);
    static std::vector<int> mapTo(const PVarset<std::pair<unsigned ,unsigned >> &from,
                                  const std::map<std::pair<unsigned ,unsigned > ,unsigned > & id2col);
    static void generateRow(Record &r, Record & x, Record & y,
                            const std::vector<int> & r_spo2x_col, const std::vector<int> & r_spo2y_col,
                            const std::vector<int> & r_edge2x_col, const std::vector<int> & r_edge2y_col,
                            const std::vector<int> & r_val2x_col, const std::vector<int> & r_val2y_col,
                            const std::vector<int> & r_prop2x_col,const std::vector<int> &  r_prop2y_col);

    static void generateRow(Record &r, Record & x,
                            const std::vector<int> & r_spo2x_col,
                            const std::vector<int> & r_edge2x_col,
                            const std::vector<int> & r_val2x_col,
                            const std::vector<int> & r_prop2x_col);

    static bool evaluateTwoTrunc(Record &x, Record &y, const GPStore::Expression *filter, KVstore * _kvstore,
                                const std::unordered_map<std::string, GPStore::Value> * params,
                                const std::vector<int> & f_spo2x_col, const std::vector<int> & f_spo2y_col,
                                const std::vector<int> & f_edge2x_col, const std::vector<int> & f_edge2y_col,
                                const std::vector<int> & f_val2x_col, const std::vector<int> & f_val2y_col,
                                const std::vector<int> & f_prop2x_col,const std::vector<int> &  f_prop2y_col,
                                const PGeneralEvaluation *general_evaluation = nullptr);

    std::string toJSON() const;
private:
    void copyColumn(unsigned col_id, unsigned part_num);

    void
    evaluateAggregationExpression(GPStore::Expression *exp, int _begin, int _end, KVstore * _kvstore,
                                  const std::unordered_map<std::string, GPStore::Value> * params,
                                  const PGeneralEvaluation *general_evaluation = nullptr);

    void
    evaluateAggregationExpression(GPStore::Atom *atom, int _begin, int _end, KVstore * _kvstore,
                                  const std::unordered_map<std::string, GPStore::Value> * params,
                                  const PGeneralEvaluation *general_evaluation = nullptr);

    void
    evaluateAggregationFunction(GPStore::FunctionInvocation *func, int _begin, int _end, KVstore * _kvstore,
                                const std::unordered_map<std::string, GPStore::Value> * params,
                                const PGeneralEvaluation *general_evaluation = nullptr);

    static void removeDuplicate(std::vector<GPStore::Value> & values);

};


#endif
