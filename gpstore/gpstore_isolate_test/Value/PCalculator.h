#ifndef VALUE_PCALCULATOR_H
#define VALUE_PCALCULATOR_H

#include <unordered_map>
#include "../Util/Value.h"
#include "Expression.h"
#include <cmath>
#include "../Util/Util.h"
#include "../KVstore/KVstore.h"
#include "../PQuery/PGeneralEvaluation.h"
#include "../Optimizer/Query.h"

class PGeneralEvaluation;
/**
 * @brief A class to evaluate an Expression for a row of PIntermediaResult or PTempResult
*/
class PCalculator{
public:

    static GPStore::Value
    evaluateExpression(
            const GPStore::Expression *exp,
            const std::vector<unsigned> & spo_embeddings,
            const std::map<unsigned, unsigned> & spo_id2col,
            const std::vector<unsigned long long> & edge_embeddings,
            const std::map<unsigned, unsigned> & edge_id2col,
            const std::vector<GPStore::Value> & value_embeddings,
            const std::map<unsigned, unsigned> & value_id2col,
            const std::map<std::pair<unsigned ,unsigned >, unsigned > & var_prop_id2col,
            const std::unordered_map<std::string, GPStore::Value> * params,
            KVstore * _kvstore,
            const PGeneralEvaluation *general_evaluation = nullptr
    );


    static GPStore::Value
    evaluateConstExpression(const GPStore::Expression *exp, const std::unordered_map<std::string, GPStore::Value> * params);



    static GPStore::Value
    evaluateBinaryLogicalExpression(
            const GPStore::Expression *exp,
            const std::vector<unsigned> & spo_embeddings,
            const std::map<unsigned, unsigned> & spo_id2col,
            const std::vector<unsigned long long> & edge_embeddings,
            const std::map<unsigned, unsigned> & edge_id2col,
            const std::vector<GPStore::Value> & value_embeddings,
            const std::map<unsigned, unsigned> & value_id2col,
            const std::map<std::pair<unsigned ,unsigned >, unsigned > & var_prop_id2col,
            const std::unordered_map<std::string, GPStore::Value> * params,
            KVstore * _kvstore,
            const PGeneralEvaluation *general_evaluation = nullptr
    );

    static GPStore::Value
    evaluateNotExpression(
            const GPStore::Expression *exp,
            const std::vector<unsigned> & spo_embeddings,
            const std::map<unsigned, unsigned> & spo_id2col,
            const std::vector<unsigned long long> & edge_embeddings,
            const std::map<unsigned, unsigned> & edge_id2col,
            const std::vector<GPStore::Value> & value_embeddings,
            const std::map<unsigned, unsigned> & value_id2col,
            const std::map<std::pair<unsigned ,unsigned >, unsigned > & var_prop_id2col,
            const std::unordered_map<std::string, GPStore::Value> * params,
            KVstore * _kvstore,
            const PGeneralEvaluation *general_evaluation = nullptr
    );

    static GPStore::Value
    evaluateBinaryCompareExpression(
            const GPStore::Expression *exp,
            const std::vector<unsigned> & spo_embeddings,
            const std::map<unsigned, unsigned> & spo_id2col,
            const std::vector<unsigned long long> & edge_embeddings,
            const std::map<unsigned, unsigned> & edge_id2col,
            const std::vector<GPStore::Value> & value_embeddings,
            const std::map<unsigned, unsigned> & value_id2col,
            const std::map<std::pair<unsigned ,unsigned >, unsigned > & var_prop_id2col,
            const std::unordered_map<std::string, GPStore::Value> * params,
            KVstore * _kvstore,
            const PGeneralEvaluation *general_evaluation = nullptr
    );

    static GPStore::Value
    evaluateInExpression(
            const GPStore::Expression *exp,
            const std::vector<unsigned> & spo_embeddings,
            const std::map<unsigned, unsigned> & spo_id2col,
            const std::vector<unsigned long long> & edge_embeddings,
            const std::map<unsigned, unsigned> & edge_id2col,
            const std::vector<GPStore::Value> & value_embeddings,
            const std::map<unsigned, unsigned> & value_id2col,
            const std::map<std::pair<unsigned ,unsigned >, unsigned > & var_prop_id2col,
            const std::unordered_map<std::string, GPStore::Value> * params,
            KVstore * _kvstore,
            const PGeneralEvaluation *general_evaluation = nullptr
    );

    static GPStore::Value
    evaluateStringExpression(
            const GPStore::Expression *exp,
            const std::vector<unsigned> & spo_embeddings,
            const std::map<unsigned, unsigned> & spo_id2col,
            const std::vector<unsigned long long> & edge_embeddings,
            const std::map<unsigned, unsigned> & edge_id2col,
            const std::vector<GPStore::Value> & value_embeddings,
            const std::map<unsigned, unsigned> & value_id2col,
            const std::map<std::pair<unsigned ,unsigned >, unsigned > & var_prop_id2col,
            const std::unordered_map<std::string, GPStore::Value> * params,
            KVstore * _kvstore,
            const PGeneralEvaluation *general_evaluation = nullptr
    );

    static GPStore::Value
    evaluateIsNullExpression(
            const GPStore::Expression *exp,
            const std::vector<unsigned> & spo_embeddings,
            const std::map<unsigned, unsigned> & spo_id2col,
            const std::vector<unsigned long long> & edge_embeddings,
            const std::map<unsigned, unsigned> & edge_id2col,
            const std::vector<GPStore::Value> & value_embeddings,
            const std::map<unsigned, unsigned> & value_id2col,
            const std::map<std::pair<unsigned ,unsigned >, unsigned > & var_prop_id2col,
            const std::unordered_map<std::string, GPStore::Value> * params,
            KVstore * _kvstore,
            const PGeneralEvaluation *general_evaluation = nullptr
    );

    static GPStore::Value
    evaluateBinaryArithmeticExpression(
            const GPStore::Expression *exp,
            const std::vector<unsigned> & spo_embeddings,
            const std::map<unsigned, unsigned> & spo_id2col,
            const std::vector<unsigned long long> & edge_embeddings,
            const std::map<unsigned, unsigned> & edge_id2col,
            const std::vector<GPStore::Value> & value_embeddings,
            const std::map<unsigned, unsigned> & value_id2col,
            const std::map<std::pair<unsigned ,unsigned >, unsigned > & var_prop_id2col,
            const std::unordered_map<std::string, GPStore::Value> * params,
            KVstore * _kvstore,
            const PGeneralEvaluation *general_evaluation = nullptr
    );

    static GPStore::Value
    evaluateUnaryAddMinusExpression(
            const GPStore::Expression *exp,
            const std::vector<unsigned> & spo_embeddings,
            const std::map<unsigned, unsigned> & spo_id2col,
            const std::vector<unsigned long long> & edge_embeddings,
            const std::map<unsigned, unsigned> & edge_id2col,
            const std::vector<GPStore::Value> & value_embeddings,
            const std::map<unsigned, unsigned> & value_id2col,
            const std::map<std::pair<unsigned ,unsigned >, unsigned > & var_prop_id2col,
            const std::unordered_map<std::string, GPStore::Value> * params,
            KVstore * _kvstore,
            const PGeneralEvaluation *general_evaluation = nullptr
    );

    static GPStore::Value
    evaluateIndexSliceExpression(
            const GPStore::Expression *exp,
            const std::vector<unsigned> & spo_embeddings,
            const std::map<unsigned, unsigned> & spo_id2col,
            const std::vector<unsigned long long> & edge_embeddings,
            const std::map<unsigned, unsigned> & edge_id2col,
            const std::vector<GPStore::Value> & value_embeddings,
            const std::map<unsigned, unsigned> & value_id2col,
            const std::map<std::pair<unsigned ,unsigned >, unsigned > & var_prop_id2col,
            const std::unordered_map<std::string, GPStore::Value> * params,
            KVstore * _kvstore,
            const PGeneralEvaluation *general_evaluation = nullptr
    );

    static GPStore::Value
    evaluateAtomExpression(
            const GPStore::Expression *exp,
            const std::vector<unsigned> & spo_embeddings,
            const std::map<unsigned, unsigned> & spo_id2col,
            const std::vector<unsigned long long> & edge_embeddings,
            const std::map<unsigned, unsigned> & edge_id2col,
            const std::vector<GPStore::Value> & value_embeddings,
            const std::map<unsigned, unsigned> & value_id2col,
            const std::map<std::pair<unsigned ,unsigned >, unsigned > & var_prop_id2col,
            const std::unordered_map<std::string, GPStore::Value> * params,
            KVstore * _kvstore,
            const PGeneralEvaluation *general_evaluation = nullptr
    );

    static GPStore::Value
    evaluateAtom(
            const GPStore::Atom *atom,
            const std::vector<unsigned> & spo_embeddings,
            const std::map<unsigned, unsigned> & spo_id2col,
            const std::vector<unsigned long long> & edge_embeddings,
            const std::map<unsigned, unsigned> & edge_id2col,
            const std::vector<GPStore::Value> & value_embeddings,
            const std::map<unsigned, unsigned> & value_id2col,
            const std::map<std::pair<unsigned ,unsigned >, unsigned > & var_prop_id2col,
            const std::unordered_map<std::string, GPStore::Value> * params,
            KVstore * _kvstore,
            const PGeneralEvaluation *general_evaluation = nullptr
    );

    static GPStore::Value
    evaluateLiteral( const GPStore::Literal *literal,
                     const std::vector<unsigned> & spo_embeddings,
                     const std::map<unsigned, unsigned> & spo_id2col,
                     const std::vector<unsigned long long> & edge_embeddings,
                     const std::map<unsigned, unsigned> & edge_id2col,
                     const std::vector<GPStore::Value> & value_embeddings,
                     const std::map<unsigned, unsigned> & value_id2col,
                     const std::map<std::pair<unsigned ,unsigned >, unsigned > & var_prop_id2col,
                     const std::unordered_map<std::string, GPStore::Value> * params,
                     KVstore * _kvstore,
                     const PGeneralEvaluation *general_evaluation = nullptr
        );

    static GPStore::Value
    evaluateParameter( const GPStore::Parameter * p, const std::unordered_map<std::string, GPStore::Value> * params);

    static GPStore::Value
    evaluateVariable(
            const GPStore::Variable *var,
            const std::vector<unsigned> & spo_embeddings,
            const std::map<unsigned, unsigned> & spo_id2col,
            const std::vector<unsigned long long> & edge_embeddings,
            const std::map<unsigned, unsigned> & edge_id2col,
            const std::vector<GPStore::Value> & value_embeddings,
            const std::map<unsigned, unsigned> & value_id2col
    );

    static GPStore::Value
    evaluateParenthesizedExpression(
            const GPStore::ParenthesizedExpression *par_exp,
            const std::vector<unsigned> & spo_embeddings,
            const std::map<unsigned, unsigned> & spo_id2col,
            const std::vector<unsigned long long> & edge_embeddings,
            const std::map<unsigned, unsigned> & edge_id2col,
            const std::vector<GPStore::Value> & value_embeddings,
            const std::map<unsigned, unsigned> & value_id2col,
            const std::map<std::pair<unsigned ,unsigned >, unsigned > & var_prop_id2col,
            const std::unordered_map<std::string, GPStore::Value> * params,
            KVstore * _kvstore,
            const PGeneralEvaluation *general_evaluation = nullptr
    );

    static GPStore::Value
    evaluateFunctionInvocation(
            const GPStore::FunctionInvocation *func,
            const std::vector<unsigned> & spo_embeddings,
            const std::map<unsigned, unsigned> & spo_id2col,
            const std::vector<unsigned long long> & edge_embeddings,
            const std::map<unsigned, unsigned> & edge_id2col,
            const std::vector<GPStore::Value> & value_embeddings,
            const std::map<unsigned, unsigned> & value_id2col,
            const std::map<std::pair<unsigned ,unsigned >, unsigned > & var_prop_id2col,
            const std::unordered_map<std::string, GPStore::Value> * params,
            KVstore * _kvstore,
            const PGeneralEvaluation *general_evaluation = nullptr
    );

    static GPStore::Value
    evaluateCaseExpression(
            const GPStore::CaseExpression *case_exp,
            const std::vector<unsigned> & spo_embeddings,
            const std::map<unsigned, unsigned> & spo_id2col,
            const std::vector<unsigned long long> & edge_embeddings,
            const std::map<unsigned, unsigned> & edge_id2col,
            const std::vector<GPStore::Value> & value_embeddings,
            const std::map<unsigned, unsigned> & value_id2col,
            const std::map<std::pair<unsigned ,unsigned >, unsigned > & var_prop_id2col,
            const std::unordered_map<std::string, GPStore::Value> * params,
            KVstore * _kvstore,
            const PGeneralEvaluation *general_evaluation = nullptr
    );

    static GPStore::Value
    evaluatePatternPredicate(
            const GPStore::PatternPredicate *ptn_pred,
            const std::vector<unsigned> & spo_embeddings,
            const std::map<unsigned, unsigned> & spo_id2col,
            const std::vector<unsigned long long> & edge_embeddings,
            const std::map<unsigned, unsigned> & edge_id2col,
            const std::vector<GPStore::Value> & value_embeddings,
            const std::map<unsigned, unsigned> & value_id2col,
            const std::map<std::pair<unsigned ,unsigned >, unsigned > & var_prop_id2col,
            const std::unordered_map<std::string, GPStore::Value> * params,
            KVstore * _kvstore,
            const PGeneralEvaluation *general_evaluation = nullptr
    );

    static GPStore::Value
    evaluateListComprehension(
            const GPStore::ListComprehension *list_comp,
            const std::vector<unsigned> & spo_embeddings,
            const std::map<unsigned, unsigned> & spo_id2col,
            const std::vector<unsigned long long> & edge_embeddings,
            const std::map<unsigned, unsigned> & edge_id2col,
            const std::vector<GPStore::Value> & value_embeddings,
            const std::map<unsigned, unsigned> & value_id2col,
            const std::map<std::pair<unsigned ,unsigned >, unsigned > & var_prop_id2col,
            const std::unordered_map<std::string, GPStore::Value> * params,
            KVstore * _kvstore,
            const PGeneralEvaluation *general_evaluation = nullptr
    );

    static GPStore::Value
    evaluateRangeExpression(
            const GPStore::RangeExpression *range_exp,
            const std::vector<unsigned> & spo_embeddings,
            const std::map<unsigned, unsigned> & spo_id2col,
            const std::vector<unsigned long long> & edge_embeddings,
            const std::map<unsigned, unsigned> & edge_id2col,
            const std::vector<GPStore::Value> & value_embeddings,
            const std::map<unsigned, unsigned> & value_id2col,
            const std::map<std::pair<unsigned ,unsigned >, unsigned > & var_prop_id2col,
            const std::unordered_map<std::string, GPStore::Value> * params,
            KVstore * _kvstore,
            const PGeneralEvaluation *general_evaluation = nullptr
    );

    static GPStore::Value
    evaluatePropertyOrMapKey(const GPStore::Value & x, const std::vector<std::string> & keynames, KVstore * _kvstore, std::shared_ptr<Transaction> txn_,
                             const std::vector<GPStore::Value> & value_embeddings,
                             const std::map<std::pair<unsigned ,unsigned >, unsigned > & var_prop_id2col,
                             unsigned var_id = GPStore::Variable::ID_NONE);

    static GPStore::Value
    logicalAnd(const GPStore::Value & x, const GPStore::Value & y);

    static GPStore::Value
    logicalOr(const GPStore::Value & x, const GPStore::Value & y);

    static GPStore::Value
    logicalXor(const GPStore::Value & x, const GPStore::Value & y);

    static GPStore::Value
    logicalNot(const GPStore::Value & x);

    static GPStore::Value
    arithmeticOperator(const GPStore::Value &x, const GPStore::Value &y, GPStore::Expression::OperatorType op);

    static GPStore::Value
    toInteger(const GPStore::Value & x);

    static GPStore::Value
    toFloat(const GPStore::Value & x);

    static GPStore::Value floor(const GPStore::Value & x);

    static int
    IntArithOperator(int x, int y, GPStore::Expression::OperatorType op);
    static long long
    LongArithOperator(long long x, long long y, GPStore::Expression::OperatorType op);


    static double
    doubleArithOperator(double x, double y, GPStore::Expression::OperatorType op);

    /**
     * @brief Compare two values x, y for comparison expression; Use Comparison semantics instead of ordering semantics.
     * @param x a value
     * @param y another value
     * @warning you must make sure that x, y is comparable on the comparison semantics.[i.e. Never evaluate to null]
     * */
    static int
    compValue(const GPStore::Value &x, const GPStore::Value &y);

    /**
     * @brief Check if two values x, y are equal; Use Comparison semantics instead of ordering semantics.
     * @param x a value
     * @param y another value
     * @warning you must make sure that x, y is comparable on the comparison semantics.[i.e. Never evaluate to null]
     * */
    static bool valueEquality(const GPStore::Value &x, const GPStore::Value &y);
};




#endif VALUE_PCALCULATOR_H
