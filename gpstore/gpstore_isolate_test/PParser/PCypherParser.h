#ifndef PPARSER_PCYPHERPARSER_H
#define PPARSER_PCYPHERPARSER_H

#include <typeinfo>
#include "antlr4-runtime.h"
#include "Cypher/CypherLexer.h"
#include "Cypher/CypherParser.h"
#include "Cypher/CypherBaseVisitor.h"
#include "CypherAST.h"
#include "Pattern.h"
#include <unordered_map>
#include <unordered_set>
#include <map>
#include "../Util/Util.h"
#include "../Util/Value.h"
#include "../KVstore/KVstore.h"

/**
 * @brief Class for Cypher Symbol. This class definition represents a Cypher symbol, which is an object that holds a variable type and an ID.
*/
class CypherSymbol{
public:
    enum VarType {NODE_VAR, EDGE_VAR, PATH_VAR, VALUE_VAR}; // Variable type 变量类型
    VarType var_ty_;  // Variable type 变量类型
    unsigned var_id_; // Variable ID 变量ID
    CypherSymbol(){}
    CypherSymbol(VarType vty):var_ty_(vty){}
    CypherSymbol(unsigned var_id):var_id_(var_id){}
    CypherSymbol(VarType vty, unsigned var_id):var_ty_(vty), var_id_(var_id){}
};

/**
 * @brief The SymbolTableStack class manages a stack of symbol tables, which store information about variables and their properties.
*/
class SymbolTableStack{
public:
    KVstore * kvstore_; // storage engine, to encode property id 存储引擎的指针
    std::vector<std::string> id2string_;  // variable id to variable name 变量ID到变量名的映射
    std::unordered_map<std::string, unsigned> prop2id_; // property name to property id 属性名到属性ID的映射
    std::map<unsigned, std::string> prop_id2string_; // property id to property name 属性ID到属性名的映射
    std::vector<std::unordered_map<std::string, CypherSymbol>> symbol_tb_st_; // symbol table stack 符号表栈
    unsigned next_var_id_, next_anno_id_, next_prop_id_;  // next id to be allocated 下一个要分配的ID
public:
    SymbolTableStack(){reset();}
    void setKVstore(KVstore * _kvstore);
    bool exists(const std::string & var) const;
    CypherSymbol& search(const std::string & var) ;
    void insert(const std::string& var_name, const CypherSymbol& sym);
    void insert(const std::string& var_name, CypherSymbol::VarType var_ty, unsigned var_id);
    void clearExcept(const std::vector<std::string> & remain_var);
    void clear();
    void push();
    void pop();
    void reset();
    unsigned getNextVarId();
    std::string getAnnoName();
    void putString(const std::string& str) { id2string_.emplace_back(str); }
    std::vector<unsigned> getAllVarId();
    std::vector<unsigned> getAllNamedVarId();
    const std::vector<std::string>& getIdToString(){return id2string_;}
    unsigned getPropId(const std::string &prop);
};

/**
 * @brief Parser for Cypher queries, inherited from CypherBaseVisitor, which is automatically generated from the Cypher grammar by ANTLR.
 * Implements visiting important grammatical units, extracts relevant information, and stores in CypherAST.
 * @warning Variable will be encoded to an id, and we have a id=>string map
 * @warning Property name, property id will be save, we have map between them; For an update clause, a property name may dont have property id in PStore.
 * @warning Node label & edge type only save name, don't have id now.
*/
class PCypherParser: public CypherBaseVisitor
{
private:
    SymbolTableStack sym_tb_; // Symbol table stack 符号表栈
public:
    const static std::unordered_map<std::string, std::vector<std::string>> proc_yield_items_; // a mapping from procedure name to yield items 过程名到返回值的映射

    PCypherParser(){}

    CypherAST* CypherParse(const std::string &query, const std::shared_ptr<std::unordered_map<std::string, GPStore::Value>> &_param,
                           KVstore * _kvstore = nullptr);	// Overall driver function
    CypherAST* CypherParse(std::istream& in, const std::shared_ptr<std::unordered_map<std::string, GPStore::Value>> &_param,KVstore * _kvstore = nullptr);

private:
    std::shared_ptr<std::unordered_map<std::string, GPStore::Value>> params_; // parameters

    /* helper functions */
    static long long parseIntegerLiteral(const std::string &s);
    static double parseDoubleLiteral(const std::string &s);
    static std::string parseStringLiteral(const std::string &s);
    static std::string parseSymbolicName(const std::string &s);
    antlrcpp::Any visitOC_Cypher(CypherParser::OC_CypherContext *ctx) override;
    antlrcpp::Any visitOC_RegularQuery(CypherParser::OC_RegularQueryContext *ctx) override;
    antlrcpp::Any visitOC_StandaloneCall(CypherParser::OC_StandaloneCallContext *ctx) override;
    antlrcpp::Any visitOC_SingleQuery(CypherParser::OC_SingleQueryContext *ctx) override;
    antlrcpp::Any visitOC_SinglePartQuery(CypherParser::OC_SinglePartQueryContext *ctx) override;
    antlrcpp::Any visitOC_MultiPartQuery(CypherParser::OC_MultiPartQueryContext *ctx) override;

    antlrcpp::Any visitOC_ReadingClause(CypherParser::OC_ReadingClauseContext *ctx) override;
    antlrcpp::Any visitOC_UpdatingClause(CypherParser::OC_UpdatingClauseContext *ctx) override;

    antlrcpp::Any visitOC_Match(CypherParser::OC_MatchContext *ctx) override;
    antlrcpp::Any visitOC_Unwind(CypherParser::OC_UnwindContext *ctx) override;
    antlrcpp::Any visitOC_InQueryCall(CypherParser::OC_InQueryCallContext *ctx) override;

    antlrcpp::Any visitOC_Create(CypherParser::OC_CreateContext *ctx) override;
    antlrcpp::Any visitOC_Set(CypherParser::OC_SetContext *ctx) override;
    antlrcpp::Any visitOC_Delete(CypherParser::OC_DeleteContext *ctx) override;
    antlrcpp::Any visitOC_Remove(CypherParser::OC_RemoveContext *ctx) override;
    antlrcpp::Any visitOC_Merge(CypherParser::OC_MergeContext *ctx) override;

    antlrcpp::Any visitOC_With(CypherParser::OC_WithContext *ctx) override;
    antlrcpp::Any visitOC_Return(CypherParser::OC_ReturnContext *ctx) override;
    antlrcpp::Any visitOC_ProjectionBody(CypherParser::OC_ProjectionBodyContext *ctx, bool is_with);

    /* Pattern */

    antlrcpp::Any visitOC_PatternPart(CypherParser::OC_PatternPartContext *ctx) override;
    antlrcpp::Any visitOC_NodePattern(CypherParser::OC_NodePatternContext *ctx) override;
    antlrcpp::Any visitOC_RelationshipPattern(CypherParser::OC_RelationshipPatternContext *ctx) override;
    antlrcpp::Any visitOC_RangeLiteral(CypherParser::OC_RangeLiteralContext *ctx) override;
    antlrcpp::Any visitOC_PatternElement(CypherParser::OC_PatternElementContext *ctx) override;
    antlrcpp::Any visitOC_RelationshipsPattern(CypherParser::OC_RelationshipsPatternContext *ctx) override;
    /* Expression */

    antlrcpp::Any visitOC_Expression(CypherParser::OC_ExpressionContext *ctx) override;
    antlrcpp::Any visitOC_OrExpression(CypherParser::OC_OrExpressionContext *ctx) override;
    antlrcpp::Any visitOC_XorExpression(CypherParser::OC_XorExpressionContext *ctx) override;
    antlrcpp::Any visitOC_AndExpression(CypherParser::OC_AndExpressionContext *ctx) override;
    antlrcpp::Any visitOC_NotExpression(CypherParser::OC_NotExpressionContext *ctx) override;
    antlrcpp::Any visitOC_ComparisonExpression(CypherParser::OC_ComparisonExpressionContext *ctx) override;
    antlrcpp::Any visitOC_StringListNullPredicateExpression(CypherParser::OC_StringListNullPredicateExpressionContext *ctx) override;
    antlrcpp::Any visitOC_AddOrSubtractExpression(CypherParser::OC_AddOrSubtractExpressionContext *ctx) override ;
    antlrcpp::Any visitOC_MultiplyDivideModuloExpression(CypherParser::OC_MultiplyDivideModuloExpressionContext *ctx) override;
    antlrcpp::Any visitOC_PowerOfExpression(CypherParser::OC_PowerOfExpressionContext *ctx) override;
    antlrcpp::Any visitOC_UnaryAddOrSubtractExpression(CypherParser::OC_UnaryAddOrSubtractExpressionContext *ctx) override;
    antlrcpp::Any visitOC_ListOperatorExpression(CypherParser::OC_ListOperatorExpressionContext *ctx) override ;
    antlrcpp::Any visitOC_PropertyOrLabelsExpression(CypherParser::OC_PropertyOrLabelsExpressionContext *ctx) override;
    antlrcpp::Any visitOC_PropertyExpression(CypherParser::OC_PropertyExpressionContext *ctx) override;
    antlrcpp::Any visitOC_Atom(CypherParser::OC_AtomContext *ctx) override;

    antlrcpp::Any visitOC_Literal(CypherParser::OC_LiteralContext *ctx) override;
    antlrcpp::Any visitOC_Parameter(CypherParser::OC_ParameterContext *ctx) override;
    antlrcpp::Any visitOC_Variable(CypherParser::OC_VariableContext *ctx) override;
    antlrcpp::Any visitOC_CaseExpression(CypherParser::OC_CaseExpressionContext *ctx) override;
    antlrcpp::Any visitOC_ListComprehension(CypherParser::OC_ListComprehensionContext *ctx) override ;
    antlrcpp::Any visitOC_PatternComprehension(CypherParser::OC_PatternComprehensionContext *ctx) override;
    antlrcpp::Any visitOC_Quantifier(CypherParser::OC_QuantifierContext *ctx) override;
    antlrcpp::Any visitOC_PatternPredicate(CypherParser::OC_PatternPredicateContext *ctx) override;
    antlrcpp::Any visitOC_FunctionInvocation(CypherParser::OC_FunctionInvocationContext *ctx) override;
    antlrcpp::Any visitOC_ExistentialSubquery(CypherParser::OC_ExistentialSubqueryContext *ctx) override;
    antlrcpp::Any visitOC_ParenthesizedExpression(CypherParser::OC_ParenthesizedExpressionContext *ctx) override ;
};

/**
	@brief Listener for errors during Cypher query parsing, which throws a corresponding exception when an error arises.
*/
class CypherErrorListener: public antlr4::BaseErrorListener
{
public:
    void syntaxError(antlr4::Recognizer *recognizer, antlr4::Token * offendingSymbol, \
		size_t line, size_t charPositionInLine, const std::string &msg, std::exception_ptr e) override;
};

#endif