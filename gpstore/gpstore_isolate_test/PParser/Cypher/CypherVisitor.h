
// Generated from ./Cypher.g4 by ANTLR 4.7.2

#pragma once


#include "antlr4-runtime.h"
#include "CypherParser.h"



/**
 * This class defines an abstract visitor for a parse tree
 * produced by CypherParser.
 */
class  CypherVisitor : public antlr4::tree::AbstractParseTreeVisitor {
public:

  /**
   * Visit parse trees produced by CypherParser.
   */
    virtual antlrcpp::Any visitOC_Cypher(CypherParser::OC_CypherContext *context) = 0;

    virtual antlrcpp::Any visitOC_Statement(CypherParser::OC_StatementContext *context) = 0;

    virtual antlrcpp::Any visitOC_Query(CypherParser::OC_QueryContext *context) = 0;

    virtual antlrcpp::Any visitOC_RegularQuery(CypherParser::OC_RegularQueryContext *context) = 0;

    virtual antlrcpp::Any visitOC_Union(CypherParser::OC_UnionContext *context) = 0;

    virtual antlrcpp::Any visitOC_SingleQuery(CypherParser::OC_SingleQueryContext *context) = 0;

    virtual antlrcpp::Any visitOC_SinglePartQuery(CypherParser::OC_SinglePartQueryContext *context) = 0;

    virtual antlrcpp::Any visitOC_MultiPartQuery(CypherParser::OC_MultiPartQueryContext *context) = 0;

    virtual antlrcpp::Any visitOC_UpdatingClause(CypherParser::OC_UpdatingClauseContext *context) = 0;

    virtual antlrcpp::Any visitOC_ReadingClause(CypherParser::OC_ReadingClauseContext *context) = 0;

    virtual antlrcpp::Any visitOC_Match(CypherParser::OC_MatchContext *context) = 0;

    virtual antlrcpp::Any visitOC_Unwind(CypherParser::OC_UnwindContext *context) = 0;

    virtual antlrcpp::Any visitOC_Merge(CypherParser::OC_MergeContext *context) = 0;

    virtual antlrcpp::Any visitOC_MergeAction(CypherParser::OC_MergeActionContext *context) = 0;

    virtual antlrcpp::Any visitOC_Create(CypherParser::OC_CreateContext *context) = 0;

    virtual antlrcpp::Any visitOC_Set(CypherParser::OC_SetContext *context) = 0;

    virtual antlrcpp::Any visitOC_SetItem(CypherParser::OC_SetItemContext *context) = 0;

    virtual antlrcpp::Any visitOC_Delete(CypherParser::OC_DeleteContext *context) = 0;

    virtual antlrcpp::Any visitOC_Remove(CypherParser::OC_RemoveContext *context) = 0;

    virtual antlrcpp::Any visitOC_RemoveItem(CypherParser::OC_RemoveItemContext *context) = 0;

    virtual antlrcpp::Any visitOC_InQueryCall(CypherParser::OC_InQueryCallContext *context) = 0;

    virtual antlrcpp::Any visitOC_StandaloneCall(CypherParser::OC_StandaloneCallContext *context) = 0;

    virtual antlrcpp::Any visitOC_YieldItems(CypherParser::OC_YieldItemsContext *context) = 0;

    virtual antlrcpp::Any visitOC_YieldItem(CypherParser::OC_YieldItemContext *context) = 0;

    virtual antlrcpp::Any visitOC_With(CypherParser::OC_WithContext *context) = 0;

    virtual antlrcpp::Any visitOC_Return(CypherParser::OC_ReturnContext *context) = 0;

    virtual antlrcpp::Any visitOC_ProjectionBody(CypherParser::OC_ProjectionBodyContext *context) = 0;

    virtual antlrcpp::Any visitOC_ProjectionItems(CypherParser::OC_ProjectionItemsContext *context) = 0;

    virtual antlrcpp::Any visitOC_ProjectionItem(CypherParser::OC_ProjectionItemContext *context) = 0;

    virtual antlrcpp::Any visitOC_Order(CypherParser::OC_OrderContext *context) = 0;

    virtual antlrcpp::Any visitOC_Skip(CypherParser::OC_SkipContext *context) = 0;

    virtual antlrcpp::Any visitOC_Limit(CypherParser::OC_LimitContext *context) = 0;

    virtual antlrcpp::Any visitOC_SortItem(CypherParser::OC_SortItemContext *context) = 0;

    virtual antlrcpp::Any visitOC_Where(CypherParser::OC_WhereContext *context) = 0;

    virtual antlrcpp::Any visitOC_Pattern(CypherParser::OC_PatternContext *context) = 0;

    virtual antlrcpp::Any visitOC_PatternPart(CypherParser::OC_PatternPartContext *context) = 0;

    virtual antlrcpp::Any visitOC_AnonymousPatternPart(CypherParser::OC_AnonymousPatternPartContext *context) = 0;

    virtual antlrcpp::Any visitOC_ShortestPathPattern(CypherParser::OC_ShortestPathPatternContext *context) = 0;

    virtual antlrcpp::Any visitOC_PatternElement(CypherParser::OC_PatternElementContext *context) = 0;

    virtual antlrcpp::Any visitOC_RelationshipsPattern(CypherParser::OC_RelationshipsPatternContext *context) = 0;

    virtual antlrcpp::Any visitOC_NodePattern(CypherParser::OC_NodePatternContext *context) = 0;

    virtual antlrcpp::Any visitOC_PatternElementChain(CypherParser::OC_PatternElementChainContext *context) = 0;

    virtual antlrcpp::Any visitOC_RelationshipPattern(CypherParser::OC_RelationshipPatternContext *context) = 0;

    virtual antlrcpp::Any visitOC_RelationshipDetail(CypherParser::OC_RelationshipDetailContext *context) = 0;

    virtual antlrcpp::Any visitOC_Properties(CypherParser::OC_PropertiesContext *context) = 0;

    virtual antlrcpp::Any visitOC_RelationshipTypes(CypherParser::OC_RelationshipTypesContext *context) = 0;

    virtual antlrcpp::Any visitOC_NodeLabels(CypherParser::OC_NodeLabelsContext *context) = 0;

    virtual antlrcpp::Any visitOC_NodeLabel(CypherParser::OC_NodeLabelContext *context) = 0;

    virtual antlrcpp::Any visitOC_RangeLiteral(CypherParser::OC_RangeLiteralContext *context) = 0;

    virtual antlrcpp::Any visitOC_LabelName(CypherParser::OC_LabelNameContext *context) = 0;

    virtual antlrcpp::Any visitOC_RelTypeName(CypherParser::OC_RelTypeNameContext *context) = 0;

    virtual antlrcpp::Any visitOC_PropertyExpression(CypherParser::OC_PropertyExpressionContext *context) = 0;

    virtual antlrcpp::Any visitOC_Expression(CypherParser::OC_ExpressionContext *context) = 0;

    virtual antlrcpp::Any visitOC_OrExpression(CypherParser::OC_OrExpressionContext *context) = 0;

    virtual antlrcpp::Any visitOC_XorExpression(CypherParser::OC_XorExpressionContext *context) = 0;

    virtual antlrcpp::Any visitOC_AndExpression(CypherParser::OC_AndExpressionContext *context) = 0;

    virtual antlrcpp::Any visitOC_NotExpression(CypherParser::OC_NotExpressionContext *context) = 0;

    virtual antlrcpp::Any visitOC_ComparisonExpression(CypherParser::OC_ComparisonExpressionContext *context) = 0;

    virtual antlrcpp::Any visitOC_PartialComparisonExpression(CypherParser::OC_PartialComparisonExpressionContext *context) = 0;

    virtual antlrcpp::Any visitOC_StringListNullPredicateExpression(CypherParser::OC_StringListNullPredicateExpressionContext *context) = 0;

    virtual antlrcpp::Any visitOC_StringPredicateExpression(CypherParser::OC_StringPredicateExpressionContext *context) = 0;

    virtual antlrcpp::Any visitOC_ListPredicateExpression(CypherParser::OC_ListPredicateExpressionContext *context) = 0;

    virtual antlrcpp::Any visitOC_NullPredicateExpression(CypherParser::OC_NullPredicateExpressionContext *context) = 0;

    virtual antlrcpp::Any visitOC_AddOrSubtractExpression(CypherParser::OC_AddOrSubtractExpressionContext *context) = 0;

    virtual antlrcpp::Any visitOC_MultiplyDivideModuloExpression(CypherParser::OC_MultiplyDivideModuloExpressionContext *context) = 0;

    virtual antlrcpp::Any visitOC_PowerOfExpression(CypherParser::OC_PowerOfExpressionContext *context) = 0;

    virtual antlrcpp::Any visitOC_UnaryAddOrSubtractExpression(CypherParser::OC_UnaryAddOrSubtractExpressionContext *context) = 0;

    virtual antlrcpp::Any visitOC_ListOperatorExpression(CypherParser::OC_ListOperatorExpressionContext *context) = 0;

    virtual antlrcpp::Any visitOC_PropertyOrLabelsExpression(CypherParser::OC_PropertyOrLabelsExpressionContext *context) = 0;

    virtual antlrcpp::Any visitOC_PropertyLookup(CypherParser::OC_PropertyLookupContext *context) = 0;

    virtual antlrcpp::Any visitOC_Atom(CypherParser::OC_AtomContext *context) = 0;

    virtual antlrcpp::Any visitOC_CaseExpression(CypherParser::OC_CaseExpressionContext *context) = 0;

    virtual antlrcpp::Any visitOC_CaseAlternative(CypherParser::OC_CaseAlternativeContext *context) = 0;

    virtual antlrcpp::Any visitOC_ListComprehension(CypherParser::OC_ListComprehensionContext *context) = 0;

    virtual antlrcpp::Any visitOC_PatternComprehension(CypherParser::OC_PatternComprehensionContext *context) = 0;

    virtual antlrcpp::Any visitOC_Quantifier(CypherParser::OC_QuantifierContext *context) = 0;

    virtual antlrcpp::Any visitOC_FilterExpression(CypherParser::OC_FilterExpressionContext *context) = 0;

    virtual antlrcpp::Any visitOC_PatternPredicate(CypherParser::OC_PatternPredicateContext *context) = 0;

    virtual antlrcpp::Any visitOC_ParenthesizedExpression(CypherParser::OC_ParenthesizedExpressionContext *context) = 0;

    virtual antlrcpp::Any visitOC_IdInColl(CypherParser::OC_IdInCollContext *context) = 0;

    virtual antlrcpp::Any visitOC_FunctionInvocation(CypherParser::OC_FunctionInvocationContext *context) = 0;

    virtual antlrcpp::Any visitOC_FunctionName(CypherParser::OC_FunctionNameContext *context) = 0;

    virtual antlrcpp::Any visitOC_ExistentialSubquery(CypherParser::OC_ExistentialSubqueryContext *context) = 0;

    virtual antlrcpp::Any visitOC_ExplicitProcedureInvocation(CypherParser::OC_ExplicitProcedureInvocationContext *context) = 0;

    virtual antlrcpp::Any visitOC_ImplicitProcedureInvocation(CypherParser::OC_ImplicitProcedureInvocationContext *context) = 0;

    virtual antlrcpp::Any visitOC_ProcedureResultField(CypherParser::OC_ProcedureResultFieldContext *context) = 0;

    virtual antlrcpp::Any visitOC_ProcedureName(CypherParser::OC_ProcedureNameContext *context) = 0;

    virtual antlrcpp::Any visitOC_Namespace(CypherParser::OC_NamespaceContext *context) = 0;

    virtual antlrcpp::Any visitOC_Variable(CypherParser::OC_VariableContext *context) = 0;

    virtual antlrcpp::Any visitOC_Literal(CypherParser::OC_LiteralContext *context) = 0;

    virtual antlrcpp::Any visitOC_BooleanLiteral(CypherParser::OC_BooleanLiteralContext *context) = 0;

    virtual antlrcpp::Any visitOC_NumberLiteral(CypherParser::OC_NumberLiteralContext *context) = 0;

    virtual antlrcpp::Any visitOC_IntegerLiteral(CypherParser::OC_IntegerLiteralContext *context) = 0;

    virtual antlrcpp::Any visitOC_DoubleLiteral(CypherParser::OC_DoubleLiteralContext *context) = 0;

    virtual antlrcpp::Any visitOC_ListLiteral(CypherParser::OC_ListLiteralContext *context) = 0;

    virtual antlrcpp::Any visitOC_MapLiteral(CypherParser::OC_MapLiteralContext *context) = 0;

    virtual antlrcpp::Any visitOC_PropertyKeyName(CypherParser::OC_PropertyKeyNameContext *context) = 0;

    virtual antlrcpp::Any visitOC_Parameter(CypherParser::OC_ParameterContext *context) = 0;

    virtual antlrcpp::Any visitOC_SchemaName(CypherParser::OC_SchemaNameContext *context) = 0;

    virtual antlrcpp::Any visitOC_ReservedWord(CypherParser::OC_ReservedWordContext *context) = 0;

    virtual antlrcpp::Any visitOC_SymbolicName(CypherParser::OC_SymbolicNameContext *context) = 0;

    virtual antlrcpp::Any visitOC_LeftArrowHead(CypherParser::OC_LeftArrowHeadContext *context) = 0;

    virtual antlrcpp::Any visitOC_RightArrowHead(CypherParser::OC_RightArrowHeadContext *context) = 0;

    virtual antlrcpp::Any visitOC_Dash(CypherParser::OC_DashContext *context) = 0;


};

