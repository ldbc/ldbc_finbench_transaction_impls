#ifndef VALUE_EXPRESSION_H
#define VALUE_EXPRESSION_H

#include <string>
#include <vector>
#include <map>
#include <set>
#include <memory>
#include "../Util/Value.h"
#include "../Util/Util.h"
/* make it able to be compiled */
namespace GPStore{
    class Expression;
}
#include "../PQuery/PVarset.h"
#include "../PParser/Pattern.h"

/**
 * @namespace GPStore
 * @brief Namespace for gpstore, including many useful utilities.
*/
namespace GPStore{

    class Expression;
    class RangeExpression;
    class AtomPropertyLabels;
    class Atom;

    /**
     * @brief Represents a mathematical or logical expression in a graph database.
     * This class provides a hierarchical structure to represent complex expressions,
     * including support for variables, properties, operators, and functions.
     * It offers various methods for analyzing, manipulating, and evaluating expressions,
     * such as checking expression types, extracting variables and properties,
     * and performing semantic checks.
     *
     * The expression tree is composed of nodes, where each node represents an operation
     * or a terminal value (e.g., a variable or a constant). The tree structure allows
     * for efficient evaluation and manipulation of expressions.
     *
     * This class is designed to be used in conjunction with the graph database query engine,
     * providing a flexible and efficient way to represent and evaluate queries.
     */
    class Expression {
    public:
        enum OperatorType {
            OR,
            XOR,
            AND,
            NOT,
            EQUAL, NOT_EQUAL, LESS, GREATER, LESS_EQUAL, GREATER_EQUAL,
            IN,
            STARTS_WITH, ENDS_WITH, CONTAINS,
            IS_NULL, IS_NOT_NULL,
            ADD, SUB,
            MUL, DIV, MOD,
            POW,
            IDENTITY, NEGATION, // 正负号
            INDEX, SLICE, // a[10], a["age"], or m[1:4]
            // 最后一种是oC_PropertyOrLabelsExpression，存在propery_label里
            EMPTY_OP    // no op, it's leaf node
        };  // all possible operators 所有可能的运算符

        OperatorType oprt_; // operator type 运算符类型
        std::vector<Expression *> children_;    // children 孩子节点
        Atom *atom_;  // atom when it is leaf node 叶节点
        AtomPropertyLabels *property_label_; // property_label when it is leaf node 叶子结点的属性标签

        PVarset<unsigned> covered_var_id_; // covered variable id set 变量覆盖集合
        // 属性覆盖集合 a.age
        // 这是建议覆盖集合，非真正属性覆盖。比如，a可能是map类型而非Node/Edge。
        PVarset<std::pair<unsigned, unsigned>> covered_props_;  // covered property set 属性覆盖集合

        Expression();

        Expression(const Expression &that);

        Expression &operator=(const Expression &that);

        ~Expression();

        void release();

        bool isAtom() const;

        bool isVariable() const;

        bool isVariableProp() const ;

        bool isSpecialExpression(const std::vector<unsigned>& table_vars, GPStore::Expression* &exp_read_only,
                                 unsigned &var_id, std::string& prop_name, GPStore::Expression::OperatorType &oprt);

        bool isVarPropEqualConst(GPStore::Expression *&exp_read_only, unsigned &var_id, unsigned & prop_id);

        bool isVarPropCompConst(GPStore::Expression *&exp_read_only, unsigned &var_id, unsigned & prop_id, GPStore::Expression::OperatorType &op);

        /**
         * a.age = 10 OR b.name = "Alice"
         * What if a.age + b.age = 10 OR b.name = "Alice" ?
         * */
        
        bool isEqualOrEqualExpression() const;

        /**
         * @brief If this is a equal predicate on a single variable and its `id`, return true.
         * */
        bool isIdEqual() const;

        bool containsAggrFunc() const;
        bool containsId() const;

        /* if this is variable, return its name */
        std::string getVariableName() const;

        unsigned getVariableId() const;

        bool isRangeExpression();
        const RangeExpression * getRangeExpression();

        void print( ) const;


        static Atom *AtomDeepCopy(Atom *atom);

        static std::string oprt2String(OperatorType op);

        static std::vector<Expression *> split(Expression *exp, OperatorType oprt);

        static Expression * VarNonEqualToExpression(const std::string& var1, const std::string& var2, unsigned id1, unsigned id2);
        static Expression * VarPropertyToExpression(unsigned var_id, const std::string & var_name, unsigned prop_id,
                                                    const std::string & prop_key_name, GPStore::Expression *exp);

        static Expression *JoinExpressionBy(const std::vector<Expression *>  & exprs, OperatorType oprt = AND);
    };

    /**
     * @brief Represents a collection of property labels for atoms in a graph database.
     * Stores property key names, corresponding labels, and optional property IDs.
    */
    class AtomPropertyLabels{
    public:
        std::vector<std::string> prop_key_names_;    // property names 属性名
        std::vector<std::string> labels_;  // labels 标签
        std::vector<unsigned> prop_ids_;  // property ids 属性ID

        AtomPropertyLabels();
        AtomPropertyLabels(const AtomPropertyLabels &that);
        AtomPropertyLabels& operator=(const AtomPropertyLabels &that);

        void print() const;
    };

    /**
     * @brief Represents a basic building block of an expression in a graph database.
     * This class provides a common interface for different types of atoms, such as literals, variables, and quantifiers.
     */
    class Atom{
    public:
        enum AtomType{LITERAL, PARAMETER, CASE_EXPRESSION, COUNT, LIST_COMPREHENSION, PATTERN_COMPREHENSION,
            QUANTIFIER, PATTERN_PREDICATE, FUNCTION_INVOCATION, EXISTENTIAL_SUBQUERY, VARIABLE,PARENTHESIZED_EXPRESSION,
            RANGE_EXPRESSION};
        AtomType atom_type_;  // atom type 原子表达式类型
        PVarset<unsigned> covered_var_id_; // covered variable id set 变量覆盖集合
        PVarset<std::pair<unsigned, unsigned>> covered_props_; // covered property set 属性覆盖集合

        Atom() = default;
        virtual ~Atom() { }
        virtual void print() const = 0;
    };

    /**
     * @brief Represents a literal in an expression.
    */
    class Literal: public Atom{
    public:
        enum LiteralType{BOOLEAN_LITERAL, NULL_LITERAL, INT_LITERAL, DOUBLE_LITERAL, STRING_LITERAL, LIST_LITERAL, 
                         MAP_LITERAL, PATH_LITERAL, DATETIME_LITERAL};
        LiteralType literal_type_;  // literal type 字面量的类型
        double d_;  // 64-bit float 64位浮点数
        // Only 64-bit long integer support in Literal.
        long long i_;   // 64-bit integer 64位整数
        bool b_;  // boolean 布尔值
        std::string s_; // string 字符串
        std::vector<Expression *> list_literal_; // list literal 列表字面量
        std::map<std::string, Expression *> map_literal_; // map literal 映射字面量
        GPStore::Value::PathContent path_literal_; // path literal 路径字面量
        GPStore::Value::DateTime datetime_literal_; // datetime literal 日期时间字面量
        Literal();
        Literal(LiteralType lt);
        Literal(const Literal& that);

        ~Literal();
        void print()const override;
    };

    /**
     * @brief Represents a parameter in an expression.
    */
    class Parameter: public Atom{
    public:
        /* Cypher中的参数'$' ( oC_SymbolicName | DecimalInteger )，总是$开头，后面是字母串或者自然数*/
        std::string param_name_; // parameter name 参数名
        unsigned  param_num_;  // parameter number 参数编号
        Parameter();
        Parameter(const Parameter& that);

        ~Parameter();
        void print() const override;
    };

    /**
     * @brief Represents a case expression.
    */
    class CaseExpression: public Atom{
    public:
        enum CaseType{SIMPLE, GENERIC};
        CaseType case_type_;  // case type case表达式类型
        Expression *test_exp_;  // test expression 测试表达式
        std::vector<Expression *> when_exps_; // when expressions when表达式
        std::vector<Expression *> then_exps_; // then expressions then表达式
        Expression *else_exp_;  // else expression else表达式

        CaseExpression();
        CaseExpression(CaseType ct);
        CaseExpression(const CaseExpression &that);
        ~CaseExpression();

        void print() const override;
    };

    /**
     * @brief Represents a count(*) expression
    */
    class Count : public Atom{
    public:
        unsigned long long cnt;       // save result for aggr function. 用于聚合函数的结果
        Count();
        Count(const Count& that);
        ~Count();

        void print() const override;
    };

    /**
     * @brief Represents a list comprehension.
    */
    class ListComprehension: public Atom{
    public:
        /* [var in expression where filter | trans]*/
        std::string var_name_;  // variable name 变量名
        unsigned var_id_;       // variable id 变量编号
        Expression *exp_;     // list expression 列表表达式
        Expression *filter_;         // filtering expression 过滤表达式
        Expression *trans_;          // transformation expression 转换表达式

        ListComprehension();
        ListComprehension(const ListComprehension& that);
        ~ListComprehension();


        void print() const override;
    };

// TODO: We dont support pattern comprehension in this version.
    /**
     * @brief Represents a pattern comprehension
    */
    class PatternComprehension: public Atom{
    public:
        std::string var_name_;	// variable name 变量名
        unsigned var_id_; // variable id 变量编号
        std::unique_ptr<RigidPattern> pattern_;  // pattern 模式
        Expression *filter_;         // filtering expression 过滤表达式
        Expression *trans_;          // transformation expression 转换表达式
        PatternComprehension();
        PatternComprehension(const PatternComprehension& that);
        ~PatternComprehension();

        void print() const override;
    };

// TODO: We dont support quantifier in this version.
    /**
     * @brief Represents a quantifier.
    */
    class Quantifier: public Atom{
    public:
        /* all(variable IN list WHERE predicate) */
        enum QuantifierType {ALL, ANY, SINGLE, NONE}; // quantifier type 量子表达式类型
        QuantifierType quantifier_type_;  // quantifier type 量子表达式类型
        std::string var_name_;  // variable name 变量名
        unsigned var_id_; // variable id 变量编号
        Expression *container_; // container expression 容器表达式
        Expression *exp_; // expression 表达式

        Quantifier();
        Quantifier(QuantifierType qt);
        Quantifier(const Quantifier& that);
        ~Quantifier();

        void print() const override;
    };

// TODO: We dont support PatternPredicate in this version.
    /**
     * @brief Represents a pattern predicate.
    */
    class PatternPredicate: public Atom{
    public:
        std::unique_ptr<RigidPattern> pattern_; // pattern 模式
        PatternPredicate();
        PatternPredicate(const PatternPredicate& that);
        ~PatternPredicate();

        void print() const override;
    };

    /**
     * @brief Represents a function invocation.
    */
    class FunctionInvocation : public Atom{
    public:
        GPStore::Value val_;    // to save result of Aggregation function 保存聚合函数的结果
        std::vector<std::string> func_name_; // function name 函数名
        bool distinct;	// distinct 去重
        std::vector<Expression *> args; // function arguments 函数参数

        FunctionInvocation();
        FunctionInvocation(const FunctionInvocation& that);
        ~FunctionInvocation();
        bool isAggregationFunction() const;
        void print() const override;
    };

// TODO: We dont support ExistentialSubquery in this version.
    /**
     * @brief Represents a existential subquery
    */
    class ExistentialSubquery : public Atom{
    public:
        //EXISTS SP? '{' SP? ( oC_RegularQuery | ( oC_Pattern ( SP? oC_Where )? ) ) SP? '}' ;
        void *query_tree_;  // query tree 查询树
        void *pattern_; // pattern 模式
        Expression *where_; // where clause where子句
        ExistentialSubquery();
        ExistentialSubquery(const ExistentialSubquery& that);
        ~ExistentialSubquery();

        void print() const override;
    };

    /**
     * @brief Represents a variable
    */
    class Variable: public Atom{
    public:
        static const unsigned ID_NONE = 0xffffffffU;  // invalid variable id 无效变量编号
        std::string var_; // variable name 变量名
        unsigned id_; // variable id 变量编号
        Variable();
        Variable(const Variable& that);
        Variable(const std::string & var_name, unsigned var_id = ID_NONE);
        ~Variable();

        void print() const override;
    };

    /**
     * @brief Represents a parenthesized expression
    */
    class ParenthesizedExpression: public Atom{
    public:
        Expression *exp_; // expression 表达式
        ParenthesizedExpression();
        ParenthesizedExpression(const ParenthesizedExpression& that);
        ~ParenthesizedExpression();

        void print() const override;
    };

    // For range Scan by property.
    /**
     * @brief Represents a range expression, for range scan by property.
    */
    class RangeExpression: public Atom{
    public:
       unsigned var_id_;  // variable id 变量编号
       TYPE_PROPERTY_ID prop_id_; // property id 属性编号
       std::unique_ptr<GPStore::Value> lower_; // lower bound 下限
       std::unique_ptr<GPStore::Value> upper_; // upper bound 上限
       RangeExpression();
       RangeExpression(const RangeExpression &that);
       ~RangeExpression();
       bool isEqualExpression() const;

       void print() const override;
    };


}   // namespace GPStore

#endif
