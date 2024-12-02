#ifndef PPARSER_CYPHERAST_H
#define PPARSER_CYPHERAST_H

#include <vector>
#include <string>
#include <unordered_map>
#include <memory>
#include "Pattern.h"
#include "../Util/Util.h"

// CypherAST
class CypherAST;
class SingleQueryAST;
class QueryUnitAST;

// clauses
class ReadingAST;
class UpdatingAST;

// reading clauses
class MatchAST;
class UnwindAST;
class InQueryCallAST;

// Updating clauses
class CreateAST;
class MergeAST;
class DeleteAST;
class SetAST;
class RemoveAST;

// Return, With Clause
class WithReturnAST;

// Aux Class
class SetItemAST;
class RemoveItemAST;


/**
 * @brief Abstract Syntax Tree for a Cypher query.
*/
class CypherAST{
public:
    std::vector<std::unique_ptr<SingleQueryAST>> single_querys_;  // single querys that are combined by UNION 多个并连接的单独查询
    std::vector<bool> union_all_; // union all or not 是否为UnionALL
    std::shared_ptr<std::vector<std::string>> id2var_name_;   // a mapping from variable id to variable name 从变量id到变量名的映射
    std::shared_ptr<std::unordered_map<std::string, unsigned>> prop2id_;  // a mapping from property name to property id 从属性名到属性id的映射
    std::shared_ptr<std::map<unsigned, std::string>> prop_id2string_; // a mapping from valid property id to property name 从有效的属性id到属性名的映射
    CypherAST() = default;
    ~CypherAST() = default;
    void print(int dep) const;
};

/**
 * @brief Abstract Syntax Tree for a single query, which is a chain of WITH clauses, and optionally ends with a RETURN clause.
*/
class SingleQueryAST{
public:
    std::vector<std::unique_ptr<QueryUnitAST>> query_units_;  // query units that are combined by WITH WITH连接的查询单元
    SingleQueryAST() = default;
    ~SingleQueryAST() = default;
    void print(int dep) const;
};

/**
 * @brief Abstract Syntax Tree for a query unit, which is a chain of Reading and Updating clauses, and optionally ends with a WITH/RETURN clause.
*/
class QueryUnitAST{
public:
    std::vector<std::unique_ptr<ReadingAST>> reading_;  // reading clauses 读取语句
    std::vector<std::unique_ptr<UpdatingAST>> updating_;  // updating clauses 更新语句
    std::unique_ptr<WithReturnAST> with_return_;  // with or return clause WITH或者RETURN语句
    QueryUnitAST() = default;
    ~QueryUnitAST() = default;
    void print(int dep) const;
};

/**
 * @brief Base class for reading clauses.
*/
class ReadingAST{
public:
    enum ReadingForm{MATCH, UNWIND, INQUERY_CALL};
    ReadingForm reading_form_;  // Type of the reading clause 读取语句的类型
    ReadingAST() = default;
    virtual ~ReadingAST(){ }
    virtual void print(int dep) const = 0;
};

/**
 * @brief Base class for updating clauses.
*/
class UpdatingAST{
public:
    enum UpdatingForm{CREATE, MERGE, DELETE, SET, REMOVE};
    UpdatingForm updating_form_;  // Type of the updating clause 更新语句的类型
    UpdatingAST() = default;
    virtual ~UpdatingAST() { };
    virtual void print(int dep) const = 0;
};

/**
 * ALL Reading Clauses
*/

/**
 * @brief Abstract Syntax Tree for MATCH clause.
*/
class MatchAST: public ReadingAST{
public:
    bool is_optional_;  // Optional or not 是否可选
    std::vector<std::unique_ptr<GPStore::RigidPattern>> pattern_; // Patterns in this match clause 当前匹配语句中的模式
    std::unique_ptr<GPStore::Expression> where_;  // (optional) where clause in this match clause 当前匹配语句中的过滤条件

    MatchAST(){ reading_form_ = MATCH; }
    ~MatchAST() = default;
    void print(int dep) const override;
};

/**
 * @brief Abstract Syntax Tree for UNWIND clause.
*/
class UnwindAST: public ReadingAST{
public:
    std::unique_ptr<GPStore::Expression> exp_;  // expression to be unwound 需要被展开的表达式
    std::string var_name_;  // new variable name 新变量名
    unsigned var_id_; // new variable id 新变量id
    UnwindAST(){ reading_form_ = UNWIND; };
    ~UnwindAST() = default;
    void print(int dep) const override;
};

/**
 * @brief Abstract Syntax Tree for Call clauses.
*/
class InQueryCallAST: public ReadingAST{
public:
    std::vector<std::string> procedure_name_; // procedure name, specified by '.' 被.分割的存储过程名
    std::vector<std::unique_ptr<GPStore::Expression>> args_; // arguments to the procedure 存储过程的参数
    std::vector<std::string> yield_items_;  // return column names 存储过程返回值
    std::vector<std::string> alias_;  // alias for yield items 返回的列名的别名

    std::vector<std::string> yield_var_;    // variable name after alias 更名后的变量名
    std::vector<unsigned> yield_var_id_;    // variable id 变量id
    std::unique_ptr<GPStore::Expression> where_;  // where clause on yield items 过滤条件

    InQueryCallAST(){reading_form_ = INQUERY_CALL;}
    ~InQueryCallAST()= default;

    void print(int dep) const override;
};

/**
 * ALL Updating Clauses
*/

/**
 * @brief Abstract Syntax Tree for CREATE clause.
*/
class CreateAST: public UpdatingAST{
public:
    std::vector<std::unique_ptr<GPStore::RigidPattern>> pattern_; // Patterns in this create clause 当前创建语句中的模式
    CreateAST() {updating_form_ = CREATE;}
    ~CreateAST()= default;
    void print(int dep) const override;
};

/**
 * @brief Abstract Syntax Tree for MERGE clause.
*/
class MergeAST: public UpdatingAST{
public:
    std::unique_ptr<GPStore::RigidPattern> rigid_pattern_;  // Pattern in this merge clause 当前合并语句中的模式
    std::vector<bool> is_on_match_;  // whether it is on match 是否包含匹配时的动作
    std::vector<std::unique_ptr<SetAST>> set_actions_;  // Set actions when matching 匹配时的设置动作
    MergeAST(){updating_form_ = MERGE;}
    ~MergeAST()= default;
    void print(int dep) const override;
};

/**
 * @brief Abstract Syntax Tree for DELETE clause.
*/
class DeleteAST: public UpdatingAST{
public:
    bool detach;  // Detach delete or not 是否是分离删除
    std::vector<std::unique_ptr<GPStore::Expression>> exp_; // items to be deleted 要删除的项
    DeleteAST(){updating_form_ = DELETE;}
    ~DeleteAST()= default;
    void print(int dep) const override;
};

/**
 * @brief Abstract Syntax Tree for SET clause.
*/
class SetAST: public UpdatingAST{
public:
    std::vector<std::unique_ptr<SetItemAST>> set_items_; // Set items in this set clause 当前设置语句中的设置项
    SetAST(){updating_form_ = SET;}
    ~SetAST()= default;
    void print(int dep) const override;
};

/**
 * @brief Abstract Syntax Tree for REMOVE clause.
*/
class RemoveAST: public UpdatingAST{
public:
    std::vector<std::unique_ptr<RemoveItemAST> > remove_items_; // Remove items in this remove clause 当前删除语句中的删除项
    RemoveAST(){updating_form_ = REMOVE;};
    ~RemoveAST()= default;
    void print(int dep) const override;
};

/**
 * @brief Abstract Syntax Tree for WITH/RETURN clause.
*/
class WithReturnAST{
public:
    bool with_; // is with or return 是WITH语句还是RETURN语句
    bool distinct_; // is distinct 是否去重
    bool asterisk_; // return * 是否返回全部变量
    bool aggregation_;  // does the projection contain aggr function ? 是否包含聚合函数
    std::vector<std::unique_ptr<GPStore::Expression>> proj_exp_;  // All the expressions in the projection 投影表达式
    std::vector<std::string> proj_exp_text_;    // plain text for the projection expressions 投影表达式的纯文本
    std::vector<std::string> alias_;            // alias for the projection, "" means no alias 投影表达式的别名
    std::vector<std::string> column_name_;      // final column name 投影后的列名
    std::vector<unsigned> column_var_id_;       // variable id of the final column 变量id
    std::vector<unsigned> implict_proj_var_id_; // variable id of the implict projection, e.g. in return *, order by xx. 隐含的投影变量id

    unsigned skip_; // skip in return 跳过若干行
    unsigned limit_;  // limit in return 限制行数
    std::vector<std::unique_ptr<GPStore::Expression>> order_by_;    // order by keys 排序键
    std::vector<bool> ascending_;    // whether the order is ascending 是否升序
    std::unique_ptr<GPStore::Expression> where_;  // where clause in with / return clause 过滤条件
    WithReturnAST(): skip_(0), limit_(INVALID){ }
    ~WithReturnAST()= default;
    void print(int dep) const;
};

/**
 * Auxiliary class for updating clauses
*/

/**
 * @brief Abstract Syntax Tree for set item.
*/
class SetItemAST{
public:
    /**
     * oC_SetItem
       :  ( oC_PropertyExpression SP? '=' SP? oC_Expression )
           | ( oC_Variable SP? '=' SP? oC_Expression )
           | ( oC_Variable SP? '+=' SP? oC_Expression )
           | ( oC_Variable SP? oC_NodeLabels )
           ;
    */
    enum SetType{SINGLE_PROPERTY, NODE_LABELS, ASSIGN_PROPERTIES, ADD_PROPERTIES};
    SetType set_type_;  // type of this set item 当前设置项的类型
    std::unique_ptr<GPStore::Expression> prop_exp_; // property expression 属性表达式
    std::string var_name_;  // variable name 变量名
    unsigned var_id_;  // variable id 变量id
    std::unique_ptr<GPStore::Expression> exp_;  // expression, in the right side of '=' 等号右侧的表达式
    std::vector<std::string> labels_;  // labels 所有标签
    SetItemAST() = default;
    ~SetItemAST() = default;
    void print(int dep) const;
};

/**
 * @brief Abstract Syntax Tree for remove item.
*/
class RemoveItemAST{
public:
    // If delete labels, only use ariable_name,label_names
    // If delete property, only use ProperyExpression

    std::string var_name_;  // variable name 变量名
    unsigned var_id_;  // variable id 变量id
    std::vector<std::string> labels_; // labels 所有标签
    std::unique_ptr<GPStore::Expression> prop_exp_; // property expression 属性表达式
    RemoveItemAST()= default;
    ~RemoveItemAST()= default;
    void print(int dep) const;
};

#endif