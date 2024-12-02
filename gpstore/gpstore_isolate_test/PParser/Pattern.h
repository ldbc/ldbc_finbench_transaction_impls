#ifndef PPARSER_PATTERN_H
#define PPARSER_PATTERN_H

#include <string>
#include <vector>
#include <map>
/* make it able to be compiled */
namespace GPStore{
    class NodePattern;
    class EdgePattern;
    class RigidPattern;
}
#include "../PQuery/PVarset.h"
#include "../Value/Expression.h"

/**
 * @namespace GPStore
 * @brief Namespace for gpstore, including many useful utilities.
*/
namespace GPStore{

    /**
     * @brief class for node pattern.
    */
    class NodePattern{
    public:
        // Only SPARQL has constant

        enum NodePatternType { NODE_VAR, NODE_CONST }; // Type of the node pattern 结点模式的类型

        NodePatternType type_;  // Type of the node pattern 结点模式的类型
        bool is_anno_var_;  // Whether the node pattern is an anonymous variable 当前结点模式是否为匿名变量
        std::string var_name_; // variable name, we give anonymous variable name such as @anno_1 变量名
        std::string constant_str_; // constant string 常量字符串
        unsigned var_id_; // variable id for variable, or constant id for constant 变量ID或结点ID

        std::vector<std::string> labels_; // node labels 结点标签

        // Only for update clauses, may be parameters, e.g. CREATE (a $prop)
        // For reading clauses, we extract constrain on property to filter.

        std::map<std::string, std::shared_ptr<GPStore::Expression>> properties_; // properties in the node pattern 点模式中的属性
        std::map<std::string, unsigned> prop_id;  //  property id for property name 属性名对应的属性ID
        std::string param_str_; // parameter string 参数字符串

        NodePattern();
        NodePattern(bool _is_anno, std::string var_name, unsigned _var_id);
        NodePattern(std::string _constant_str);
        NodePattern(const NodePattern& that);
        NodePattern& operator=(const NodePattern& that);
        bool constraintEmpty() const;
        ~NodePattern();

        void print() const;
    };

    /**
     * @brief class for edge pattern.
    */
    class EdgePattern{
    public:
        // Only SPARQL has constant
        enum EdgePatternType { EDGE_VAR, EDGE_CONST };  // Type of the edge pattern 边模式的类型
        enum EdgeArrowType { LEFT_ARROW, RIGHT_ARROW, UNDIRECTED }; // Arrow direction of the edge pattern 边的方向

        EdgePatternType type_;  // Type of the edge pattern 边模式的类型
        EdgeArrowType arrow_direction_; // Arrow direction 边的方向

        bool is_anno_var_;  // Whether the edge pattern is an anonymous variable 当前边模式是否为匿名变量
        std::string var_name_;  // variable name we give anonymous variable name such as @anno_1 变量名
        std::string constant_str_;  // constant string 常量字符串

        unsigned var_id_; // variable id for variable, or constant id for constant 变量ID或边ID

        bool is_edge_length_v_; // is variable-length pattern 是否是变长模式
        unsigned long long range_left_, range_right_; // left and right range 多跳边的范围
        static const unsigned long long RANGE_INF = 0xffffffffffffffffULL;  // for infinity 无穷大

        std::vector<std::string> edge_types_; // edge types, separated by '|' 边的类型

        std::map<std::string, std::shared_ptr<GPStore::Expression>> properties_;  // properties in the edge pattern 边模式中的属性
        std::map<std::string, unsigned> prop_id;  //  property id for property name 属性名对应的属性ID
        std::string param_str_; // parameter string 参数字符串

        EdgePattern();
        EdgePattern(bool is_anno, std::string _var_name, unsigned _var_id, EdgeArrowType _direction);
        EdgePattern(bool is_anno, std::string _var_name, unsigned _var_id, EdgeArrowType _direction, unsigned long long _left, unsigned long long _right);
        EdgePattern(std::string constant_str);
        EdgePattern(const EdgePattern& that);
        EdgePattern& operator= (const EdgePattern& that);
        bool constraintEmpty() const;
        ~EdgePattern();

        void print() const;

    };


    /**
     * @brief class for path pattern, which is a sequence of node patterns and edge patterns
    */
    class RigidPattern{
    public:
        enum RigidPatternType {PATH, SHORTEST_PATH, ALL_SHORTEST_PATHS }; // Type of the path pattern 路径模式的类型
        RigidPatternType type_; // Type of the path pattern 路径模式的类型

        std::string var_name_;  // variable name, optionally 变量名
        unsigned var_id_; // variable id 变量ID

        bool is_anno_var_;  // Whether the path pattern is an anonymous variable 当前路径模式是否为匿名变量

        std::vector<std::unique_ptr<NodePattern>> nodes_; // Node patterns 当前路径模式包含的结点模式
        std::vector<std::unique_ptr<EdgePattern>> edges_; // Edge patterns 当前路径模式包含的边模式

        // Named node vars and named edge vars
        PVarset<unsigned> covered_var_id_;  // Covered variables 覆盖的变量集合
        PVarset<unsigned> covered_node_var_id_; // Covered node variables 被覆盖的结点变量
        PVarset<unsigned> covered_edge_var_id_; // Covered edge variables 被覆盖的边变量
        RigidPattern();
        RigidPattern(const NodePattern& node);
        RigidPattern(const RigidPattern& that);
        RigidPattern& operator=(const RigidPattern& that);
        ~RigidPattern();

        void print() const;
    };

} // namespace GPStore

#endif