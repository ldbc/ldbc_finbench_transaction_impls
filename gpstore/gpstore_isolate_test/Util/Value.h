#ifndef VALUE_VALUE_H
#define VALUE_VALUE_H

/**
 * 所有的值都组织为Value。 一切赋值，构造都是深拷贝。
 * 警告：
 * (1)int comp(const Value & other) const兼适合比较大小，判断相等
 * (2)此处的大小只适用于order by的排序，和表达式中比较大小的'<'不是同一概念
 * (3)所谓相等，语义上适用于Distinct，Groupby，而不适用与表达式的相等‘=’
 * (4)hashCode函数暂时保留接口，不用
 * (5)
*/

#include <string>
#include <map>
#include <vector>

/**
 * @namespace GPStore
 * @brief Namespace for gpstore, including many useful utilities.
*/
namespace GPStore{
    typedef int int_32;
    typedef long long int_64;
    typedef unsigned short uint_16;
    typedef unsigned int uint_32;
    typedef unsigned long long uint_64;
    typedef unsigned char byte;

    /**
     * @brief A versatile class representing a value that can be of various types, and provides methods for constructing, comparing, and manipulating values of different types.
    */
    class Value {
    public:
        /**
         * @brief enum for value type
        */
        enum Type {
            // INTEGER: 32-bit; LONG: 64-bit; FLOAT: 64-bit
            INTEGER, FLOAT, STRING, BOOLEAN, LONG, // above && their array are storable
            NODE, EDGE, PATH,
            LIST, MAP,
            DATE_TIME,
            NO_VALUE,   // Cypher null
            ERROR_VALUE // ERROR
        };

        /**
         * @brief struct for path, i.e. a sequence of nodes and edges
        */
        struct PathContent{
            std::vector<uint_32> node_id_;  // node id 结点标识符
            std::vector<uint_64> edge_id_;  // edge id 边标识符
        };

        /**
         * @brief Represents a DateTime object with various time components.  
         * The structure includes fields for year, month, day, hour, minute,  
         * second, millisecond, microsecond, nanosecond, and timezone. 
         * Provides a default constructor and a copy constructor for initialization.
         * 
         * @remark 日期时间结构体
         */
        struct DateTime{
            // semantics: https://neo4j.com/docs/cypher-manual/current/functions/temporal/#functions-datetime-calendar
            uint_16 year; // An expression consisting of at least four digits that specifies the year.
            uint_16 month; // An integer between 1 and 12 that specifies the month.
            uint_16 day; // An integer between 1 and 31 that specifies the day of the month.
            uint_16 hour; // An integer between 0 and 23 that specifies the hour of the day.
            uint_16 minute; // An integer between 0 and 59 that specifies the number of minutes.
            uint_16 second; // An integer between 0 and 59 that specifies the number of seconds.
            uint_16 millisecond; // An integer between 0 and 999 that specifies the number of milliseconds.
            uint_32 microsecond; // An integer between 0 and 999,999 that specifies the number of microseconds.
            uint_32 nanosecond; // An integer between 0 and 999,999,999 that specifies the number of nanoseconds.
            std::string timezone; // An expression that specifies the time zone.
            DateTime():year(0), month(0), day(0), hour(0), minute(0), second(0), millisecond(0), microsecond(0), nanosecond(0){}
            DateTime(const DateTime&other):year(other.year), month(other.month), day(other.day), hour(other.hour), minute(other.minute), second(other.second), millisecond(other.millisecond), microsecond(other.microsecond), nanosecond(other.nanosecond), timezone(other.timezone){}
        };

        /**
         * @brief Union for various value types
         */
        union ValueUnion{
            int_32 Int; // 32-bit integer 32位整数
            int_64 Long;  // 64-bit integer 64位整数
            double Float; // 64-bit float 64位浮点数
            std::string *String;  // string 字符串
            bool Boolean; // boolean 布尔值

            uint_32 Node; // node id 结点标识符
            uint_64 Edge; // edge id 边标识符
            PathContent *Path; // path 路径
            std::vector<Value *> *List; // list 列表
            std::map<std::string, Value *> *Map; // map 字典

            DateTime *Datetime; // datetime 日期时间
        };

        Type type_; // value type 值类型
        ValueUnion data_; // value data 值数据

        Value();
        Value(Value* value_ptr);
        Value(Type _type_);
        Value(const Value &other);

        Value(int_32 int_);
        Value(int_64 long_);
        Value(double float_);
        Value(const std::string& str);
        Value(const char* s);
        Value(bool b);
        
        /* Construct Node or Edge */

        Value(Type _type, uint_32 _id);
        Value(Type _type, uint_64 _id);

        /* Construct Path */

        Value(const PathContent &path_);
        Value(const std::vector<unsigned >& node_id, const std::vector<uint_64> &edge_id);

        /* Construct List */

        Value(const std::vector<Value *> &list_, bool deep_copy = true);

        /* Construct Map */

        Value(const std::vector<std::string> &keys, const std::vector<Value *> &values, bool deep_copy = true);
        
        /* Construct DateTime */

        Value(const DateTime &datetime_);

        ~Value();

        bool isNull() const;
        bool isErrorValue() const;
        bool isNumber() const;
        bool isInteger() const;
        bool storable() const;

        bool isIntArray() const;
        bool isLongArray() const;
        bool isFloatArray() const;
        bool isStringArray() const;
        bool isBooleanArray() const;

        uint_64 convertToBytes(void * ptr);

        void constructFromBytes(const void * ptr, uint_64 n);

        Type getType() const;
        int_64 hashCode() const ;
        bool operator==(const Value& other) const;    // check equivalence for DISTINCT
        bool operator<(const Value& other) const;     // used for ORDER
        int comp(const Value & other) const;    // return -1 if less than, 1 if greater, 0 if equal
        Value& operator=(const Value&other);


        const std::vector<GPStore::Value*>* getListContent();

        void append(const Value& value);    // deep copy
        void append(Value * value);         // shallow copy

        Value &operator[](uint_64 index);

        void insert(const std::string &key, const Value &value);

        void insert(const std::string &key, Value *val_ptr);

        Value *search(const std::string &key);

        uint_64 size();

        std::string toString() const ;
        int toInt() const ;
        long long toLLong() const ;

        static Value * ValueDeepCopy(const Value *value);

        void moveTo(GPStore::Value & that);
        void swap(GPStore::Value &that) noexcept;

        void SetDatetime(long long sumtime, long long nanosecond_);
        void SetDatetime(GPStore::Value &other);
        std::pair<uint_16, uint_16> getMonthDay();

    private:

        /* for order by return -1 if less than, 1 if greater, 0 if equal */

        int CompareIntWith(const Value &other) const;
        int CompareLongWith(const Value &other) const;
        int CompareFloatWith(const Value &other) const;
        int CompareStringWith(const Value &other) const;
        int CompareBooleanWith(const Value &other) const;
        int CompareNodeWith(const Value &other) const;
        int CompareEdgeWith(const Value &other) const;
        int ComparePathWith(const Value &other) const;
        int CompareListWith(const Value &other) const;
        int CompareMapWith(const Value &other) const;
        int CompareNoValueWith(const Value &other) const;
        int CompareDatetimeWith(const Value &other) const;

        /* for distinct, group by */

        bool IntEqualTo(const Value &other) const;
        bool LongEqualTo(const Value &other) const;
        bool FloatEqualTo(const Value &other) const;
        bool StringEqualTo(const Value &other) const;
        bool BooleanEqualTo(const Value &other) const;
        bool NodeEqualTo(const Value &other) const;
        bool EdgeEqualTo(const Value &other) const;
        bool PathEqualTo(const Value &other) const;
        bool ListEqualTo(const Value &other) const;
        bool MapEqualTo(const Value &other) const;
        bool NoValueEqualTo(const Value &other) const;
        bool DatetimeEqualTo(const Value &other) const;
    public:
        /* Construct Functions */

        void ConstructFrom(const Value& other);

        /* destruct functions */

        void Destruct();
    };


/* helper functions */
}
#endif