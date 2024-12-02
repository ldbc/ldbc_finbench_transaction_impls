/*=============================================================================
# Filename: PVarset.h
# Author: Chunshan Zhao
# Mail: 1900016633@pku.edu.cn
# Last Modified: 2023-04-19
# Description:
=============================================================================*/
#ifndef PQUERY_PVARSET_H
#define PQUERY_PVARSET_H

#include <iostream>
#include <string>
#include <vector>

inline std::ostream&  operator<<(std::ostream& o, std::pair<unsigned, unsigned> p){
    std::cout << "(" << p.first << "," << p.second << ")";
    return o;
}

/**
 * @brief This class is a template class that represents a set of variables of type T.
 * The contents of the set are stored in a std::vector<T> object, which is efficient if the set is small.
*/
template <class T>
class PVarset
{
public:
    std::vector<T> vars;  // the set of variables in this PVarset 所有的变量

    PVarset(){};
    PVarset(const T &_var){  this->addVar(_var);}
    PVarset(const PVarset<T> & that){
        for(auto & v : that.vars)
            vars.push_back(v);
    }
    PVarset(const std::vector<T> &_vars)
    {
        for (int i = 0; i < (int)_vars.size(); i++)
            this->addVar(_vars[i]);
    }

    bool empty() const { return this->vars.empty(); }
    void clear() { vars.clear(); }
    int getVarsetSize() const { return this->vars.size(); }
    /**
     * @brief Checks if the given variable exists in the set.
     * @param _var The variable to check for.
     * @return true if the variable exists in the set, false otherwise.
     * @remark 检查给定变量是否在集合中
     */
    bool findVar(const T &_var) const{
        if (this->vars.empty())
            return false;

        for (int i = 0; i < (int)this->vars.size(); i++)
            if (this->vars[i] == _var)    return true;

        return false;
    }
    /**
     * @brief Adds a variable to the set, if it is not already present.
     * @param _var The variable to add.
     * @remark 将变量添加到集合若集合中不存在该变量
     */
    void addVar(const T &_var){
        if (!this->findVar(_var))
            this->vars.push_back(_var);
    }

    /**
     * @brief Combines the variables from this set with another PVarset.
     *
     * This operator creates a new PVarset which contains all unique variables
     * from both this PVarset and the provided PVarset. It performs a union
     * operation on the two sets of variables.
     *
     * @param _varset The PVarset to combine with this one.
     * @return A new PVarset containing all unique variables from both sets.
     * @remark 合并两个集合
     */
    PVarset operator + (const PVarset &_varset) const{
        PVarset<T> r(*this);

        for (int i = 0; i < (int)_varset.vars.size(); i++)
            r.addVar(_varset.vars[i]);

        return r;
    }

    PVarset& operator += (const PVarset &_varset){
        for (int i = 0; i < (int)_varset.vars.size(); i++)
            this->addVar(_varset.vars[i]);

        return *this;
    }

    /**
     * @brief Computes the intersection of this set with another PVarset.
     *
     * This operator creates a new PVarset which contains only the variables
     * that are present in both this PVarset and the provided PVarset.
     * It performs an intersection operation on the two sets of variables.
     *
     * @param _varset The PVarset to intersect with this one.
     * @return A new PVarset containing only the variables that are present in both sets.
     * @remark 两个集合的交集
     */
    PVarset operator * (const PVarset &_varset) const{
        PVarset<T> r;

        for (int i = 0; i < (int)this->vars.size(); i++)
            if (_varset.findVar(this->vars[i]))
                r.addVar(this->vars[i]);

        return r;
    }

    /**
     * @brief Computes the difference of this set with another PVarset.
     *
     * This operator creates a new PVarset which contains only the variables
     * that are present in this PVarset but not in the provided PVarset.
     * It performs a difference operation on the two sets of variables.
     *
     * @param _varset The PVarset to subtract from this one.
     * @return A new PVarset containing only the variables that are present in this set but not in the other set.
     * @remark 两个集合的差集
     */
    PVarset operator - (const PVarset &_varset) const{
        PVarset<T> r;

        for (int i = 0; i < (int)this->vars.size(); i++)
            if (!_varset.findVar(this->vars[i]))
                r.addVar(this->vars[i]);

        return r;
    }

    PVarset& operator = (const PVarset & _varset) {
        if(this == &_varset) return *this;
        vars.clear();
        for(auto & v : _varset.vars)
            vars.emplace_back(v);
        return *this;
    }


    /**
     * @brief Compares this PVarset with another PVarset for equality.
     *
     * This operator checks if two PVarsets contain the same variables.
     * It returns true if both PVarsets have the same size and all variables
     * in this PVarset are present in the provided PVarset.
     *
     * @param _varset The PVarset to compare against this one.
     * @return True if both PVarsets are equal, false otherwise.
     * @remark 判断两个变量集是否相等
     */
    bool operator ==(const PVarset &_varset) const{
        if ((int)this->vars.size() != (int)_varset.vars.size())
            return false;

        for (int i = 0; i < (int)this->vars.size(); i++)
            if (!_varset.findVar(this->vars[i]))
                return false;

        return true;
    }

    /**
     * @brief Checks if two PVarsets have any variable in common.
     *
     * This function checks if there is any variable that is present in both
     * this PVarset and the provided PVarset.
     *
     * @param _varset The PVarset to compare against this one.
     * @return True if there is any variable in common, false otherwise.
     * @remark 检查两个变量集是否有公共变量
     */
    bool hasCommonVar(const PVarset &_varset) const{
        for (int i = 0; i < (int)this->vars.size(); i++)
            if (_varset.findVar(this->vars[i]))
                return true;

        return false;
    }

    /**
     * @brief Checks if all variables in this PVarset are present in the provided PVarset.
     *
     * This function checks if all variables in this PVarset are present in the provided
     * PVarset. It returns true if all variables are present, false otherwise.
     *
     * @param _varset The PVarset to compare against this one.
     * @return True if all variables belong to the provided PVarset, false otherwise.
     * @remark 检查该变量集的所有变量是否都在提供的变量集中
     */
    bool belongTo(const PVarset &_varset) const{
        for (int i = 0; i < (int)this->vars.size(); i++)
            if (!_varset.findVar(this->vars[i]))
                return false;

        return true;
    }

    std::vector<int> mapTo(const PVarset &_varset) const{
        std::vector<int> r;

        for (int i = 0; i < (int)this->vars.size(); i++)
        {
            r.push_back(-1);
            for (int j = 0; j < (int)_varset.vars.size(); j++)
                if (this->vars[i] == _varset.vars[j])
                    r[i] = j;
        }

        return r;

    }

    void print() const{
        printf("Varset: ");

        for (int i = 0; i < (int)this->vars.size(); i++)
        {
            std::cout << this->vars[i] << " ";
        }

        printf("\n");
    }
};


#endif