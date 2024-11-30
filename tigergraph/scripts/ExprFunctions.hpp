/******************************************************************************
 * Copyright (c) 2015-2016, TigerGraph Inc.
 * All rights reserved.
 * Project: TigerGraph Query Language
 * udf.hpp: a library of user defined functions used in queries.
 *
 * - This library should only define functions that will be used in
 *   TigerGraph Query scripts. Other logics, such as structs and helper
 *   functions that will not be directly called in the GQuery scripts,
 *   must be put into "ExprUtil.hpp" under the same directory where
 *   this file is located.
 *
 * - Supported type of return value and parameters
 *     - int
 *     - float
 *     - double
 *     - bool
 *     - string (don't use std::string)
 *     - accumulators
 *
 * - Function names are case sensitive, unique, and can't be conflict with
 *   built-in math functions and reserve keywords.
 *
 * - Please don't remove necessary codes in this file
 *
 * - A backup of this file can be retrieved at
 *     <tigergraph_root_path>/dev_<backup_time>/gdk/gsql/src/QueryUdf/ExprFunctions.hpp
 *   after upgrading the system.
 *
 ******************************************************************************/

#ifndef EXPRFUNCTIONS_HPP_
#define EXPRFUNCTIONS_HPP_

#include <stdlib.h>
#include <stdio.h>
#include <string>
#include <gle/engine/cpplib/headers.hpp>
#include <vector>
#include <algorithm>

/**     XXX Warning!! Put self-defined struct in ExprUtil.hpp **
 *  No user defined struct, helper functions (that will not be directly called
 *  in the GQuery scripts) etc. are allowed in this file. This file only
 *  contains user-defined expression function's signature and body.
 *  Please put user defined structs, helper functions etc. in ExprUtil.hpp
 */
#include "ExprUtil.hpp"

namespace UDIMPL {
  typedef std::string string; //XXX DON'T REMOVE

  /****** BIULT-IN FUNCTIONS **************/
  /****** XXX DON'T REMOVE ****************/
  inline int64_t str_to_int (string str) {
    return atoll(str.c_str());
  }

  inline int64_t float_to_int (float val) {
    return (int64_t) val;
  }

  using namespace std;

  inline bool circle_and_len_check(string str_path, string key, uint path_len, string delimiter) {
    size_t pos_start = 0, pos_end, delim_len = delimiter.length();
    string token;
    vector<string> vector_str;

    while ((pos_end = str_path.find (delimiter, pos_start)) != string::npos) {
        token = str_path.substr (pos_start, pos_end - pos_start);
        if(token != ""){
            vector_str.push_back(token);
        }
        pos_start = pos_end + delim_len;
    }
    
    token = str_path.substr (pos_start);
    if(token != "") {
        vector_str.push_back(token);
    }

    if (vector_str.size() == path_len && find(vector_str.begin(), vector_str.end(), key) == vector_str.end()) {
        return true;
    }
    return false;
  }

  inline uint64_t get_path_len(string str_path, string delimiter) {
    size_t pos_start = 0, pos_end, delim_len = delimiter.length();
    string token;
    vector<string> vector_str;

    while ((pos_end = str_path.find (delimiter, pos_start)) != string::npos) {
        token = str_path.substr (pos_start, pos_end - pos_start);
        if(token != ""){
            vector_str.push_back(token);
        }
        pos_start = pos_end + delim_len;
    }
    
    token = str_path.substr (pos_start);
    if(token != "") {
        vector_str.push_back(token);
    }
    if (vector_str.size() >= 1) {
        return vector_str.size() - 1;
    }
    return 0;
  }
}
/****************************************/

#endif /* EXPRFUNCTIONS_HPP_ */
